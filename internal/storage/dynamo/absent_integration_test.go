package dynamo

import (
	"testing"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/storage"
)

// TestIntegrationAddColumnAbsent: rows written before ALTER TABLE ADD COLUMN
// read Absent for the new DEFAULT column under MarkAbsent (zero otherwise),
// and DeleteRows still finds them when the engine passes the computed default.
func TestIntegrationAddColumnAbsent(t *testing.T) {
	e := newEnv(t)
	for _, engine := range []string{catalog.EngineMergeTree, catalog.EngineReplacingMergeTree} {
		t.Run(engine, func(t *testing.T) {
			e.t = t
			tbl := e.table(&catalog.Table{
				Database: "default", Name: "tenants_" + engine, Engine: engine,
				Columns: cols("tenant_id", "String", "seats", "UInt64"),
				OrderBy: []string{"tenant_id"},
			})
			e.insert(tbl, storage.Row{"acme", uint64(10)}, storage.Row{"globex", uint64(1)})
			alt := *tbl
			alt.Columns = append(append([]catalog.Column{}, tbl.Columns...),
				catalog.Column{Name: "region", Type: chtype.TString, TypeText: "String", DefaultKind: "DEFAULT", DefaultExpr: "'us-east-1'"},
				catalog.Column{Name: "seat_cap", Type: chtype.TUInt64, TypeText: "UInt64", DefaultKind: "DEFAULT", DefaultExpr: "seats * 2"})
			if err := e.cat.UpdateTable(e.ctx, &alt); err != nil {
				t.Fatal(err)
			}
			tbl, _ = e.cat.Table("default", tbl.Name)
			e.insert(tbl, storage.Row{"hooli", uint64(100), "eu-west-1", uint64(0)})

			eqRows(t, e.all(tbl), []storage.Row{
				{"acme", uint64(10), "", uint64(0)}, {"globex", uint64(1), "", uint64(0)}, {"hooli", uint64(100), "eu-west-1", uint64(0)},
			})
			got := e.scan(tbl, storage.ScanRequest{MarkAbsent: true})
			eqRows(t, got, []storage.Row{
				{"acme", uint64(10), storage.Absent, storage.Absent}, {"globex", uint64(1), storage.Absent, storage.Absent},
				{"hooli", uint64(100), "eu-west-1", uint64(0)},
			})
			got = e.scan(tbl, storage.ScanRequest{MarkAbsent: true, Columns: []int{0, 3},
				Ranges: []storage.KeyRange{{Partition: []any{"acme"}}}})
			if len(got) != 1 || got[0][0] != "acme" || !storage.IsAbsent(got[0][3]) {
				t.Fatalf("projected key read: %v", got)
			}
			// The engine deletes the rows it read, with defaults computed.
			if err := e.store.DeleteRows(e.ctx, tbl, []storage.Row{{"acme", uint64(10), "us-east-1", uint64(20)}}); err != nil {
				t.Fatal(err)
			}
			eqRows(t, e.all(tbl), []storage.Row{{"globex", uint64(1), "", uint64(0)}, {"hooli", uint64(100), "eu-west-1", uint64(0)}})
		})
	}
}
