package dynamo

import (
	"fmt"
	"testing"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/storage"
)

// splitOutput is one source row's view output: index i has key i%3 and
// value i.
func splitOutput(n int) []storage.MergeRow {
	rows := make([]storage.MergeRow, n)
	for i := range rows {
		rows[i] = storage.MergeRow{Row: storage.Row{fmt.Sprint("k", i%3), uint64(i)},
			Source: storage.SourceRef{View: "default.mv", RowID: "r1"}, Index: i}
	}
	return rows
}

// The engine merges one source row's output in batches of 500, even inside
// the row, and a retry may split it elsewhere. Merge applies each (Source,
// key, Index) once whatever the split, for every kind of target.
func TestIntegrationMergeSplitBatchesApplyOnce(t *testing.T) {
	e := newEnv(t)
	const n = 300
	out := splitOutput(n)
	targets := []struct {
		engine string
		args   []string
		want   []storage.Row
	}{
		{catalog.EngineSummingMergeTree, nil, []storage.Row{
			{"k0", uint64(n * (n - 3) / 6)}, {"k1", uint64(n*(n-3)/6 + n/3)}, {"k2", uint64(n*(n-3)/6 + 2*n/3)}}},
		{catalog.EngineReplacingMergeTree, []string{"v"}, []storage.Row{
			{"k0", uint64(n - 3)}, {"k1", uint64(n - 2)}, {"k2", uint64(n - 1)}}},
		{catalog.EngineMergeTree, nil, nil},
	}
	for _, c := range targets {
		tbl := e.table(&catalog.Table{Database: "default", Name: "split_" + c.engine, Engine: c.engine, EngineArgs: c.args,
			Columns: cols("k", "String", "v", "UInt64"), OrderBy: []string{"k"}})
		for _, cuts := range [][]int{{0, 125, 250}, {0, 150, n}} {
			for i := 0; i+1 < len(cuts); i++ {
				e.merge(tbl, out[cuts[i]:cuts[i+1]]...)
			}
		}
		e.merge(tbl, out[250:]...)
		got := e.all(tbl)
		if c.engine == catalog.EngineMergeTree {
			if len(got) != n {
				t.Fatalf("%s: %d rows, want %d", c.engine, len(got), n)
			}
			continue
		}
		eqRows(t, got, c.want)
	}
}
