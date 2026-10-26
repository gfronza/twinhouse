package memstore

import (
	"context"
	"io"
	"reflect"
	"sort"
	"testing"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/storage"
)

func mkTable(engine string, cols [][2]string, pk, sk []string) *catalog.Table {
	t := &catalog.Table{Database: "db", Name: engine, Engine: engine, PartitionKey: pk, SortKey: sk, PhysicalName: "db." + engine}
	t.OrderBy = append(append([]string{}, pk...), sk...)
	for _, c := range cols {
		t.Columns = append(t.Columns, catalog.Column{Name: c[0], Type: chtype.MustParse(c[1]), TypeText: c[1]})
	}
	return t
}

func newStore(t *testing.T, tb *catalog.Table) *Store {
	t.Helper()
	s := New()
	if err := s.EnsureTable(context.Background(), tb); err != nil {
		t.Fatal(err)
	}
	return s
}

func scan(t *testing.T, s *Store, tb *catalog.Table, req storage.ScanRequest) []storage.Row {
	t.Helper()
	it, err := s.Scan(context.Background(), tb, req)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	var out []storage.Row
	for {
		r, err := it.Next(context.Background())
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
}

func eq(t *testing.T, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v\nwant %#v", got, want)
	}
}

func TestMergeTreeInsertAndScan(t *testing.T) {
	tb := mkTable(catalog.EngineMergeTree, [][2]string{{"a", "String"}, {"b", "UInt64"}, {"c", "String"}}, []string{"a"}, []string{"b"})
	s := newStore(t, tb)
	ctx := context.Background()
	res, err := s.Insert(ctx, tb, []storage.Row{{"x", uint64(2), "p"}, {"x", uint64(1), "q"}, {"x", uint64(2), "p"}, {"y", uint64(5), "r"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.RowIDs) != 4 || res.RowIDs[0] == res.RowIDs[2] {
		t.Fatalf("row ids must be unique: %v", res.RowIDs)
	}
	eq(t, res.Skipped, []bool{false, false, false, false})
	eq(t, s.ItemCount(tb), 4) // duplicates are distinct items
	all := scan(t, s, tb, storage.ScanRequest{})
	eq(t, all, []storage.Row{{"x", uint64(1), "q"}, {"x", uint64(2), "p"}, {"x", uint64(2), "p"}, {"y", uint64(5), "r"}})

	ranges := func(rs ...storage.KeyRange) storage.ScanRequest { return storage.ScanRequest{Ranges: rs} }
	cases := []struct {
		req  storage.ScanRequest
		want int
	}{
		{ranges(storage.KeyRange{Partition: []any{"x"}}), 3},
		{ranges(storage.KeyRange{Partition: []any{"x"}, SortPrefix: []any{uint64(2)}}), 2},
		{ranges(storage.KeyRange{Partition: []any{"x"}, Lower: &storage.Bound{Value: uint64(1), Inclusive: false}}), 2},
		{ranges(storage.KeyRange{Partition: []any{"x"}, Upper: &storage.Bound{Value: uint64(2), Inclusive: false}}), 1},
		{ranges(storage.KeyRange{Partition: []any{"x"}, Lower: &storage.Bound{Value: uint64(1), Inclusive: true}, Upper: &storage.Bound{Value: uint64(2), Inclusive: true}}), 3},
		{ranges(storage.KeyRange{Partition: []any{"x"}}, storage.KeyRange{Partition: []any{"y"}}), 4},
		{ranges(storage.KeyRange{Partition: []any{"z"}}), 0},
		{storage.ScanRequest{Ranges: []storage.KeyRange{}}, 0},
		{storage.ScanRequest{Limit: 2}, 2},
	}
	for i, c := range cases {
		if got := len(scan(t, s, tb, c.req)); got != c.want {
			t.Errorf("case %d: got %d rows, want %d", i, got, c.want)
		}
	}
	rev := scan(t, s, tb, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{"x"}}}, Reverse: true})
	eq(t, rev[0][1], uint64(2))
	proj := scan(t, s, tb, storage.ScanRequest{Columns: []int{1}, Ranges: []storage.KeyRange{{Partition: []any{"y"}}}})
	eq(t, proj, []storage.Row{{nil, uint64(5), nil}})

	// Merge into a MergeTree target is idempotent per (source, index).
	mr := []storage.MergeRow{{Row: storage.Row{"m", uint64(1), "v"}, Source: storage.SourceRef{View: "db.v", RowID: "r1"}}}
	for i := 0; i < 3; i++ {
		if err := s.Merge(ctx, tb, mr); err != nil {
			t.Fatal(err)
		}
	}
	mr[0].Index = 1
	if err := s.Merge(ctx, tb, mr); err != nil {
		t.Fatal(err)
	}
	eq(t, len(scan(t, s, tb, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{"m"}}}})), 2)

	if err := s.DeleteRows(ctx, tb, []storage.Row{{"x", uint64(2), "p"}}); err != nil {
		t.Fatal(err)
	}
	eq(t, len(scan(t, s, tb, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{"x"}}}})), 1)
	if err := s.TruncateTable(ctx, tb); err != nil {
		t.Fatal(err)
	}
	eq(t, s.ItemCount(tb), 0)
	if err := s.DropTable(ctx, tb); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Insert(ctx, tb, []storage.Row{{"x", uint64(1), ""}}); !cherr.Is(err, cherr.UnknownTable) {
		t.Fatalf("insert into a dropped table: %v", err)
	}
	if _, err := s.Scan(ctx, tb, storage.ScanRequest{}); !cherr.Is(err, cherr.UnknownTable) {
		t.Fatalf("scan of a dropped table: %v", err)
	}
}

func TestReplacingVersions(t *testing.T) {
	tb := mkTable(catalog.EngineReplacingMergeTree, [][2]string{{"k", "String"}, {"ver", "UInt64"}, {"v", "String"}}, []string{"k"}, nil)
	tb.VersionColumn = "ver"
	s := newStore(t, tb)
	ctx := context.Background()
	res, err := s.Insert(ctx, tb, []storage.Row{{"a", uint64(2), "two"}, {"a", uint64(1), "one"}, {"a", uint64(2), "two-again"}, {"b", uint64(0), "b"}})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, res.Skipped, []bool{false, true, false, false})
	eq(t, scan(t, s, tb, storage.ScanRequest{}), []storage.Row{{"a", uint64(2), "two-again"}, {"b", uint64(0), "b"}})

	noVer := mkTable(catalog.EngineReplacingMergeTree, [][2]string{{"k", "String"}, {"v", "String"}}, []string{"k"}, nil)
	noVer.PhysicalName = "db.nover"
	if err := s.EnsureTable(ctx, noVer); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Insert(ctx, noVer, []storage.Row{{"a", "1"}, {"a", "2"}}); err != nil {
		t.Fatal(err)
	}
	eq(t, scan(t, s, noVer, storage.ScanRequest{}), []storage.Row{{"a", "2"}})
	// Merge (MV output) follows the same version rule.
	if err := s.Merge(ctx, tb, []storage.MergeRow{{Row: storage.Row{"a", uint64(1), "old"}, Source: storage.SourceRef{View: "v", RowID: "x"}}}); err != nil {
		t.Fatal(err)
	}
	eq(t, scan(t, s, tb, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{"a"}}}})[0][2], "two-again")
	if err := s.DeleteRows(ctx, tb, []storage.Row{{"a", nil, nil}}); err != nil {
		t.Fatal(err)
	}
	eq(t, s.ItemCount(tb), 1)
}

func TestSumming(t *testing.T) {
	tb := mkTable(catalog.EngineSummingMergeTree, [][2]string{{"k", "String"}, {"a", "UInt8"}, {"b", "Int64"}, {"f", "Float64"}, {"label", "String"}}, []string{"k"}, nil)
	s := newStore(t, tb)
	ctx := context.Background()
	if _, err := s.Insert(ctx, tb, []storage.Row{{"x", uint64(200), int64(-1), 0.5, "first"}, {"x", uint64(100), int64(-2), 0.25, "second"}}); err != nil {
		t.Fatal(err)
	}
	eq(t, scan(t, s, tb, storage.ScanRequest{}), []storage.Row{{"x", uint64(44), int64(-3), 0.75, "first"}})
	// Merge is exactly once per source.
	mr := []storage.MergeRow{{Row: storage.Row{"x", uint64(1), int64(1), 0.0, "mv"}, Source: storage.SourceRef{View: "v", RowID: "r"}}}
	for i := 0; i < 2; i++ {
		if err := s.Merge(ctx, tb, mr); err != nil {
			t.Fatal(err)
		}
	}
	eq(t, scan(t, s, tb, storage.ScanRequest{})[0][1], uint64(45))

	only := mkTable(catalog.EngineSummingMergeTree, [][2]string{{"k", "String"}, {"a", "UInt64"}, {"b", "UInt64"}}, []string{"k"}, nil)
	only.PhysicalName, only.SumColumns = "db.only", []string{"a"}
	if err := s.EnsureTable(ctx, only); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Insert(ctx, only, []storage.Row{{"x", uint64(1), uint64(5)}, {"x", uint64(2), uint64(6)}}); err != nil {
		t.Fatal(err)
	}
	eq(t, scan(t, s, only, storage.ScanRequest{}), []storage.Row{{"x", uint64(3), uint64(5)}})
}

func state(f string, mut func(p *chtype.AggPartial)) *chtype.AggPartial {
	p := &chtype.AggPartial{Func: f, SetLoaded: true}
	mut(p)
	return p
}

func TestAggregatingMerge(t *testing.T) {
	tb := mkTable(catalog.EngineAggregatingMergeTree, [][2]string{
		{"k", "String"},
		{"s", "SimpleAggregateFunction(sum, UInt64)"},
		{"mx", "SimpleAggregateFunction(max, String)"},
		{"mn", "SimpleAggregateFunction(min, Float64)"},
		{"an", "SimpleAggregateFunction(any, String)"},
		{"al", "SimpleAggregateFunction(anyLast, String)"},
		{"gu", "SimpleAggregateFunction(groupUniqArrayArray, Array(String))"},
		{"c", "AggregateFunction(count)"},
		{"u", "AggregateFunction(uniqExact, String)"},
		{"am", "AggregateFunction(argMax, String, UInt64)"},
		{"av", "AggregateFunction(avg, UInt64)"},
		{"ga", "AggregateFunction(groupArray, String)"},
	}, []string{"k"}, nil)
	s := newStore(t, tb)
	ctx := context.Background()
	row := func(sum uint64, mx string, mn float64, an string, gu []any, members []any, arg string, key uint64) storage.Row {
		return storage.Row{"k1", sum, mx, mn, an, an, gu,
			state("count", func(p *chtype.AggPartial) { p.Count = 1 }),
			state("uniqExact", func(p *chtype.AggPartial) { p.Set = members; p.SetSize = uint64(len(members)) }),
			state("argMax", func(p *chtype.AggPartial) { p.Value, p.Key, p.HasValue = arg, key, true }),
			state("avg", func(p *chtype.AggPartial) { p.Sum, p.Count = float64(sum), 1 }),
			state("groupArray", func(p *chtype.AggPartial) { p.List = []any{arg} }),
		}
	}
	apply := func(r storage.Row, view, rid string, idx int) {
		t.Helper()
		if err := s.Merge(ctx, tb, []storage.MergeRow{{Row: r, Source: storage.SourceRef{View: view, RowID: rid}, Index: idx}}); err != nil {
			t.Fatal(err)
		}
	}
	apply(row(5, "b", 2.5, "first", []any{"x"}, []any{"u1", "u2"}, "early", 10), "db.v", "r1", 0)
	apply(row(7, "z", -1, "second", []any{"y", "x"}, []any{"u2", "u3"}, "late", 20), "db.v", "r2", 0)
	// Redelivery of r1/r2 must not change anything.
	apply(row(5, "b", 2.5, "first", []any{"x"}, []any{"u1", "u2"}, "early", 10), "db.v", "r1", 0)
	apply(row(7, "z", -1, "second", []any{"y", "x"}, []any{"u2", "u3"}, "late", 20), "db.v", "r2", 0)
	// The same row id through another view, or another index, is distinct.
	apply(row(1, "a", 9, "third", nil, []any{"u9"}, "middle", 15), "db.other", "r1", 0)
	apply(row(1, "a", 9, "third", nil, nil, "middle", 15), "db.v", "r1", 1)

	rows := scan(t, s, tb, storage.ScanRequest{})
	if len(rows) != 1 {
		t.Fatalf("expected one merged row, got %d", len(rows))
	}
	r := rows[0]
	eq(t, r[1], uint64(14))
	eq(t, r[2], "z")
	eq(t, r[3], -1.0)
	eq(t, r[4], "first")
	eq(t, r[5], "third")
	gu := append([]any(nil), r[6].([]any)...)
	sort.Slice(gu, func(i, j int) bool { return gu[i].(string) < gu[j].(string) })
	eq(t, gu, []any{"x", "y"})
	eq(t, r[7].(*chtype.AggPartial).Count, uint64(4))
	u := r[8].(*chtype.AggPartial)
	eq(t, u.SetSize, uint64(4))
	eq(t, u.SetLoaded, false)
	members, err := u.LoadSet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(members, func(i, j int) bool { return members[i].(string) < members[j].(string) })
	eq(t, members, []any{"u1", "u2", "u3", "u9"})
	am := r[9].(*chtype.AggPartial)
	eq(t, []any{am.Value, am.Key}, []any{"late", uint64(20)})
	av := r[10].(*chtype.AggPartial)
	eq(t, []any{av.Sum, av.Count}, []any{14.0, uint64(4)})
	eq(t, len(r[11].(*chtype.AggPartial).List), 4)
	if s.MarkerCount() != 4 {
		t.Errorf("expected 4 exactly-once markers, got %d", s.MarkerCount())
	}
	// Direct inserts are merged too but are not idempotent.
	if _, err := s.Insert(ctx, tb, []storage.Row{row(1, "", 0, "", nil, nil, "x", 1), row(1, "", 0, "", nil, nil, "x", 1)}); err != nil {
		t.Fatal(err)
	}
	eq(t, scan(t, s, tb, storage.ScanRequest{})[0][1], uint64(16))
	// A state of the wrong function is rejected.
	bad := row(1, "", 0, "", nil, nil, "x", 1)
	bad[7] = state("sum", func(*chtype.AggPartial) {})
	if err := s.Merge(ctx, tb, []storage.MergeRow{{Row: bad, Source: storage.SourceRef{View: "v", RowID: "bad"}}}); !cherr.Is(err, cherr.TypeMismatch) {
		t.Fatalf("expected TYPE_MISMATCH, got %v", err)
	}
	eq(t, scan(t, s, tb, storage.ScanRequest{})[0][1], uint64(16)) // failed merge left the row untouched
}

func TestEmptySetStateIsLoaded(t *testing.T) {
	tb := mkTable(catalog.EngineAggregatingMergeTree, [][2]string{{"k", "String"}, {"u", "AggregateFunction(uniqExact, String)"}}, []string{"k"}, nil)
	s := newStore(t, tb)
	ctx := context.Background()
	if err := s.Merge(ctx, tb, []storage.MergeRow{{Row: storage.Row{"a", state("uniqExact", func(*chtype.AggPartial) {})}, Source: storage.SourceRef{View: "v", RowID: "1"}}}); err != nil {
		t.Fatal(err)
	}
	p := scan(t, s, tb, storage.ScanRequest{})[0][1].(*chtype.AggPartial)
	eq(t, []any{p.SetSize, p.SetLoaded}, []any{uint64(0), true})
}

func TestSchemaEvolution(t *testing.T) {
	tb := mkTable(catalog.EngineMergeTree, [][2]string{{"k", "String"}, {"a", "UInt8"}}, []string{"k"}, nil)
	s := newStore(t, tb)
	ctx := context.Background()
	if _, err := s.Insert(ctx, tb, []storage.Row{{"x", uint64(1)}}); err != nil {
		t.Fatal(err)
	}
	evolved := *tb
	evolved.Columns = []catalog.Column{tb.Columns[0], {Name: "b", Type: chtype.TString, TypeText: "String"}, tb.Columns[1],
		{Name: "al", Type: chtype.TString, TypeText: "String", DefaultKind: "ALIAS", DefaultExpr: "k"}}
	eq(t, scan(t, s, &evolved, storage.ScanRequest{}), []storage.Row{{"x", "", uint64(1), nil}})
	if _, err := s.Insert(ctx, &evolved, []storage.Row{{"y", "b", uint64(2)}}); !cherr.Is(err, cherr.LogicalError) {
		t.Fatalf("short rows must be rejected, got %v", err)
	}
}
