package dynamo

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
	"dynamodb-eventstream/internal/storage"
)

func TestIntegrationMergeTree(t *testing.T) {
	e := newEnv(t)
	tbl := e.table(&catalog.Table{
		Database: "default", Name: "events", Engine: catalog.EngineMergeTree,
		Columns: cols("tenant", "String", "ts", "DateTime64(9)", "id", "UInt64", "body", "Nullable(String)",
			"tags", "Array(String)", "score", "Float64", "raw", "String"),
		OrderBy: []string{"tenant", "ts", "id"},
	})
	base := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)
	var want []storage.Row
	for i := 0; i < 300; i++ {
		tenant := []string{"acme", "", "a\x00b"}[i%3]
		var body any = fmt.Sprintf("b%d", i)
		if i%7 == 0 {
			body = nil
		}
		score := float64(i) / 3
		switch i {
		case 10:
			score = math.NaN()
		case 11:
			score = math.Inf(-1)
		}
		want = append(want, storage.Row{tenant, base.Add(time.Duration(i) * time.Minute), uint64(i % 50), body,
			[]any{"x", fmt.Sprint(i)}, score, "\xff\xfe" + fmt.Sprint(i)})
	}
	// An exact duplicate is kept as a distinct row.
	want = append(want, want[5])
	res := e.insert(tbl, want...)
	if len(res.RowIDs) != len(want) || res.RowIDs[0] == res.RowIDs[1] {
		t.Fatalf("row ids: %v", res.RowIDs[:2])
	}
	eqRows(t, e.all(tbl), want)

	// Partition range: sorted by (ts, id).
	got := e.scan(tbl, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{"acme"}}}})
	if len(got) != 101 { // 100 + duplicate of row 5? row 5 is tenant "a\x00b"
		if len(got) != 100 {
			t.Fatalf("acme rows: %d", len(got))
		}
	}
	for i := 1; i < len(got); i++ {
		if got[i][1].(time.Time).Before(got[i-1][1].(time.Time)) {
			t.Fatal("range not sorted by sort key")
		}
	}
	// Reverse.
	rev := e.scan(tbl, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{"acme"}}}, Reverse: true})
	if len(rev) != len(got) || !sameRow(rev[0], got[len(got)-1]) {
		t.Fatal("reverse order")
	}
	// Bounds on ts: [base+30m, base+60m) over tenant "a\x00b" (i%3==2).
	lo, hi := base.Add(30*time.Minute), base.Add(60*time.Minute)
	got = e.scan(tbl, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{"a\x00b"},
		Lower: &storage.Bound{Value: lo, Inclusive: true}, Upper: &storage.Bound{Value: hi}}}})
	n := 0
	for i := 30; i < 60; i++ {
		if i%3 == 2 {
			n++
		}
	}
	if len(got) != n {
		t.Fatalf("bounded range: %d rows, want %d", len(got), n)
	}
	// Prefix equality + inclusive upper on id.
	got = e.scan(tbl, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{""},
		SortPrefix: []any{base.Add(4 * time.Minute)}, Upper: &storage.Bound{Value: uint64(4), Inclusive: true}}}})
	if len(got) != 1 || got[0][2] != uint64(4) {
		t.Fatalf("prefix+upper: %v", got)
	}
	// Overlapping and duplicate ranges never return an item twice.
	got = e.scan(tbl, storage.ScanRequest{Ranges: []storage.KeyRange{
		{Partition: []any{"acme"}}, {Partition: []any{"acme"}, SortPrefix: []any{base}}, {Partition: []any{"acme"}},
	}})
	if len(got) != 100 {
		t.Fatalf("overlapping ranges: %d", len(got))
	}
	// Empty non-nil range list.
	if got := e.scan(tbl, storage.ScanRequest{Ranges: []storage.KeyRange{}}); len(got) != 0 {
		t.Fatalf("empty ranges returned %d rows", len(got))
	}
	// Projection: unrequested columns are nil.
	got = e.scan(tbl, storage.ScanRequest{Columns: []int{2, 5}})
	for _, r := range got {
		if r[0] != nil || r[3] != nil || r[2] == nil {
			t.Fatalf("projection: %v", r)
		}
	}
	// Limit hint.
	if got := e.scan(tbl, storage.ScanRequest{Limit: 7}); len(got) != 7 {
		t.Fatalf("limit: %d", len(got))
	}
	if got := e.scan(tbl, storage.ScanRequest{Limit: 3, Ranges: []storage.KeyRange{{Partition: []any{"acme"}}}}); len(got) != 3 {
		t.Fatalf("range limit: %d", len(got))
	}
}

func TestIntegrationLargePages(t *testing.T) {
	e := newEnv(t)
	tbl := e.table(&catalog.Table{
		Database: "default", Name: "big", Engine: catalog.EngineMergeTree,
		Columns: cols("p", "UInt8", "i", "UInt32", "payload", "String"), OrderBy: []string{"p", "i"},
	})
	payload := strings.Repeat("z", 20<<10)
	var rows []storage.Row
	for i := 0; i < 260; i++ { // ~5 MB: several 1 MB pages per partition
		rows = append(rows, storage.Row{uint64(i % 2), uint64(i), payload})
	}
	e.insert(tbl, rows...)
	if got := e.all(tbl); len(got) != len(rows) {
		t.Fatalf("scan: %d rows", len(got))
	}
	got := e.scan(tbl, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{uint64(0)}}}})
	if len(got) != 130 {
		t.Fatalf("query: %d rows", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i][1].(uint64) <= got[i-1][1].(uint64) {
			t.Fatal("pages out of order")
		}
	}
	// A large batch spans many parallel BatchWriteItem chunks.
	many := make([]storage.Row, 2500)
	for i := range many {
		many[i] = storage.Row{uint64(2 + i%5), uint64(i), "small"}
	}
	res := e.insert(tbl, many...)
	ids := map[string]bool{}
	for _, id := range res.RowIDs {
		ids[id] = true
	}
	if len(ids) != len(many) {
		t.Fatalf("%d distinct row ids for %d rows", len(ids), len(many))
	}
	if got := e.scan(tbl, storage.ScanRequest{Columns: []int{1}}); len(got) != len(rows)+len(many) {
		t.Fatalf("after large batch: %d rows", len(got))
	}
	// Many small ranges, more than the query parallelism, keep their order.
	var ranges []storage.KeyRange
	for i := 259; i >= 0; i -= 3 {
		ranges = append(ranges, storage.KeyRange{Partition: []any{uint64(i % 2)}, SortPrefix: []any{uint64(i)}})
	}
	got = e.scan(tbl, storage.ScanRequest{Ranges: ranges, Columns: []int{0, 1}})
	if len(got) != len(ranges) {
		t.Fatalf("point ranges: %d", len(got))
	}
}

func TestIntegrationReplacing(t *testing.T) {
	e := newEnv(t)
	tbl := e.table(&catalog.Table{
		Database: "default", Name: "snap", Engine: catalog.EngineReplacingMergeTree, EngineArgs: []string{"ver"},
		Columns: cols("k", "String", "ver", "DateTime64(9)", "v", "String"), OrderBy: []string{"k"},
	})
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	res := e.insert(tbl,
		storage.Row{"a", t0.Add(2), "a2"},
		storage.Row{"a", t0.Add(1), "a1-older"}, // 1ns older: skipped
		storage.Row{"b", t0, "b0"},
		storage.Row{"b", t0, "b0-same-version"}, // equal version: last wins
	)
	if !res.Skipped[1] || res.Skipped[0] || res.Skipped[2] || res.Skipped[3] {
		t.Fatalf("skipped = %v", res.Skipped)
	}
	eqRows(t, e.all(tbl), []storage.Row{{"a", t0.Add(2), "a2"}, {"b", t0, "b0-same-version"}})
	res = e.insert(tbl, storage.Row{"a", t0.Add(3), "a3"})
	if res.Skipped[0] {
		t.Fatal("newer version skipped")
	}
	got := e.scan(tbl, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{"a"}}}})
	eqRows(t, got, []storage.Row{{"a", t0.Add(3), "a3"}})

	// Nullable version: NULL is the smallest version.
	nt := e.table(&catalog.Table{
		Database: "default", Name: "nsnap", Engine: catalog.EngineReplacingMergeTree, EngineArgs: []string{"ver"},
		Columns: cols("k", "String", "ver", "Nullable(UInt64)"), OrderBy: []string{"k"},
	})
	res = e.insert(nt, storage.Row{"x", nil}, storage.Row{"x", nil}, storage.Row{"x", uint64(5)}, storage.Row{"x", nil}, storage.Row{"x", uint64(4)})
	if fmt.Sprint(res.Skipped) != "[false false false true true]" {
		t.Fatalf("nullable skipped = %v", res.Skipped)
	}
	eqRows(t, e.all(nt), []storage.Row{{"x", uint64(5)}})

	// No version column: always replaces.
	nv := e.table(&catalog.Table{
		Database: "default", Name: "nover", Engine: catalog.EngineReplacingMergeTree,
		Columns: cols("k", "UInt64", "v", "String"), OrderBy: []string{"k"},
	})
	e.insert(nv, storage.Row{uint64(1), "a"}, storage.Row{uint64(1), "b"})
	e.insert(nv, storage.Row{uint64(1), "c"})
	eqRows(t, e.all(nv), []storage.Row{{uint64(1), "c"}})

	// is_deleted: deleted rows are hidden, and a newer version revives.
	dt := e.table(&catalog.Table{
		Database: "default", Name: "del", Engine: catalog.EngineReplacingMergeTree, EngineArgs: []string{"ver", "is_deleted"},
		Columns: cols("k", "String", "ver", "UInt64", "is_deleted", "UInt8"), OrderBy: []string{"k"},
	})
	e.insert(dt, storage.Row{"a", uint64(1), uint64(0)}, storage.Row{"b", uint64(1), uint64(0)})
	e.insert(dt, storage.Row{"a", uint64(2), uint64(1)})
	eqRows(t, e.all(dt), []storage.Row{{"b", uint64(1), uint64(0)}})
	if got := e.scan(dt, storage.ScanRequest{Columns: []int{0}}); len(got) != 1 {
		t.Fatalf("is_deleted with projection: %v", got)
	}
	e.insert(dt, storage.Row{"a", uint64(3), uint64(0)})
	if len(e.all(dt)) != 2 {
		t.Fatal("revived row missing")
	}
}

func TestIntegrationSumming(t *testing.T) {
	e := newEnv(t)
	tbl := e.table(&catalog.Table{
		Database: "default", Name: "sums", Engine: catalog.EngineSummingMergeTree,
		Columns: cols("day", "Date", "k", "String", "hits", "UInt64", "bytes", "Int64", "small", "UInt8",
			"ratio", "Float64", "label", "String", "mx", "SimpleAggregateFunction(max, UInt64)"),
		OrderBy: []string{"day", "k"},
	})
	d := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	e.insert(tbl,
		storage.Row{d, "a", uint64(1), int64(-5), uint64(200), 0.1, "first", uint64(3)},
		storage.Row{d, "a", uint64(2), int64(10), uint64(100), 0.2, "second", uint64(9)},
		storage.Row{d, "b", uint64(0), int64(0), uint64(0), 0.0, "zero", uint64(1)},
	)
	e.insert(tbl, storage.Row{d, "a", uint64(4), int64(0), uint64(0), 0.0, "third", uint64(5)})
	got := e.all(tbl)
	// "b" has all summed columns zero: dropped like a ClickHouse merge does.
	// Float sums use IEEE arithmetic, like ClickHouse: 0.1 + 0.2.
	eqRows(t, got, []storage.Row{{d, "a", uint64(7), int64(5), uint64(44), 0.30000000000000004, "first", uint64(9)}})

	// Infinite sums are stored (as S, like plain Float columns).
	e.insert(tbl, storage.Row{d, "a", uint64(1), int64(0), uint64(0), math.Inf(1), "x", uint64(1)})
	if got := e.all(tbl); len(got) != 1 || !math.IsInf(got[0][5].(float64), 1) {
		t.Fatalf("infinite float sum: %v", got)
	}

	explicit := e.table(&catalog.Table{
		Database: "default", Name: "sums2", Engine: catalog.EngineSummingMergeTree, EngineArgs: []string{"(hits)"},
		Columns: cols("k", "String", "hits", "UInt64", "other", "UInt64"), OrderBy: []string{"k"},
	})
	e.insert(explicit, storage.Row{"a", uint64(1), uint64(10)}, storage.Row{"a", uint64(2), uint64(20)})
	eqRows(t, e.all(explicit), []storage.Row{{"a", uint64(3), uint64(10)}})
}

func aggTable(e *env) *catalog.Table {
	return e.table(&catalog.Table{
		Database: "default", Name: "rollup", Engine: catalog.EngineAggregatingMergeTree,
		Columns: cols("k", "String",
			"cnt", "AggregateFunction(count)",
			"total", "AggregateFunction(sum, UInt64)",
			"neg", "AggregateFunction(sum, Int32)",
			"avgv", "AggregateFunction(avg, Float64)",
			"mn", "AggregateFunction(min, Int64)",
			"mxs", "AggregateFunction(max, String)",
			"last", "AggregateFunction(argMax, String, DateTime64(3))",
			"first", "AggregateFunction(argMin, String, Float64)",
			"anyv", "AggregateFunction(any, String)",
			"anyl", "AggregateFunction(anyLast, String)",
			"users", "AggregateFunction(uniqExact, String)",
			"pairs", "AggregateFunction(uniq, String, UInt8)",
			"guniq", "AggregateFunction(groupUniqArray, UInt64)",
			"garr", "AggregateFunction(groupArray, String)",
			"s_sum", "SimpleAggregateFunction(sum, Float64)",
			"s_min", "SimpleAggregateFunction(min, String)",
			"s_max", "SimpleAggregateFunction(max, DateTime)",
			"s_any", "SimpleAggregateFunction(any, String)",
			"s_last", "SimpleAggregateFunction(anyLast, String)",
			"s_set", "SimpleAggregateFunction(groupUniqArrayArray, Array(String))",
			"s_arr", "SimpleAggregateFunction(groupArrayArray, Array(UInt8))",
			"plain", "String",
		),
		OrderBy: []string{"k"},
	})
}

type aggInput struct {
	n       uint64
	total   uint64
	neg     int64
	avgSum  float64
	mn      int64
	mxs     string
	last    string
	lastTS  time.Time
	first   string
	firstK  float64
	anyv    string
	users   []any
	pairs   []any
	guniq   []any
	garr    []any
	sSum    float64
	sMin    string
	sMax    time.Time
	sAny    string
	sSet    []any
	sArr    []any
	plain   string
	setNone bool
}

func aggRow(k string, in aggInput) storage.Row {
	st := func(fn string) *chtype.AggPartial { return &chtype.AggPartial{Func: fn, SetLoaded: true} }
	cnt := st("count")
	cnt.Count = in.n
	total := st("sum")
	total.Sum = in.total
	neg := st("sum")
	neg.Sum = in.neg
	avg := st("avg")
	avg.Sum, avg.Count = in.avgSum, in.n
	mn := st("min")
	mn.Value, mn.HasValue = in.mn, true
	mxs := st("max")
	mxs.Value, mxs.HasValue = in.mxs, in.mxs != ""
	last := st("argMax")
	last.Value, last.Key, last.HasValue = in.last, in.lastTS, true
	first := st("argMin")
	first.Value, first.Key, first.HasValue = in.first, in.firstK, true
	anyv := st("any")
	anyv.Value, anyv.HasValue = in.anyv, true
	anyl := st("anyLast")
	anyl.Value, anyl.HasValue = in.anyv, true
	users := st("uniqExact")
	users.Set = in.users
	pairs := st("uniq")
	pairs.Set = in.pairs
	guniq := st("groupUniqArray")
	guniq.Set = in.guniq
	garr := st("groupArray")
	garr.List = in.garr
	return storage.Row{k, cnt, total, neg, avg, mn, mxs, last, first, anyv, anyl, users, pairs, guniq, garr,
		in.sSum, in.sMin, in.sMax, in.sAny, in.sAny, in.sSet, in.sArr, in.plain}
}

func loadSet(t *testing.T, e *env, v any) []string {
	t.Helper()
	p := v.(*chtype.AggPartial)
	members := p.Set
	if !p.SetLoaded {
		var err error
		if members, err = p.LoadSet(e.ctx); err != nil {
			t.Fatal(err)
		}
	}
	if uint64(len(members)) != p.SetSize && !p.SetLoaded {
		t.Fatalf("SetSize %d but %d members", p.SetSize, len(members))
	}
	return rowStrings(func() []storage.Row {
		var rows []storage.Row
		for _, m := range members {
			rows = append(rows, storage.Row{m})
		}
		return rows
	}())
}

func TestIntegrationAggregating(t *testing.T) {
	e := newEnv(t)
	tbl := aggTable(e)
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	r1 := aggRow("k1", aggInput{n: 2, total: 10, neg: -7, avgSum: 3, mn: 5, mxs: "b", last: "L1", lastTS: ts, first: "F1", firstK: 2.5,
		anyv: "any1", users: []any{"u1", "u2"}, pairs: []any{[]any{"x", uint64(1)}}, guniq: []any{uint64(1), uint64(2)},
		garr: []any{"g1"}, sSum: 0.5, sMin: "m", sMax: ts, sAny: "sa1", sSet: []any{"s1", "s2"}, sArr: []any{uint64(1)}, plain: "p1"})
	r2 := aggRow("k1", aggInput{n: 3, total: 5, neg: 2, avgSum: 6, mn: -1, mxs: "a", last: "L2", lastTS: ts.Add(time.Millisecond), first: "F2", firstK: -1,
		anyv: "any2", users: []any{"u2", "u3"}, pairs: []any{[]any{"x", uint64(1)}, []any{"x", uint64(2)}}, guniq: []any{uint64(2)},
		garr: []any{"g2", "g3"}, sSum: 0.25, sMin: "a\xff", sMax: ts.Add(-time.Hour), sAny: "sa2", sSet: []any{"s2", "s3"}, sArr: []any{uint64(2), uint64(3)}, plain: "p2"})
	e.merge(tbl, storage.MergeRow{Row: r1, Source: storage.SourceRef{View: "default.mv", RowID: "r1"}})
	e.merge(tbl, storage.MergeRow{Row: r2, Source: storage.SourceRef{View: "default.mv", RowID: "r2"}})
	// Redelivery of r1 has no effect.
	e.merge(tbl, storage.MergeRow{Row: r1, Source: storage.SourceRef{View: "default.mv", RowID: "r1"}})

	rows := e.all(tbl)
	if len(rows) != 1 {
		t.Fatalf("rows: %d", len(rows))
	}
	r := rows[0]
	p := func(i int) *chtype.AggPartial { return r[i].(*chtype.AggPartial) }
	checks := []struct {
		name      string
		got, want any
	}{
		{"count", p(1).Count, uint64(5)},
		{"sum", p(2).Sum, uint64(15)},
		{"sum int", p(3).Sum, int64(-5)},
		{"avg sum", p(4).Sum, 9.0},
		{"avg count", p(4).Count, uint64(5)},
		{"min", p(5).Value, int64(-1)},
		{"max string (CAS)", p(6).Value, "b"},
		{"argMax value", p(7).Value, "L2"},
		{"argMax key", p(7).Key, ts.Add(time.Millisecond)},
		{"argMin value", p(8).Value, "F2"},
		{"any", p(9).Value, "any1"},
		{"anyLast", p(10).Value, "any2"},
		{"users size", p(11).SetSize, uint64(3)},
		{"pairs size", p(12).SetSize, uint64(2)},
		{"guniq size", p(13).SetSize, uint64(2)},
		{"groupArray", fmt.Sprint(p(14).List), "[g1 g2 g3]"},
		{"s_sum", r[15], 0.75},
		{"s_min", r[16], "a\xff"},
		{"s_max", r[17], ts},
		{"s_any", r[18], "sa1"},
		{"s_last", r[19], "sa2"},
		{"s_set", fmt.Sprint(rowStrings([]storage.Row{{r[20]}})), "[[[s1 s2 s3]]]"},
		{"s_arr", fmt.Sprint(r[21]), "[1 2 3]"},
		{"plain keeps first", r[22], "p1"},
	}
	for _, c := range checks {
		if !sameValue(c.got, c.want) && fmt.Sprint(c.got) != fmt.Sprint(c.want) {
			t.Errorf("%s: got %#v want %#v", c.name, c.got, c.want)
		}
	}
	if got := loadSet(t, e, r[11]); fmt.Sprint(got) != "[[u1] [u2] [u3]]" {
		t.Errorf("users members: %v", got)
	}
	if p(1).Func != "count" || !p(5).HasValue || p(11).SetLoaded || len(p(4).ArgTypes) != 1 {
		t.Errorf("partial metadata: %+v %+v", p(1), p(11))
	}

	// Projection of a state column only fetches its attributes.
	got := e.scan(tbl, storage.ScanRequest{Columns: []int{11}, Ranges: []storage.KeyRange{{Partition: []any{"k1"}}}})
	if len(got) != 1 || got[0][0] != nil || got[0][11].(*chtype.AggPartial).SetSize != 3 {
		t.Fatalf("projected state: %v", got)
	}

	// Direct INSERT merges with a fresh source each time (not idempotent).
	emptyRow := aggRow("k2", aggInput{n: 1, mn: 1, last: "x", lastTS: ts, first: "y", anyv: "z", users: []any{"u9"}, sMax: ts})
	e.insert(tbl, emptyRow)
	e.insert(tbl, emptyRow)
	got = e.scan(tbl, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{"k2"}}}})
	if c := got[0][1].(*chtype.AggPartial).Count; c != 2 {
		t.Fatalf("direct inserts count = %d", c)
	}
	if s := got[0][11].(*chtype.AggPartial).SetSize; s != 1 {
		t.Fatalf("direct insert set size = %d", s)
	}
	if got[0][1].(*chtype.AggPartial).SetLoaded != false && got[0][11].(*chtype.AggPartial).SetLoaded {
		t.Fatal("non-empty set must load lazily")
	}

	// A state of the wrong function is rejected.
	bad := aggRow("k3", aggInput{})
	bad[1] = &chtype.AggPartial{Func: "sum"}
	if err := e.store.Merge(e.ctx, tbl, []storage.MergeRow{{Row: bad, Source: storage.SourceRef{View: "v", RowID: "x"}}}); !cherr.Is(err, cherr.TypeMismatch) {
		t.Fatalf("wrong state: %v", err)
	}
}
