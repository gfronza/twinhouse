package dynamo

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/chtype"
	"dynamodb-eventstream/internal/storage"
)

func uniqTable(e *env, name string) *catalog.Table {
	return e.table(&catalog.Table{
		Database: "default", Name: name, Engine: catalog.EngineAggregatingMergeTree,
		Columns: cols("k", "String", "cnt", "AggregateFunction(count)", "users", "AggregateFunction(uniqExact, String)",
			"mx", "AggregateFunction(max, UInt64)", "last", "AggregateFunction(argMax, String, UInt64)"),
		OrderBy: []string{"k"},
	})
}

func uniqRow(k string, n uint64, users []any, mx uint64) storage.Row {
	return storage.Row{k,
		&chtype.AggPartial{Func: "count", Count: n},
		&chtype.AggPartial{Func: "uniqExact", Set: users, SetLoaded: true},
		&chtype.AggPartial{Func: "max", Value: mx, HasValue: true},
		&chtype.AggPartial{Func: "argMax", Value: fmt.Sprint("v", mx), Key: mx, HasValue: true},
	}
}

func readState(t *testing.T, e *env, tbl *catalog.Table, k string) (count, size uint64, members []string, mx any, last any) {
	t.Helper()
	rows := e.scan(tbl, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{k}}}})
	if len(rows) != 1 {
		t.Fatalf("%d rows for %s", len(rows), k)
	}
	r := rows[0]
	users := r[2].(*chtype.AggPartial)
	return r[1].(*chtype.AggPartial).Count, users.SetSize, loadSet(t, e, users), r[3].(*chtype.AggPartial).Value, r[4].(*chtype.AggPartial).Value
}

func TestIntegrationMergeSameSourceConcurrently(t *testing.T) {
	e := newEnv(t)
	tbl := uniqTable(e, "once")
	src := storage.SourceRef{View: "default.mv", RowID: "row-1"}
	row := uniqRow("k", 1, []any{"a", "b", "c"}, 7)
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- e.store.Merge(e.ctx, tbl, []storage.MergeRow{{Row: row, Source: src}})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	count, size, members, mx, last := readState(t, e, tbl, "k")
	if count != 1 || size != 3 || len(members) != 3 || mx != uint64(7) || last != "v7" {
		t.Fatalf("count=%d size=%d members=%v mx=%v last=%v", count, size, members, mx, last)
	}
	// One more Index of the same source is a different application.
	e.merge(tbl, storage.MergeRow{Row: row, Source: src, Index: 1})
	if count, _, _, _, _ := readState(t, e, tbl, "k"); count != 2 {
		t.Fatalf("index 1 not applied: %d", count)
	}
}

func TestIntegrationMergeDistinctSourcesConcurrently(t *testing.T) {
	e := newEnv(t)
	tbl := uniqTable(e, "many")
	const goroutines, perG = 20, 10
	var wg sync.WaitGroup
	var failed atomic.Value
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				// Overlapping members across goroutines: user ids 0..49.
				users := []any{fmt.Sprint("u", (g*perG+i)%50), fmt.Sprint("u", (g+i)%50)}
				row := uniqRow("hot", 1, users, uint64(g*perG+i))
				src := storage.SourceRef{View: "default.mv", RowID: fmt.Sprintf("g%d-%d", g, i)}
				if err := e.store.Merge(e.ctx, tbl, []storage.MergeRow{{Row: row, Source: src}}); err != nil {
					failed.Store(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if err, _ := failed.Load().(error); err != nil {
		t.Fatal(err)
	}
	count, size, members, mx, last := readState(t, e, tbl, "hot")
	if count != goroutines*perG || size != 50 || len(members) != 50 {
		t.Fatalf("count=%d size=%d members=%d", count, size, len(members))
	}
	if mx != uint64(goroutines*perG-1) || last != fmt.Sprint("v", goroutines*perG-1) {
		t.Fatalf("max=%v argMax=%v", mx, last)
	}
	if n := e.itemCount(membersTable(tbl.PhysicalName)); n != 50 {
		t.Fatalf("member items: %d", n)
	}
}

func TestIntegrationLargeUniqExact(t *testing.T) {
	e := newEnv(t)
	tbl := uniqTable(e, "big")
	users := make([]any, 5000)
	for i := range users {
		users[i] = fmt.Sprintf("user-%05d", i)
	}
	e.merge(tbl, storage.MergeRow{Row: uniqRow("k", 1, users, 1), Source: storage.SourceRef{View: "v", RowID: "a"}})
	_, size, members, _, _ := readState(t, e, tbl, "k")
	if size != 5000 || len(members) != 5000 {
		t.Fatalf("size=%d members=%d", size, len(members))
	}
	// 2500 known + 2500 new members, plus in-state duplicates.
	more := append(append([]any{}, users[2500:]...), users[:10]...)
	for i := 5000; i < 7500; i++ {
		more = append(more, fmt.Sprintf("user-%05d", i), fmt.Sprintf("user-%05d", i))
	}
	e.merge(tbl, storage.MergeRow{Row: uniqRow("k", 1, more, 1), Source: storage.SourceRef{View: "v", RowID: "b"}})
	_, size, members, _, _ = readState(t, e, tbl, "k")
	if size != 7500 || len(members) != 7500 {
		t.Fatalf("after second merge: size=%d members=%d", size, len(members))
	}
	// Members longer than the sort-key limit are hashed but kept intact.
	long := strings.Repeat("x", 3000)
	e.merge(tbl, storage.MergeRow{Row: uniqRow("long", 1, []any{long, long + "y"}, 1), Source: storage.SourceRef{View: "v", RowID: "c"}})
	e.merge(tbl, storage.MergeRow{Row: uniqRow("long", 1, []any{long}, 1), Source: storage.SourceRef{View: "v", RowID: "d"}})
	_, size, members, _, _ = readState(t, e, tbl, "long")
	if size != 2 || len(members) != 2 || !strings.Contains(members[0]+members[1], long+"y") {
		t.Fatalf("long members: size=%d", size)
	}
}

func TestIntegrationMergeIntoNonAggregatingTargets(t *testing.T) {
	e := newEnv(t)
	mt := e.table(&catalog.Table{
		Database: "default", Name: "mvout", Engine: catalog.EngineMergeTree,
		Columns: cols("k", "String", "v", "UInt64"), OrderBy: []string{"k"},
	})
	src := storage.SourceRef{View: "default.mv", RowID: "r1"}
	e.merge(mt, storage.MergeRow{Row: storage.Row{"a", uint64(1)}, Source: src},
		storage.MergeRow{Row: storage.Row{"a", uint64(2)}, Source: src, Index: 1})
	e.merge(mt, storage.MergeRow{Row: storage.Row{"a", uint64(1)}, Source: src}) // redelivery
	eqRows(t, e.all(mt), []storage.Row{{"a", uint64(1)}, {"a", uint64(2)}})

	rt := e.table(&catalog.Table{
		Database: "default", Name: "mvsnap", Engine: catalog.EngineReplacingMergeTree, EngineArgs: []string{"v"},
		Columns: cols("k", "String", "v", "UInt64"), OrderBy: []string{"k"},
	})
	e.merge(rt, storage.MergeRow{Row: storage.Row{"a", uint64(5)}, Source: src})
	e.merge(rt, storage.MergeRow{Row: storage.Row{"a", uint64(3)}, Source: storage.SourceRef{View: "default.mv", RowID: "r2"}})
	eqRows(t, e.all(rt), []storage.Row{{"a", uint64(5)}})

	// MV output is flagged so the stream worker does not re-trigger views.
	raw, err := e.clients.DynamoDB.Scan(e.ctx, &dynamodb.ScanInput{TableName: &rt.PhysicalName})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := e.store.DecodeRecord(rt, "INSERT", raw.Items[0])
	if err != nil || !rec.IsMVOutput || rec.Origin != "us-east-1" || rec.RowID == "" {
		t.Fatalf("decoded record: %+v %v", rec, err)
	}
}

func TestIntegrationTruncateAndDelete(t *testing.T) {
	e := newEnv(t)
	tbl := uniqTable(e, "tr")
	src := storage.SourceRef{View: "v", RowID: "r"}
	e.merge(tbl, storage.MergeRow{Row: uniqRow("a", 1, []any{"x", "y"}, 1), Source: src},
		storage.MergeRow{Row: uniqRow("b", 1, []any{"z"}, 1), Source: src})
	if err := e.store.TruncateTable(e.ctx, tbl); err != nil {
		t.Fatal(err)
	}
	if n := len(e.all(tbl)); n != 0 {
		t.Fatalf("%d rows after truncate", n)
	}
	if n := e.itemCount(membersTable(tbl.PhysicalName)); n != 0 {
		t.Fatalf("%d members after truncate", n)
	}
	// Markers are tagged with the reset epoch: re-applying the same source
	// works (POPULATE).
	e.merge(tbl, storage.MergeRow{Row: uniqRow("a", 1, []any{"x"}, 1), Source: src})
	count, size, _, _, _ := readState(t, e, tbl, "a")
	if count != 1 || size != 1 {
		t.Fatalf("after truncate+merge: count=%d size=%d", count, size)
	}

	// DeleteRows on an aggregating table removes the row and its members.
	rows := e.scan(tbl, storage.ScanRequest{})
	if err := e.store.DeleteRows(e.ctx, tbl, rows); err != nil {
		t.Fatal(err)
	}
	if len(e.all(tbl)) != 0 || e.itemCount(membersTable(tbl.PhysicalName)) != 0 {
		t.Fatal("delete left items")
	}

	// MergeTree: identical rows are deleted together, others kept.
	mt := e.table(&catalog.Table{
		Database: "default", Name: "ev", Engine: catalog.EngineMergeTree,
		Columns: cols("k", "String", "ts", "DateTime", "v", "Nullable(String)"), OrderBy: []string{"k", "ts"},
	})
	t0 := time.Unix(1700000000, 0).UTC()
	e.insert(mt, storage.Row{"a", t0, "x"}, storage.Row{"a", t0, "x"}, storage.Row{"a", t0, nil},
		storage.Row{"a", t0.Add(time.Second), "x"}, storage.Row{"b", t0, "x"})
	if err := e.store.DeleteRows(e.ctx, mt, []storage.Row{{"a", t0, "x"}}); err != nil {
		t.Fatal(err)
	}
	eqRows(t, e.all(mt), []storage.Row{{"a", t0, nil}, {"a", t0.Add(time.Second), "x"}, {"b", t0, "x"}})
	// Constant partition, empty sort key.
	ct := e.table(&catalog.Table{Database: "default", Name: "log", Engine: catalog.EngineLog, Columns: cols("v", "String")})
	e.insert(ct, storage.Row{"1"}, storage.Row{"2"}, storage.Row{"1"})
	if err := e.store.DeleteRows(e.ctx, ct, []storage.Row{{"1"}}); err != nil {
		t.Fatal(err)
	}
	eqRows(t, e.all(ct), []storage.Row{{"2"}})
	if err := e.store.TruncateTable(e.ctx, ct); err != nil {
		t.Fatal(err)
	}
	if len(e.all(ct)) != 0 {
		t.Fatal("truncate log table")
	}
	// Replacing: delete by key.
	rt := e.table(&catalog.Table{Database: "default", Name: "r", Engine: catalog.EngineReplacingMergeTree,
		Columns: cols("k", "UInt64", "v", "String"), OrderBy: []string{"k"}})
	e.insert(rt, storage.Row{uint64(1), "a"}, storage.Row{uint64(2), "b"})
	if err := e.store.DeleteRows(e.ctx, rt, []storage.Row{{uint64(2), "b"}}); err != nil {
		t.Fatal(err)
	}
	eqRows(t, e.all(rt), []storage.Row{{uint64(1), "a"}})
}

// countingAPI counts calls and records replica updates (which DynamoDB Local
// does not support).
type countingAPI struct {
	ddbAPI
	queries, scans atomic.Int64
	mu             sync.Mutex
	replicaCalls   []*dynamodb.UpdateTableInput
}

func (c *countingAPI) Query(ctx context.Context, in *dynamodb.QueryInput, o ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	c.queries.Add(1)
	return c.ddbAPI.Query(ctx, in, o...)
}

func (c *countingAPI) Scan(ctx context.Context, in *dynamodb.ScanInput, o ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	c.scans.Add(1)
	return c.ddbAPI.Scan(ctx, in, o...)
}

func (c *countingAPI) UpdateTable(ctx context.Context, in *dynamodb.UpdateTableInput, o ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error) {
	if len(in.ReplicaUpdates) > 0 {
		c.mu.Lock()
		c.replicaCalls = append(c.replicaCalls, in)
		c.mu.Unlock()
		return &dynamodb.UpdateTableOutput{}, nil
	}
	return c.ddbAPI.UpdateTable(ctx, in, o...)
}

func TestIntegrationPaginationCalls(t *testing.T) {
	var api *countingAPI
	e := newEnvWith(t, func(o *Options) { o.ScanSegments = 2 }, func(d ddbAPI) ddbAPI { api = &countingAPI{ddbAPI: d}; return api })
	tbl := e.table(&catalog.Table{
		Database: "default", Name: "pages", Engine: catalog.EngineMergeTree,
		Columns: cols("p", "UInt8", "payload", "String"), OrderBy: []string{"p"},
	})
	payload := strings.Repeat("q", 50<<10)
	rows := make([]storage.Row, 100) // 5 MB in one partition
	for i := range rows {
		rows[i] = storage.Row{uint64(0), payload}
	}
	e.insert(tbl, rows...)
	api.queries.Store(0)
	if got := e.scan(tbl, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{uint64(0)}}}}); len(got) != 100 {
		t.Fatalf("query rows %d", len(got))
	}
	if q := api.queries.Load(); q < 5 {
		t.Fatalf("expected >= 5 query pages for 5 MB, got %d", q)
	}
	api.scans.Store(0)
	if got := e.all(tbl); len(got) != 100 {
		t.Fatalf("scan rows %d", len(got))
	}
	if s := api.scans.Load(); s < 5 {
		t.Fatalf("expected >= 5 scan pages, got %d", s)
	}
	// Closing early stops the readers.
	it, err := e.store.Scan(e.ctx, tbl, storage.ScanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := it.Next(e.ctx); err != nil {
		t.Fatal(err)
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := it.Next(e.ctx); err == nil {
		t.Fatal("Next after Close must end")
	}
}

func TestIntegrationGlobalAWSReplicas(t *testing.T) {
	var api *countingAPI
	e := newEnvWith(t, func(o *Options) {
		o.GlobalMode = "aws"
		o.ReplicaRegions = []string{"eu-west-1", "ap-south-1"}
		o.HomeRegion = "us-east-1" // this region creates the global tables
	}, func(d ddbAPI) ddbAPI { api = &countingAPI{ddbAPI: d}; return api })
	// EnsureSystemTables ran in newEnv: only __catalog is global.
	got := map[string][]string{}
	collect := func() {
		api.mu.Lock()
		defer api.mu.Unlock()
		for _, in := range api.replicaCalls {
			got[*in.TableName] = append(got[*in.TableName], *in.ReplicaUpdates[0].Create.RegionName)
		}
		api.replicaCalls = nil
	}
	collect()
	if fmt.Sprint(got) != fmt.Sprintf("map[%s__catalog:[eu-west-1 ap-south-1]]", e.prefix) {
		t.Fatalf("system replicas: %v", got)
	}
	got = map[string][]string{}
	g := e.table(&catalog.Table{Database: "default", Name: "g", Engine: catalog.EngineMergeTree, Columns: cols("k", "String"), OrderBy: []string{"k"}})
	uniqTable(e, "regional")
	collect()
	if fmt.Sprint(got) != fmt.Sprintf("map[%s:[eu-west-1 ap-south-1]]", g.PhysicalName) {
		t.Fatalf("table replicas (regional tables must have none): %v", got)
	}
}

// Extremum states and SimpleAggregateFunction(min|max) columns on Enum
// values keep the extremum by the enum's numeric value, not its name (the
// compare-and-swap path; 'b' = 1 < 'a' = 2).
func TestIntegrationMergeEnumExtremum(t *testing.T) {
	e := newEnv(t)
	const enum = "Enum8('b' = 1, 'a' = 2)"
	tbl := e.table(&catalog.Table{
		Database: "default", Name: "enum_ext", Engine: catalog.EngineAggregatingMergeTree,
		Columns: cols("k", "String", "mx", "AggregateFunction(max, "+enum+")", "mn", "AggregateFunction(min, "+enum+")",
			"amx", "AggregateFunction(argMax, String, "+enum+")",
			"smx", "SimpleAggregateFunction(max, "+enum+")", "smn", "SimpleAggregateFunction(min, "+enum+")"),
		OrderBy: []string{"k"},
	})
	row := func(v string) storage.Row {
		return storage.Row{"k",
			&chtype.AggPartial{Func: "max", Value: v, HasValue: true},
			&chtype.AggPartial{Func: "min", Value: v, HasValue: true},
			&chtype.AggPartial{Func: "argMax", Value: "at-" + v, Key: v, HasValue: true},
			v, v,
		}
	}
	for i, v := range []string{"a", "b", "a", "b"} {
		e.merge(tbl, storage.MergeRow{Row: row(v), Source: storage.SourceRef{View: "default.mv", RowID: fmt.Sprint("r", i)}})
	}
	rows := e.scan(tbl, storage.ScanRequest{})
	if len(rows) != 1 {
		t.Fatalf("%d rows", len(rows))
	}
	r := rows[0]
	got := []any{r[1].(*chtype.AggPartial).Value, r[2].(*chtype.AggPartial).Value, r[3].(*chtype.AggPartial).Value, r[4], r[5]}
	want := []any{"a", "b", "at-a", "a", "b"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("max, min, argMax, smax, smin = %v, want %v", got, want)
	}
}
