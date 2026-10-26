package dynamo

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/storage"
)

// callCounter counts DynamoDB calls by operation and can fail the next
// extremum UpdateItem calls (a crash between step 1 and steps 2/3).
type callCounter struct {
	ddbAPI
	mu          sync.Mutex
	calls       map[string]int
	failUpdates atomic.Int32
}

func (c *callCounter) count(op string) {
	c.mu.Lock()
	c.calls[op]++
	c.mu.Unlock()
}

func (c *callCounter) reset() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.calls
	c.calls = map[string]int{}
	return out
}

func (c *callCounter) TransactWriteItems(ctx context.Context, in *dynamodb.TransactWriteItemsInput, o ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	c.count("TransactWriteItems")
	return c.ddbAPI.TransactWriteItems(ctx, in, o...)
}

func (c *callCounter) UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, o ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	c.count("UpdateItem")
	if c.failUpdates.Add(-1) >= 0 {
		return nil, errors.New("injected failure")
	}
	return c.ddbAPI.UpdateItem(ctx, in, o...)
}

func (c *callCounter) GetItem(ctx context.Context, in *dynamodb.GetItemInput, o ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	c.count("GetItem")
	return c.ddbAPI.GetItem(ctx, in, o...)
}

func (c *callCounter) BatchGetItem(ctx context.Context, in *dynamodb.BatchGetItemInput, o ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	c.count("BatchGetItem")
	return c.ddbAPI.BatchGetItem(ctx, in, o...)
}

func (c *callCounter) BatchWriteItem(ctx context.Context, in *dynamodb.BatchWriteItemInput, o ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	c.count("BatchWriteItem")
	return c.ddbAPI.BatchWriteItem(ctx, in, o...)
}

func newCountingEnv(t *testing.T) (*env, *callCounter) {
	var cc *callCounter
	e := newEnvWith(t, func(*Options) {}, func(api ddbAPI) ddbAPI {
		cc = &callCounter{ddbAPI: api, calls: map[string]int{}}
		return cc
	})
	cc.failUpdates.Store(-1 << 30)
	return e, cc
}

func hotTable(e *env, name string) *catalog.Table {
	return e.table(&catalog.Table{
		Database: "default", Name: name, Engine: catalog.EngineAggregatingMergeTree,
		Columns: cols("k", "String", "c", "AggregateFunction(count)", "u", "AggregateFunction(uniqExact, String)",
			"m", "AggregateFunction(max, UInt64)", "g", "AggregateFunction(groupArray, UInt64)", "s", "AggregateFunction(sum, Float64)"),
		OrderBy: []string{"k"},
	})
}

func hotRows(from, to int) []storage.MergeRow {
	var out []storage.MergeRow
	for i := from; i < to; i++ {
		out = append(out, storage.MergeRow{
			Row: storage.Row{"day",
				&chtype.AggPartial{Func: "count", Count: 1},
				&chtype.AggPartial{Func: "uniqExact", Set: []any{fmt.Sprint("u", i%200)}, SetLoaded: true},
				&chtype.AggPartial{Func: "max", Value: uint64(i), HasValue: true},
				&chtype.AggPartial{Func: "groupArray", List: []any{uint64(i)}},
				&chtype.AggPartial{Func: "sum", Sum: 0.5, HasValue: true},
			},
			Source: storage.SourceRef{View: "default.mv", RowID: fmt.Sprint("src-", i)},
		})
	}
	return out
}

func checkHot(t *testing.T, e *env, tbl *catalog.Table, n int) {
	t.Helper()
	rows := e.all(tbl)
	if len(rows) != 1 {
		t.Fatalf("%d rows", len(rows))
	}
	r := rows[0]
	set := r[2].(*chtype.AggPartial)
	list := r[4].(*chtype.AggPartial).List
	if c := r[1].(*chtype.AggPartial).Count; c != uint64(n) {
		t.Fatalf("count %d, want %d", c, n)
	}
	if set.SetSize != uint64(min(n, 200)) {
		t.Fatalf("set size %d", set.SetSize)
	}
	if m := r[3].(*chtype.AggPartial).Value; m != uint64(n-1) {
		t.Fatalf("max %v", m)
	}
	if s := r[5].(*chtype.AggPartial).Sum; s != float64(n)/2 {
		t.Fatalf("sum %v", s)
	}
	if len(list) != n {
		t.Fatalf("groupArray has %d elements", len(list))
	}
	for i, v := range list {
		if v != uint64(i) {
			t.Fatalf("groupArray[%d] = %v: order lost", i, v)
		}
	}
}

// TestIntegrationMergeGroupsHotKey: rows of one key are pre-aggregated:
// one marker transaction per 99 rows (not two transactions per row), one
// extremum write and one member pass per group; a partially applied group
// only adds the new rows; a full replay (the stream worker after inline
// application) skips every row through the done markers.
func TestIntegrationMergeGroupsHotKey(t *testing.T) {
	e, cc := newCountingEnv(t)
	tbl := hotTable(e, "hot")
	cc.reset()
	e.merge(tbl, hotRows(0, 500)...)
	calls := cc.reset()
	checkHot(t, e, tbl, 500)
	// 6 marker transactions + 4 member transactions (200 members, 50 per
	// transaction); one extremum update; one CAS read of the float sum.
	if calls["TransactWriteItems"] != 6+4 || calls["UpdateItem"] != 1 || calls["GetItem"] > 2 {
		t.Fatalf("calls for 500 rows on one key: %v", calls)
	}

	// Rows 0..499 were applied: only 500..599 are added.
	e.merge(tbl, hotRows(0, 600)...)
	checkHot(t, e, tbl, 600)
	cc.reset()

	// Full replay: every marker is done, nothing but the cancelled marker
	// transactions (one per chunk) and the CAS read.
	e.merge(tbl, hotRows(0, 600)...)
	calls = cc.reset()
	checkHot(t, e, tbl, 600)
	if calls["TransactWriteItems"] != 7 || calls["UpdateItem"] != 0 || calls["BatchGetItem"] != 0 || calls["BatchWriteItem"] != 0 {
		t.Fatalf("replay calls: %v", calls)
	}

	// The same application twice in one call counts once.
	dup := hotRows(600, 601)
	e.merge(tbl, append(dup, dup...)...)
	checkHot(t, e, tbl, 601)
}

// TestIntegrationMergeFinishesAfterCrash: a failure between step 1 and
// steps 2/3 leaves the marker not done; the re-application (stream
// worker) runs steps 2/3 without counting the row again.
func TestIntegrationMergeFinishesAfterCrash(t *testing.T) {
	e, cc := newCountingEnv(t)
	tbl := hotTable(e, "crash")
	cc.failUpdates.Store(1)
	if err := e.store.Merge(e.ctx, tbl, hotRows(0, 10)); err == nil {
		t.Fatal("injected failure not reported")
	}
	cc.failUpdates.Store(-1 << 30)
	rows := e.all(tbl)
	if c := rows[0][1].(*chtype.AggPartial).Count; c != 10 || rows[0][3].(*chtype.AggPartial).HasValue {
		t.Fatalf("after the failure: count %d, max %+v", c, rows[0][3])
	}
	e.merge(tbl, hotRows(0, 10)...)
	checkHot(t, e, tbl, 10)
	cc.reset()
	e.merge(tbl, hotRows(0, 10)...)
	if calls := cc.reset(); calls["UpdateItem"] != 0 || calls["BatchGetItem"] != 0 {
		t.Fatalf("replay after finishing: %v", calls)
	}
	checkHot(t, e, tbl, 10)
}

// TestIntegrationFloatSums: float sums are added in IEEE arithmetic by
// compare-and-swap. DynamoDB ADD sums decimals exactly and rejects sums
// needing more than 38 significant digits (1500000.25 + 5.55e-17), which
// would drop the whole row application (count and avg included) and keep
// failing for the key.
func TestIntegrationFloatSums(t *testing.T) {
	e := newEnv(t)
	agg := e.table(&catalog.Table{
		Database: "default", Name: "fagg", Engine: catalog.EngineAggregatingMergeTree,
		Columns: cols("k", "String", "s", "AggregateFunction(sum, Float64)", "a", "AggregateFunction(avg, Float64)",
			"c", "AggregateFunction(count)", "f", "SimpleAggregateFunction(sum, Float64)", "ai", "AggregateFunction(avg, UInt64)"),
		OrderBy: []string{"k"},
	})
	noise := 0.1
	noise = noise + 0.2 - 0.3 // 5.551115123125783e-17
	row := func(k string, v float64, id string) storage.MergeRow {
		return storage.MergeRow{Row: storage.Row{k,
			&chtype.AggPartial{Func: "sum", Sum: v, HasValue: true},
			&chtype.AggPartial{Func: "avg", Sum: v, Count: 1},
			&chtype.AggPartial{Func: "count", Count: 1},
			v,
			&chtype.AggPartial{Func: "avg", Sum: float64(3), Count: 1},
		}, Source: storage.SourceRef{View: "default.mv", RowID: id}}
	}
	vals := []float64{1500000.25, noise, 1e-20, 1}
	for i, v := range vals { // separate merges: separate step-1 writes
		e.merge(agg, row("x", v, fmt.Sprint("x", i)))
	}
	// Sticky order: the noise first, then large values.
	sticky := []float64{noise, 1500000.25, 3, 1e6}
	e.merge(agg, row("z", sticky[0], "z0"))
	e.merge(agg, row("z", sticky[1], "z1"), row("z", sticky[2], "z2"), row("z", sticky[3], "z3"))
	for _, c := range []struct {
		k    string
		vals []float64
	}{{"x", vals}, {"z", sticky}} {
		want := 0.0
		for _, v := range c.vals {
			want += v
		}
		got := e.scan(agg, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{c.k}}}})
		r := got[0]
		s, a, n := r[1].(*chtype.AggPartial), r[2].(*chtype.AggPartial), r[3].(*chtype.AggPartial)
		if s.Sum != want || a.Sum != want || a.Count != 4 || n.Count != 4 || r[4] != want {
			t.Fatalf("%s: sum %v avg %v/%d count %d simple %v, want %v and 4", c.k, s.Sum, a.Sum, a.Count, n.Count, r[4], want)
		}
		if ai := r[5].(*chtype.AggPartial); ai.Sum != float64(12) || ai.Count != 4 {
			t.Fatalf("%s: integer avg %v/%d", c.k, ai.Sum, ai.Count)
		}
	}

	// SummingMergeTree (direct INSERT): 1e30 + 0.30000000000000004, a
	// Float32 column rounding each step, and NaN.
	sm := e.table(&catalog.Table{Database: "default", Name: "fsum", Engine: catalog.EngineSummingMergeTree,
		Columns: cols("k", "UInt8", "v", "Float64", "w", "Float32", "n", "UInt64"), OrderBy: []string{"k"}})
	e.insert(sm, storage.Row{uint64(1), 1e30, 0.1, uint64(1)})
	e.insert(sm, storage.Row{uint64(1), 0.30000000000000004, float64(float32(0.2)), uint64(2)})
	e.insert(sm, storage.Row{uint64(2), math.NaN(), 1.0, uint64(1)})
	got := e.all(sm)
	sort.Slice(got, func(i, j int) bool { return got[i][0].(uint64) < got[j][0].(uint64) })
	v := 1e30
	v += 0.30000000000000004
	w := float32(0.1) + float32(0.2)
	if len(got) != 2 || got[0][1] != v || got[0][2] != float64(w) || got[0][3] != uint64(3) || !math.IsNaN(got[1][1].(float64)) {
		t.Fatalf("summing rows %v", got)
	}

	// Concurrent writers of the same float sums (inline and the stream
	// worker of several servers): compare-and-swap loses no addend.
	var wg sync.WaitGroup
	var failed atomic.Value
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				// Every row is merged twice (replay): counted once.
				r := row("hot", 0.5, fmt.Sprintf("h%d-%d", g, i))
				for rep := 0; rep < 2; rep++ {
					if err := e.store.Merge(e.ctx, agg, []storage.MergeRow{r}); err != nil {
						failed.Store(err)
						return
					}
				}
				if _, err := e.store.Insert(e.ctx, sm, []storage.Row{{uint64(3), 1.5, 0.5, uint64(1)}}); err != nil {
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
	hot := e.scan(agg, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{"hot"}}}})[0]
	if s, n := hot[1].(*chtype.AggPartial).Sum, hot[3].(*chtype.AggPartial).Count; s != 40.0 || n != 80 {
		t.Fatalf("concurrent float sums: sum %v count %d", s, n)
	}
	h3 := e.scan(sm, storage.ScanRequest{Ranges: []storage.KeyRange{{Partition: []any{uint64(3)}}}})[0]
	if h3[1] != 120.0 || h3[2] != 40.0 || h3[3] != uint64(80) {
		t.Fatalf("concurrent summing inserts: %v", h3)
	}
}
