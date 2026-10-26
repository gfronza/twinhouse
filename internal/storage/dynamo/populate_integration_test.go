package dynamo

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gfronza/twinhouse/internal/exec"
)

// The iterator exposes the stored row ids POPULATE needs.
var _ exec.RowIDIterator = (*iterator)(nil)

// TestIntegrationPopulateConcurrentInserts: CREATE MATERIALIZED VIEW ...
// POPULATE while rows are being inserted (with inline views) counts every
// source row exactly once. A row both scanned by the backfill and applied
// inline (inserted after the view was created; the hook inserts a batch
// when the backfill's scan starts) shares one exactly-once marker only if
// the backfill uses the stored row id, as the inline path does.
func TestIntegrationPopulateConcurrentInserts(t *testing.T) {
	var hook *truncateHookAPI
	e := newEnvWith(t, func(*Options) {}, func(d ddbAPI) ddbAPI { hook = &truncateHookAPI{ddbAPI: d}; return hook })
	eng, err := exec.New(exec.Config{Store: e.store, Catalog: e.cat, Region: "us-east-1",
		InlineViews: true, ConsistentReads: true, PhysicalName: e.store.PhysicalName})
	if err != nil {
		t.Fatal(err)
	}
	run := func(sql string) string {
		t.Helper()
		res, err := eng.Execute(e.ctx, &exec.Request{Query: sql})
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		var b strings.Builder
		for _, r := range res.Rows {
			fmt.Fprintln(&b, r...)
		}
		return b.String()
	}
	run(`CREATE TABLE src (k String, id UInt64) ENGINE = MergeTree ORDER BY (k, id)`)
	run(`CREATE TABLE roll (k String, n UInt64) ENGINE = SummingMergeTree ORDER BY k`)
	insert := func(from, to int) error {
		var vals []string
		for i := from; i < to; i++ {
			vals = append(vals, fmt.Sprintf("('k%d', %d)", i%5, i))
		}
		_, err := eng.Execute(e.ctx, &exec.Request{Query: "INSERT INTO src VALUES " + strings.Join(vals, ",")})
		return err
	}
	if err := insert(0, 200); err != nil {
		t.Fatal(err)
	}
	stored, err := e.cat.Table("default", "src")
	if err != nil {
		t.Fatal(err)
	}
	var hookErr atomic.Value
	hook.table = stored.PhysicalName
	hook.hook = func() {
		// The view exists: these rows are applied inline and are also
		// visible to the backfill scan that is starting.
		if err := insert(200, 300); err != nil {
			hookErr.Store(err)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for b := 0; b < 15; b++ {
			if err := insert(300+b*20, 320+b*20); err != nil {
				errs <- err
				return
			}
		}
	}()
	run(`CREATE MATERIALIZED VIEW mv TO roll POPULATE AS SELECT k, count() AS n FROM src GROUP BY k`)
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if err, _ := hookErr.Load().(error); err != nil {
		t.Fatal(err)
	}
	src := run(`SELECT count() FROM src`)
	roll := run(`SELECT sum(n) FROM roll`)
	if src != "600\n" || roll != src {
		t.Fatalf("count(src) = %q, sum(roll.n) = %q; want both 600", src, roll)
	}
	if got, want := run(`SELECT k, sum(n) FROM roll GROUP BY k ORDER BY k`), run(`SELECT k, count() FROM src GROUP BY k ORDER BY k`); got != want {
		t.Fatalf("per key: roll\n%s\nsrc\n%s", got, want)
	}
}
