package exec

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/storage"
)

// mergeSpy records the size of every Store.Merge call and can fail one.
type mergeSpy struct {
	storage.Store
	mu    sync.Mutex
	sizes []int
	// failAt is the 1-based call that fails once (0: none).
	failAt int
}

func (s *mergeSpy) Merge(ctx context.Context, t *catalog.Table, rows []storage.MergeRow) error {
	s.mu.Lock()
	s.sizes = append(s.sizes, len(rows))
	fail := len(s.sizes) == s.failAt
	s.mu.Unlock()
	if fail {
		return cherr.Wrap(cherr.Unavailable, errors.New("injected"), "merge failed")
	}
	return s.Store.Merge(ctx, t, rows)
}

func (s *mergeSpy) calls() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.sizes...)
}

// fanOutElements is the array length of the fan-out source row: the view
// outputs its square.
const fanOutElements = 1000

// newFanOutEnv has a view that outputs fanOutElements² rows for one source
// row, summed into ten keys, with ApplyViews limited to viewMem bytes.
func newFanOutEnv(t *testing.T, viewMem int64) (*testEnv, *mergeSpy, *catalog.Table) {
	spy := &mergeSpy{}
	te := newTestEnvConfig(t, func(c *Config) {
		spy.Store = c.Store
		c.Store, c.InlineViews, c.MaxMemoryUsage, c.ViewMemoryUsage = spy, false, testMaxMemoryUsage, viewMem
	})
	te.exec("CREATE TABLE src (id UInt64, a Array(UInt64)) ENGINE = MergeTree ORDER BY id")
	te.exec("CREATE TABLE dst (k UInt64, v UInt64, n UInt64) ENGINE = SummingMergeTree ORDER BY k")
	te.exec("CREATE MATERIALIZED VIEW mv TO dst AS SELECT arrayJoin(a) % 10 AS k, arrayJoin(arrayReverse(a)) AS v, 1 AS n FROM src")
	src, err := te.cat.Table("default", "src")
	if err != nil {
		t.Fatal(err)
	}
	return te, spy, src
}

func fanOutRow() storage.Row {
	a := make([]any, fanOutElements)
	for i := range a {
		a[i] = uint64(i)
	}
	return storage.Row{uint64(1), a}
}

// wantFanOutTotals checks that dst holds the fan-out row's output once.
func wantFanOutTotals(te *testEnv) {
	te.t.Helper()
	const n = fanOutElements
	te.expectRows("SELECT count(), sum(v), sum(n) FROM dst", fmt.Sprintf("10\t%d\t%d", n*(n*(n-1)/2), n*n))
}

// One source row's output is merged in batches as it is produced, so a
// fan-out far larger than the view's memory limit completes, and the split
// does not change what is applied: re-applying the row is a no-op.
func TestViewFanOutMergesInBatchesWithinTheLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("applies a million view rows")
	}
	guardFunctionTest(t)
	te, spy, src := newFanOutEnv(t, 16_000_000)
	ctx := context.Background()
	for range 2 {
		if err := te.e.ApplyViews(ctx, src, []storage.Row{fanOutRow()}, []string{"r1"}); err != nil {
			t.Fatal(err)
		}
		wantFanOutTotals(te)
	}
	calls := spy.calls()
	if len(calls) < 2*fanOutElements*fanOutElements/mergeBatch {
		t.Fatalf("%d Merge calls for %d rows", len(calls), 2*fanOutElements*fanOutElements)
	}
	for _, n := range calls {
		if n > mergeBatch {
			t.Fatalf("a Merge call of %d rows (batch %d)", n, mergeBatch)
		}
	}
}

// A Merge failing part way through one source row's output leaves part of
// it applied; applying the row again applies the rest, each output row
// once.
func TestViewRetryAfterPartialMergeAppliesEachRowOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("applies a million view rows")
	}
	guardFunctionTest(t)
	te, spy, src := newFanOutEnv(t, 16_000_000)
	spy.failAt = 3
	ctx := context.Background()
	if err := te.e.ApplyViews(ctx, src, []storage.Row{fanOutRow()}, []string{"r1"}); !cherr.Is(err, cherr.Unavailable) {
		t.Fatalf("first application: %v", err)
	}
	te.expectRows("SELECT sum(n) FROM dst", fmt.Sprint(2*mergeBatch))
	if err := te.e.ApplyViews(ctx, src, []storage.Row{fanOutRow()}, []string{"r1"}); err != nil {
		t.Fatal(err)
	}
	wantFanOutTotals(te)
}

// The batch of view output waiting for Merge counts against the limit: an
// output row larger than the limit fails with 241.
func TestViewOutputBatchCountsAgainstTheLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("applies a view under a limit")
	}
	guardFunctionTest(t)
	te := newMemoryEnv(t, 0, 1_000_000)
	te.e.cfg.InlineViews = false
	te.exec("CREATE TABLE src (n UInt64) ENGINE = MergeTree ORDER BY n")
	te.exec("CREATE TABLE dst (s String) ENGINE = MergeTree ORDER BY tuple()")
	te.exec("CREATE MATERIALIZED VIEW mv TO dst AS SELECT repeat('ab', n) AS s FROM src")
	src, err := te.cat.Table("default", "src")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	err = te.e.ApplyViews(ctx, src, []storage.Row{{uint64(1_000_000)}}, []string{"r1"})
	if !cherr.Is(err, cherr.MemoryLimitExceeded) || !strings.Contains(err.Error(), "Query memory limit exceeded") {
		t.Fatalf("a 2 MB output row at a 1 MB limit: %v", err)
	}
	// Rows of 1 KB: a batch of 500 fits, and each is released once merged.
	rows, ids := make([]storage.Row, 2000), make([]string, 2000)
	for i := range rows {
		rows[i], ids[i] = storage.Row{uint64(500)}, fmt.Sprint("s", i)
	}
	if err := te.e.ApplyViews(ctx, src, rows, ids); err != nil {
		t.Fatal(err)
	}
	te.expectRows("SELECT count(), sum(length(s)) FROM dst", "2000\t2000000")
}

// An aggregating view whose state outgrows ViewMemoryUsage fails ApplyViews
// with 241 (the stream worker dead-letters the record, see mvworker).
func TestAggregatingViewOverLimitFailsApplyViews(t *testing.T) {
	if testing.Short() {
		t.Skip("aggregates a 2000x2000 product")
	}
	guardFunctionTest(t)
	te := newMemoryEnv(t, 0, 64<<20)
	te.e.cfg.InlineViews = false
	te.exec("CREATE TABLE src (id UInt64, a Array(UInt64)) ENGINE = MergeTree ORDER BY id")
	te.exec("CREATE TABLE dst (id UInt64, xs Array(UInt64)) ENGINE = MergeTree ORDER BY id")
	te.exec("CREATE MATERIALIZED VIEW mv TO dst AS SELECT id, groupArray(arrayJoin(a) * arrayJoin(arrayReverse(a))) AS xs FROM src GROUP BY id")
	src, err := te.cat.Table("default", "src")
	if err != nil {
		t.Fatal(err)
	}
	a := make([]any, 2000)
	for i := range a {
		a[i] = uint64(i)
	}
	err = te.e.ApplyViews(context.Background(), src, []storage.Row{{uint64(1), a}}, []string{"r1"})
	if !cherr.Is(err, cherr.MemoryLimitExceeded) {
		t.Fatalf("ApplyViews: %v", err)
	}
	te.expectRows("SELECT count() FROM dst", "0")
}
