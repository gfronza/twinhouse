package exec

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/storage"
)

// insertSpy records the size of every Store.Insert call.
type insertSpy struct {
	storage.Store
	mu    sync.Mutex
	sizes []int
}

func (s *insertSpy) Insert(ctx context.Context, t *catalog.Table, rows []storage.Row) (*storage.InsertResult, error) {
	s.mu.Lock()
	s.sizes = append(s.sizes, len(rows))
	s.mu.Unlock()
	return s.Store.Insert(ctx, t, rows)
}

func (s *insertSpy) calls() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.sizes...)
}

// newInsertEnv has a table t (n UInt64, s String) on a store that records
// its Insert calls; statements are limited to maxMem bytes.
func newInsertEnv(t *testing.T, maxMem int64) (*testEnv, *insertSpy) {
	spy := &insertSpy{}
	te := newTestEnvConfig(t, func(c *Config) {
		spy.Store = c.Store
		c.Store, c.MaxMemoryUsage = spy, maxMem
	})
	te.exec("CREATE TABLE t (n UInt64, s String) ENGINE = MergeTree ORDER BY n")
	spy.sizes = nil
	return te, spy
}

// wideInsert writes 5000 rows of 10 KB: about 50 MB of estimated memory.
const wideInsert = "INSERT INTO t SELECT number, repeat(toString(number % 10), 10000) FROM numbers(5000)"

// Without an explicit min_insert_block_size_bytes, a block of a limited
// query ends at a quarter of the limit, so an INSERT larger than the limit
// completes in blocks of under 8 MB of estimate (about 775 of these rows).
func TestInsertBlockEndsAtAQuarterOfTheLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("inserts 50 MB")
	}
	guardFunctionTest(t)
	te, spy := newInsertEnv(t, testMaxMemoryUsage)
	te.exec(wideInsert + " SETTINGS max_memory_usage = 32000000")
	te.expectRows("SELECT count(), sum(length(s)) FROM t", "5000\t50000000")
	calls := spy.calls()
	if len(calls) == 0 || calls[0] < 700 || calls[0] >= insertBatch {
		t.Fatalf("Insert calls %v: want blocks of about 775 rows", calls)
	}
	for _, n := range calls {
		if n > calls[0] {
			t.Fatalf("Insert calls %v: a block above the first", calls)
		}
	}
}

// An explicit min_insert_block_size_bytes keeps its value bound only: a
// block larger than the limit fails with 241.
func TestInsertWithExplicitBlockBytesHitsTheLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("buffers 32 MB")
	}
	guardFunctionTest(t)
	te, spy := newInsertEnv(t, testMaxMemoryUsage)
	wantMemoryLimit(t, te, wideInsert+" SETTINGS max_memory_usage = 32000000, min_insert_block_size_bytes = 1000000000", nil)
	if calls := spy.calls(); len(calls) != 0 {
		t.Fatalf("Insert calls %v before the block failed", calls)
	}
	te.expectRows("SELECT count() FROM t", "0")
}

// An unlimited query keeps ClickHouse's block bounds: the storage batches
// are full.
func TestUnlimitedInsertKeepsTheDefaultBlock(t *testing.T) {
	te, spy := newInsertEnv(t, 0)
	te.exec("INSERT INTO t SELECT number, toString(number) FROM numbers(2500)")
	if calls := spy.calls(); fmt.Sprint(calls) != "[1000 1000 500]" {
		t.Fatalf("Insert calls %v", calls)
	}
}

// A row larger than the limit fails with 241 and writes nothing.
func TestInsertRowLargerThanTheLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("inserts a 2 MB row under a limit")
	}
	guardFunctionTest(t)
	te, _ := newInsertEnv(t, testMaxMemoryUsage)
	wantMemoryLimit(t, te, "INSERT INTO t SELECT 1, repeat('ab', 1000000) SETTINGS max_memory_usage = 1000000", nil)
	te.expectRows("SELECT count() FROM t", "0")
	te.exec("INSERT INTO t SELECT 1, repeat('ab', 1000) SETTINGS max_memory_usage = 1000000")
	te.expectRows("SELECT count() FROM t", "1")
}

func BenchmarkInsert(b *testing.B) {
	te := newTestEnv(b)
	te.exec("CREATE TABLE t (n UInt64, s String, ts DateTime) ENGINE = MergeTree ORDER BY n")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		te.exec(fmt.Sprintf("INSERT INTO t SELECT number, toString(number), toDateTime(number) FROM numbers(%d, 10000)", i*10000))
	}
	b.ReportMetric(float64(b.N*10000)/b.Elapsed().Seconds(), "rows/s")
}
