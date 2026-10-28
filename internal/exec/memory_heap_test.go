//go:build !race

package exec

// The race detector pads allocations, so the live heap matches the
// estimates only without it.

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/storage"
)

// maxLiveHeapDuring runs fn while another goroutine collects garbage in a
// loop, and returns the largest live heap it saw over the one before fn.
func maxLiveHeapDuring(fn func()) int64 {
	base := liveHeapBytes()
	var peak int64
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			peak = max(peak, liveHeapBytes()-base)
			select {
			case <-done:
				return
			default:
			}
		}
	})
	fn()
	close(done)
	wg.Wait()
	return peak
}

// wantLimitBeforeHeap runs sql, which must fail with MEMORY_LIMIT_EXCEEDED
// at limit before the live heap grows by more than limit + 8 MiB.
func wantLimitBeforeHeap(t *testing.T, te *testEnv, sql string, limit int64) {
	t.Helper()
	var err error
	peak := maxLiveHeapDuring(func() {
		_, err = te.run(fmt.Sprintf("%s SETTINGS max_memory_usage = %d", sql, limit))
	})
	if ce := cherr.As(err); ce == nil || ce.Code != cherr.MemoryLimitExceeded || !memoryLimitMsg.MatchString(ce.Msg) {
		t.Errorf("%s: got %v, want MEMORY_LIMIT_EXCEEDED", sql, err)
	}
	t.Logf("%s: live heap grew by %.1f MiB", sql, float64(peak)/(1<<20))
	if peak > limit+8<<20 {
		t.Errorf("%s: live heap grew by %d B, over the limit of %d B + 8 MiB", sql, peak, limit)
	}
}

// heapSample is a query's tracked bytes as its tracker passed a sample
// step, and the live heap's growth over the heap before the query then.
type heapSample struct{ used, live int64 }

// trackedHeap runs run and samples its queries every 16 MiB of tracked
// bytes.
func trackedHeap(te *testEnv, run func()) []heapSample {
	var mu sync.Mutex
	var samples []heapSample
	base := liveHeapBytes()
	te.e.impl.testHookStarted = func(q *query) {
		q.mem.SetSampleHook(16<<20, func(used int64) {
			mu.Lock()
			defer mu.Unlock()
			samples = append(samples, heapSample{used: used, live: liveHeapBytes() - base})
		})
	}
	defer func() { te.e.impl.testHookStarted = nil }()
	run()
	return samples
}

// checkSamples fails unless the live heap stays within the tracked bytes
// plus 8 MiB at every sample, and the tracked bytes are at most twice the
// live heap at the largest.
func checkSamples(t *testing.T, name string, samples []heapSample) {
	t.Helper()
	if len(samples) < 3 {
		t.Errorf("%s: %d samples, want at least 3", name, len(samples))
		return
	}
	last := samples[0]
	for _, s := range samples {
		if s.live > s.used+8<<20 {
			t.Errorf("%s: live heap %d over %d tracked bytes + 8 MiB", name, s.live, s.used)
		}
		if s.used > last.used {
			last = s
		}
	}
	t.Logf("%-44s %2d samples; at the largest, %6.1f MiB tracked for %6.1f MiB live (%.2f)", name, len(samples),
		float64(last.used)/(1<<20), float64(last.live)/(1<<20), float64(last.used)/float64(max(last.live, 1)))
	if last.used > 2*last.live {
		t.Errorf("%s: %d tracked bytes over twice the live heap of %d", name, last.used, last.live)
	}
}

// TestMemoryEstimatesCoverLiveHeap checks what each holder charges as it
// grows: the live heap never passes the tracked bytes by more than 8 MiB,
// and at the largest sample the tracked bytes are at most twice it.
func TestMemoryEstimatesCoverLiveHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("fills holders of about 100 MB")
	}
	guardFunctionTest(t)
	te := newTestEnvConfig(t, func(c *Config) {
		c.MaxMemoryUsage, c.Store = testMaxMemoryUsage, discardMerges{c.Store}
	})
	te.exec("CREATE TABLE wide (n UInt64, s String) ENGINE = MergeTree ORDER BY n")
	for _, c := range []struct{ name, sql string }{
		{"ORDER BY", "SELECT count() FROM (SELECT number, toString(number) AS s FROM numbers(1000000) ORDER BY s DESC)"},
		{"DISTINCT", "SELECT count() FROM (SELECT DISTINCT toString(number) FROM numbers(1500000))"},
		{"LIMIT BY", "SELECT count() FROM (SELECT toString(number) AS s FROM numbers(1500000) LIMIT 1 BY s)"},
		{"GROUP BY and states", "SELECT count() FROM (SELECT number % 200000 AS k, sum(number), uniqExact(number), " +
			"groupArray(toString(number)) FROM numbers(1000000) GROUP BY k)"},
		{"JOIN build", "SELECT count() FROM numbers(10) AS l JOIN " +
			"(SELECT number AS k, toString(number) AS s FROM numbers(1000000)) AS r ON l.number = r.k"},
		{"IN set", "SELECT count() FROM numbers(10) WHERE toString(number) IN (SELECT toString(number) FROM numbers(1000000))"},
		{"INSERT block", "INSERT INTO wide SELECT number, repeat(toString(number), 10) FROM numbers(400000)"},
		{"ARRAY JOIN keeping the array, then ORDER BY", "SELECT count() FROM (SELECT number, a, x FROM " +
			"(SELECT number, range(number, number + 1000) AS a FROM numbers(1000)) ARRAY JOIN a AS x ORDER BY x DESC)"},
	} {
		checkSamples(t, c.name, trackedHeap(te, func() { te.exec(c.sql) }))
	}

	// A view groups the elements of one source row's array, as the stream
	// worker applies it. Its output is dropped, so the store holds nothing
	// new.
	te.exec("CREATE TABLE src (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple()")
	te.exec("CREATE TABLE dst (k UInt64, c UInt64) ENGINE = SummingMergeTree ORDER BY k")
	te.exec("CREATE MATERIALIZED VIEW mv TO dst AS SELECT arrayJoin(a) AS k, count() AS c FROM src GROUP BY k")
	src, err := te.cat.Table("default", "src")
	if err != nil {
		t.Fatal(err)
	}
	a := make([]any, 600000)
	for i := range a {
		a[i] = uint64(1000 + i)
	}
	checkSamples(t, "view GROUP BY", trackedHeap(te, func() {
		if err := te.e.ApplyViews(context.Background(), src, []storage.Row{{a}}, []string{"r1"}); err != nil {
			t.Fatal(err)
		}
	}))
}

// peakHeapObjects collects garbage, runs fn and returns the largest heap
// (live and garbage) sampled every 2 ms meanwhile.
func peakHeapObjects(fn func()) uint64 {
	runtime.GC()
	done, peak := make(chan struct{}), make(chan uint64)
	go func() {
		s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
		tick := time.NewTicker(2 * time.Millisecond)
		defer tick.Stop()
		var mx uint64
		for {
			metrics.Read(s)
			mx = max(mx, s[0].Value.Uint64())
			select {
			case <-done:
				peak <- mx
				return
			case <-tick.C:
			}
		}
	}()
	fn()
	close(done)
	return <-peak
}

// TestQueryAtLimitStaysBelowHeap runs a query that fills each kind of
// holder at a limit of half the Go memory limit: each fails with 241
// before the heap, garbage included, reaches the Go limit.
func TestQueryAtLimitStaysBelowHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("fills holders up to 192 MB")
	}
	guardFunctionTest(t)
	const goLimit = 384 << 20
	defer debug.SetMemoryLimit(debug.SetMemoryLimit(goLimit))
	te := newTestEnv(t)
	te.exec("CREATE TABLE src (id UInt64, a Array(UInt64)) ENGINE = MergeTree ORDER BY id")
	te.exec("CREATE TABLE dst (id UInt64, xs Array(UInt64)) ENGINE = MergeTree ORDER BY id")
	te.exec("CREATE MATERIALIZED VIEW mv TO dst AS SELECT id, groupArray(arrayJoin(a) * arrayJoin(arrayReverse(a))) AS xs FROM src GROUP BY id")
	for _, c := range []struct{ name, sql string }{
		{"ARRAY JOIN product, sorted", "SELECT count() FROM (SELECT x, y FROM " +
			"(SELECT arrayJoin(range(20000)) AS x, arrayJoin(range(20001)) AS y) ORDER BY x DESC, y DESC)"},
		{"aggregating view fan-out", "INSERT INTO src SELECT 1, range(3000)"},
		{"ORDER BY without LIMIT", "SELECT number FROM numbers(100000000) ORDER BY number DESC"},
		{"scalar subquery of one large row", "SELECT length((SELECT groupArray(number) FROM numbers(100000000)))"},
		{"GROUP BY", "SELECT count() FROM (SELECT number AS k, count() FROM numbers(100000000) GROUP BY k)"},
		{"JOIN build", "SELECT count() FROM numbers(10) AS a CROSS JOIN system.numbers AS b"},
		{"IN set", "SELECT count() FROM numbers(10) WHERE number IN (SELECT number FROM system.numbers)"},
		{"uniqExact", "SELECT uniqExact(number) FROM numbers(100000000)"},
	} {
		var err error
		peak := peakHeapObjects(func() { _, err = te.run(c.sql + " SETTINGS max_memory_usage = 192000000") })
		// A view's error names the view before the limit's message.
		if !cherr.Is(err, cherr.MemoryLimitExceeded) {
			t.Errorf("%s: got %v, want MEMORY_LIMIT_EXCEEDED", c.name, err)
		}
		t.Logf("%-38s heap peak %6.1f MiB", c.name, float64(peak)/(1<<20))
		if peak >= goLimit {
			t.Errorf("%s: heap reached %d B, the Go memory limit is %d B", c.name, peak, goLimit)
		}
	}
}

// A query's limit counts only what the query holds: unrelated heap does
// not fail it.
func TestLimitedQueryIgnoresUnrelatedHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("holds 500 MB")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	ballast := make([]byte, 500<<20)
	for i := 0; i < len(ballast); i += 4096 {
		ballast[i] = 1
	}
	const s = " SETTINGS max_memory_usage = 10000000"
	te.expectRows("SELECT count(), sum(number) FROM numbers(1000000)"+s, "1000000\t499999500000")
	te.expectRows("SELECT sum(c) FROM (SELECT number % 1000 AS k, count() AS c FROM numbers(100000) GROUP BY k ORDER BY k)"+s, "100000")
	runtime.KeepAlive(ballast)
}

// 1500 strings of 1 MB fail with 241 in each holder before the live heap
// they build passes the limit by 8 MiB: none waits for a size threshold
// or a row count.
func TestLargeStringsStopAtTheLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("fills holders up to 32 MB")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	const big = "concat(repeat('x', 1000000), toString(number))"
	for _, sql := range []string{
		"SELECT " + big + " AS s FROM numbers(1500) ORDER BY s DESC",
		"SELECT length(groupArray(" + big + ")) FROM numbers(1500)",
		"SELECT count() FROM numbers(10) WHERE toString(number) IN (SELECT " + big + " FROM numbers(1500))",
		"SELECT count(), max(length(b.s)) FROM numbers(10) AS a JOIN (SELECT number AS k, " + big + " AS s FROM numbers(1500)) AS b ON a.number = b.k",
	} {
		wantLimitBeforeHeap(t, te, sql, 32000000)
	}
}
