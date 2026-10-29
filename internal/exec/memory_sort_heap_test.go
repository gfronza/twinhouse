//go:build !race

package exec

import (
	"fmt"
	"sync"
	"testing"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// Each holder gives 241 before the live heap it builds passes the limit
// by more than 8 MiB.
func TestSortAndKeySetsStopAtTheMemoryLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("fills 32 MB holders")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	for _, sql := range []string{
		"SELECT number FROM numbers(100000000) ORDER BY number DESC",
		"SELECT count() FROM (SELECT DISTINCT number FROM numbers(100000000))",
		"SELECT count() FROM (SELECT number FROM numbers(100000000) LIMIT 1 BY number)",
		"SELECT count() FROM (SELECT number FROM numbers(100000000) UNION DISTINCT SELECT number FROM numbers(100000000))",
		"SELECT concat(repeat('x', 1000000), toString(number)) AS s FROM numbers(1500) ORDER BY s DESC LIMIT 100000",
		"SELECT concat(repeat('x', 1000000), toString(number)) AS s FROM numbers(1500) ORDER BY s DESC LIMIT 1 BY s",
	} {
		wantLimitBeforeHeap(t, te, sql, 32000000)
	}
}

// syncPeakLive runs req, sampling the live heap each time its tracked
// bytes pass a MiB. Reserve calls the hook, so the sample is taken where
// the query charges, with nothing allocating meanwhile. It returns the
// largest growth over the heap before the query, and the samples taken.
func syncPeakLive(te *testEnv, req *Request) (peak int64, samples int, err error) {
	base := liveHeapBytes()
	var mu sync.Mutex
	te.e.impl.testHookStarted = func(q *query) {
		q.mem.SetSampleHook(1<<20, func(int64) {
			mu.Lock()
			defer mu.Unlock()
			peak = max(peak, liveHeapBytes()-base)
			samples++
		})
	}
	defer func() { te.e.impl.testHookStarted = nil }()
	_, err = te.runReq(req)
	return peak, samples, err
}

// A sorted subquery whose small rows come out first, read by a holder that
// grows: each row released as it goes out frees only its own charge, so
// the holder cannot grow into the charge of the large rows still held.
func TestSortedRowsReleaseOnlyTheirOwnCharge(t *testing.T) {
	if testing.Short() {
		t.Skip("fills holders up to 32 MB")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	const limit = 32000000
	inner := func(tail string) string {
		return "(SELECT if(number < 1900, '', concat(repeat('x', 280000), toString(number))) AS s FROM numbers(2000) ORDER BY length(s)" + tail + ")"
	}
	for _, sql := range []string{
		"SELECT count() FROM (SELECT DISTINCT s, x FROM " + inner("") + " ARRAY JOIN range(200) AS x)",
		"SELECT count() FROM (SELECT DISTINCT s, x FROM " + inner(" LIMIT 2000") + " ARRAY JOIN range(200) AS x)",
		"SELECT count() FROM (SELECT s, x FROM " + inner("") + " ARRAY JOIN range(200) AS x LIMIT 1 BY x, s)",
		"SELECT count() FROM (SELECT s, x FROM " + inner("") + " ARRAY JOIN range(200) AS x GROUP BY x, s)",
		"SELECT count() FROM numbers(10) WHERE (number, toString(number)) IN (SELECT x, s FROM " + inner("") + " ARRAY JOIN range(200) AS x)",
		// Rows sharing an ARRAY JOIN source array come out interleaved
		// with those of the other arrays.
		"SELECT count() FROM (SELECT DISTINCT x, toString(x) FROM (SELECT a, x FROM (SELECT range(number * 2000, number * 2000 + 2000) AS a " +
			"FROM numbers(100)) ARRAY JOIN a AS x ORDER BY x % 2000))",
	} {
		peak, _, err := syncPeakLive(te, &Request{Query: fmt.Sprintf("%s SETTINGS max_memory_usage = %d", sql, limit)})
		if !cherr.Is(err, cherr.MemoryLimitExceeded) {
			t.Errorf("%s: got %v, want MEMORY_LIMIT_EXCEEDED", sql, err)
		}
		t.Logf("%s: live heap grew by %.1f MiB", sql, float64(peak)/(1<<20))
		if peak > limit+8<<20 {
			t.Errorf("%s: live heap grew by %d B, over the limit of %d B + 8 MiB", sql, peak, limit)
		}
	}
}

// wantPeakWithinLimit runs sql at limit and fails if the live heap grows
// by more than limit + 8 MiB, sampled each MiB the query tracks. It
// returns the query's error, which may only be MEMORY_LIMIT_EXCEEDED.
func wantPeakWithinLimit(t *testing.T, te *testEnv, sql string, limit int64) error {
	t.Helper()
	peak, _, err := syncPeakLive(te, &Request{Query: fmt.Sprintf("%s SETTINGS max_memory_usage = %d", sql, limit)})
	t.Logf("live heap grew by %.1f MiB (%v): %s", float64(peak)/(1<<20), err != nil, sql)
	if err != nil && !cherr.Is(err, cherr.MemoryLimitExceeded) {
		t.Errorf("%s: %v", sql, err)
	}
	if peak > limit+8<<20 {
		t.Errorf("%s: live heap grew by %d B, over the limit of %d B + 8 MiB", sql, peak, limit)
	}
	return err
}

// An aggregation, sort or top-N keeps the row it returned last charged
// until it is asked for the next: a reader that streams (ARRAY JOIN, a
// join probe) still holds that row's values while a holder after it
// grows.
func TestStreamedRowsStayChargedWhileTheirReaderHoldsThem(t *testing.T) {
	if testing.Short() {
		t.Skip("fills holders up to 32 MB")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	const arr = "SELECT groupArray(concat(repeat('x', 1000), toString(number))) AS a FROM numbers(24000)"
	for _, sql := range []string{
		"SELECT count() FROM (SELECT concat(x, 'y') AS y FROM (" + arr + ") ARRAY JOIN a AS x ORDER BY y DESC)",
		"SELECT count() FROM (SELECT DISTINCT concat(x, 'y') FROM (" + arr + ") ARRAY JOIN a AS x)",
		"SELECT count() FROM (SELECT arrayJoin(a) AS x FROM (" + arr + ") ORDER BY x)",
		"SELECT count() FROM (SELECT concat(x, 'y') AS y FROM (" + arr + " ORDER BY length(a)) ARRAY JOIN a AS x ORDER BY y)",
		"SELECT count() FROM (SELECT concat(x, 'y') AS y FROM (" + arr + " ORDER BY length(a) LIMIT 1) ARRAY JOIN a AS x ORDER BY y)",
		"SELECT count() FROM (SELECT concat(toString(r.number), repeat('q', 1000)) AS w FROM (" + arr + ") AS l " +
			"CROSS JOIN numbers(40000) AS r ORDER BY w)",
	} {
		_ = wantPeakWithinLimit(t, te, sql, 32000000) // hitting the limit is fine; other errors fail inside
	}
}

// A full sort releases each row's own charge as it goes out, whatever the
// order: the same rows sorted either way give the same outcome. Values
// rows share stay charged until the sort closes, also when the first row
// holding one goes out first.
func TestSortedRowsReleaseTheSameInEitherOrder(t *testing.T) {
	if testing.Short() {
		t.Skip("fills holders up to 32 MB")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	const limit = 32000000
	skewed := func(dir string) string {
		return "SELECT s FROM (SELECT concat(repeat('x', if(number < 20, 600000, 10)), toString(number)) AS s FROM numbers(100000) ORDER BY length(s) " + dir + ")"
	}
	asc, desc := wantPeakWithinLimit(t, te, skewed("ASC"), limit), wantPeakWithinLimit(t, te, skewed("DESC"), limit)
	if asc != nil || desc != nil {
		t.Errorf("sorted ascending: %v; descending: %v; want both to fit", asc, desc)
	}
	for _, n := range []int{600000, 800000, 850000} {
		for _, src := range []string{
			fmt.Sprintf("SELECT range(number + %d) AS arr FROM numbers(1)", n),
			fmt.Sprintf("SELECT groupArray(number) AS arr FROM numbers(%d)", n),
		} {
			for _, c := range []struct{ dir, x string }{{"", "1"}, {" DESC", "2"}} {
				inner := "(SELECT x, arr FROM (" + src + ") ARRAY JOIN [1, 2] AS x ORDER BY x" + c.dir + ")"
				_ = wantPeakWithinLimit(t, te, "SELECT count() FROM (SELECT DISTINCT toString(k) FROM (SELECT arrayJoin(arr) AS k FROM "+
					inner+" WHERE x = "+c.x+"))", limit)
			}
		}
	}
}
