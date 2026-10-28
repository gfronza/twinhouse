//go:build !race

package exec

import (
	"context"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// TestAggregationEstimatesCoverLiveHeap checks what GROUP BY charges
// against the live heap its groups and states hold, at the end of its input
// (with the groups map) and after (without it): not below it and at most
// 1.6 times it.
func TestAggregationEstimatesCoverLiveHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("aggregates about 100 MB")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	str := func(prefix string, i int) string { return prefix + strconv.Itoa(i) }
	for _, c := range []struct {
		name  string
		keys  []string
		calls []string
		n     int
		row   func(i int) []any
	}{
		{"count by number", []string{"a"}, []string{"count()"}, 300000,
			func(i int) []any { return []any{int64(i), nil, ""} }},
		{"sum by string", []string{"s"}, []string{"sum(a)"}, 300000,
			func(i int) []any { return []any{int64(i), nil, str("key-", i)} }},
		{"several keys and states", []string{"a", "b", "s"}, []string{"count()", "sum(b)", "avg(b)", "min(a)", "argMax(a, b)"}, 200000,
			func(i int) []any { return []any{int64(i), int64(i % 7), str("k", i%3)} }},
		{"uniqExact of numbers", nil, []string{"uniqExact(a)"}, 1000000,
			func(i int) []any { return []any{int64(i), nil, ""} }},
		{"uniqExact of strings by group", []string{"a % 1000"}, []string{"uniqExact(s)"}, 500000,
			func(i int) []any { return []any{int64(i), nil, str("member-", i)} }},
		{"one-member sets", []string{"a"}, []string{"uniqExact(s)"}, 200000,
			func(i int) []any { return []any{int64(i), nil, str("m", i)} }},
		{"groupUniqArray", nil, []string{"groupUniqArray(a)"}, 1000000,
			func(i int) []any { return []any{int64(i), nil, ""} }},
		{"groupArray of strings", []string{"a % 100"}, []string{"groupArray(s)"}, 1000000,
			func(i int) []any { return []any{int64(i), nil, str("element-", i)} }},
		{"groupArray of short lists", []string{"a"}, []string{"groupArray(b)"}, 300000,
			func(i int) []any { return []any{int64(i / 3), int64(i), ""} }},
		{"argMax of strings", []string{"a % 50000"}, []string{"argMax(s, a)"}, 400000,
			func(i int) []any { return []any{int64(i), nil, strings.Repeat("v", 100+i%50)} }},
		{"min of strings", []string{"a % 50000"}, []string{"min(s)"}, 400000,
			func(i int) []any { return []any{int64(i), nil, strings.Repeat("v", 200-i%50)} }},
		{"any of strings", []string{"a"}, []string{"any(s)"}, 200000,
			func(i int) []any { return []any{int64(i), nil, str("any-", i)} }},
	} {
		q := te.e.newQuery(context.Background(), &Request{})
		ac := compileAggregation(t, q, c.keys, c.calls)
		before := liveHeapBytes()
		var atEndHeap, atEndUsed int64
		in := &freshRows{n: c.n, row: c.row, atEnd: func() {
			atEndHeap, atEndUsed = liveHeapBytes()-before, q.mem.Used()
		}}
		out, err := runAggregation(q, in, ac)
		if err != nil {
			t.Fatal(c.name, err)
		}
		heap, used := liveHeapBytes()-before, q.mem.Used()
		runtime.KeepAlive(out)
		for _, m := range []struct {
			when       string
			heap, used int64
		}{{"at the end of the input", atEndHeap, atEndUsed}, {"once built", heap, used}} {
			ratio := float64(m.used) / float64(max(m.heap, 1))
			t.Logf("%-30s %-24s live %10d charged %10d ratio %.2f", c.name, m.when, m.heap, m.used, ratio)
			if m.used < m.heap-256<<10 || ratio > 1.6 {
				t.Errorf("%s %s: charged %d for a live heap of %d (want between 1x and 1.6x)", c.name, m.when, m.used, m.heap)
			}
		}
		out.close()
		q.finish()
	}
}
