//go:build !race

package exec

// The race detector pads allocations, so the live heap matches the
// estimates only without it.

import (
	"runtime"
	"runtime/metrics"
	"testing"
)

func liveHeapBytes() int64 {
	runtime.GC()
	s := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(s)
	return int64(s[0].Value.Uint64())
}

// resultEstimate is what collecting res.Rows charges: each row, and the
// headers of the capacity left over.
func resultEstimate(res *Result) int64 {
	sz := newRowSizer(res.Columns)
	n := sliceHeader * int64(cap(res.Rows)-len(res.Rows))
	for _, r := range res.Rows {
		n += sz.size(r)
	}
	return n
}

// TestRowSizerCoversCollectedResults checks engine-built results: the
// estimate is not below the live heap the rows hold, and at most 1.6 times
// it.
func TestRowSizerCoversCollectedResults(t *testing.T) {
	if testing.Short() {
		t.Skip("collects about 100 MB")
	}
	guardFunctionTest(t)
	te := newTestEnvConfig(t, func(*Config) {})
	// memstore returns stored set states unloaded, with a LoadSet closure.
	te.exec("CREATE TABLE st (k UInt64, u AggregateFunction(uniqExact, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k")
	te.exec("INSERT INTO st SELECT number, uniqExactState(number) FROM numbers(30000) GROUP BY number")
	for _, c := range []struct{ name, sql string }{
		// Each row boxes new strings over bytes the previous row holds.
		{"substrings of one string", "SELECT substring(s, 1), substring(s, 2), substring(s, 3) FROM (SELECT repeat('a', 100) AS s) ARRAY JOIN range(200000) AS x"},
		// Each number is boxed right after a temporary.
		{"computed numbers", "SELECT number + 1, number + 2, number + 3, number + 4, number + 5, number + 6, number + 7, number + 8, " +
			"number + 9, number + 10, number + 11, number + 12, number + 13, number + 14, number + 15, number + 16 FROM numbers(50000)"},
		{"short strings", "SELECT toString(number) FROM numbers(200000)"},
		// The two branches are the same two strings in every row.
		{"alternating constants", "SELECT number, if(number % 2 = 0, repeat('a', 1000), repeat('b', 1000)) FROM numbers(200000)"},
		{"alternating short constants", "SELECT number, if(number % 2 = 0, 'a', 'b') FROM numbers(200000)"},
		{"states", "SELECT argMaxState(toString(number), number), sumState(number) FROM numbers(200000) GROUP BY number % 20000"},
		{"unloaded set states", "SELECT k, u FROM st"},
		// The hidden sort and LIMIT BY keys are dropped from the rows.
		{"large ORDER BY key", "SELECT number FROM numbers(20000) ORDER BY repeat(toString(number), 100)"},
		{"large LIMIT BY key", "SELECT number FROM numbers(20000) LIMIT 1 BY repeat(toString(number), 50)"},
		{"concat", "SELECT concat(repeat('x', 128), toString(number % 10)) FROM numbers(200000)"},
		{"concat of varying parts", "SELECT concat(repeat('a', number % 300), '/', repeat('b', number % 37)) FROM numbers(100000)"},
		{"array toString", "SELECT toString(range(number % 50)) FROM numbers(50000)"},
		// One part, rendered 257 bytes long: just past a buffer growth.
		{"concat of one array", "SELECT concat([repeat('a', 253 + number % 1)]) FROM numbers(100000)"},
		{"arrayStringConcat of one array", "SELECT arrayStringConcat([[repeat('a', 125 + number % 1)]]) FROM numbers(100000)"},
		// The quoted-literal reader builds each element byte by byte.
		{"CAST to Array(String)", "SELECT CAST(concat('[''', repeat('a', 257 + number % 1), ''']') AS Array(String)) FROM numbers(100000)"},
		{"CAST of escapes to Array(String)", `SELECT CAST(concat('[''', repeat('\\x41', 257 + number % 1), ''']') AS Array(String)) FROM numbers(100000)`},
		{"CAST to Tuple", "SELECT CAST(concat('(''', repeat('a', 257 + number % 1), ''', 1)') AS Tuple(String, UInt8)) FROM numbers(100000)"},
		{"formatDateTime", "SELECT formatDateTime(toDateTime(number), '%Y-%m-%d %H:%i:%S') FROM numbers(200000)"},
	} {
		before := liveHeapBytes()
		res, err := te.run(c.sql)
		if err != nil {
			t.Fatal(c.name, err)
		}
		measured := liveHeapBytes() - before
		est := resultEstimate(res)
		runtime.KeepAlive(res)
		t.Logf("%-24s measured %10d estimate %10d ratio %.2f", c.name, measured, est, float64(est)/float64(max(measured, 1)))
		if est < measured-256<<10 || float64(est) > 1.6*float64(measured) {
			t.Errorf("%s: estimate %d for a live heap of %d (want between 1x and 1.6x)", c.name, est, measured)
		}
	}
}
