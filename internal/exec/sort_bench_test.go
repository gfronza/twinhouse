package exec

import "testing"

// newSortBenchEnv has a table of 100k rows of a short string and a number.
func newSortBenchEnv(b *testing.B) *testEnv {
	te := newTestEnv(b)
	te.exec(`CREATE TABLE t (k String, v UInt64) ENGINE = MergeTree ORDER BY k`)
	te.exec(`INSERT INTO t SELECT toString(number % 1000), number FROM numbers(100000)`)
	return te
}

// BenchmarkOrderBy sorts every row (no LIMIT).
func BenchmarkOrderBy(b *testing.B) {
	te := newSortBenchEnv(b)
	for b.Loop() {
		te.exec(`SELECT k, v FROM t ORDER BY v % 997 DESC, k`)
	}
}

// BenchmarkDistinct keeps 50k distinct keys of 100k rows.
func BenchmarkDistinct(b *testing.B) {
	te := newSortBenchEnv(b)
	for b.Loop() {
		te.exec(`SELECT DISTINCT v % 50000, k FROM t`)
	}
}

// BenchmarkOrderByLimitStrings keeps half of 200k shuffled rows that hold
// a long string (19 B or more) or a short one.
func BenchmarkOrderByLimitStrings(b *testing.B) {
	te := newTestEnv(b)
	te.exec(`CREATE TABLE t (s String, k String, v UInt64) ENGINE = MergeTree ORDER BY v`)
	te.exec(`INSERT INTO t SELECT concat('long-string-value-', toString(number)), toString(number % 1000), number FROM numbers(200000)`)
	for _, c := range []struct{ name, col string }{{"Long", "s"}, {"Short", "k"}} {
		b.Run(c.name, func(b *testing.B) {
			for b.Loop() {
				te.exec(`SELECT ` + c.col + `, v FROM t ORDER BY v * 2654435761 % 1000003 LIMIT 100000`)
			}
		})
	}
}
