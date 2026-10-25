package exec

import (
	"fmt"
	"testing"
)

func BenchmarkInsertWithViews(b *testing.B) {
	te := newTestEnv(b)
	for _, s := range rollupDDL {
		te.exec(s)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		te.exec(fmt.Sprintf(`INSERT INTO events SELECT concat('t', toString(number %% 10)), concat('u', toString(number %% 1000)), 'k', number, toDateTime64('2024-01-01 00:00:00', 9) + number FROM numbers(%d, 10000)`, i*10000))
	}
	b.ReportMetric(float64(b.N*10000)/b.Elapsed().Seconds(), "rows/s")
}

func BenchmarkGroupBy(b *testing.B) {
	te := newTestEnv(b)
	te.exec(`CREATE TABLE t (k String, v UInt64) ENGINE = MergeTree ORDER BY k`)
	te.exec(`INSERT INTO t SELECT toString(number % 1000), number FROM numbers(100000)`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		te.exec(`SELECT k, sum(v), uniqExact(v), argMax(v, v) FROM t GROUP BY k ORDER BY k LIMIT 10`)
	}
	b.ReportMetric(float64(b.N*100000)/b.Elapsed().Seconds(), "rows/s")
}
