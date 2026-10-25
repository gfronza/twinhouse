//go:build e2e

package e2e

import (
	"strings"
	"testing"
)

// TestRollupFloatSumNoise: float rollup sums whose exact decimal sum needs
// more than DynamoDB's 38 significant digits (1500000.25 + 5.55e-17) used
// to fail the view application ("precision up to 38 digits"), dropping the
// row from count/avg too, and the stream worker dead-lettered it. Now the
// INSERTs succeed, the rollup matches the source, and the worker's
// re-application leaves it unchanged.
func TestRollupFloatSumNoise(t *testing.T) {
	t.Parallel()
	heavy(t)
	db := newDB(t, "float_noise")
	for _, q := range []string{
		"CREATE TABLE src (k String, v Float64) ENGINE = MergeTree ORDER BY k",
		"CREATE TABLE agg (k String, s AggregateFunction(sum, Float64), a AggregateFunction(avg, Float64), c AggregateFunction(count)) ENGINE = AggregatingMergeTree ORDER BY k",
		"CREATE MATERIALIZED VIEW mv TO agg AS SELECT k, sumState(v) AS s, avgState(v) AS a, countState() AS c FROM src GROUP BY k",
		"INSERT INTO src VALUES ('x', 1500000.25)",
		"INSERT INTO src SELECT 'x', 0.1 + 0.2 - 0.3",
		"INSERT INTO src VALUES ('x', 1e-20)",
		"INSERT INTO src SELECT 'z', 0.1 + 0.2 - 0.3",
		"INSERT INTO src VALUES ('z', 1500000.25), ('z', 1000000)",
		"CREATE TABLE sm (k UInt8, v Float64) ENGINE = SummingMergeTree ORDER BY k",
		"INSERT INTO sm VALUES (1, 1e30)",
		"INSERT INTO sm VALUES (1, 0.30000000000000004)",
		"INSERT INTO sm VALUES (2, 0.1), (2, 0.2)",
	} {
		mustExec(t, db, q)
	}
	src := tsv(t, db, "SELECT k, count(), sum(v), avg(v) FROM src GROUP BY k ORDER BY k")
	rollup := tsv(t, db, "SELECT k, countMerge(c), sumMerge(s), avgMerge(a) FROM agg GROUP BY k ORDER BY k")
	if src != rollup || !strings.HasPrefix(src, "x\t3\t1500000.25\t") {
		t.Fatalf("rollup %q, source %q", rollup, src)
	}
	if got := tsv(t, db, "SELECT k, v FROM sm ORDER BY k"); got != "1\t1e30\n2\t0.30000000000000004\n" {
		t.Fatalf("summing float sums: %q", got)
	}
	staysEqual(t, db, "SELECT k, countMerge(c), sumMerge(s), avgMerge(a) FROM agg GROUP BY k ORDER BY k", src, "rollup after the stream worker")
}
