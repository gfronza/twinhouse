package exec

import (
	"context"
	"strings"
	"testing"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// TestInSubqueryResults pins IN over a subquery, whose rows stream into the
// set, against ClickHouse 25.8: NULLs, conversions, tuples and GLOBAL IN.
func TestInSubqueryResults(t *testing.T) {
	te := newTestEnv(t)
	for _, c := range []struct{ sql, want string }{
		{"SELECT number FROM numbers(6) WHERE number IN (SELECT number * 2 FROM numbers(3))", "0 2 4"},
		{"SELECT number IN (SELECT NULL) FROM numbers(2)", "0 0"},
		{"SELECT number NOT IN (SELECT if(number = 1, NULL, number) FROM numbers(3)) FROM numbers(4)", "0 1 0 1"},
		{"SELECT toNullable(number) IN (SELECT toNullable(1)) FROM numbers(3)", "0 1 0"},
		{"SELECT NULL IN (SELECT 1)", `\N`},
		{"SELECT if(number = 0, NULL, number) IN (SELECT 1) FROM numbers(2)", `\N 1`},
		{"SELECT (number, toString(number)) IN (SELECT number, toString(number) FROM numbers(2)) FROM numbers(3)", "1 1 0"},
		{"SELECT (number, 1) IN (SELECT number, if(number = 0, NULL, 1) FROM numbers(2)) FROM numbers(2)", "0 1"},
		{"SELECT ((number, 'x'), number) IN (SELECT (number, 'x'), number FROM numbers(2)) FROM numbers(3)", "1 1 0"},
		{"SELECT number FROM numbers(5) WHERE number GLOBAL IN (SELECT 3)", "3"},
		{"SELECT number FROM numbers(5) WHERE number GLOBAL NOT IN (SELECT 3)", "0 1 2 4"},
		{"SELECT toUInt8(number) IN (SELECT 256 + number FROM numbers(3)) FROM numbers(3)", "0 0 0"},
		{"SELECT toInt8(-1) IN (SELECT toUInt64(18446744073709551615))", "0"},
		{"SELECT toFixedString('a', 3) IN (SELECT 'a')", "1"},
		{"SELECT 1 IN (SELECT 1 WHERE 0)", "0"},
		{"SELECT count() FROM numbers(10) WHERE number % 3 IN (SELECT number % 2 FROM numbers(100))", "7"},
		{"SELECT toDate('2024-01-02') IN (SELECT '2024-01-02')", "1"},
		{"SELECT [1, 2] IN (SELECT [1, 2])", "1"},
		{"SELECT toLowCardinality('a') IN (SELECT 'a')", "1"},
		{"SELECT 1.0 IN (SELECT toUInt8(1))", "1"},
		{"SELECT number IN (SELECT toFloat64(number) + 0.5 FROM numbers(3)) FROM numbers(2)", "0 0"},
		{"SELECT toString(number) IN (SELECT toString(number) FROM numbers(2) UNION ALL SELECT '7') FROM numbers(8)", "1 1 0 0 0 0 0 1"},
	} {
		te.expectRows(c.sql, strings.ReplaceAll(c.want, " ", "\n"))
	}
}

// TestInSubqueryRunsOnce checks that the filter and key pushdown share one
// run of an IN subquery, also for a tuple over the key.
func TestInSubqueryRunsOnce(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (k String, n UInt64) ENGINE = MergeTree ORDER BY (k, n)`)
	te.exec(`INSERT INTO t VALUES ('a', 1), ('b', 2), ('c', 3)`)
	for _, c := range []struct{ sql, want string }{
		{`SELECT n FROM t WHERE k IN (SELECT k FROM t WHERE n >= 2) ORDER BY n`, "2\n3"},
		{`SELECT n FROM t WHERE (k, n) IN (SELECT k, n FROM t WHERE n = 2)`, "2"},
		{`SELECT n FROM t WHERE k IN (SELECT k FROM t WHERE n = 3) AND k NOT IN (SELECT 'x')`, "3"},
	} {
		before := len(te.spy.scans)
		te.expectRows(c.sql, c.want)
		// One subquery scan plus the outer scan, narrowed by the set.
		if n := len(te.spy.scans) - before; n != 2 {
			t.Errorf("%s: %d scans, want 2", c.sql, n)
		}
		if len(te.spy.lastScan().Ranges) == 0 {
			t.Errorf("%s: the outer scan was not narrowed", c.sql)
		}
	}
}

// TestInSubqueryOfAnotherTypeRunsAgain checks that a set is reused only for
// the type it was converted to.
func TestInSubqueryOfAnotherTypeRunsAgain(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n`)
	te.exec(`INSERT INTO t VALUES (1), (300)`)
	before := len(te.spy.scans)
	te.expectRows(`WITH (SELECT n FROM t) AS s SELECT number FROM numbers(400) WHERE number IN s AND toUInt8(number) IN s`, "1")
	// One run per left type (UInt64, UInt8); numbers() is not a table scan.
	eq(t, len(te.spy.scans)-before, 2)
}

// TestInSubquerySetIsReleased checks that a set built under a limit is
// released when the query finishes, also when building it failed.
func TestInSubquerySetIsReleased(t *testing.T) {
	if testing.Short() {
		t.Skip("builds sets of 200000 values under a limit")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	for _, c := range []struct {
		sql  string
		code int
	}{
		{sql: "SELECT count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(200000)) SETTINGS max_memory_usage = 64000000"},
		{sql: "SELECT count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(200000)) SETTINGS max_memory_usage = 4000000",
			code: cherr.MemoryLimitExceeded},
		// The subquery fails after 150000 rows are in the set.
		{sql: "SELECT count() FROM numbers(10) WHERE number IN (SELECT intDiv(1, toInt64(number) - 150000) FROM numbers(200000)) " +
			"SETTINGS max_memory_usage = 64000000", code: cherr.IllegalDivision},
	} {
		wantNoTrackedMemory(t, te, context.Background(), c.sql, c.code)
	}
}

// A constant IN set counts against the limit, once: the filter and key
// pushdown share it. Charged twice, the second query would need about
// 45 MB.
func TestConstantInSetIsChargedOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("builds sets of 200000 values under a limit")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n`)
	te.exec(`INSERT INTO t VALUES (1), (300000)`)
	wantMemoryLimit(t, te, "SELECT count() FROM t WHERE n IN range(200000) SETTINGS max_memory_usage = 10000000", nil)
	te.expectRows("SELECT count() FROM t WHERE n IN range(200000) SETTINGS max_memory_usage = 35000000", "1")
	te.expectRows("SELECT count() FROM t WHERE n NOT IN range(200000) AND n IN (1, 300000) SETTINGS max_memory_usage = 35000000", "1")
}

func BenchmarkInSubquery(b *testing.B) {
	te := newTestEnv(b)
	for b.Loop() {
		te.exec(`SELECT count() FROM numbers(200000) WHERE number IN (SELECT number * 3 FROM numbers(100000))`)
		te.exec(`SELECT count() FROM numbers(200000) WHERE toString(number) IN (SELECT toString(number * 3) FROM numbers(100000))`)
	}
}
