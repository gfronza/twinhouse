package exec

import (
	"context"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
)

func TestResultMetadata(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (k String, n UInt64) ENGINE = MergeTree ORDER BY k`)
	res := te.exec(`INSERT INTO t SELECT toString(number), number FROM numbers(10)`)
	eq(t, res.WrittenRows, uint64(10))
	eq(t, len(res.Columns), 0)
	res = te.exec(`SELECT k FROM t ORDER BY n LIMIT 3 FORMAT JSONCompact`)
	eq(t, res.Format, "JSONCompact")
	eq(t, res.HasRowsBeforeLimit, true)
	eq(t, res.RowsBeforeLimit, uint64(10))
	eq(t, res.ReadRows, uint64(10))
	eq(t, len(res.Rows), 3)
	res = te.exec(`SELECT 1 UNION ALL SELECT 2 FORMAT TSV`)
	eq(t, res.Format, "TSV")
	res = te.exec(`SELECT count() FROM t`)
	eq(t, res.HasRowsBeforeLimit, false)
	res = te.exec(`SHOW TABLES FORMAT JSON`)
	eq(t, res.Format, "JSON")
}

func TestHandshakeQuery(t *testing.T) {
	te := newTestEnv(t)
	res := te.exec(`SELECT displayName(), version(), revision(), timezone()`)
	eq(t, res.Rows[0], []any{"test", "25.8.1.1", uint64(54477), "UTC"})
	got := []string{}
	for _, c := range res.Columns {
		got = append(got, c.Name+" "+c.Type.String())
	}
	eq(t, got, []string{"displayName() String", "version() String", "revision() UInt32", "timezone() String"})
}

func TestMaxResultRows(t *testing.T) {
	te := newTestEnv(t)
	_, err := te.runReq(&Request{Query: `SELECT number FROM numbers(10)`, Settings: map[string]string{"max_result_rows": "5"}})
	if !cherr.Is(err, cherr.TooManyRowsOrBytes) {
		t.Fatalf("expected TOO_MANY_ROWS_OR_BYTES, got %v", err)
	}
	res, err := te.runReq(&Request{Query: `SELECT number FROM numbers(10)`, Settings: map[string]string{"max_result_rows": "5", "result_overflow_mode": "break"}})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(res.Rows), 5)
	te.e.cfg.MaxResultRows = 3
	te.expectErr(`SELECT number FROM numbers(4)`, cherr.TooManyRowsOrBytes)
	te.expectRows(`SELECT number FROM numbers(3)`, "0\n1\n2")
	te.expectErr(`SELECT number FROM numbers(10) SETTINGS max_result_rows = 2`, cherr.TooManyRowsOrBytes)
}

func TestCancellation(t *testing.T) {
	te := newTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := te.e.Execute(ctx, &Request{Query: `SELECT count() FROM system.numbers WHERE number % 7 = 100`})
	if err == nil || time.Since(start) > 5*time.Second {
		t.Fatalf("expected the infinite query to be cancelled, got %v after %v", err, time.Since(start))
	}
}

func TestSystemNumbersStreams(t *testing.T) {
	te := newTestEnv(t)
	te.expectRows(`SELECT number * 2 FROM system.numbers WHERE number % 1000 = 999 LIMIT 2`, "1998\n3998")
	te.expectRows(`SELECT sum(number) FROM numbers(100000)`, "4999950000")
}

func TestScalarSubqueryEvaluatedOnce(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (k String, n UInt64) ENGINE = MergeTree ORDER BY k`)
	te.exec(`INSERT INTO t VALUES ('a', 1), ('b', 2)`)
	before := len(te.spy.scans)
	te.expectRows(`SELECT k FROM t WHERE k IN (SELECT k FROM t WHERE n = 2) AND n = (SELECT max(n) FROM t)`, "b")
	// Two subquery scans plus the outer scan: subqueries are not re-run for
	// key pushdown.
	eq(t, len(te.spy.scans)-before, 3)
}

func TestQueryParameters(t *testing.T) {
	te := newTestEnv(t)
	te.expectRows(`SELECT {s:String}, {n:Nullable(UInt8)}, {a:Array(UInt16)}, {d:Date}, {dt:DateTime64(3)}, {m:Map(String, UInt8)}`,
		"x\\ty\t\\N\t[1,2]\t2024-01-02\t2024-01-02 03:04:05.678\t{'k':1}",
		"s", "x\\ty", "n", "\\N", "a", "[1,2]", "d", "2024-01-02", "dt", "2024-01-02 03:04:05.678", "m", "{'k':1}")
	te.expectErr(`SELECT {n:UInt8}`, cherr.BadQueryParameter, "n", "x")
	te.expectErr(`SELECT {n:NoSuchType}`, cherr.UnknownType, "n", "1")
	te.expectRows(`SELECT {n:UInt8} + {n:UInt8}`, "4", "n", "2")
	te.expectRows(`SELECT number FROM numbers(10) LIMIT {l:UInt8}`, "0\n1", "l", "2")
}

func TestExpressionEdgeCases(t *testing.T) {
	te := newTestEnv(t)
	cases := []struct{ sql, want string }{
		{`SELECT CASE WHEN number = 0 THEN 'zero' WHEN number = 1 THEN 'one' ELSE 'many' END FROM numbers(3)`, "zero\none\nmany"},
		{`SELECT number FROM numbers(5) WHERE number IN (1, 3) OR number = 4`, "1\n3\n4"},
		{`SELECT (number, number * 2) FROM numbers(2)`, "(0,0)\n(1,2)"},
		{`SELECT (number AS x) + x FROM numbers(2)`, "0\n2"},
		{`SELECT number FROM numbers(4) ORDER BY number % 2, number DESC`, "2\n0\n3\n1"},
		{`SELECT number % 3 AS g, groupArray(number) FROM numbers(7) GROUP BY g ORDER BY g`, "0\t[0,3,6]\n1\t[1,4]\n2\t[2,5]"},
		{`SELECT number FROM numbers(10) ORDER BY number LIMIT 2 BY number % 3 LIMIT 4`, "0\n1\n2\n3"},
		{`SELECT number FROM numbers(3) WHERE 0`, ""},
		{`SELECT count() FROM numbers(3) WHERE NULL`, "0"},
		{`SELECT toUInt8(1) = toFloat64(1.0)`, "1"},
	}
	for _, c := range cases {
		te.expectRows(c.sql, c.want)
	}
	te.exec(`CREATE TABLE t (k String, n UInt64) ENGINE = MergeTree ORDER BY k`)
	te.exec(`INSERT INTO t VALUES ('a', 1), ('b', 2)`)
	te.expectRows(`SELECT k FROM t PREWHERE n > 1`, "b")
	te.expectRows(`SELECT t.k, x.n FROM t AS x WHERE x.k = 'a'`, "a\t1")
	te.expectRows(`SELECT default.t.k FROM t ORDER BY k`, "a\nb")
	te.expectRows(`SELECT sq.total FROM (SELECT sum(n) AS total FROM t) AS sq`, "3")
	te.expectErr(`SELECT k FROM t WHERE k`, cherr.IllegalTypeOfArgument)
	// Without aggregation HAVING filters like WHERE (ClickHouse 25.8).
	te.expectRows(`SELECT k FROM t HAVING k = 'a'`, "a")
	te.expectErr(`SELECT k FROM t ORDER BY 3`, cherr.BadArguments)
	te.expectErr(`SELECT sum(n) OVER () FROM t`, cherr.NotImplemented)
	te.expectErr(`SELECT k FROM t ASOF JOIN t AS u ON t.k = u.k`, cherr.NotImplemented)
	te.expectErr(`SELECT k FROM t RIGHT SEMI JOIN t AS u ON t.k = u.k`, cherr.NotImplemented)
	te.expectErr(`SELECT k FROM t RIGHT JOIN t AS u ON t.n < u.n`, cherr.NotImplemented)
	te.expectErr(`SELECT quantile(0.5)(n) FROM t`, cherr.NotImplemented)
	te.expectErr(`SELECT sumArray([n]) FROM t`, cherr.NotImplemented)
	te.expectErr(`SELECT x -> x FROM t`, cherr.NotImplemented)
	te.expectErr(`SELECT INTERVAL 1 DAY`, cherr.NotImplemented)
	te.expectErr(`SELECT k, count() FROM t GROUP BY k WITH TOTALS`, cherr.NotImplemented)
}

func TestTypeInference(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (u8 UInt8, i32 Int32, u64 UInt64, f32 Float32, s String, lc LowCardinality(String), dt DateTime64(9), n Nullable(Int16), d Date) ENGINE = MergeTree ORDER BY u8`)
	cases := []struct{ expr, want string }{
		{"count()", "UInt64"}, {"sum(u64)", "UInt64"}, {"sum(i32)", "Int64"}, {"sum(f32)", "Float64"}, {"avg(u8)", "Float64"},
		{"max(dt)", "DateTime64(9)"}, {"min(d)", "Date"}, {"uniqExact(s)", "UInt64"}, {"argMax(s, dt)", "String"}, {"countIf(u8 > 1)", "UInt64"},
		{"if(u8 > 0, u8, toInt16(1))", "Int16"}, {"u8 = 1", "UInt8"}, {"sum(n)", "Nullable(Int64)"}, {"max(n)", "Nullable(Int16)"},
		{"argMax(lc, dt)", "String"}, {"any(lc)", "String"}, {"groupArray(n)", "Array(Int16)"}, {"uniqExactState(lc)", "AggregateFunction(uniqExact, String)"},
		{"u8 + i32", "Int64"}, {"u64 * f32", "Float64"}, {"lc = 'a'", "LowCardinality(UInt8)"}, {"concat(lc, s)", "LowCardinality(String)"},
		{"toDate(dt)", "Date"}, {"toStartOfHour(dt)", "DateTime"}, {"dt + 1", "DateTime64(9)"}, {"d + 1", "Date"},
		{"n + 1", "Nullable(Int32)"}, {"coalesce(n, 0)", "Int16"}, {"ifNull(n, u8)", "Int16"}, {"[u8, i32]", "Array(Int32)"},
		{"(u8, s)", "Tuple(UInt8, String)"}, {"tuple(u8).1", "UInt8"}, {"sumIf(f32, n > 0)", "Float64"}, {"uniqExactIfState(s, u8 > 0)", "AggregateFunction(uniqExactIf, String, UInt8)"},
	}
	for _, c := range cases {
		res := te.exec("SELECT " + c.expr + " FROM t")
		if got := res.Columns[0].Type.String(); got != c.want {
			t.Errorf("%s: got %s, want %s", c.expr, got, c.want)
		}
	}
}

func TestAggPartialStatesOutput(t *testing.T) {
	te := newTestEnv(t)
	res := te.exec(`SELECT uniqExactState(number % 3), sumState(number), argMaxState(toString(number), number), countState() FROM numbers(5)`)
	u := res.Rows[0][0].(*chtype.AggPartial)
	if u.Func != "uniqExact" || !u.SetLoaded || len(u.Set) != 3 || u.SetSize != 3 {
		t.Errorf("uniqExactState: %+v", u)
	}
	s := res.Rows[0][1].(*chtype.AggPartial)
	eq(t, []any{s.Func, s.Sum}, []any{"sum", uint64(10)})
	a := res.Rows[0][2].(*chtype.AggPartial)
	eq(t, []any{a.Func, a.Value, a.Key, a.HasValue}, []any{"argMax", "4", uint64(4), true})
	c := res.Rows[0][3].(*chtype.AggPartial)
	eq(t, []any{c.Func, c.Count}, []any{"count", uint64(5)})
}

func TestRecursionGuards(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x`)
	te.exec(`INSERT INTO t VALUES (1), (2)`)
	// Inside its own body a CTE name refers to the table.
	te.expectRows(`WITH t AS (SELECT x * 10 AS x FROM t) SELECT x FROM t ORDER BY x`, "10\n20")
	te.exec(`CREATE VIEW v AS SELECT 1 AS a`)
	te.exec(`CREATE OR REPLACE VIEW v AS SELECT a + 1 AS a FROM v`)
	te.expectErr(`SELECT * FROM v`, tooDeepRecursion)
	// A failed OR REPLACE keeps the old definition.
	te.expectErr(`CREATE OR REPLACE VIEW v AS SELECT nope FROM t`, cherr.UnknownIdentifier)
	if _, err := te.cat.View("default", "v"); err != nil {
		t.Fatal("failed CREATE OR REPLACE VIEW dropped the existing view")
	}
}

// An error in a constant expression is raised when the query is planned,
// before any row is read, as ClickHouse raises it when it analyzes the
// query: also with no rows and in a branch of multiIf or CASE not taken,
// but not in the branch of if that a constant UInt8, Bool or NULL
// condition rules out.
func TestConstantErrorsAreRaisedWhenPlanned(t *testing.T) {
	te := newTestEnv(t)
	for _, c := range []struct {
		sql  string
		code int
	}{
		{"SELECT intDiv(1, 0)", 153},
		{"SELECT number + intDiv(1, 0) FROM numbers(3) WHERE 0", 153},
		{"SELECT number, if(number > 100, intDiv(1, 0), 1) FROM numbers(3)", 153},
		{"SELECT number FROM numbers(3) WHERE 0 AND intDiv(1, 0) = 1", 153},
		{"SELECT toUInt8('abc') FROM numbers(3) WHERE 0", 6},
		{"SELECT multiIf(1, 1, intDiv(1, 0))", 153},
		{"SELECT CASE WHEN 0 THEN toUInt8('abc') ELSE 1 END", 6},
		{"SELECT 1 OR intDiv(1, 0) = 1", 153},
		{"SELECT ifNull(1, intDiv(1, 0))", 153},
		{"SELECT coalesce(1, intDiv(1, 0))", 153},
		{"SELECT materialize(intDiv(1, 0)) FROM numbers(0)", 153},
		{"SELECT if(toInt8(1), 1, intDiv(1, 0))", 153},
		{"SELECT if(toUInt16(1), 1, intDiv(1, 0))", 153},
		{"SELECT if(256, 1, intDiv(1, 0))", 153},
		{"SELECT if(-1, 1, intDiv(1, 0))", 153},
		{"SELECT if(1.5, 1, intDiv(1, 0))", 153},
		{"SELECT if(toNullable(toInt8(1)), 1, intDiv(1, 0))", 153},
	} {
		for _, stream := range []bool{false, true} {
			_, err := te.e.Execute(context.Background(), &Request{Query: c.sql, Stream: stream})
			wantCode(t, c.sql, err, c.code, "")
		}
	}
	// A function of a column or of materialize() is evaluated per row, and
	// the branch of if (or ?:) that a constant UInt8, Bool or NULL condition
	// rules out is never evaluated (results from 25.8.33.6).
	for _, c := range []struct{ sql, want string }{
		{"SELECT intDiv(number, 0) FROM numbers(3) WHERE 0", ""},
		{"SELECT intDiv(materialize(1), 0) FROM numbers(3) WHERE 0", ""},
		{"SELECT materialize(2) + 1, toTypeName(materialize(2))", "3\tUInt8"},
		{"SELECT if(1, 1, intDiv(1, 0))", "1"},
		{"SELECT if(0, intDiv(1, 0), 1)", "1"},
		{"SELECT number, if(1, number, intDiv(1, 0)) FROM numbers(2)", "0\t0\n1\t1"},
		{"SELECT if(1, 1, toUInt8('abc'))", "1"},
		{"SELECT if(1, if(0, intDiv(1, 0), 2), 3)", "2"},
		{"SELECT if(0, if(1, intDiv(1, 0), 2), 3)", "3"},
		{"SELECT 1 ? 2 : intDiv(1, 0)", "2"},
		{"SELECT if(1 = 1, 1, intDiv(1, 0))", "1"},
		{"WITH 0 AS z SELECT if(z, intDiv(1, 0), 1)", "1"},
		{"SELECT if(NULL, intDiv(1, 0), 1)", "1"},
		{"SELECT if(true, 1, intDiv(1, 0))", "1"},
		{"SELECT if(toNullable(1), 1, intDiv(1, 0))", "1"},
		{"SELECT if(toLowCardinality(toUInt8(1)), 1, intDiv(1, 0))", "1"},
		{"SELECT number FROM numbers(3) WHERE if(0, intDiv(1, 0), 1) = 1", "0\n1\n2"},
	} {
		te.expectRows(c.sql, c.want)
	}
}
