package sqlparse

import (
	"math"
	"reflect"
	"testing"
)

// Extra expressions exercising rendering corner cases beyond the corpus.
var roundTripExprs = []string{
	"-(1)", "-(-1)", "- -x", "-(1.5)", "-(inf)", "-'a'", "-INTERVAL 1 DAY",
	"NOT NOT a", "NOT (a AND b)", "NOT ((a AND b))", "NOT (1, 2)", "NOT ((1, 2))", "(NOT a) = 1",
	"(a AND b) AND c", "a AND (b AND c)", "((a OR b))", "x = (a AND b)",
	"(1).1", "(-1)[1]", "tup.1.2", "(tup).name", "f(x).name", "[1, 2][1][1]",
	"x -> x", "(x, y) -> x + y", "arrayMap((x) -> (x, 1), arr)", "(x -> x) = 1",
	"(x AS y) + y", "f(x AS y, y)", "[(1 AS a), a]",
	"a BETWEEN (b BETWEEN 1 AND 2) AND 3", "a IS NULL IS NULL", "(a = b) IS NULL",
	"INTERVAL (1 + 1) DAY", "INTERVAL x * 2 HOUR", "INTERVAL '1' DAY", "INTERVAL '5 minutes'",
	"CAST(x AS Array(Tuple(a String, b UInt8)))", "CAST(x, 'Nullable( String )')", "CAST(x, y)",
	"1::UInt8", "[1, 2]::Array(UInt8)", "x::DateTime64(9, 'UTC')",
	"{p:Array(Tuple(String, String, String))}", "(a, b) IN {keys:Array(Tuple(String, String))}",
	"x IN (SELECT y FROM t WHERE z = 1)", "(SELECT max(x) FROM t) + 1",
	"x IN (SELECT 1 UNION ALL SELECT 2)", "exists(SELECT 1)",
	"`weird col` + `select`", "`a.b`", "db.t.col", "\"quoted\"",
	"sum(x) OVER (PARTITION BY a ORDER BY b DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
	"quantiles(0.5, 0.9)(DISTINCT x)", "count(DISTINCT a, b)",
	"CASE WHEN a THEN b END", "CASE x WHEN 1 THEN 'a' ELSE 'b' END",
	"'esc\\'aped\\n\\t\\\\'", "'\\x41\\0'", "1e400", "0.1 + 0.2", "18446744073709551615",
	"-9223372036854775808", "-9223372036854775809", "x DIV 2", "s REGEXP '^a'", "c ? 1 : 2",
	"trim(BOTH 'x' FROM s)", "POSITION('a' IN s)", "substring(s FROM 1 FOR 2)", "EXTRACT(YEAR FROM ts)",
	"dateDiff(second, a, b)", "date_add(DAY, 1, ts)", "DATE '2024-01-01'", "TIMESTAMP '2024-01-01 00:00:00'",
	"a NOT ILIKE 'x' OR b GLOBAL NOT IN (1, 2)", "NULL", "true AND false", "nan", "-nan",
}

func TestRoundTripExpressions(t *testing.T) {
	inputs := append([]string(nil), roundTripExprs...)
	for _, c := range loadColnameCorpus(t) {
		inputs = append(inputs, c.expr)
	}
	for _, in := range inputs {
		e1, err := ParseExpr(in)
		if err != nil {
			t.Errorf("ParseExpr(%q): %v", in, err)
			continue
		}
		s := String(e1)
		e2, err := ParseExpr(s)
		if err != nil {
			t.Errorf("ParseExpr(String(%q) = %q): %v", in, s, err)
			continue
		}
		if !astEqual(e1, e2) {
			t.Errorf("round trip changed AST:\n in:  %s\n out: %s\n e1: %#v\n e2: %#v", in, s, e1, e2)
		}
		if ColumnName(e1) != ColumnName(e2) {
			t.Errorf("round trip changed name of %q: %q vs %q", in, ColumnName(e1), ColumnName(e2))
		}
		if s2 := String(e2); s2 != s {
			t.Errorf("String not stable for %q: %q then %q", in, s, s2)
		}
	}
}

var roundTripSelects = []string{
	"SELECT 1",
	"SELECT a, argMax(t, ts) AS latest, max(ts) FROM db.e WHERE s = {s:String} AND (a, b) IN {keys:Array(Tuple(String, String))} AND ts BETWEEN toDateTime64({lo:String}, 9) AND toDateTime64({hi:String}, 9) GROUP BY a",
	"SELECT x, countIf(latest != 'remove') AS u, count() AS c FROM (SELECT x, rid, argMax(kind, ts) AS latest FROM db.e WHERE g = {g:String} GROUP BY x, rid) GROUP BY x",
	"SELECT * FROM db.snap FINAL WHERE id = {id:String} LIMIT 1",
	"WITH 1 AS one, cte AS (SELECT 2 AS two) SELECT DISTINCT one, two FROM cte AS c PREWHERE x WHERE y GROUP BY ALL WITH TOTALS HAVING count() > 1 ORDER BY one DESC NULLS FIRST, two COLLATE 'en' LIMIT 2 BY one LIMIT 10 OFFSET 5 SETTINGS max_threads = 1",
	"SELECT 1 UNION ALL SELECT 2 UNION DISTINCT (SELECT 3 UNION ALL SELECT 4) SETTINGS a = 1 FORMAT JSON",
	"SELECT a FROM t1 AS x LEFT ANY JOIN t2 AS y ON x.id = y.id INNER JOIN t3 USING (id, k) CROSS JOIN t4",
	"SELECT a, b FROM t ARRAY JOIN arr AS a, arr2 AS b",
	"SELECT number FROM numbers(10) WHERE number % 2 = 0",
	"SELECT a b, c `d e` FROM t t1",
	"SELECT * FROM system.tables WHERE database = currentDatabase()",
	"SELECT (SELECT 1), x IN (SELECT y FROM u) FROM t",
	"SELECT count() FROM t OFFSET 3",
	"SELECT sum(x) OVER (PARTITION BY a) FROM t",
	"SELECT 1 FROM t LEFT ARRAY JOIN arr",
}

func TestRoundTripSelects(t *testing.T) {
	for _, in := range roundTripSelects {
		st, err := Parse(in)
		if err != nil {
			t.Errorf("Parse(%q): %v", in, err)
			continue
		}
		s := SelectString(st.(*SelectStmt))
		st2, err := Parse(s)
		if err != nil {
			t.Errorf("Parse(SelectString(%q) = %q): %v", in, s, err)
			continue
		}
		if !astEqual(st, st2) {
			t.Errorf("select round trip changed AST:\n in:  %s\n out: %s", in, s)
		}
	}
}

// astEqual compares ASTs, treating NaN literals as equal.
func astEqual(a, b any) bool {
	return deepEqual(reflect.ValueOf(a), reflect.ValueOf(b))
}

func deepEqual(a, b reflect.Value) bool {
	if a.IsValid() != b.IsValid() {
		return false
	}
	if !a.IsValid() {
		return true
	}
	if a.Type() != b.Type() {
		return false
	}
	switch a.Kind() {
	case reflect.Pointer, reflect.Interface:
		if a.IsNil() || b.IsNil() {
			return a.IsNil() == b.IsNil()
		}
		return deepEqual(a.Elem(), b.Elem())
	case reflect.Struct:
		for i := 0; i < a.NumField(); i++ {
			if !deepEqual(a.Field(i), b.Field(i)) {
				return false
			}
		}
		return true
	case reflect.Slice:
		if a.IsNil() != b.IsNil() || a.Len() != b.Len() {
			return false
		}
		for i := 0; i < a.Len(); i++ {
			if !deepEqual(a.Index(i), b.Index(i)) {
				return false
			}
		}
		return true
	case reflect.Float64:
		x, y := a.Float(), b.Float()
		return x == y || (math.IsNaN(x) && math.IsNaN(y))
	}
	return reflect.DeepEqual(a.Interface(), b.Interface())
}
