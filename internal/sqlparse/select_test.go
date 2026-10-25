package sqlparse

import (
	"testing"

	"dynamodb-eventstream/internal/cherr"
)

func TestSelectAnalyticsQueries(t *testing.T) {
	s := mustSelect(t, `SELECT a, argMax(t, ts) AS latest, max(ts) FROM db.e
		WHERE s = {s:String} AND (a, b) IN {keys:Array(Tuple(String, String))}
		  AND ts BETWEEN toDateTime64({lo:String}, 9) AND toDateTime64({hi:String}, 9)
		GROUP BY a`)
	if len(s.Columns) != 3 || s.Columns[1].Alias != "latest" || s.Columns[2].Alias != "" {
		t.Fatalf("columns: %+v", s.Columns)
	}
	if got := ColumnName(s.Columns[2].Expr); got != "max(ts)" {
		t.Errorf("name = %q", got)
	}
	assertAST(t, "from", s.From, &TableExpr{Database: "db", Table: "e"})
	wantWhere := bin("AND",
		bin("AND",
			bin("=", id("s"), param("s", "String")),
			bin("IN", &TupleExpr{Elems: []Expr{id("a"), id("b")}}, param("keys", "Array(Tuple(String, String))"))),
		&BetweenExpr{X: id("ts"),
			Low:  fn("toDateTime64", param("lo", "String"), u(9)),
			High: fn("toDateTime64", param("hi", "String"), u(9))})
	assertAST(t, "where", s.Where, wantWhere)
	assertAST(t, "group by", s.GroupBy, []Expr{id("a")})

	s = mustSelect(t, `SELECT x, countIf(latest != 'remove') AS u, count() AS c
		FROM (SELECT x, rid, argMax(kind, ts) AS latest FROM db.e WHERE g = {g:String} GROUP BY x, rid)
		GROUP BY x`)
	if s.From.Subquery == nil || s.From.Table != "" {
		t.Fatalf("from: %+v", s.From)
	}
	inner := s.From.Subquery
	assertAST(t, "inner where", inner.Where, bin("=", id("g"), param("g", "String")))
	assertAST(t, "inner group", inner.GroupBy, []Expr{id("x"), id("rid")})
	if inner.Columns[2].Alias != "latest" {
		t.Errorf("inner alias: %+v", inner.Columns[2])
	}

	s = mustSelect(t, "SELECT * FROM db.snap FINAL WHERE id = {id:String} LIMIT 1")
	assertAST(t, "from", s.From, &TableExpr{Database: "db", Table: "snap", Final: true})
	assertAST(t, "cols", s.Columns, []SelectItem{{Expr: &Star{}}})
	assertAST(t, "limit", s.Limit, u(1))
	if s.Offset != nil {
		t.Errorf("offset: %v", s.Offset)
	}
}

func TestSelectClauses(t *testing.T) {
	s := mustSelect(t, `WITH 10 AS lim, recent AS (SELECT * FROM e WHERE ts > now() - INTERVAL 1 HOUR)
		SELECT DISTINCT a AS x, b y, count() c, "quoted alias" FROM recent AS r FINAL
		PREWHERE p = 1 WHERE q = 2
		GROUP BY ALL WITH TOTALS HAVING count() > 1
		ORDER BY x DESC NULLS LAST, y ASC NULLS FIRST, z COLLATE 'tr'
		LIMIT 2 BY x
		LIMIT 5, 10
		SETTINGS max_threads = 4, use_index = 'yes'
		FORMAT JSONEachRow`)
	if len(s.With) != 2 || s.With[0].Name != "lim" || s.With[0].Expr == nil || s.With[1].Name != "recent" || s.With[1].Subquery == nil {
		t.Fatalf("with: %+v", s.With)
	}
	if !s.Distinct {
		t.Error("distinct")
	}
	wantAliases := []string{"x", "y", "c", ""}
	for i, c := range s.Columns {
		if c.Alias != wantAliases[i] {
			t.Errorf("alias %d = %q, want %q", i, c.Alias, wantAliases[i])
		}
	}
	assertAST(t, "quoted ident", s.Columns[3].Expr, id("quoted alias"))
	assertAST(t, "from", s.From, &TableExpr{Table: "recent", Alias: "r", Final: true})
	assertAST(t, "prewhere", s.Prewhere, bin("=", id("p"), u(1)))
	assertAST(t, "where", s.Where, bin("=", id("q"), u(2)))
	if !s.GroupByAll || s.GroupBy != nil || !s.WithTotals {
		t.Errorf("group by all/totals: %v %v %v", s.GroupByAll, s.GroupBy, s.WithTotals)
	}
	tr, fa := true, false
	assertAST(t, "order", s.OrderBy, []OrderItem{
		{Expr: id("x"), Desc: true, NullsFirst: &fa},
		{Expr: id("y"), NullsFirst: &tr},
		{Expr: id("z"), Collate: "tr"},
	})
	assertAST(t, "limit by", s.LimitBy, &LimitByClause{Limit: u(2), By: []Expr{id("x")}})
	assertAST(t, "limit", s.Limit, u(10))
	assertAST(t, "offset", s.Offset, u(5))
	assertAST(t, "settings", s.Settings, []Setting{{"max_threads", u(4)}, {"use_index", str("yes")}})
	if s.Format != "JSONEachRow" {
		t.Errorf("format %q", s.Format)
	}
}

func TestSelectLimitForms(t *testing.T) {
	tests := []struct {
		sql           string
		limit, offset Expr
		limitBy       *LimitByClause
	}{
		{"SELECT 1 LIMIT 3", u(3), nil, nil},
		{"SELECT 1 LIMIT 3 OFFSET 4", u(3), u(4), nil},
		{"SELECT 1 LIMIT 4, 3", u(3), u(4), nil},
		{"SELECT 1 OFFSET 4", nil, u(4), nil},
		{"SELECT 1 OFFSET 4 ROWS", nil, u(4), nil},
		{"SELECT 1 LIMIT {n:UInt64}", param("n", "UInt64"), nil, nil},
		{"SELECT 1 LIMIT 1 OFFSET 2 BY a, b LIMIT 5", u(5), nil, &LimitByClause{Limit: u(1), Offset: u(2), By: []Expr{id("a"), id("b")}}},
		{"SELECT TOP 7 a FROM t", u(7), nil, nil},
	}
	for _, tt := range tests {
		s := mustSelect(t, tt.sql)
		assertAST(t, tt.sql+" limit", s.Limit, tt.limit)
		assertAST(t, tt.sql+" offset", s.Offset, tt.offset)
		assertAST(t, tt.sql+" limit by", s.LimitBy, tt.limitBy)
	}
	expectErr(t, "SELECT 1 LIMIT 1 LIMIT 2", cherr.SyntaxError, "position 18")
}

func TestSelectUnion(t *testing.T) {
	s := mustSelect(t, "SELECT 1 UNION ALL SELECT 2 UNION DISTINCT SELECT 3 SETTINGS a = 1 FORMAT CSV")
	if len(s.Union) != 2 || s.UnionOps[0] != "ALL" || s.UnionOps[1] != "DISTINCT" {
		t.Fatalf("union: %+v %v", s.Union, s.UnionOps)
	}
	if len(s.Settings) != 1 || s.Union[1].Settings != nil {
		t.Errorf("settings not hoisted to head: head=%v last=%v", s.Settings, s.Union[1].Settings)
	}
	if s.Format != "CSV" {
		t.Errorf("format %q", s.Format)
	}

	s = mustSelect(t, "(SELECT 1) UNION ALL (SELECT 2 UNION ALL SELECT 3)")
	if len(s.Union) != 1 || len(s.Union[0].Union) != 1 {
		t.Fatalf("nested union: %s", SelectString(s))
	}

	s = mustSelect(t, "SELECT * FROM (SELECT a FROM x UNION ALL SELECT a FROM y) WHERE a > 1")
	if len(s.From.Subquery.Union) != 1 || s.Where == nil {
		t.Fatalf("union in subquery: %s", SelectString(s))
	}

	s = mustSelect(t, "(SELECT 1)")
	assertAST(t, "parenthesized", s.Columns, []SelectItem{{Expr: u(1)}})

	expectErr(t, "SELECT 1 UNION SELECT 2", cherr.ExpectedAllOrDistinct, "Expected ALL or DISTINCT")
	expectErr(t, "SELECT 1 EXCEPT SELECT 2", cherr.NotImplemented, "EXCEPT")
	expectErr(t, "SELECT 1 INTERSECT SELECT 2", cherr.NotImplemented, "INTERSECT")
}

func TestSelectFromForms(t *testing.T) {
	tests := []struct {
		sql  string
		want *TableExpr
	}{
		{"SELECT 1 FROM t", &TableExpr{Table: "t"}},
		{"SELECT 1 FROM db.t AS x", &TableExpr{Database: "db", Table: "t", Alias: "x"}},
		{"SELECT 1 FROM db.t x FINAL", &TableExpr{Database: "db", Table: "t", Alias: "x", Final: true}},
		{"SELECT 1 FROM `my db`.`my t`", &TableExpr{Database: "my db", Table: "my t"}},
		{"SELECT 1 FROM system.numbers", &TableExpr{Database: "system", Table: "numbers"}},
		{"SELECT number FROM numbers(10)", &TableExpr{Func: fn("numbers", u(10))}},
		{"SELECT number FROM numbers(1, 10) n", &TableExpr{Func: fn("numbers", u(1), u(10)), Alias: "n"}},
	}
	for _, tt := range tests {
		assertAST(t, tt.sql, mustSelect(t, tt.sql).From, tt.want)
	}
	if s := mustSelect(t, "SELECT 1"); s.From != nil {
		t.Error("SELECT without FROM has From")
	}
	expectErr(t, "SELECT * FROM t FINAL AS x", cherr.SyntaxError, "AS")
	expectErr(t, "SELECT * FROM t SAMPLE 0.1", cherr.NotImplemented, "SAMPLE")
	expectErr(t, "SELECT * FROM db.f(1)", cherr.NotImplemented, "table functions")
}

func TestSelectJoins(t *testing.T) {
	s := mustSelect(t, `SELECT * FROM a
		JOIN b ON a.id = b.id
		LEFT JOIN c USING (id)
		LEFT OUTER JOIN d USING id, k
		GLOBAL ANY RIGHT JOIN e ON 1
		ALL FULL OUTER JOIN f ON 1
		LEFT SEMI JOIN g ON 1
		ASOF LEFT JOIN h ON a.t >= h.t
		CROSS JOIN i, j`)
	want := []JoinClause{
		{Kind: "INNER", Table: &TableExpr{Table: "b"}, On: bin("=", id("a", "id"), id("b", "id"))},
		{Kind: "LEFT", Table: &TableExpr{Table: "c"}, Using: []string{"id"}},
		{Kind: "LEFT", Table: &TableExpr{Table: "d"}, Using: []string{"id", "k"}},
		{Kind: "RIGHT", Strictness: "ANY", Table: &TableExpr{Table: "e"}, On: u(1)},
		{Kind: "FULL", Strictness: "ALL", Table: &TableExpr{Table: "f"}, On: u(1)},
		{Kind: "LEFT", Strictness: "SEMI", Table: &TableExpr{Table: "g"}, On: u(1)},
		{Kind: "LEFT", Strictness: "ASOF", Table: &TableExpr{Table: "h"}, On: bin(">=", id("a", "t"), id("h", "t"))},
		{Kind: "CROSS", Table: &TableExpr{Table: "i"}},
		{Kind: "CROSS", Table: &TableExpr{Table: "j"}},
	}
	assertAST(t, "joins", s.Joins, want)
	expectErr(t, "SELECT * FROM a JOIN b", cherr.SyntaxError, "Expected ON or USING")
	expectErr(t, "SELECT * FROM a PASTE JOIN b", cherr.NotImplemented, "PASTE")
}

func TestSelectArrayJoin(t *testing.T) {
	s := mustSelect(t, "SELECT a, n FROM t LEFT ARRAY JOIN arr AS a, nums n WHERE a > 0")
	assertAST(t, "array join", s.ArrayJoin, &ArrayJoinClause{Left: true, Items: []SelectItem{
		{Expr: id("arr"), Alias: "a"}, {Expr: id("nums"), Alias: "n"}}})
	if s.Where == nil {
		t.Error("where lost")
	}
	s = mustSelect(t, "SELECT x FROM t ARRAY JOIN arr")
	assertAST(t, "array join", s.ArrayJoin, &ArrayJoinClause{Items: []SelectItem{{Expr: id("arr")}}})
	expectErr(t, "SELECT x FROM t ARRAY JOIN a ARRAY JOIN b", cherr.NotImplemented, "ARRAY JOIN")
}

func TestSelectItemsAndAliases(t *testing.T) {
	s := mustSelect(t, "SELECT a b, 1 + 2 AS `three`, f(x) AS \"f\", t.*, *, (x AS y) + 1 z FROM t")
	assertAST(t, "items", s.Columns, []SelectItem{
		{Expr: id("a"), Alias: "b"},
		{Expr: bin("+", u(1), u(2)), Alias: "three"},
		{Expr: fn("f", id("x")), Alias: "f"},
		{Expr: &Star{Table: "t"}},
		{Expr: &Star{}},
		{Expr: bin("+", &AliasExpr{X: id("x"), Alias: "y"}, u(1)), Alias: "z"},
	})
	// Keywords are not implicit aliases.
	s = mustSelect(t, "SELECT a FROM t WHERE b")
	if s.Columns[0].Alias != "" {
		t.Errorf("alias %q", s.Columns[0].Alias)
	}
	s = mustSelect(t, "SELECT a, b, FROM t WHERE x IN (1, 2,)")
	if len(s.Columns) != 2 {
		t.Errorf("trailing comma: %+v", s.Columns)
	}
	assertAST(t, "tuple trailing comma", s.Where, bin("IN", id("x"), &TupleExpr{Elems: []Expr{u(1), u(2)}}))
	if s := mustSelect(t, "SELECT 1, 2,"); len(s.Columns) != 2 {
		t.Errorf("trailing comma at end: %+v", s.Columns)
	}
	expectErr(t, "SELECT f(1,)", cherr.SyntaxError, "position 12")
	expectErr(t, "SELECT [1, 2,]", cherr.SyntaxError, "position 14")
	expectErr(t, "SELECT 1 + 2 AS x * 3", cherr.SyntaxError, "position 19")
	expectErr(t, "SELECT", cherr.SyntaxError, "end of query")
	expectErr(t, "SELECT DISTINCT ON (a) a FROM t", cherr.NotImplemented, "DISTINCT ON")
}

func TestSelectUnsupportedClauses(t *testing.T) {
	tests := map[string]string{
		"SELECT a FROM t GROUP BY a WITH ROLLUP":          "ROLLUP",
		"SELECT a FROM t GROUP BY ROLLUP(a)":              "ROLLUP",
		"SELECT a FROM t GROUP BY GROUPING SETS ((a))":    "GROUPING",
		"SELECT a FROM t ORDER BY a WITH FILL":            "WITH FILL",
		"SELECT a FROM t LIMIT 1 WITH TIES":               "WITH TIES",
		"SELECT a FROM t WINDOW w AS (ORDER BY a)":        "WINDOW",
		"SELECT a FROM t QUALIFY a > 1":                   "QUALIFY",
		"SELECT a FROM t INTO OUTFILE 'x'":                "OUTFILE",
		"WITH RECURSIVE x AS (SELECT 1) SELECT * FROM x":  "RECURSIVE",
		"SELECT a FROM t ORDER BY ALL":                    "ORDER BY ALL",
		"SELECT a FROM t OFFSET 1 FETCH FIRST 1 ROW ONLY": "FETCH",
	}
	for sql, want := range tests {
		expectErr(t, sql, cherr.NotImplemented, want)
	}
}

func TestSelectTrailing(t *testing.T) {
	for _, sql := range []string{
		"SELECT 1;",
		"SELECT 1 ;  ",
		"SELECT 1; -- done",
		"SELECT 1 /* c */ ;;",
		"  -- leading\nSELECT 1 # trailing comment",
		"SELECT 1 FORMAT JSON;",
		"SELECT 1 FORMAT JSON SETTINGS a = 1",
	} {
		mustParse(t, sql)
	}
	expectErr(t, "SELECT 1; SELECT 2", cherr.SyntaxError, "Multi-statements")
	expectErr(t, "SELECT 1 2", cherr.SyntaxError, "position 10")
}

func TestSubqueryWithFormatIsRejected(t *testing.T) {
	expectErr(t, "SELECT * FROM (SELECT 1 FORMAT JSON)", cherr.SyntaxError, "FORMAT")
}

// ClickHouse accepts keywords as bare column names in expressions.
func TestKeywordsAsIdentifiers(t *testing.T) {
	s := mustSelect(t, "SELECT end, order, values, key, value, date, timestamp, interval, final, format FROM t WHERE end > start AND order = 1 ORDER BY order LIMIT 1")
	want := []string{"end", "order", "values", "key", "value", "date", "timestamp", "interval", "final", "format"}
	if len(s.Columns) != len(want) {
		t.Fatalf("columns: %d", len(s.Columns))
	}
	for i, w := range want {
		assertAST(t, w, s.Columns[i].Expr, id(w))
	}
	assertAST(t, "where", s.Where, bin("AND", bin(">", id("end"), id("start")), bin("=", id("order"), u(1))))
	assertAST(t, "order by", s.OrderBy, []OrderItem{{Expr: id("order")}})
	for _, e := range []string{"end", "order", "from"} {
		if got := String(id(e)); got != "`"+e+"`" {
			t.Errorf("String(%s) = %s, want back-quoted", e, got)
		}
	}
	expectErr(t, "SELECT a FROM t WHERE distinct = 1", cherr.SyntaxError, "position 23")
}
