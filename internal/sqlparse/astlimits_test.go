package sqlparse

import (
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// boundStack makes a stack overflow fail fast and cheaply should a limit
// regress.
func boundStack(t *testing.T) {
	old := debug.SetMaxStack(64 << 20)
	t.Cleanup(func() { debug.SetMaxStack(old) })
}

func chain(op string, n int, atom string) string {
	return strings.Repeat(atom+" "+op+" ", n-1) + atom
}

func list(n int, item string) string {
	return strings.Repeat(item+", ", n-1) + item
}

// expectLimit is expectErr for huge queries: it does not print them.
func expectLimit(t *testing.T, sql string, code int, want string) {
	t.Helper()
	_, err := Parse(sql)
	ce, ok := err.(*cherr.Error)
	if !ok || ce.Code != code || !strings.Contains(ce.Msg, want) {
		t.Errorf("Parse(%s) = %v, want code %d %q", truncate(sql, 80), err, code, want)
	}
}

func expectParses(t *testing.T, sql string) Statement {
	t.Helper()
	st, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%s): %v", truncate(sql, 80), err)
	}
	return st
}

const (
	tooDeep = "AST is too deep. Maximum: 1000"
	tooBig  = "AST is too big. Maximum: 50000"
)

func TestLongChainsAreTooDeep(t *testing.T) {
	boundStack(t)
	const n = 5000
	for _, sql := range []string{
		"SELECT " + chain("+", n, "a"),
		"SELECT " + chain("*", n, "1"),
		"SELECT " + chain("=", n, "a"),
		"SELECT " + chain("LIKE", n, "a"),
		"SELECT " + chain("DIV", n, "a"),
		"SELECT " + chain("REGEXP", n, "a"),
		"SELECT a" + strings.Repeat(" BETWEEN 1 AND 2", n),
		"SELECT a" + strings.Repeat(" IS NULL", n),
		"SELECT x" + strings.Repeat("[1]", n),
		"SELECT 1" + strings.Repeat("::UInt8", n),
		"SELECT tuple(1)" + strings.Repeat(".1.1[1]", n),
		"SELECT tuple(1)" + strings.Repeat(".x", n),
		"SELECT -1" + strings.Repeat("[1]", n),
		"SELECT NOT(a)" + strings.Repeat("[1]", n),
		"SELECT position(" + chain("+", n, "a") + ", b)",
		// Chains that are each within the limit add up through nesting.
		"SELECT (" + chain("+", 600, "a") + ")" + strings.Repeat(" + a", 600),
		"SELECT f(" + chain("+", 600, "a") + ")" + strings.Repeat(" + a", 600),
		"SELECT [" + chain("+", 600, "a") + "]" + strings.Repeat(" + a", 600),
		"SELECT (SELECT " + chain("+", 600, "a") + ")" + strings.Repeat(" + a", 600),
		"SELECT (SELECT 1 FROM (SELECT " + chain("+", 600, "a") + "))" + strings.Repeat(" + a", 600),
		"SELECT exists(SELECT " + chain("+", 600, "a") + ")" + strings.Repeat(" + a", 600),
		"SELECT CASE WHEN " + chain("+", 600, "a") + " THEN 1 END" + strings.Repeat(" + a", 600),
		"SELECT arrayMap(x -> " + chain("+", 600, "a") + ", b)" + strings.Repeat(" + a", 600),
		"SELECT x[" + chain("+", 600, "a") + "]" + strings.Repeat("[1]", 600),
		"SELECT NOT " + chain("+", 600, "a") + strings.Repeat(" = a", 600),
		"SELECT -(" + chain("+", 600, "a") + ")" + strings.Repeat(" * a", 600),
		"SELECT " + chain("AND", 3, chain("+", n, "a")),
		"SELECT " + chain("AND", 3, "("+chain("+", 600, "a")+")"+strings.Repeat(" + a", 600)),
		"SELECT 1 WHERE " + chain("+", n, "a"),
		"CREATE TABLE t (a UInt8 DEFAULT " + chain("+", n, "1") + ") ENGINE = Memory",
		"INSERT INTO t SELECT " + chain("+", n, "a"),
		"EXPLAIN AST SELECT " + chain("+", n, "a"),
		"INSERT INTO t VALUES (" + chain("+", n, "1") + ")",
		"SELECT [1]" + strings.Repeat(" + 1", 1000),
	} {
		expectLimit(t, sql, cherr.TooDeepAST, tooDeep)
	}
	if _, err := ParseExpr(chain("-", n, "a")); err == nil || err.(*cherr.Error).Code != cherr.TooDeepAST {
		t.Errorf("ParseExpr of a long chain: %v", err)
	}
}

func TestChainsWithinDepthParse(t *testing.T) {
	boundStack(t)
	// One level per operator: a chain of 1000 operands is 1000 deep.
	expectParses(t, "SELECT "+chain("+", 1000, "a"))
	expectLimit(t, "SELECT "+chain("+", 1001, "a"), cherr.TooDeepAST, tooDeep)
	expectParses(t, "SELECT "+strings.Repeat("NOT ", 400)+"1")
	expectParses(t, "SELECT "+strings.Repeat("- ", 400)+"x")
	expectParses(t, "SELECT CASE "+strings.Repeat("WHEN a THEN 1 ", 3000)+"END")
	expectParses(t, "SELECT 1"+strings.Repeat(" UNION ALL SELECT 1", 3000))
	expectParses(t, "SELECT "+chain("AND", 3, chain("+", 900, "a")))
	expectParses(t, "INSERT INTO t VALUES "+list(3000, "("+chain("+", 900, "1")+")"))
	expectParses(t, "INSERT INTO t VALUES ("+chain("+", 1000, "1")+")")
	expectLimit(t, "INSERT INTO t VALUES ("+chain("+", 1001, "1")+")", cherr.TooDeepAST, tooDeep)
}

// A collection of literals is one literal node in ClickHouse, however deeply
// it nests, so it adds one level to the expression around it.
func TestLiteralCollectionsAreOneLevel(t *testing.T) {
	boundStack(t)
	nest := func(n int, open, mid, close string) string {
		return strings.Repeat(open, n) + mid + strings.Repeat(close, n)
	}
	for _, sql := range []string{
		"SELECT length(" + nest(900, "[", "1", "]") + ")" + strings.Repeat(" + 1", 300),
		"SELECT length(" + nest(700, "[", "1", "]") + ")" + strings.Repeat(" + 1", 450),
		"SELECT length(" + nest(900, "(1, ", "1", ")") + ")" + strings.Repeat(" + 1", 300),
		"SELECT [" + nest(600, "[", "1", "]") + ", [[1]]]" + strings.Repeat("[1]", 420),
		"INSERT INTO t VALUES (length(" + nest(900, "[", "1", "]") + ")" + strings.Repeat(" + 1", 300) + ")",
	} {
		expectParses(t, sql)
	}
	// A collection that is not all literals is a function over its elements.
	expectLimit(t, "SELECT length("+nest(900, "[", "a", "]")+")"+strings.Repeat(" + 1", 300), cherr.TooDeepAST, tooDeep)
}

// ClickHouse merges a chain of AND, OR or || into one function, so a long
// chain is neither too deep nor large, and later passes must handle it.
func TestMergeableChainsAreFlat(t *testing.T) {
	boundStack(t)
	// VALUES rows are not counted, so their chains are bounded by neither.
	for _, sql := range []string{
		"INSERT INTO t VALUES (" + chain("AND", 40000, "1") + ")",
		"INSERT INTO t VALUES (" + chain("OR", 20000, "0") + ")",
		"INSERT INTO t VALUES (" + chain("||", 20000, "'1'") + ")",
	} {
		expectParses(t, sql)
	}
	for _, sql := range []string{
		chain("AND", 40000, "a"),
		chain("OR", 40000, "a"),
		chain("||", 40000, "'a'"),
		chain("AND", 2000, "(a AND b)"),
		chain("OR", 2000, chain("+", 5, "a")),
	} {
		e, err := ParseExpr(sql)
		if err != nil {
			t.Fatalf("ParseExpr(%s): %v", truncate(sql, 40), err)
		}
		if name := ColumnName(e); !strings.HasSuffix(name, ")") {
			t.Errorf("ColumnName = %s", truncate(name, 40))
		}
		if _, err := ParseExpr(String(e)); err != nil {
			t.Errorf("re-render of %s: %v", truncate(sql, 40), err)
		}
	}
	if got := ColumnName(mustExpr(t, "a AND b AND (c AND d) AND e")); got != "and(a, b, and(c, d), e)" {
		t.Errorf("ColumnName = %s", got)
	}
}

func TestASTElementLimit(t *testing.T) {
	expectLimit(t, "SELECT "+list(50001, "a"), cherr.TooBigAST, tooBig)
	expectLimit(t, "SELECT "+list(50001, "1"), cherr.TooBigAST, tooBig)
	expectLimit(t, "SELECT f("+list(50001, "a")+")", cherr.TooBigAST, tooBig)
	expectLimit(t, "SELECT 1 IN ("+list(50001, "a")+")", cherr.TooBigAST, tooBig)
	expectLimit(t, "SELECT "+chain("AND", 50001, "a"), cherr.TooBigAST, tooBig)
	expectLimit(t, "SELECT ["+list(30000, "(1, [1])")+"]", cherr.TooBigAST, tooBig)
	expectLimit(t, "SELECT ["+list(30000, "-a")+"]", cherr.TooBigAST, tooBig)
	expectLimit(t, "SELECT 1 WHERE x IN ("+list(50001, "'a' || 'b'")+")", cherr.TooBigAST, tooBig)
	expectLimit(t, "INSERT INTO t SELECT "+list(50001, "a"), cherr.TooBigAST, tooBig)
	expectLimit(t, "INSERT INTO t SETTINGS a = ["+list(50001, "b")+"] VALUES (1)", cherr.TooBigAST, tooBig)
	expectParses(t, "SELECT "+list(49000, "a"))
	expectParses(t, "SELECT "+chain("AND", 49000, "a"))
	// A collection of literals is one literal node, as in ClickHouse.
	expectParses(t, "SELECT 1 IN ("+list(100000, "1")+")")
	expectParses(t, "SELECT 1 IN ("+list(100000, "-1")+")")
	expectParses(t, "SELECT 1 IN ("+list(100000, "(1)")+")")
	expectParses(t, "SELECT x IN ("+list(60000, "(1, 'a')")+")")
	expectParses(t, "SELECT ["+list(60000, "[NULL, true]")+"]")
	expectParses(t, "SELECT ["+list(60000, "'a'")+"]::Array(String)")
	if _, err := ParseExpr("[" + list(50001, "a") + "]"); err == nil || err.(*cherr.Error).Code != cherr.TooBigAST {
		t.Errorf("ParseExpr of a large array: %v", err)
	}
}

// INSERT ... VALUES rows are data, which ClickHouse does not parse as AST.
func TestValuesRowsAreNotCounted(t *testing.T) {
	expectParses(t, "INSERT INTO t VALUES "+list(60000, "(now(), 'x', a)"))
	expectParses(t, "INSERT INTO t VALUES (["+list(60000, "f(a)")+"])")
	expectParses(t, "INSERT INTO t VALUES ({"+list(60000, "'k': [1]")+"})")
	// The rows do not hide what the query around them counts.
	expectLimit(t, "INSERT INTO t SETTINGS a = ["+list(50001, "b")+"] VALUES "+list(10, "(1)"),
		cherr.TooBigAST, tooBig)
}

func TestOpenParensAreNotBuffered(t *testing.T) {
	sql := "SELECT " + strings.Repeat("(", 400000)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	expectLimit(t, sql, cherr.TooDeepRecursion, "Maximum parse depth (1000) exceeded")
	runtime.ReadMemStats(&after)
	if n := after.TotalAlloc - before.TotalAlloc; n > 16<<20 {
		t.Errorf("parsing %d bytes of '(' allocated %d bytes", len(sql), n)
	}
}

func TestNestedExplain(t *testing.T) {
	boundStack(t)
	expectErr(t, "EXPLAIN EXPLAIN SELECT 1", cherr.SyntaxError, "failed at position 17 (SELECT)")
	expectErr(t, "EXPLAIN PLAN EXPLAIN SELECT 1", cherr.SyntaxError, "Expected equals sign")
	expectErr(t, "EXPLAIN SYNTAX EXPLAIN AST SELECT 1", cherr.SyntaxError, "Expected equals sign")
	ex := mustParse(t, "EXPLAIN AST EXPLAIN SELECT 1").(*ExplainStmt)
	if inner, ok := ex.Stmt.(*ExplainStmt); !ok || inner.Kind != "" {
		t.Errorf("EXPLAIN AST EXPLAIN: inner %#v", ex.Stmt)
	}
	expectLimit(t, strings.Repeat("EXPLAIN AST ", 300000)+"SELECT 1", cherr.TooDeepRecursion,
		"Maximum parse depth (1000) exceeded. Consider rising max_parser_depth parameter.")
}

// TestDeepRecursionIsBounded covers every recursive path of the parser.
func TestDeepRecursionIsBounded(t *testing.T) {
	boundStack(t)
	const n = 100000
	nest := func(open, mid, close string) string {
		return strings.Repeat(open, n) + mid + strings.Repeat(close, n)
	}
	for _, sql := range []string{
		"SELECT " + nest("(", "1", ")"),
		"SELECT " + nest("f(", "1", ")"),
		"SELECT " + nest("[", "1", "]"),
		"SELECT " + nest("(1, ", "1", ")"),
		"SELECT " + nest("x[", "1", "]"),
		"SELECT " + strings.Repeat("NOT ", n) + "1",
		"SELECT " + strings.Repeat("- ", n) + "1",
		"SELECT " + strings.Repeat("+ ", n) + "1",
		"SELECT " + strings.Repeat("x -> ", n) + "1",
		"SELECT " + nest("CASE WHEN ", "1", " THEN 1 END"),
		"SELECT " + nest("CAST(", "1", " AS UInt8)"),
		"SELECT " + nest("(SELECT ", "1", ")"),
		"SELECT " + nest("exists(SELECT ", "1", ")"),
		"SELECT 1 FROM " + nest("(SELECT 1 FROM ", "t", ")"),
		nest("(", "SELECT 1", ")"),
		"SELECT 1 UNION ALL " + nest("(SELECT 1 UNION ALL ", "SELECT 1", ")"),
		"WITH " + nest("a AS (WITH ", "b AS (SELECT 1) SELECT 1", ") SELECT 1"),
		"INSERT INTO t " + nest("(SELECT * FROM ", "t", ")"),
		"INSERT INTO t VALUES (" + nest("(", "1", ")") + ")",
		"CREATE VIEW v AS SELECT " + nest("(", "1", ")"),
		"SELECT 1 SETTINGS a = " + nest("[", "1", "]"),
		"SELECT f(" + nest("x -> f(", "1", ")") + ")",
		"SELECT count() OVER (PARTITION BY " + nest("f(", "1", ")") + ")",
		"SELECT INTERVAL " + nest("(", "1", ")") + " DAY",
	} {
		expectLimit(t, sql, cherr.TooDeepRecursion, "Maximum parse depth (1000) exceeded")
	}
}

// TestNestingClickHouseAcceptsParses uses the deepest nestings ClickHouse
// 25.8 accepts with default settings.
func TestNestingClickHouseAcceptsParses(t *testing.T) {
	boundStack(t)
	nest := func(n int, open, mid, close string) string {
		return strings.Repeat(open, n) + mid + strings.Repeat(close, n)
	}
	for _, sql := range []string{
		"SELECT " + nest(983, "[", "1", "]"),
		"SELECT " + nest(983, "(1, ", "1", ")"),
		"SELECT " + nest(497, "1 + (", "1", ")"),
		"SELECT " + nest(497, "1 AND (", "1", ")"),
		"SELECT " + nest(497, "1 ? 1 : (", "1", ")"),
	} {
		expectParses(t, sql)
	}
}

// TestHeightsFollowTree checks the height the parser works out against the
// height of the tree it returns, for the expressions of the other tests.
func TestHeightsFollowTree(t *testing.T) {
	var exprs []string
	for _, c := range loadColnameCorpus(t) {
		exprs = append(exprs, c.expr)
	}
	exprs = append(exprs, roundTripExprs...)
	exprs = append(exprs, "a AND b AND (c OR d OR e) AND f", "x || y || z", "1::UInt8::String",
		"dateDiff(day, a, b)", "INTERVAL '1 day'", "DATE '2020-01-01'", "-1", "-x[1].1::String",
		"position(a IN b)", "position(a, b AS c)", "trim(BOTH 'x' FROM s)", "substring(s FROM 1 FOR 2)",
		"date_add(DAY, date_add(DAY, 1 + 1, d), d)", "exists(SELECT exists(SELECT 1 + 1))", "count(*) OVER (PARTITION BY a + 1 ORDER BY b)",
		"(SELECT a + 1 + 1 FROM (SELECT b + b + b + b))", "x -> x + 1 + 1", "NOT (a + 1)", "a ? b + 1 : c",
		"[1, 2, 3]::Array(UInt8) + 1", "((a + 1)) * 2", "CASE WHEN a + 1 THEN b ELSE c + 1 + 1 END")
	for _, s := range exprs {
		p := newParser(s)
		var e Expr
		if err := p.run(func() { e = p.parseExprAlias() }); err != nil {
			continue
		}
		// One more is allowed: count(*) drops the * it has counted.
		want := treeHeight(e)
		if p.height < want || p.height > want+1 {
			t.Errorf("%s: height %d, tree height %d", s, p.height, want)
		}
	}
}

// treeHeight returns the height of e. The expressions in a subquery are its
// children, and a collection of literals is one node. It finds children by reflection, so that it does not share a
// list of node fields with the parser.
func treeHeight(e Expr) int {
	if isArrayLiteral(e) || isTupleLiteral(e) {
		return 1
	}
	h := 0
	for _, c := range childExprs(e) {
		h = max(h, treeHeight(c))
	}
	if b, ok := e.(*BinaryExpr); ok && isMergedLink(b) {
		return max(treeHeight(b.Left), 1+treeHeight(b.Right))
	}
	if a, ok := e.(*AliasExpr); ok {
		return treeHeight(a.X)
	}
	return h + 1
}

func isMergedLink(b *BinaryExpr) bool {
	l, ok := b.Left.(*BinaryExpr)
	return ok && l.Op == b.Op && !l.Parens && (b.Op == "AND" || b.Op == "OR" || b.Op == "||")
}

func childExprs(e Expr) []Expr {
	var out []Expr
	exprType := reflect.TypeOf((*Expr)(nil)).Elem()
	var visit func(v reflect.Value)
	visit = func(v reflect.Value) {
		switch v.Kind() {
		case reflect.Interface:
			if !v.IsNil() && v.Type() == exprType {
				out = append(out, v.Interface().(Expr))
			}
		case reflect.Pointer:
			if !v.IsNil() {
				visit(v.Elem())
			}
		case reflect.Slice:
			for i := 0; i < v.Len(); i++ {
				visit(v.Index(i))
			}
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				visit(v.Field(i))
			}
		}
	}
	visit(reflect.ValueOf(e).Elem())
	return out
}
