package exec

import (
	"context"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// doublingAliases returns "1 AS a0, a0+a0 AS a1, ..." up to a<n>; a<n>
// expands to 2^(n+1)-1 nodes.
func doublingAliases(n int) string {
	var b strings.Builder
	b.WriteString("1 AS a0")
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, ", a%d+a%d AS a%d", i-1, i-1, i)
	}
	return b.String()
}

// chainedAliases returns "WITH <first> AS c0, <step(1)> AS c1, ..." up to c<n>.
func chainedAliases(n int, first string, step func(i int) string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "WITH %s AS c0", first)
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, ", %s AS c%d", step(i), i)
	}
	return b.String()
}

// joinTerms returns n terms term(0..n-1) joined by op.
func joinTerms(n int, op string, term func(i int) string) string {
	terms := make([]string, n)
	for i := range terms {
		terms[i] = term(i)
	}
	return strings.Join(terms, " "+op+" ")
}

func TestAliasExpansionRejectsTooBigQueryTree(t *testing.T) {
	defer debug.SetMaxStack(debug.SetMaxStack(64 << 20))
	te := newTestEnv(t)
	te.expectErr("SELECT "+doublingAliases(19), cherr.BadArguments)
	te.expectErr("WITH "+doublingAliases(19)+" SELECT a19", cherr.BadArguments)
	_, err := te.run("SELECT " + doublingAliases(19))
	if err == nil || !strings.Contains(err.Error(), "Query tree is too big. Maximum: 500000") {
		t.Errorf("got %v, want ClickHouse's query tree size message", err)
	}
	// About 131k nodes for a16, 262k for the whole select list.
	te.expectRows("WITH "+doublingAliases(16)+" SELECT a16", "65536")
	_, err = te.run("SELECT " + doublingAliases(16))
	if err != nil {
		t.Errorf("select list a0..a16: %v", err)
	}
}

// Like ClickHouse, each expression may expand to MaxExpandedASTElements;
// the whole query has the larger maxQueryExpandedNodes.
func TestAliasExpansionBudgetIsPerExpression(t *testing.T) {
	defer debug.SetMaxStack(debug.SetMaxStack(64 << 20))
	te := newTestEnv(t)
	with := "WITH " + doublingAliases(16)
	te.expectRows(with+" SELECT a16, a16, a16, a16", "65536\t65536\t65536\t65536")
	te.expectRows(with+" SELECT a16 FROM numbers(1) WHERE a16 > 0 AND a16 > 1 ORDER BY a16", "65536")
	te.expectRows(with+" SELECT a16, a16, (SELECT a16), (SELECT a16)", "65536\t65536\t65536\t65536")
	te.expectRows(with+" SELECT * FROM (SELECT a16 AS x, a16 AS y) WHERE x IN (SELECT a16) AND y IN (SELECT a16)", "65536\t65536")
	// One expression with four copies of a16 is too big in ClickHouse too.
	te.expectErr(with+" SELECT a16 + a16 + a16 + a16", cherr.BadArguments)
	te.expectErr(with+" SELECT count() FROM numbers(1) WHERE a16 > 0 AND a16 > 1 AND a16 > 2 AND a16 > 3", cherr.BadArguments)
	te.expectErr(with+" SELECT number FROM numbers(1) GROUP BY number HAVING a16 + a16 + a16 + a16 > 0", cherr.BadArguments)
	// 30 copies of a16 take about 3.9M nodes.
	q := with + " SELECT " + joinTerms(30, ",", func(int) string { return "a16" })
	te.expectErr(q, cherr.BadArguments)
	_, err := te.run(q)
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("Query tree is too big. Maximum: %d", maxQueryExpandedNodes)) {
		t.Errorf("got %v, want the query-wide size message", err)
	}
}

func TestAliasExpansionDepth(t *testing.T) {
	defer debug.SetMaxStack(debug.SetMaxStack(64 << 20))
	te := newTestEnv(t)
	plusOne := func(i int) string { return fmt.Sprintf("c%d+1", i-1) }
	te.expectRows(chainedAliases(570, "0", plusOne)+" SELECT c570", "570")
	if _, err := te.run("SELECT 0 AS c0" + strings.TrimPrefix(chainedAliases(570, "0", plusOne), "WITH 0 AS c0")); err != nil {
		t.Errorf("select list chain c0..c570: %v", err)
	}
	te.expectErr(chainedAliases(1500, "0", plusOne)+" SELECT c1500", cherr.TooDeepRecursion)
	_, err := te.run(chainedAliases(1500, "0", plusOne) + " SELECT c1500")
	if err == nil || !strings.Contains(err.Error(), "Maximum expression depth after alias expansion (1000) exceeded") {
		t.Errorf("got %v, want the expansion depth message", err)
	}
	orStep := func(i int) string { return fmt.Sprintf("c%d OR number = %d", i-1, i) }
	te.expectRows(chainedAliases(560, "number = 0", orStep)+" SELECT count() FROM numbers(3000) WHERE c560", "561")
	te.expectErr(chainedAliases(5000, "number = 0", orStep)+" SELECT count() FROM numbers(1) WHERE c5000", cherr.TooDeepRecursion)
	te.expectErr(chainedAliases(5000, "number", orStep)+" SELECT count() FROM numbers(1) GROUP BY number HAVING c5000", cherr.TooDeepRecursion)
	concatStep := func(i int) string { return fmt.Sprintf("c%d || 'a'", i-1) }
	te.expectRows(chainedAliases(560, "'a'", concatStep)+" SELECT length(c560)", "561")
	te.expectErr(chainedAliases(5000, "'a'", concatStep)+" SELECT length(c5000)", cherr.TooDeepRecursion)
}

func TestAliasExpansionNesting(t *testing.T) {
	defer debug.SetMaxStack(debug.SetMaxStack(64 << 20))
	te := newTestEnv(t)
	rename := func(i int) string { return fmt.Sprintf("c%d", i-1) }
	te.expectRows(chainedAliases(6000, "number", rename)+" SELECT c6000 FROM numbers(1)", "0")
	q := chainedAliases(maxAliasNesting, "number", rename) + fmt.Sprintf(" SELECT c%d FROM numbers(1)", maxAliasNesting)
	te.expectErr(q, cherr.TooDeepRecursion)
	_, err := te.run(q)
	if err == nil || !strings.Contains(err.Error(), "Maximum alias nesting (10000) exceeded") {
		t.Errorf("got %v, want the alias nesting message", err)
	}
}

func TestAliasExpansionCountsWrittenOutChainsAsOneLevel(t *testing.T) {
	defer debug.SetMaxStack(debug.SetMaxStack(64 << 20))
	te := newTestEnv(t)
	eq := func(i int) string { return fmt.Sprintf("number = %d", i) }
	ne := func(i int) string { return fmt.Sprintf("number != %d", i) }
	a := func(int) string { return "'a'" }
	te.expectRows("SELECT count() FROM numbers(3000) WHERE "+joinTerms(1500, "OR", eq), "1500")
	te.expectRows("SELECT count() FROM numbers(3000) WHERE "+joinTerms(1500, "AND", ne), "1500")
	te.expectRows("SELECT length("+joinTerms(1500, "||", a)+")", "1500")
	te.expectRows("WITH "+joinTerms(1500, "OR", eq)+" AS c SELECT count() FROM numbers(3000) WHERE c", "1500")
}

func TestAliasExpansionCountsConstantCollectionAsOneNode(t *testing.T) {
	defer debug.SetMaxStack(debug.SetMaxStack(64 << 20))
	te := newTestEnv(t)
	ids := joinTerms(10000, ",", strconv.Itoa)
	has := joinTerms(60, "+", func(int) string { return "has(ids, number)" })
	te.expectRows("WITH ["+ids+"] AS ids SELECT sum("+has+") FROM numbers(10)", "600")
	in := joinTerms(40, "OR", func(int) string { return "number IN s" })
	te.expectRows("WITH ("+ids+") AS s SELECT count() FROM numbers(10) WHERE "+in, "10")
	// Counting the elements would take the query past the node budget.
	with := "WITH [" + joinTerms(40000, ",", strconv.Itoa) + "] AS ids, " + doublingAliases(16)
	has = joinTerms(7, "AND", func(int) string { return "has(ids, number)" })
	te.expectRows(with+" SELECT count() FROM numbers(10) WHERE "+has+" AND a16 > 0", "10")
}

func TestPositionalArgumentsCountAgainstQueryBudget(t *testing.T) {
	defer debug.SetMaxStack(debug.SetMaxStack(64 << 20))
	te := newTestEnv(t)
	sel := "WITH " + doublingAliases(16) + " SELECT a16 FROM numbers(1) "
	ones := joinTerms(30, ",", func(int) string { return "1" })
	for _, clause := range []string{"ORDER BY 1, 1, 1, 1, 1", "GROUP BY 1, 1, 1, 1, 1", "LIMIT 1 BY 1, 1, 1, 1, 1"} {
		te.expectRows(sel+clause, "65536")
	}
	for _, clause := range []string{"ORDER BY ", "GROUP BY ", "LIMIT 1 BY "} {
		te.expectErr(sel+clause+ones, cherr.BadArguments)
	}
	// GROUP BY ALL repeats the 12 select expressions.
	items := joinTerms(12, ",", func(i int) string { return fmt.Sprintf("a16 + %d", i) })
	te.expectRows("WITH "+doublingAliases(16)+" SELECT count() FROM (SELECT "+items+" FROM numbers(1))", "1")
	te.expectErr("WITH "+doublingAliases(16)+" SELECT "+items+" FROM numbers(1) GROUP BY ALL", cherr.BadArguments)
}

func TestCTEReferencesShareASTBudget(t *testing.T) {
	defer debug.SetMaxStack(debug.SetMaxStack(64 << 20))
	te := newTestEnv(t)
	cte := func(n int) string {
		var b strings.Builder
		b.WriteString("WITH t0 AS (SELECT 1 AS x)")
		for i := 1; i <= n; i++ {
			fmt.Fprintf(&b, ", t%d AS (SELECT x FROM t%d UNION ALL SELECT x FROM t%d)", i, i-1, i-1)
		}
		fmt.Fprintf(&b, " SELECT count() FROM t%d", n)
		return b.String()
	}
	te.expectRows(cte(10), "1024")
	// 2^17 planned SELECTs.
	te.expectErr(cte(16), cherr.BadArguments)
}

// wideTable creates an empty MergeTree table name with n UInt8 columns.
func wideTable(te *testEnv, name string, n int) {
	cols := make([]string, n)
	for i := range cols {
		cols[i] = fmt.Sprintf("c%d UInt8", i)
	}
	te.exec("CREATE TABLE " + name + " (" + strings.Join(cols, ", ") + ") ENGINE = MergeTree ORDER BY c0")
}

// nodesTaken returns how much of the query budget planning sql takes.
func nodesTaken(t *testing.T, te *testEnv, sql string) int {
	t.Helper()
	sel, err := parseSelect(sql)
	if err != nil {
		t.Fatal(err)
	}
	q := te.e.newQuery(context.Background(), &Request{})
	if _, err := q.planSelect(sel, &planEnv{}); err != nil {
		t.Fatal(err)
	}
	return maxQueryExpandedNodes - q.plan.nodesLeft
}

// Each frame copied from a CTE or view holds a scope of its source's
// columns, so it takes the source's width from the query budget; other
// frames do not.
func TestCTEAndViewReferencesChargeSourceWidth(t *testing.T) {
	te := newTestEnv(t)
	wideTable(te, "w", 1000)
	te.exec("CREATE VIEW wv AS SELECT c0 FROM w")
	branches := func(from string) string {
		return "SELECT count() FROM (" + joinTerms(3, "UNION ALL", func(int) string { return "SELECT c0 FROM " + from }) + ")"
	}
	if n := nodesTaken(t, te, branches("w")); n >= 1000 {
		t.Errorf("three reads of w took %d nodes, want no width charge", n)
	}
	if n := nodesTaken(t, te, "SELECT count() FROM w WHERE c0 IN (SELECT c0 FROM w) OR c0 = (SELECT max(c0) FROM w)"); n >= 1000 {
		t.Errorf("subqueries over w took %d nodes, want no width charge", n)
	}
	for _, sql := range []string{"WITH t AS (SELECT c0 FROM w) " + branches("t"), branches("wv")} {
		if n := nodesTaken(t, te, sql); n < 3*1000 {
			t.Errorf("%s took %d nodes, want each of the three copies to take the width of w", sql, n)
		}
	}
	// A subquery inside a copied CTE is part of the copy.
	if n := nodesTaken(t, te, "WITH t AS (SELECT c0 FROM (SELECT c0, c1 FROM w)) "+branches("t")); n < 3*1000 {
		t.Errorf("copies of a CTE over a subquery took %d nodes, want the width of w each", n)
	}
}

// Queries ClickHouse 25.8 runs over wide tables: plain frames take nothing
// from the budget for their width, and CTE copies fit in it.
func TestWideTableFramesFitTheBudget(t *testing.T) {
	te := newTestEnv(t)
	wideTable(te, "w1", 1000)
	wideTable(te, "w3", 3000)
	read := func(table string) func(int) string {
		return func(int) string { return "SELECT c0 FROM " + table }
	}
	te.expectRows("SELECT count() FROM ("+joinTerms(600, "UNION ALL", read("w1"))+")", "0")
	te.expectRows("SELECT count() FROM ("+joinTerms(200, "UNION ALL", read("w3"))+")", "0")
	te.expectRows("SELECT count() FROM w3 WHERE "+joinTerms(200, "OR", func(int) string { return "c0 IN (SELECT c0 FROM w3)" }), "0")
	scalars := joinTerms(500, ",", func(int) string { return "(SELECT max(c0) FROM w1)" })
	te.expectRows("SELECT count() FROM (SELECT "+scalars+")", "1")
	te.expectRows("WITH t AS (SELECT c0 AS x FROM w3) SELECT count() FROM ("+joinTerms(200, "UNION ALL", func(int) string { return "SELECT x FROM t" })+")", "0")
}

func TestViewReferencesShareASTBudget(t *testing.T) {
	defer debug.SetMaxStack(debug.SetMaxStack(64 << 20))
	te := newTestEnv(t)
	te.exec("CREATE VIEW v0 AS SELECT 1 AS x")
	view := func(i int) string {
		return fmt.Sprintf("CREATE VIEW v%d AS SELECT x FROM v%d UNION ALL SELECT x FROM v%d", i, i-1, i-1)
	}
	for i := 1; i <= 14; i++ {
		te.exec(view(i))
	}
	te.expectRows("SELECT count() FROM v14", "16384")
	te.expectErr(view(15), cherr.BadArguments)
}

func TestASTBudgetResetsPerTopLevelSelect(t *testing.T) {
	defer debug.SetMaxStack(debug.SetMaxStack(64 << 20))
	te := newTestEnv(t)
	sel, err := parseSelect("WITH " + doublingAliases(16) + " SELECT a16")
	if err != nil {
		t.Fatal(err)
	}
	// Materialized views replan their SELECT per batch with one query.
	q := te.e.newQuery(context.Background(), &Request{})
	for range 3 {
		if _, err := q.planSelect(sel, &planEnv{}); err != nil {
			t.Fatal(err)
		}
	}
}
