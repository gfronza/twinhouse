package exec

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/sqlparse"
)

func TestScalarSubqueryStopsAtSecondRow(t *testing.T) {
	guardFunctionTest(t)
	te := newTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := te.e.Execute(ctx, &Request{Query: `SELECT (SELECT number FROM system.numbers)`})
	if !cherr.Is(err, cherr.IncorrectResultOfScalarSubquery) {
		t.Fatalf("got %v, want code %d", err, cherr.IncorrectResultOfScalarSubquery)
	}
	if !strings.Contains(err.Error(), "Scalar subquery returned more than one row") {
		t.Errorf("message: %v", err)
	}
}

func TestScalarSubqueryKeyPushdownRunsOnce(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (k String, n UInt64) ENGINE = MergeTree ORDER BY k`)
	te.exec(`INSERT INTO t VALUES ('a', 1), ('b', 2)`)
	before := len(te.spy.scans)
	te.expectRows(`SELECT n FROM t WHERE k = (SELECT k FROM t WHERE n = 2)`, "2")
	// One subquery scan plus the outer scan, which is narrowed to 'b'.
	eq(t, len(te.spy.scans)-before, 2)
	if r := te.spy.lastScan().Ranges; len(r) != 1 || r[0].Partition[0] != "b" {
		t.Errorf("outer scan not narrowed: %s", fmtRanges(r))
	}
}

func TestScalarAndInShareOneSubqueryRun(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (k String, n UInt64) ENGINE = MergeTree ORDER BY k`)
	te.exec(`INSERT INTO t VALUES ('a', 1), ('b', 2)`)
	for _, q := range []string{
		`WITH (SELECT k FROM t WHERE n = 2) AS s SELECT n FROM t WHERE k = s AND k IN s`,
		`WITH (SELECT k FROM t WHERE n = 2) AS s SELECT n FROM t WHERE k IN s AND k = s`,
	} {
		before := len(te.spy.scans)
		te.expectRows(q, "2")
		if n := len(te.spy.scans) - before; n != 2 {
			t.Errorf("%s: %d scans, want 2", q, n)
		}
	}
	// With two rows, IN alone sees both and the shared scalar fails with 125.
	te.expectRows(`WITH (SELECT k FROM t) AS s SELECT n FROM t WHERE k IN s ORDER BY n`, "1\n2")
	te.expectErr(`WITH (SELECT k FROM t) AS s SELECT n FROM t WHERE k = s AND k IN s`, cherr.IncorrectResultOfScalarSubquery)
}

// SQL cannot reach a full read after a failed scalar (the 125 ends the
// query), so this drives query.subquery through its two consumers directly.
func TestSubqueryCachesOnlyCompleteResults(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (k String) ENGINE = MergeTree ORDER BY k`)
	te.exec(`INSERT INTO t VALUES ('a'), ('b'), ('c')`)
	sub := func(sql string) *sqlparse.SubqueryExpr {
		sel, err := parseSelect(sql)
		if err != nil {
			t.Fatal(err)
		}
		return &sqlparse.SubqueryExpr{Select: sel}
	}
	newCompiler := func() *compiler {
		return &compiler{q: te.e.newQuery(context.Background(), &Request{}), env: &planEnv{}}
	}
	scans := func(f func()) int {
		before := len(te.spy.scans)
		f()
		return len(te.spy.scans) - before
	}

	// A scalar read stops at two rows; IN then reruns it and sees all three.
	c, all := newCompiler(), sub(`SELECT k FROM t`)
	if _, err := c.scalarSubquery(all.Select); !cherr.Is(err, cherr.IncorrectResultOfScalarSubquery) {
		t.Fatalf("scalar: got %v, want code %d", err, cherr.IncorrectResultOfScalarSubquery)
	}
	var vals []any
	if n := scans(func() {
		var err error
		if vals, err = c.inValues(chtype.TString, all); err != nil {
			t.Fatal(err)
		}
	}); n != 1 {
		t.Errorf("IN after a truncated scalar: %d scans, want 1", n)
	}
	slices.SortFunc(vals, func(a, b any) int { return strings.Compare(a.(string), b.(string)) })
	eq(t, vals, []any{"a", "b", "c"})

	// A complete scalar result (one row) is reused by IN, and a complete IN
	// result by a scalar.
	for _, scalarFirst := range []bool{true, false} {
		c, one := newCompiler(), sub(`SELECT k FROM t WHERE k = 'b'`)
		scalar := func() {
			if _, err := c.scalarSubquery(one.Select); err != nil {
				t.Fatal(err)
			}
		}
		in := func() {
			if vals, err := c.inValues(chtype.TString, one); err != nil || len(vals) != 1 {
				t.Fatalf("IN: %v, %v", vals, err)
			}
		}
		first, second := in, scalar
		if scalarFirst {
			first, second = scalar, in
		}
		if n := scans(func() { first(); second() }); n != 1 {
			t.Errorf("scalar first %v: %d scans, want 1", scalarFirst, n)
		}
	}
}

func TestScalarSubqueryEmptyAndSingleRow(t *testing.T) {
	te := newTestEnv(t)
	te.expectRows(`SELECT (SELECT number FROM numbers(0)), toTypeName((SELECT number FROM numbers(0)))`, `\N	Nullable(UInt64)`)
	te.expectRows(`SELECT (SELECT number + 5 FROM numbers(1)), toTypeName((SELECT number + 5 FROM numbers(1)))`, "5\tNullable(UInt64)")
	te.expectRows(`SELECT (SELECT number, 'x' FROM numbers(3) WHERE number = 2)`, "(2,'x')")
	te.expectErr(`SELECT (SELECT number FROM numbers(2))`, cherr.IncorrectResultOfScalarSubquery)
	te.expectErr(`SELECT (SELECT number, 'x' FROM numbers(0))`, cherr.IncorrectResultOfScalarSubquery)
	te.expectErr(`SELECT (SELECT [1] WHERE 0)`, cherr.IncorrectResultOfScalarSubquery)
}

func TestScalarSubqueryErrorPrecedence(t *testing.T) {
	te := newTestEnv(t)
	for _, c := range []struct {
		sql  string
		code int
	}{
		// An error within the first block wins over "more than one row".
		{`SELECT (SELECT intDiv(1, number) FROM numbers(10))`, illegalDivision},
		{`SELECT (SELECT intDiv(1, number - 5) FROM numbers(10))`, illegalDivision},
		{`SELECT number FROM numbers(3) WHERE number = (SELECT intDiv(10, number - 2) FROM numbers(5))`, illegalDivision},
		// Past it the read stops and the second row is reported.
		{`SELECT (SELECT intDiv(1, number - 70000) FROM numbers(100000))`, cherr.IncorrectResultOfScalarSubquery},
	} {
		te.expectErr(c.sql, c.code)
	}
}

func TestScalarSubqueryDefaultReadsWholeSmallSource(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE one (x UInt64) ENGINE = MergeTree ORDER BY x`)
	te.exec(`INSERT INTO one VALUES (7)`)
	te.exec(`CREATE TABLE d (id UInt64, b Nullable(UInt64) DEFAULT (SELECT x FROM one)) ENGINE = MergeTree ORDER BY id`)
	te.exec(`INSERT INTO d (id) VALUES (1)`)
	te.exec(`ALTER TABLE d ADD COLUMN c Nullable(UInt64) DEFAULT (SELECT x FROM one)`)
	te.exec(`INSERT INTO one SELECT number + 100 FROM numbers(1000)`)
	// Both DEFAULTs now fail with 125 and are skipped, but each subquery still
	// reads all of one: 1 row of d plus 2 × 1001 rows of one.
	res := te.exec(`SELECT * FROM d`)
	eq(t, res.Rows, [][]any{{uint64(1), uint64(7), nil}})
	eq(t, res.ReadRows, uint64(2003))
}
