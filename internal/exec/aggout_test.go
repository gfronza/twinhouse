package exec

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"testing"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec/aggstate"
	"github.com/gfronza/twinhouse/internal/sqlparse"
)

// eagerAggregationOracle is runAggregation before lazy output: every
// group's results are computed into a second slice of rows up front.
func eagerAggregationOracle(q *query, it rowIter, ac *aggContext) ([][]any, error) {
	groups := map[string]*group{}
	var order []*group
	newGroup := func(keys []any) *group {
		g := &group{keys: keys, accs: make([]*acc, len(ac.calls))}
		for i, c := range ac.calls {
			g.accs[i] = c.newAcc()
		}
		order = append(order, g)
		return g
	}
	var kb []byte
	for n := 0; ; n++ {
		if n&1023 == 0 {
			if err := q.ctx.Err(); err != nil {
				return nil, cherr.Wrap(cherr.Timeout, err, "query cancelled")
			}
		}
		row, err := it.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		keys := make([]any, len(ac.keys))
		kb = kb[:0]
		for i, k := range ac.keys {
			if keys[i], err = k.eval(row); err != nil {
				return nil, err
			}
			kb = aggstate.AppendKey(kb, keys[i])
		}
		g, ok := groups[string(kb)]
		if !ok {
			g = newGroup(keys)
			groups[string(kb)] = g
		}
		for i, c := range ac.calls {
			if err := c.feed(g.accs[i], row); err != nil {
				return nil, err
			}
		}
	}
	if len(order) == 0 && len(ac.keys) == 0 {
		newGroup(nil)
	}
	rows := make([][]any, len(order))
	for i, g := range order {
		row := make([]any, len(ac.keys)+len(ac.calls))
		copy(row, g.keys)
		for j, c := range ac.calls {
			v, err := c.result(g.accs[j])
			if err != nil {
				return nil, err
			}
			row[len(ac.keys)+j] = v
		}
		rows[i] = row
	}
	return rows, nil
}

// compileAggregation compiles GROUP BY keys and aggregate calls over a
// scope of (a Int64, b Nullable(Int64), s String), as planSingle does.
func compileAggregation(t *testing.T, q *query, keys, calls []string) *aggContext {
	t.Helper()
	sc := &scope{cols: []scopeCol{
		{name: "a", t: chtype.MustParse("Int64")},
		{name: "b", t: chtype.MustParse("Nullable(Int64)")},
		{name: "s", t: chtype.MustParse("String")},
	}}
	base := &compiler{q: q, env: &planEnv{}, sc: sc, used: map[int]bool{}, clause: "GROUP BY"}
	ac := newAggContext()
	for _, k := range keys {
		e, err := sqlparse.ParseExpr(k)
		if err != nil {
			t.Fatal(err)
		}
		x, err := base.child().compile(e)
		if err != nil {
			t.Fatal(err)
		}
		ac.keys = append(ac.keys, x)
	}
	proj := &compiler{q: q, env: base.env, sc: sc, used: base.used, agg: ac}
	for _, c := range calls {
		e, err := sqlparse.ParseExpr(c)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := proj.compile(e); err != nil {
			t.Fatal(err)
		}
	}
	return ac
}

func drainRows(t *testing.T, it rowIter) [][]any {
	t.Helper()
	var rows [][]any
	for {
		row, err := it.next()
		if err == io.EOF {
			return rows
		}
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
}

// rowsKey encodes rows so NaN and nested values compare by content.
func rowsKey(rows [][]any) string {
	var b []byte
	for _, row := range rows {
		for _, v := range row {
			b = aggstate.AppendKey(b, v)
		}
		b = append(b, '\n')
	}
	return string(b)
}

func TestLazyAggregationMatchesEager(t *testing.T) {
	te := newTestEnv(t)
	q := te.e.newQuery(context.Background(), &Request{})
	calls := []string{"count()", "sum(b)", "avg(b)", "min(s)", "argMax(s, b)", "uniqExact(s)", "groupArray(b)", "groupUniqArray(s)", "countIf(b > 0)"}
	r := rand.New(rand.NewSource(2))
	for iter := 0; iter < 300; iter++ {
		keys := [][]string{nil, {"a"}, {"s"}, {"a", "s"}, {"a % 2", "b"}}[r.Intn(5)]
		n := r.Intn(80)
		if iter%10 == 0 {
			n = 0
		}
		rows := make([][]any, n)
		for i := range rows {
			var b any = int64(r.Intn(7) - 3)
			if r.Intn(5) == 0 {
				b = nil
			}
			rows[i] = []any{int64(r.Intn(4)), b, []string{"x", "y", "z", ""}[r.Intn(4)]}
		}
		want, err := eagerAggregationOracle(q, &sliceIter{rows: rows}, compileAggregation(t, q, keys, calls))
		if err != nil {
			t.Fatal(err)
		}
		out, err := runAggregation(q, &sliceIter{rows: rows}, compileAggregation(t, q, keys, calls))
		if err != nil {
			t.Fatal(err)
		}
		got := drainRows(t, out)
		if rowsKey(got) != rowsKey(want) {
			t.Fatalf("GROUP BY %v over %v:\n got %v\nwant %v", keys, rows, got, want)
		}
	}
}

// A group whose stored members fail to load fails the aggregation before
// any row is out, also when that group lies past the LIMIT.
func TestMemberLoadErrorFailsBeforeAnyRow(t *testing.T) {
	te := newTestEnv(t)
	q := te.e.newQuery(context.Background(), &Request{})
	ac := compileAggregation(t, q, []string{"a"}, nil)
	merge := &aggCall{spec: aggSpec{base: "groupUniqArray", merge: true}, name: "groupUniqArrayMerge",
		args: []*expr{{fn: func(row []any) (any, error) { return row[1], nil }}}, ctx: q.ctx}
	ac.calls = []*aggCall{merge}
	loadErr := cherr.New(cherr.LogicalError, "member load failed")
	stored := func(m ...any) *chtype.AggPartial {
		p := aggstate.New("groupUniqArray", nil)
		p.Set = m
		return p
	}
	failing := &chtype.AggPartial{Func: "groupUniqArray", SetSize: 2,
		LoadSet: func(context.Context) ([]any, error) { return nil, loadErr }}
	input := [][]any{{int64(1), stored("x")}, {int64(2), failing}}
	if _, err := runAggregation(q, &sliceIter{rows: input}, ac); !errors.Is(err, loadErr) {
		t.Fatalf("got %v, want the member load error", err)
	}
}

// Without ORDER BY the rows past the LIMIT are not evaluated, so an error
// there does not fail the query. rows_before_limit is the group count
// (10, as ClickHouse 25.8 reports); after HAVING the rows past the LIMIT
// are counted, and an error there stops the count.
func TestErrorPastLimitWithoutOrderByIsNotRaised(t *testing.T) {
	te := newTestEnv(t)
	for _, c := range []struct {
		sql, want string
		before    uint64
	}{
		{`SELECT number AS n, intDiv(10, toInt64(n) - 5) FROM numbers(10) GROUP BY n LIMIT 2`, "0\t-2\n1\t-2", 10},
		{`SELECT number AS n FROM numbers(10) GROUP BY n HAVING intDiv(10, toInt64(n) - 5) > -100 LIMIT 2`, "0\n1", 5},
	} {
		te.expectRows(c.sql, c.want)
		if res := te.exec(c.sql); res.RowsBeforeLimit != c.before {
			t.Errorf("%s: rows_before_limit %d, want %d", c.sql, res.RowsBeforeLimit, c.before)
		}
	}
}

// With ORDER BY every row is evaluated before the LIMIT applies.
func TestErrorPastLimitWithOrderByFails(t *testing.T) {
	te := newTestEnv(t)
	te.expectErr(`SELECT number AS n, intDiv(10, toInt64(n) - 5) FROM numbers(10) GROUP BY n ORDER BY n LIMIT 2`, cherr.IllegalDivision)
	te.expectErr(`SELECT number AS n, intDiv(10, toInt64(n) - 5) FROM numbers(10) ORDER BY n LIMIT 2`, cherr.IllegalDivision)
}

func TestLazyAggregationThroughEngine(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (k String, v Int64) ENGINE = MergeTree ORDER BY k`)
	te.exec(`INSERT INTO t VALUES ('b', 1), ('a', 2), ('b', 3), ('c', -1), ('a', 5)`)
	for _, c := range []struct {
		sql, want string
		before    uint64
	}{
		{`SELECT k, sum(v) FROM t GROUP BY k`, "a\t7\nb\t4\nc\t-1", 0},
		{`SELECT k, sum(v) AS s FROM t GROUP BY k HAVING s > 0 LIMIT 1`, "a\t7", 2},
		{`SELECT k, sum(v) FROM t GROUP BY k LIMIT 1 OFFSET 1`, "b\t4", 3},
		{`SELECT k, sum(v) FROM t GROUP BY k LIMIT 0`, "", 3},
		{`SELECT count(), sum(v) FROM t WHERE v > 100`, "0\t0", 0},
		{`SELECT k, count() FROM t WHERE v > 100 GROUP BY k`, "", 0},
		{`SELECT count() FROM t WHERE v > 100 LIMIT 1`, "0", 1},
		{`SELECT k FROM t GROUP BY k ORDER BY max(v) DESC LIMIT 2`, "a\nb", 3},
	} {
		te.expectRows(c.sql, c.want)
		if res := te.exec(c.sql); res.RowsBeforeLimit != c.before {
			t.Errorf("%s: rows_before_limit %d, want %d", c.sql, res.RowsBeforeLimit, c.before)
		}
	}
}
