package exec

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// joinOracleTables creates tables with NULL and duplicate keys (of
// different integer types, so keys are converted), plus an empty table.
func joinOracleTables(te *testEnv, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, 1))
	key := func() string {
		if rng.IntN(6) == 0 {
			return "NULL"
		}
		return fmt.Sprint(rng.IntN(8))
	}
	str := func() string { return fmt.Sprintf("'s%d'", rng.IntN(4)) }
	insert := func(table string, n int, row func(i int) string) {
		vals := make([]string, n)
		for i := range vals {
			vals[i] = row(i)
		}
		te.exec(fmt.Sprintf("INSERT INTO %s VALUES %s", table, strings.Join(vals, ", ")))
	}
	te.exec(`CREATE TABLE l (id UInt32, k Nullable(Int32), s String, v Int64) ENGINE = MergeTree ORDER BY id`)
	te.exec(`CREATE TABLE r (id UInt32, k Nullable(Int64), s String, w Float64) ENGINE = MergeTree ORDER BY id`)
	te.exec(`CREATE TABLE r2 (id UInt32, k Int32, x String) ENGINE = MergeTree ORDER BY id`)
	te.exec(`CREATE TABLE e (id UInt32, k Nullable(Int32), s String) ENGINE = MergeTree ORDER BY id`)
	insert("l", 30, func(i int) string { return fmt.Sprintf("(%d, %s, %s, %d)", i, key(), str(), rng.IntN(10)) })
	insert("r", 30, func(i int) string { return fmt.Sprintf("(%d, %s, %s, %d.5)", i, key(), str(), rng.IntN(10)) })
	insert("r2", 12, func(i int) string { return fmt.Sprintf("(%d, %d, %s)", i, rng.IntN(8), str()) })
}

// joinOracleQueries covers every join kind and strictness with USING, ON,
// expression keys, residual conditions, key-less joins, empty sides, LIMIT
// and several joins in one query.
func joinOracleQueries() []string {
	conds := []string{
		"ON l.k = r.k",
		"ON r.k = l.k",
		"ON l.k = r.k AND l.s = r.s",
		"ON l.k + 1 = r.k",
		"ON l.k = r.k AND l.v < r.w",
		"ON l.k = r.k AND (l.v > 100 OR r.s = 's1')",
		"ON l.v < r.w",
		"ON l.k = r.k OR l.s = r.s",
		"USING (k)",
		"USING (k, s)",
		// Raises an error on some candidates only.
		"ON l.k = r.k AND toInt32(if(r.s = 's3', 'x', '1')) = 1",
	}
	var qs []string
	for _, kind := range []string{"INNER", "LEFT", "RIGHT", "FULL"} {
		for _, strict := range []string{"", "ALL", "ANY", "SEMI", "ANTI"} {
			for _, c := range conds {
				j := strings.TrimSpace(strict + " " + kind)
				qs = append(qs,
					fmt.Sprintf("SELECT * FROM l %s JOIN r %s", j, c),
					fmt.Sprintf("SELECT l.id, r.id, k, l.s, r.w FROM l %s JOIN r %s LIMIT 7", j, c),
					fmt.Sprintf("SELECT count(), sum(l.v), sum(r.w) FROM l %s JOIN r %s", j, c),
					// Raises an error on some emitted rows only.
					fmt.Sprintf("SELECT intDiv(1, r.id %% 3) FROM l %s JOIN r %s LIMIT 1", j, c),
				)
			}
			qs = append(qs,
				fmt.Sprintf("SELECT * FROM l %s JOIN e ON l.k = e.k", strings.TrimSpace(strict+" "+kind)),
				fmt.Sprintf("SELECT * FROM e %s JOIN r ON e.k = r.k", strings.TrimSpace(strict+" "+kind)),
			)
		}
	}
	return append(qs,
		"SELECT * FROM l CROSS JOIN r",
		"SELECT * FROM l CROSS JOIN r LIMIT 5 OFFSET 40",
		"SELECT * FROM l CROSS JOIN r WHERE l.k = r.k",
		"SELECT * FROM l, r WHERE l.k = r.k AND l.v < r.w",
		"SELECT * FROM l CROSS JOIN e",
		"SELECT * FROM e CROSS JOIN r",
		"SELECT * FROM l JOIN r ON l.k = r.k LEFT JOIN r2 ON r.k = r2.k",
		"SELECT * FROM l FULL JOIN r ON l.k = r.k LEFT JOIN r2 ON l.k = r2.k AND r2.x <> l.s",
		"SELECT * FROM l LEFT JOIN r USING (k) RIGHT JOIN r2 ON l.k = r2.k",
		"SELECT * FROM l LEFT ANTI JOIN r ON l.k = r.k LEFT SEMI JOIN r2 ON l.k = r2.k",
		"SELECT * FROM l CROSS JOIN r2 FULL JOIN r ON r2.k = r.k",
		"SELECT * FROM l AS a JOIN l AS b ON a.k = b.k",
		"SELECT * FROM l JOIN (SELECT k, count() AS c FROM r GROUP BY k) AS g ON l.k = g.k ORDER BY l.id",
		"SELECT * FROM l JOIN r ON l.k = r.k SETTINGS join_use_nulls = 1",
		"SELECT * FROM l JOIN r ON l.k = r.s",
		"SELECT * FROM l JOIN r ON l.k = r.k AND l.s",
		"SELECT * FROM l ASOF JOIN r ON l.k = r.k AND l.v < r.w",
	)
}

// joinOutcome renders a query's result with its read and LIMIT stats, or
// its error.
func joinOutcome(te *testEnv, sql string) string {
	res, err := te.run(sql)
	if err != nil {
		return "error: " + err.Error()
	}
	return fmt.Sprintf("%sread %d rows, %d bytes; before limit %d (%v)\n",
		renderTSV(te.t, res), res.ReadRows, res.ReadBytes, res.RowsBeforeLimit, res.HasRowsBeforeLimit)
}

func TestJoinMatchesOldImplementation(t *testing.T) {
	queries := joinOracleQueries()
	for seed := uint64(1); seed <= 4; seed++ {
		te := newTestEnv(t)
		joinOracleTables(te, seed)
		for _, q := range queries {
			got := joinOutcome(te, q)
			startJoin = startOldJoin
			want := joinOutcome(te, q)
			startJoin = startHashJoin
			if got != want {
				t.Fatalf("seed %d: %s\n got: %s\nwant: %s", seed, q, got, want)
			}
		}
	}
}

func TestJoinResidualErrorComesBeforeRows(t *testing.T) {
	// A residual error on a later candidate of a left row wins over a LIMIT
	// already met and over an error on an earlier emitted row.
	te := newTestEnv(t)
	te.exec(`CREATE TABLE el (k UInt32, t String) ENGINE = MergeTree ORDER BY k`)
	te.exec(`INSERT INTO el VALUES (1, 'a')`)
	te.exec(`CREATE TABLE er (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id`)
	te.exec(`INSERT INTO er VALUES (1, 1), (2, 1), (3, 1), (4, 1)`)
	on := "ON el.k = er.k AND toInt32(if(er.id = 3, 'x', '1')) = 1"
	for _, kind := range []string{"INNER", "ALL INNER", "LEFT", "ALL LEFT", "RIGHT", "ALL RIGHT", "FULL", "ALL FULL"} {
		for _, sql := range []string{
			"SELECT er.id FROM el %s JOIN er %s LIMIT 1",
			"SELECT intDiv(1, er.id - 1) FROM el %s JOIN er %s",
			"SELECT sum(intDiv(1, er.id - 1)) FROM el %s JOIN er %s",
			"SELECT er.id FROM el %s JOIN er %s WHERE intDiv(1, er.id - 1) > 0",
		} {
			te.expectErr(fmt.Sprintf(sql, kind, on), cherr.CannotParseText)
		}
	}
}

func TestJoinOracleSeesEveryOutcome(t *testing.T) {
	// The differential test is only as good as its cases: they must produce
	// rows, empty results and errors.
	te := newTestEnv(t)
	joinOracleTables(te, 1)
	var rows, empty, errs int
	for _, q := range joinOracleQueries() {
		switch out := joinOutcome(te, q); {
		case strings.HasPrefix(out, "error: "):
			errs++
		case strings.Count(out, "\n") > 3: // names, types and stats lines
			rows++
		default:
			empty++
		}
	}
	if rows < 100 || empty < 10 || errs < 10 {
		t.Fatalf("rows %d, empty %d, errors %d", rows, empty, errs)
	}
}

func TestJoinProbeStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	j := &joinIter{q: &query{ctx: ctx}, spec: &joinSpec{kind: "INNER", leftW: 1, rightW: 1}, b: &joinBuild{},
		in: &sliceIter{rows: [][]any{{uint64(1)}}}, scratch: make([]any, 2)}
	for i := range 5000 {
		j.b.rows.add([]any{uint64(i)})
	}
	j.b.matched = make([]bool, j.b.rows.n)
	for range 1500 {
		if _, err := j.next(); err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	var err error
	for range 1024 {
		if _, err = j.next(); err != nil {
			break
		}
	}
	if err == nil || !strings.Contains(err.Error(), "query cancelled") {
		t.Fatalf("got %v, want a cancellation error", err)
	}
}

func TestChunkedRowsKeepsOrder(t *testing.T) {
	var c chunkedRows
	for i := range 3*chunkRows + 5 {
		c.add([]any{i})
	}
	if c.n != 3*chunkRows+5 || len(c.chunks) != 4 {
		t.Fatalf("n %d, chunks %d", c.n, len(c.chunks))
	}
	for i := range c.n {
		if c.at(i)[0] != i {
			t.Fatalf("row %d holds %v", i, c.at(i)[0])
		}
	}
}

// allocated returns the bytes allocated while running sql, and its rows.
func allocated(te *testEnv, sql string) (uint64, string) {
	te.t.Helper()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	out := te.rows(sql)
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc, out
}

func TestJoinProbeStreams(t *testing.T) {
	if testing.Short() {
		t.Skip("heavy")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	cases := []struct {
		join, right, want string
	}{
		{
			join:  `SELECT * FROM numbers(1000) AS a CROSS JOIN numbers(100000) AS b LIMIT 1`,
			right: `SELECT count() FROM numbers(100000)`,
			want:  "0\t0\n",
		},
		{
			join:  `SELECT * FROM numbers(1000) AS a JOIN (SELECT number, 0 AS k FROM numbers(3000000)) AS b ON a.number * 0 = b.k LIMIT 1`,
			right: `SELECT count() FROM (SELECT number, 0 AS k FROM numbers(3000000))`,
			want:  "0\t0\t0\n",
		},
	}
	for _, c := range cases {
		rightBytes, _ := allocated(te, c.right)
		start := time.Now()
		joinBytes, got := allocated(te, c.join)
		if got != c.want {
			t.Fatalf("%s: got %q, want %q", c.join, got, c.want)
		}
		t.Logf("%s: join %d B, right side %d B", c.join, joinBytes, rightBytes)
		if joinBytes > 3*rightBytes {
			t.Errorf("%s allocated %d B, over 3x the right side (%d B)", c.join, joinBytes, rightBytes)
		}
		if d := time.Since(start); d > 20*time.Second {
			t.Errorf("%s took %v", c.join, d)
		}
	}
}

func BenchmarkHashJoin(b *testing.B) {
	te := newTestEnv(b)
	te.exec(`CREATE TABLE jl (id UInt64, k UInt64, v UInt64) ENGINE = MergeTree ORDER BY id`)
	te.exec(`CREATE TABLE jr (id UInt64, k UInt64, s String) ENGINE = MergeTree ORDER BY id`)
	te.exec(`INSERT INTO jl SELECT number, number % 20000, number FROM numbers(100000)`)
	te.exec(`INSERT INTO jr SELECT number, number % 10000, toString(number) FROM numbers(20000)`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		te.exec(`SELECT count(), sum(v), max(s) FROM jl LEFT JOIN jr ON jl.k = jr.k AND jl.v > jr.id`)
		te.exec(`SELECT jl.id, jr.s FROM jl JOIN jr ON jl.k = jr.k`)
	}
}

func BenchmarkCrossJoinLimit(b *testing.B) {
	te := newTestEnv(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		te.exec(`SELECT * FROM numbers(1000) AS a CROSS JOIN numbers(100000) AS b LIMIT 1`)
	}
}
