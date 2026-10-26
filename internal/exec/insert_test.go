package exec

import (
	"fmt"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
)

func TestInsertFormats(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (k String, n UInt32 DEFAULT 7, s Nullable(String), d DateTime DEFAULT toDateTime('2024-01-01 00:00:00')) ENGINE = MergeTree ORDER BY k`)
	cases := []struct {
		query, body string
		rows        uint64
	}{
		{`INSERT INTO t FORMAT JSONEachRow`, `{"k":"j1","n":1,"s":"x","d":"2024-02-02 00:00:00"}` + "\n" + `{"k":"j2"}` + "\n", 2},
		{`INSERT INTO t (k, s) FORMAT TSV`, "t1\t\\N\nt2\tq\n", 2},
		{`INSERT INTO t FORMAT CSV`, "c1,5,\"a,b\",2024-03-03 00:00:00\n", 1},
		{`INSERT INTO t (k, n) VALUES`, `('v1', 9), ('v2', 10)`, 2},
		{"INSERT INTO t (k) FORMAT JSONEachRow {\"k\":\"inline\"}", "", 1},
		{`INSERT INTO t (k) FORMAT TSV`, "", 0},
	}
	for _, c := range cases {
		req := &Request{Query: c.query}
		if c.body != "" {
			req.Data = strings.NewReader(c.body)
		}
		res, err := te.runReq(req)
		if err != nil {
			t.Fatalf("%s: %v", c.query, err)
		}
		eq(t, res.WrittenRows, c.rows)
	}
	te.expectRows(`SELECT k, n, s, d FROM t ORDER BY k`, strings.Join([]string{
		"c1\t5\ta,b\t2024-03-03 00:00:00",
		"inline\t7\t\\N\t2024-01-01 00:00:00",
		"j1\t1\tx\t2024-02-02 00:00:00",
		"j2\t7\t\\N\t2024-01-01 00:00:00",
		"t1\t7\t\\N\t2024-01-01 00:00:00",
		"t2\t7\tq\t2024-01-01 00:00:00",
		"v1\t9\t\\N\t2024-01-01 00:00:00",
		"v2\t10\t\\N\t2024-01-01 00:00:00",
	}, "\n"))
	te.expectErr(`INSERT INTO t FORMAT NoSuchFormat`, cherr.UnknownFormat)
}

func TestInsertParamsAndExpressions(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (k String, n UInt64, tags Array(String), at DateTime64(3), id UUID, ins DateTime64(9) DEFAULT now64(9)) ENGINE = MergeTree ORDER BY k`)
	before := time.Now().Add(-time.Second)
	res := te.exec(`INSERT INTO t (k, n, tags, at, id) VALUES ({k:String}, {n:UInt64} + 1, {tags:Array(String)}, {at:DateTime64(3)}, generateUUIDv4()), ('lit', 2 * 3, ['a'], now64(3), '61f0c404-5cb3-11e7-907b-a6006ad3dba0')`,
		"k", "p\\tq", "n", "41", "tags", "['x','y']", "at", "2024-01-02 03:04:05.678")
	eq(t, res.WrittenRows, uint64(2))
	te.expectRows(`SELECT k, n, tags, at FROM t WHERE k != 'lit'`, "p\\tq\t42\t['x','y']\t2024-01-02 03:04:05.678")
	te.expectRows(`SELECT n, tags, toString(id) FROM t WHERE k = 'lit'`, "6\t['a']\t61f0c404-5cb3-11e7-907b-a6006ad3dba0")
	r := te.exec(`SELECT ins, id FROM t WHERE k != 'lit'`)
	ins := r.Rows[0][0].(time.Time)
	if ins.Before(before) || ins.After(time.Now().Add(time.Second)) {
		t.Errorf("DEFAULT now64(9) produced %v", ins)
	}
	if id := r.Rows[0][1].(string); len(id) != 36 {
		t.Errorf("generateUUIDv4 produced %q", id)
	}
	te.expectErr(`INSERT INTO t (k) VALUES ({missing:String})`, cherr.UnknownQueryParameter)
	te.expectErr(`INSERT INTO t (k, n) VALUES ('x', {n:UInt64})`, cherr.BadQueryParameter, "n", "abc")
	te.expectErr(`INSERT INTO t (k, n) VALUES ('x')`, cherr.NumberOfArgsDoesntMatch)
	te.expectErr(`INSERT INTO t (k, k) VALUES ('x', 'y')`, duplicateColumn)
	te.expectErr(`INSERT INTO nope VALUES (1)`, cherr.UnknownTable)
}

func TestInsertSelect(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE src (k String, n Int64, s Nullable(String)) ENGINE = MergeTree ORDER BY k`)
	te.exec(`CREATE TABLE dst (k String, n UInt8, s String, extra String DEFAULT concat(k, '-x')) ENGINE = MergeTree ORDER BY k`)
	te.exec(`INSERT INTO src VALUES ('a', 1, NULL), ('b', 300, 'q'), ('c', -1, 'r')`)
	res := te.exec(`INSERT INTO dst (k, n, s) SELECT k, n, s FROM src WHERE n != 0`)
	eq(t, res.WrittenRows, uint64(3))
	te.expectRows(`SELECT * FROM dst ORDER BY k`, "a\t1\t\ta-x\nb\t44\tq\tb-x\nc\t255\tr\tc-x")
	te.expectErr(`INSERT INTO dst (k, n) SELECT k FROM src`, cherr.NumberOfArgsDoesntMatch)
	te.exec(`INSERT INTO dst SELECT 'z', 1, 'y', 'explicit'`)
	te.expectRows(`SELECT extra FROM dst WHERE k = 'z'`, "explicit")
}

func TestInsertLargeBatches(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE src (k UInt64, g UInt8) ENGINE = MergeTree ORDER BY k`)
	te.exec(`CREATE TABLE agg (g UInt8, n SimpleAggregateFunction(sum, UInt64), u AggregateFunction(uniqExact, UInt64)) ENGINE = AggregatingMergeTree ORDER BY g`)
	te.exec(`CREATE MATERIALIZED VIEW mv TO agg AS SELECT g, count() AS n, uniqExactState(k) AS u FROM src GROUP BY g`)
	res := te.exec(`INSERT INTO src SELECT number, number % 3 FROM numbers(2500)`)
	eq(t, res.WrittenRows, uint64(2500))
	te.expectRows(`SELECT g, sum(n), uniqExactMerge(u) FROM agg GROUP BY g ORDER BY g`, "0\t834\t834\n1\t833\t833\n2\t833\t833")
	var b strings.Builder
	for i := 0; i < 1500; i++ {
		fmt.Fprintf(&b, "{\"k\": %d, \"g\": 9}\n", 10000+i)
	}
	res, err := te.runReq(&Request{Query: `INSERT INTO src FORMAT JSONEachRow`, Data: strings.NewReader(b.String())})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, res.WrittenRows, uint64(1500))
	te.expectRows(`SELECT sum(n), uniqExactMerge(u) FROM agg WHERE g = 9`, "1500\t1500")
}

func TestInsertIntoMaterializedViewName(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE src (k String) ENGINE = MergeTree ORDER BY k`)
	te.exec(`CREATE TABLE dst (k String) ENGINE = MergeTree ORDER BY k`)
	te.exec(`CREATE MATERIALIZED VIEW mv TO dst AS SELECT k FROM src`)
	te.exec(`INSERT INTO mv VALUES ('direct')`)
	te.expectRows(`SELECT k FROM dst`, "direct")
	te.expectRows(`SELECT k FROM mv`, "direct")
}

func TestInsertAggregatingDirect(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE agg (k String, s SimpleAggregateFunction(sum, UInt64), m SimpleAggregateFunction(max, String), u AggregateFunction(uniqExact, String)) ENGINE = AggregatingMergeTree ORDER BY k`)
	te.exec(`INSERT INTO agg (k, s, m) VALUES ('a', 1, 'x'), ('a', 2, 'z'), ('a', 3, 'y')`)
	te.exec(`INSERT INTO agg SELECT 'a', 10, 'b', uniqExactState(toString(number)) FROM numbers(4)`)
	te.expectRows(`SELECT k, s, m, finalizeAggregation(u) FROM agg`, "a\t16\tz\t4")
	te.expectErr(`INSERT INTO agg (k, u) VALUES ('a', 'x')`, cherr.TypeMismatch)
}

func TestSummingMergeTree(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE s (k String, a UInt8, b Int64, label String) ENGINE = SummingMergeTree ORDER BY k`)
	te.exec(`INSERT INTO s VALUES ('x', 200, -1, 'first'), ('y', 1, 1, 'y')`)
	te.exec(`INSERT INTO s VALUES ('x', 100, -2, 'second')`)
	te.expectRows(`SELECT k, a, b, label FROM s FINAL ORDER BY k`, "x\t44\t-3\tfirst\ny\t1\t1\ty")
	te.exec(`CREATE TABLE s2 (k String, a UInt64, b UInt64) ENGINE = SummingMergeTree(a) ORDER BY k`)
	te.exec(`INSERT INTO s2 VALUES ('x', 1, 5), ('x', 2, 6)`)
	te.expectRows(`SELECT a, b FROM s2`, "3\t5")
}

// A VALUES row is not bounded by the AST limits, so a chain of AND, OR or ||
// in it must compile and evaluate without a level of recursion per operand.
func TestValuesChainsInsertWithoutRecursion(t *testing.T) {
	guardFunctionTest(t)
	te := newTestEnv(t)
	te.exec("CREATE TABLE t (a UInt8, b UInt8, s String) ENGINE = Memory")
	chain := func(op string, n int, atom string) string {
		return strings.Repeat(atom+" "+op+" ", n-1) + atom
	}
	sql := "INSERT INTO t VALUES (" + chain("AND", 40000, "1") + ", " + chain("OR", 40000, "0") + ", " +
		chain("||", 20000, "'x'") + ")"
	// A walk per operand needs several MiB of stack.
	old := debug.SetMaxStack(1 << 20)
	_, err := te.run(sql)
	debug.SetMaxStack(old)
	if err != nil {
		t.Fatal(err)
	}
	te.expectRows("SELECT a, b, length(s), s = repeat('x', 20000) FROM t", "1\t0\t20000\t1")
	te.expectRows("SELECT 'a' || ('b' || 'c') || 'd', toTypeName('a' || toNullable('b') || 'c')", "abcd\tNullable(String)")
}
