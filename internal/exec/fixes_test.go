package exec

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/storage"
)

// Expectations in this file were verified against ClickHouse 25.8.

// scanWithIDs reads every row of db.name with its stored row id.
func scanWithIDs(t *testing.T, te *testEnv, db, name string) ([]storage.Row, []string) {
	t.Helper()
	tbl, err := te.cat.Table(db, name)
	if err != nil {
		t.Fatal(err)
	}
	it, err := te.store.Scan(context.Background(), tbl, storage.ScanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	ri, ok := it.(RowIDIterator)
	if !ok {
		t.Fatal("memstore iterator does not implement RowIDIterator")
	}
	var rows []storage.Row
	var ids []string
	for {
		r, err := it.Next(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, r)
		ids = append(ids, ri.RowID())
	}
	return rows, ids
}

const mvSetup = `CREATE TABLE src (k String, ts DateTime, n UInt64, u String) ENGINE = MergeTree ORDER BY (k, ts);
CREATE TABLE agg (k String, c SimpleAggregateFunction(sum, UInt64), s SimpleAggregateFunction(sum, UInt64), uu AggregateFunction(uniqExact, String)) ENGINE = AggregatingMergeTree ORDER BY k`

func execAll(te *testEnv, script string) {
	te.t.Helper()
	for _, st := range strings.Split(script, ";") {
		if strings.TrimSpace(st) != "" {
			te.exec(st)
		}
	}
}

// TestPopulateMarkersMatchStreamReplay: POPULATE uses the stored row ids, so
// a stream worker replaying the same source rows is a no-op.
func TestPopulateMarkersMatchStreamReplay(t *testing.T) {
	te := newTestEnv(t)
	execAll(te, mvSetup)
	te.exec(`INSERT INTO src VALUES ('a', '2024-01-01 00:00:00', 1, 'x'), ('a', '2024-01-01 01:00:00', 2, 'y'), ('b', '2024-01-01 00:00:00', 5, 'x')`)
	te.exec(`CREATE MATERIALIZED VIEW mv TO agg POPULATE AS SELECT k, count() AS c, sum(n) AS s, uniqExactState(u) AS uu FROM src GROUP BY k`)
	const want = "a\t2\t3\t2\nb\t1\t5\t1"
	read := `SELECT k, sum(c), sum(s), uniqExactMerge(uu) FROM agg GROUP BY k ORDER BY k`
	te.expectRows(read, want)
	tbl, _ := te.cat.Table("default", "src")
	rows, ids := scanWithIDs(t, te, "default", "src")
	// Worker replay of every source record (TRIM_HORIZON) without write times.
	if err := te.e.ApplyViews(context.Background(), tbl, rows, ids); err != nil {
		t.Fatal(err)
	}
	te.expectRows(read, want)
	te.exec(`INSERT INTO src VALUES ('b', '2024-01-02 00:00:00', 7, 'w')`)
	te.expectRows(read, "a\t2\t3\t2\nb\t2\t12\t2")
}

// TestApplyViewList: the stream worker applies only the views a row is
// eligible for.
func TestApplyViewList(t *testing.T) {
	te := newTestEnv(t)
	execAll(te, mvSetup)
	te.exec(`CREATE TABLE agg2 (k String, c SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k`)
	te.exec(`CREATE MATERIALIZED VIEW mv TO agg AS SELECT k, count() AS c, sum(n) AS s, uniqExactState(u) AS uu FROM src GROUP BY k`)
	te.exec(`CREATE MATERIALIZED VIEW mv2 TO agg2 AS SELECT k, count() AS c FROM src GROUP BY k`)
	te.exec(`INSERT INTO src VALUES ('a', '2024-01-01 00:00:00', 1, 'x')`)
	tbl, _ := te.cat.Table("default", "src")
	rows, ids := scanWithIDs(t, te, "default", "src")
	te.exec(`CREATE TABLE src_other (k String) ENGINE = MergeTree ORDER BY k`)
	mv2, err := te.cat.View("default", "mv2")
	if err != nil {
		t.Fatal(err)
	}
	// Redelivery to one view only: still exactly once.
	if err := te.e.ApplyViewList(context.Background(), tbl, []*catalog.View{mv2}, rows, ids); err != nil {
		t.Fatal(err)
	}
	te.expectRows(`SELECT sum(c) FROM agg2`, "1")
	// A fresh row id through mv2 only: agg is untouched.
	if err := te.e.ApplyViewList(context.Background(), tbl, []*catalog.View{mv2}, rows, []string{"other"}); err != nil {
		t.Fatal(err)
	}
	te.expectRows(`SELECT sum(c) FROM agg2`, "2")
	te.expectRows(`SELECT sum(c) FROM agg`, "1")
	other, _ := te.cat.Table("default", "src_other")
	if err := te.e.ApplyViewList(context.Background(), other, []*catalog.View{mv2}, nil, nil); err == nil {
		t.Error("ApplyViewList with a view of another table: expected an error")
	}
}

// TestPopulateAppliesColumnDefaults: rows stored before ALTER ADD COLUMN feed
// the view with the column's default.
func TestPopulateAppliesColumnDefaults(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE s2 (k String) ENGINE = MergeTree ORDER BY k`)
	te.exec(`INSERT INTO s2 VALUES ('a'), ('a')`)
	te.exec(`ALTER TABLE s2 ADD COLUMN w UInt64 DEFAULT 10`)
	te.exec(`CREATE TABLE t2 (k String, total SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k`)
	te.exec(`CREATE MATERIALIZED VIEW mv2 TO t2 POPULATE AS SELECT k, sum(w) AS total FROM s2 GROUP BY k`)
	te.expectRows(`SELECT k, sum(total) FROM t2 GROUP BY k`, "a\t20")
}

func TestInsertValuesDateTime64Literals(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE ev (k String, ts64 DateTime64(3), ts DateTime, d Date, a Array(DateTime64(3)), n Nullable(DateTime64(3))) ENGINE = MergeTree ORDER BY k`)
	te.exec(`INSERT INTO ev VALUES ('a', 1714989600250, 1714989600, '2024-05-06', [1714989600250], 1714989600250), ('b', '1714989600250', '2024-05-06 10:00:00', 19849, [], NULL), ('c', 1714989600, 0, 0, ['2024-05-06 10:00:00.5'], '2024-05-06 10:00:00.123')`)
	te.expectRows(`SELECT k, ts64, ts, d, a, n FROM ev ORDER BY k`,
		"a\t2024-05-06 10:00:00.250\t2024-05-06 10:00:00\t2024-05-06\t['2024-05-06 10:00:00.250']\t2024-05-06 10:00:00.250\n"+
			"b\t2024-05-06 10:00:00.250\t2024-05-06 10:00:00\t2024-05-06\t[]\t\\N\n"+
			"c\t1970-01-20 20:23:09.600\t1970-01-01 00:00:00\t1970-01-01\t['2024-05-06 10:00:00.500']\t2024-05-06 10:00:00.123")
	// Expressions still go through CAST semantics.
	te.exec(`INSERT INTO ev (k, ts64) VALUES ('d', toDateTime64('2024-05-06 10:00:00.5', 3) + 1)`)
	te.expectRows(`SELECT ts64 FROM ev WHERE k = 'd'`, "2024-05-06 10:00:01.500")
}

func TestUnknownTableEngine(t *testing.T) {
	te := newTestEnv(t)
	te.expectErr(`CREATE TABLE x1 (x String) ENGINE = NoSuchEngine ORDER BY x`, cherr.UnknownStorage)
	te.expectErr(`CREATE TABLE x2 (x String) ENGINE = mergetree ORDER BY x`, cherr.UnknownStorage)
	te.expectErr(`CREATE TABLE x3 (x String, s Int8) ENGINE = CollapsingMergeTree(s) ORDER BY x`, cherr.NotImplemented)
	te.expectErr(`CREATE TABLE x4 (x String) ENGINE = Kafka`, cherr.NotImplemented)
}

func TestHavingWithoutAggregation(t *testing.T) {
	te := newTestEnv(t)
	te.expectRows(`SELECT 1 AS x HAVING x = 1`, "1")
	te.expectRows(`SELECT number FROM numbers(5) HAVING number > 2`, "3\n4")
	te.expectRows(`SELECT number AS n, n * 2 AS m FROM numbers(5) WHERE n > 1 HAVING m > 4 ORDER BY n`, "3\t6\n4\t8")
	te.expectRows(`SELECT number FROM numbers(10) HAVING number > 7 LIMIT 1`, "8")
}

func TestModuloResultType(t *testing.T) {
	te := newTestEnv(t)
	cases := []struct{ a, b, want string }{
		{"UInt8", "UInt8", "UInt8"}, {"UInt8", "Int16", "UInt16"}, {"UInt64", "Int8", "UInt8"},
		{"Int8", "UInt8", "Int16"}, {"Int8", "Int8", "Int16"}, {"Int64", "Int8", "Int16"},
		{"Int16", "UInt32", "Int64"}, {"Int32", "Int64", "Int64"}, {"Int64", "UInt64", "Int64"},
	}
	for _, c := range cases {
		te.expectRows(`SELECT toTypeName(to`+c.a+`(7) % to`+c.b+`(3)), toTypeName(moduloOrZero(to`+c.a+`(7), to`+c.b+`(3)))`, c.want+"\t"+c.want)
	}
	te.expectRows(`SELECT -7 % -3, toTypeName(-7 % -3)`, "-1\tInt16")
}

func TestDateConversionsLikeClickHouse(t *testing.T) {
	te := newTestEnv(t)
	te.expectRows(`SELECT toStartOfWeek(toDate('1970-01-01')), toMonday(toDate('1970-01-02')), toStartOfWeek(toDate('1970-01-03'), 1)`, "1970-01-01\t1970-01-01\t1970-01-01")
	te.expectRows(`SELECT toMonday(toDateTime('1970-01-02 00:00:00', 'UTC')), toStartOfWeek(toDateTime('1970-01-02 00:00:00', 'UTC'))`, "1970-01-01\t1970-01-01")
	te.expectRows(`SELECT toMonday(toDate('2024-05-08')), toStartOfWeek(toDate('2024-05-08'))`, "2024-05-06\t2024-05-05")
	// Wrapping of time-to-time conversions.
	te.expectRows(`SELECT toDateTime(toDate('2149-06-06')), toDateTime(toDateTime64('2200-01-01 00:00:00', 3, 'UTC'))`, "2013-04-29 17:31:44\t2063-11-24 17:31:44")
	te.expectRows(`SELECT toDate(toDate32('2200-01-01')), toDate(toDate32('1900-01-01'))`, "2020-07-27\t2079-06-07")
	te.expectRows(`SELECT toDateTime(toDate('2106-02-07')), toDateTime64(toDate('2149-06-06'), 3)`, "2106-02-07 00:00:00\t2149-06-06 00:00:00.000")
	// Numbers saturate.
	te.expectRows(`SELECT toDateTime(-1), toDateTime(4294967296), toDateTime(1e10), toDateTime(toUInt64(18446744073709551615))`, "1970-01-01 00:00:00\t2106-02-07 06:28:15\t2106-02-07 06:28:15\t2106-02-07 06:28:15")
	te.expectRows(`SELECT toDate(-1), toDate(65535), toDate(65536), toDate(100000), toDate(1e12), toDate(4294967296)`, "1970-01-01\t2149-06-06\t1970-01-01\t1970-01-02\t2149-06-06\t2106-02-07")
	te.expectRows(`SELECT toDateTime64(1714989600250, 3), toDateTime64(-1e12, 3), toDateTime64(1714989600.25, 3), toDateTime64(-1.5, 3)`, "2299-12-31 23:59:59.000\t1900-01-01 00:00:00.000\t2024-05-06 10:00:00.250\t1969-12-31 23:59:58.500")
	// Strings use the basic text parser (as CAST), not best effort.
	te.expectRows(`SELECT toDate('2024-1-5'), toDate32('1900-1-5'), toDate('2024-01-05 10:20:30'), toDate('20240105')`, "2024-01-05\t1900-01-05\t2024-01-05\t2024-01-05")
	te.expectRows(`SELECT toDateTime('1714989600'), toDateTime64('1714989600250', 3), toDateTime64('1714989600.25', 3), toDateTime('0000-00-00 00:00:00')`, "2024-05-06 10:00:00\t2024-05-06 10:00:00.250\t2024-05-06 10:00:00.250\t1970-01-01 00:00:00")
	te.expectRows(`SELECT toDate32('2299-12-31'), toDate32('2299-12-31') + 1`, "2299-12-31\t2299-12-31")
	te.expectErr(`SELECT toDateTime('2024-01-05 10:20')`, cherr.CannotParseDateTime)
	te.expectErr(`SELECT toDate('05/01/2024')`, cherr.CannotParseDate)
	for _, q := range []string{`SELECT toDateTime('2024-01-05T10:20:30Z')`, `SELECT toDateTime('2024-01-05 10:20:30 ')`, `SELECT toDate('19000')`, `SELECT toUnixTimestamp('2024-01-05T10:20:30Z')`} {
		if _, err := te.run(q); err == nil {
			t.Errorf("%s: expected an error", q)
		}
	}
	te.expectRows(`SELECT toUnixTimestamp('2024-01-05 10:20:30')`, "1704450030")
}

func TestAggregateArgsDropNestedLowCardinality(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE lc (op LowCardinality(String), nop LowCardinality(Nullable(String)), v UInt64) ENGINE = MergeTree ORDER BY v`)
	te.exec(`INSERT INTO lc VALUES ('a', 'b', 1)`)
	te.expectRows(`SELECT toTypeName(argMax(tuple(op), v)), toTypeName(argMax((op, nop), v)), toTypeName(groupArray(tuple(op))), toTypeName(any(op)), toTypeName(argMax([op], v)), toTypeName(any(nop)) FROM lc`,
		"Tuple(String)\tTuple(String, Nullable(String))\tArray(Tuple(String))\tString\tArray(String)\tNullable(String)")
	te.expectRows(`SELECT toTypeName(argMaxState(tuple(op), v)) FROM lc`, "AggregateFunction(argMax, Tuple(String), UInt64)")
}

func TestMapParameterColumnName(t *testing.T) {
	te := newTestEnv(t)
	res := te.exec(`SELECT {m:Map(String, UInt8)}`, "m", "{'a':1,'b':2}")
	if got, want := res.Columns[0].Name, `_CAST([('a', 1), ('b', 2)], 'Map(String, UInt8)')`; got != want {
		t.Errorf("column name %q, want %q", got, want)
	}
}

func TestNamedTupleAliasDotAccess(t *testing.T) {
	te := newTestEnv(t)
	te.expectRows(`SELECT CAST((1, 'a') AS Tuple(a UInt8, b String)) AS t, t.a, t.b`, "(1,'a')\t1\ta")
	res := te.exec(`SELECT CAST((1, 'a') AS Tuple(a UInt8, b String)) AS t, t.a`)
	if got := res.Columns[1].Name; got != "t.a" {
		t.Errorf("column name %q, want t.a", got)
	}
	te.expectErr(`SELECT 1 AS t, t.a`, cherr.UnknownIdentifier)
	// A table-qualified column still wins over an alias of the same name.
	te.exec(`CREATE TABLE t (a UInt8) ENGINE = MergeTree ORDER BY a`)
	te.exec(`INSERT INTO t VALUES (5)`)
	te.expectRows(`SELECT a + 1 AS t, t.a FROM t`, "6\t5")
}

func TestAlterAddColumnDefaultForOldRows(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE al (id UInt64, msg String) ENGINE = MergeTree ORDER BY id`)
	te.exec(`INSERT INTO al VALUES (2, 'b'), (1, 'a')`)
	te.exec(`ALTER TABLE al ADD COLUMN level UInt8 DEFAULT 3`)
	te.exec(`INSERT INTO al VALUES (3, 'c', 9)`)
	te.exec(`ALTER TABLE al ADD COLUMN l2 UInt8 DEFAULT id * 2`)
	te.expectRows(`SELECT * FROM al ORDER BY id`, "1\ta\t3\t2\n2\tb\t3\t4\n3\tc\t9\t6")
	te.expectRows(`SELECT level FROM al WHERE id = 1`, "3")
	te.expectRows(`SELECT sum(level), sum(l2) FROM al`, "15\t12")
	te.expectRows(`SELECT id FROM al WHERE level = 3 ORDER BY id`, "1\n2")
	te.exec(`ALTER TABLE al DROP COLUMN msg`)
	te.expectRows(`SELECT * FROM al ORDER BY id`, "1\t3\t2\n2\t3\t4\n3\t9\t6")
	te.exec(`ALTER TABLE al MODIFY COLUMN level UInt32`)
	te.expectRows(`SELECT id, level, toTypeName(level) FROM al ORDER BY id`, "1\t3\tUInt32\n2\t3\tUInt32\n3\t9\tUInt32")
	te.exec(`ALTER TABLE al MODIFY COLUMN l2 UInt8 DEFAULT 7`)
	te.expectRows(`SELECT id, l2 FROM al ORDER BY id`, "1\t7\n2\t7\n3\t7")
	// Lightweight DELETE matching on a defaulted column still finds old rows.
	te.exec(`DELETE FROM al WHERE level = 3 AND id = 1`)
	te.expectRows(`SELECT id FROM al ORDER BY id`, "2\n3")
	// Nullable column without DEFAULT reads NULL.
	te.exec(`ALTER TABLE al ADD COLUMN note Nullable(String)`)
	te.expectRows(`SELECT id, note FROM al ORDER BY id`, "2\t\\N\n3\t\\N")
}

func TestNoCommonTypeCode(t *testing.T) {
	te := newTestEnv(t)
	te.expectErr(`SELECT if(1, 'a', 1)`, cherr.NoCommonType)
	te.expectErr(`SELECT [1, 'a']`, cherr.NoCommonType)
	te.expectErr(`SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 'a')`, cherr.NoCommonType)
	te.expectErr(`SELECT multiIf(1, 'a', 2)`, cherr.NoCommonType)
}

func TestMultipleExpressionsForAlias(t *testing.T) {
	te := newTestEnv(t)
	te.expectErr(`SELECT 1 AS x, 2 AS x`, multipleExpressionsForAlias)
	te.expectErr(`SELECT (1 AS x) + (2 AS x)`, multipleExpressionsForAlias)
	te.expectErr(`SELECT number AS x FROM numbers(2) WHERE (number + 1 AS x) > 0`, multipleExpressionsForAlias)
	te.expectRows(`SELECT 1 AS x, 1 AS x`, "1\t1")
	te.expectRows(`SELECT number AS x FROM numbers(3) WHERE x > 0 ORDER BY x`, "1\n2")
	te.expectRows(`SELECT 1 AS x, (SELECT 2 AS x)`, "1\t2")
}

func TestGreatestLeastIgnoreNull(t *testing.T) {
	te := newTestEnv(t)
	te.expectRows(`SELECT greatest(1, NULL), least(NULL, 2), toTypeName(greatest(1, NULL))`, "1\t2\tNullable(UInt8)")
	te.expectRows(`SELECT greatest(NULL, NULL), toTypeName(greatest(NULL, NULL))`, "\\N\tNullable(Nothing)")
	te.expectRows(`SELECT least(1, 2, NULL, 0), greatest(toNullable(1), 2), toTypeName(greatest(toNullable(1), 2))`, "0\t2\tNullable(UInt8)")
	te.expectRows(`SELECT least(toLowCardinality('a'), 'b'), toTypeName(least(toLowCardinality('a'), 'b'))`, "a\tLowCardinality(String)")
	te.exec(`CREATE TABLE gl (a Nullable(UInt8), b Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple()`)
	te.exec(`INSERT INTO gl VALUES (1, NULL), (NULL, NULL), (3, 2)`)
	te.expectRows(`SELECT greatest(a, b) FROM gl ORDER BY a`, "1\n3\n\\N")
}

func TestReplacingIsDeleted(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE rd (k String, v UInt64, d UInt8, x String) ENGINE = ReplacingMergeTree(v, d) ORDER BY k`)
	te.exec(`INSERT INTO rd VALUES ('a', 1, 0, 'x'), ('b', 1, 0, 'y')`)
	te.exec(`INSERT INTO rd VALUES ('a', 2, 1, 'x2'), ('c', 1, 1, 'z')`)
	te.expectRows(`SELECT * FROM rd FINAL ORDER BY k`, "b\t1\t0\ty")
	te.exec(`INSERT INTO rd VALUES ('a', 3, 0, 'x3')`)
	te.expectRows(`SELECT * FROM rd FINAL ORDER BY k`, "a\t3\t0\tx3\nb\t1\t0\ty")
	te.exec(`INSERT INTO rd VALUES ('b', 1, 1, 'y-del')`)
	te.exec(`INSERT INTO rd VALUES ('a', 2, 0, 'old')`)
	te.expectRows(`SELECT * FROM rd ORDER BY k`, "a\t3\t0\tx3")
	te.expectRows(`SELECT count() FROM rd WHERE k = 'b'`, "0")
	if got := te.rows(`SHOW CREATE TABLE rd`); !strings.Contains(got, "ReplacingMergeTree(v, d)") {
		t.Errorf("SHOW CREATE TABLE: %q", got)
	}
	te.expectErr(`CREATE TABLE rd2 (k String, v UInt64, d String) ENGINE = ReplacingMergeTree(v, d) ORDER BY k`, badTypeOfField)
	te.expectErr(`CREATE TABLE rd3 (k String, v UInt64, d UInt8) ENGINE = ReplacingMergeTree(v, nope) ORDER BY k`, cherr.NoSuchColumnInTable)
	te.expectErr(`CREATE TABLE rd4 (k String, v UInt64, d UInt8) ENGINE = ReplacingMergeTree(v, d, 3) ORDER BY k`, cherr.NumberOfArgsDoesntMatch)
}

func TestQueryParameterParseError(t *testing.T) {
	te := newTestEnv(t)
	_, err := te.run(`SELECT {x:UInt8}`, "x", "abc")
	if !cherr.Is(err, cherr.BadQueryParameter) {
		t.Fatalf("got %v, want code 457", err)
	}
	if msg := cherr.As(err).Msg; !strings.HasPrefix(msg, "Value abc cannot be parsed as UInt8 for query parameter 'x'") || strings.Contains(msg, "Code:") {
		t.Errorf("message %q", msg)
	}
	te.expectErr(`SELECT {d:DateTime}`, cherr.CannotParseDateTime, "d", "xx")
	// Numbers: the parser stopping early is 457 with the parsed prefix; a
	// parser exception keeps its code (CH 25.8).
	for _, c := range []struct {
		typ, val string
		code     int
		msg      string
	}{
		{"UInt8", "abc", 457, "Value abc cannot be parsed as UInt8 for query parameter 'a' because it isn't parsed completely: only 0 of 3 bytes was parsed: "},
		{"Int64", "1x", 457, "Value 1x cannot be parsed as Int64 for query parameter 'a' because it isn't parsed completely: only 1 of 2 bytes was parsed: 1"},
		{"UInt8", "-5x", 457, "Value -5x cannot be parsed as UInt8 for query parameter 'a' because it isn't parsed completely: only 0 of 3 bytes was parsed: "},
		{"Int32", "1e5", 457, "Value 1e5 cannot be parsed as Int32 for query parameter 'a' because it isn't parsed completely: only 1 of 3 bytes was parsed: 1"},
		{"Nullable(UInt8)", "abc", 457, "Value abc cannot be parsed as Nullable(UInt8) for query parameter 'a' because it isn't parsed completely: only 0 of 3 bytes was parsed: "},
		{"Float64", "x", 457, "Value x cannot be parsed as Float64 for query parameter 'a' because it isn't parsed completely: only 0 of 1 bytes was parsed: "},
		{"Float64", "-1.5e3x", 457, "Value -1.5e3x cannot be parsed as Float64 for query parameter 'a' because it isn't parsed completely: only 6 of 7 bytes was parsed: -1.5e3"},
		{"Float64", "nanx", 457, "Value nanx cannot be parsed as Float64 for query parameter 'a' because it isn't parsed completely: only 3 of 4 bytes was parsed: nan"},
		{"Int64", "", 32, "Attempt to read after eof: value  cannot be parsed as Int64 for query parameter 'a'"},
		{"Int64", "-", 32, "Attempt to read after eof: value - cannot be parsed as Int64 for query parameter 'a'"},
		{"Float64", "", 72, "Cannot read floating point value: value  cannot be parsed as Float64 for query parameter 'a'"},
		{"Float64", "-", 72, "Cannot read floating point value: no digits read: value - cannot be parsed as Float64 for query parameter 'a'"},
		{"Float64", "1e", 72, "Cannot read floating point value: nothing after exponent: value 1e cannot be parsed as Float64 for query parameter 'a'"},
		{"Float64", "e", 72, "Cannot read floating point value: nothing after exponent: value e cannot be parsed as Float64 for query parameter 'a'"},
		{"Float64", "infx", 27, "Cannot parse infinity: value infx cannot be parsed as Float64 for query parameter 'a'"},
	} {
		_, err := te.run(`SELECT {a:`+c.typ+`}`, "a", c.val)
		if !cherr.Is(err, c.code) || cherr.As(err).Msg != c.msg {
			t.Errorf("%s %q: got %v, want %d %q", c.typ, c.val, err, c.code, c.msg)
		}
	}
	// Structural parser failures keep their code (CH 25.8).
	for _, c := range []struct {
		typ, val string
		code     int
	}{{"Array(UInt8)", "1,2", cherr.CannotReadArrayFromText}, {"Array(UInt8)", "[1,x]", cherr.CannotReadArrayFromText}, {"Tuple(UInt8, String)", "(1,x)", cherr.CannotParseQuotedString}, {"UUID", "abc", 376}, {"Bool", "maybe", 467}} {
		_, err := te.run(`SELECT {a:`+c.typ+`}`, "a", c.val)
		if !cherr.Is(err, c.code) {
			t.Errorf("%s %q: got %v, want code %d", c.typ, c.val, err, c.code)
		} else if want := ": value " + c.val + " cannot be parsed as " + c.typ + " for query parameter 'a'"; !strings.HasSuffix(cherr.As(err).Msg, want) {
			t.Errorf("%s %q: message %q, want suffix %q", c.typ, c.val, cherr.As(err).Msg, want)
		}
	}
}

func TestInsertUnsupportedInputFormat(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE fi (x UInt8) ENGINE = MergeTree ORDER BY x`)
	te.expectErr("INSERT INTO fi FORMAT RowBinary 1", cherr.NotImplemented)
	te.expectErr("INSERT INTO fi FORMAT Parquet 1", cherr.NotImplemented)
	_, err := te.run("INSERT INTO fi FORMAT NoSuchFormat 1")
	if !cherr.Is(err, cherr.UnknownFormat) || cherr.As(err).Msg != "Unknown format NoSuchFormat" {
		t.Errorf("got %v, want 73 'Unknown format NoSuchFormat'", err)
	}
}
