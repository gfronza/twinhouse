package exec

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/storage"
)

func ts3(s string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05.000", s)
	if err != nil {
		panic(err)
	}
	return t
}

func incl(v any) *storage.Bound { return &storage.Bound{Value: v, Inclusive: true} }
func excl(v any) *storage.Bound { return &storage.Bound{Value: v, Inclusive: false} }

func TestPushdownRanges(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE ev (tenant String, user_id UInt64, ts DateTime64(3), small UInt8, v String) ENGINE = MergeTree ORDER BY (tenant, user_id, ts)`)
	te.exec(`CREATE TABLE ev2 (a String, b UInt32, c String) ENGINE = MergeTree ORDER BY (a, b, c) SETTINGS dynamodb_partition_key = 'a, b'`)
	te.exec(`CREATE TABLE ev0 (a String, b UInt32) ENGINE = MergeTree ORDER BY (a, b) SETTINGS dynamodb_partition_key = ''`)
	none := []storage.KeyRange{}
	cases := []struct {
		where  string
		table  string
		params []string
		want   []storage.KeyRange // nil = full scan
	}{
		{where: "tenant = 'a'", want: []storage.KeyRange{{Partition: []any{"a"}}}},
		{where: "'a' = tenant", want: []storage.KeyRange{{Partition: []any{"a"}}}},
		{where: "tenant = 'a' AND user_id = 7", want: []storage.KeyRange{{Partition: []any{"a"}, SortPrefix: []any{uint64(7)}}}},
		{where: "tenant = 'a' AND user_id = 7 AND ts = '2024-01-01 00:00:00.5'", want: []storage.KeyRange{{Partition: []any{"a"}, SortPrefix: []any{uint64(7), ts3("2024-01-01 00:00:00.500")}}}},
		{where: "tenant IN ('a', 'b') AND user_id IN (1, 2)", want: []storage.KeyRange{
			{Partition: []any{"a"}, SortPrefix: []any{uint64(1)}}, {Partition: []any{"a"}, SortPrefix: []any{uint64(2)}},
			{Partition: []any{"b"}, SortPrefix: []any{uint64(1)}}, {Partition: []any{"b"}, SortPrefix: []any{uint64(2)}}}},
		{where: "(tenant, user_id) IN (('a', 1), ('b', 2), ('a', 1))", want: []storage.KeyRange{
			{Partition: []any{"a"}, SortPrefix: []any{uint64(1)}}, {Partition: []any{"b"}, SortPrefix: []any{uint64(2)}}}},
		{where: "(user_id, tenant) IN ((1, 'a'), (2, 'b'))", want: []storage.KeyRange{
			{Partition: []any{"a"}, SortPrefix: []any{uint64(1)}}, {Partition: []any{"b"}, SortPrefix: []any{uint64(2)}}}},
		{where: "(tenant, v) IN (('a', 'x'), ('b', 'y'))", want: []storage.KeyRange{{Partition: []any{"a"}}, {Partition: []any{"b"}}}},
		{where: "tenant = 'a' AND user_id = 7 AND ts >= '2024-01-01 00:00:00' AND ts < '2024-01-02 00:00:00'", want: []storage.KeyRange{{
			Partition: []any{"a"}, SortPrefix: []any{uint64(7)},
			Lower: incl(ts3("2024-01-01 00:00:00.000")), Upper: excl(ts3("2024-01-02 00:00:00.000"))}}},
		{where: "tenant = 'a' AND user_id > 5", want: []storage.KeyRange{{Partition: []any{"a"}, Lower: excl(uint64(5))}}},
		{where: "tenant = 'a' AND 5 >= user_id", want: []storage.KeyRange{{Partition: []any{"a"}, Upper: incl(uint64(5))}}},
		{where: "tenant = 'a' AND user_id BETWEEN 3 AND 9 AND user_id > 4", want: []storage.KeyRange{{Partition: []any{"a"}, Lower: excl(uint64(4)), Upper: incl(uint64(9))}}},
		{where: "tenant = 'a' AND user_id > 9 AND user_id < 3", want: none},
		{where: "tenant = 'a' AND ts > '2024-01-01 00:00:00'", want: []storage.KeyRange{{Partition: []any{"a"}}}},
		{where: "tenant = 'a' AND user_id > 2.5", want: []storage.KeyRange{{Partition: []any{"a"}}}},
		{where: "user_id = 7", want: nil},
		{where: "tenant = 'a' OR tenant = 'b'", want: nil},
		{where: "lower(tenant) = 'a'", want: nil},
		{where: "tenant = 'a' AND tenant = 'b'", want: none},
		{where: "tenant IN ('a') AND tenant IN ('a', 'b')", want: []storage.KeyRange{{Partition: []any{"a"}}}},
		{where: "tenant = 'a' AND user_id = -1", want: none},
		{where: "tenant = 'a' AND user_id IN (-1, 3)", want: []storage.KeyRange{{Partition: []any{"a"}, SortPrefix: []any{uint64(3)}}}},
		{where: "tenant = NULL", want: none},
		{where: "tenant = {t:String} AND user_id = {u:UInt64}", params: []string{"t", "p", "u", "4"}, want: []storage.KeyRange{{Partition: []any{"p"}, SortPrefix: []any{uint64(4)}}}},
		{where: "(tenant, user_id) IN {keys:Array(Tuple(String, UInt64))}", params: []string{"keys", "[('x',1),('y',2)]"}, want: []storage.KeyRange{
			{Partition: []any{"x"}, SortPrefix: []any{uint64(1)}}, {Partition: []any{"y"}, SortPrefix: []any{uint64(2)}}}},
		{where: "tenant IN (SELECT 'q')", want: []storage.KeyRange{{Partition: []any{"q"}}}},
		{where: "tenant = concat('a', 'b')", want: []storage.KeyRange{{Partition: []any{"ab"}}}},
		{table: "ev2", where: "a = 'x'", want: nil},
		{table: "ev2", where: "a = 'x' AND b = 1", want: []storage.KeyRange{{Partition: []any{"x", uint64(1)}}}},
		{table: "ev2", where: "a = 'x' AND b = 1 AND c >= 'm'", want: []storage.KeyRange{{Partition: []any{"x", uint64(1)}, Lower: incl("m")}}},
		{table: "ev0", where: "a = 'x'", want: []storage.KeyRange{{SortPrefix: []any{"x"}}}},
		{table: "ev0", where: "a > 'x'", want: []storage.KeyRange{{Lower: excl("x")}}},
		{table: "ev0", where: "b = 1", want: nil},
	}
	for _, c := range cases {
		table := c.table
		if table == "" {
			table = "ev"
		}
		te.exec(fmt.Sprintf("SELECT count() FROM %s WHERE %s", table, c.where), c.params...)
		got := te.spy.lastScan().Ranges
		if !rangesEqual(got, c.want) {
			t.Errorf("WHERE %s:\n got  %s\n want %s", c.where, fmtRanges(got), fmtRanges(c.want))
		}
	}
}

func rangesEqual(a, b []storage.KeyRange) bool {
	if (a == nil) != (b == nil) || len(a) != len(b) {
		return false
	}
	norm := func(x []any) []any {
		if len(x) == 0 {
			return nil
		}
		return x
	}
	for i := range a {
		if !reflect.DeepEqual(norm(a[i].Partition), norm(b[i].Partition)) || !reflect.DeepEqual(norm(a[i].SortPrefix), norm(b[i].SortPrefix)) ||
			!reflect.DeepEqual(a[i].Lower, b[i].Lower) || !reflect.DeepEqual(a[i].Upper, b[i].Upper) {
			return false
		}
	}
	return true
}

func fmtRanges(rs []storage.KeyRange) string {
	if rs == nil {
		return "<full scan>"
	}
	var parts []string
	for _, r := range rs {
		s := fmt.Sprintf("{P:%v S:%v", r.Partition, r.SortPrefix)
		if r.Lower != nil {
			s += fmt.Sprintf(" L:%v/%v", r.Lower.Value, r.Lower.Inclusive)
		}
		if r.Upper != nil {
			s += fmt.Sprintf(" U:%v/%v", r.Upper.Value, r.Upper.Inclusive)
		}
		parts = append(parts, s+"}")
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func TestPushdownCap(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE ev (a String, b UInt32) ENGINE = MergeTree ORDER BY (a, b)`)
	var as, bs []string
	for i := 0; i < 101; i++ {
		as = append(as, fmt.Sprintf("'a%d'", i))
		bs = append(bs, fmt.Sprint(i))
	}
	// 101 x 101 > 10000: the sort-key prefix is dropped, partitions kept.
	te.exec(fmt.Sprintf("SELECT count() FROM ev WHERE a IN (%s) AND b IN (%s)", strings.Join(as, ","), strings.Join(bs, ",")))
	got := te.spy.lastScan().Ranges
	if len(got) != 101 || len(got[0].SortPrefix) != 0 {
		t.Fatalf("expected 101 partition-only ranges, got %d", len(got))
	}
	var many []string
	for i := 0; i <= maxKeyRanges; i++ {
		many = append(many, fmt.Sprintf("'k%d'", i))
	}
	te.exec(fmt.Sprintf("SELECT count() FROM ev WHERE a IN (%s)", strings.Join(many, ",")))
	if te.spy.lastScan().Ranges != nil {
		t.Fatal("expected a full scan above the range cap")
	}
}

func TestProjectionAndScanOptions(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE ev (tenant String, user_id UInt64, v String, w String, al String ALIAS concat(w, '!')) ENGINE = MergeTree ORDER BY (tenant, user_id)`)
	te.exec(`INSERT INTO ev VALUES ('a', 1, 'x', 'y')`)
	check := func(sql string, settings map[string]string, want storage.ScanRequest) {
		t.Helper()
		if _, err := te.runReq(&Request{Query: sql, Settings: settings}); err != nil {
			t.Fatal(err)
		}
		got := te.spy.lastScan()
		if !reflect.DeepEqual(got.Columns, want.Columns) || got.Consistent != want.Consistent || got.Limit != want.Limit {
			t.Errorf("%s: got columns %v consistent %v limit %d; want %v %v %d", sql, got.Columns, got.Consistent, got.Limit, want.Columns, want.Consistent, want.Limit)
		}
	}
	check(`SELECT v FROM ev WHERE tenant = 'a'`, nil, storage.ScanRequest{Columns: []int{0, 2}, Consistent: true})
	check(`SELECT count() FROM ev`, nil, storage.ScanRequest{Columns: []int{0}, Consistent: true})
	check(`SELECT al FROM ev`, nil, storage.ScanRequest{Columns: []int{3}, Consistent: true})
	check(`SELECT * FROM ev LIMIT 3 OFFSET 1`, nil, storage.ScanRequest{Columns: []int{0, 1, 2, 3}, Consistent: true, Limit: 4})
	check(`SELECT v FROM ev ORDER BY v LIMIT 3`, nil, storage.ScanRequest{Columns: []int{2}, Consistent: true})
	check(`SELECT v FROM ev`, map[string]string{"select_sequential_consistency": "0"}, storage.ScanRequest{Columns: []int{2}})
	check(`SELECT v FROM ev SETTINGS select_sequential_consistency = 0`, nil, storage.ScanRequest{Columns: []int{2}})
	te.e.cfg.ConsistentReads = false
	check(`SELECT v FROM ev`, nil, storage.ScanRequest{Columns: []int{2}})
	check(`SELECT v FROM ev`, map[string]string{"select_sequential_consistency": "1"}, storage.ScanRequest{Columns: []int{2}, Consistent: true})
	te.expectRows(`SELECT al FROM ev`, "y!")
}

// TestTupleInProbeSevenColumnKey probes a 7-column sorting key with an
// Array(Tuple) parameter, the typical "lookup these entities" query.
func TestTupleInProbeSevenColumnKey(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE q (org String, region LowCardinality(String), qid UInt32, bucket String, prio UInt8, owner String, at DateTime64(3), payload String)
		ENGINE = MergeTree ORDER BY (org, region, qid, bucket, prio, owner, at)`)
	var vals []string
	for i := 0; i < 40; i++ {
		vals = append(vals, fmt.Sprintf("('o%d', 'r%d', %d, 'b%d', %d, 'w%d', '2024-01-01 00:00:%02d.000', 'p%d')", i%3, i%2, i%5, i%4, i%2, i%3, i, i))
	}
	te.exec(`INSERT INTO q VALUES ` + strings.Join(vals, ", "))
	keys := "[('o0','r0',0,'b0',0,'w0','2024-01-01 00:00:00.000'),('o1','r1',1,'b1',1,'w1','2024-01-01 00:00:01.000'),('o2','r0',2,'b2',0,'w2','2024-01-01 00:00:02.000'),('o0','r1',3,'b3',1,'w0','2024-01-01 00:00:03.000'),('o0','r0',0,'b0',0,'w0','2024-01-01 00:00:59.000')]"
	sql := `SELECT payload FROM q WHERE (org, region, qid, bucket, prio, owner, at) IN {keys:Array(Tuple(String, String, UInt32, String, UInt8, String, DateTime64(3)))} ORDER BY payload`
	te.expectRows(sql, "p0\np1\np2\np3", "keys", keys)
	got := te.spy.lastScan().Ranges
	if len(got) != 5 {
		t.Fatalf("expected one range per probe tuple, got %s", fmtRanges(got))
	}
	for _, r := range got {
		if len(r.Partition) != 1 || len(r.SortPrefix) != 6 {
			t.Fatalf("expected full-key ranges, got %s", fmtRanges(got))
		}
	}
	// A prefix probe of the first three key columns.
	te.expectRows(`SELECT count() FROM q WHERE (org, region, qid) IN {k:Array(Tuple(String, String, UInt32))}`, "4", "k", "[('o0','r0',0),('o1','r1',1)]")
	if r := te.spy.lastScan().Ranges; len(r) != 2 || len(r[0].SortPrefix) != 2 {
		t.Fatalf("expected two prefix ranges, got %s", fmtRanges(r))
	}
}

func TestPushdownResultsMatchFullScan(t *testing.T) {
	// Every pushed-down query must return exactly what the unfiltered scan
	// plus filter returns.
	te := newTestEnv(t)
	te.exec(`CREATE TABLE ev (a String, b Int32, c DateTime, d String) ENGINE = MergeTree ORDER BY (a, b, c)`)
	var vals []string
	for i := 0; i < 60; i++ {
		vals = append(vals, fmt.Sprintf("('a%d', %d, toDateTime('2024-01-01 00:00:00') + %d, 'd%d')", i%4, i%7-3, i*3600, i%5))
	}
	te.exec(`INSERT INTO ev VALUES ` + strings.Join(vals, ", "))
	wheres := []string{
		"a = 'a1'", "a = 'a1' AND b = 0", "a IN ('a0', 'a3') AND b IN (-3, 3)", "a = 'a2' AND b >= -1 AND b < 2",
		"a = 'a2' AND b = 1 AND c > '2024-01-02 00:00:00'", "(a, b) IN (('a0', -3), ('a1', 2))", "a = 'a0' AND b BETWEEN -2 AND 2 AND d = 'd1'",
		"a = 'a1' AND b > 100", "a = 'a3' AND b < -3", "a = 'a1' AND (b = 1 OR b = 2)",
	}
	for _, w := range wheres {
		pushed := te.rows("SELECT a, b, c, d FROM ev WHERE " + w + " ORDER BY a, b, c, d")
		full := te.rows("SELECT a, b, c, d FROM (SELECT * FROM ev) WHERE " + w + " ORDER BY a, b, c, d")
		if pushed != full {
			t.Errorf("WHERE %s: pushdown result differs:\n%s\nvs\n%s", w, pushed, full)
		}
	}
}

func TestPushdownThroughJoins(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE ja (k String, v UInt8) ENGINE = MergeTree ORDER BY k`)
	te.exec(`CREATE TABLE jb (k String, w String) ENGINE = MergeTree ORDER BY k`)
	te.exec(`INSERT INTO ja VALUES ('a', 1), ('b', 2)`)
	te.exec(`INSERT INTO jb VALUES ('b', 'x'), ('c', 'y')`)
	scanOf := func(sql string) []storage.KeyRange {
		t.Helper()
		before := len(te.spy.scans)
		te.exec(sql)
		// Scans: the right (build) side first, then the left (probe) side.
		scans := te.spy.scans[before:]
		return scans[len(scans)-1].Ranges
	}
	if r := scanOf(`SELECT jb.w FROM ja JOIN jb ON ja.k = jb.k WHERE ja.k = 'b'`); len(r) != 1 || r[0].Partition[0] != "b" {
		t.Errorf("INNER JOIN left pushdown: %s", fmtRanges(r))
	}
	if r := scanOf(`SELECT jb.w FROM ja LEFT JOIN jb ON ja.k = jb.k WHERE ja.k = 'b'`); len(r) != 1 {
		t.Errorf("LEFT JOIN left pushdown: %s", fmtRanges(r))
	}
	if r := scanOf(`SELECT jb.w FROM ja RIGHT JOIN jb ON ja.k = jb.k WHERE ja.k = ''`); r != nil {
		t.Errorf("RIGHT JOIN must not push WHERE into the left scan: %s", fmtRanges(r))
	}
	te.expectRows(`SELECT jb.w FROM ja RIGHT JOIN jb ON ja.k = jb.k WHERE ja.k = ''`, "y")
	te.expectRows(`SELECT v FROM ja JOIN jb ON ja.k = jb.k LIMIT 1`, "2")
}
