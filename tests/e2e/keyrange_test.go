//go:build e2e

package e2e

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestKeyRangePushdown checks predicates that the planner turns into
// DynamoDB key conditions (partition equality/IN, sort-key prefix and
// ranges, tuple IN) — including tricky key encodings (embedded NUL bytes,
// invalid UTF-8, negative integers and floats, pre-1970 times) and
// predicates that must NOT be pushed down (OR, NOT, functions of keys).
// Expected results are computed in Go from the inserted rows.
func TestKeyRangePushdown(t *testing.T) {
	t.Parallel()
	db := newDB(t, "keyrange")
	mustExec(t, db, "CREATE TABLE kr (p String, n Int64, s String, f Float64, v UInt64) ENGINE = MergeTree ORDER BY (p, n, s)")
	mustExec(t, db, "CREATE TABLE kf (p String, f Float64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY (p, f)")
	mustExec(t, db, "CREATE TABLE kt (p String, ts DateTime64(3), d DateTime, v UInt64) ENGINE = MergeTree ORDER BY (p, ts)")

	type row struct {
		p, s string
		n    int64
		v    uint64
	}
	ps := []string{"a", "a\x00", "a\x00b", "ab", "", "ü"}
	ss := []string{"", "x", "x\x00", "xy", "\xff"}
	sqlStr := func(s string) string {
		r := strings.NewReplacer(`\`, `\\`, `'`, `\'`, "\x00", `\0`, "\xff", `\xFF`)
		return "'" + r.Replace(s) + "'"
	}
	var rows []row
	var vals []string
	var v uint64
	for _, p := range ps {
		for n := int64(-3); n <= 3; n++ {
			for _, s := range ss {
				v++
				rows = append(rows, row{p, s, n, v})
				vals = append(vals, fmt.Sprintf("(%s, %d, %s, %d.5, %d)", sqlStr(p), n, sqlStr(s), n, v))
			}
		}
	}
	mustExec(t, db, "INSERT INTO kr VALUES "+strings.Join(vals, ","))

	cases := []struct {
		where string
		match func(r row) bool
	}{
		{"p = 'a'", func(r row) bool { return r.p == "a" }},
		{`p = 'a\0'`, func(r row) bool { return r.p == "a\x00" }},
		{`p = 'a\0b'`, func(r row) bool { return r.p == "a\x00b" }},
		{"p = ''", func(r row) bool { return r.p == "" }},
		{"p IN ('a', 'ab', 'zz')", func(r row) bool { return r.p == "a" || r.p == "ab" }},
		{"p = 'a' AND n = 0", func(r row) bool { return r.p == "a" && r.n == 0 }},
		{"p = 'a' AND n > -2 AND n <= 2", func(r row) bool { return r.p == "a" && r.n > -2 && r.n <= 2 }},
		{"p = 'a' AND n BETWEEN -3 AND -1", func(r row) bool { return r.p == "a" && r.n >= -3 && r.n <= -1 }},
		{"p = 'a' AND n >= 2 AND n >= 1", func(r row) bool { return r.p == "a" && r.n >= 2 }},
		{"p = 'a' AND n > 1 AND n < 2", func(r row) bool { return false }},
		{"p = 'a' AND n > -1.5", func(r row) bool { return r.p == "a" && float64(r.n) > -1.5 }},
		{"p = 'a' AND n < 1.5", func(r row) bool { return r.p == "a" && float64(r.n) < 1.5 }},
		{"p = 'a' AND n <= -0.5", func(r row) bool { return r.p == "a" && float64(r.n) <= -0.5 }},
		{"p = 'a' AND n IN (-1, 2)", func(r row) bool { return r.p == "a" && (r.n == -1 || r.n == 2) }},
		{"p = 'a' AND n != 0", func(r row) bool { return r.p == "a" && r.n != 0 }},
		{"p = 'a' AND n = 1 AND s = 'x'", func(r row) bool { return r.p == "a" && r.n == 1 && r.s == "x" }},
		{`p = 'a' AND n = 1 AND s = 'x\0'`, func(r row) bool { return r.p == "a" && r.n == 1 && r.s == "x\x00" }},
		{"p = 'a' AND n = 1 AND s >= 'x' AND s < 'xz'", func(r row) bool { return r.p == "a" && r.n == 1 && r.s >= "x" && r.s < "xz" }},
		{"p = 'a' AND n = 1 AND s > 'x'", func(r row) bool { return r.p == "a" && r.n == 1 && r.s > "x" }},
		{"p = 'a' AND n = -2 AND s < 'x'", func(r row) bool { return r.p == "a" && r.n == -2 && r.s < "x" }},
		{`p = 'ab' AND n = 3 AND s = '\xFF'`, func(r row) bool { return r.p == "ab" && r.n == 3 && r.s == "\xff" }},
		{`p = 'ab' AND n = 3 AND s > 'xy'`, func(r row) bool { return r.p == "ab" && r.n == 3 && r.s > "xy" }},
		{"p = 'a' AND s = 'x'", func(r row) bool { return r.p == "a" && r.s == "x" }},
		{"(p, n) IN (('a', 1), ('ab', -3), ('zz', 0))", func(r row) bool { return (r.p == "a" && r.n == 1) || (r.p == "ab" && r.n == -3) }},
		{`(p, n, s) IN (('a', 1, 'x\0'), ('a\0b', 2, ''), ('a', 1, 'nope'))`, func(r row) bool {
			return (r.p == "a" && r.n == 1 && r.s == "x\x00") || (r.p == "a\x00b" && r.n == 2 && r.s == "")
		}},
		{"(n, p) IN ((1, 'a'), (2, 'ü'))", func(r row) bool { return (r.p == "a" && r.n == 1) || (r.p == "ü" && r.n == 2) }},
		{"p = 'a' OR n = 3", func(r row) bool { return r.p == "a" || r.n == 3 }},
		{"p = 'a' AND (n = 1 OR s = 'xy')", func(r row) bool { return r.p == "a" && (r.n == 1 || r.s == "xy") }},
		{"(p = 'a' AND n > 2) OR (p = 'ab' AND n < -2)", func(r row) bool { return (r.p == "a" && r.n > 2) || (r.p == "ab" && r.n < -2) }},
		{"NOT (p = 'a')", func(r row) bool { return r.p != "a" }},
		{"p > 'a' AND p < 'b'", func(r row) bool { return r.p > "a" && r.p < "b" }},
		{"p = 'ü' AND s LIKE 'x%'", func(r row) bool { return r.p == "ü" && strings.HasPrefix(r.s, "x") }},
		{"concat(p, '') = 'a' AND n + 0 = 1", func(r row) bool { return r.p == "a" && r.n == 1 }},
		{"p = 'a' AND p = 'ab'", func(r row) bool { return false }},
		{"p IN ('a') AND p IN ('a', 'ab') AND n IN (1, 2) AND n IN (2, 3)", func(r row) bool { return r.p == "a" && r.n == 2 }},
		{"p = 'a' AND n = 1 AND f = 1.5", func(r row) bool { return r.p == "a" && r.n == 1 }},
	}
	for _, c := range cases {
		c := c
		t.Run(c.where, func(t *testing.T) {
			t.Parallel()
			var want []string
			for _, r := range rows {
				if c.match(r) {
					want = append(want, fmt.Sprint(r.v))
				}
			}
			sort.Slice(want, func(i, j int) bool {
				return len(want[i]) < len(want[j]) || len(want[i]) == len(want[j]) && want[i] < want[j]
			})
			got := tsv(t, db, "SELECT v FROM kr WHERE "+c.where+" ORDER BY v")
			w := strings.Join(want, "\n")
			if w != "" {
				w += "\n"
			}
			if got != w {
				t.Errorf("WHERE %s:\n got  %q\n want %q", c.where, strings.ReplaceAll(got, "\n", ","), strings.ReplaceAll(w, "\n", ","))
			}
		})
	}

	t.Run("order_limit_on_sort_key", func(t *testing.T) {
		got := tsv(t, db, "SELECT n, v FROM kr WHERE p = 'ab' AND n < 0 ORDER BY n DESC, s DESC LIMIT 4")
		// p='ab' rows for n=-1 sorted by s DESC: \xff, xy, x\0, x
		var want []string
		var abRows []row
		for _, r := range rows {
			if r.p == "ab" && r.n < 0 {
				abRows = append(abRows, r)
			}
		}
		sort.Slice(abRows, func(i, j int) bool {
			if abRows[i].n != abRows[j].n {
				return abRows[i].n > abRows[j].n
			}
			return abRows[i].s > abRows[j].s
		})
		for _, r := range abRows[:4] {
			want = append(want, fmt.Sprintf("%d\t%d", r.n, r.v))
		}
		if got != strings.Join(want, "\n")+"\n" {
			t.Errorf("ORDER BY sort key DESC LIMIT:\n%s\nwant\n%s", got, strings.Join(want, "\n"))
		}
		got = tsv(t, db, "SELECT count() FROM (SELECT v FROM kr WHERE p = 'a' AND s = 'xy' LIMIT 3)")
		if got != "3\n" {
			t.Errorf("LIMIT under filter: %q", got)
		}
		got = tsv(t, db, "SELECT p = 'a\\0', length(p), n, s = 'x\\0', length(s) FROM kr WHERE p = 'a\\0' AND n = 0 AND s = 'x\\0'")
		if got != "1\t2\t0\t1\t2\n" {
			t.Errorf("NUL round trip: %q", got)
		}
	})

	t.Run("float_sort_key", func(t *testing.T) {
		fs := []float64{-1e300, -2.5, -1, -0.5, 0, 0.25, 1, 3.75, 1e300, math.Inf(1), math.Inf(-1)}
		var fv []string
		for i, f := range fs {
			lit := fmt.Sprint(f)
			if math.IsInf(f, 1) {
				lit = "inf"
			} else if math.IsInf(f, -1) {
				lit = "-inf"
			}
			fv = append(fv, fmt.Sprintf("('k', %s, %d)", lit, i))
		}
		mustExec(t, db, "INSERT INTO kf VALUES "+strings.Join(fv, ","))
		for _, c := range []struct {
			where string
			match func(f float64) bool
		}{
			{"f > -1", func(f float64) bool { return f > -1 }},
			{"f >= -2.5 AND f < 0.25", func(f float64) bool { return f >= -2.5 && f < 0.25 }},
			{"f < 0", func(f float64) bool { return f < 0 }},
			{"f = 0", func(f float64) bool { return f == 0 }},
			{"f BETWEEN -1e301 AND 1e301", func(f float64) bool { return f >= -1e301 && f <= 1e301 }},
			{"f > 1e300", func(f float64) bool { return f > 1e300 }},
		} {
			var want []string
			for i, f := range fs {
				if c.match(f) {
					want = append(want, fmt.Sprint(i))
				}
			}
			sort.Strings(want)
			got := tsv(t, db, "SELECT toString(v) AS x FROM kf WHERE p = 'k' AND "+c.where+" ORDER BY x")
			if got != strings.Join(want, "\n")+strings.Repeat("\n", min(1, len(want))) {
				t.Errorf("float key WHERE %s: got %q want %v", c.where, got, want)
			}
		}
		if got := tsv(t, db, "SELECT v FROM kf WHERE p = 'k' ORDER BY f"); got != "10\n0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n" {
			t.Errorf("ORDER BY float key: %q", strings.ReplaceAll(got, "\n", ","))
		}
	})

	t.Run("time_sort_key", func(t *testing.T) {
		times := []time.Time{
			time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(1969, 12, 31, 23, 59, 59, 999e6, time.UTC),
			time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(1970, 1, 1, 0, 0, 0, 1e6, time.UTC),
			time.Date(2024, 5, 6, 12, 0, 0, 0, time.UTC),
			time.Date(2024, 5, 6, 12, 0, 0, 1e6, time.UTC),
			time.Date(2024, 5, 6, 12, 0, 1, 0, time.UTC),
			time.Date(2099, 12, 31, 23, 59, 59, 999e6, time.UTC),
		}
		var tv []string
		for i, ts := range times {
			d := ts
			if d.Year() < 1970 {
				d = time.Unix(0, 0).UTC()
			}
			tv = append(tv, fmt.Sprintf("('k', '%s', '%s', %d)", ts.Format("2006-01-02 15:04:05.000"), d.Format(time.DateTime), i))
		}
		mustExec(t, db, "INSERT INTO kt VALUES "+strings.Join(tv, ","))
		for _, c := range []struct {
			where string
			match func(ts time.Time) bool
		}{
			{"ts > '2024-05-06 12:00:00'", func(ts time.Time) bool { return ts.After(times[4]) }},
			{"ts >= '2024-05-06 12:00:00' AND ts <= '2024-05-06 12:00:00.001'", func(ts time.Time) bool { return !ts.Before(times[4]) && !ts.After(times[5]) }},
			{"ts < '1970-01-01 00:00:00'", func(ts time.Time) bool { return ts.Before(times[2]) }},
			{"ts BETWEEN '1960-01-01 00:00:00' AND '1970-01-01 00:00:00.000'", func(ts time.Time) bool { return !ts.After(times[2]) }},
			{"ts = '1970-01-01 00:00:00.001'", func(ts time.Time) bool { return ts.Equal(times[3]) }},
			{"ts > toDateTime('2024-05-06 12:00:00')", func(ts time.Time) bool { return ts.After(times[4]) }},
			{"ts < toDateTime64('2024-05-06 12:00:00.001', 3)", func(ts time.Time) bool { return ts.Before(times[5]) }},
		} {
			var want []string
			for i, ts := range times {
				if c.match(ts) {
					want = append(want, fmt.Sprint(i))
				}
			}
			got := tsv(t, db, "SELECT v FROM kt WHERE p = 'k' AND "+c.where+" ORDER BY v")
			if got != strings.Join(want, "\n")+strings.Repeat("\n", min(1, len(want))) {
				t.Errorf("time key WHERE %s: got %q want %v", c.where, got, want)
			}
		}
		if got := tsv(t, db, "SELECT ts FROM kt WHERE p = 'k' AND v IN (0, 1, 3)  ORDER BY ts"); got != "1960-01-01 00:00:00.000\n1969-12-31 23:59:59.999\n1970-01-01 00:00:00.001\n" {
			t.Errorf("pre-1970 DateTime64 round trip: %q", got)
		}
	})
}

// TestLargePartitionPagination: a single partition larger than one DynamoDB
// page (1 MB) read by key, plus a full scan of several partitions.
func TestLargePartitionPagination(t *testing.T) {
	t.Parallel()
	heavy(t)
	db := newDB(t, "pagination")
	mustExec(t, db, "CREATE TABLE blobs (tenant_id String, seq UInt64, payload String) ENGINE = MergeTree ORDER BY (tenant_id, seq)")
	payload := strings.Repeat("x", 1200)
	var sb strings.Builder
	const n = 1250
	var wantSum uint64
	for i := 0; i < n; i++ {
		tenant := "big"
		if i%5 == 0 {
			tenant = fmt.Sprintf("small-%d", i%3)
		}
		if tenant == "big" {
			wantSum += uint64(i)
		}
		fmt.Fprintf(&sb, `{"tenant_id":%q,"seq":%d,"payload":"%s%d"}`+"\n", tenant, i, payload, i)
	}
	r := doHTTP(t, req{Params: map[string][]string{"query": {"INSERT INTO " + db + ".blobs FORMAT JSONEachRow"}},
		Body: strings.NewReader(sb.String())})
	if r.Status != 200 {
		t.Fatalf("insert: %d %s", r.Status, r.Body)
	}
	if got, want := tsv(t, db, "SELECT count(), sum(seq), max(length(payload)) FROM blobs WHERE tenant_id = 'big'"), fmt.Sprintf("%d\t%d\t%d\n", n*4/5, wantSum, len(payload)+4); got != want {
		t.Errorf("big partition: got %q want %q", got, want)
	}
	if got := tsv(t, db, "SELECT count() FROM blobs"); got != fmt.Sprintf("%d\n", n) {
		t.Errorf("full scan count = %q", got)
	}
	if got := tsv(t, db, "SELECT seq FROM blobs WHERE tenant_id = 'big' AND seq > 1240 ORDER BY seq"); got != "1241\n1242\n1243\n1244\n1246\n1247\n1248\n1249\n" {
		t.Errorf("range tail: %q", got)
	}
	conn := openNative(t, db)
	rows, err := conn.Query(ctxT(t), "SELECT tenant_id, seq, payload FROM blobs ORDER BY seq")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	i := 0
	for rows.Next() {
		var tn, p string
		var seq uint64
		if err := rows.Scan(&tn, &seq, &p); err != nil {
			t.Fatal(err)
		}
		if seq != uint64(i) || p != fmt.Sprintf("%s%d", payload, i) {
			t.Fatalf("row %d: seq=%d len(payload)=%d", i, seq, len(p))
		}
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if i != n {
		t.Errorf("streamed %d rows, want %d", i, n)
	}
}
