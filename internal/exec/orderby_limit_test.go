package exec

import (
	"bytes"
	"strings"
	"testing"

	"github.com/gfronza/twinhouse/internal/format"
)

// Each case lists the rows and rows_before_limit a full stable sort followed
// by LIMIT gives.
func TestOrderByLimitMatchesFullSort(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE t (id UInt64, k Nullable(Int64), f Float64, s String) ENGINE = MergeTree ORDER BY id`)
	te.exec(`INSERT INTO t VALUES (1, 2, nan, 'b'), (2, NULL, 1, 'a'), (3, 2, -1, 'a'), (4, 1, nan, 'c'), (5, NULL, 0, 'b'), (6, 1, 1, 'a'), (7, 2, 1, 'b')`)
	for _, c := range []struct {
		sql, want string
		before    uint64
	}{
		{`SELECT id FROM t ORDER BY k LIMIT 3`, "4\n6\n1", 7},
		{`SELECT id FROM t ORDER BY k DESC LIMIT 4`, "1\n3\n7\n4", 7},
		{`SELECT id FROM t ORDER BY k NULLS FIRST LIMIT 3`, "2\n5\n4", 7},
		{`SELECT id FROM t ORDER BY k DESC NULLS FIRST, s LIMIT 3 OFFSET 1`, "5\n3\n1", 7},
		{`SELECT id FROM t ORDER BY f LIMIT 3`, "3\n5\n2", 7},
		{`SELECT id FROM t ORDER BY f DESC LIMIT 5`, "2\n6\n7\n5\n3", 7},
		{`SELECT id FROM t ORDER BY f NULLS FIRST LIMIT 2`, "1\n4", 7},
		{`SELECT id FROM t ORDER BY s, k DESC LIMIT 2, 3`, "2\n1\n7", 7},
		{`SELECT id FROM t ORDER BY id % 3 LIMIT 5 OFFSET 2`, "1\n4\n7\n2\n5", 7},
		{`SELECT id FROM t ORDER BY s LIMIT 0`, "", 7},
		{`SELECT id FROM t ORDER BY s LIMIT 100`, "2\n3\n6\n1\n5\n7\n4", 7},
		{`SELECT id FROM t ORDER BY s LIMIT 3 OFFSET 10`, "", 7},
		{`SELECT id FROM t ORDER BY s LIMIT 0 OFFSET 2`, "", 7},
		{`SELECT id FROM t ORDER BY s LIMIT 18446744073709551615 OFFSET 18446744073709551615`, "", 7},
		{`SELECT id FROM t ORDER BY s LIMIT 1000000000000000000`, "2\n3\n6\n1\n5\n7\n4", 7},
		{`SELECT id FROM t WHERE id > 100 ORDER BY s LIMIT 2`, "", 0},
		{`SELECT DISTINCT s FROM t ORDER BY s DESC LIMIT 2`, "c\nb", 3},
		{`SELECT id FROM t ORDER BY s LIMIT 1 BY s LIMIT 2`, "2\n1", 3},
		{`SELECT s, count() FROM t GROUP BY s ORDER BY count() DESC, s LIMIT 2`, "a\t3\nb\t3", 3},
	} {
		te.expectRows(c.sql, c.want)
		res := te.exec(c.sql)
		if !res.HasRowsBeforeLimit || res.RowsBeforeLimit != c.before {
			t.Errorf("%s: rows_before_limit %v %d, want %d", c.sql, res.HasRowsBeforeLimit, res.RowsBeforeLimit, c.before)
		}
	}
}

// rows_before_limit_at_least counts every sorted row, also those the top-N
// sort dropped.
func TestJSONRowsBeforeLimitCountsDroppedRows(t *testing.T) {
	te := newTestEnv(t)
	for _, c := range []struct {
		sql  string
		want string
	}{
		{`SELECT number FROM numbers(100) ORDER BY number DESC LIMIT 3`, `"rows_before_limit_at_least": 100`},
		{`SELECT number FROM numbers(100) ORDER BY number LIMIT 3 OFFSET 5`, `"rows_before_limit_at_least": 100`},
		{`SELECT number FROM numbers(100) ORDER BY number LIMIT 0`, `"rows_before_limit_at_least": 100`},
		{`SELECT number % 7 AS k, count() FROM numbers(100) GROUP BY k ORDER BY k LIMIT 2`, `"rows_before_limit_at_least": 7`},
	} {
		res := te.exec(c.sql)
		var buf bytes.Buffer
		enc, err := format.NewEncoder("JSON", &buf, res.Columns, format.Options{Timezone: "UTC"})
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.WriteRows(res.Rows); err != nil {
			t.Fatal(err)
		}
		if err := enc.Finish(format.Stats{HasRowsBeforeLimit: res.HasRowsBeforeLimit, RowsBeforeLimit: res.RowsBeforeLimit}); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(buf.String(), c.want) {
			t.Errorf("%s: JSON lacks %s:\n%s", c.sql, c.want, buf.String())
		}
	}
}

// A result cap that stops reading early sees only the rows it read, as
// before top-N.
func TestOrderByLimitWithResultCapCountsReadRows(t *testing.T) {
	te := newTestEnv(t)
	res := te.exec(`SELECT number FROM numbers(100) ORDER BY number DESC LIMIT 10 SETTINGS max_result_rows = 3, result_overflow_mode = 'break'`)
	eq(t, len(res.Rows), 3)
	eq(t, res.RowsBeforeLimit, uint64(4))
}

func BenchmarkOrderByLimit(b *testing.B) {
	te := newTestEnv(b)
	te.exec(`CREATE TABLE t (k String, v UInt64) ENGINE = MergeTree ORDER BY k`)
	te.exec(`INSERT INTO t SELECT toString(number % 1000), number FROM numbers(100000)`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		te.exec(`SELECT k, v FROM t ORDER BY v % 997 DESC, k LIMIT 10 OFFSET 5`)
	}
	b.ReportMetric(float64(b.N*100000)/b.Elapsed().Seconds(), "rows/s")
}

// Limits near or past the row count: OFFSET paging over presorted input,
// reversed and shuffled input, and a LIMIT that keeps every row.
func BenchmarkOrderByLargeLimit(b *testing.B) {
	te := newTestEnv(b)
	for _, c := range []struct{ name, sql string }{
		{"PresortedOffset", `SELECT number FROM numbers(1000000) ORDER BY number LIMIT 10 OFFSET 900000`},
		{"ReversedNearAll", `SELECT number FROM numbers(1000000) ORDER BY number DESC LIMIT 900000`},
		{"ShuffledNearAll", `SELECT number FROM numbers(1000000) ORDER BY number * 2654435761 % 1000003 LIMIT 900000`},
		{"PastAll", `SELECT number FROM numbers(1000000) ORDER BY number DESC LIMIT 2000000`},
	} {
		b.Run(c.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				te.exec(c.sql)
			}
		})
	}
}
