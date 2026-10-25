//go:build e2e

package e2e

import (
	"strings"
	"testing"
)

// TestSQLSemantics runs a set of queries over a small hand-made dataset and
// compares the exact TabSeparated output (verified against ClickHouse 25.8),
// then re-runs each through clickhouse-go (Native over HTTP) to check the
// row count and that every result type decodes.
func TestSQLSemantics(t *testing.T) {
	t.Parallel()
	db := newDB(t, "sem")
	for _, s := range semSetup {
		mustExec(t, "", strings.ReplaceAll(s, "{db}", db))
	}
	conn := openNative(t, db)
	for _, c := range semCases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			r := post(t, db, c.query)
			if r.Status != 200 {
				t.Fatalf("%s\nHTTP %d: %s", c.query, r.Status, r.Body)
			}
			if r.Body != c.want {
				t.Errorf("%s\n got:\n%s\nwant:\n%s", c.query, r.Body, c.want)
			}
			rows, err := conn.Query(ctxT(t), c.query)
			if err != nil {
				t.Fatalf("clickhouse-go: %v", err)
			}
			defer rows.Close()
			cols := rows.ColumnTypes()
			n := 0
			for rows.Next() {
				vals := make([]any, len(cols))
				for i := range vals {
					vals[i] = newScanDest(cols[i].ScanType())
				}
				if err := rows.Scan(vals...); err != nil {
					t.Fatalf("clickhouse-go scan: %v", err)
				}
				n++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("clickhouse-go rows: %v", err)
			}
			if want := strings.Count(c.want, "\n"); n != want {
				t.Errorf("clickhouse-go returned %d rows, want %d", n, want)
			}
		})
	}
}
