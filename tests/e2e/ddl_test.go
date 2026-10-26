//go:build e2e

package e2e

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func queryStrings(t *testing.T, conn *sql.DB, q string, args ...any) []string {
	t.Helper()
	rows, err := conn.QueryContext(ctxT(t), q, args...)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("%s scan: %v", q, err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s rows: %v", q, err)
	}
	return out
}

func TestDDL(t *testing.T) {
	t.Parallel()
	db := newDB(t, "ddl")
	conn := openSQL(t, db)
	ctx := ctxT(t)
	createSchema(t, db)

	t.Run("show_tables", func(t *testing.T) {
		got := queryStrings(t, conn, "SHOW TABLES")
		want := []string{"ad_events", "audience_ops", "campaign_daily", "campaign_hourly", "campaigns", "mv_campaign_daily", "mv_campaign_hourly", "tenants"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("SHOW TABLES = %v, want %v", got, want)
		}
		got = queryStrings(t, conn, fmt.Sprintf("SHOW TABLES FROM %s LIKE 'campaign%%'", db))
		want = []string{"campaign_daily", "campaign_hourly", "campaigns"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("SHOW TABLES LIKE = %v, want %v", got, want)
		}
		got = queryStrings(t, conn, "SELECT name FROM system.tables WHERE database = ? AND engine = 'MaterializedView' ORDER BY name", db)
		if strings.Join(got, ",") != "mv_campaign_daily,mv_campaign_hourly" {
			t.Errorf("system.tables MVs = %v", got)
		}
	})

	t.Run("show_databases", func(t *testing.T) {
		got := queryStrings(t, conn, "SHOW DATABASES")
		if !slices.Contains(got, db) || !slices.Contains(got, "default") || !slices.Contains(got, "system") {
			t.Errorf("SHOW DATABASES = %v, missing %s/default/system", got, db)
		}
		got = queryStrings(t, conn, "SELECT name FROM system.databases WHERE name = ?", db)
		if len(got) != 1 {
			t.Errorf("system.databases = %v", got)
		}
	})

	t.Run("describe", func(t *testing.T) {
		rows, err := conn.QueryContext(ctx, "DESCRIBE TABLE ad_events")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var got []string
		for rows.Next() {
			var name, typ, dt, de, cm, codec, ttl string
			if err := rows.Scan(&name, &typ, &dt, &de, &cm, &codec, &ttl); err != nil {
				t.Fatal(err)
			}
			got = append(got, name+" "+typ)
		}
		want := []string{"tenant_id String", "event_time DateTime", "event_id String", "campaign_id String", "user_id String",
			"event_type LowCardinality(String)", "cost_micros UInt64", "is_bot UInt8", "country Nullable(String)", "client_ts DateTime64(3)"}
		if strings.Join(got, "|") != strings.Join(want, "|") {
			t.Errorf("DESCRIBE =\n%v\nwant\n%v", got, want)
		}
		out := tsv(t, db, "DESCRIBE campaign_hourly FORMAT TSV")
		for _, w := range []string{"events\tSimpleAggregateFunction(sum, UInt64)", "last_seen\tSimpleAggregateFunction(max, DateTime)", "users\tAggregateFunction(uniqExact, String)"} {
			if !strings.Contains(out, w) {
				t.Errorf("DESCRIBE campaign_hourly missing %q:\n%s", w, out)
			}
		}
	})

	t.Run("exists", func(t *testing.T) {
		var n uint8
		if err := conn.QueryRowContext(ctx, "EXISTS TABLE campaigns").Scan(&n); err != nil || n != 1 {
			t.Errorf("EXISTS campaigns = %d, %v", n, err)
		}
		if err := conn.QueryRowContext(ctx, "EXISTS "+db+".no_such_table").Scan(&n); err != nil || n != 0 {
			t.Errorf("EXISTS no_such_table = %d, %v", n, err)
		}
		if err := conn.QueryRowContext(ctx, "EXISTS DATABASE "+db).Scan(&n); err != nil || n != 1 {
			t.Errorf("EXISTS DATABASE = %d, %v", n, err)
		}
	})

	t.Run("show_create", func(t *testing.T) {
		var stmt string
		if err := conn.QueryRowContext(ctx, "SHOW CREATE TABLE campaigns").Scan(&stmt); err != nil {
			t.Fatal(err)
		}
		for _, w := range []string{"CREATE TABLE " + db + ".campaigns", "`budget` UInt64", "`tags` Array(String)", "ENGINE = ReplacingMergeTree(version)", "ORDER BY campaign_id"} {
			if !strings.Contains(stmt, w) {
				t.Errorf("SHOW CREATE TABLE missing %q:\n%s", w, stmt)
			}
		}
		// The statement must round-trip: re-run it under another name.
		re := strings.Replace(stmt, db+".campaigns", db+".campaigns_copy", 1)
		if _, err := conn.ExecContext(ctx, re); err != nil {
			t.Errorf("re-executing SHOW CREATE output: %v\n%s", err, re)
		}
		if err := conn.QueryRowContext(ctx, "SHOW CREATE TABLE campaign_daily").Scan(&stmt); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(stmt, "SummingMergeTree") {
			t.Errorf("SHOW CREATE campaign_daily: %s", stmt)
		}
	})

	t.Run("if_not_exists", func(t *testing.T) {
		if _, err := conn.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS tenants (x String) ENGINE = MergeTree ORDER BY x"); err != nil {
			t.Errorf("CREATE TABLE IF NOT EXISTS existing: %v", err)
		}
		// Original definition kept.
		var stmt string
		_ = conn.QueryRowContext(ctx, "SHOW CREATE TABLE tenants").Scan(&stmt)
		if !strings.Contains(stmt, "`plan`") {
			t.Errorf("IF NOT EXISTS replaced the table: %s", stmt)
		}
		if _, err := conn.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+db); err != nil {
			t.Errorf("CREATE DATABASE IF NOT EXISTS: %v", err)
		}
		if _, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS no_such_table"); err != nil {
			t.Errorf("DROP TABLE IF EXISTS: %v", err)
		}
		if _, err := conn.ExecContext(ctx, "DROP DATABASE IF EXISTS e2e_never_created_"+runID); err != nil {
			t.Errorf("DROP DATABASE IF EXISTS: %v", err)
		}
		if _, err := conn.ExecContext(ctx, "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_campaign_daily TO campaign_daily AS SELECT tenant_id FROM ad_events"); err != nil {
			t.Errorf("CREATE MATERIALIZED VIEW IF NOT EXISTS: %v", err)
		}
	})

	t.Run("errors", func(t *testing.T) {
		_, err := conn.ExecContext(ctx, "CREATE TABLE tenants (x String) ENGINE = MergeTree ORDER BY x")
		wantCode(t, err, 57, "CREATE existing table")
		_, err = conn.QueryContext(ctx, "SELECT * FROM no_such_table")
		wantCode(t, err, 60, "SELECT unknown table")
		_, err = conn.ExecContext(ctx, "DROP TABLE no_such_table")
		wantCode(t, err, 60, "DROP unknown table")
		_, err = conn.QueryContext(ctx, "SELEC 1")
		wantCode(t, err, 62, "syntax error")
		_, err = conn.QueryContext(ctx, "SELECT * FROM e2e_no_such_db_"+runID+".t")
		wantCode(t, err, 81, "unknown database")
		_, err = conn.ExecContext(ctx, "CREATE TABLE e2e_no_such_db_"+runID+".t (x String) ENGINE = MergeTree ORDER BY x")
		wantCode(t, err, 81, "create in unknown database")
		_, err = conn.ExecContext(ctx, "CREATE DATABASE "+db)
		wantCode(t, err, 82, "create existing database")
		_, err = conn.QueryContext(ctx, "SELECT no_such_column FROM tenants")
		wantCode(t, err, 47, "unknown identifier")
		_, err = conn.QueryContext(ctx, "SELECT no_such_function_xyz(1)")
		wantCode(t, err, 46, "unknown function")
		_, err = conn.ExecContext(ctx, "CREATE TABLE bad_engine (x String) ENGINE = NoSuchEngine ORDER BY x")
		wantCode(t, err, 56, "unknown engine")

		// The native API surfaces the same exception.
		nconn := openNative(t, db)
		err = nconn.Exec(ctxT(t), "SELECT * FROM no_such_table")
		wantCode(t, err, 60, "native: unknown table")
		_, err = nconn.Query(ctxT(t), "SELECT * FROM e2e_no_such_db_"+runID+".x")
		wantCode(t, err, 81, "native: unknown database")
	})

	t.Run("truncate_drop", func(t *testing.T) {
		mustExec(t, db, "INSERT INTO tenants VALUES ('t1','pro','us'),('t2','free','eu')")
		var n uint64
		if err := conn.QueryRowContext(ctx, "SELECT count() FROM tenants").Scan(&n); err != nil || n != 2 {
			t.Fatalf("count before truncate = %d, %v", n, err)
		}
		if _, err := conn.ExecContext(ctx, "TRUNCATE TABLE tenants"); err != nil {
			t.Fatalf("TRUNCATE: %v", err)
		}
		if err := conn.QueryRowContext(ctx, "SELECT count() FROM tenants").Scan(&n); err != nil || n != 0 {
			t.Errorf("count after truncate = %d, %v", n, err)
		}
		mustExec(t, db, "INSERT INTO tenants VALUES ('t3','pro','us')")
		if err := conn.QueryRowContext(ctx, "SELECT count() FROM tenants").Scan(&n); err != nil || n != 1 {
			t.Errorf("count after truncate+insert = %d, %v", n, err)
		}
		if _, err := conn.ExecContext(ctx, "TRUNCATE TABLE IF EXISTS no_such_table"); err != nil {
			t.Errorf("TRUNCATE IF EXISTS: %v", err)
		}
		if _, err := conn.ExecContext(ctx, "DROP TABLE campaigns_copy"); err != nil {
			t.Errorf("DROP TABLE: %v", err)
		}
		if _, err := conn.ExecContext(ctx, "DROP VIEW mv_campaign_daily"); err != nil {
			t.Errorf("DROP VIEW: %v", err)
		}
		got := queryStrings(t, conn, "SHOW TABLES")
		if slices.Contains(got, "campaigns_copy") || slices.Contains(got, "mv_campaign_daily") {
			t.Errorf("dropped objects still listed: %v", got)
		}
		var e uint8
		_ = conn.QueryRowContext(ctx, "EXISTS TABLE campaigns_copy").Scan(&e)
		if e != 0 {
			t.Errorf("EXISTS after DROP = %d", e)
		}
		// Re-create with the same name works after DROP.
		if _, err := conn.ExecContext(ctx, "CREATE TABLE campaigns_copy (id String, v UInt64) ENGINE = MergeTree ORDER BY id"); err != nil {
			t.Fatalf("re-create after drop: %v", err)
		}
		mustExec(t, db, "INSERT INTO campaigns_copy VALUES ('a', 1)")
		if err := conn.QueryRowContext(ctx, "SELECT count() FROM campaigns_copy").Scan(&n); err != nil || n != 1 {
			t.Errorf("re-created table count = %d, %v (old rows must not resurface)", n, err)
		}
	})

	t.Run("drop_database", func(t *testing.T) {
		other := fmt.Sprintf("e2e_%s_ddl_tmp", runID)
		mustExec(t, "", "CREATE DATABASE "+other)
		mustExec(t, "", "CREATE TABLE "+other+".x (k String) ENGINE = MergeTree ORDER BY k")
		mustExec(t, "", "INSERT INTO "+other+".x VALUES ('a')")
		mustExec(t, "", "DROP DATABASE "+other)
		got := queryStrings(t, conn, "SHOW DATABASES")
		if slices.Contains(got, other) {
			t.Errorf("dropped database still listed")
		}
		_, err := conn.QueryContext(ctx, "SELECT * FROM "+other+".x")
		wantCode(t, err, 81, "select from dropped database")
		_, err = conn.ExecContext(ctx, "DROP DATABASE "+other)
		wantCode(t, err, 81, "drop dropped database")
	})
}
