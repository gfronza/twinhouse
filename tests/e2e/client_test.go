//go:build e2e

package e2e

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// compressionVariants are the client options an app may use over HTTP.
var compressionVariants = []struct {
	name string
	comp *clickhouse.Compression
}{
	{"none", nil},
	{"lz4", &clickhouse.Compression{Method: clickhouse.CompressionLZ4}},
	{"zstd", &clickhouse.Compression{Method: clickhouse.CompressionZSTD}},
	{"gzip", &clickhouse.Compression{Method: clickhouse.CompressionGZIP}},
	{"deflate", &clickhouse.Compression{Method: clickhouse.CompressionDeflate}},
	{"brotli", &clickhouse.Compression{Method: clickhouse.CompressionBrotli}},
}

func TestClientHandshakeSQL(t *testing.T) {
	t.Parallel()
	db := newDB(t, "hs_sql")
	mustExec(t, db, "CREATE TABLE ev (tenant_id String, n UInt64, ts DateTime) ENGINE = MergeTree ORDER BY (tenant_id, ts)")
	for _, cv := range compressionVariants {
		cv := cv
		t.Run(cv.name, func(t *testing.T) {
			t.Parallel()
			conn := openSQL(t, db, func(o *clickhouse.Options) { o.Compression = cv.comp })
			ctx := ctxT(t)
			if err := conn.PingContext(ctx); err != nil {
				t.Fatalf("ping: %v", err)
			}
			var v string
			if err := conn.QueryRowContext(ctx, "SELECT version()").Scan(&v); err != nil {
				t.Fatalf("version: %v", err)
			}
			if !strings.HasPrefix(v, "25.") {
				t.Errorf("version() = %q, want 25.x", v)
			}
			var db2 string
			if err := conn.QueryRowContext(ctx, "SELECT currentDatabase()").Scan(&db2); err != nil {
				t.Fatalf("currentDatabase: %v", err)
			}
			if db2 != db {
				t.Errorf("currentDatabase() = %q, want %q", db2, db)
			}
			tenant := "t_" + cv.name
			// Write through the client with this compression, then read back.
			tx, err := conn.Begin()
			if err != nil {
				t.Fatal(err)
			}
			st, err := tx.Prepare("INSERT INTO ev (tenant_id, n, ts)")
			if err != nil {
				t.Fatalf("prepare: %v", err)
			}
			base := time.Date(2024, 3, 1, 10, 0, 0, 0, time.UTC)
			var want uint64
			for i := 0; i < 300; i++ {
				want += uint64(i)
				if _, err := st.Exec(tenant, uint64(i), base.Add(time.Duration(i)*time.Second)); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("commit: %v", err)
			}
			var cnt, sum uint64
			var maxTS time.Time
			if err := conn.QueryRowContext(ctx, "SELECT count(), sum(n), max(ts) FROM ev WHERE tenant_id = ?", tenant).Scan(&cnt, &sum, &maxTS); err != nil {
				t.Fatalf("select: %v", err)
			}
			if cnt != 300 || sum != want || !maxTS.Equal(base.Add(299*time.Second)) {
				t.Errorf("got count=%d sum=%d max=%s, want 300 %d %s", cnt, sum, maxTS, want, base.Add(299*time.Second))
			}
			// A larger streamed result through the compressed reader.
			rows, err := conn.QueryContext(ctx, "SELECT number, toString(number) FROM numbers(20000)")
			if err != nil {
				t.Fatalf("numbers: %v", err)
			}
			defer rows.Close()
			var i uint64
			for rows.Next() {
				var n uint64
				var s string
				if err := rows.Scan(&n, &s); err != nil {
					t.Fatal(err)
				}
				if n != i || s != fmt.Sprint(i) {
					t.Fatalf("row %d = (%d,%q)", i, n, s)
				}
				i++
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if i != 20000 {
				t.Errorf("numbers(20000) returned %d rows", i)
			}
		})
	}
}

func TestClientHandshakeNative(t *testing.T) {
	t.Parallel()
	for _, cv := range compressionVariants {
		cv := cv
		t.Run(cv.name, func(t *testing.T) {
			t.Parallel()
			conn := openNative(t, "default", func(o *clickhouse.Options) { o.Compression = cv.comp })
			ctx := ctxT(t)
			if err := conn.Ping(ctx); err != nil {
				t.Fatalf("ping: %v", err)
			}
			sv, err := conn.ServerVersion()
			if err != nil {
				t.Fatalf("ServerVersion: %v", err)
			}
			if sv.Version.Major != 25 || sv.Revision == 0 {
				t.Errorf("server version = %+v, want major 25 and non-zero revision", sv)
			}
			if sv.DisplayName == "" {
				t.Errorf("empty display name: %+v", sv)
			}
			var tz, name string
			var rev uint32
			if err := conn.QueryRow(ctx, "SELECT timezone(), displayName(), revision()").Scan(&tz, &name, &rev); err != nil {
				t.Fatalf("query: %v", err)
			}
			if tz == "" || name != sv.DisplayName || uint64(rev) != sv.Revision {
				t.Errorf("timezone=%q displayName=%q revision=%d vs handshake %+v", tz, name, rev, sv)
			}
			var one uint8
			if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
				t.Errorf("SELECT 1 = %d, %v", one, err)
			}
		})
	}
}

// TestClientSettings checks settings via Options.Settings, WithSettings and
// the query ID option.
func TestClientSettings(t *testing.T) {
	t.Parallel()
	conn := openNative(t, "default", func(o *clickhouse.Options) {
		o.Settings = clickhouse.Settings{"max_execution_time": 60}
	})
	ctx := clickhouse.Context(ctxT(t),
		clickhouse.WithSettings(clickhouse.Settings{"max_block_size": 1000, "some_unknown_setting_xyz": 1}),
		clickhouse.WithQueryID("e2e-qid-"+runID))
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM numbers(1234)").Scan(&n); err != nil {
		t.Fatalf("query with settings: %v", err)
	}
	if n != 1234 {
		t.Errorf("count = %d", n)
	}
	// SETTINGS clause inside the query.
	if err := conn.QueryRow(ctxT(t), "SELECT count() FROM numbers(10) SETTINGS max_threads = 1").Scan(&n); err != nil {
		t.Fatalf("SETTINGS clause: %v", err)
	}
	if n != 10 {
		t.Errorf("count = %d", n)
	}
	sqlc := openSQL(t, "default")
	ctx2 := clickhouse.Context(ctxT(t), clickhouse.WithSettings(clickhouse.Settings{"max_threads": 2}))
	if err := sqlc.QueryRowContext(ctx2, "SELECT count() FROM numbers(7)").Scan(&n); err != nil || n != 7 {
		t.Errorf("database/sql with settings: n=%d err=%v", n, err)
	}
}

// campaignRow is scanned / appended with clickhouse-go struct tags.
type campaignRow struct {
	CampaignID string    `ch:"campaign_id"`
	TenantID   string    `ch:"tenant_id"`
	Name       string    `ch:"name"`
	Status     string    `ch:"status"`
	Budget     uint64    `ch:"budget"`
	Tags       []string  `ch:"tags"`
	UpdatedAt  time.Time `ch:"updated_at"`
	Version    uint64    `ch:"version"`
}

// TestClientStructsAndIdioms covers common application idioms: AppendStruct,
// Select into a slice of structs, ScanStruct, sql.ErrNoRows, Exec with
// Named args, async insert settings and the native API's error codes.
func TestClientStructsAndIdioms(t *testing.T) {
	t.Parallel()
	db := newDB(t, "idioms")
	createSchema(t, db)
	conn := openNative(t, db)
	ctx := ctxT(t)
	upd := time.Date(2024, 7, 1, 8, 30, 0, 0, time.UTC)
	in := []campaignRow{
		{"cmp-1", "acme", "Summer", "active", 1000, []string{"summer", "search"}, upd, 1},
		{"cmp-2", "acme", "Winter", "paused", 250, []string{}, upd.Add(time.Hour), 3},
		{"cmp-3", "globex", "Ünïcode ✓", "active", 75, []string{"x"}, upd.Add(2 * time.Hour), 2},
	}
	b, err := conn.PrepareBatch(ctx, "INSERT INTO campaigns")
	if err != nil {
		t.Fatal(err)
	}
	for i := range in {
		if err := b.AppendStruct(&in[i]); err != nil {
			t.Fatalf("AppendStruct: %v", err)
		}
	}
	if err := b.Send(); err != nil {
		t.Fatalf("Send: %v", err)
	}
	var out []campaignRow
	if err := conn.Select(ctx, &out, "SELECT * FROM campaigns FINAL ORDER BY campaign_id"); err != nil {
		t.Fatalf("Select: %v", err)
	}
	if fmt.Sprintf("%+v", out) != fmt.Sprintf("%+v", in) {
		t.Errorf("Select =\n%+v\nwant\n%+v", out, in)
	}
	rows, err := conn.Query(ctx, "SELECT campaign_id, tenant_id, name, status, budget, tags, updated_at, version FROM campaigns WHERE campaign_id = ?", "cmp-3")
	if err != nil {
		t.Fatal(err)
	}
	var one campaignRow
	n := 0
	for rows.Next() {
		if err := rows.ScanStruct(&one); err != nil {
			t.Fatalf("ScanStruct: %v", err)
		}
		n++
	}
	rows.Close()
	if n != 1 || one.Name != in[2].Name || !one.UpdatedAt.Equal(in[2].UpdatedAt) {
		t.Errorf("ScanStruct = %+v (%d rows)", one, n)
	}
	// Column metadata.
	cols := rows.ColumnTypes()
	if len(cols) != 8 || cols[5].DatabaseTypeName() != "Array(String)" || cols[3].DatabaseTypeName() != "LowCardinality(String)" {
		var names []string
		for _, c := range cols {
			names = append(names, c.Name()+":"+c.DatabaseTypeName())
		}
		t.Errorf("column types = %v", names)
	}

	// database/sql idioms.
	sqlc := openSQL(t, db)
	var name string
	err = sqlc.QueryRowContext(ctx, "SELECT name FROM campaigns WHERE campaign_id = ?", "nope").Scan(&name)
	if err != sql.ErrNoRows {
		t.Errorf("no rows: err = %v, want sql.ErrNoRows", err)
	}
	if _, err := sqlc.ExecContext(ctx, "INSERT INTO campaigns (campaign_id, tenant_id, name, status, budget, version) VALUES (@id, @tenant, @name, 'draft', @budget, @ver)",
		clickhouse.Named("id", "cmp-4"), clickhouse.Named("tenant", "initech"), clickhouse.Named("name", "O'Brien \"Q3\""),
		clickhouse.Named("budget", uint64(9)), clickhouse.Named("ver", uint64(1))); err != nil {
		t.Fatalf("Exec named insert: %v", err)
	}
	if err := sqlc.QueryRowContext(ctx, "SELECT name FROM campaigns WHERE campaign_id = 'cmp-4'").Scan(&name); err != nil || name != "O'Brien \"Q3\"" {
		t.Errorf("named insert read back = %q, %v", name, err)
	}
	var m map[string]uint64
	if err := sqlc.QueryRowContext(ctx, "SELECT map('a', toUInt64(1), 'b', toUInt64(2))").Scan(&m); err != nil || m["a"] != 1 || m["b"] != 2 {
		t.Errorf("map scan = %v, %v", m, err)
	}
	var f64 float64
	var i64 int64
	var bl bool
	if err := sqlc.QueryRowContext(ctx, "SELECT toFloat64(1) / 4, toInt64(-42), 1 = 1").Scan(&f64, &i64, &bl); err != nil || f64 != 0.25 || i64 != -42 || !bl {
		t.Errorf("scalar scan = %v %v %v, %v", f64, i64, bl, err)
	}
	// Loader settings commonly sent by apps must be accepted.
	actx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"async_insert": 1, "wait_for_async_insert": 1, "insert_deduplicate": 0}))
	if err := conn.Exec(actx, "INSERT INTO tenants VALUES ('acme', 'pro', 'us')"); err != nil {
		t.Errorf("insert with async_insert settings: %v", err)
	}
	if err := conn.AsyncInsert(ctx, "INSERT INTO tenants VALUES ('globex', 'free', 'eu')", true); err != nil {
		t.Errorf("AsyncInsert(wait=true): %v", err)
	}
	var cnt uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM tenants").Scan(&cnt); err != nil || cnt != 2 {
		t.Errorf("tenants count = %d, %v", cnt, err)
	}
	// Exception details through the native API.
	err = conn.Exec(ctx, "SELECT sum(count()) FROM numbers(1)")
	wantCode(t, err, 184, "nested aggregate")
	var ex *clickhouse.Exception
	if errors.As(err, &ex) && ex.Name != "" && ex.Name != "DB::Exception" {
		t.Logf("exception name %q", ex.Name)
	}
}
