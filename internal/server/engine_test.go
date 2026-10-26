package server

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/exec"
	"github.com/gfronza/twinhouse/internal/storage/memstore"
)

// TestRealEngineIntegration wires the HTTP server to the real engine over an
// in-memory store and drives it with clickhouse-go and raw HTTP.
func TestRealEngineIntegration(t *testing.T) {
	cat := catalog.NewMemory()
	eng, err := exec.New(exec.Config{
		Store: memstore.New(), Catalog: cat, Region: "local", InlineViews: true,
		ServerVersion: "25.8.1.1", ServerRevision: ServerRevision, DisplayName: "it-node", Timezone: "UTC",
	})
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{Executor: eng, Catalog: cat, ServerVersion: "25.8.1.1", DisplayName: "it-node", Logger: quietLogger()})
	if err != nil {
		t.Fatal(err)
	}
	base := httpTestServer(t, s)
	ctx := context.Background()

	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{strings.TrimPrefix(base, "http://")}, Protocol: clickhouse.HTTP,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	sv, err := conn.ServerVersion()
	if err != nil || sv.DisplayName != "it-node" || sv.Revision != ServerRevision {
		t.Fatalf("server version %+v %v", sv, err)
	}
	ddl := []string{
		"CREATE TABLE events (tenant String, id UInt64, ts DateTime64(3), amount Float64) ENGINE = MergeTree ORDER BY (tenant, id)",
		"CREATE TABLE totals (tenant String, n AggregateFunction(count), s SimpleAggregateFunction(sum, Float64)) ENGINE = AggregatingMergeTree ORDER BY tenant",
		"CREATE MATERIALIZED VIEW mv TO totals AS SELECT tenant, countState() AS n, sum(amount) AS s FROM events GROUP BY tenant",
	}
	for _, q := range ddl {
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO events")
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	for i := 1; i <= 4; i++ {
		tenant := "a"
		if i%2 == 0 {
			tenant = "b"
		}
		if err := batch.Append(tenant, uint64(i), ts, float64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send: %v", err)
	}
	pctx := clickhouse.Context(ctx, clickhouse.WithParameters(clickhouse.Parameters{"t": "c", "id": "9"}))
	if err := conn.Exec(pctx, "INSERT INTO events (tenant, id, ts, amount) VALUES ({t:String}, {id:UInt64}, now64(3), 10)"); err != nil {
		t.Fatalf("param insert: %v", err)
	}
	// Python-style JSONEachRow insert.
	r := post(t, base+"/?query="+url.QueryEscape("INSERT INTO events FORMAT JSONEachRow"),
		`{"tenant":"a","id":20,"ts":"2026-03-04 05:06:07.000","amount":0.5}`+"\n")
	if r.Status != 200 {
		t.Fatalf("json insert: %d %s", r.Status, r.Body)
	}

	var n uint64
	var sum float64
	qctx := clickhouse.Context(ctx, clickhouse.WithParameters(clickhouse.Parameters{"t": "a"}))
	if err := conn.QueryRow(qctx, "SELECT countMerge(n), sum(s) FROM totals WHERE tenant = {t:String}").Scan(&n, &sum); err != nil {
		t.Fatalf("rollup: %v", err)
	}
	if n != 3 || sum != 4.5 {
		t.Errorf("rollup a: n=%d sum=%g", n, sum)
	}
	r = get(t, base+"/?query="+url.QueryEscape("SELECT tenant, count() FROM events GROUP BY tenant ORDER BY tenant FORMAT CSV"))
	if r.Status != 200 || r.Body != "\"a\",3\n\"b\",2\n\"c\",1\n" {
		t.Errorf("csv: %d %q", r.Status, r.Body)
	}
	// Errors from the engine reach clickhouse-go typed.
	var ex *clickhouse.Exception
	if _, err := conn.Query(ctx, "SELECT * FROM nope"); !errors.As(err, &ex) || ex.Code != 60 {
		t.Errorf("unknown table: %v", err)
	}
	// GET is read-only.
	r = get(t, base+"/?query="+url.QueryEscape("DROP TABLE events"))
	if r.Status != 500 || r.Header.Get("X-ClickHouse-Exception-Code") != "164" {
		t.Errorf("GET drop: %d %s", r.Status, r.Body)
	}
	// Sessions keep USE.
	post(t, base+"/", "CREATE DATABASE d1")
	post(t, base+"/?session_id=x", "USE d1")
	if r := post(t, base+"/?session_id=x", "SELECT currentDatabase()"); r.Body != "d1\n" {
		t.Errorf("session db %q", r.Body)
	}
	if r := post(t, base+"/?database=nodb", "SELECT 1"); r.Status != 404 || r.Header.Get("X-ClickHouse-Exception-Code") != "81" {
		t.Errorf("unknown db %d %s", r.Status, r.Body)
	}
}
