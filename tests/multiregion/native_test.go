//go:build multiregion

package multiregion

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func hostPort(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return u.Host
}

func chOptions(base string, comp clickhouse.CompressionMethod) *clickhouse.Options {
	o := &clickhouse.Options{
		Addr:        []string{hostPort(base)},
		Protocol:    clickhouse.HTTP,
		Auth:        clickhouse.Auth{Database: "default", Username: "default"},
		DialTimeout: 10 * time.Second,
		ReadTimeout: 5 * time.Minute, // DynamoDB Local is slow under parallel load
	}
	if comp != clickhouse.CompressionNone {
		o.Compression = &clickhouse.Compression{Method: comp}
	}
	return o
}

func openNative(t *testing.T, base string) driver.Conn {
	t.Helper()
	conn, err := clickhouse.Open(chOptions(base, clickhouse.CompressionLZ4))
	if err != nil {
		t.Fatalf("clickhouse.Open(%s): %v", base, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("ping %s: %v", base, err)
	}
	return conn
}

func openStd(t *testing.T, base string) *sql.DB {
	t.Helper()
	db := clickhouse.OpenDB(chOptions(base, clickhouse.CompressionGZIP))
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type rollupRec struct {
	Tenant   string    `ch:"tenant_id"`
	Campaign string    `ch:"campaign_id"`
	Hour     time.Time `ch:"hour"`
	Events   uint64    `ch:"events"`
	Clicks   uint64    `ch:"clicks"`
	Cost     uint64    `ch:"cost"`
	Last     time.Time `ch:"last_seen"`
	Users    uint64    `ch:"users"`
}

type replayRec struct {
	Audience string `ch:"audience_id"`
	State    string `ch:"state"`
	Adds     uint64 `ch:"adds"`
	Net      int64  `ch:"netcount"`
}

const ddlOps = `CREATE TABLE %s.audience_ops (
	tenant_id   String,
	audience_id String,
	user_id     String,
	op          LowCardinality(String),
	delta       Int64,
	version     UInt64,
	state       String
) ENGINE = MergeTree ORDER BY (tenant_id, audience_id, version, user_id)`

type audienceOp struct {
	audience, user, op string
	delta              int64
	version            uint64
	state              string
}

// Scenario 9: the same clickhouse-go queries (native API and database/sql,
// both over HTTP) return identical results in both regions after
// convergence, and equal the exact expected values.
func TestNativeClientParity(t *testing.T) {
	requireStack(t)
	parallel(t)
	rng := newRNG(t)
	db := setupSchema(t, "native")
	us().mustExec(t, fmt.Sprintf(ddlOps, db))
	waitResult(t, eu(), cfg.converge, "audience_ops usable", "SELECT count() FROM "+db+".audience_ops", nil, "0\n")
	cu, ce := openNative(t, cfg.usURL), openNative(t, cfg.euURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Native-format batches over HTTP in both regions.
	var all []event
	for _, w := range []struct {
		conn driver.Conn
		name string
	}{{cu, "us"}, {ce, "eu"}} {
		evs := genEvents(rng, genSpec{idPrefix: "n" + w.name, n: 250, tenants: 3, campaigns: 3, hours: 4, users: 40})
		b, err := w.conn.PrepareBatch(ctx, "INSERT INTO "+db+".events")
		if err != nil {
			t.Fatalf("%s PrepareBatch: %v", w.name, err)
		}
		for _, e := range evs {
			et, _ := time.ParseInLocation("2006-01-02 15:04:05", e.Time, time.UTC)
			rt, _ := time.ParseInLocation("2006-01-02 15:04:05.000", e.Received, time.UTC)
			if err := b.Append(e.Tenant, et, e.ID, e.Campaign, e.User, e.Type, e.Clicks, e.Cost, e.Referrer, rt); err != nil {
				t.Fatalf("%s Append: %v", w.name, err)
			}
		}
		sendStart := time.Now()
		if err := b.Send(); err != nil {
			t.Fatalf("%s Send after %s: %v", w.name, time.Since(sendStart).Round(time.Millisecond), err)
		}
		t.Logf("%s: Native batch of %d rows sent in %s", w.name, len(evs), time.Since(sendStart).Round(time.Millisecond))
		all = append(all, evs...)
	}
	var ops []audienceOp
	for i := 0; i < 60; i++ {
		op := audienceOp{audience: fmt.Sprintf("aud-%d", i%4), user: fmt.Sprintf("user-%02d", i%17),
			op: "add", delta: int64(1 + i%3), version: uint64(i + 1), state: fmt.Sprintf("s%d", i)}
		if i%5 == 0 {
			op.op = "remove"
		}
		ops = append(ops, op)
	}
	for i, op := range ops {
		conn := cu
		if i%2 == 1 {
			conn = ce
		}
		if err := conn.Exec(ctx, "INSERT INTO "+db+".audience_ops VALUES (?, ?, ?, ?, ?, ?, ?)",
			"tenant-00", op.audience, op.user, op.op, op.delta, op.version, op.state); err != nil {
			t.Fatalf("insert op %d: %v", i, err)
		}
	}
	checkConverged(t, db, all, cfg.converge)
	waitResult(t, us(), cfg.converge, "audience_ops rows", "SELECT count() FROM "+db+".audience_ops", nil, fmt.Sprintf("%d\n", len(ops)))
	waitResult(t, eu(), cfg.converge, "audience_ops rows", "SELECT count() FROM "+db+".audience_ops", nil, fmt.Sprintf("%d\n", len(ops)))
	if t.Failed() {
		t.FailNow()
	}

	both := func(name string, fn func(conn driver.Conn) (any, error)) {
		t.Helper()
		a, errA := fn(cu)
		b, errB := fn(ce)
		if errA != nil || errB != nil {
			t.Errorf("%s: us err=%v eu err=%v", name, errA, errB)
			return
		}
		if !reflect.DeepEqual(a, b) {
			t.Errorf("%s: regions differ:\nus: %+v\neu: %+v", name, a, b)
		}
	}

	// Rollup with a server-side {tenant:String} parameter and settings.
	expected := aggregate(all)
	for _, tenant := range []string{"tenant-00", "tenant-02"} {
		qctx := clickhouse.Context(ctx,
			clickhouse.WithParameters(clickhouse.Parameters{"tenant": tenant}),
			clickhouse.WithSettings(clickhouse.Settings{"max_execution_time": 60}))
		both("rollup "+tenant, func(conn driver.Conn) (any, error) {
			var out []rollupRec
			err := conn.Select(qctx, &out, `SELECT tenant_id, campaign_id, hour, sum(events) AS events, sum(clicks) AS clicks,
				sum(cost) AS cost, max(last_seen) AS last_seen, uniqExactMerge(users) AS users
				FROM `+db+`.rollup_hourly WHERE tenant_id = {tenant:String}
				GROUP BY tenant_id, campaign_id, hour ORDER BY campaign_id, hour`)
			if err == nil {
				for _, r := range out {
					k := aggKey{r.Tenant, r.Campaign, r.Hour.UTC().Format("2006-01-02 15:04:05")}
					e := expected[k]
					if e == nil || r.Events != e.events || r.Clicks != e.clicks || r.Cost != e.cost ||
						r.Users != uint64(len(e.users)) || r.Last.UTC().Format("2006-01-02 15:04:05") != e.last {
						t.Errorf("rollup row %+v does not match expected %+v", r, e)
					}
				}
			}
			return out, err
		})
	}

	// Positional ? and clickhouse.Named (client-side binding), QueryRow.
	both("count positional", func(conn driver.Conn) (any, error) {
		var n uint64
		err := conn.QueryRow(ctx, "SELECT count() FROM "+db+".events WHERE tenant_id = ? AND clicks = ?", "tenant-01", uint8(1)).Scan(&n)
		want := uint64(0)
		for _, e := range all {
			if e.Tenant == "tenant-01" && e.Clicks == 1 {
				want++
			}
		}
		if err == nil && n != want {
			t.Errorf("count = %d, want %d", n, want)
		}
		return n, err
	})
	both("sum named", func(conn driver.Conn) (any, error) {
		var cost uint64
		var users uint64
		err := conn.QueryRow(ctx, "SELECT sum(cost), uniqExact(user_id) FROM "+db+".events WHERE tenant_id = @t",
			clickhouse.Named("t", "tenant-02")).Scan(&cost, &users)
		return [2]uint64{cost, users}, err
	})

	// Dedup probe: tuple IN an Array(Tuple(String, String)) parameter plus a
	// DateTime range.
	var keys []string
	var wantIDs []string
	for i, e := range all {
		if i%37 == 0 {
			keys = append(keys, fmt.Sprintf("('%s','%s')", e.Tenant, e.ID))
			wantIDs = append(wantIDs, e.ID)
		}
	}
	keys = append(keys, "('tenant-00','missing-1')", "('nobody','"+all[0].ID+"')")
	slices.Sort(wantIDs)
	pctx := clickhouse.Context(ctx, clickhouse.WithParameters(clickhouse.Parameters{
		"keys": "[" + strings.Join(keys, ",") + "]",
		"from": "2026-03-01 00:00:00", "to": "2026-03-01 23:59:59",
	}))
	both("dedup probe", func(conn driver.Conn) (any, error) {
		var ids []string
		rows, err := conn.Query(pctx, "SELECT event_id FROM "+db+".events WHERE (tenant_id, event_id) IN {keys:Array(Tuple(String, String))} "+
			"AND event_time BETWEEN {from:DateTime} AND {to:DateTime} ORDER BY event_id")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			ids = append(ids, id)
		}
		if !slices.Equal(ids, wantIDs) {
			t.Errorf("dedup probe = %v, want %v", ids, wantIDs)
		}
		return ids, rows.Err()
	})

	// Latest-wins replay with argMax / countIf / sumIf.
	wantReplay := map[string]replayRec{}
	best := map[string]uint64{}
	for _, op := range ops {
		r := wantReplay[op.audience]
		r.Audience = op.audience
		if op.version >= best[op.audience] {
			best[op.audience], r.State = op.version, op.state
		}
		if op.op == "add" {
			r.Adds++
			r.Net += op.delta
		} else {
			r.Net -= op.delta
		}
		wantReplay[op.audience] = r
	}
	both("latest-wins replay", func(conn driver.Conn) (any, error) {
		var out []replayRec
		err := conn.Select(ctx, &out, "SELECT audience_id, argMax(state, version) AS state, countIf(op = 'add') AS adds, "+
			"sumIf(delta, op = 'add') - sumIf(delta, op = 'remove') AS netcount FROM "+db+".audience_ops "+
			"WHERE tenant_id = @t GROUP BY audience_id ORDER BY audience_id",
			clickhouse.Named("t", "tenant-00"))
		if err == nil {
			if len(out) != len(wantReplay) {
				t.Errorf("replay returned %d audiences, want %d", len(out), len(wantReplay))
			}
			for _, r := range out {
				if r != wantReplay[r.Audience] {
					t.Errorf("replay %s = %+v, want %+v", r.Audience, r, wantReplay[r.Audience])
				}
			}
		}
		return out, err
	})

	// Nullable -> *string, DateTime64 -> time.Time.
	probe := all[3]
	both("nullable scan", func(conn driver.Conn) (any, error) {
		var ref *string
		var recv time.Time
		err := conn.QueryRow(ctx, "SELECT referrer, received_at FROM "+db+".events WHERE tenant_id = ? AND event_id = ?",
			probe.Tenant, probe.ID).Scan(&ref, &recv)
		if err == nil {
			if (ref == nil) != (probe.Referrer == nil) || (ref != nil && *ref != *probe.Referrer) {
				t.Errorf("referrer = %v, want %v", ref, probe.Referrer)
			}
			if got := recv.UTC().Format("2006-01-02 15:04:05.000"); got != probe.Received {
				t.Errorf("received_at = %s, want %s", got, probe.Received)
			}
		}
		return fmt.Sprintf("%v|%s", deref(ref), recv.UTC()), err
	})

	// Errors surface as *clickhouse.Exception with the same code in both regions.
	both("exception", func(conn driver.Conn) (any, error) {
		err := conn.Exec(ctx, "SELECT * FROM "+db+".no_such_table")
		var ex *clickhouse.Exception
		if !errors.As(err, &ex) {
			return nil, fmt.Errorf("want *clickhouse.Exception, got %T %v", err, err)
		}
		return ex.Code, nil
	})

	// database/sql over HTTP (gzip).
	su, se := openStd(t, cfg.usURL), openStd(t, cfg.euURL)
	stdRows := func(db2 *sql.DB) ([]string, error) {
		rows, err := db2.QueryContext(ctx, "SELECT tenant_id, campaign_id, hour, sum(events), uniqExactMerge(users) FROM "+db+
			".rollup_hourly WHERE tenant_id = ? GROUP BY tenant_id, campaign_id, hour ORDER BY campaign_id, hour", "tenant-01")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var tn, cmp string
			var hour time.Time
			var ev, users uint64
			if err := rows.Scan(&tn, &cmp, &hour, &ev, &users); err != nil {
				return nil, err
			}
			out = append(out, fmt.Sprintf("%s\t%s\t%s\t%d\t%d", tn, cmp, hour.UTC().Format("2006-01-02 15:04:05"), ev, users))
		}
		return out, rows.Err()
	}
	a, errA := stdRows(su)
	b, errB := stdRows(se)
	if errA != nil || errB != nil {
		t.Fatalf("database/sql: us err=%v eu err=%v", errA, errB)
	}
	var wantStd []string
	for _, l := range splitLines(expectedRollup(all)) {
		f := strings.Split(l, "\t")
		if f[0] == "tenant-01" {
			wantStd = append(wantStd, strings.Join([]string{f[0], f[1], f[2], f[3], f[7]}, "\t"))
		}
	}
	if !slices.Equal(a, b) || !slices.Equal(a, wantStd) {
		t.Errorf("database/sql rollup:\nus:   %v\neu:   %v\nwant: %v", a, b, wantStd)
	}
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
