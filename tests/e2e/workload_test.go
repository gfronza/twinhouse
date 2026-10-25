//go:build e2e

package e2e

import (
	"bytes"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// insertEventsNative inserts events through the native API batch (Native over HTTP).
func insertEventsNative(t *testing.T, conn driver.Conn, evs []adEvent) {
	t.Helper()
	ctx := ctxT(t)
	b, err := conn.PrepareBatch(ctx, "INSERT INTO ad_events")
	if err != nil {
		t.Fatalf("PrepareBatch: %v", err)
	}
	for _, e := range evs {
		if err := b.Append(e.Tenant, e.Time, e.ID, e.Campaign, e.User, e.Type, e.Cost, e.Bot, e.Country, e.ClientTS); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := b.Send(); err != nil {
		t.Fatalf("Send: %v", err)
	}
}

// checkHourly compares campaign_hourly (read with -Merge/sum/max) with the expectation.
func checkHourly(t *testing.T, conn driver.Conn, evs []adEvent, label string) {
	t.Helper()
	want := expectedHourly(evs)
	rows, err := conn.Query(ctxT(t), `
		SELECT tenant_id, campaign_id, hour,
		       sum(events), sum(impressions), sum(clicks), sum(cost), max(last_seen), uniqExactMerge(users)
		FROM campaign_hourly
		GROUP BY tenant_id, campaign_id, hour
		ORDER BY tenant_id, campaign_id, hour`)
	if err != nil {
		t.Fatalf("%s: rollup query: %v", label, err)
	}
	defer rows.Close()
	keys := sortedHourKeys(want)
	i, bad := 0, 0
	for rows.Next() {
		var k hourKey
		var got hourAgg
		var users uint64
		if err := rows.Scan(&k.Tenant, &k.Campaign, &k.Hour, &got.Events, &got.Impressions, &got.Clicks, &got.Cost, &got.LastSeen, &users); err != nil {
			t.Fatalf("%s: scan: %v", label, err)
		}
		k.Hour = k.Hour.UTC()
		if i >= len(keys) || keys[i] != k {
			if bad++; bad < 5 {
				exp := "<end>"
				if i < len(keys) {
					exp = fmt.Sprint(keys[i])
				}
				t.Errorf("%s: rollup row %d key %v, want %s", label, i, k, exp)
			}
			i++
			continue
		}
		w := want[k]
		if got.Events != w.Events || got.Impressions != w.Impressions || got.Clicks != w.Clicks || got.Cost != w.Cost ||
			!got.LastSeen.Equal(w.LastSeen) || users != uint64(len(w.Users)) {
			if bad++; bad < 5 {
				got.Users = map[string]bool{}
				t.Errorf("%s: rollup %v:\n got  %s users=%d\n want %s", label, k, got, users, w)
			}
		}
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: rows: %v", label, err)
	}
	if i != len(keys) {
		t.Errorf("%s: rollup returned %d groups, want %d", label, i, len(keys))
	}
	if bad > 0 {
		t.Errorf("%s: %d mismatching rollup groups", label, bad)
	}
}

func checkDaily(t *testing.T, conn driver.Conn, evs []adEvent, label string) {
	t.Helper()
	want := expectedDaily(evs)
	rows, err := conn.Query(ctxT(t), `
		SELECT tenant_id, toString(day), campaign_id, sum(clicks), sum(cost)
		FROM campaign_daily GROUP BY tenant_id, day, campaign_id ORDER BY tenant_id, day, campaign_id`)
	if err != nil {
		t.Fatalf("%s: daily query: %v", label, err)
	}
	defer rows.Close()
	got := map[dayKey]dayAgg{}
	for rows.Next() {
		var k dayKey
		var a dayAgg
		if err := rows.Scan(&k.Tenant, &k.Day, &k.Campaign, &a.Clicks, &a.Cost); err != nil {
			t.Fatalf("%s: scan: %v", label, err)
		}
		got[k] = a
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Errorf("%s: daily rollup has %d groups, want %d", label, len(got), len(want))
	}
	bad := 0
	for k, w := range want {
		if g := got[k]; g != w {
			if bad++; bad < 5 {
				t.Errorf("%s: daily %v = %+v, want %+v", label, k, g, w)
			}
		}
	}
	// Reading the SummingMergeTree without GROUP BY must also be merged
	// (one row per key; documented "reads behave as FINAL").
	var n uint64
	if err := conn.QueryRow(ctxT(t), "SELECT count() FROM campaign_daily").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != uint64(len(want)) {
		t.Errorf("%s: SELECT count() FROM campaign_daily = %d, want %d merged rows", label, n, len(want))
	}
}

// TestWorkloadEventsAndRollups covers shapes 1, 2, 6 and 7: an append-only
// event table written by clickhouse-go batches, a Python-style JSONEachRow
// loader (plain and gzip) and database/sql inserts, with MV rollups that must
// be correct immediately (inline views) and not double count after the stream
// worker processes the same records.
func TestWorkloadEventsAndRollups(t *testing.T) {
	t.Parallel()
	heavy(t)
	db := newDB(t, "wl_events")
	createSchema(t, db)
	conn := openNative(t, db)
	ctx := ctxT(t)

	var all []adEvent

	// Batch 1: native batch insert.
	b1 := genEvents(1, 300, "b1")
	start := time.Now()
	insertEventsNative(t, conn, b1)
	t.Logf("native batch of %d rows: %s", len(b1), time.Since(start))
	all = append(all, b1...)
	checkHourly(t, conn, all, "after native batch")
	checkDaily(t, conn, all, "after native batch")

	// Batch 2: JSONEachRow loader over plain HTTP, half plain, half gzip.
	b2 := genEvents(2, 200, "b2")
	r := doHTTP(t, req{
		Params: url.Values{"query": {"INSERT INTO " + db + ".ad_events FORMAT JSONEachRow"}},
		Body:   bytes.NewReader(jsonEachRow(b2[:100])),
	})
	if r.Status != 200 {
		t.Fatalf("JSONEachRow insert: %d %s", r.Status, r.Body)
	}
	if !strings.Contains(r.Header.Get("X-ClickHouse-Summary"), `"written_rows":"100"`) {
		t.Errorf("X-ClickHouse-Summary after insert = %s, want written_rows 100", r.Header.Get("X-ClickHouse-Summary"))
	}
	r = doHTTP(t, req{
		Params:  url.Values{"query": {"INSERT INTO " + db + ".ad_events FORMAT JSONEachRow"}},
		Body:    bytes.NewReader(gzipBytes(t, jsonEachRow(b2[100:]))),
		Headers: map[string]string{"Content-Encoding": "gzip"},
	})
	if r.Status != 200 {
		t.Fatalf("gzip JSONEachRow insert: %d %s", r.Status, r.Body)
	}
	all = append(all, b2...)
	checkHourly(t, conn, all, "after JSONEachRow")
	checkDaily(t, conn, all, "after JSONEachRow")

	// Batch 3: database/sql with positional parameters (INSERT ... VALUES).
	sqlc := openSQL(t, db)
	b3 := genEvents(3, 20, "b3")
	for i := range b3 {
		// Positional time.Time binds with second precision (client-side
		// toDateTime('...')), exactly as against ClickHouse.
		b3[i].ClientTS = b3[i].ClientTS.Truncate(time.Second)
	}
	for _, e := range b3 {
		_, err := sqlc.ExecContext(ctx, "INSERT INTO ad_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			e.Tenant, e.Time, e.ID, e.Campaign, e.User, e.Type, e.Cost, e.Bot, e.Country, e.ClientTS)
		if err != nil {
			t.Fatalf("sql insert: %v", err)
		}
	}
	all = append(all, b3...)
	checkHourly(t, conn, all, "after database/sql inserts")
	checkDaily(t, conn, all, "after database/sql inserts")

	// Raw table (shape 1): exact counts and Nullable / DateTime64 round trip.
	t.Run("raw", func(t *testing.T) {
		checkRawEvents(t, conn, all)
	})

	// The rollup must equal the same aggregation over the raw table.
	t.Run("rollup_equals_raw", func(t *testing.T) {
		raw := tsv(t, db, `SELECT tenant_id, campaign_id, toStartOfHour(event_time) h, count(), countIf(event_type='impression'),
			countIf(event_type='click'), sum(cost_micros), max(event_time), uniqExact(user_id)
			FROM ad_events GROUP BY tenant_id, campaign_id, h ORDER BY tenant_id, campaign_id, h`)
		roll := tsv(t, db, `SELECT tenant_id, campaign_id, hour, sum(events), sum(impressions), sum(clicks), sum(cost), max(last_seen), uniqExactMerge(users)
			FROM campaign_hourly GROUP BY tenant_id, campaign_id, hour ORDER BY tenant_id, campaign_id, hour`)
		if raw != roll {
			t.Errorf("rollup differs from raw aggregation:\nraw:\n%s\nrollup:\n%s", abbrev(raw), abbrev(roll))
		}
	})

	// After the stream worker has seen the records too: no double counting.
	time.Sleep(workerSettle)
	checkHourly(t, conn, all, "after stream worker")
	checkDaily(t, conn, all, "after stream worker")

	// Per-tenant rollup read with server-side parameters (dashboard query).
	t.Run("tenant_dashboard", func(t *testing.T) {
		want := map[string][3]uint64{}
		users := map[string]map[string]bool{}
		for _, e := range all {
			if e.Tenant != "acme" {
				continue
			}
			w := want[e.Campaign]
			w[0]++
			if e.Type == "click" {
				w[1]++
			}
			w[2] += e.Cost
			want[e.Campaign] = w
			if users[e.Campaign] == nil {
				users[e.Campaign] = map[string]bool{}
			}
			users[e.Campaign][e.User] = true
		}
		pctx := clickhouse.Context(ctxT(t), clickhouse.WithParameters(clickhouse.Parameters{
			"tenant": "acme", "from": eventBase.Format(time.DateTime), "to": eventBase.Add(24 * time.Hour).Format(time.DateTime)}))
		rows, err := conn.Query(pctx, `
			SELECT campaign_id, sum(events) AS ev, sum(clicks) AS cl, sum(cost) AS cost, uniqExactMerge(users) AS u
			FROM campaign_hourly
			WHERE tenant_id = {tenant:String} AND hour >= {from:DateTime} AND hour < {to:DateTime}
			GROUP BY campaign_id
			ORDER BY campaign_id`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var seen []string
		for rows.Next() {
			var c string
			var ev, cl, cost, u uint64
			if err := rows.Scan(&c, &ev, &cl, &cost, &u); err != nil {
				t.Fatal(err)
			}
			seen = append(seen, c)
			if w := want[c]; w != [3]uint64{ev, cl, cost} || u != uint64(len(users[c])) {
				t.Errorf("campaign %s: got ev=%d cl=%d cost=%d users=%d, want %v users=%d", c, ev, cl, cost, u, w, len(users[c]))
			}
		}
		if len(seen) != len(want) || !sort.StringsAreSorted(seen) {
			t.Errorf("campaigns = %v, want %d sorted", seen, len(want))
		}
	})
}

func checkRawEvents(t *testing.T, conn driver.Conn, all []adEvent) {
	ctx := ctxT(t)
	perTenant := map[string]uint64{}
	nullCountry := uint64(0)
	for _, e := range all {
		perTenant[e.Tenant]++
		if e.Country == nil {
			nullCountry++
		}
	}
	rows, err := conn.Query(ctx, "SELECT tenant_id, count() FROM ad_events GROUP BY tenant_id ORDER BY tenant_id")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]uint64{}
	for rows.Next() {
		var tn string
		var n uint64
		if err := rows.Scan(&tn, &n); err != nil {
			t.Fatal(err)
		}
		got[tn] = n
	}
	rows.Close()
	if fmt.Sprint(got) != fmt.Sprint(perTenant) {
		t.Errorf("per-tenant counts = %v, want %v", got, perTenant)
	}
	var nn uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM ad_events WHERE country IS NULL").Scan(&nn); err != nil {
		t.Fatal(err)
	}
	if nn != nullCountry {
		t.Errorf("country IS NULL count = %d, want %d", nn, nullCountry)
	}
	// Point read of a few events by full sorting key, scanning every column.
	for _, e := range []adEvent{all[0], all[len(all)/2], all[len(all)-1]} {
		var g adEvent
		err := conn.QueryRow(ctx, `SELECT tenant_id, event_time, event_id, campaign_id, user_id, event_type, cost_micros, is_bot, country, client_ts
			FROM ad_events WHERE tenant_id = ? AND event_time = ? AND event_id = ?`, e.Tenant, e.Time, e.ID).
			Scan(&g.Tenant, &g.Time, &g.ID, &g.Campaign, &g.User, &g.Type, &g.Cost, &g.Bot, &g.Country, &g.ClientTS)
		if err != nil {
			t.Errorf("point read %s: %v", e.ID, err)
			continue
		}
		if g.Tenant != e.Tenant || !g.Time.Equal(e.Time) || g.ID != e.ID || g.Campaign != e.Campaign || g.User != e.User ||
			g.Type != e.Type || g.Cost != e.Cost || g.Bot != e.Bot || !g.ClientTS.Equal(e.ClientTS) ||
			(g.Country == nil) != (e.Country == nil) || (g.Country != nil && *g.Country != *e.Country) {
			t.Errorf("point read %s:\n got  %+v\n want %+v", e.ID, g, e)
		}
	}
	// Time-range read on the sorting key (event_time is the 2nd key column).
	from, to := eventBase.Add(time.Hour), eventBase.Add(2*time.Hour)
	var want uint64
	for _, e := range all {
		if e.Tenant == "globex" && !e.Time.Before(from) && e.Time.Before(to) {
			want++
		}
	}
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM ad_events WHERE tenant_id = 'globex' AND event_time >= ? AND event_time < ?", from, to).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != want {
		t.Errorf("range count = %d, want %d", n, want)
	}
}

// TestWorkloadReplacingFinal covers shape 3: a ReplacingMergeTree(version)
// "current state" table read with FINAL and WHERE key = {p:String}.
func TestWorkloadReplacingFinal(t *testing.T) {
	t.Parallel()
	db := newDB(t, "wl_replacing")
	createSchema(t, db)
	conn := openNative(t, db)
	ctx := ctxT(t)

	type st struct {
		id, tenant, name, status string
		budget                   uint64
		tags                     []string
		ver                      uint64
	}
	// Versions arrive out of order; the highest version must win; for equal
	// versions the last inserted wins.
	writes := []st{
		{"c-1", "acme", "Spring", "draft", 100, []string{"a"}, 1},
		{"c-1", "acme", "Spring sale", "active", 500, []string{"a", "b"}, 3},
		{"c-1", "acme", "Spring (old)", "paused", 200, nil, 2},
		{"c-2", "acme", "Brand", "active", 50, []string{"x"}, 7},
		{"c-2", "acme", "Brand v7b", "active", 70, []string{"y"}, 7},
		{"c-3", "globex", "It's \"quoted\" ünïcode", "active", 1, []string{"q'uote"}, 1},
	}
	upd := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)
	for i, w := range writes {
		// Separate INSERTs (versions in different parts in ClickHouse terms).
		b, err := conn.PrepareBatch(ctx, "INSERT INTO campaigns")
		if err != nil {
			t.Fatal(err)
		}
		tags := w.tags
		if tags == nil {
			tags = []string{}
		}
		if err := b.Append(w.id, w.tenant, w.name, w.status, w.budget, tags, upd.Add(time.Duration(i)*time.Minute), w.ver); err != nil {
			t.Fatal(err)
		}
		if err := b.Send(); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	want := map[string]st{
		"c-1": writes[1],
		"c-2": writes[4],
		"c-3": writes[5],
	}
	for id, w := range want {
		pctx := clickhouse.Context(ctx, clickhouse.WithParameters(clickhouse.Parameters{"p": id}))
		var g st
		var updated time.Time
		err := conn.QueryRow(pctx, `SELECT campaign_id, tenant_id, name, status, budget, tags, updated_at, version
			FROM campaigns FINAL WHERE campaign_id = {p:String}`).
			Scan(&g.id, &g.tenant, &g.name, &g.status, &g.budget, &g.tags, &updated, &g.ver)
		if err != nil {
			t.Errorf("FINAL read %s: %v", id, err)
			continue
		}
		if g.id != w.id || g.name != w.name || g.status != w.status || g.budget != w.budget || g.ver != w.ver ||
			strings.Join(g.tags, ",") != strings.Join(w.tags, ",") {
			t.Errorf("FINAL %s = %+v, want %+v", id, g, w)
		}
	}
	// FINAL scan of the whole table: one row per key.
	out := tsv(t, db, "SELECT campaign_id, version, budget FROM campaigns FINAL ORDER BY campaign_id")
	if out != "c-1\t3\t500\nc-2\t7\t70\nc-3\t1\t1\n" {
		t.Errorf("FINAL scan = %q", out)
	}
	// Aggregate over FINAL.
	out = tsv(t, db, "SELECT tenant_id, sum(budget), count() FROM campaigns FINAL GROUP BY tenant_id ORDER BY tenant_id")
	if out != "acme\t570\t2\nglobex\t1\t1\n" {
		t.Errorf("FINAL aggregate = %q", out)
	}
	// database/sql flavour with a server-side parameter through clickhouse.Named is
	// not possible ({p:String} needs WithParameters); use positional binding.
	sqlc := openSQL(t, db)
	var name string
	if err := sqlc.QueryRowContext(ctx, "SELECT name FROM campaigns FINAL WHERE campaign_id = ?", "c-3").Scan(&name); err != nil || name != writes[5].name {
		t.Errorf("database/sql FINAL read = %q, %v", name, err)
	}
	// A stale version written later must not replace the current state.
	mustExec(t, db, "INSERT INTO campaigns (campaign_id, tenant_id, name, status, budget, version) VALUES ('c-1', 'acme', 'stale', 'deleted', 0, 2)")
	if out := tsv(t, db, "SELECT name, version FROM campaigns FINAL WHERE campaign_id = 'c-1'"); out != "Spring sale\t3\n" {
		t.Errorf("after stale write: %q", out)
	}
	mustExec(t, db, "INSERT INTO campaigns (campaign_id, tenant_id, name, status, budget, version) VALUES ('c-1', 'acme', 'Summer', 'active', 900, 4)")
	if out := tsv(t, db, "SELECT name, version, budget FROM campaigns FINAL WHERE campaign_id = 'c-1'"); out != "Summer\t4\t900\n" {
		t.Errorf("after newer write: %q", out)
	}
}

// TestWorkloadDedupProbe covers shape 4: SELECT ... WHERE (tenant_id,
// event_id) IN {keys:Array(Tuple(String, String))} AND event_time BETWEEN.
func TestWorkloadDedupProbe(t *testing.T) {
	t.Parallel()
	db := newDB(t, "wl_dedup")
	mustExec(t, "", strings.ReplaceAll(schemaDDL[0], "{db}", db))
	conn := openNative(t, db)
	evs := genEvents(4, 400, "dd")
	insertEventsNative(t, conn, evs)

	from, to := eventBase.Add(30*time.Minute), eventBase.Add(4*time.Hour)
	// Probe keys: 40 existing events, some outside the window, plus unknown ones.
	type pk struct{ tenant, id string }
	var probe []pk
	wantIDs := map[string]bool{}
	for i := 0; i < 400; i += 10 {
		e := evs[i]
		probe = append(probe, pk{e.Tenant, e.ID})
		if !e.Time.Before(from) && !e.Time.After(to) {
			wantIDs[e.ID] = true
		}
	}
	probe = append(probe, pk{"acme", "missing-1"}, pk{"nobody", evs[5].ID}, pk{evs[20].Tenant + "x", evs[20].ID})
	// Wrong tenant for an existing id must not match.
	if evs[30].Tenant != "acme" {
		probe = append(probe, pk{"acme", evs[30].ID})
	} else {
		probe = append(probe, pk{"globex", evs[30].ID})
	}
	var lit []string
	for _, p := range probe {
		lit = append(lit, fmt.Sprintf("('%s','%s')", p.tenant, p.id))
	}
	keysParam := "[" + strings.Join(lit, ",") + "]"
	const q = `SELECT tenant_id, event_id FROM ad_events
		WHERE (tenant_id, event_id) IN {keys:Array(Tuple(String, String))}
		  AND event_time BETWEEN {from:DateTime} AND {to:DateTime}
		ORDER BY tenant_id, event_id`
	wantList := func() string {
		var ls []string
		for _, e := range evs {
			if wantIDs[e.ID] {
				ls = append(ls, e.Tenant+"\t"+e.ID)
			}
		}
		sort.Strings(ls)
		return strings.Join(ls, "\n") + "\n"
	}()

	t.Run("http_params", func(t *testing.T) {
		r := doHTTP(t, req{
			Params: url.Values{"database": {db}, "param_keys": {keysParam},
				"param_from": {from.Format(time.DateTime)}, "param_to": {to.Format(time.DateTime)}},
			Body: strings.NewReader(q),
		})
		if r.Status != 200 {
			t.Fatalf("%d %s", r.Status, r.Body)
		}
		if r.Body != wantList {
			t.Errorf("dedup probe =\n%s\nwant\n%s", r.Body, wantList)
		}
	})
	t.Run("clickhouse_go_params", func(t *testing.T) {
		ctx := clickhouse.Context(ctxT(t), clickhouse.WithParameters(clickhouse.Parameters{
			"keys": keysParam, "from": from.Format(time.DateTime), "to": to.Format(time.DateTime)}))
		rows, err := conn.Query(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var sb strings.Builder
		for rows.Next() {
			var a, b string
			if err := rows.Scan(&a, &b); err != nil {
				t.Fatal(err)
			}
			sb.WriteString(a + "\t" + b + "\n")
		}
		if sb.String() != wantList {
			t.Errorf("dedup probe (clickhouse-go) =\n%s\nwant\n%s", sb.String(), wantList)
		}
	})
	t.Run("count_existing", func(t *testing.T) {
		// The typical loader dedup: count how many of the batch already exist.
		ctx := clickhouse.Context(ctxT(t), clickhouse.WithParameters(clickhouse.Parameters{
			"keys": keysParam, "from": eventBase.Add(-time.Hour).Format(time.DateTime), "to": eventBase.Add(24 * time.Hour).Format(time.DateTime)}))
		var n uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM ad_events WHERE (tenant_id, event_id) IN {keys:Array(Tuple(String, String))}
			AND event_time BETWEEN {from:DateTime} AND {to:DateTime}`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 40 {
			t.Errorf("existing = %d, want 40", n)
		}
	})
	t.Run("literal_tuple_in", func(t *testing.T) {
		out := tsv(t, db, fmt.Sprintf(`SELECT count() FROM ad_events WHERE (tenant_id, event_id) IN (%s)`, strings.Join(lit, ",")))
		if out != "40\n" {
			t.Errorf("literal tuple IN count = %q, want 40", out)
		}
	})
}

// TestWorkloadLatestWinsReplay covers shape 5: argMax/countIf/sumIf replay
// of membership operations grouped by key.
func TestWorkloadLatestWinsReplay(t *testing.T) {
	t.Parallel()
	db := newDB(t, "wl_replay")
	createSchema(t, db)
	conn := openNative(t, db)
	type op struct {
		seg, user, op string
		delta         int64
		state         string
		ver           uint64
	}
	var ops []op
	segs := []string{"seg-high-value", "seg-churn-risk", "seg-new"}
	states := []string{"pending", "active", "frozen", "archived"}
	for i := 0; i < 300; i++ {
		s := segs[i%3]
		o := "add"
		if i%4 == 3 {
			o = "remove"
		}
		ops = append(ops, op{s, fmt.Sprintf("u%02d", i%25), o, int64(i%5 + 1), states[(i*7)%4], uint64(1000 + i*3)})
	}
	b, err := conn.PrepareBatch(ctxT(t), "INSERT INTO audience_ops")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	for i, o := range ops {
		if err := b.Append(o.seg, o.user, o.op, o.delta, o.state, o.ver, base.Add(time.Duration(i)*time.Millisecond*1500)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Send(); err != nil {
		t.Fatal(err)
	}
	type res struct {
		state       string
		adds        uint64
		net         int64
		maxVer      uint64
		removeUsers map[string]bool
	}
	want := map[string]*res{}
	for _, o := range ops {
		r := want[o.seg]
		if r == nil {
			r = &res{removeUsers: map[string]bool{}}
			want[o.seg] = r
		}
		if o.ver > r.maxVer {
			r.maxVer, r.state = o.ver, o.state
		}
		if o.op == "add" {
			r.adds++
			r.net += o.delta
		} else {
			r.net -= o.delta
			r.removeUsers[o.user] = true
		}
	}
	rows, err := conn.Query(ctxT(t), `
		SELECT segment_id,
		       argMax(state, version)                                    AS latest_state,
		       max(version)                                              AS v,
		       countIf(op = 'add')                                       AS adds,
		       sumIf(delta, op = 'add') - sumIf(delta, op = 'remove')    AS netcount,
		       uniqExactIf(user_id, op = 'remove')                       AS removers
		FROM audience_ops
		GROUP BY segment_id
		ORDER BY segment_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var seen []string
	for rows.Next() {
		var seg, state string
		var v, adds, removers uint64
		var net int64
		if err := rows.Scan(&seg, &state, &v, &adds, &net, &removers); err != nil {
			t.Fatal(err)
		}
		seen = append(seen, seg)
		w := want[seg]
		if w == nil || state != w.state || v != w.maxVer || adds != w.adds || net != w.net || removers != uint64(len(w.removeUsers)) {
			t.Errorf("%s: got state=%s v=%d adds=%d net=%d removers=%d, want %+v", seg, state, v, adds, net, removers, w)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(seen, ",") != "seg-churn-risk,seg-high-value,seg-new" {
		t.Errorf("segments = %v", seen)
	}
	// Per (segment,user) latest op, with a HAVING on the replayed state.
	type su struct{ seg, user string }
	latest := map[su]op{}
	for _, o := range ops {
		k := su{o.seg, o.user}
		if cur, ok := latest[k]; !ok || o.ver > cur.ver {
			latest[k] = o
		}
	}
	var members []string
	for k, o := range latest {
		if o.op == "add" {
			members = append(members, k.seg+"\t"+k.user)
		}
	}
	sort.Strings(members)
	out := tsv(t, db, `SELECT segment_id, user_id FROM audience_ops GROUP BY segment_id, user_id
		HAVING argMax(op, version) = 'add' ORDER BY segment_id, user_id`)
	if out != strings.Join(members, "\n")+"\n" {
		t.Errorf("latest-wins members =\n%s\nwant\n%s", abbrev(out), abbrev(strings.Join(members, "\n")))
	}
}
