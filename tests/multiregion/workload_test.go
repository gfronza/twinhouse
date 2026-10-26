//go:build multiregion

package multiregion

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Neutral "campaign analytics" schema:
//
//	events          MergeTree source: ad impressions / clicks per tenant
//	rollup_hourly   AggregatingMergeTree, fed by mv_hourly (TO)
//	campaign_state  ReplacingMergeTree(version) current state of campaigns

const ddlEvents = `CREATE TABLE %s.events (
	tenant_id   String,
	event_time  DateTime,
	event_id    String,
	campaign_id LowCardinality(String),
	user_id     String,
	event_type  LowCardinality(String),
	clicks      UInt8,
	cost        UInt64,
	referrer    Nullable(String),
	received_at DateTime64(3)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (tenant_id, event_time, event_id)`

const ddlRollup = `CREATE TABLE %s.rollup_hourly (
	tenant_id   String,
	campaign_id String,
	hour        DateTime,
	events      SimpleAggregateFunction(sum, UInt64),
	clicks      SimpleAggregateFunction(sum, UInt64),
	cost        SimpleAggregateFunction(sum, UInt64),
	last_seen   SimpleAggregateFunction(max, DateTime),
	users       AggregateFunction(uniqExact, String)
) ENGINE = AggregatingMergeTree
ORDER BY (tenant_id, campaign_id, hour)`

const ddlMV = `CREATE MATERIALIZED VIEW %s.mv_hourly TO %s.rollup_hourly AS
SELECT tenant_id, campaign_id, toStartOfHour(event_time) AS hour,
       count() AS events, sum(clicks) AS clicks, sum(cost) AS cost,
       max(event_time) AS last_seen, uniqExactState(user_id) AS users
FROM %s.events
GROUP BY tenant_id, campaign_id, hour`

const ddlState = `CREATE TABLE %s.campaign_state (
	tenant_id   String,
	campaign_id String,
	status      LowCardinality(String),
	budget      UInt64,
	updated_by  String,
	version     UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (tenant_id, campaign_id)`

// createSchema creates events, rollup_hourly, mv_hourly and campaign_state in
// db through r.
func createSchema(t testing.TB, r *region, db string) {
	t.Helper()
	r.mustExec(t, fmt.Sprintf(ddlEvents, db))
	r.mustExec(t, fmt.Sprintf(ddlRollup, db))
	r.mustExec(t, fmt.Sprintf(ddlMV, db, db, db))
	r.mustExec(t, fmt.Sprintf(ddlState, db))
}

// waitSchema waits until region r can query every object of the schema
// (catalog replicated and physical tables reconciled).
func waitSchema(t testing.TB, r *region, db string) {
	t.Helper()
	ok := eventually(t, cfg.converge, r.name+": schema of "+db+" usable", func() (bool, string) {
		for _, q := range []string{
			"SELECT count() FROM " + db + ".events",
			"SELECT count() FROM " + db + ".rollup_hourly",
			"SELECT count() FROM " + db + ".campaign_state",
		} {
			// Only usability matters: rows may already be replicated.
			if out, err := r.query(q, nil); err != nil {
				return false, fmt.Sprintf("%s -> %q %v", q, out, err)
			}
		}
		out, err := r.query("SELECT name FROM system.tables WHERE database = {db:String} ORDER BY name", map[string]string{"db": db})
		if err != nil {
			return false, err.Error()
		}
		if out != "campaign_state\nevents\nmv_hourly\nrollup_hourly\n" {
			return false, fmt.Sprintf("system.tables: %q", out)
		}
		return true, ""
	})
	if !ok {
		t.FailNow()
	}
}

// setupSchema creates a fresh database with the schema in us and waits until
// both regions can use it.
func setupSchema(t testing.TB, scenario string) string {
	t.Helper()
	db := newDB(t, scenario)
	createSchema(t, us(), db)
	waitSchema(t, us(), db)
	waitSchema(t, eu(), db)
	return db
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

type event struct {
	Tenant, Time, ID, Campaign, User, Type string
	Clicks                                 uint8
	Cost                                   uint64
	Referrer                               *string
	Received                               string // DateTime64(3) text
}

// baseDay is the day all generated events fall on.
var baseDay = time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

// genSpec describes a generated batch.
type genSpec struct {
	idPrefix  string // makes event ids unique per writer
	n         int
	tenants   int
	campaigns int
	hours     int
	users     int
}

func genEvents(rng *rand.Rand, s genSpec) []event {
	out := make([]event, s.n)
	for i := range out {
		ts := baseDay.Add(time.Duration(rng.IntN(s.hours))*time.Hour +
			time.Duration(rng.IntN(3600))*time.Second)
		e := event{
			Tenant:   fmt.Sprintf("tenant-%02d", rng.IntN(s.tenants)),
			Time:     ts.Format("2006-01-02 15:04:05"),
			ID:       fmt.Sprintf("%s-%06d", s.idPrefix, i),
			Campaign: fmt.Sprintf("cmp-%02d", rng.IntN(s.campaigns)),
			User:     fmt.Sprintf("user-%04d", rng.IntN(s.users)),
			Type:     "impression",
			Cost:     uint64(rng.IntN(1000)),
			Received: ts.Add(time.Duration(rng.IntN(5000)) * time.Millisecond).Format("2006-01-02 15:04:05.000"),
		}
		if rng.IntN(4) == 0 {
			e.Type, e.Clicks = "click", 1
		}
		if rng.IntN(3) != 0 {
			ref := fmt.Sprintf("site-%d.example", rng.IntN(7))
			e.Referrer = &ref
		}
		out[i] = e
	}
	return out
}

// jsonRow is the JSONEachRow object of an event.
func (e event) jsonRow() map[string]any {
	var ref any
	if e.Referrer != nil {
		ref = *e.Referrer
	}
	return map[string]any{
		"tenant_id": e.Tenant, "event_time": e.Time, "event_id": e.ID, "campaign_id": e.Campaign,
		"user_id": e.User, "event_type": e.Type, "clicks": e.Clicks, "cost": e.Cost,
		"referrer": ref, "received_at": e.Received,
	}
}

func jsonRows(evs []event) []map[string]any {
	out := make([]map[string]any, len(evs))
	for i, e := range evs {
		out[i] = e.jsonRow()
	}
	return out
}

// tsv is the TabSeparated line of an event in sourceQuery's column order.
func (e event) tsv() string {
	ref := `\N`
	if e.Referrer != nil {
		ref = *e.Referrer
	}
	return strings.Join([]string{e.Tenant, e.Time, e.ID, e.Campaign, e.User, e.Type,
		strconv.Itoa(int(e.Clicks)), strconv.FormatUint(e.Cost, 10), ref, e.Received}, "\t")
}

func sourceQuery(db string) string {
	return "SELECT tenant_id, event_time, event_id, campaign_id, user_id, event_type, clicks, cost, referrer, received_at FROM " +
		db + ".events ORDER BY tenant_id, event_time, event_id FORMAT TabSeparated"
}

// expectedSource is the exact TabSeparated output of sourceQuery.
func expectedSource(evs []event) string {
	s := slices.Clone(evs)
	slices.SortFunc(s, func(a, b event) int {
		return strings.Compare(a.Tenant+"\x00"+a.Time+"\x00"+a.ID, b.Tenant+"\x00"+b.Time+"\x00"+b.ID)
	})
	var b strings.Builder
	for _, e := range s {
		b.WriteString(e.tsv())
		b.WriteByte('\n')
	}
	return b.String()
}

// rollupQuery reads the AggregatingMergeTree rollup the ClickHouse way.
func rollupQuery(db string) string {
	return "SELECT tenant_id, campaign_id, hour, sum(events), sum(clicks), sum(cost), max(last_seen), uniqExactMerge(users) FROM " +
		db + ".rollup_hourly GROUP BY tenant_id, campaign_id, hour ORDER BY tenant_id, campaign_id, hour FORMAT TabSeparated"
}

// sourceAggQuery computes the same aggregation directly over the source.
func sourceAggQuery(db string) string {
	return "SELECT tenant_id, campaign_id, toStartOfHour(event_time) AS hour, count(), sum(clicks), sum(cost), max(event_time), uniqExact(user_id) FROM " +
		db + ".events GROUP BY tenant_id, campaign_id, hour ORDER BY tenant_id, campaign_id, hour FORMAT TabSeparated"
}

type aggKey struct{ tenant, campaign, hour string }

type agg struct {
	events, clicks, cost uint64
	last                 string
	users                map[string]bool
}

// aggregate computes the exact rollup of evs.
func aggregate(evs []event) map[aggKey]*agg {
	m := map[aggKey]*agg{}
	for _, e := range evs {
		k := aggKey{e.Tenant, e.Campaign, e.Time[:13] + ":00:00"}
		a := m[k]
		if a == nil {
			a = &agg{users: map[string]bool{}}
			m[k] = a
		}
		a.events++
		a.clicks += uint64(e.Clicks)
		a.cost += e.Cost
		if e.Time > a.last {
			a.last = e.Time
		}
		a.users[e.User] = true
	}
	return m
}

// expectedRollup is the exact TabSeparated output of rollupQuery (and of
// sourceAggQuery) for evs.
func expectedRollup(evs []event) string {
	m := aggregate(evs)
	keys := make([]aggKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b aggKey) int {
		return strings.Compare(a.tenant+"\x00"+a.campaign+"\x00"+a.hour, b.tenant+"\x00"+b.campaign+"\x00"+b.hour)
	})
	var b strings.Builder
	for _, k := range keys {
		a := m[k]
		fmt.Fprintf(&b, "%s\t%s\t%s\t%d\t%d\t%d\t%s\t%d\n", k.tenant, k.campaign, k.hour, a.events, a.clicks, a.cost, a.last, len(a.users))
	}
	return b.String()
}

// rollupRow is one parsed line of rollupQuery / sourceAggQuery output.
type rollupRow struct {
	events, clicks, cost, users uint64
	last                        string
}

func parseRollup(out string) (map[aggKey]rollupRow, error) {
	m := map[aggKey]rollupRow{}
	for _, f := range parseTSV(out) {
		if len(f) != 8 {
			return nil, fmt.Errorf("rollup line has %d fields: %q", len(f), strings.Join(f, "\t"))
		}
		var r rollupRow
		var err error
		nums := []*uint64{&r.events, &r.clicks, &r.cost}
		for i, p := range nums {
			if *p, err = strconv.ParseUint(f[3+i], 10, 64); err != nil {
				return nil, err
			}
		}
		r.last = f[6]
		if r.users, err = strconv.ParseUint(f[7], 10, 64); err != nil {
			return nil, err
		}
		k := aggKey{f[0], f[1], f[2]}
		if _, dup := m[k]; dup {
			return nil, fmt.Errorf("duplicate rollup key %v", k)
		}
		m[k] = r
	}
	return m, nil
}

// ---------------------------------------------------------------------------
// Event log shared by concurrent writers
// ---------------------------------------------------------------------------

type eventLog struct {
	mu  sync.Mutex
	evs []event
}

func (l *eventLog) add(evs []event) {
	l.mu.Lock()
	l.evs = append(l.evs, evs...)
	l.mu.Unlock()
}

func (l *eventLog) all() []event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.evs)
}

// insertEvents inserts evs into db.events through r in JSONEachRow batches
// (alternating plain and gzip bodies) and records them in log after each
// successful batch.
func insertEvents(r *region, db string, evs []event, batch int, log *eventLog) error {
	for i := 0; i < len(evs); i += batch {
		chunk := evs[i:min(i+batch, len(evs))]
		if err := r.insertJSON(db+".events", jsonRows(chunk), (i/batch)%2 == 1); err != nil {
			return fmt.Errorf("insert %d rows into %s.events via %s: %w", len(chunk), db, r.name, err)
		}
		if log != nil {
			log.add(chunk)
		}
	}
	return nil
}

func newRNG(t testing.TB) *rand.Rand {
	seed := uint64(time.Now().UnixNano())
	t.Logf("random seed %d", seed)
	return newSeeded(seed)
}

func newSeeded(seed uint64) *rand.Rand { return rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)) }

// checkConverged waits until both regions hold exactly evs in db.events and
// the matching rollup (also cross-checked against an aggregation over each
// region's own source rows).
func checkConverged(t testing.TB, db string, evs []event, timeout time.Duration) {
	t.Helper()
	wantSrc, wantRoll := expectedSource(evs), expectedRollup(evs)
	wantCount := fmt.Sprintf("%d\n", len(evs))
	_ = runAll(
		func() error {
			waitResult(t, us(), timeout, "source count", "SELECT count() FROM "+db+".events", nil, wantCount)
			waitResult(t, us(), timeout, "source rows", sourceQuery(db), nil, wantSrc)
			waitResult(t, us(), timeout, "rollup", rollupQuery(db), nil, wantRoll)
			waitResult(t, us(), timeout, "aggregation over source", sourceAggQuery(db), nil, wantRoll)
			return nil
		},
		func() error {
			waitResult(t, eu(), timeout, "source count", "SELECT count() FROM "+db+".events", nil, wantCount)
			waitResult(t, eu(), timeout, "source rows", sourceQuery(db), nil, wantSrc)
			waitResult(t, eu(), timeout, "rollup", rollupQuery(db), nil, wantRoll)
			waitResult(t, eu(), timeout, "aggregation over source", sourceAggQuery(db), nil, wantRoll)
			return nil
		},
	)
}
