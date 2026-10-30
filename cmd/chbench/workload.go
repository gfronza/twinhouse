package main

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// defaultEventsKey is events' sorting key; -events-key replaces it.
const defaultEventsKey = "(tenant_id, ts)"

// The same DDL runs on both servers. events is keyed for DynamoDB as
// _pk = tenant_id, _sk = ts (+ uniquifier), so tenant/time ranges are Queries.
var schema = []string{
	`CREATE TABLE events (
		tenant_id UInt32, ts DateTime64(3), event_id UUID, user_id UInt64,
		kind String, url String, duration_ms UInt32, amount Float64
	) ENGINE = MergeTree ORDER BY (tenant_id, ts)`,
	`CREATE TABLE events_mv_src AS events`,
	`CREATE TABLE events_small AS events`,
	`CREATE TABLE users_latest (
		user_id UInt64, ver UInt64, plan String, country String, updated_at DateTime
	) ENGINE = ReplacingMergeTree(ver) ORDER BY user_id`,
	`CREATE TABLE events_daily (
		tenant_id UInt32, day Date, kind String, n UInt64, amount Float64
	) ENGINE = SummingMergeTree ORDER BY (tenant_id, day, kind)`,
	// W4's source and rollups. kind_totals has a handful of rows and
	// tenant_totals is skewed like the tenants, so concurrent inserts keep
	// updating the same rollup rows.
	`CREATE TABLE events_agg_src AS events`,
	`CREATE TABLE kind_totals (
		kind String, n SimpleAggregateFunction(sum, UInt64),
		users AggregateFunction(uniqExact, UInt64), last SimpleAggregateFunction(max, DateTime64(3))
	) ENGINE = AggregatingMergeTree ORDER BY kind`,
	`CREATE TABLE tenant_totals (
		tenant_id UInt32, kind String, n SimpleAggregateFunction(sum, UInt64),
		users AggregateFunction(uniqExact, UInt64), last SimpleAggregateFunction(max, DateTime64(3))
	) ENGINE = AggregatingMergeTree ORDER BY (tenant_id, kind)`,
	`CREATE TABLE tenant_hourly (
		tenant_id UInt32, hour DateTime, kind String, n SimpleAggregateFunction(sum, UInt64),
		amount SimpleAggregateFunction(sum, Float64)
	) ENGINE = AggregatingMergeTree ORDER BY (tenant_id, hour, kind)`,

	// Views come last: primeStreams runs before the first one.
	`CREATE MATERIALIZED VIEW events_daily_mv TO events_daily AS
		SELECT tenant_id, toDate(ts) AS day, kind, count() AS n, sum(amount) AS amount
		FROM events_mv_src GROUP BY tenant_id, day, kind`,
	`CREATE MATERIALIZED VIEW kind_totals_mv TO kind_totals AS
		SELECT kind, toUInt64(count()) AS n, uniqExactState(user_id) AS users, max(ts) AS last
		FROM events_agg_src GROUP BY kind`,
	`CREATE MATERIALIZED VIEW tenant_totals_mv TO tenant_totals AS
		SELECT tenant_id, kind, toUInt64(count()) AS n, uniqExactState(user_id) AS users, max(ts) AS last
		FROM events_agg_src GROUP BY tenant_id, kind`,
	`CREATE MATERIALIZED VIEW tenant_hourly_mv TO tenant_hourly AS
		SELECT tenant_id, toStartOfHour(ts) AS hour, kind, toUInt64(count()) AS n, sum(amount) AS amount
		FROM events_agg_src GROUP BY tenant_id, hour, kind`,
}

// rollupChecks read W4's rollups; both servers must return the same rows.
var rollupChecks = []string{
	"SELECT count() FROM events_agg_src",
	"SELECT kind, sum(n), uniqExactMerge(users), max(last) FROM kind_totals GROUP BY kind ORDER BY kind",
	"SELECT tenant_id, kind, sum(n), uniqExactMerge(users), max(last) FROM tenant_totals GROUP BY tenant_id, kind ORDER BY tenant_id, kind",
	"SELECT tenant_id, hour, kind, sum(n), sum(amount) FROM tenant_hourly GROUP BY tenant_id, hour, kind ORDER BY tenant_id, hour, kind",
}

// readCase is one read query shape. sql draws its parameters from p.
type readCase struct {
	id, shape string
	conc      []int
	sql       func(p *params) string
}

const dt64 = "2006-01-02 15:04:05.000"

var reads = []readCase{
	{"R1", "point lookup (ReplacingMergeTree FINAL)", []int{1, 8}, func(p *params) string {
		return fmt.Sprintf("SELECT * FROM users_latest FINAL WHERE user_id = %d", p.user())
	}},
	{"R2", "key range, aggregated (1 hour of a tenant)", []int{1, 8}, func(p *params) string {
		h := p.hour()
		return fmt.Sprintf("SELECT count(), sum(amount) FROM events WHERE tenant_id = %d AND ts >= toDateTime64('%s', 3) AND ts < toDateTime64('%s', 3)",
			p.tenant(), h.Format(dt64), h.Add(time.Hour).Format(dt64))
	}},
	{"R3", "key range, top-N (latest 100 of a tenant)", []int{1, 8}, func(p *params) string {
		return fmt.Sprintf("SELECT * FROM events WHERE tenant_id = %d ORDER BY ts DESC LIMIT 100", p.tenant())
	}},
	{"R4", "rollup read (SummingMergeTree, one tenant)", []int{1, 8}, func(p *params) string {
		return fmt.Sprintf("SELECT day, kind, sum(n), sum(amount) FROM events_daily WHERE tenant_id = %d GROUP BY day, kind ORDER BY day, kind", p.tenant())
	}},
	{"R5", "full scan + filter + GROUP BY", []int{1}, func(*params) string {
		return "SELECT kind, count(), avg(duration_ms) FROM events WHERE url LIKE '%/checkout/%' GROUP BY kind ORDER BY kind"
	}},
	{"R6", "high-cardinality GROUP BY (50k users)", []int{1}, func(*params) string {
		return "SELECT user_id, count() AS c FROM events GROUP BY user_id ORDER BY c DESC, user_id LIMIT 10"
	}},
	{"R7", "rollup read with uniqExactMerge (one tenant)", []int{1, 8}, func(p *params) string {
		return fmt.Sprintf("SELECT kind, sum(n), uniqExactMerge(users), max(last) FROM tenant_totals WHERE tenant_id = %d AND kind IN ('view', 'click', 'purchase') GROUP BY kind ORDER BY kind", p.tenant())
	}},
	{"R8", "rollup read with uniqExactMerge (largest sets, all kinds)", []int{1, 8}, func(*params) string {
		return "SELECT sum(n), uniqExactMerge(users), max(last) FROM kind_totals"
	}},
	{"R9", "latest state per user (argMax, nested GROUP BY, one tenant)", []int{1, 8}, func(p *params) string {
		return fmt.Sprintf("SELECT latest, count(), sum(c) FROM (SELECT user_id, argMax(kind, ts) AS latest, count() AS c FROM events WHERE tenant_id = %d GROUP BY user_id) GROUP BY latest ORDER BY latest", p.tenant())
	}},
	{"R10", "duplicate check (tuple IN + 10-minute window)", []int{1, 8}, func(p *params) string {
		at := p.minute()
		keys := make([]string, 3)
		for i := range keys {
			keys[i] = fmt.Sprintf("(%d, %d, '%s')", p.tenant(), p.user(), p.kind())
		}
		return fmt.Sprintf("SELECT tenant_id, user_id, kind, event_id, ts FROM events WHERE ts BETWEEN toDateTime64('%s', 3) AND toDateTime64('%s', 3) AND (tenant_id, user_id, kind) IN (%s) ORDER BY ts",
			at.Add(-5*time.Minute).Format(dt64), at.Add(5*time.Minute).Format(dt64), strings.Join(keys, ", "))
	}},
}

// bench holds the run's servers and settings.
type bench struct {
	th, ch  *server
	cfg     config
	quiet   idler
	logf    func(format string, args ...any)
	results *results
}

func (b *bench) sides() []*server { return []*server{b.th, b.ch} }

// step records when step id runs, so DynamoDB's consumed capacity can be
// matched to it afterwards (CloudWatch sums it per minute). With
// -align-steps, the step starts on a minute boundary; call the returned
// func when it ends.
func (b *bench) step(ctx context.Context, id string) func() {
	if b.cfg.align {
		now := time.Now()
		sleep(ctx, now.Truncate(time.Minute).Add(time.Minute+time.Second).Sub(now))
	}
	start := time.Now().UTC()
	return func() {
		b.results.Steps = append(b.results.Steps, stepWindow{ID: id, Start: start, End: time.Now().UTC()})
	}
}

// ordered alternates which side goes first, so a load swing on the host
// does not always hit the same side.
func (b *bench) ordered(i int) []*server {
	if i%2 == 0 {
		return []*server{b.th, b.ch}
	}
	return []*server{b.ch, b.th}
}

func (b *bench) setup(ctx context.Context) error {
	for _, s := range b.sides() {
		d := *s
		d.db = "default"
		if err := d.exec(ctx, "DROP DATABASE IF EXISTS "+s.db); err != nil {
			return err
		}
		if err := d.exec(ctx, "CREATE DATABASE "+s.db); err != nil {
			return err
		}
		primed := false
		for i, q := range schema {
			if i == 0 {
				q = strings.Replace(q, "ORDER BY "+defaultEventsKey, "ORDER BY ("+b.cfg.eventsKey+")", 1)
			}
			if !primed && s == b.th && strings.HasPrefix(q, "CREATE MATERIALIZED VIEW") {
				if err := primeStreams(ctx, s); err != nil {
					return err
				}
				primed = true
			}
			if err := s.exec(ctx, q); err != nil {
				return err
			}
		}
	}
	return nil
}

// primeStreams writes and deletes one sentinel row in each of twinhouse's
// stream-enabled tables before any load. DynamoDB Local slows every write
// to a stream-enabled table in proportion to the stream records written
// before that table's first record (about 0.8 s per 25-item batch after
// 200k records), so a table first written after a large load would measure
// the emulator rather than twinhouse. DynamoDB has no such cost. It runs
// before the views exist, so the sentinels never reach the rollups.
func primeStreams(ctx context.Context, s *server) error {
	for _, q := range []string{
		`INSERT INTO events (tenant_id, ts, event_id) VALUES (0, '2000-01-01 00:00:00', generateUUIDv4())`,
		`INSERT INTO events_mv_src (tenant_id, ts, event_id) VALUES (0, '2000-01-01 00:00:00', generateUUIDv4())`,
		`INSERT INTO events_small (tenant_id, ts, event_id) VALUES (0, '2000-01-01 00:00:00', generateUUIDv4())`,
		`INSERT INTO events_agg_src (tenant_id, ts, event_id) VALUES (0, '2000-01-01 00:00:00', generateUUIDv4())`,
		`INSERT INTO users_latest (user_id, ver) VALUES (0, 0)`,
		`ALTER TABLE events DELETE WHERE tenant_id = 0`,
		`ALTER TABLE events_mv_src DELETE WHERE tenant_id = 0`,
		`ALTER TABLE events_small DELETE WHERE tenant_id = 0`,
		`ALTER TABLE events_agg_src DELETE WHERE tenant_id = 0`,
		`ALTER TABLE users_latest DELETE WHERE user_id = 0`,
	} {
		if err := s.exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// load sends the batches of next to table on both sides, batch by batch,
// and returns each side's total insert time.
func (b *bench) load(ctx context.Context, table string, next func() ([]byte, int)) (rows int, took map[string]time.Duration, err error) {
	took = map[string]time.Duration{}
	q := "INSERT INTO " + table + " FORMAT JSONEachRow"
	for i := 0; ; i++ {
		data, n := next()
		if n == 0 {
			return rows, took, nil
		}
		for _, s := range b.ordered(i) {
			r, err := s.do(ctx, q, data, nil, false, false)
			if err != nil {
				return rows, took, err
			}
			took[s.name] += r.elapsed
		}
		rows += n
		if i%20 == 19 {
			b.logf("  %s: %d rows", table, rows)
		}
	}
}

func (b *bench) writes(ctx context.Context) error {
	// W1: bulk load of the main table.
	if b.cfg.wants("W1") || b.cfg.wantsReads() {
		g := newEventGen(b.cfg.seed, b.cfg.rows)
		b.logf("W1: loading %d events", b.cfg.rows)
		done := b.step(ctx, "W1")
		rows, took, err := b.load(ctx, "events", func() ([]byte, int) { return g.batch(batchRows) })
		if err != nil {
			return err
		}
		done()
		b.results.addThroughput("W1", "bulk load, 10k rows per INSERT, no views", rows, took)

		ub := userBatches(b.cfg.seed)
		i := 0
		b.logf("loading users_latest (%d users × %d versions)", users, versions)
		if _, _, err := b.load(ctx, "users_latest", func() ([]byte, int) {
			if i == len(ub) {
				return nil, 0
			}
			i++
			return ub[i-1], strings.Count(string(ub[i-1]), "\n")
		}); err != nil {
			return err
		}
	}

	// W2: the same rows into a table with a materialized view.
	if b.cfg.wants("W2") || b.cfg.wantsReads() {
		n := min(b.cfg.rows, b.cfg.mvRows)
		g := newEventGen(b.cfg.seed, b.cfg.rows)
		left := n
		b.logf("W2: loading %d events through the view", n)
		done := b.step(ctx, "W2")
		rows, took, err := b.load(ctx, "events_mv_src", func() ([]byte, int) {
			data, k := g.batch(min(batchRows, left))
			left -= k
			return data, k
		})
		if err != nil {
			return err
		}
		b.results.addThroughput("W2", "bulk load, 10k rows per INSERT, one SummingMergeTree view", rows, took)
		b.logf("waiting for background work (stream worker, merges) to finish")
		if d, ok := b.quiet.wait(ctx); ok {
			b.results.Context.ViewCatchUp = d.Round(time.Second).String()
		}
		done() // the window includes the stream worker's re-check
	}

	// W3: single-row inserts from concurrent clients.
	if b.cfg.wants("W3") {
		b.logf("W3: %d single-row inserts per side, %d clients", b.cfg.smallInserts, b.cfg.conc)
		done := b.step(ctx, "W3")
		if err := b.smallInserts(ctx); err != nil {
			return err
		}
		done()
	}

	// W5: bulk load through W4's three views. It runs before W4, so W4
	// updates rollups that already hold large sets, and R7/R8 read them.
	if b.cfg.wants("W5") || b.cfg.wants("R7") || b.cfg.wants("R8") {
		n := min(b.cfg.rows, b.cfg.mvRows)
		g := newEventGen(b.cfg.seed, b.cfg.rows)
		left := n
		b.logf("W5: loading %d events through 3 views", n)
		done := b.step(ctx, "W5")
		rows, took, err := b.load(ctx, "events_agg_src", func() ([]byte, int) {
			data, k := g.batch(min(batchRows, left))
			left -= k
			return data, k
		})
		if err != nil {
			return err
		}
		b.results.addThroughput("W5", "bulk load, 10k rows per INSERT, three AggregatingMergeTree views", rows, took)
		b.logf("waiting for background work (stream worker, merges) to finish")
		if d, ok := b.quiet.wait(ctx); ok {
			b.results.Context.ViewCatchUpW5 = d.Round(time.Second).String()
		}
		done()
		if err := b.checkRollups(ctx); err != nil {
			return fmt.Errorf("W5: %w", err)
		}
	}

	// W4: single-row inserts that also update three rollups.
	if b.cfg.wants("W4") {
		b.logf("W4: %d single-row inserts per side through 3 views, %d clients", b.cfg.smallInserts, b.cfg.conc)
		done := b.step(ctx, "W4")
		if err := b.viewInserts(ctx); err != nil {
			return err
		}
		done()
	}
	return nil
}

// viewInserts runs W4. Both servers get the same rows, so their rollups can
// be compared afterwards. ClickHouse runs without async_insert here: W3
// already shows what that setting costs per insert.
func (b *bench) viewInserts(ctx context.Context) error {
	g := newEventGen(b.cfg.seed+10, b.cfg.smallInserts)
	lat := map[string][]time.Duration{}
	wall := map[string]time.Duration{}
	const chunk = 200
	for done := 0; done < b.cfg.smallInserts; done += chunk {
		bodies := make([][]byte, min(chunk, b.cfg.smallInserts-done))
		for i := range bodies {
			bodies[i], _ = g.batch(1)
		}
		for _, s := range b.ordered(done / chunk) {
			start := time.Now()
			l, err := fanOut(ctx, b.cfg.conc, bodies, func(body []byte) (time.Duration, error) {
				r, err := s.do(ctx, "INSERT INTO events_agg_src FORMAT JSONEachRow", body, nil, false, false)
				return r.elapsed, err
			})
			if err != nil {
				return err
			}
			wall[s.name] += time.Since(start)
			lat[s.name] = append(lat[s.name], l...)
		}
	}
	what := "single-row INSERT through 3 AggregatingMergeTree views, " + strconv.Itoa(b.cfg.conc) + " concurrent clients"
	for _, s := range b.sides() {
		b.results.addLatency("W4", what, s.name, lat[s.name], wall[s.name])
	}
	// A view application that fails inline is retried by the stream worker,
	// so wait for it before comparing.
	_, _ = b.quiet.wait(ctx)
	if err := b.checkRollups(ctx); err != nil {
		return fmt.Errorf("W4: %w", err)
	}
	return nil
}

func (b *bench) checkRollups(ctx context.Context) error {
	for _, q := range rollupChecks {
		if err := b.same(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func (b *bench) smallInserts(ctx context.Context) error {
	type variant struct {
		label string
		s     *server
		set   url.Values
	}
	vs := []variant{
		{"twinhouse", b.th, nil},
		{"clickhouse", b.ch, nil},
		{"clickhouse async_insert=1", b.ch, url.Values{"async_insert": {"1"}, "wait_for_async_insert": {"1"}}},
	}
	g := newEventGen(b.cfg.seed+9, len(vs)*b.cfg.smallInserts)
	lat := make([][]time.Duration, len(vs))
	wall := make([]time.Duration, len(vs))
	const chunk = 200
	for done := 0; done < b.cfg.smallInserts; done += chunk {
		k := min(chunk, b.cfg.smallInserts-done)
		for j := range vs {
			idx := (j + done/chunk) % len(vs)
			v := vs[idx]
			bodies := make([][]byte, k)
			for i := range bodies {
				bodies[i], _ = g.batch(1)
			}
			start := time.Now()
			l, err := fanOut(ctx, b.cfg.conc, bodies, func(body []byte) (time.Duration, error) {
				r, err := v.s.do(ctx, "INSERT INTO events_small FORMAT JSONEachRow", body, v.set, false, false)
				return r.elapsed, err
			})
			if err != nil {
				return err
			}
			wall[idx] += time.Since(start)
			lat[idx] = append(lat[idx], l...)
		}
	}
	for i, v := range vs {
		b.results.addLatency("W3", "single-row INSERT, "+strconv.Itoa(b.cfg.conc)+" concurrent clients", v.label, lat[i], wall[i])
	}
	return nil
}

// fanOut runs f over items with conc workers and returns each call's latency.
func fanOut[T any](ctx context.Context, conc int, items []T, f func(T) (time.Duration, error)) ([]time.Duration, error) {
	out := make([]time.Duration, len(items))
	errs := make([]error, len(items))
	var wg sync.WaitGroup
	next := make(chan int)
	for range conc {
		wg.Go(func() {
			for i := range next {
				out[i], errs[i] = f(items[i])
			}
		})
	}
	for i := range items {
		select {
		case next <- i:
		case <-ctx.Done():
		}
	}
	close(next)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return out, ctx.Err()
}

// gate compares both servers' results before any read is timed, so a
// wrong answer can never be reported as a fast one.
func (b *bench) gate(ctx context.Context) error {
	for _, t := range []string{"events", "events_mv_src", "users_latest FINAL", "events_daily FINAL"} {
		if err := b.same(ctx, "SELECT count() FROM "+t); err != nil {
			return err
		}
	}
	for i, rc := range reads {
		if !b.cfg.wants(rc.id) {
			continue
		}
		p := newParams(b.cfg.seed + 100 + uint64(i))
		for range 3 {
			if err := b.same(ctx, rc.sql(p)); err != nil {
				return fmt.Errorf("%s: %w", rc.id, err)
			}
		}
	}
	return nil
}

func (b *bench) same(ctx context.Context, q string) error {
	a, err := b.th.text(ctx, q)
	if err != nil {
		return err
	}
	c, err := b.ch.text(ctx, q)
	if err != nil {
		return err
	}
	if !sameTSV(a, c) {
		return fmt.Errorf("results differ\nquery: %s\ntwinhouse:\n%s\nclickhouse:\n%s", q, clip([]byte(a), 1500), clip([]byte(c), 1500))
	}
	return nil
}

// sameTSV compares two TSV results field by field; numbers that are not
// integers may differ in the last bits (float sums depend on order).
func sameTSV(a, b string) bool {
	al, bl := strings.Split(a, "\n"), strings.Split(b, "\n")
	if len(al) != len(bl) {
		return false
	}
	for i := range al {
		af, bf := strings.Split(al[i], "\t"), strings.Split(bl[i], "\t")
		if len(af) != len(bf) {
			return false
		}
		for j := range af {
			if af[j] == bf[j] {
				continue
			}
			x, e1 := strconv.ParseFloat(af[j], 64)
			y, e2 := strconv.ParseFloat(bf[j], 64)
			// NaN and Inf only match themselves, and the strings differ.
			if e1 != nil || e2 != nil || !finite(x) || !finite(y) ||
				math.Abs(x-y) > 1e-9*math.Max(1, math.Max(math.Abs(x), math.Abs(y))) {
				return false
			}
		}
	}
	return true
}

func finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }

func (b *bench) readsPhase(ctx context.Context) error {
	for i, rc := range reads {
		if !b.cfg.wants(rc.id) {
			continue
		}
		for _, conc := range rc.conc {
			if conc > 1 && b.cfg.conc <= 1 {
				continue
			}
			if conc > 1 {
				conc = b.cfg.conc
			}
			b.logf("%s, concurrency %d", rc.id, conc)
			done := b.step(ctx, fmt.Sprintf("%s c%d", rc.id, conc))
			err := b.readCase(ctx, rc, conc, b.cfg.seed+200+uint64(i)*10+uint64(conc))
			done()
			if err != nil {
				return fmt.Errorf("%s: %w", rc.id, err)
			}
		}
	}
	return nil
}

// readCase times one query shape at one concurrency. Each round draws conc
// parameter sets and runs them on both sides, in alternating order.
func (b *bench) readCase(ctx context.Context, rc readCase, conc int, seed uint64) error {
	p := newParams(seed)
	run := func(s *server, qs []string) ([]time.Duration, time.Duration, error) {
		start := time.Now()
		l, err := fanOut(ctx, conc, qs, func(q string) (time.Duration, error) {
			r, err := s.do(ctx, q, nil, nil, false, false)
			return r.elapsed, err
		})
		return l, time.Since(start), err
	}
	draw := func() []string {
		qs := make([]string, conc)
		for i := range qs {
			qs[i] = rc.sql(p)
		}
		return qs
	}
	// Warm-up; its timings also size the run.
	slowest := time.Duration(0)
	for w := range b.cfg.warmup {
		qs := draw()
		for _, s := range b.ordered(w) {
			_, d, err := run(s, qs)
			if err != nil {
				return err
			}
			slowest = max(slowest, d)
		}
	}
	rounds := b.cfg.rounds
	if slowest > 2*time.Second {
		rounds = max(5, rounds/4)
	}
	lat := map[string][]time.Duration{}
	wall := map[string]time.Duration{}
	for r := range rounds {
		qs := draw()
		for _, s := range b.ordered(r) {
			l, d, err := run(s, qs)
			if err != nil {
				return err
			}
			lat[s.name] = append(lat[s.name], l...)
			wall[s.name] += d
		}
	}
	b.results.addRead(rc, conc, rounds, lat, wall)
	return nil
}
