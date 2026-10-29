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
	`CREATE MATERIALIZED VIEW events_daily_mv TO events_daily AS
		SELECT tenant_id, toDate(ts) AS day, kind, count() AS n, sum(amount) AS amount
		FROM events_mv_src GROUP BY tenant_id, day, kind`,
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
}

// bench holds the run's servers and settings.
type bench struct {
	th, ch  *server
	cfg     config
	quiet   *quiescence
	logf    func(format string, args ...any)
	results *results
}

func (b *bench) sides() []*server { return []*server{b.th, b.ch} }

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
		for i, q := range schema {
			if i == len(schema)-1 && s == b.th {
				if err := primeStreams(ctx, s); err != nil {
					return err
				}
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
// before the view exists, so the sentinel never reaches the rollup.
func primeStreams(ctx context.Context, s *server) error {
	for _, q := range []string{
		`INSERT INTO events (tenant_id, ts, event_id) VALUES (0, '2000-01-01 00:00:00', generateUUIDv4())`,
		`INSERT INTO events_mv_src (tenant_id, ts, event_id) VALUES (0, '2000-01-01 00:00:00', generateUUIDv4())`,
		`INSERT INTO events_small (tenant_id, ts, event_id) VALUES (0, '2000-01-01 00:00:00', generateUUIDv4())`,
		`INSERT INTO users_latest (user_id, ver) VALUES (0, 0)`,
		`ALTER TABLE events DELETE WHERE tenant_id = 0`,
		`ALTER TABLE events_mv_src DELETE WHERE tenant_id = 0`,
		`ALTER TABLE events_small DELETE WHERE tenant_id = 0`,
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
		rows, took, err := b.load(ctx, "events", func() ([]byte, int) { return g.batch(batchRows) })
		if err != nil {
			return err
		}
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
		b.results.Context.ViewCatchUp = b.quiet.wait(ctx).Round(time.Second).String()
	}

	// W3: single-row inserts from concurrent clients.
	if b.cfg.wants("W3") {
		b.logf("W3: %d single-row inserts per side, %d clients", b.cfg.smallInserts, b.cfg.conc)
		if err := b.smallInserts(ctx); err != nil {
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
			if e1 != nil || e2 != nil || math.Abs(x-y) > 1e-9*math.Max(1, math.Max(math.Abs(x), math.Abs(y))) {
				return false
			}
		}
	}
	return true
}

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
			if err := b.readCase(ctx, rc, conc, b.cfg.seed+200+uint64(i)*10+uint64(conc)); err != nil {
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
