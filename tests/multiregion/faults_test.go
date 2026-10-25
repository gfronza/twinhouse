//go:build multiregion

package multiregion

// Fault-injection scenarios. They stop / kill / recreate containers of the
// compose project, so they never call t.Parallel: Go runs them one after the
// other before any parallel test starts, and each restores the stack.

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// streamWriter keeps inserting fresh batches into db.events through r until
// stop is closed; every acknowledged batch is recorded in log.
type streamWriter struct {
	r       *region
	db      string
	batch   int
	pause   time.Duration
	spec    genSpec
	log     *eventLog
	written atomic.Int64
	err     error
}

func (w *streamWriter) run(stop <-chan struct{}, seed uint64, wg *sync.WaitGroup) {
	defer wg.Done()
	rng := newSeeded(seed)
	for i := 0; ; i++ {
		select {
		case <-stop:
			return
		default:
		}
		s := w.spec
		s.n = w.batch
		s.idPrefix = fmt.Sprintf("%s-%04d", w.spec.idPrefix, i)
		evs := genEvents(rng, s)
		if err := insertEvents(w.r, w.db, evs, w.batch, w.log); err != nil {
			w.err = err
			return
		}
		w.written.Add(int64(len(evs)))
		time.Sleep(w.pause)
	}
}

// waitWritten waits until the writers wrote at least n rows in total.
func waitWritten(t *testing.T, n int64, ws ...*streamWriter) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		var total int64
		for _, w := range ws {
			if w.err != nil {
				t.Fatalf("writer %s failed: %v", w.r.name, w.err)
			}
			total += w.written.Load()
		}
		if total >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("writers wrote only %d of %d rows", total, n)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Scenario 4: exactly-once under redelivery. While both regions keep
// inserting, mvworker-eu and mvworker-us are SIGKILLed (no graceful
// checkpoint) and restarted, the replicator is killed and restarted, and
// finally both workers are restarted gracefully. Batches that were applied
// but not checkpointed are redelivered; together with the inline application
// in the writer region the rollups must still be exact.
func TestFaultRedeliveryExactlyOnce(t *testing.T) {
	faultTest(t)
	db := setupSchema(t, "redeliv")
	spec := genSpec{tenants: 3, campaigns: 3, hours: 4, users: 80}
	var log eventLog
	wu := &streamWriter{r: us(), db: db, batch: 60, pause: 100 * time.Millisecond, log: &log, spec: spec}
	we := &streamWriter{r: eu(), db: db, batch: 30, pause: 150 * time.Millisecond, log: &log, spec: spec}
	wu.spec.idPrefix, we.spec.idPrefix = "us", "eu"
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	seed := uint64(time.Now().UnixNano())
	t.Logf("random seed %d", seed)
	go wu.run(stop, seed, &wg)
	go we.run(stop, seed+1, &wg)
	stopped := false
	defer func() {
		if !stopped {
			close(stop)
			wg.Wait()
		}
	}()

	steps := []struct {
		after int64
		args  []string
		pause time.Duration
	}{
		// Kill, let a backlog build up, restart, and kill again while the
		// worker is busy applying that backlog (most likely mid-batch).
		{150, []string{"kill", "mvworker-eu"}, 2 * time.Second},
		{0, []string{"start", "mvworker-eu"}, 400 * time.Millisecond},
		{0, []string{"kill", "mvworker-eu"}, time.Second},
		{0, []string{"start", "mvworker-eu"}, 0},
		{450, []string{"kill", "mvworker-us"}, 2 * time.Second},
		{0, []string{"start", "mvworker-us"}, 400 * time.Millisecond},
		{0, []string{"kill", "mvworker-us"}, time.Second},
		{0, []string{"start", "mvworker-us"}, 0},
		{700, []string{"kill", "replicator"}, 2 * time.Second},
		{0, []string{"start", "replicator"}, 0},
		{950, []string{"kill", "mvworker-eu", "mvworker-us"}, 500 * time.Millisecond},
		{0, []string{"start", "mvworker-eu", "mvworker-us"}, 0},
		{1200, []string{"restart", "mvworker-eu", "mvworker-us", "replicator"}, 0},
	}
	for _, s := range steps {
		if s.after > 0 {
			waitWritten(t, s.after, wu, we)
		}
		compose(t, s.args...)
		time.Sleep(s.pause)
	}
	waitWritten(t, 1400, wu, we)
	close(stop)
	wg.Wait()
	stopped = true
	for _, w := range []*streamWriter{wu, we} {
		if w.err != nil {
			t.Fatalf("writer %s: %v", w.r.name, w.err)
		}
	}
	all := log.all()
	t.Logf("%d rows written (us %d, eu %d) across worker/replicator kills", len(all), wu.written.Load(), we.written.Load())
	checkConverged(t, db, all, 2*cfg.converge)
	// Once every worker has checkpointed everything, nothing changes anymore.
	waitCaughtUp(t, ddbUS(), db, "events")
	waitCaughtUp(t, ddbEU(), db, "events")
	want := expectedRollup(all)
	for _, r := range []*region{us(), eu()} {
		if got := r.mustQuery(t, rollupQuery(db), nil); got != want {
			t.Errorf("%s: rollup after workers caught up:\n%s", r.name, diffText(want, got))
		}
	}
}

// Scenario 6 (deterministic variant): with the replicator stopped, both
// regions accept conflicting versions of the same keys; the lower version is
// written LATER in wall-clock time. After the replicator resumes, both
// regions must agree and hold the max-version row (ReplacingMergeTree(ver)
// keeps the highest version, regardless of insertion time).
func TestFaultReplacingConflictWhilePartitioned(t *testing.T) {
	faultTest(t)
	db := setupSchema(t, "replpart")
	const tenant = "tenant-07"
	compose(t, "stop", "replicator")

	a2 := stateVersion(tenant, "cmp-a", 2, "eu") // max version, written first (eu)
	a1 := stateVersion(tenant, "cmp-a", 1, "us") // lower version, written later (us)
	b2 := stateVersion(tenant, "cmp-b", 2, "us") // max version, written first (us)
	b1 := stateVersion(tenant, "cmp-b", 1, "eu") // lower version, written later (eu)
	c5 := stateVersion(tenant, "cmp-c", 5, "us") // only us
	d4 := stateVersion(tenant, "cmp-d", 4, "eu") // only eu
	for _, w := range []struct {
		r   *region
		row stateRow
	}{{eu(), a2}, {us(), b2}, {us(), c5}, {eu(), d4}, {us(), a1}, {eu(), b1}} {
		time.Sleep(20 * time.Millisecond) // distinct write timestamps
		if err := insertState(w.r, db, []stateRow{w.row}); err != nil {
			t.Fatal(err)
		}
	}
	q := fmt.Sprintf(stateQuery, db)
	p := map[string]string{"t": tenant}
	// While partitioned each region shows its own max version.
	if got, want := us().mustQuery(t, q, p), expectedState([]stateRow{a1, b2, c5}); got != want {
		t.Errorf("us while partitioned:\n%s", diffText(want, got))
	}
	if got, want := eu().mustQuery(t, q, p), expectedState([]stateRow{a2, b1, d4}); got != want {
		t.Errorf("eu while partitioned:\n%s", diffText(want, got))
	}
	compose(t, "start", "replicator")
	checkStateConverged(t, db, tenant, []stateRow{a2, a1, b2, b1, c5, d4})
}

// Scenario 7: with an artificial replication lag (REPLICATION_DELAY=5s on a
// recreated replicator), each region reads its own writes at once, the other
// region is stale at first and then converges. While converging, a
// region's rollup never shows more than its replicated source rows justify
// (and never more than the final values), and it never goes backwards.
func TestFaultReplicationLag(t *testing.T) {
	faultTest(t)
	const delay = 5 * time.Second
	db := setupSchema(t, "lag") // before the lag: the catalog is replicated too
	composeWithEnv(t, []string{"REPLICATION_DELAY=" + delay.String()},
		"up", "-d", "--no-build", "--no-deps", "--force-recreate", "--wait", "--wait-timeout", "120", "replicator")

	rng := newRNG(t)
	// Small batches: the writes must complete well within the delay so the
	// staleness probe below is meaningful even on a slow DynamoDB Local.
	spec := genSpec{tenants: 2, campaigns: 3, hours: 3, users: 40, n: 40}
	spec.idPrefix = "us"
	usEvs := genEvents(rng, spec)
	spec.idPrefix = "eu"
	euEvs := genEvents(rng, spec)
	all := append(slices.Clone(usEvs), euEvs...)

	t0 := time.Now()
	if err := runAll(
		func() error { return insertEvents(us(), db, usEvs, len(usEvs), nil) },
		func() error { return insertEvents(eu(), db, euEvs, len(euEvs), nil) },
	); err != nil {
		t.Fatal(err)
	}
	insertDur := time.Since(t0)
	// Reads right after the writes: own rows complete, the other region's
	// rows not there yet.
	for _, c := range []struct {
		r          *region
		own, other []event
	}{{us(), usEvs, euEvs}, {eu(), euEvs, usEvs}} {
		got := c.r.mustQuery(t, "SELECT count() FROM "+db+".events WHERE startsWith(event_id, {p:String})",
			map[string]string{"p": c.own[0].ID[:3]})
		if got != fmt.Sprintf("%d\n", len(c.own)) {
			t.Errorf("%s: own rows right after INSERT = %q, want %d", c.r.name, got, len(c.own))
		}
		if el := time.Since(t0); el < delay-time.Second {
			got := c.r.mustQuery(t, "SELECT count() FROM "+db+".events WHERE startsWith(event_id, {p:String})",
				map[string]string{"p": c.other[0].ID[:3]})
			if got != "0\n" {
				t.Errorf("%s: %s after the writes (delay %s) already sees %q remote rows", c.r.name, el.Round(time.Millisecond), delay, got)
			}
		} else {
			t.Logf("inserts took %s; skipping the staleness probe", insertDur)
		}
	}

	final := aggregate(all)
	var wg sync.WaitGroup
	for _, r := range []*region{us(), eu()} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			watchConvergence(t, r, db, all, final, t0, delay)
		}()
	}
	wg.Wait()
	checkConverged(t, db, all, cfg.converge)
}

// watchConvergence polls a region until its source rows and rollup are
// final, checking the in-flight invariants at every poll.
func watchConvergence(t *testing.T, r *region, db string, all []event, final map[aggKey]*agg, t0 time.Time, delay time.Duration) {
	wantSrc, wantRoll := expectedSource(all), expectedRollup(all)
	prev := map[aggKey]rollupRow{}
	deadline := time.Now().Add(cfg.converge)
	violations := 0
	report := func(format string, args ...any) {
		if violations < 10 {
			t.Errorf(r.name+": "+format, args...)
		}
		violations++
	}
	for polls := 0; ; polls++ {
		rollOut, err1 := r.query(rollupQuery(db), nil)   // rollup first ...
		srcOut, err2 := r.query(sourceAggQuery(db), nil) // ... then the source it derives from
		if err1 == nil && err2 == nil {
			roll, err := parseRollup(rollOut)
			if err != nil {
				report("%v", err)
			}
			src, err := parseRollup(srcOut)
			if err != nil {
				report("%v", err)
			}
			for k, v := range roll {
				s, f := src[k], final[k]
				switch {
				case f == nil:
					report("rollup has unexpected key %v: %+v", k, v)
				case v.events > s.events || v.clicks > s.clicks || v.cost > s.cost || v.users > s.users || v.last > s.last:
					report("rollup %v = %+v exceeds what the replicated source rows justify %+v", k, v, s)
				case v.events > f.events || v.clicks > f.clicks || v.cost > f.cost || v.users > uint64(len(f.users)) || v.last > f.last:
					report("rollup %v = %+v exceeds the final value", k, v)
				}
				if p, ok := prev[k]; ok && (v.events < p.events || v.clicks < p.clicks || v.cost < p.cost || v.users < p.users || v.last < p.last) {
					report("rollup %v went backwards: %+v -> %+v", k, p, v)
				}
			}
			for k := range prev {
				if _, ok := roll[k]; !ok {
					report("rollup key %v disappeared", k)
				}
			}
			prev = roll
			if rollOut == wantRoll {
				if got, err := r.query(sourceQuery(db), nil); err == nil && got == wantSrc {
					el := time.Since(t0)
					t.Logf("%s: converged %s after the writes (%d polls)", r.name, el.Round(10*time.Millisecond), polls)
					if el < delay-500*time.Millisecond {
						t.Errorf("%s: converged after %s although REPLICATION_DELAY=%s", r.name, el, delay)
					}
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Errorf("%s: not converged within %s (last errors: %v %v)\n%s", r.name, cfg.converge, err1, err2, diffText(wantRoll, rollOut))
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
}

const ddlDaily = `CREATE TABLE %s.rollup_daily (
	tenant_id String,
	day       Date,
	events    SimpleAggregateFunction(sum, UInt64),
	users     AggregateFunction(uniqExact, String)
) ENGINE = AggregatingMergeTree
ORDER BY (tenant_id, day)`

const ddlDailyMV = `CREATE MATERIALIZED VIEW %s.mv_daily TO %s.rollup_daily AS
SELECT tenant_id, toDate(event_time) AS day, count() AS events, uniqExactState(user_id) AS users
FROM %s.events GROUP BY tenant_id, day`

func dailyQuery(db string) string {
	return "SELECT tenant_id, day, sum(events), uniqExactMerge(users) FROM " + db +
		".rollup_daily GROUP BY tenant_id, day ORDER BY tenant_id, day"
}

func expectedDaily(evs []event) string {
	type k struct{ tenant, day string }
	n := map[k]int{}
	users := map[k]map[string]bool{}
	for _, e := range evs {
		key := k{e.Tenant, e.Time[:10]}
		n[key]++
		if users[key] == nil {
			users[key] = map[string]bool{}
		}
		users[key][e.User] = true
	}
	keys := make([]k, 0, len(n))
	for key := range n {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b k) int { return strings.Compare(a.tenant+a.day, b.tenant+b.day) })
	var b strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&b, "%s\t%s\t%d\t%d\n", key.tenant, key.day, n[key], len(users[key]))
	}
	return b.String()
}

// Scenario 8: region outage. chserver-eu and mvworker-eu are stopped while
// us keeps writing rows, ReplacingMergeTree updates and even new DDL (a
// second view with its own target table). When eu comes back it reconciles
// the new tables, its worker applies the backlog, and eu matches us exactly.
func TestFaultRegionOutage(t *testing.T) {
	faultTest(t)
	rng := newRNG(t)
	db := setupSchema(t, "outage")
	u, e := us(), eu()
	spec := genSpec{tenants: 3, campaigns: 3, hours: 5, users: 50, n: 150, idPrefix: "b1"}
	batch1 := genEvents(rng, spec)
	if err := insertEvents(u, db, batch1, 75, nil); err != nil {
		t.Fatal(err)
	}
	checkConverged(t, db, batch1, cfg.converge)
	waitCaughtUp(t, ddbUS(), db, "events")
	waitCaughtUp(t, ddbEU(), db, "events")
	if t.Failed() {
		t.FailNow()
	}

	compose(t, "stop", "chserver-eu", "mvworker-eu")
	if _, err := e.query("SELECT 1", nil); err == nil {
		t.Fatalf("eu still answers after chserver-eu was stopped: is MR_EU_URL the dhmr stack?")
	}

	// New DDL while eu is down.
	u.mustExec(t, fmt.Sprintf(ddlDaily, db))
	u.mustExec(t, fmt.Sprintf(ddlDailyMV, db, db, db))
	var outage []event
	var states []stateRow
	for i := 0; i < 3; i++ {
		spec.idPrefix = fmt.Sprintf("out%d", i)
		b := genEvents(rng, spec)
		if err := insertEvents(u, db, b, 75, nil); err != nil {
			t.Fatal(err)
		}
		outage = append(outage, b...)
		for c := 0; c < 4; c++ {
			s := stateVersion("tenant-01", fmt.Sprintf("cmp-%02d", c), uint64(10*i+c+1), "us")
			states = append(states, s)
			if err := insertState(u, db, []stateRow{s}); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Second)
	}
	all := append(slices.Clone(batch1), outage...)
	// us is complete right away.
	if got, want := u.mustQuery(t, rollupQuery(db), nil), expectedRollup(all); got != want {
		t.Errorf("us rollup during the eu outage:\n%s", diffText(want, got))
	}
	if got, want := u.mustQuery(t, dailyQuery(db), nil), expectedDaily(outage); got != want {
		t.Errorf("us daily rollup (view created during the outage):\n%s", diffText(want, got))
	}

	compose(t, "start", "chserver-eu", "mvworker-eu")
	waitPing(t, e)
	waitPhysical(t, ddbEU(), db, []string{"campaign_state", "events", "rollup_daily", "rollup_daily.members", "rollup_hourly", "rollup_hourly.members"})
	checkConverged(t, db, all, 2*cfg.converge)
	wantDaily := expectedDaily(outage)
	waitResult(t, e, cfg.converge, "daily rollup (created during the outage)", dailyQuery(db), nil, wantDaily)
	waitResult(t, u, cfg.converge, "daily rollup", dailyQuery(db), nil, wantDaily)
	stateQ := fmt.Sprintf(stateQuery, db)
	wantState := expectedState(states)
	waitResult(t, e, cfg.converge, "campaign_state after the outage", stateQ, map[string]string{"t": "tenant-01"}, wantState)
	waitCaughtUp(t, ddbEU(), db, "events")
	if got := e.mustQuery(t, rollupQuery(db), nil); got != expectedRollup(all) {
		t.Errorf("eu rollup after its worker caught up:\n%s", diffText(expectedRollup(all), got))
	}
}
