//go:build multiregion

package multiregion

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSoak runs random concurrent writers in both regions for
// MR_SOAK_DURATION (default 2m) while a checker verifies the in-flight
// invariants in each region:
//
//   - no duplicate source rows (count() = uniqExact(event_id)) and never
//     more rows than were written;
//   - the rollup never exceeds an aggregation of the region's own source
//     rows (read after the rollup) and never goes backwards;
//
// and finally that both regions converge to the exact union (source rows,
// rollup) and to the max-version ReplacingMergeTree state. Enabled with
// MR_SOAK=1.
func TestSoak(t *testing.T) {
	if os.Getenv("MR_SOAK") != "1" {
		t.Skip("set MR_SOAK=1 to run the soak / consistency checker")
	}
	requireStack(t)
	dur := 2 * time.Minute
	if d, err := time.ParseDuration(os.Getenv("MR_SOAK_DURATION")); err == nil && d > 0 {
		dur = d
	}
	db := setupSchema(t, "soak")
	seed := uint64(time.Now().UnixNano())
	t.Logf("random seed %d, duration %s", seed, dur)

	const writersPerRegion, maxBatch = 3, 80
	var log eventLog
	var inflight atomic.Int64
	var versions atomic.Uint64
	var stateMu sync.Mutex
	var states []stateRow
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var werrs []string
	fail := func(err error) {
		errMu.Lock()
		werrs = append(werrs, err.Error())
		errMu.Unlock()
	}
	stopped := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}
	n := 0
	for _, r := range []*region{us(), eu()} {
		for w := 0; w < writersPerRegion; w++ {
			n++
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				rng := newSeeded(seed + uint64(id))
				for i := 0; !stopped(); i++ {
					evs := genEvents(rng, genSpec{idPrefix: fmt.Sprintf("%s%d-%05d", r.name, id, i), n: 1 + rng.IntN(maxBatch),
						tenants: 4, campaigns: 4, hours: 6, users: 200})
					inflight.Add(int64(len(evs)))
					err := insertEvents(r, db, evs, len(evs), &log)
					inflight.Add(-int64(len(evs)))
					if err != nil {
						fail(err)
						return
					}
					time.Sleep(time.Duration(rng.IntN(300)) * time.Millisecond)
				}
			}(n)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := newSeeded(seed ^ uint64(len(r.name)))
			for !stopped() {
				s := stateVersion("tenant-00", fmt.Sprintf("cmp-%02d", rng.IntN(20)), versions.Add(1), r.name)
				if err := insertState(r, db, []stateRow{s}); err != nil {
					fail(err)
					return
				}
				stateMu.Lock()
				states = append(states, s)
				stateMu.Unlock()
				time.Sleep(time.Duration(50+rng.IntN(200)) * time.Millisecond)
			}
		}()
	}

	// Checker.
	var checks atomic.Int64
	checkerDone := make(chan struct{})
	go func() {
		defer close(checkerDone)
		prev := map[string]map[aggKey]rollupRow{}
		violations := 0
		report := func(format string, args ...any) {
			if violations < 20 {
				t.Errorf(format, args...)
			}
			violations++
		}
		for !stopped() {
			for _, r := range []*region{us(), eu()} {
				rollOut, err1 := r.query(rollupQuery(db), nil)
				srcOut, err2 := r.query(sourceAggQuery(db), nil)
				cnt, err3 := r.query("SELECT count(), uniqExact(event_id) FROM "+db+".events", nil)
				upper := int64(len(log.all())) + inflight.Load()
				if err := firstErr(err1, err2, err3); err != nil {
					report("%s: checker query failed: %v", r.name, err)
					continue
				}
				f := strings.Fields(cnt)
				if len(f) == 2 {
					c, _ := strconv.ParseInt(f[0], 10, 64)
					if f[0] != f[1] {
						report("%s: duplicate source rows: count %s, distinct event ids %s", r.name, f[0], f[1])
					}
					if c > upper {
						report("%s: %d source rows but at most %d were written", r.name, c, upper)
					}
				}
				roll, errA := parseRollup(rollOut)
				src, errB := parseRollup(srcOut)
				if err := firstErr(errA, errB); err != nil {
					report("%s: %v", r.name, err)
					continue
				}
				for k, v := range roll {
					s := src[k]
					if v.events > s.events || v.clicks > s.clicks || v.cost > s.cost || v.users > s.users || v.last > s.last {
						report("%s: rollup %v = %+v exceeds its source rows %+v", r.name, k, v, s)
					}
					if p, ok := prev[r.name][k]; ok && (v.events < p.events || v.users < p.users || v.cost < p.cost || v.last < p.last) {
						report("%s: rollup %v went backwards: %+v -> %+v", r.name, k, p, v)
					}
				}
				prev[r.name] = roll
				checks.Add(1)
			}
			time.Sleep(2 * time.Second)
		}
	}()

	time.Sleep(dur)
	close(stop)
	wg.Wait()
	<-checkerDone
	if len(werrs) > 0 {
		t.Fatalf("writers failed: %s", strings.Join(werrs, "; "))
	}
	all := log.all()
	t.Logf("soak wrote %d events and %d state updates; %d invariant checks", len(all), len(states), checks.Load())
	checkConverged(t, db, all, 3*cfg.converge)
	checkStateConverged(t, db, "tenant-00", states)
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
