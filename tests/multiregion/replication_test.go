//go:build multiregion

package multiregion

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// Scenario 2: write in one region, read in the other. The writer region has
// read-your-writes immediately (source rows and inline-applied rollup); the
// other region converges to identical source rows and an identical rollup.
// Then the same in the opposite direction.
func TestWriteOneRegionReadOther(t *testing.T) {
	requireStack(t)
	parallel(t)
	rng := newRNG(t)
	db := setupSchema(t, "rw")
	u, e := us(), eu()

	var all []event
	for _, step := range []struct {
		writer *region
		prefix string
	}{{u, "us"}, {e, "eu"}} {
		batch := genEvents(rng, genSpec{idPrefix: step.prefix, n: 500, tenants: 3, campaigns: 4, hours: 6, users: 50})
		if err := insertEvents(step.writer, db, batch, 125, nil); err != nil {
			t.Fatal(err)
		}
		all = append(all, batch...)
		// Read-your-writes in the writer region, no polling (rows of the
		// previous step are already converged in both regions).
		w := step.writer
		wantSrc, wantRoll := expectedSource(all), expectedRollup(all)
		if got := w.mustQuery(t, "SELECT count() FROM "+db+".events", nil); got != fmt.Sprintf("%d\n", len(all)) {
			t.Errorf("%s read-your-writes: count = %q, want %d", w.name, got, len(all))
		}
		if got := w.mustQuery(t, sourceQuery(db), nil); got != wantSrc {
			t.Errorf("%s read-your-writes: source rows differ:\n%s", w.name, diffText(wantSrc, got))
		}
		if got := w.mustQuery(t, rollupQuery(db), nil); got != wantRoll {
			t.Errorf("%s read-your-writes: rollup differs:\n%s", w.name, diffText(wantRoll, got))
		}
		checkConverged(t, db, all, cfg.converge)
	}

	// Every client-facing format returns identical bytes in both regions.
	for _, f := range []string{"TabSeparated", "TSVWithNames", "CSV", "JSONEachRow", "CSVWithNames", "Values"} {
		q := strings.TrimSuffix(rollupQuery(db), " FORMAT TabSeparated") + " FORMAT " + f
		want := u.mustQuery(t, q, nil)
		if got := e.mustQuery(t, q, nil); got != want {
			t.Errorf("FORMAT %s differs between regions:\n%s", f, diffText(want, got))
		}
	}
	// JSON carries timing statistics; compare its data section.
	q := strings.TrimSuffix(rollupQuery(db), " FORMAT TabSeparated") + " FORMAT JSON"
	if a, b := jsonData(t, u.mustQuery(t, q, nil)), jsonData(t, e.mustQuery(t, q, nil)); a != b {
		t.Errorf("FORMAT JSON data differs between regions:\nus: %.300s\neu: %.300s", a, b)
	}
}

// jsonData returns the "meta"+"data"+"rows" part of FORMAT JSON output.
func jsonData(t testing.TB, out string) string {
	t.Helper()
	i := strings.Index(out, `"statistics"`)
	if i < 0 {
		i = len(out)
	}
	return out[:i]
}

// Scenario 3: concurrent active-active writers in both regions on the same
// tenants / campaigns / hours / users. After convergence each region holds
// the union of all rows and each rollup equals the exact aggregation over the
// union: no lost counter updates, no double counting, exact uniq sets.
func TestConcurrentActiveActive(t *testing.T) {
	requireStack(t)
	parallel(t)
	rng := newRNG(t)
	db := setupSchema(t, "aa")
	const writersPerRegion, batches, rows = 4, 6, 40
	spec := genSpec{tenants: 3, campaigns: 3, hours: 4, users: 60, n: batches * rows}

	var log eventLog
	var fns []func() error
	for _, r := range []*region{us(), eu()} {
		for w := 0; w < writersPerRegion; w++ {
			s := spec
			s.idPrefix = fmt.Sprintf("%s-w%d", r.name, w)
			evs := genEvents(rng, s)
			fns = append(fns, func() error { return insertEvents(r, db, evs, rows, &log) })
		}
	}
	start := time.Now()
	if err := runAll(fns...); err != nil {
		t.Fatal(err)
	}
	all := log.all()
	t.Logf("%d rows written concurrently in both regions in %s", len(all), time.Since(start).Round(time.Millisecond))
	if len(all) != 2*writersPerRegion*batches*rows {
		t.Fatalf("logged %d rows", len(all))
	}
	checkConverged(t, db, all, 2*cfg.converge)
}

// Scenario 5: the writer region applies the view inline during INSERT; its
// stream worker later sees the same records and must not apply them again.
func TestInlineAndStreamDedup(t *testing.T) {
	requireStack(t)
	parallel(t)
	rng := newRNG(t)
	db := setupSchema(t, "dedup")

	var all []event
	for _, step := range []struct {
		writer *region
		store  *ddb
	}{{us(), ddbUS()}, {eu(), ddbEU()}} {
		r := step.writer
		batch := genEvents(rng, genSpec{idPrefix: r.name, n: 300, tenants: 2, campaigns: 3, hours: 3, users: 30})
		if err := insertEvents(r, db, batch, 100, nil); err != nil {
			t.Fatal(err)
		}
		all = append(all, batch...)
		want := expectedRollup(all)
		// Inline application: exact immediately after INSERT returned (the
		// rows of the previous step are already converged).
		if got := r.mustQuery(t, rollupQuery(db), nil); got != want {
			t.Errorf("%s: rollup right after INSERT (inline views):\n%s", r.name, diffText(want, got))
		}
		// The region's mvworker processes the very same records ...
		waitCaughtUp(t, step.store, db, "events")
		// ... and the rollup is unchanged (exactly-once markers).
		if got := r.mustQuery(t, rollupQuery(db), nil); got != want {
			t.Errorf("%s: rollup after the stream worker re-processed the inline rows:\n%s", r.name, diffText(want, got))
		}
		checkConverged(t, db, all, cfg.converge)
		waitCaughtUp(t, ddbUS(), db, "events")
		waitCaughtUp(t, ddbEU(), db, "events")
		for _, rr := range []*region{us(), eu()} {
			if got := rr.mustQuery(t, rollupQuery(db), nil); got != want {
				t.Errorf("%s: rollup after both workers caught up:\n%s", rr.name, diffText(want, got))
			}
		}
	}
}

// ---------------------------------------------------------------------------
// ReplacingMergeTree latest-wins
// ---------------------------------------------------------------------------

type stateRow struct {
	tenant, campaign, status string
	budget                   uint64
	by                       string
	version                  uint64
}

func (s stateRow) json() map[string]any {
	return map[string]any{"tenant_id": s.tenant, "campaign_id": s.campaign, "status": s.status,
		"budget": s.budget, "updated_by": s.by, "version": s.version}
}

func (s stateRow) tsv() string {
	return fmt.Sprintf("%s\t%s\t%s\t%d\t%s\t%d", s.tenant, s.campaign, s.status, s.budget, s.by, s.version)
}

func stateVersion(tenant, campaign string, v uint64, by string) stateRow {
	statuses := []string{"draft", "active", "paused", "active", "archived"}
	return stateRow{tenant: tenant, campaign: campaign, status: statuses[v%uint64(len(statuses))], budget: 100 * v, by: by, version: v}
}

func insertState(r *region, db string, rows []stateRow) error {
	js := make([]map[string]any, len(rows))
	for i, s := range rows {
		js[i] = s.json()
	}
	return r.insertJSON(db+".campaign_state", js, false)
}

const stateQuery = "SELECT tenant_id, campaign_id, status, budget, updated_by, version FROM %s.campaign_state FINAL WHERE tenant_id = {t:String} ORDER BY campaign_id"

func expectedState(rows []stateRow) string {
	best := map[string]stateRow{}
	for _, s := range rows {
		k := s.tenant + "\x00" + s.campaign
		if cur, ok := best[k]; !ok || s.version > cur.version {
			best[k] = s
		}
	}
	keys := make([]string, 0, len(best))
	for k := range best {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(best[k].tsv() + "\n")
	}
	return b.String()
}

// checkStateConverged first requires both regions to agree (a property any
// replication scheme must have), then requires the agreed state to be the
// max-version row per key (ReplacingMergeTree(version) semantics).
func checkStateConverged(t *testing.T, db, tenant string, written []stateRow) {
	t.Helper()
	q := fmt.Sprintf(stateQuery, db)
	p := map[string]string{"t": tenant}
	var usOut, euOut string
	t.Run("RegionsAgree", func(t *testing.T) {
		eventually(t, cfg.converge, "us and eu FINAL state equal", func() (bool, string) {
			var err error
			if usOut, err = us().query(q, p); err != nil {
				return false, err.Error()
			}
			if euOut, err = eu().query(q, p); err != nil {
				return false, err.Error()
			}
			// Equal and stable across one more read in us.
			if again, _ := us().query(q, p); usOut == euOut && again == usOut && usOut != "" {
				return true, ""
			}
			return false, "us vs eu:\n" + diffText(usOut, euOut)
		})
	})
	want := expectedState(written)
	t.Run("MaxVersionWins", func(t *testing.T) {
		waitResult(t, us(), cfg.converge, "FINAL state = max version", q, p, want)
		waitResult(t, eu(), cfg.converge, "FINAL state = max version", q, p, want)
		// Point lookups with a server-side parameter on the key.
		for _, line := range splitLines(want)[:min(3, len(splitLines(want)))] {
			f := strings.Split(line, "\t")
			pq := fmt.Sprintf("SELECT tenant_id, campaign_id, status, budget, updated_by, version FROM %s.campaign_state FINAL WHERE tenant_id = {t:String} AND campaign_id = {c:String}", db)
			pp := map[string]string{"t": f[0], "c": f[1]}
			for _, r := range []*region{us(), eu()} {
				if got := r.mustQuery(t, pq, pp); got != line+"\n" {
					t.Errorf("%s: point lookup %s/%s = %q, want %q", r.name, f[0], f[1], got, line)
				}
			}
		}
	})
}

// Scenario 6: conflicting ReplacingMergeTree(version) updates of the same
// keys written concurrently in both regions converge, in both regions, to
// the max-version row (read with FINAL).
func TestReplacingLatestWinsConcurrent(t *testing.T) {
	requireStack(t)
	parallel(t)
	db := setupSchema(t, "repl")
	const tenant, keys, versions = "tenant-00", 12, 6

	// Version v of key i is written by us when (v+i) is even, else by eu,
	// so the max version comes from us for some keys and from eu for others.
	// Both regions write version by version without waiting for each other.
	var written []stateRow
	perRegion := map[string][][]stateRow{}
	for v := uint64(1); v <= versions; v++ {
		batch := map[string][]stateRow{}
		for i := 0; i < keys; i++ {
			by := "us"
			if (int(v)+i)%2 == 1 {
				by = "eu"
			}
			s := stateVersion(tenant, fmt.Sprintf("cmp-%02d", i), v, by)
			batch[by] = append(batch[by], s)
			written = append(written, s)
		}
		for r, rows := range batch {
			perRegion[r] = append(perRegion[r], rows)
		}
	}
	if err := runAll(
		func() error {
			for _, rows := range perRegion["us"] {
				if err := insertState(us(), db, rows); err != nil {
					return err
				}
			}
			return nil
		},
		func() error {
			for _, rows := range perRegion["eu"] {
				if err := insertState(eu(), db, rows); err != nil {
					return err
				}
			}
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	checkStateConverged(t, db, tenant, written)

	// Strictly sequential updates (each written after the previous one has
	// replicated) on a separate tenant: the higher version wins everywhere
	// and a stale version written later is ignored everywhere.
	t.Run("SequentialUpdate", func(t *testing.T) {
		const tn = "tenant-01"
		vq := fmt.Sprintf("SELECT version FROM %s.campaign_state FINAL WHERE tenant_id = {t:String} AND campaign_id = 'cmp-00'", db)
		p := map[string]string{"t": tn}
		var seq []stateRow
		for i, w := range []struct {
			r       *region
			version uint64
		}{{us(), 1}, {eu(), 2}, {us(), 3}, {eu(), 1}, {us(), 2}} {
			s := stateVersion(tn, "cmp-00", w.version, w.r.name)
			if err := insertState(w.r, db, []stateRow{s}); err != nil {
				t.Fatal(err)
			}
			seq = append(seq, s)
			want := fmt.Sprintf("%d\n", maxVersion(seq))
			// Both regions show the max version before the next write.
			waitResult(t, us(), cfg.converge, fmt.Sprintf("step %d", i), vq, p, want)
			waitResult(t, eu(), cfg.converge, fmt.Sprintf("step %d", i), vq, p, want)
		}
		checkStateConverged(t, db, tn, seq)
	})
}

func maxVersion(rows []stateRow) uint64 {
	var m uint64
	for _, r := range rows {
		m = max(m, r.version)
	}
	return m
}
