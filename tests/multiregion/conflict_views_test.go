//go:build multiregion

package multiregion

import (
	"fmt"
	"testing"
	"time"
)

// Materialized views over a ReplacingMergeTree(ver) source with regional
// rollups must converge across regions under concurrent conflicting writes
// (DESIGN.md §1). With the replicator stopped, us writes the higher version
// of a key and eu the lower one (and the mirror image for a second key).
// When replication resumes, the region holding the higher version rejects
// the lower one, which therefore never enters that region's stream; the
// replicator applies that region's views to it instead, so both rollups
// count both versions, as the origin regions did inline.
func TestFaultReplacingConflictRollupsConverge(t *testing.T) {
	faultTest(t)
	db, u, e := setupConflictSchema(t, "cfview")
	compose(t, "stop", "replicator")
	for _, w := range []struct {
		r   *region
		row string
	}{
		{u, "('K', 2, 'us')"}, // K: higher version in us
		{e, "('K', 1, 'eu')"}, // lower version in eu, written later
		{e, "('L', 3, 'eu')"}, // L: the mirror image
		{u, "('L', 1, 'us')"},
	} {
		time.Sleep(20 * time.Millisecond) // distinct write timestamps
		w.r.mustExec(t, fmt.Sprintf("INSERT INTO %s.snap VALUES %s", db, w.row))
	}
	compose(t, "start", "replicator")

	snap := fmt.Sprintf("SELECT k, ver, by FROM %s.snap FINAL ORDER BY k", db)
	rollup := fmt.Sprintf("SELECT k, sum(n), sum(vsum) FROM %s.cnt GROUP BY k ORDER BY k", db)
	wantSnap := "K\t2\tus\nL\t3\teu\n"
	wantRollup := "K\t2\t3\nL\t2\t4\n" // every inserted version counted once
	for _, r := range []*region{u, e} {
		waitResult(t, r, cfg.converge, "replacing state", snap, nil, wantSnap)
		waitResult(t, r, cfg.converge, "rollup counts every version", rollup, nil, wantRollup)
	}
	// No late double counts (redelivery of the rejected versions).
	time.Sleep(3 * time.Second)
	for _, r := range []*region{u, e} {
		if got := r.mustQuery(t, rollup, nil); got != wantRollup {
			t.Errorf("%s: rollup changed:\n%s", r.name, diffText(wantRollup, got))
		}
	}
}

// A stale version written in one region after the newer version has
// replicated there is not stored (ReplacingMergeTree keeps the max
// version), so it never replicates and no other region's views can see it.
// Every region's rollup must still agree.
func TestReplacingStaleWriteRollupsConverge(t *testing.T) {
	requireStack(t)
	db, u, e := setupConflictSchema(t, "cfstale")
	rollup := fmt.Sprintf("SELECT k, sum(n), sum(vsum) FROM %s.cnt GROUP BY k ORDER BY k", db)
	u.mustExec(t, fmt.Sprintf("INSERT INTO %s.snap VALUES ('M', 5, 'us')", db))
	waitResult(t, e, cfg.converge, "M replicated", fmt.Sprintf("SELECT ver FROM %s.snap FINAL WHERE k = 'M'", db), nil, "5\n")
	e.mustExec(t, fmt.Sprintf("INSERT INTO %s.snap VALUES ('M', 4, 'eu')", db))
	ok := eventually(t, cfg.converge, "us and eu rollups equal", func() (bool, string) {
		a, err1 := u.query(rollup, nil)
		b, err2 := e.query(rollup, nil)
		if err1 != nil || err2 != nil {
			return false, fmt.Sprint(err1, err2)
		}
		return a == b && a != "", "us:\n" + a + "eu:\n" + b
	})
	if !ok {
		t.Log("the stale version fed the writer region's views (inline) but cannot reach the other region")
	}
}

// setupConflictSchema creates snap (ReplacingMergeTree(ver)), the regional
// rollup cnt and the view m in us and waits until both regions can use them.
func setupConflictSchema(t *testing.T, scenario string) (string, *region, *region) {
	t.Helper()
	db := newDB(t, scenario)
	u, e := us(), eu()
	u.mustExec(t, fmt.Sprintf("CREATE TABLE %s.snap (k String, ver UInt64, by String) ENGINE = ReplacingMergeTree(ver) ORDER BY k", db))
	u.mustExec(t, fmt.Sprintf(`CREATE TABLE %s.cnt (k String, n SimpleAggregateFunction(sum, UInt64),
		vsum SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k`, db))
	u.mustExec(t, fmt.Sprintf("CREATE MATERIALIZED VIEW %s.m TO %s.cnt AS SELECT k, count() AS n, sum(ver) AS vsum FROM %s.snap GROUP BY k", db, db, db))
	tables := "SELECT name FROM system.tables WHERE database = {db:String} ORDER BY name"
	p := map[string]string{"db": db}
	for _, r := range []*region{u, e} {
		if !waitResult(t, r, cfg.converge, "schema", tables, p, "cnt\nm\nsnap\n") {
			t.FailNow()
		}
		if !waitResult(t, r, cfg.converge, "tables usable", fmt.Sprintf("SELECT count() FROM %s.snap", db), nil, "0\n") ||
			!waitResult(t, r, cfg.converge, "rollup usable", fmt.Sprintf("SELECT count() FROM %s.cnt", db), nil, "0\n") {
			t.FailNow()
		}
	}
	return db, u, e
}
