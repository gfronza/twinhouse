//go:build multiregion

package multiregion

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Schemas of the re-creation scenarios: a MergeTree source and a
// SummingMergeTree rollup fed by a materialized view.
const (
	incSrcDDL  = `CREATE %s TABLE %s.src (k String, id UInt64, v UInt64) ENGINE = MergeTree ORDER BY (k, id)`
	incRollDDL = `CREATE %s TABLE %s.roll (k String, n UInt64, s UInt64) ENGINE = SummingMergeTree ORDER BY k`
	incMVDDL   = `CREATE MATERIALIZED VIEW %s.mv TO %s.roll AS SELECT k, count() AS n, sum(v) AS s FROM %s.src GROUP BY k`
	incSrcQ    = `SELECT k, count(), sum(v) FROM %s.src GROUP BY k ORDER BY k`
	incRollQ   = `SELECT k, sum(n), sum(s) FROM %s.roll GROUP BY k ORDER BY k`
)

// incInsert inserts n rows with keys prefixed by tag (v = weight each).
func incInsert(t *testing.T, r *region, db, tag string, from, n int, weight int) {
	t.Helper()
	var vals []string
	for i := from; i < from+n; i++ {
		vals = append(vals, fmt.Sprintf("('%s-%d', %d, %d)", tag, i%3, i, weight))
	}
	r.mustExec(t, "INSERT INTO "+db+".src VALUES "+strings.Join(vals, ","))
}

// incWant is the per-key (count, sum) of incInsert(tag, from, n, weight).
func incWant(tag string, from, n, weight int) string {
	cnt := map[int]int{}
	for i := from; i < from+n; i++ {
		cnt[i%3]++
	}
	var b strings.Builder
	for k := 0; k < 3; k++ {
		if cnt[k] > 0 {
			fmt.Fprintf(&b, "%s-%d\t%d\t%d\n", tag, k, cnt[k], cnt[k]*weight)
		}
	}
	return b.String()
}

// checkOnlyNew waits until both regions hold exactly want in the source and
// the rollup, then checks that it stays so (rows of the old incarnation
// still in flight must never show up in the new one).
func checkOnlyNew(t *testing.T, db, want string) {
	t.Helper()
	for _, r := range []*region{us(), eu()} {
		waitResult(t, r, cfg.converge, "source has only the new incarnation's rows", fmt.Sprintf(incSrcQ, db), nil, want)
		waitResult(t, r, cfg.converge, "rollup has only the new incarnation's rows", fmt.Sprintf(incRollQ, db), nil, want)
	}
	time.Sleep(4 * time.Second)
	for _, r := range []*region{us(), eu()} {
		for _, q := range []string{incSrcQ, incRollQ} {
			if got := r.mustQuery(t, fmt.Sprintf(q, db), nil); got != want {
				t.Errorf("%s: %s changed after converging:\n%s", r.name, fmt.Sprintf(q, db), diffText(want, got))
			}
		}
	}
}

// TestRecreateTableIncarnations: DROP TABLE + CREATE TABLE back to back, and
// CREATE OR REPLACE TABLE, on a MergeTree source and a SummingMergeTree
// rollup with its view, with no wait for the other region in between. Each
// incarnation has its own physical table, so the other region ends up with
// only the new incarnation's rows: rows of the old one (already replicated,
// or still in flight in the replicator and the stream workers) stay in the
// old physical table, which the reconcilers drop.
func TestRecreateTableIncarnations(t *testing.T) {
	requireStack(t)
	parallel(t)
	u, e := us(), eu()

	t.Run("DropCreate", func(t *testing.T) {
		db := newDB(t, "reinc")
		u.mustExec(t, fmt.Sprintf(incSrcDDL, "", db))
		u.mustExec(t, fmt.Sprintf(incRollDDL, "", db))
		u.mustExec(t, fmt.Sprintf(incMVDDL, db, db, db))
		incInsert(t, u, db, "old", 0, 60, 1)
		// eu holds the old incarnation's rows (source and rollup) ...
		waitResult(t, e, cfg.converge, "old rows replicated", fmt.Sprintf(incSrcQ, db), nil, incWant("old", 0, 60, 1))
		waitResult(t, e, cfg.converge, "old rollup built", fmt.Sprintf(incRollQ, db), nil, incWant("old", 0, 60, 1))
		// ... and more are in flight when the tables are re-created.
		incInsert(t, u, db, "old", 60, 60, 1)
		u.mustExec(t, "DROP VIEW "+db+".mv")
		u.mustExec(t, "DROP TABLE "+db+".src")
		u.mustExec(t, "DROP TABLE "+db+".roll")
		u.mustExec(t, fmt.Sprintf(incSrcDDL, "", db))
		u.mustExec(t, fmt.Sprintf(incRollDDL, "", db))
		u.mustExec(t, fmt.Sprintf(incMVDDL, db, db, db))
		incInsert(t, u, db, "new", 0, 30, 7)
		checkOnlyNew(t, db, incWant("new", 0, 30, 7))
		// The other region writes into the new incarnation too.
		waitResult(t, e, cfg.converge, "view visible", "EXISTS TABLE "+db+".mv", nil, "1\n")
		incInsert(t, e, db, "new", 30, 30, 7)
		checkOnlyNew(t, db, incWant("new", 0, 60, 7))
	})

	t.Run("CreateOrReplace", func(t *testing.T) {
		db := newDB(t, "reinc")
		u.mustExec(t, fmt.Sprintf(incSrcDDL, "", db))
		u.mustExec(t, fmt.Sprintf(incRollDDL, "", db))
		u.mustExec(t, fmt.Sprintf(incMVDDL, db, db, db))
		incInsert(t, u, db, "old", 0, 60, 1)
		waitResult(t, e, cfg.converge, "old rows replicated", fmt.Sprintf(incSrcQ, db), nil, incWant("old", 0, 60, 1))
		waitResult(t, e, cfg.converge, "old rollup built", fmt.Sprintf(incRollQ, db), nil, incWant("old", 0, 60, 1))
		incInsert(t, u, db, "old", 60, 60, 1)
		// The view keeps targeting roll and reading src by name.
		u.mustExec(t, fmt.Sprintf(incSrcDDL, "OR REPLACE", db))
		u.mustExec(t, fmt.Sprintf(incRollDDL, "OR REPLACE", db))
		incInsert(t, u, db, "new", 0, 30, 7)
		checkOnlyNew(t, db, incWant("new", 0, 30, 7))
	})
}
