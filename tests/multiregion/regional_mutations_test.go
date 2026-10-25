//go:build multiregion

package multiregion

import (
	"errors"
	"testing"
	"time"
)

// Rollups are regional: every region keeps its own copy. A TRUNCATE, ALTER
// TABLE ... DELETE or DELETE FROM of a rollup issued in one region must not
// leave the regions with different rollups for good: either the statement
// is refused (NOT_IMPLEMENTED), or every region's copy reflects it.
func TestRegionalRollupMutations(t *testing.T) {
	requireStack(t)
	parallel(t)
	db := newDB(t, "rmut")
	u, e := us(), eu()
	u.mustExec(t, "CREATE TABLE "+db+".src (k String, n UInt64) ENGINE = MergeTree ORDER BY k")
	u.mustExec(t, "CREATE TABLE "+db+".roll (k String, n UInt64) ENGINE = SummingMergeTree ORDER BY k")
	u.mustExec(t, "CREATE MATERIALIZED VIEW "+db+".mv TO "+db+".roll AS SELECT k, n FROM "+db+".src")
	waitResult(t, e, cfg.converge, "view replicated", "EXISTS TABLE "+db+".mv", nil, "1\n")
	time.Sleep(3 * time.Second) // let eu's stream worker pick up the view
	u.mustExec(t, "INSERT INTO "+db+".src VALUES ('a', 10), ('b', 5)")
	roll := "SELECT k, sum(n) FROM " + db + ".roll GROUP BY k ORDER BY k"
	for _, r := range []*region{u, e} {
		waitResult(t, r, cfg.converge, "rollup", roll, nil, "a\t10\nb\t5\n")
	}
	if t.Failed() {
		t.FailNow()
	}

	refusedOr := func(sql, want string) {
		t.Helper()
		_, err := u.query(sql, nil)
		var ce *chError
		switch {
		case errors.As(err, &ce) && ce.Code == "48":
			t.Logf("%s refused: %v", sql, err)
			want = u.mustQuery(t, roll, nil) // nothing changed anywhere
		case err != nil:
			t.Fatalf("%s: %v", sql, err)
		}
		for _, r := range []*region{u, e} {
			waitResult(t, r, cfg.converge, sql, roll, nil, want)
		}
	}
	refusedOr("ALTER TABLE "+db+".roll DELETE WHERE k = 'a'", "b\t5\n")
	refusedOr("DELETE FROM "+db+".roll WHERE k = 'b'", "")
	refusedOr("TRUNCATE TABLE "+db+".roll", "")
	// Later rows add to the same state in both regions.
	before := u.mustQuery(t, roll, nil)
	u.mustExec(t, "INSERT INTO "+db+".src VALUES ('c', 1)")
	want := before + "c\t1\n" // keys sort before c
	for _, r := range []*region{u, e} {
		waitResult(t, r, cfg.converge, "rollup after a later insert", roll, nil, want)
	}
}
