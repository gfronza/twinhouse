//go:build multiregion

package multiregion

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/gfronza/twinhouse/internal/catalog"
)

// listPhysical lists the region's physical tables of database db.
func (d *ddb) listPhysical(ctx context.Context, db string) ([]string, error) {
	prefix := cfg.prefix + db + "."
	var out []string
	var start *string
	for {
		lt, err := d.db.ListTables(ctx, &dynamodb.ListTablesInput{ExclusiveStartTableName: start})
		if err != nil {
			return nil, err
		}
		for _, n := range lt.TableNames {
			if strings.HasPrefix(n, prefix) {
				// Strip the per-incarnation suffix: "t.<id>" -> "t",
				// "t.<id>.members" -> "t.members".
				out = append(out, catalog.StripIncarnation(strings.TrimPrefix(n, prefix)))
			}
		}
		start = lt.LastEvaluatedTableName
		if start == nil {
			slices.Sort(out)
			return out, nil
		}
	}
}

// waitPhysical waits until the region's physical tables of db are exactly want.
func waitPhysical(t testing.TB, d *ddb, db string, want []string) bool {
	t.Helper()
	slices.Sort(want)
	return eventually(t, cfg.converge, fmt.Sprintf("%s: physical tables of %s = %v", d.name, db, want), func() (bool, string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		got, err := d.listPhysical(ctx, db)
		if err != nil {
			return false, err.Error()
		}
		if slices.Equal(got, want) {
			return true, ""
		}
		return false, fmt.Sprintf("  got %v", got)
	})
}

// Scenario 1: DDL executed in us becomes usable in eu (catalog replication,
// eu reconciler creating the physical tables, eu mvworker picking up the
// view), and DROP VIEW / DROP TABLE / DROP DATABASE propagate.
func TestDDLReplication(t *testing.T) {
	requireStack(t)
	parallel(t)
	rng := newRNG(t)
	db := newDB(t, "ddl")
	u, e := us(), eu()

	// Create the schema and write immediately, without waiting for eu: the
	// eu side must still end up with every row and the exact rollup.
	createSchema(t, u, db)
	batch1 := genEvents(rng, genSpec{idPrefix: "b1", n: 300, tenants: 3, campaigns: 3, hours: 4, users: 40})
	if err := insertEvents(u, db, batch1, 100, nil); err != nil {
		t.Fatal(err)
	}
	waitSchema(t, e, db)

	tables := []string{"campaign_state", "events", "rollup_hourly", "rollup_hourly.members"}
	waitPhysical(t, ddbUS(), db, tables)
	waitPhysical(t, ddbEU(), db, tables)

	for _, obj := range []string{"events", "rollup_hourly", "mv_hourly", "campaign_state"} {
		q := "SHOW CREATE TABLE " + db + "." + obj
		want := u.mustQuery(t, q, nil)
		waitResult(t, e, cfg.converge, "SHOW CREATE "+obj+" equals us", q, nil, want)
	}
	checkConverged(t, db, batch1, cfg.converge)
	if t.Failed() {
		t.FailNow()
	}

	t.Run("DropView", func(t *testing.T) {
		u.mustExec(t, "DROP VIEW "+db+".mv_hourly")
		waitResult(t, e, cfg.converge, "view gone", "EXISTS TABLE "+db+".mv_hourly", nil, "0\n")
		// The stream workers refresh the catalog every CATALOG_REFRESH (1s):
		// give them a few rounds to forget the view before writing again.
		time.Sleep(3 * time.Second)
		batch2 := genEvents(rng, genSpec{idPrefix: "b2", n: 100, tenants: 3, campaigns: 3, hours: 4, users: 40})
		if err := insertEvents(u, db, batch2, 100, nil); err != nil {
			t.Fatal(err)
		}
		all := append(slices.Clone(batch1), batch2...)
		wantRoll := expectedRollup(batch1)
		if got := u.mustQuery(t, rollupQuery(db), nil); got != wantRoll {
			t.Errorf("us: rollup changed by an insert after DROP VIEW:\n%s", diffText(wantRoll, got))
		}
		waitResult(t, e, cfg.converge, "source rows after DROP VIEW", sourceQuery(db), nil, expectedSource(all))
		// Without views the workers stop reading the source stream (their
		// checkpoints no longer advance), so there is nothing to wait for:
		// give a stale worker a few seconds to (wrongly) apply the rows.
		time.Sleep(3 * time.Second)
		for _, r := range []*region{u, e} {
			if got := r.mustQuery(t, rollupQuery(db), nil); got != wantRoll {
				t.Errorf("%s: rollup changed by rows inserted after DROP VIEW:\n%s", r.name, diffText(wantRoll, got))
			}
		}
	})

	t.Run("DropTable", func(t *testing.T) {
		u.mustExec(t, "DROP TABLE "+db+".campaign_state")
		waitResult(t, e, cfg.converge, "table gone", "EXISTS TABLE "+db+".campaign_state", nil, "0\n")
		rest := []string{"events", "rollup_hourly", "rollup_hourly.members"}
		waitPhysical(t, ddbUS(), db, rest)
		waitPhysical(t, ddbEU(), db, rest)
		if _, err := e.query("SELECT count() FROM "+db+".campaign_state", nil); err == nil {
			t.Errorf("eu: dropped table is still queryable")
		}
	})

	t.Run("DropDatabase", func(t *testing.T) {
		u.mustExec(t, "DROP DATABASE "+db)
		waitResult(t, e, cfg.converge, "database gone", "EXISTS DATABASE "+db, nil, "0\n")
		waitResult(t, e, cfg.converge, "no tables in system.tables",
			"SELECT count() FROM system.tables WHERE database = {db:String}", map[string]string{"db": db}, "0\n")
		waitPhysical(t, ddbUS(), db, nil)
		waitPhysical(t, ddbEU(), db, nil)
		// Re-creating the same name in eu works and is clean (no old rows).
		e.mustExec(t, "CREATE DATABASE "+db)
		e.mustExec(t, fmt.Sprintf(ddlEvents, db))
		waitResult(t, u, cfg.converge, "recreated table empty", "SELECT count() FROM "+db+".events", nil, "0\n")
		waitResult(t, e, cfg.converge, "recreated table empty", "SELECT count() FROM "+db+".events", nil, "0\n")
	})
}

// A materialized view only processes rows inserted after it was created
// (ClickHouse semantics without POPULATE, verified against ClickHouse 25.8:
// the rollup only counts the post-CREATE rows). Both regions' stream workers
// must not apply a new view to source rows that were already in the stream.
func TestViewIgnoresRowsBeforeCreate(t *testing.T) {
	requireStack(t)
	parallel(t)
	rng := newRNG(t)
	db := newDB(t, "mvlate")
	u, e := us(), eu()
	u.mustExec(t, fmt.Sprintf(ddlEvents, db))
	waitResult(t, e, cfg.converge, "events usable", "SELECT count() FROM "+db+".events", nil, "0\n")

	before := genEvents(rng, genSpec{idPrefix: "pre", n: 120, tenants: 2, campaigns: 2, hours: 3, users: 20})
	if err := insertEvents(u, db, before, 60, nil); err != nil {
		t.Fatal(err)
	}
	waitResult(t, e, cfg.converge, "pre-view rows replicated", sourceQuery(db), nil, expectedSource(before))

	u.mustExec(t, fmt.Sprintf(ddlRollup, db))
	u.mustExec(t, fmt.Sprintf(ddlMV, db, db, db))
	waitResult(t, e, cfg.converge, "view visible", "EXISTS TABLE "+db+".mv_hourly", nil, "1\n")
	waitResult(t, e, cfg.converge, "rollup usable", "SELECT count() FROM "+db+".rollup_hourly", nil, "0\n")

	after := genEvents(rng, genSpec{idPrefix: "post", n: 120, tenants: 2, campaigns: 2, hours: 3, users: 20})
	if err := insertEvents(u, db, after, 60, nil); err != nil {
		t.Fatal(err)
	}
	all := append(slices.Clone(before), after...)
	waitResult(t, e, cfg.converge, "all rows replicated", sourceQuery(db), nil, expectedSource(all))
	waitCaughtUp(t, ddbUS(), db, "events")
	waitCaughtUp(t, ddbEU(), db, "events")
	want := expectedRollup(after)
	for _, r := range []*region{u, e} {
		// Give a lagging worker no excuse: poll briefly for the correct value,
		// then require it to be stable.
		waitResult(t, r, 10*time.Second, "rollup has only post-CREATE rows", rollupQuery(db), nil, want)
	}
}
