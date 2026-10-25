//go:build e2e

package e2e

import (
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

const mvClicksDDL = "CREATE TABLE clicks (campaign_id String, ts DateTime, cost UInt64, user_id String) ENGINE = MergeTree ORDER BY (campaign_id, ts)"

const mvStatsDDL = `CREATE TABLE stats (campaign_id String, n SimpleAggregateFunction(sum, UInt64), total SimpleAggregateFunction(sum, UInt64),
	top SimpleAggregateFunction(max, UInt64), users AggregateFunction(uniqExact, String), avg_cost AggregateFunction(avg, UInt64),
	last_user AggregateFunction(argMax, String, DateTime)) ENGINE = AggregatingMergeTree ORDER BY campaign_id`

const mvStatsView = `CREATE MATERIALIZED VIEW stats_mv TO stats {populate} AS
	SELECT campaign_id, count() AS n, sum(cost) AS total, max(cost) AS top, uniqExactState(user_id) AS users,
	       avgState(cost) AS avg_cost, argMaxState(user_id, ts) AS last_user
	FROM clicks GROUP BY campaign_id`

const mvStatsRead = `SELECT campaign_id, sum(n), sum(total), max(top), uniqExactMerge(users), avgMerge(avg_cost), argMaxMerge(last_user)
	FROM stats GROUP BY campaign_id ORDER BY campaign_id`

const mvSeedRows = "INSERT INTO clicks VALUES ('a','2024-01-01 00:00:00',1,'x'),('a','2024-01-01 01:00:00',2,'y'),('b','2024-01-01 00:00:00',5,'x')"

// TestMVStatesAndInsertSelect: -State/-Merge of avg/argMax/uniqExact through
// VALUES, INSERT ... SELECT and partial-column inserts, immediately and after
// the stream worker; TRUNCATE of the source leaves the target intact.
func TestMVStatesAndInsertSelect(t *testing.T) {
	t.Parallel()
	db := newDB(t, "mvstates")
	mustExec(t, db, mvClicksDDL)
	mustExec(t, db, mvStatsDDL)
	mustExec(t, db, strings.ReplaceAll(mvStatsView, "{populate}", ""))
	mustExec(t, db, mvSeedRows)
	if got, want := tsv(t, db, mvStatsRead), "a\t2\t3\t2\t2\t1.5\ty\nb\t1\t5\t5\t1\t5\tx\n"; got != want {
		t.Errorf("after VALUES:\n%s\nwant\n%s", got, want)
	}
	mustExec(t, db, "INSERT INTO clicks SELECT 'c', toDateTime('2024-01-02 00:00:00') + number, number, toString(number % 3) FROM numbers(10)")
	mustExec(t, db, "INSERT INTO clicks (campaign_id, ts, cost) VALUES ('a', '2024-01-03 00:00:00', 10)")
	// argMax for 'a' is the empty user_id of the latest row.
	want := "a\t3\t13\t10\t3\t4.333333333333333\t\nb\t1\t5\t5\t1\t5\tx\nc\t10\t45\t9\t3\t4.5\t0\n"
	if got := tsv(t, db, mvStatsRead); got != want {
		t.Errorf("after INSERT SELECT:\n%s\nwant\n%s", got, want)
	}
	staysEqual(t, db, mvStatsRead, want, "after stream worker")
	mustExec(t, db, "TRUNCATE TABLE clicks")
	if got := tsv(t, db, "SELECT count() FROM clicks"); got != "0\n" {
		t.Errorf("source after TRUNCATE: %q", got)
	}
	staysEqual(t, db, mvStatsRead, want, "target must survive source TRUNCATE")
}

// TestMVIgnoresRowsBeforeCreation: like ClickHouse, a materialized view
// (without POPULATE) only sees rows inserted after it was created — also
// once the stream worker reads the source table's stream.
func TestMVIgnoresRowsBeforeCreation(t *testing.T) {
	t.Parallel()
	db := newDB(t, "mvbefore")
	mustExec(t, db, mvClicksDDL)
	mustExec(t, db, mvStatsDDL)
	mustExec(t, db, mvSeedRows)
	mustExec(t, db, strings.ReplaceAll(mvStatsView, "{populate}", ""))
	if got := tsv(t, db, mvStatsRead); got != "" {
		t.Errorf("right after CREATE MATERIALIZED VIEW: %q, want empty", got)
	}
	mustExec(t, db, "INSERT INTO clicks VALUES ('b','2024-01-02 00:00:00',7,'w')")
	want := "b\t1\t7\t7\t1\t7\tw\n"
	if got := tsv(t, db, mvStatsRead); got != want {
		t.Errorf("after insert:\n%s\nwant\n%s", got, want)
	}
	staysEqual(t, db, mvStatsRead, want, "after stream worker: rows inserted before the view was created were applied")
}

// TestMVPopulate: POPULATE (documented extension: allowed together with TO)
// backfills existing rows exactly once.
func TestMVPopulate(t *testing.T) {
	t.Parallel()
	db := newDB(t, "mvpopulate")
	mustExec(t, db, mvClicksDDL)
	mustExec(t, db, mvStatsDDL)
	mustExec(t, db, mvSeedRows)
	if r := post(t, db, strings.ReplaceAll(mvStatsView, "{populate}", "POPULATE")); r.Status != 200 {
		// A multi-region stack (GLOBAL_MODE emulated/aws) rejects POPULATE
		// into a regional target: it would back-fill only this region's
		// copy. Use the documented alternative: create the view, then
		// back-fill with INSERT ... SELECT (in every region; e2e reads us).
		if !strings.Contains(r.Body, "NOT_IMPLEMENTED") || !strings.Contains(r.Body, "multi-region") {
			t.Fatalf("POPULATE: HTTP %d: %s", r.Status, r.Body)
		}
		mustExec(t, db, strings.ReplaceAll(mvStatsView, "{populate}", ""))
		sel := mvStatsView[strings.Index(mvStatsView, "SELECT"):]
		mustExec(t, db, "INSERT INTO stats "+sel)
	}
	want := "a\t2\t3\t2\t2\t1.5\ty\nb\t1\t5\t5\t1\t5\tx\n"
	if got := tsv(t, db, mvStatsRead); got != want {
		t.Errorf("after POPULATE:\n%s\nwant\n%s", got, want)
	}
	mustExec(t, db, "INSERT INTO clicks VALUES ('b','2024-01-02 00:00:00',7,'w')")
	want = "a\t2\t3\t2\t2\t1.5\ty\nb\t2\t12\t7\t2\t6\tw\n"
	if got := tsv(t, db, mvStatsRead); got != want {
		t.Errorf("after insert:\n%s\nwant\n%s", got, want)
	}
	staysEqual(t, db, mvStatsRead, want, "after stream worker (backfilled rows counted twice?)")
}

// TestMVDropViewStopsUpdates: once DROP VIEW returns, inserts into the
// source no longer reach the target.
func TestMVDropViewStopsUpdates(t *testing.T) {
	t.Parallel()
	db := newDB(t, "mvdrop")
	mustExec(t, db, mvClicksDDL)
	mustExec(t, db, mvStatsDDL)
	mustExec(t, db, strings.ReplaceAll(mvStatsView, "{populate}", ""))
	mustExec(t, db, mvSeedRows)
	want := "a\t2\t3\t2\t2\t1.5\ty\nb\t1\t5\t5\t1\t5\tx\n"
	staysEqual(t, db, mvStatsRead, want, "before DROP VIEW (worker catching up with the seed rows)")
	if t.Failed() {
		return
	}
	mustExec(t, db, "DROP VIEW stats_mv")
	mustExec(t, db, "INSERT INTO clicks VALUES ('a','2024-01-05 00:00:00',100,'z')")
	if got := tsv(t, db, mvStatsRead); got != want {
		t.Errorf("target changed right after DROP VIEW + insert:\n%s", got)
	}
	staysEqual(t, db, mvStatsRead, want, "stream worker applied a dropped view")
}

// TestConcurrentRollupWriters: many writers hitting the same rollup keys at
// once; sums and distinct counts must be exact, now and after the worker.
func TestConcurrentRollupWriters(t *testing.T) {
	t.Parallel()
	heavy(t)
	db := newDB(t, "concurrent")
	createSchema(t, db)
	const writers, perWriter = 6, 30
	var wg sync.WaitGroup
	var mu sync.Mutex
	var all []adEvent
	errs := make(chan string, writers)
	for w := 0; w < writers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			evs := genEvents(int64(100+w), perWriter, fmt.Sprintf("w%d", w))
			for i := range evs {
				// Concentrate on few keys to maximise contention.
				evs[i].Tenant = "acme"
				evs[i].Campaign = fmt.Sprintf("acme-cmp-%d", i%2)
				evs[i].Time = eventBase.Add(time.Duration(i%3) * time.Minute)
			}
			r, err := doHTTPErr(req{
				Params: url.Values{"query": {"INSERT INTO " + db + ".ad_events FORMAT JSONEachRow"}},
				Body:   strings.NewReader(string(jsonEachRow(evs))),
			})
			if err != nil {
				errs <- fmt.Sprintf("writer %d: %v", w, err)
				return
			}
			if r.Status != 200 {
				errs <- fmt.Sprintf("writer %d: %d %s", w, r.Status, r.Body)
				return
			}
			mu.Lock()
			all = append(all, evs...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	conn := openNative(t, db)
	checkHourly(t, conn, all, "concurrent writers")
	checkDaily(t, conn, all, "concurrent writers")
	time.Sleep(workerSettle)
	checkHourly(t, conn, all, "concurrent writers after worker")
	checkDaily(t, conn, all, "concurrent writers after worker")
}

// TestAlterAndDelete: catalog-only ALTER and lightweight deletes.
func TestAlterAndDelete(t *testing.T) {
	t.Parallel()
	db := newDB(t, "alter")
	mustExec(t, db, "CREATE TABLE tenants (tenant_id String, plan String, seats UInt64) ENGINE = MergeTree ORDER BY tenant_id")
	mustExec(t, db, "INSERT INTO tenants VALUES ('acme','pro',10),('globex','free',1),('initech','pro',25)")

	t.Run("add_column_default", func(t *testing.T) {
		mustExec(t, db, "ALTER TABLE tenants ADD COLUMN region String DEFAULT 'us-east-1', ADD COLUMN seat_cap UInt64 DEFAULT seats * 2")
		mustExec(t, db, "INSERT INTO tenants (tenant_id, plan, seats) VALUES ('hooli','enterprise',100)")
		// ClickHouse computes the DEFAULT for rows written before the ALTER
		// when they are read.
		got := tsv(t, db, "SELECT tenant_id, region, seat_cap FROM tenants ORDER BY tenant_id")
		want := "acme\tus-east-1\t20\nglobex\tus-east-1\t2\nhooli\tus-east-1\t200\ninitech\tus-east-1\t50\n"
		if got != want {
			t.Errorf("columns added by ALTER:\n%s\nwant\n%s", got, want)
		}
	})
	t.Run("drop_modify_column", func(t *testing.T) {
		mustExec(t, db, "ALTER TABLE tenants DROP COLUMN seat_cap")
		mustExec(t, db, "ALTER TABLE tenants MODIFY COLUMN seats UInt32")
		got := tsv(t, db, "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'tenants' ORDER BY position")
		if got != "tenant_id\tString\nplan\tString\nseats\tUInt32\nregion\tString\n" {
			t.Errorf("columns after DROP/MODIFY:\n%s", got)
		}
		if got := tsv(t, db, "SELECT sum(seats), toTypeName(seats) FROM tenants GROUP BY 2"); got != "136\tUInt32\n" {
			t.Errorf("after MODIFY: %q", got)
		}
	})
	t.Run("delete", func(t *testing.T) {
		mustExec(t, db, "ALTER TABLE tenants DELETE WHERE plan = 'free'")
		eventually(t, 10*time.Second, func() error {
			if got := tsv(t, db, "SELECT tenant_id FROM tenants ORDER BY tenant_id"); got != "acme\nhooli\ninitech\n" {
				return fmt.Errorf("after ALTER DELETE: %q", got)
			}
			return nil
		})
		mustExec(t, db, "DELETE FROM tenants WHERE seats > 50")
		if got := tsv(t, db, "SELECT tenant_id FROM tenants ORDER BY tenant_id"); got != "acme\ninitech\n" {
			t.Errorf("after DELETE FROM: %q", got)
		}
	})
}

// TestMVNonAggregatingTargets: views into MergeTree (filtering) and
// ReplacingMergeTree (latest state) targets; identical source rows are two
// rows; the stream worker must not duplicate MV output.
func TestMVNonAggregatingTargets(t *testing.T) {
	t.Parallel()
	db := newDB(t, "mvplain")
	mustExec(t, db, "CREATE TABLE ev (k String, t DateTime, typ LowCardinality(String), v UInt64) ENGINE = MergeTree ORDER BY (k, t)")
	mustExec(t, db, "CREATE TABLE clicks (k String, t DateTime, v UInt64) ENGINE = MergeTree ORDER BY (k, t)")
	mustExec(t, db, "CREATE TABLE latest (k String, t DateTime, v UInt64) ENGINE = ReplacingMergeTree(t) ORDER BY k")
	mustExec(t, db, "CREATE MATERIALIZED VIEW mv_clicks TO clicks AS SELECT k, t, v FROM ev WHERE typ = 'click'")
	mustExec(t, db, "CREATE MATERIALIZED VIEW mv_latest TO latest AS SELECT k, t, v * 10 AS v FROM ev")
	mustExec(t, db, "INSERT INTO ev VALUES ('a','2024-01-01 00:00:00','click',1),('a','2024-01-01 00:00:00','click',1),('a','2024-01-01 00:00:05','imp',2),('b','2024-01-01 00:00:00','click',3)")
	mustExec(t, db, "INSERT INTO ev VALUES ('b','2023-12-31 00:00:00','imp',4)")
	check := func(label string) {
		if got := tsv(t, db, "SELECT k, count(), sum(v) FROM clicks GROUP BY k ORDER BY k"); got != "a\t2\t2\nb\t1\t3\n" {
			t.Errorf("%s: clicks = %q", label, got)
		}
		if got := tsv(t, db, "SELECT k, t, v FROM latest FINAL ORDER BY k"); got != "a\t2024-01-01 00:00:05\t20\nb\t2024-01-01 00:00:00\t30\n" {
			t.Errorf("%s: latest = %q", label, got)
		}
		if got := tsv(t, db, "SELECT count() FROM ev"); got != "5\n" {
			t.Errorf("%s: source count = %q", label, got)
		}
	}
	check("immediately")
	time.Sleep(workerSettle)
	check("after stream worker")
}

// TestInsertValuesDateTime64Numbers: loaders often send epoch values in
// INSERT ... VALUES. ClickHouse parses VALUES literals with the column
// type's text parser: an unquoted integer for DateTime64(3) is raw ticks
// (milliseconds), a quoted 13-digit number is also milliseconds, and the
// same holds for CAST from a string (verified against ClickHouse 25.8).
func TestInsertValuesDateTime64Numbers(t *testing.T) {
	t.Parallel()
	db := newDB(t, "dt64num")
	mustExec(t, db, "CREATE TABLE ev (k String, ts64 DateTime64(3)) ENGINE = MergeTree ORDER BY k")
	mustExec(t, db, "INSERT INTO ev VALUES ('a', 1714989600250), ('b', '1714989600250'), ('c', 1714989600)")
	mustExec(t, db, "INSERT INTO ev FORMAT TSV\ne\t1714989600250\nf\t1714989600\n")
	want := "a\t2024-05-06 10:00:00.250\nb\t2024-05-06 10:00:00.250\nc\t1970-01-20 20:23:09.600\ne\t2024-05-06 10:00:00.250\nf\t2024-05-06 10:00:00.000\n"
	if got := tsv(t, db, "SELECT k, ts64 FROM ev ORDER BY k"); got != want {
		t.Errorf("DateTime64 from numbers:\n%s\nwant\n%s", got, want)
	}
	r := post(t, db, "SELECT CAST('1714989600250' AS DateTime64(3, 'UTC')), toDateTime64('1714989600', 3, 'UTC'), CAST('1714989600' AS DateTime('UTC'))")
	if r.Status != 200 || r.Body != "2024-05-06 10:00:00.250\t2024-05-06 10:00:00.000\t2024-05-06 10:00:00\n" {
		t.Errorf("CAST string to DateTime64: %d %q", r.Status, r.Body)
	}
}
