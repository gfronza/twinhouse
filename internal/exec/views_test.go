package exec

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
	"dynamodb-eventstream/internal/storage"
)

// rollupDDL is a MergeTree event source feeding three AggregatingMergeTree
// rollups (SimpleAggregateFunction sum/max + AggregateFunction uniqExact).
var rollupDDL = []string{
	`CREATE TABLE events (tenant String, user String, kind String, amount UInt64, ts DateTime64(9)) ENGINE = MergeTree ORDER BY (tenant, ts)`,
	`CREATE TABLE by_day (tenant String, day Date, total SimpleAggregateFunction(sum, UInt64), last_ts SimpleAggregateFunction(max, DateTime64(9)), users AggregateFunction(uniqExact, String)) ENGINE = AggregatingMergeTree ORDER BY (tenant, day)`,
	`CREATE TABLE by_kind (tenant String, kind String, n SimpleAggregateFunction(sum, UInt64), users AggregateFunction(uniqExact, String)) ENGINE = AggregatingMergeTree ORDER BY (tenant, kind)`,
	`CREATE TABLE by_user (tenant String, user String, last AggregateFunction(argMax, String, DateTime64(9)), events SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY (tenant, user)`,
	`CREATE MATERIALIZED VIEW mv_day TO by_day AS SELECT tenant, toDate(ts) AS day, sum(amount) AS total, max(ts) AS last_ts, uniqExactState(user) AS users FROM events GROUP BY tenant, day`,
	`CREATE MATERIALIZED VIEW mv_kind TO by_kind AS SELECT tenant, kind, count() AS n, uniqExactState(user) AS users FROM events GROUP BY tenant, kind`,
	`CREATE MATERIALIZED VIEW mv_user TO by_user AS SELECT tenant, user, argMaxState(kind, ts) AS last, count() AS events FROM events GROUP BY tenant, user`,
}

func setupRollups(t *testing.T, inline bool) *testEnv {
	te := newTestEnv(t)
	te.e.cfg.InlineViews = inline
	for _, s := range rollupDDL {
		te.exec(s)
	}
	return te
}

const rollupInsert = `INSERT INTO events VALUES
	('t1', 'alice', 'login', 1, '2024-01-01 10:00:00'), ('t1', 'bob', 'buy', 30, '2024-01-01 11:00:00'),
	('t1', 'alice', 'buy', 12, '2024-01-01 12:00:00'), ('t1', 'carol', 'login', 1, '2024-01-02 09:00:00'),
	('t2', 'alice', 'login', 1, '2024-01-01 08:00:00'), ('t1', 'alice', 'logout', 0, '2024-01-02 23:00:00')`

func checkRollups(t *testing.T, te *testEnv) {
	t.Helper()
	te.expectRows(`SELECT tenant, day, sum(total), max(last_ts), uniqExactMerge(users) FROM by_day GROUP BY tenant, day ORDER BY tenant, day`,
		"t1\t2024-01-01\t43\t2024-01-01 12:00:00.000000000\t2\n"+
			"t1\t2024-01-02\t1\t2024-01-02 23:00:00.000000000\t2\n"+
			"t2\t2024-01-01\t1\t2024-01-01 08:00:00.000000000\t1")
	// Re-aggregation across rows merges several stored sets.
	te.expectRows(`SELECT tenant, sum(total), uniqExactMerge(users) FROM by_day GROUP BY tenant ORDER BY tenant`, "t1\t44\t3\nt2\t1\t1")
	te.expectRows(`SELECT kind, sum(n), uniqExactMerge(users) FROM by_kind WHERE tenant = 't1' GROUP BY kind ORDER BY kind`,
		"buy\t2\t2\nlogin\t2\t2\nlogout\t1\t1")
	te.expectRows(`SELECT user, argMaxMerge(last), sum(events) FROM by_user WHERE tenant = 't1' GROUP BY user ORDER BY user`,
		"alice\tlogout\t3\nbob\tbuy\t1\ncarol\tlogin\t1")
	te.expectRows(`SELECT tenant, kind, n FROM by_kind FINAL WHERE tenant = 't2'`, "t2\tlogin\t1")
}

func TestMaterializedViewPipeline(t *testing.T) {
	te := setupRollups(t, true)
	te.exec(rollupInsert)
	checkRollups(t, te)
	// One stored row per key: the store merged in place.
	tb, _ := te.cat.Table("default", "by_day")
	eq(t, te.store.ItemCount(tb), 3)
}

// sourceRows reads the source table back with its rows in scan order.
func sourceRows(t *testing.T, te *testEnv, name string) (*catalog.Table, []storage.Row) {
	t.Helper()
	tb, err := te.cat.Table("default", name)
	if err != nil {
		t.Fatal(err)
	}
	it, err := te.store.Scan(context.Background(), tb, storage.ScanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	var rows []storage.Row
	for {
		r, err := it.Next(context.Background())
		if err == io.EOF {
			return tb, rows
		}
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, r)
	}
}

func TestApplyViewsExactlyOnce(t *testing.T) {
	te := setupRollups(t, false)
	te.exec(rollupInsert)
	te.expectRows(`SELECT count() FROM by_day`, "0") // no inline application
	src, rows := sourceRows(t, te, "events")
	ids := make([]string, len(rows))
	for i := range rows {
		ids[i] = fmt.Sprintf("rid-%d", i)
	}
	ctx := context.Background()
	// The stream worker may deliver the same records several times, in
	// overlapping batches: the result must not change.
	for _, batch := range [][2]int{{0, 3}, {0, len(rows)}, {2, 5}, {0, len(rows)}} {
		if err := te.e.ApplyViews(ctx, src, rows[batch[0]:batch[1]], ids[batch[0]:batch[1]]); err != nil {
			t.Fatal(err)
		}
	}
	checkRollups(t, te)
	// A new row id is a new event and is counted.
	if err := te.e.ApplyViews(ctx, src, rows[:1], []string{"other"}); err != nil {
		t.Fatal(err)
	}
	te.expectRows(`SELECT sum(n) FROM by_kind WHERE tenant = 't1' AND kind = 'login'`, "3")
}

func TestInlineViewsThenStreamReplay(t *testing.T) {
	// Inline application during INSERT followed by the stream worker
	// re-applying the same rows (same row ids) must not double count.
	te := setupRollups(t, true)
	tb, _ := te.cat.Table("default", "events")
	spy := &idSpy{spyStore: te.spy}
	te.e.cfg.Store = spy
	te.exec(rollupInsert)
	_, rows := sourceRows(t, te, "events")
	if len(spy.ids) != len(rows) {
		t.Fatalf("captured %d row ids for %d rows", len(spy.ids), len(rows))
	}
	if err := te.e.ApplyViews(context.Background(), tb, spy.rows, spy.ids); err != nil {
		t.Fatal(err)
	}
	checkRollups(t, te)
}

// idSpy captures inserted rows and their ids.
type idSpy struct {
	*spyStore
	mu   sync.Mutex
	rows []storage.Row
	ids  []string
}

func (s *idSpy) Insert(ctx context.Context, t *catalog.Table, rows []storage.Row) (*storage.InsertResult, error) {
	res, err := s.spyStore.Insert(ctx, t, rows)
	if err == nil && t.Name == "events" {
		s.mu.Lock()
		s.rows = append(s.rows, rows...)
		s.ids = append(s.ids, res.RowIDs...)
		s.mu.Unlock()
	}
	return res, err
}

func TestMaterializedViewPopulate(t *testing.T) {
	te := newTestEnv(t)
	te.exec(rollupDDL[0])
	te.exec(rollupDDL[1])
	te.exec(rollupInsert)
	res := te.exec(`CREATE MATERIALIZED VIEW mv_day TO by_day POPULATE AS SELECT tenant, toDate(ts) AS day, sum(amount) AS total, max(ts) AS last_ts, uniqExactState(user) AS users FROM events GROUP BY tenant, day`)
	eq(t, res.WrittenRows, uint64(6))
	te.expectRows(`SELECT tenant, sum(total), uniqExactMerge(users) FROM by_day GROUP BY tenant ORDER BY tenant`, "t1\t44\t3\nt2\t1\t1")
	// Later inserts flow through the view as usual.
	te.exec(`INSERT INTO events VALUES ('t2', 'dave', 'buy', 5, '2024-01-01 09:00:00')`)
	te.expectRows(`SELECT sum(total), uniqExactMerge(users) FROM by_day WHERE tenant = 't2'`, "6\t2")
}

func TestMaterializedViewValidation(t *testing.T) {
	te := newTestEnv(t)
	te.exec(rollupDDL[0])
	te.exec(rollupDDL[1])
	cases := []struct {
		sql  string
		code int
	}{
		{`CREATE MATERIALIZED VIEW v TO by_day AS SELECT tenant, toDate(ts) AS day, sum(amount) AS bogus FROM events GROUP BY tenant, day`, thereIsNoColumn},
		{`CREATE MATERIALIZED VIEW v TO by_day AS SELECT tenant, toDate(ts) AS day, uniqExact(user) AS users FROM events GROUP BY tenant, day`, cherr.TypeMismatch},
		{`CREATE MATERIALIZED VIEW v TO by_day AS SELECT tenant, toDate(ts) AS day, sumState(amount) AS users FROM events GROUP BY tenant, day`, cherr.TypeMismatch},
		{`CREATE MATERIALIZED VIEW v TO by_day AS SELECT tenant, sumState(amount) AS total FROM events GROUP BY tenant`, cherr.TypeMismatch},
		{`CREATE MATERIALIZED VIEW v TO nope AS SELECT tenant FROM events`, cherr.UnknownTable},
		{`CREATE MATERIALIZED VIEW v TO by_day AS SELECT tenant FROM nope`, cherr.UnknownTable},
		{`CREATE MATERIALIZED VIEW v TO by_day AS SELECT tenant FROM (SELECT tenant FROM events)`, cherr.NotImplemented},
		{`CREATE MATERIALIZED VIEW v AS SELECT tenant FROM events`, cherr.NotImplemented},
		{`CREATE MATERIALIZED VIEW v TO by_day AS SELECT nope FROM events`, cherr.UnknownIdentifier},
	}
	for _, c := range cases {
		te.expectErr(c.sql, c.code)
	}
	if len(te.cat.Views("default")) != 0 {
		t.Fatal("failed CREATE MATERIALIZED VIEW must not register a view")
	}
	// Missing target columns get defaults; columns are matched by name in
	// any order.
	te.exec(`CREATE MATERIALIZED VIEW v TO by_day AS SELECT uniqExactState(user) AS users, toDate(ts) AS day, tenant FROM events GROUP BY tenant, day`)
	te.exec(`INSERT INTO events VALUES ('t', 'u', 'k', 9, '2024-05-05 00:00:00')`)
	te.expectRows(`SELECT tenant, day, sum(total), uniqExactMerge(users) FROM by_day GROUP BY tenant, day`, "t\t2024-05-05\t0\t1")
}

func TestMaterializedViewMissingTarget(t *testing.T) {
	te := setupRollups(t, true)
	te.exec(`DROP TABLE by_user`)
	_, err := te.run(`INSERT INTO events VALUES ('t1', 'alice', 'login', 1, '2024-01-01 10:00:00')`)
	if !cherr.Is(err, cherr.UnknownTable) || !strings.Contains(err.Error(), "mv_user") {
		t.Fatalf("expected UNKNOWN_TABLE naming the view, got %v", err)
	}
	// The other views were still applied and the source row persisted.
	te.expectRows(`SELECT count() FROM events`, "1")
	te.expectRows(`SELECT sum(n) FROM by_kind`, "1")
}

func TestMaterializedViewToMergeTreeIdempotent(t *testing.T) {
	te := newTestEnv(t)
	te.e.cfg.InlineViews = false
	te.exec(`CREATE TABLE src (k String, v Int64) ENGINE = MergeTree ORDER BY k`)
	te.exec(`CREATE TABLE dst (k String, v2 Int64, tag String DEFAULT 'mv') ENGINE = MergeTree ORDER BY k`)
	te.exec(`CREATE TABLE snap (k String, v Int64) ENGINE = ReplacingMergeTree(v) ORDER BY k`)
	te.exec(`CREATE MATERIALIZED VIEW m1 TO dst AS SELECT k, v * 2 AS v2 FROM src WHERE v > 0`)
	te.exec(`CREATE MATERIALIZED VIEW m2 TO snap AS SELECT k, v FROM src`)
	te.exec(`INSERT INTO src VALUES ('a', 1), ('a', 3), ('b', -1), ('a', 2)`)
	srcT, rows := sourceRows(t, te, "src")
	ids := []string{"1", "2", "3", "4"}
	for i := 0; i < 3; i++ {
		if err := te.e.ApplyViews(context.Background(), srcT, rows, ids); err != nil {
			t.Fatal(err)
		}
	}
	te.expectRows(`SELECT k, v2, tag FROM dst ORDER BY v2`, "a\t2\tmv\na\t4\tmv\na\t6\tmv")
	te.expectRows(`SELECT k, v FROM snap FINAL ORDER BY k`, "a\t3\nb\t-1")
}

func TestApplyViewsArgErrors(t *testing.T) {
	te := setupRollups(t, true)
	src, _ := te.cat.Table("default", "events")
	if err := te.e.ApplyViews(context.Background(), src, []storage.Row{{}}, nil); err == nil {
		t.Fatal("expected an error for mismatched row ids")
	}
	if err := te.e.ApplyViews(context.Background(), src, nil, nil); err != nil {
		t.Fatal(err)
	}
}

// Like ClickHouse (an MV fires for every inserted block), a view sees the
// ReplacingMergeTree rows the store did not keep (older version).
func TestReplacingSupersededRowsTriggerViews(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE snap (k String, ver UInt64, v String) ENGINE = ReplacingMergeTree(ver) ORDER BY k`)
	te.exec(`CREATE TABLE cnt (k String, n SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k`)
	te.exec(`CREATE MATERIALIZED VIEW m TO cnt AS SELECT k, count() AS n FROM snap GROUP BY k`)
	te.exec(`INSERT INTO snap VALUES ('a', 5, 'x')`)
	te.exec(`INSERT INTO snap VALUES ('a', 3, 'older')`)
	te.exec(`INSERT INTO snap VALUES ('a', 6, 'newer')`)
	te.expectRows(`SELECT v FROM snap WHERE k = 'a'`, "newer")
	te.expectRows(`SELECT sum(n) FROM cnt`, "3")
}

func TestUniqExactMergeLazySetSize(t *testing.T) {
	// A single stored state is finalized from its maintained size without
	// loading members; several states are loaded and unioned.
	te := setupRollups(t, true)
	te.exec(rollupInsert)
	loads := 0
	te.e.cfg.Store = &loadCounter{spyStore: te.spy, loads: &loads}
	te.expectRows(`SELECT uniqExactMerge(users) FROM by_kind WHERE tenant = 't1' AND kind = 'buy'`, "2")
	eq(t, loads, 0)
	te.expectRows(`SELECT uniqExactMerge(users) FROM by_kind WHERE tenant = 't1'`, "3")
	eq(t, loads, 3)
}

type loadCounter struct {
	*spyStore
	loads *int
}

func (l *loadCounter) Scan(ctx context.Context, t *catalog.Table, req storage.ScanRequest) (storage.RowIterator, error) {
	it, err := l.spyStore.Scan(ctx, t, req)
	if err != nil {
		return nil, err
	}
	return &countingIter{RowIterator: it, loads: l.loads}, nil
}

type countingIter struct {
	storage.RowIterator
	loads *int
}

func (c *countingIter) Next(ctx context.Context) (storage.Row, error) {
	r, err := c.RowIterator.Next(ctx)
	if err != nil {
		return nil, err
	}
	for _, v := range r {
		p, ok := v.(*chtype.AggPartial)
		if !ok || p.LoadSet == nil {
			continue
		}
		load := p.LoadSet
		p.LoadSet = func(ctx context.Context) ([]any, error) {
			*c.loads++
			return load(ctx)
		}
	}
	return r, nil
}

func TestConcurrentInsertsAndReplays(t *testing.T) {
	te := setupRollups(t, true)
	spy := &idSpy{spyStore: te.spy}
	te.e.cfg.Store = spy
	src, _ := te.cat.Table("default", "events")
	const writers, perWriter = 8, 25
	var wg sync.WaitGroup
	errs := make(chan error, writers*2)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				q := fmt.Sprintf("INSERT INTO events VALUES ('t%d', 'u%d', 'k%d', 1, toDateTime64('2024-01-01 00:00:00', 9) + %d)", w%2, i%5, i%3, w*1000+i)
				if _, err := te.e.Execute(context.Background(), &Request{Query: q}); err != nil {
					errs <- err
					return
				}
				if _, err := te.e.Execute(context.Background(), &Request{Query: "SELECT sum(n), uniqExactMerge(users) FROM by_kind"}); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	// The stream worker replays everything, concurrently, twice.
	spy.mu.Lock()
	rows, ids := append([]storage.Row(nil), spy.rows...), append([]string(nil), spy.ids...)
	spy.mu.Unlock()
	var rg sync.WaitGroup
	for r := 0; r < 4; r++ {
		rg.Add(1)
		go func() {
			defer rg.Done()
			if err := te.e.ApplyViews(context.Background(), src, rows, ids); err != nil {
				t.Error(err)
			}
		}()
	}
	rg.Wait()
	total := writers * perWriter
	te.expectRows(`SELECT sum(n) FROM by_kind`, fmt.Sprint(total))
	te.expectRows(`SELECT sum(total) FROM by_day`, fmt.Sprint(total))
	te.expectRows(`SELECT tenant, uniqExactMerge(users) FROM by_kind GROUP BY tenant ORDER BY tenant`, "t0\t5\nt1\t5")
	te.expectRows(`SELECT sum(events) FROM by_user`, fmt.Sprint(total))
}
