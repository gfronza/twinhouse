package exec

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/storage"
	"github.com/gfronza/twinhouse/internal/storage/memstore"
)

// A view reading FROM a SummingMergeTree/AggregatingMergeTree table would
// see merged state in the source stream, not the inserted rows.
func TestViewFromMergingSourceRejected(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE daily (k String, n UInt64) ENGINE = SummingMergeTree ORDER BY k`)
	te.exec(`CREATE TABLE dagg (k String, n SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k`)
	te.exec(`CREATE TABLE total (k String, n UInt64) ENGINE = SummingMergeTree ORDER BY k`)
	te.expectErr(`CREATE MATERIALIZED VIEW mv TO total AS SELECT k, sum(n) AS n FROM daily GROUP BY k`, cherr.NotImplemented)
	te.expectErr(`CREATE MATERIALIZED VIEW mv TO total AS SELECT k, sum(n) AS n FROM dagg GROUP BY k`, cherr.NotImplemented)
	te.expectErr(`CREATE MATERIALIZED VIEW mv TO total POPULATE AS SELECT k, sum(n) AS n FROM daily GROUP BY k`, cherr.NotImplemented)
	te.expectRows(`EXISTS TABLE mv`, "0")
}

// A view created before the check (already in a catalog) is ignored by
// both the inline path and ApplyViews (the stream worker's entry point).
func TestLegacyViewFromMergingSourceIgnored(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE daily (k String, n UInt64) ENGINE = MergeTree ORDER BY k`)
	te.exec(`CREATE TABLE total (k String, n UInt64) ENGINE = SummingMergeTree ORDER BY k`)
	te.exec(`CREATE MATERIALIZED VIEW mv TO total AS SELECT k, sum(n) AS n FROM daily GROUP BY k`)
	// Simulate a catalog written by an older build: the source became a
	// SummingMergeTree table with the view still attached.
	tb, err := te.cat.Table("default", "daily")
	if err != nil {
		t.Fatal(err)
	}
	tb.Engine = "SummingMergeTree"
	defer func() { tb.Engine = "MergeTree" }()
	if err := te.e.ApplyViews(context.Background(), tb, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := te.e.applyViewsQuery(te.e.newQuery(context.Background(), &Request{}), tb, nil, nil); err != nil {
		t.Fatal(err)
	}
	q := te.e.newQuery(context.Background(), &Request{})
	if err := q.applyViewList(te.cat.ViewsFrom("default", "daily"), tb, []storage.Row{{"a", uint64(1)}}, []string{"r1"}); err != nil {
		t.Fatal(err)
	}
	te.expectRows(`SELECT count() FROM total`, "0")
}

// A view whose target sorting key depends on now() (or another volatile
// function) would compute different keys inline and in the stream worker,
// so both applications would count.
func TestViewVolatileKeyRejected(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE events (id UInt32, ts DateTime) ENGINE = MergeTree ORDER BY id`)
	te.exec(`CREATE TABLE per_sec (t DateTime, c UInt64) ENGINE = SummingMergeTree ORDER BY t`)
	te.exec(`CREATE TABLE per_id (id UInt32, last DateTime, c UInt64) ENGINE = SummingMergeTree ORDER BY id`)
	te.exec(`CREATE TABLE per_u (u UUID, c UInt64) ENGINE = SummingMergeTree ORDER BY u`)
	for _, q := range []string{
		`CREATE MATERIALIZED VIEW m1 TO per_sec AS SELECT now() AS t, count() AS c FROM events`,
		`CREATE MATERIALIZED VIEW m1 TO per_sec AS SELECT toStartOfHour(NOW()) AS t, count() AS c FROM events GROUP BY t`,
		`CREATE MATERIALIZED VIEW m1 TO per_sec AS SELECT x AS t, count() AS c FROM events GROUP BY x, now() AS x`,
		`CREATE MATERIALIZED VIEW m1 TO per_sec AS WITH today() AS d SELECT toDateTime(d) AS t, count() AS c FROM events`,
		`CREATE MATERIALIZED VIEW m1 TO per_u AS SELECT generateUUIDv4() AS u, count() AS c FROM events`,
	} {
		te.expectErr(q, cherr.NotImplemented)
	}
	// Deterministic keys, and volatile values outside the key, are fine.
	te.exec(`CREATE MATERIALIZED VIEW m1 TO per_sec AS SELECT toStartOfMinute(ts) AS t, count() AS c FROM events GROUP BY t`)
	te.exec(`CREATE MATERIALIZED VIEW m2 TO per_id AS SELECT id, now() AS last, count() AS c FROM events GROUP BY id`)
	spy := &idSpy{spyStore: te.spy}
	te.e.cfg.Store = spy
	te.exec(`INSERT INTO events VALUES (1, '2024-01-01 00:00:10'), (2, '2024-01-01 00:00:20')`)
	// The stream worker replays the same rows later: nothing changes.
	time.Sleep(1100 * time.Millisecond)
	tb, _ := te.cat.Table("default", "events")
	if err := te.e.ApplyViews(context.Background(), tb, spy.rows, spy.ids); err != nil {
		t.Fatal(err)
	}
	te.expectRows(`SELECT t, c FROM per_sec`, "2024-01-01 00:00:00\t2")
	te.expectRows(`SELECT sum(c) FROM per_id`, "2")
}

// catBackend is a catalog.Backend shared by two engines (two servers on one
// DynamoDB).
type catBackend struct {
	mu      sync.Mutex
	entries map[string]catalog.Entry
	loads   int
}

func (b *catBackend) LoadAll(context.Context) ([]catalog.Entry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.loads++
	out := make([]catalog.Entry, 0, len(b.entries))
	for _, e := range b.entries {
		out = append(out, e)
	}
	return out, nil
}

func (b *catBackend) Put(_ context.Context, e catalog.Entry, conds []catalog.Cond) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, c := range conds {
		cur, ok := b.entries[c.Key]
		if ok != c.Exists || (c.Version > 0 && cur.Version != c.Version) {
			return &catalog.CondFailedError{Index: i}
		}
	}
	b.entries[e.Key] = e
	return nil
}

func (b *catBackend) Delete(_ context.Context, keys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, k := range keys {
		delete(b.entries, k)
	}
	return nil
}

// Two servers on one store: DDL on A is visible to B's next statement
// without waiting for B's periodic catalog refresh.
func TestCatalogMissReloads(t *testing.T) {
	ms := memstore.New()
	be := &catBackend{entries: map[string]catalog.Entry{}}
	mk := func() *testEnv {
		cat := catalog.New(be, catalog.Options{})
		e, err := New(Config{Store: ms, Catalog: cat, InlineViews: true, ConsistentReads: true})
		if err != nil {
			t.Fatal(err)
		}
		return &testEnv{t: t, e: e, store: ms, cat: cat}
	}
	a, b := mk(), mk()
	a.exec(`CREATE TABLE lb_t (k UInt32) ENGINE = MergeTree ORDER BY k`)
	b.exec(`INSERT INTO lb_t VALUES (1)`)
	a.exec(`ALTER TABLE lb_t ADD COLUMN v String`)
	b.exec(`INSERT INTO lb_t (k, v) VALUES (2, 'x')`)
	a.expectRows(`SELECT k, v FROM lb_t ORDER BY k`, "1\t\n2\tx")
	a.exec(`CREATE DATABASE other`)
	b.exec(`CREATE TABLE other.t2 (k UInt32) ENGINE = MergeTree ORDER BY k`)
	a.expectRows(`SELECT count() FROM other.t2`, "0")

	// Misses are coalesced and rate limited.
	be.mu.Lock()
	before := be.loads
	be.mu.Unlock()
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := b.run(`SELECT * FROM no_such_table`); !cherr.Is(err, cherr.UnknownTable) {
				t.Errorf("got %v", err)
			}
		}()
	}
	wg.Wait()
	be.mu.Lock()
	loads := be.loads - before
	be.mu.Unlock()
	if max := int(time.Since(start)/missRefreshInterval) + 2; loads > max {
		t.Errorf("%d catalog loads for 50 concurrent misses (max %d)", loads, max)
	}
}

// POPULATE would back-fill only this region's copy of a regional target.
func TestPopulateRegionalRejectedMultiRegion(t *testing.T) {
	te := newTestEnv(t)
	te.e.cfg.MultiRegion = true
	te.exec(`CREATE TABLE src (region String, id UInt64) ENGINE = MergeTree ORDER BY (region, id)`)
	te.exec(`CREATE TABLE roll (region String, n UInt64) ENGINE = SummingMergeTree ORDER BY region`)
	te.exec(`CREATE TABLE copy (region String, id UInt64) ENGINE = MergeTree ORDER BY (region, id)`)
	te.exec(`INSERT INTO src VALUES ('us', 1), ('us', 2)`)
	te.expectErr(`CREATE MATERIALIZED VIEW mv TO roll POPULATE AS SELECT region, count() AS n FROM src GROUP BY region`, cherr.NotImplemented)
	te.expectRows(`EXISTS TABLE mv`, "0")
	te.exec(`CREATE MATERIALIZED VIEW mv TO roll AS SELECT region, count() AS n FROM src GROUP BY region`)
	// A global target is replicated, so POPULATE is fine.
	te.exec(`CREATE MATERIALIZED VIEW mv2 TO copy POPULATE AS SELECT region, id FROM src`)
	te.expectRows(`SELECT count() FROM copy`, "2")
}

// Enums compare, sort and aggregate by their numeric value, not by name
// (expected values from ClickHouse 25.8).
func TestEnumOrderByValue(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE p (id UInt32, en Enum8('x' = 1, 'a' = 2, 'm' = 3)) ENGINE = MergeTree ORDER BY id`)
	te.exec(`INSERT INTO p VALUES (1, 'x'), (2, 'a'), (3, 'm'), (4, 'x'), (5, 'a')`)
	e := `CAST('a', 'Enum8(''x''=1,''a''=2)')`
	for _, c := range [][2]string{
		{`SELECT count() FROM p WHERE en > 'x'`, "3"},
		{`SELECT countIf(en < 'm') FROM p`, "4"},
		{`SELECT count() FROM p WHERE en BETWEEN 'x' AND 'a'`, "4"},
		{`SELECT en FROM p GROUP BY en ORDER BY en`, "x\na\nm"},
		{`SELECT en FROM p GROUP BY en ORDER BY en DESC`, "m\na\nx"},
		{`SELECT id FROM p ORDER BY en, id`, "1\n4\n2\n5\n3"},
		{`SELECT min(en), max(en), argMax(id, en), argMin(id, en) FROM p`, "x\tm\t3\t1"},
		{`SELECT count() FROM p WHERE en = 'a'`, "2"},
		{`SELECT count() FROM p WHERE en IN ('a', 'm')`, "3"},
		{`SELECT ` + e + ` > 'x'`, "1"},
		{`SELECT ` + e + ` > 1, ` + e + ` = 2, ` + e + ` < 3`, "1\t1\t1"},
		{`SELECT 'x' < ` + e, "1"},
		{`SELECT ` + e + ` = 'zz', ` + e + ` < 'zz', 'zz' < ` + e + `, ` + e + ` >= 'zz'`, "0\t0\t0\t0"},
		{`SELECT ` + e + ` = CAST('a', 'Enum8(''a''=1)')`, "0"},
		{`SELECT ` + e + ` < CAST('x', 'Enum8(''a''=1,''x''=5)')`, "1"},
		// A non-constant string compares with the element name.
		{`SELECT en < s, en > s FROM (SELECT ` + e + ` AS en, arrayJoin(['x']) AS s)`, "1\t0"},
		{`SELECT greatest(` + e + `, CAST('x', 'Enum8(''x''=1,''a''=2)')), least(` + e + `, CAST('x', 'Enum8(''x''=1,''a''=2)'))`, "a\tx"},
		{`SELECT arraySort([` + e + `, CAST('x', 'Enum8(''x''=1,''a''=2)')])`, "['x','a']"},
		{`SELECT finalizeAggregation(minState(en)), finalizeAggregation(maxState(en)) FROM p`, "x\tm"},
	} {
		te.expectRows(c[0], c[1])
	}
	te.expectErr(`SELECT `+e+` != 'zz'`, cherr.UnknownElementOfEnum)

	// Aggregate states merged in a rollup compare by value too.
	te.exec(`CREATE TABLE r (k UInt8, lo AggregateFunction(min, Enum8('x' = 1, 'a' = 2, 'm' = 3)), hi SimpleAggregateFunction(max, Enum8('x' = 1, 'a' = 2, 'm' = 3))) ENGINE = AggregatingMergeTree ORDER BY k`)
	te.exec(`CREATE MATERIALIZED VIEW rv TO r AS SELECT 1 AS k, minState(en) AS lo, max(en) AS hi FROM p`)
	te.exec(`INSERT INTO p VALUES (6, 'a'), (7, 'x'), (8, 'm'), (9, 'a')`)
	te.expectRows(`SELECT minMerge(lo), max(hi) FROM r`, "x\tm")
}

// A Date compares with a DateTime as midnight in the DateTime's time zone
// (explicit, else the server's), in comparisons, IN and key pushdown
// (expected values from ClickHouse 25.8).
func TestDateVsDateTimeTimezone(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE ev (id UInt32, ts DateTime('Asia/Tokyo')) ENGINE = MergeTree ORDER BY id`)
	te.exec(`CREATE TABLE evk (ts DateTime('Asia/Tokyo'), id UInt32) ENGINE = MergeTree ORDER BY ts`)
	te.exec(`INSERT INTO ev VALUES (1, '2024-01-01 05:00:00'), (2, '2024-01-01 21:00:00')`)
	te.exec(`INSERT INTO evk VALUES ('2024-01-01 05:00:00', 1), ('2024-01-01 21:00:00', 2)`)
	for _, tb := range []string{"ev", "evk"} {
		te.expectRows(`SELECT count() FROM `+tb+` WHERE ts >= toDate('2024-01-01')`, "2")
		te.expectRows(`SELECT count() FROM `+tb+` WHERE toDate('2024-01-01') <= ts`, "2")
		te.expectRows(`SELECT count() FROM `+tb+` WHERE ts BETWEEN toDate('2024-01-01') AND toDate('2024-01-02')`, "2")
		te.expectRows(`SELECT count() FROM `+tb+` WHERE ts < toDate32('2024-01-02')`, "2")
	}
	te.expectRows(`SELECT toDate('2024-01-01') = toDateTime('2024-01-01 00:00:00', 'Asia/Tokyo'), toDate('2024-01-01') IN (toDateTime('2024-01-01 00:00:00', 'Asia/Tokyo'))`, "1\t1")
	te.expectRows(`SELECT toDate('2024-01-01') = toDateTime64('2024-01-01 00:00:00', 3, 'Asia/Tokyo'), toDateTime('2024-01-01 00:00:00', 'Asia/Tokyo') > toDate('2024-01-01')`, "1\t0")

	// Server time zone (CH: session_timezone = 'America/New_York').
	te.e.cfg.Timezone = "America/New_York"
	for _, c := range [][2]string{
		{`SELECT toDateTime('2024-01-01 00:00:00') = toDate('2024-01-01')`, "1"},
		{`SELECT toDateTime('2024-01-01 00:00:00') = '2024-01-01 00:00:00'`, "1"},
		{`SELECT toDateTime('2024-01-01 00:00:00') IN (toDate('2024-01-01'))`, "1"},
		{`SELECT toDateTime('2024-01-01 00:00:00') IN ('2024-01-01 00:00:00')`, "1"},
		{`SELECT toDateTime64('2024-01-01 00:00:00', 3) = toDate('2024-01-01')`, "1"},
		{`SELECT toDate32('2024-01-01') = toDateTime('2024-01-01 00:00:00', 'Asia/Tokyo')`, "0"},
		{`SELECT toDate32('2024-01-01') = toDateTime('2024-01-01 00:00:00')`, "1"},
	} {
		te.expectRows(c[0], c[1])
	}
	te.exec(`CREATE TABLE evn (ts DateTime, id UInt32) ENGINE = MergeTree ORDER BY ts`)
	te.exec(`INSERT INTO evn VALUES ('2023-12-31 23:00:00', 1), ('2024-01-01 00:00:00', 2), ('2024-01-01 01:00:00', 3)`)
	te.expectRows(`SELECT id FROM evn WHERE ts >= toDate('2024-01-01') ORDER BY id`, "2\n3")
	te.expectRows(`SELECT id FROM evn WHERE ts = toDate('2024-01-01')`, "2")
	te.expectRows(`SELECT id FROM evn WHERE ts IN (toDate('2024-01-01'), '2024-01-01 01:00:00') ORDER BY id`, "2\n3")
}

// toXOrNull/toXOrZero return NULL/0 for text that does not fit the type;
// a '-' sign is never valid for an unsigned type (ClickHouse 25.8).
func TestToIntOrNullRange(t *testing.T) {
	te := newTestEnv(t)
	te.expectRows(`SELECT toUInt8OrNull('-1'), toUInt8OrNull('300'), toInt8OrNull('200'), toUInt8OrZero('-1'), toUInt8OrZero('300'), toInt32OrNull('3000000000'), toUInt64OrNull('-1'), toUInt32OrNull('4294967296'), toInt64OrNull('9223372036854775808'), toUInt8OrNull('255'), toInt8OrNull('-128'), toInt8OrZero('-129')`,
		"\\N\t\\N\t\\N\t0\t0\t\\N\t\\N\t\\N\t\\N\t255\t-128\t0")
	te.expectRows(`SELECT toUInt8OrNull('-0'), toUInt8OrNull('+300'), toUInt16OrNull('65535'), toUInt16OrNull('65536'), toInt32OrNull('-2147483649'), toInt32OrNull('-2147483648')`,
		"\\N\t\\N\t65535\t\\N\t\\N\t-2147483648")
	// Plain conversions wrap, but reject '-' for unsigned types.
	te.expectRows(`SELECT toUInt8('300'), toInt32('3000000000'), toInt8('-129')`, "44\t-1294967296\t127")
	te.expectErr(`SELECT toUInt8('-1')`, cherr.CannotParseNumber)
	te.expectErr(`SELECT toUInt64('-1')`, cherr.CannotParseNumber)
	te.expectErr(`SELECT toUInt8('-0')`, cherr.CannotParseNumber)
}

// Float modulo is x - trunc(x/y)*y like ClickHouse (values from 25.8).
func TestFloatModulo(t *testing.T) {
	te := newTestEnv(t)
	te.expectRows(`SELECT 1 % inf, -1 % inf, inf % 2, 1 % 0., 1 % 0.1, 10 % 0.3, 1e20 % 3.0, -1 % 0.1, -7.5 % 2, 5.5 % -2, 1e300 % 7, -4. % 2, toFloat32(1) % toFloat32(0.1), 0.30000000000000004 % 0.1, moduloOrZero(1., 0.)`,
		"nan\tnan\tnan\tnan\t0\t0.09999999999999964\t0\t0\t-1.5\t1.5\t0\t0\t0.09999998658895493\t0\t0")
}

// FixedString compares with String ignoring trailing zero bytes on both
// sides (ClickHouse 25.8), in filters, IN and key pushdown.
func TestFixedStringTrailingNul(t *testing.T) {
	te := newTestEnv(t)
	te.expectRows(`SELECT toFixedString('a', 3) = 'a\0\0', toFixedString('a', 3) = 'a\0', toFixedString('a', 3) = 'a\0\0\0', toFixedString('a', 3) < 'a\0', toFixedString('a', 3) IN ('a\0')`, "1\t1\t1\t0\t1")
	te.exec(`CREATE TABLE p1 (fs FixedString(3), id UInt32) ENGINE = MergeTree ORDER BY fs`)
	te.exec(`CREATE TABLE p2 (id UInt32, fs FixedString(3)) ENGINE = MergeTree ORDER BY id`)
	te.exec(`INSERT INTO p1 VALUES ('a', 1), ('a', 2), ('b', 3)`)
	te.exec(`INSERT INTO p2 VALUES (1, 'a'), (2, 'a'), (3, 'b')`)
	for _, tb := range []string{"p1", "p2"} {
		for _, w := range []string{`fs = 'a'`, `fs = 'a\0\0'`, `fs = 'a\0'`, `fs IN ('a\0')`, `fs = 'a\0\0\0\0'`} {
			te.expectRows(`SELECT count() FROM `+tb+` WHERE `+w, "2")
		}
		te.expectRows(`SELECT count() FROM `+tb+` WHERE fs = 'abcd'`, "0")
		te.expectRows(`SELECT count() FROM `+tb+` WHERE fs > 'a\0'`, "1")
	}
}

// LEFT ANTI JOIN: the right key columns of the emitted rows hold the left
// key values (ClickHouse 25.8).
func TestAntiJoinRightKeys(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE l (id UInt32, k UInt32, k2 UInt8) ENGINE = MergeTree ORDER BY id`)
	te.exec(`CREATE TABLE r (rid UInt32, k UInt64, k2 UInt8) ENGINE = MergeTree ORDER BY rid`)
	te.exec(`INSERT INTO l VALUES (1,1,1),(2,2,2),(3,3,3)`)
	te.exec(`INSERT INTO r VALUES (10,1,1),(11,1,1),(12,2,2),(13,4,4),(14,2,9)`)
	te.expectRows(`SELECT l.id, r.k, toTypeName(r.k) FROM l AS l LEFT ANTI JOIN r AS r USING (k) ORDER BY l.id`, "3\t3\tUInt64")
	te.expectRows(`SELECT l.id, r.k, r.k2, r.rid FROM l AS l LEFT ANTI JOIN r AS r USING (k, k2) ORDER BY l.id`, "3\t3\t3\t0")
	te.expectRows(`SELECT l.id, r.k FROM l AS l LEFT ANTI JOIN r AS r ON l.k = r.k ORDER BY l.id`, "3\t3")
	te.expectRows(`SELECT count() FROM l AS l LEFT ANTI JOIN r AS r USING (k) WHERE r.k = 3`, "1")
	te.expectRows(`SELECT l.id, k, r.k FROM l AS l LEFT ANTI JOIN r AS r USING (k) ORDER BY l.id`, "3\t3\t3")
	// Plain LEFT JOIN keeps the right side's defaults.
	te.expectRows(`SELECT l.id, r.k FROM l AS l LEFT JOIN r AS r USING (k) ORDER BY l.id, r.k`, "1\t1\n1\t1\n2\t2\n2\t2\n3\t0")
}

// Result-affecting settings the engine does not implement are rejected
// instead of being ignored; unknown names in SETTINGS/SET are errors;
// session_timezone drives parsing and timezone() (ClickHouse 25.8).
func TestQuerySettingsChecked(t *testing.T) {
	te := newTestEnv(t)
	for _, q := range []string{
		`SELECT number FROM numbers(5) SETTINGS limit = 2`,
		`SELECT number FROM numbers(5) LIMIT 2 SETTINGS offset = 1`,
		`SELECT 1 SETTINGS transform_null_in = 1`,
		`SELECT sum(number) FROM numbers(5) WHERE 0 SETTINGS aggregate_functions_null_for_empty = 1`,
		`SELECT 1 SETTINGS prefer_column_name_to_alias = true`,
		`SELECT 1 SETTINGS cast_keep_nullable = 1`,
		`SELECT 1 SETTINGS join_default_strictness = 'ANY'`,
		`SET transform_null_in = 1`,
		`SELECT 1 SETTINGS session_timezone = 'Asia/Kolkata'`,
	} {
		te.expectErr(q, cherr.NotImplemented)
	}
	te.expectErr(`SELECT 1 SETTINGS no_such_setting_xyz = 1`, cherr.UnknownSetting)
	te.expectErr(`SET no_such_setting_xyz = 1`, cherr.UnknownSetting)
	te.expectRows(`SELECT 1 SETTINGS limit = 0, offset = 0, transform_null_in = 0, join_default_strictness = 'ALL', max_threads = 4, SQL_mine = 1`, "1")
	if _, err := te.runReq(&Request{Query: `SELECT 1`, Settings: map[string]string{"limit": "2"}}); !cherr.Is(err, cherr.NotImplemented) {
		t.Errorf("URL limit: %v", err)
	}
	if _, err := te.runReq(&Request{Query: `SELECT 1`, Settings: map[string]string{"session_timezone": "Nope/Zone"}}); !cherr.Is(err, cherr.BadArguments) {
		t.Errorf("bad session_timezone: %v", err)
	}
	res, err := te.runReq(&Request{Query: `SELECT timezone(), serverTimezone(), toUnixTimestamp(toDateTime('2024-01-01 00:00:00')), toDateTime('2024-01-01 00:00:00') = '2024-01-01 00:00:00'`,
		Settings: map[string]string{"session_timezone": "Asia/Kolkata"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := renderTSV(t, res); !strings.HasSuffix(got, "\nAsia/Kolkata\tUTC\t1704047400\t1\n") {
		t.Errorf("session_timezone: got %q", got)
	}
	// The same zone in a SETTINGS clause is a no-op.
	if _, err := te.runReq(&Request{Query: `SELECT 1 SETTINGS session_timezone = 'Asia/Kolkata'`, Settings: map[string]string{"session_timezone": "Asia/Kolkata"}}); err != nil {
		t.Error(err)
	}
}

// In a multi-region deployment a superseded ReplacingMergeTree row reaches
// no other region, so it feeds no view (rollups stay convergent).
func TestReplacingSupersededRowsMultiRegion(t *testing.T) {
	te := newTestEnv(t)
	te.e.cfg.MultiRegion = true
	te.exec(`CREATE TABLE snap (k String, ver UInt64, v String) ENGINE = ReplacingMergeTree(ver) ORDER BY k`)
	te.exec(`CREATE TABLE cnt (v String, n UInt64) ENGINE = SummingMergeTree ORDER BY v`)
	te.exec(`CREATE MATERIALIZED VIEW m TO cnt AS SELECT v, count() AS n FROM snap GROUP BY v`)
	te.exec(`INSERT INTO snap VALUES ('a', 5, 'x')`)
	te.exec(`INSERT INTO snap VALUES ('a', 3, 'older')`)
	te.expectRows(`SELECT v, n FROM cnt ORDER BY v`, "x\t1")
}
