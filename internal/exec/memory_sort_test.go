package exec

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/format"
	"github.com/gfronza/twinhouse/internal/memtrack"
	"github.com/gfronza/twinhouse/internal/sqlparse"
)

// Sorts, DISTINCT, LIMIT BY and UNION DISTINCT give the same results with
// the memory accounting on as with it off.
func TestSortAndKeySetResultsDoNotDependOnAccounting(t *testing.T) {
	limited, unlimited := newTestEnv(t), newMemoryEnv(t, 0, 0)
	for _, te := range []*testEnv{limited, unlimited} {
		te.exec("CREATE TABLE t (id UInt64, k Nullable(Int64), s String, a Array(String)) ENGINE = MergeTree ORDER BY id")
		te.exec("INSERT INTO t SELECT number, if(number % 5 = 0, NULL, toInt64(number % 7) - 3), repeat(toString(number % 13), number % 40), " +
			"[toString(number % 3), repeat('z', number % 20)] FROM numbers(3000)")
	}
	for _, sql := range []string{
		"SELECT id, k, s FROM t ORDER BY k, s DESC, id",
		"SELECT id FROM t ORDER BY k NULLS FIRST, length(s)",
		"SELECT id, s FROM t ORDER BY s DESC LIMIT 50 OFFSET 7",
		"SELECT id FROM t ORDER BY k LIMIT 2000",
		"SELECT id, a FROM t ORDER BY a, id LIMIT 5000",
		"SELECT DISTINCT k, length(s) FROM t ORDER BY k, length(s)",
		"SELECT DISTINCT a FROM t ORDER BY a",
		"SELECT id, s FROM t ORDER BY id DESC LIMIT 2 BY k",
		"SELECT id FROM t LIMIT 1 BY s, k",
		"SELECT s FROM t UNION DISTINCT SELECT toString(k) FROM t ORDER BY s",
		"SELECT count() FROM (SELECT a FROM t UNION DISTINCT SELECT ['0', ''])",
	} {
		if got, want := limited.tsv(sql), unlimited.tsv(sql); got != want {
			t.Errorf("%s: with accounting\n%s\nwithout\n%s", sql, got, want)
		}
	}
}

// A top-N sort charges only the rows it keeps: an evicted row's charge is
// released.
func TestTopNChargesOnlyKeptRows(t *testing.T) {
	if testing.Short() {
		t.Skip("sorts a million rows under a limit")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	te.expectRows("SELECT number FROM numbers(1000000) ORDER BY number DESC LIMIT 2 SETTINGS max_memory_usage = 1000000", "999999\n999998")
	te.expectRows("SELECT length(s) FROM (SELECT repeat('x', number % 1000) AS s FROM numbers(200000)) ORDER BY s DESC LIMIT 2 "+
		"SETTINGS max_memory_usage = 1000000", "999\n999")
	wantMemoryLimit(t, te, "SELECT number FROM numbers(1000000) ORDER BY number DESC LIMIT 100000 SETTINGS max_memory_usage = 1000000", nil)
}

// A value top-N rows share (a constant, an ARRAY JOIN source array or
// string, one of if's branches) is charged once, as a full sort charges
// it: with a LIMIT these fit the limit as they do without one.
func TestTopNChargesSharedValuesOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("keeps sorted rows of about 100 MB of shared values under a limit")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	for _, c := range []struct{ sql, want string }{
		{"SELECT count() FROM (SELECT number, repeat('x', 100000) AS s FROM numbers(1000) ORDER BY number DESC LIMIT 1000)", "1000"},
		{"SELECT count() FROM (SELECT a, x FROM (SELECT range(3000) AS a) ARRAY JOIN a AS x ORDER BY x DESC LIMIT 3000)", "3000"},
		{"SELECT count() FROM (SELECT number, range(300000) AS a FROM numbers(3) ORDER BY number LIMIT 2)", "2"},
		// Strings rows share.
		{"SELECT count() FROM (SELECT number, if(number % 2 = 0, repeat('a', 10000), repeat('b', 10000)) AS s FROM numbers(10000) " +
			"ORDER BY number DESC LIMIT 10000)", "10000"},
		{"SELECT count() FROM (SELECT l.number, r.s FROM numbers(5000) AS l CROSS JOIN (SELECT concat(repeat('z', 10000), toString(number)) AS s " +
			"FROM numbers(1)) AS r ORDER BY l.number DESC LIMIT 5000)", "5000"},
		{"SELECT count() FROM (SELECT s, x FROM (SELECT concat(repeat('x', 100000), toString(number)) AS s, range(1000) AS a FROM numbers(3)) " +
			"ARRAY JOIN a AS x ORDER BY x LIMIT 1000000)", "3000"},
	} {
		te.expectRows(c.sql+" SETTINGS max_memory_usage = 32000000", c.want)
	}
}

// A shared value stays charged while any kept row holds it, and is
// released with the last one, evicted or read out.
func TestTopNReleasesASharedValueWithItsLastRow(t *testing.T) {
	te := newTestEnv(t)
	q := te.e.newQuery(context.Background(), &Request{})
	cols := []format.Column{{Type: chtype.TUInt64}, {Type: chtype.NewArray(chtype.TUInt64)}, {Type: chtype.TUInt64}}
	ch := newRowCharge(q.mem.NewAccount(), cols)
	elems := make([]any, 100000)
	for i := range elems {
		elems[i] = uint64(i)
	}
	// Rows share a value through one box, as the engine copies it.
	var shared any = elems
	arr := memtrack.Value(shared)
	top := newTopN(newRowOrder(2, []sqlparse.OrderItem{{}}), 3, ch)
	add := func(key uint64, a any) {
		t.Helper()
		if err := top.add([]any{key, a, key}); err != nil {
			t.Fatal(err)
		}
	}
	// Three rows share the array, then rows of smaller keys that share it
	// too evict them.
	for _, k := range []uint64{30, 20, 10, 3, 2, 1} {
		add(k, shared)
	}
	if c := ch.acct.Charged(); c < arr || c > arr+arr/4 {
		t.Errorf("rows sharing the array charged %d, the array takes %d", c, arr)
	}
	// Rows of their own evict the last rows holding it.
	for range 3 {
		add(0, []any{uint64(0)})
	}
	if c := ch.acct.Charged(); c >= arr {
		t.Errorf("the array is no longer held, yet %d is charged", c)
	}
	it := &topNIter{rows: top.sorted(), t: top}
	for {
		if _, err := it.next(); err == io.EOF {
			break
		}
	}
	it.close()
	if c := ch.acct.Charged(); c != 0 {
		t.Errorf("%d charged after close", c)
	}
}

// A sort's row holds a selected column and an ORDER BY or LIMIT BY key
// that is that column as one value, charged once: sorting by the column
// fits the limit as sorting by another does.
func TestSortChargesAColumnItSortsByOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("sorts 20 MB of strings under a limit")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	te.exec("CREATE TABLE ts (id UInt64, s String) ENGINE = MergeTree ORDER BY id")
	te.exec("INSERT INTO ts SELECT number, concat(repeat('x', 1000), toString(number)) FROM numbers(20000)")
	const limit = " SETTINGS max_memory_usage = 32000000"
	for _, c := range []struct{ sql, want string }{
		{"SELECT s FROM ts ORDER BY id DESC", "20000"},
		{"SELECT s FROM ts ORDER BY s", "20000"},
		{"SELECT s AS x FROM ts ORDER BY x", "20000"},
		{"SELECT s FROM ts ORDER BY length(s), s LIMIT 100000", "20000"},
		{"SELECT s FROM ts ORDER BY id DESC LIMIT 1 BY s", "20000"},
		{"SELECT s FROM ts WHERE id < 12000 ORDER BY s LIMIT 1 BY s", "12000"},
	} {
		te.expectRows("SELECT count() FROM ("+c.sql+")"+limit, c.want)
	}
	if n := len(te.exec("SELECT s FROM ts ORDER BY s" + limit).Rows); n != 20000 {
		t.Errorf("collected %d sorted rows, want 20000", n)
	}
}

// A long string kept top-N rows share (one of if's branches, a CROSS JOIN
// right-side value) is charged once while any of them holds it, as in a
// full sort, and released with the last.
func TestTopNChargesAStringItsRowsShareOnce(t *testing.T) {
	te := newTestEnv(t)
	q := te.e.newQuery(context.Background(), &Request{})
	ch := newRowCharge(q.mem.NewAccount(), []format.Column{{Type: chtype.TString}, {Type: chtype.TUInt64}})
	defer ch.acct.Close()
	// Rows share each string through one box, as the engine copies it.
	var a, b any = strings.Repeat("a", 1000000), strings.Repeat("b", 1000000)
	str := memtrack.String(a.(string))
	top := newTopN(newRowOrder(1, []sqlparse.OrderItem{{}}), 1000, ch)
	add := func(i uint64, v any) {
		t.Helper()
		if err := top.add([]any{v, i}); err != nil {
			t.Fatal(err)
		}
	}
	// 2000 rows alternate between the two strings; the later half evicts
	// the first.
	for i := range uint64(2000) {
		v := a
		if i%2 == 1 {
			v = b
		}
		add(2000-i, v)
	}
	if c := ch.acct.Charged(); c < 2*str || c > 2*str+str/2 {
		t.Errorf("rows sharing two strings charged %d, the strings take %d", c, 2*str)
	}
	// Rows of their own evict them all.
	for range 1000 {
		add(0, strings.Repeat("c", 20))
	}
	if c := ch.acct.Charged(); c >= str {
		t.Errorf("the shared strings are no longer held, yet %d is charged", c)
	}
}

// stringTopN is a top-N over rows of two String columns and a UInt64 key,
// keeping the keep smallest keys.
type stringTopN struct {
	t    *testing.T
	name string
	top  *topN
	ch   rowCharge
}

func newStringTopN(t *testing.T, name string, keep uint64) *stringTopN {
	te := newTestEnv(t)
	q := te.e.newQuery(context.Background(), &Request{})
	cols := []format.Column{{Type: chtype.TString}, {Type: chtype.TString}, {Type: chtype.TUInt64}}
	ch := newRowCharge(q.mem.NewAccount(), cols)
	t.Cleanup(ch.acct.Close)
	return &stringTopN{t: t, name: name, top: newTopN(newRowOrder(2, []sqlparse.OrderItem{{}}), keep, ch), ch: ch}
}

// add adds the row [x, y, key] and checks the top-N is charged at least
// the distinct strings of 16 B or more its rows hold.
func (s *stringTopN) add(x, y any, key uint64) {
	s.t.Helper()
	if err := s.top.add([]any{x, y, key}); err != nil {
		s.t.Fatal(err)
	}
	held := map[uintptr]int64{}
	for _, row := range s.top.rows {
		for i := range 2 {
			if v := row[i].(string); len(v) >= memtrack.TinyBlock {
				box, _ := shareable(&row[i])
				held[box] = memtrack.String(v)
			}
		}
	}
	var want int64
	for _, n := range held {
		want += n
	}
	if c := s.ch.acct.Charged(); c < want {
		s.t.Fatalf("%s: after [%.1s, %.1s] with key %d: charged %d for %d of held strings", s.name, x, y, key, c, want)
	}
}

// drain reads the rows out, passing over the first skip, and checks the
// top-N then counts no shared string and is charged only its row slots.
func (s *stringTopN) drain(skip uint64) {
	s.t.Helper()
	it := &topNIter{rows: s.top.sorted(), t: s.top}
	it.skip(skip)
	for {
		if _, err := it.next(); err == io.EOF {
			break
		} else if err != nil {
			s.t.Fatal(err)
		}
	}
	slots := sliceHeader * int64(cap(s.top.rows))
	if c := s.ch.acct.Charged(); len(s.top.strs) != 0 || c != slots {
		s.t.Fatalf("%s: drained, %d strings still counted and %d charged, want %d", s.name, len(s.top.strs), c, slots)
	}
}

// A string box two String columns share is counted per box: the column
// that last remembered it cannot count a row that no longer holds it.
func TestTopNSharesAStringAcrossColumnsPerBox(t *testing.T) {
	var p, b, q, r, u any = strings.Repeat("p", 100000), strings.Repeat("b", 1000000),
		strings.Repeat("q", 100000), strings.Repeat("r", 100000), strings.Repeat("t", 100000)
	s := newStringTopN(t, "sequence", 3)
	s.add(p, b, 100)
	s.add(b, q, 99)
	s.add(b, r, 98)
	// Rows of short strings drop all three.
	for key := uint64(97); key > 94; key-- {
		s.add("", "", key)
	}
	s.add(u, b, 94)
	for key := uint64(93); key > 85; key-- {
		s.add(u, b, key)
	}
}

// Random rows over small shared pools: the top-N is never charged less
// than the distinct strings its rows hold, and once its rows are read out
// or skipped it is charged only its row slots.
func TestTopNChargesAtLeastTheStringsItHolds(t *testing.T) {
	for seed := range uint64(300) {
		r := rand.New(rand.NewPCG(seed, 1))
		pool := make([]any, 2+r.IntN(6))
		for i := range pool {
			pool[i] = strings.Repeat(string(rune('a'+i)), 1000)
		}
		keep := uint64(1 + r.IntN(8))
		s := newStringTopN(t, fmt.Sprintf("seed %d", seed), keep)
		for range 100 {
			s.add(pool[r.IntN(len(pool))], pool[r.IntN(len(pool))], r.Uint64N(50))
		}
		skip := uint64(0)
		if r.IntN(2) == 0 {
			skip = r.Uint64N(keep + 2)
		}
		s.drain(skip)
	}
}

// A top-N drops its OFFSET rows at once, releasing what reading them one
// by one releases.
func TestTopNOffsetReleasesTheRowsItSkips(t *testing.T) {
	te := newTestEnv(t)
	for _, key := range []chtype.Type{chtype.TUInt64, chtype.TString} {
		// charged returns what the top-N holds once its first n rows are
		// dropped, skipped or read.
		charged := func(n int, skip bool) int64 {
			q := te.e.newQuery(context.Background(), &Request{})
			ch := newRowCharge(q.mem.NewAccount(), []format.Column{{Type: key}, {Type: chtype.TUInt64}})
			defer ch.acct.Close()
			top := newTopN(newRowOrder(1, []sqlparse.OrderItem{{}}), 1000, ch)
			for i := range 1000 {
				var v any = uint64(i)
				if key.IsString() {
					v = strings.Repeat("x", 100+i)
				}
				if err := top.add([]any{v, uint64(i)}); err != nil {
					t.Fatal(err)
				}
			}
			it := &topNIter{rows: top.sorted(), t: top}
			if skip {
				if got := it.skip(uint64(n)); got != uint64(min(n, 1000)) {
					t.Errorf("%s: skipped %d rows of %d", key, got, n)
				}
				return ch.acct.Charged()
			}
			for range min(n, 1000) {
				if _, err := it.next(); err != nil {
					t.Fatal(err)
				}
			}
			it.release()
			return ch.acct.Charged()
		}
		for _, n := range []int{600, 1000, 1500} {
			if read, skipped := charged(n, false), charged(n, true); read != skipped {
				t.Errorf("%s: %d rows read leave %d charged, skipped %d", key, n, read, skipped)
			}
		}
	}
}

// peakTracked runs sql and returns the most bytes its query tracked.
func peakTracked(te *testEnv, sql string) int64 {
	var peak atomic.Int64
	te.e.impl.testHookStarted = func(q *query) {
		q.mem.SetSampleHook(1, func(used int64) {
			for p := peak.Load(); used > p && !peak.CompareAndSwap(p, used); p = peak.Load() {
			}
		})
	}
	defer func() { te.e.impl.testHookStarted = nil }()
	te.exec(sql)
	return peak.Load()
}

// A sort releases each row as it emits it, so a collected sorted result
// is not charged twice, once to the sort and once to the collect.
func TestCollectedSortIsChargedOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("sorts 300000 rows")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	for _, sql := range []string{
		"SELECT number, toString(number) AS s FROM numbers(300000) ORDER BY s DESC",
		"SELECT number, toString(number) AS s FROM numbers(300000) ORDER BY s DESC LIMIT 200000",
	} {
		sorted := peakTracked(te, "SELECT count() FROM ("+sql+")")
		collected := peakTracked(te, sql)
		t.Logf("%s: sorted %d, collected %d", sql, sorted, collected)
		if float64(collected) > 1.25*float64(sorted) {
			t.Errorf("%s: collecting the sorted rows tracked %d bytes, sorting them %d", sql, collected, sorted)
		}
	}
}

// wantNoTrackedMemory runs req and checks that the query ended holding no
// tracked memory, with an error of code wantErr (0: none).
func wantNoTrackedMemory(t *testing.T, te *testEnv, ctx context.Context, sql string, wantErr int) {
	t.Helper()
	finished := false
	te.e.impl.testHookFinished = func(q *query) {
		finished = true
		if u := q.mem.Used(); u != 0 {
			t.Errorf("%s: finished holding %d tracked bytes", sql, u)
		}
	}
	_, err := te.e.Execute(ctx, &Request{Query: sql})
	if ce := cherr.As(err); (wantErr == 0 && err != nil) || (wantErr != 0 && (ce == nil || ce.Code != wantErr)) {
		t.Errorf("%s: got %v, want code %d", sql, err, wantErr)
	}
	if !finished {
		t.Errorf("%s: the query never finished", sql)
	}
}

// Each query below charges more than an account batches (64 KiB) before it
// stops, so a holder that kept its charge would show.
func TestSortAndKeySetsReleaseWhenQueryStopsEarly(t *testing.T) {
	if testing.Short() {
		t.Skip("fills holders of up to a million rows")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	for _, c := range []struct {
		sql  string
		code int
	}{
		// LIMIT stops reading.
		{sql: "SELECT DISTINCT number FROM numbers(1000000) LIMIT 100000"},
		{sql: "SELECT number FROM numbers(1000000) LIMIT 1 BY number LIMIT 100000"},
		{sql: "SELECT * FROM (SELECT number FROM numbers(1000000) UNION DISTINCT SELECT 1) LIMIT 100000"},
		{sql: "SELECT number FROM numbers(200000) ORDER BY number DESC LIMIT 1 BY number % 7 LIMIT 3"},
		{sql: "SELECT number FROM numbers(200000) ORDER BY number DESC LIMIT 100000, 3"},
		{sql: "SELECT (SELECT number FROM numbers(200000) ORDER BY number DESC)", code: cherr.IncorrectResultOfScalarSubquery},
		// An error while the holder fills, or after it is full.
		{sql: "SELECT number FROM numbers(200000) ORDER BY intDiv(1, toInt64(number) - 150000)", code: cherr.IllegalDivision},
		{sql: "SELECT number FROM numbers(200000) ORDER BY intDiv(1, toInt64(number) - 150000) LIMIT 100000", code: cherr.IllegalDivision},
		{sql: "SELECT intDiv(1, toInt64(x) - 100) FROM (SELECT number AS x FROM numbers(200000) ORDER BY x DESC)", code: cherr.IllegalDivision},
		{sql: "SELECT count() FROM (SELECT DISTINCT number, intDiv(1, toInt64(number) - 150000) FROM numbers(200000))", code: cherr.IllegalDivision},
		{sql: "SELECT count() FROM (SELECT intDiv(1, toInt64(number) - 150000) FROM numbers(200000) LIMIT 1 BY number)", code: cherr.IllegalDivision},
		{sql: "SELECT count() FROM (SELECT toInt64(number) FROM numbers(200000) UNION DISTINCT SELECT intDiv(1, toInt64(number) - 150000) FROM numbers(200000))", code: cherr.IllegalDivision},
		{sql: "SELECT number FROM numbers(1000000) ORDER BY toString(number) SETTINGS max_memory_usage = 10000000", code: cherr.MemoryLimitExceeded},
	} {
		wantNoTrackedMemory(t, te, context.Background(), c.sql, c.code)
	}
}

// Each query reads system.numbers until it is cancelled.
func TestSortAndKeySetsReleaseWhenQueryIsCancelled(t *testing.T) {
	if testing.Short() {
		t.Skip("reads system.numbers until cancelled")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	for _, sql := range []string{
		"SELECT count() FROM (SELECT DISTINCT number % 100000 FROM system.numbers)",
		"SELECT count() FROM (SELECT number FROM system.numbers LIMIT 1 BY number % 100000)",
		"SELECT count() FROM (SELECT number % 100000 FROM system.numbers UNION DISTINCT SELECT 1)",
		"SELECT count() FROM (SELECT number FROM system.numbers ORDER BY number % 1000 LIMIT 100000)",
		// A full sort: the limit only stops it if the cancel comes late.
		"SELECT count() FROM (SELECT number FROM system.numbers ORDER BY number) SETTINGS max_memory_usage = 256000000",
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		wantNoTrackedMemory(t, te, ctx, sql, cherr.Timeout)
		cancel()
	}
}
