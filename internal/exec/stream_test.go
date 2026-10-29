package exec

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// openStream runs sql as a streamed top-level SELECT.
func (te *testEnv) openStream(sql string) ResultStream {
	te.t.Helper()
	return te.openStreamCtx(context.Background(), sql)
}

func (te *testEnv) openStreamCtx(ctx context.Context, sql string) ResultStream {
	te.t.Helper()
	res, err := te.e.Execute(ctx, &Request{Query: sql, Stream: true})
	if err != nil {
		te.t.Fatalf("%s: %v", sql, err)
	}
	if res.Stream == nil {
		te.t.Fatalf("%s: no stream", sql)
	}
	return res.Stream
}

// readStream reads s to its end or first error in blocks of n rows.
func readStream(s ResultStream, n int) (rows [][]any, sizes []int, err error) {
	for {
		block, err := s.Next(n)
		if err == io.EOF {
			return rows, sizes, nil
		}
		if err != nil {
			return rows, sizes, err
		}
		rows = append(rows, block...)
		sizes = append(sizes, len(block))
	}
}

// Next returns at most maxRows rows, and EOF after the last block.
func TestStreamReturnsBlocksOfAtMostMaxRows(t *testing.T) {
	te := newTestEnv(t)
	s := te.openStream("SELECT number FROM numbers(10)")
	defer s.Close()
	rows, sizes, err := readStream(s, 4)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, fmt.Sprint(sizes), "[4 4 2]")
	eq(t, fmt.Sprint(rows[9]), "[9]")
	if _, err := s.Next(4); err != io.EOF {
		t.Fatalf("Next after the end: %v", err)
	}
	st := s.Stats()
	eq(t, st.ResultRows, uint64(10))
	eq(t, st.ReadRows, uint64(10))
}

// Only a top-level SELECT streams; every other statement returns its rows
// as before.
func TestStreamOnlyForTopLevelSelect(t *testing.T) {
	te := newTestEnv(t)
	te.exec("CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n")
	for _, sql := range []string{
		"INSERT INTO t SELECT number FROM numbers(5)",
		"CREATE TABLE t2 ENGINE = MergeTree ORDER BY n AS SELECT n FROM t",
		"SHOW TABLES",
		"DESCRIBE TABLE t",
		"EXISTS TABLE t",
	} {
		res, err := te.runReq(&Request{Query: sql, Stream: true})
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if res.Stream != nil {
			res.Stream.Close()
			t.Errorf("%s: streamed", sql)
		}
	}
	if res := te.exec("SELECT n FROM t2 ORDER BY n"); res.Stream != nil || len(res.Rows) != 5 {
		t.Errorf("SELECT without Stream: stream %v, %d rows", res.Stream, len(res.Rows))
	}
}

// A streamed result has the rows and counts of the collected one.
func TestStreamMatchesCollectedResult(t *testing.T) {
	te := newTestEnv(t)
	te.exec("CREATE TABLE t (k String, v UInt64) ENGINE = MergeTree ORDER BY k")
	te.exec("INSERT INTO t SELECT toString(number % 7), number FROM numbers(1000)")
	for _, sql := range []string{
		"SELECT number FROM numbers(100) WHERE number % 3 = 0 LIMIT 5 OFFSET 2",
		"SELECT k, sum(v) FROM t GROUP BY k ORDER BY k",
		"SELECT k, v FROM t ORDER BY v DESC LIMIT 10",
		"SELECT DISTINCT k FROM t ORDER BY k",
		"SELECT v FROM t WHERE k = '3' LIMIT 2 BY k",
		"SELECT arrayJoin([1, 2, 3]) AS x, k FROM t WHERE v < 5 ORDER BY x, k",
		"SELECT a.number, b.number FROM numbers(30) AS a JOIN numbers(30) AS b ON a.number = b.number * 2 ORDER BY 1",
		"SELECT number FROM numbers(5) UNION ALL SELECT number FROM numbers(3)",
		"SELECT count() FROM t",
		"SELECT number FROM numbers(1000) LIMIT 0",
	} {
		want := te.exec(sql)
		s := te.openStream(sql)
		rows, _, err := readStream(s, 7)
		st := s.Stats()
		s.Close()
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if len(rows) != len(want.Rows) || (len(rows) > 0 && !reflect.DeepEqual(rows, want.Rows)) {
			t.Errorf("%s: rows %v, want %v", sql, rows, want.Rows)
		}
		got := Stats{ReadRows: st.ReadRows, ReadBytes: st.ReadBytes, ResultRows: st.ResultRows,
			RowsBeforeLimit: st.RowsBeforeLimit, HasRowsBeforeLimit: st.HasRowsBeforeLimit}
		wantSt := want.Counts()
		wantSt.PeakMemory = 0
		if got != wantSt {
			t.Errorf("%s: stats %+v, want %+v", sql, got, wantSt)
		}
	}
}

// A block also ends once its rows hold about 1 MB.
func TestStreamEndsBlocksAtAboutOneMB(t *testing.T) {
	te := newTestEnv(t)
	s := te.openStream("SELECT concat(toString(number), repeat('x', 100000)) FROM numbers(30)")
	defer s.Close()
	_, sizes, err := readStream(s, 1000)
	if err != nil {
		t.Fatal(err)
	}
	// A row of a new 100 kB string is estimated at about 106 kB, so a
	// block ends at the row that takes it past 1 MB.
	eq(t, fmt.Sprint(sizes), "[10 10 10]")
}

// The stream holds one block at a time: a result many times the limit
// streams within it, while collecting it fails.
func TestStreamChargesOnlyItsCurrentBlock(t *testing.T) {
	guardFunctionTest(t)
	if testing.Short() {
		t.Skip("heavy: skipped with -short")
	}
	const limit = 4_000_000
	te := newMemoryEnv(t, limit, 0)
	const sql = "SELECT repeat(toString(number), 5000) FROM numbers(4000)" // about 20 MB
	_, err := te.run(sql)
	wantCode(t, "collected", err, cherr.MemoryLimitExceeded, "")

	var q *query
	te.e.impl.testHookStarted = func(started *query) { q = started }
	s := te.openStream(sql)
	defer s.Close()
	maxUsed, n := int64(0), 0
	for {
		block, err := s.Next(1000)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		n += len(block)
		maxUsed = max(maxUsed, q.mem.Used())
	}
	eq(t, n, 4000)
	if maxUsed == 0 || maxUsed > 1_200_000 {
		t.Errorf("tracked %d bytes while holding a block, want about 1 MB", maxUsed)
	}
	if p := s.Stats().PeakMemory; p < maxUsed {
		t.Errorf("peak %d below the %d bytes seen", p, maxUsed)
	}
}

// max_result_rows counts the rows read in whole blocks, as ClickHouse 25.8
// does (pinned against it with max_block_size=10 over numbers(1000)).
func TestStreamMaxResultRowsCountsBlocks(t *testing.T) {
	guardFunctionTest(t)
	te := newTestEnv(t)
	for _, c := range []struct {
		settings string
		rows     int
		err      string
	}{
		{settings: "max_result_rows = 5, result_overflow_mode = 'break'", rows: 10},
		{settings: "max_result_rows = 9, result_overflow_mode = 'break'", rows: 10},
		{settings: "max_result_rows = 10, result_overflow_mode = 'break'", rows: 10},
		{settings: "max_result_rows = 11, result_overflow_mode = 'break'", rows: 20},
		{settings: "max_result_rows = 15, result_overflow_mode = 'break'", rows: 20},
		{settings: "max_result_rows = 5", err: "Limit for result exceeded, max rows: 5.00, current rows: 10.00"},
		{settings: "max_result_rows = 10", rows: 10, err: "Limit for result exceeded, max rows: 10.00, current rows: 20.00"},
		{settings: "max_result_rows = 100", rows: 100, err: "Limit for result exceeded, max rows: 100.00, current rows: 110.00"},
		{settings: "max_result_rows = 1000", rows: 1000},
	} {
		s := te.openStream("SELECT number FROM numbers(1000) SETTINGS " + c.settings)
		rows, _, err := readStream(s, 10)
		s.Close()
		if len(rows) != c.rows {
			t.Errorf("%s: %d rows, want %d", c.settings, len(rows), c.rows)
		}
		if c.err == "" && err != nil {
			t.Errorf("%s: %v", c.settings, err)
		}
		if c.err != "" {
			wantCode(t, c.settings, err, cherr.TooManyRowsOrBytes, c.err)
		}
	}
	s := te.openStream("SELECT number FROM system.numbers SETTINGS max_result_rows = 1000")
	defer s.Close()
	_, err := s.Next(5000)
	wantCode(t, "large block", err, cherr.TooManyRowsOrBytes, "Limit for result exceeded, max rows: 1.00 thousand, current rows: 5.00 thousand")
}

// A block ends with the source block its rows come from, as in
// ClickHouse, which filters each source block of max_block_size rows into
// one result block: with blocks of 10, number % 2 = 0 gives blocks of 5.
// The break counts are 25.8.33.6's over numbers(1000) with
// max_block_size = 10 (number % 7 = 0: one or two rows per block).
func TestStreamBlocksFollowTheSourceBlocks(t *testing.T) {
	te := newTestEnv(t)
	s := te.openStream("SELECT number FROM numbers(100) WHERE number % 2 = 0")
	_, sizes, err := readStream(s, 10)
	s.Close()
	if err != nil {
		t.Fatal(err)
	}
	eq(t, fmt.Sprint(sizes), "[5 5 5 5 5 5 5 5 5 5]")
	s = te.openStream("SELECT number FROM numbers(100) WHERE number % 7 = 0")
	_, sizes, err = readStream(s, 10)
	s.Close()
	if err != nil {
		t.Fatal(err)
	}
	eq(t, fmt.Sprint(sizes), "[2 1 2 1 2 1 1 2 1 2]")
	for _, c := range []struct {
		filter    string
		max, rows int
	}{
		{"number % 2 = 0", 4, 5}, {"number % 2 = 0", 6, 10},
		{"number % 7 = 0", 3, 3}, {"number % 7 = 0", 4, 5}, {"number % 7 = 0", 6, 6},
		{"number % 13 = 0", 2, 2}, {"number % 100 = 0", 1, 1},
	} {
		sql := fmt.Sprintf("SELECT number FROM numbers(1000) WHERE %s SETTINGS max_result_rows = %d, result_overflow_mode = 'break'", c.filter, c.max)
		s := te.openStream(sql)
		rows, _, err := readStream(s, 10)
		s.Close()
		if err != nil || len(rows) != c.rows {
			t.Errorf("%s: %d rows (%v), want %d", sql, len(rows), err, c.rows)
		}
	}
}

// A selective filter over a large source returns its first block once a
// later row passes, not after max_block_size rows have passed: here after
// about 100 thousand rows read, where 65409 matches would take 6.5
// billion.
func TestStreamSelectiveFilterReturnsItsFirstBlockPromptly(t *testing.T) {
	te := newTestEnv(t)
	s := te.openStream("SELECT number FROM system.numbers WHERE number % 100000 = 0")
	defer s.Close()
	for i, want := range []string{"[[0]]", "[[100000]]", "[[200000]]"} {
		block, err := s.Next(65409)
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(block) != want {
			t.Fatalf("block %d: %v, want %s", i, block, want)
		}
	}
	if read := s.Stats().ReadRows; read > 400000 {
		t.Errorf("read %d rows for three blocks", read)
	}
}

// rows_before_limit counts a sort's or GROUP BY's whole input however
// early the result ends, so also with result_overflow_mode = 'break'
// (values from 25.8.33.6 with max_result_rows = 5 and blocks of 10).
func TestStreamRowsBeforeLimitWithBreak(t *testing.T) {
	te := newTestEnv(t)
	const brk = " SETTINGS max_result_rows = 5, result_overflow_mode = 'break'"
	for _, c := range []struct {
		sql  string
		want uint64
	}{
		{sql: "SELECT number FROM numbers(100) ORDER BY number LIMIT 50", want: 100},
		{sql: "SELECT number FROM numbers(100) ORDER BY number LIMIT 20, 50", want: 100},
		{sql: "SELECT number % 50 AS k FROM numbers(1000) GROUP BY k LIMIT 40", want: 50},
		{sql: "SELECT number % 50 AS k, count() FROM numbers(1000) GROUP BY k LIMIT 40 OFFSET 3", want: 50},
		{sql: "SELECT number % 50 AS k FROM numbers(1000) GROUP BY k ORDER BY k LIMIT 40", want: 50},
		{sql: "SELECT DISTINCT number % 50 AS k FROM numbers(1000) LIMIT 40", want: 10},
		{sql: "SELECT number FROM numbers(100) LIMIT 50", want: 10},
		{sql: "SELECT number FROM numbers(100) ORDER BY number LIMIT 1 BY number % 30 LIMIT 25", want: 10},
	} {
		s := te.openStream(c.sql + brk)
		rows, _, err := readStream(s, 10)
		st := s.Stats()
		s.Close()
		if err != nil || len(rows) != 10 || !st.HasRowsBeforeLimit || st.RowsBeforeLimit != c.want {
			t.Errorf("%s: %d rows, rows_before_limit %d (%v), want %d; %v", c.sql, len(rows), st.RowsBeforeLimit, st.HasRowsBeforeLimit, c.want, err)
		}
	}
}

// max_result_bytes counts ClickHouse's column bytes per block of 10 rows;
// the bytes are those 25.8.33.6 reported for each query.
func TestStreamMaxResultBytesCountsBlocks(t *testing.T) {
	te := newTestEnv(t)
	for _, c := range []struct {
		expr    string
		rows    int
		current string
	}{
		{expr: "number", rows: 10, current: "160.00 B"},
		{expr: "toString(number)", rows: 10, current: "190.00 B"},
		{expr: "toString(number * 1000)", current: "117.00 B"},
		{expr: "toNullable(number)", rows: 10, current: "180.00 B"},
		{expr: "[number, number]", current: "240.00 B"},
		{expr: "toUInt8(number)", rows: 100, current: "110.00 B"},
		{expr: "(number, 'x')", current: "170.00 B"},
		{expr: "toDateTime(number)", rows: 20, current: "120.00 B"},
		{expr: "number, toString(number)", current: "170.00 B"},
		{expr: "map('a', number)", current: "250.00 B"},
	} {
		s := te.openStream("SELECT " + c.expr + " FROM numbers(1000) SETTINGS max_result_bytes = 100")
		rows, _, err := readStream(s, 10)
		s.Close()
		if len(rows) != c.rows {
			t.Errorf("%s: %d rows, want %d", c.expr, len(rows), c.rows)
		}
		wantCode(t, c.expr, err, cherr.TooManyRowsOrBytes, "Limit for result exceeded, max bytes: 100.00 B, current bytes: "+c.current)
	}
	s := te.openStream("SELECT number FROM numbers(1000) SETTINGS max_result_bytes = 100, result_overflow_mode = 'break'")
	defer s.Close()
	rows, _, err := readStream(s, 10)
	if len(rows) != 20 || err != nil {
		t.Errorf("break: %d rows, %v", len(rows), err)
	}
}

// readableQuantity renders counts as ClickHouse's formatReadableQuantity.
func TestReadableQuantity(t *testing.T) {
	for n, want := range map[uint64]string{
		0: "0.00", 5: "5.00", 999: "999.00", 1000: "1.00 thousand", 65409: "65.41 thousand",
		1e6: "1.00 million", 1e9: "1.00 billion", 1e12: "1.00 trillion", 1e15: "1.00 quadrillion",
		1e18: "1.00 quintillion", 1<<64 - 1: "18.45 quintillion",
	} {
		if got := readableQuantity(n); got != want {
			t.Errorf("readableQuantity(%d) = %q, want %q", n, got, want)
		}
	}
}

// A failed or abandoned stream finishes its query once, when it is
// closed, and releases everything (the test engine checks that a finished
// query holds no tracked memory).
func TestStreamReleasesItsMemoryOnEveryEnd(t *testing.T) {
	guardFunctionTest(t)
	te := newMemoryEnv(t, 64_000_000, 0)
	const failing = "intDiv(1, toInt64(number) - 2500)"
	for _, c := range []struct {
		sql    string
		blocks int
		code   int
	}{
		{sql: "SELECT " + failing + " FROM numbers(10000)", blocks: -1, code: 153},
		{sql: "SELECT number, " + failing + " FROM (SELECT number FROM numbers(10000) ORDER BY number)", blocks: -1, code: 153},
		{sql: "SELECT number, toString(number) FROM numbers(100000) ORDER BY number DESC", blocks: 2},
		{sql: "SELECT number % 1000 AS k, groupArray(number) FROM numbers(100000) GROUP BY k", blocks: 1},
		{sql: "SELECT a.number FROM numbers(1000) AS a JOIN numbers(1000) AS b ON a.number = b.number", blocks: 1},
		{sql: "SELECT number FROM system.numbers", blocks: 3},
		{sql: "SELECT number FROM system.numbers SETTINGS max_result_rows = 20000, result_overflow_mode = 'break'", blocks: -1},
		{sql: "SELECT number FROM numbers(10)", blocks: 0},
	} {
		finished := te.finished.Load()
		s := te.openStream(c.sql)
		var err error
		for i := 0; c.blocks < 0 || i < c.blocks; i++ {
			if _, err = s.Next(1000); err != nil {
				break
			}
		}
		s.Close()
		s.Close()
		if n := te.finished.Load() - finished; n != 1 {
			t.Errorf("%s: closing the stream finished the query %d times", c.sql, n)
		}
		if c.code != 0 {
			wantCode(t, c.sql, err, c.code, "")
		} else if err != nil && err != io.EOF {
			t.Errorf("%s: %v", c.sql, err)
		}
	}
}

// After an error the stream keeps returning it.
func TestStreamErrorIsSticky(t *testing.T) {
	te := newTestEnv(t)
	s := te.openStream("SELECT intDiv(1, toInt64(number) - 25) FROM numbers(100)")
	defer s.Close()
	rows, _, err := readStream(s, 10)
	wantCode(t, "first", err, 153, "")
	eq(t, len(rows), 20)
	_, err = s.Next(10)
	wantCode(t, "again", err, 153, "")
}

// panicIter panics on its first row.
type panicIter struct{ calls int }

func (p *panicIter) next() ([]any, error) {
	p.calls++
	panic("broken iterator")
}

func (p *panicIter) close() {}

// After a panic the stream keeps failing without reading on.
func TestStreamPanicIsSticky(t *testing.T) {
	te := newTestEnv(t)
	s := te.openStream("SELECT number FROM numbers(100)").(*resultStream)
	defer s.Close()
	// The plan is opened by the first Next; the stream reads it instead.
	it := &panicIter{}
	s.it = it
	_, err := s.Next(10)
	wantCode(t, "first", err, cherr.LogicalError, "internal error: broken iterator")
	_, err = s.Next(10)
	wantCode(t, "again", err, cherr.LogicalError, "internal error: broken iterator")
	eq(t, it.calls, 1)
}

// registerPanicAt adds panicAt(x, n) for the test: x, or a panic once x
// is n.
func registerPanicAt(t *testing.T) {
	funcs["panicAt"] = func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 2, 2); err != nil {
			return nil, err
		}
		x, n := args[0], args[1]
		return &expr{t: x.t, volatile: true, fn: func(row []any) (any, error) {
			v, err := x.eval(row)
			if err == nil && v == n.val {
				panic("injected")
			}
			return v, err
		}}, nil
	}
	t.Cleanup(func() { delete(funcs, "panicAt") })
}

// A panic while the first Next opens the plan (a sort, top-N, GROUP BY or
// JOIN build reading its input) releases what was charged.
func TestStreamPanicWhileOpeningReleasesItsMemory(t *testing.T) {
	registerPanicAt(t)
	te := newMemoryEnv(t, 1<<40, 0)
	const p = "panicAt(number, toUInt64(1500))"
	for _, sql := range []string{
		"SELECT " + p + " AS n FROM numbers(3000) ORDER BY n DESC",
		"SELECT " + p + " AS n FROM numbers(3000) ORDER BY n DESC LIMIT 100",
		"SELECT " + p + " % 100 AS k, groupArray(number) FROM numbers(3000) GROUP BY k",
		"SELECT DISTINCT " + p + " AS n FROM numbers(3000) ORDER BY n",
		"SELECT a.number FROM numbers(3000) AS a JOIN (SELECT " + p + " AS n FROM numbers(3000)) AS b ON a.number = b.n",
	} {
		finished := te.finished.Load()
		s := te.openStream(sql)
		_, err := s.Next(1000)
		wantCode(t, sql, err, cherr.LogicalError, "internal error: injected")
		s.Close()
		if n := te.finished.Load() - finished; n != 1 {
			t.Errorf("%s: closing the stream finished the query %d times", sql, n)
		}
	}
}

// A cancelled query's stream fails at its next block.
func TestStreamStopsWhenTheContextEnds(t *testing.T) {
	guardFunctionTest(t)
	te := newTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	s := te.openStreamCtx(ctx, "SELECT number FROM system.numbers")
	defer s.Close()
	if _, err := s.Next(100); err != nil {
		t.Fatal(err)
	}
	cancel()
	_, err := s.Next(100)
	wantCode(t, "cancelled", err, cherr.Timeout, "")
}

// What the caller charges counts against the query's limit.
func TestStreamChargeCountsAgainstTheLimit(t *testing.T) {
	te := newMemoryEnv(t, 2_000_000, 0)
	s := te.openStream("SELECT 1")
	defer s.Close()
	if err := s.Charge(1_000_000); err != nil {
		t.Fatal(err)
	}
	err := s.Charge(1_500_000)
	wantCode(t, "over the limit", err, cherr.MemoryLimitExceeded, "")
	if !memoryLimitMsg.MatchString(cherr.As(err).Msg) {
		t.Errorf("message %q", cherr.As(err).Msg)
	}
}

// An unlimited query charges nothing but still ends blocks by size.
func TestStreamWithoutLimitHasNoPeak(t *testing.T) {
	te := newMemoryEnv(t, 0, 0)
	s := te.openStream("SELECT concat(toString(number), repeat('x', 300000)) FROM numbers(10)")
	defer s.Close()
	_, sizes, err := readStream(s, 100)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, fmt.Sprint(sizes), "[4 4 2]")
	eq(t, s.Stats().PeakMemory, int64(0))
	if err := s.Charge(1 << 40); err != nil {
		t.Fatal(err)
	}
}

// The block's rows are not kept once the next block is read.
func TestStreamDropsTheLastBlock(t *testing.T) {
	te := newTestEnv(t)
	s := te.openStream("SELECT toString(number) FROM numbers(10)")
	defer s.Close()
	first, err := s.Next(8)
	if err != nil {
		t.Fatal(err)
	}
	all := first[:cap(first)]
	if _, err := s.Next(8); err != nil {
		t.Fatal(err)
	}
	for i, r := range all[2:] {
		if r != nil {
			t.Fatalf("slot %d still holds %v", i+2, r)
		}
	}
	if got := fmt.Sprint(all[:2]); !strings.Contains(got, "[8] [9]") {
		t.Errorf("second block %s", got)
	}
}

// An error while the plan reads its input (here a sort's) comes from
// Next, after the output is set up, as an execution error in ClickHouse.
func TestStreamRaisesExecutionErrorsFromNext(t *testing.T) {
	te := newTestEnv(t)
	for _, sql := range []string{
		"SELECT number, intDiv(1, toInt64(number) - 5) FROM numbers(10) ORDER BY number",
		"SELECT intDiv(1, toInt64(number) - 5) AS k, count() FROM numbers(10) GROUP BY k",
	} {
		s := te.openStream(sql)
		_, err := s.Next(10)
		wantCode(t, sql, err, 153, "")
		s.Close()
	}
}

// With timeout_overflow_mode = 'break' the stream ends at the deadline
// after the rows read so far; with throw it fails.
func TestStreamTimeoutBreakEndsTheStream(t *testing.T) {
	guardFunctionTest(t)
	te := newTestEnv(t)
	run := func(mode string) (int, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		s := te.openStreamCtx(ctx, "SELECT number FROM system.numbers SETTINGS timeout_overflow_mode = '"+mode+"'")
		defer s.Close()
		rows, _, err := readStream(s, 1000)
		return len(rows), err
	}
	if n, err := run("break"); err != nil || n == 0 {
		t.Errorf("break: %d rows, %v", n, err)
	}
	_, err := run("throw")
	wantCode(t, "throw", err, cherr.Timeout, "")
}
