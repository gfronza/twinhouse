package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec"
	"github.com/gfronza/twinhouse/internal/format"
	"github.com/gfronza/twinhouse/internal/memtrack"
	"github.com/gfronza/twinhouse/internal/storage/memstore"
)

// fakeStream is a scriptable exec.ResultStream: it yields its blocks, then
// returns end() (io.EOF when unset).
type fakeStream struct {
	blocks [][][]any
	end    func() error
	// live are the stats before the end, final those after io.EOF.
	live, final exec.Stats
	acct        *memtrack.Account
	eof         bool
	closed      atomic.Bool
}

func (f *fakeStream) Next(int) ([][]any, error) {
	if len(f.blocks) > 0 {
		b := f.blocks[0]
		f.blocks = f.blocks[1:]
		return b, nil
	}
	if f.end != nil {
		return nil, f.end()
	}
	f.eof = true
	return nil, io.EOF
}

func (f *fakeStream) Stats() exec.Stats {
	if f.eof {
		return f.final
	}
	return f.live
}

func (f *fakeStream) Charge(n int64) error {
	if n < 0 {
		f.acct.Shrink(-n)
		return nil
	}
	return f.acct.Grow(n)
}

func (f *fakeStream) Close() {
	f.acct.Close()
	f.closed.Store(true)
}

var numberColumn = []format.Column{{Name: "n", Type: chtype.TUInt64}}

// streamExec answers every query with the stream newStream returns, and
// keeps the last one.
func streamExec(newStream func(ctx context.Context) *fakeStream) (*fakeExec, *atomic.Pointer[fakeStream]) {
	last := &atomic.Pointer[fakeStream]{}
	return &fakeExec{fn: func(ctx context.Context, req *exec.Request) (*exec.Result, error) {
		if !req.Stream {
			return nil, fmt.Errorf("the server did not ask for a stream")
		}
		s := newStream(ctx)
		last.Store(s)
		return &exec.Result{Columns: numberColumn, Stream: s}, nil
	}}, last
}

// numberBlocks returns n blocks of size UInt64 rows, counting from 0.
func numberBlocks(n, size int) [][][]any {
	var out [][][]any
	for b := range n {
		block := make([][]any, size)
		for i := range block {
			block[i] = []any{uint64(b*size + i)}
		}
		out = append(out, block)
	}
	return out
}

var errDivision = cherr.New(cherr.IllegalDivision, "Division by zero")

// failingAfter is a stream of blocks of 3 rows that then fails with 153.
func failingAfter(blocks int) func(context.Context) *fakeStream {
	return func(context.Context) *fakeStream {
		return &fakeStream{blocks: numberBlocks(blocks, 3), end: func() error { return errDivision }}
	}
}

// wantClosed waits for the last stream to be closed.
func wantClosed(t *testing.T, last *atomic.Pointer[fakeStream]) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for s := last.Load(); s == nil || !s.closed.Load(); s = last.Load() {
		if time.Now().After(deadline) {
			t.Fatal("the stream was not closed")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

const divisionText = "Code: 153. DB::Exception: Division by zero. (ILLEGAL_DIVISION) (version 25.8.1.1)"

// An error before the response is committed is an HTTP error in the
// result's format, as ClickHouse 25.8 sends it: the format's Content-Type,
// JSON with the result's meta and no rows, and the rows read so far in the
// summary.
func TestStreamErrorBeforeCommitIsAnHTTPError(t *testing.T) {
	fe, last := streamExec(func(context.Context) *fakeStream {
		return &fakeStream{blocks: numberBlocks(2, 3), live: exec.Stats{ReadRows: 6, ReadBytes: 48}, end: func() error { return errDivision }}
	})
	_, ts := newTestServer(t, fe, nil)
	jsonBody := "{\n\t\"meta\":\n\t[\n\t\t{\n\t\t\t\"name\": \"n\",\n\t\t\t\"type\": \"UInt64\"\n\t\t}\n\t],\n\n\t\"data\":\n\t[\n\n\t],\n\n\t\"rows\": 0,\n\n\t\"exception\": \"" + divisionText + "\"\n}\n"
	for _, c := range []struct{ format, ctype, body string }{
		{"TSV", "text/tab-separated-values; charset=UTF-8", divisionText + "\n"},
		{"CSV", "text/csv; charset=UTF-8; header=absent", divisionText + "\n"},
		{"Values", "text/plain; charset=UTF-8", divisionText + "\n"},
		{"JSON", "application/json; charset=UTF-8", jsonBody},
		{"JSONEachRow", "application/x-ndjson; charset=UTF-8", "{\"exception\": \"" + divisionText + "\"}\n"},
		{"JSONCompactEachRow", "text/plain; charset=UTF-8", "[\"" + divisionText + "\"]\n"},
	} {
		r := post(t, ts.URL+"/?default_format="+c.format, "SELECT n")
		if r.Status != 500 || r.Header.Get("X-ClickHouse-Exception-Code") != "153" || r.Body != c.body ||
			r.Header.Get("Content-Type") != c.ctype || r.Header.Get("X-ClickHouse-Format") != c.format {
			t.Errorf("%s: %d %v %q", c.format, r.Status, r.Header, r.Body)
		}
		if s := r.Header.Get("X-ClickHouse-Summary"); !strings.HasPrefix(s, `{"read_rows":"6","read_bytes":"48","written_rows":"0","written_bytes":"0","total_rows_to_read":"6","result_rows":"0","result_bytes":"0"`) {
			t.Errorf("%s: summary %s", c.format, s)
		}
		wantClosed(t, last)
	}
}

// After the response is committed, an error ends the body as ClickHouse
// 25.8 does (probed with a failing row past the first block): the JSON
// formats carry the exception and end normally; the others end with the
// __exception__ marker and the transfer is cut off.
func TestStreamErrorAfterCommitEndsTheBody(t *testing.T) {
	fe, last := streamExec(failingAfter(2))
	_, ts := newTestServer(t, fe, func(c *Config) { c.ResponseBufferSize = 1 })
	cases := []struct {
		format, want string
		cut          bool
	}{
		{"TSV", "0\n1\n2\n3\n4\n5\n__exception__\r\n" + divisionText + "\n", true},
		{"CSV", "0\n1\n2\n3\n4\n5\n__exception__\r\n" + divisionText + "\n", true},
		{"JSONEachRow", "{\"n\":0}\n{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n{\"n\":4}\n{\"n\":5}\n{\"exception\": \"" + divisionText + "\"}\n", false},
		{"JSONCompactEachRow", "[0]\n[1]\n[2]\n[3]\n[4]\n[5]\n[\"" + divisionText + "\"]\n", false},
	}
	for _, c := range cases {
		r := post(t, ts.URL+"/?default_format="+c.format, "SELECT n")
		if r.Status != 200 || r.Body != c.want || r.Cut != c.cut {
			t.Errorf("%s: %d %q cut %v, want %q cut %v", c.format, r.Status, r.Body, r.Cut, c.want, c.cut)
		}
		wantClosed(t, last)
	}
	r := post(t, ts.URL+"/?default_format=JSONEachRow&output_format_json_array_of_rows=1", "SELECT n")
	if want := "[\n{\"n\":0},\n{\"n\":1},\n{\"n\":2},\n{\"n\":3},\n{\"n\":4},\n{\"n\":5},\n{\"exception\": \"" + divisionText + "\"}\n]\n"; r.Body != want || r.Cut {
		t.Errorf("array of rows: %q cut %v, want %q", r.Body, r.Cut, want)
	}
}

// With HTTP compression a format without an in-format exception is cut
// off without the text, as ClickHouse 25.8 does; JSON ends its stream
// with the exception inside.
func TestStreamErrorAfterCommitWithHTTPCompression(t *testing.T) {
	fe, _ := streamExec(failingAfter(2))
	_, ts := newTestServer(t, fe, func(c *Config) { c.ResponseBufferSize = 1 })
	gz := func(format string) (text string, cut bool, err error) {
		r := post(t, ts.URL+"/?enable_http_compression=1&default_format="+format, "SELECT n", "Accept-Encoding", "gzip")
		zr, err := gzip.NewReader(strings.NewReader(r.Body))
		if err != nil {
			return "", r.Cut, err
		}
		b, err := io.ReadAll(zr)
		return string(b), r.Cut, err
	}
	text, cut, err := gz("TSV")
	if !cut || err == nil || strings.Contains(text, "__exception__") {
		t.Errorf("TSV: cut %v, %v, %q", cut, err, text)
	}
	text, cut, err = gz("JSONCompactEachRow")
	if cut || err != nil || !strings.HasSuffix(text, "[5]\n[\""+divisionText+"\"]\n") {
		t.Errorf("JSONCompactEachRow: cut %v, %v, %q", cut, err, text)
	}
}

// JSON closes the data and adds the exception, without statistics.
func TestStreamErrorAfterCommitInJSON(t *testing.T) {
	fe, _ := streamExec(failingAfter(1))
	_, ts := newTestServer(t, fe, func(c *Config) { c.ResponseBufferSize = 1 })
	r := post(t, ts.URL+"/?default_format=JSONCompact", "SELECT n")
	want := "{\n\t\"meta\":\n\t[\n\t\t{\n\t\t\t\"name\": \"n\",\n\t\t\t\"type\": \"UInt64\"\n\t\t}\n\t],\n\n\t\"data\":\n\t[\n" +
		"\t\t[0],\n\t\t[1],\n\t\t[2]\n\t],\n\n\t\"rows\": 3,\n\n\t\"exception\": \"" + divisionText + "\"\n}\n"
	if r.Status != 200 || r.Body != want {
		t.Fatalf("%d %q, want %q", r.Status, r.Body, want)
	}
	var v map[string]any
	if err := json.Unmarshal([]byte(r.Body), &v); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
}

// With compress=1 each block ends a frame, as in ClickHouse 25.8 (a frame
// holds at most 1 MiB, so each block of 200000 rows takes two), and the
// exception is a frame of its own after the rows (the rows must pass the
// 1 MiB buffer for the response to be committed before the error).
func TestStreamErrorAfterCommitCompressed(t *testing.T) {
	fe, _ := streamExec(func(context.Context) *fakeStream {
		return &fakeStream{blocks: numberBlocks(2, 200000), end: func() error { return errDivision }}
	})
	_, ts := newTestServer(t, fe, nil)
	r := post(t, ts.URL+"/?compress=1", "SELECT n")
	frames := nativeFrames(t, []byte(r.Body))
	if r.Status != 200 || len(frames) != 5 {
		t.Fatalf("status %d, %d frames", r.Status, len(frames))
	}
	if first := readNative(t, bytes.Join(frames[:2], nil)); !strings.HasPrefix(first, "0\n1\n") || !strings.HasSuffix(first, "\n199999\n") {
		t.Errorf("first block's frames end %q", first[max(0, len(first)-20):])
	}
	if rows := readNative(t, bytes.Join(frames[2:4], nil)); !strings.HasPrefix(rows, "200000\n") || !strings.HasSuffix(rows, "\n399999\n") {
		t.Errorf("second block's frames end %q", rows[max(0, len(rows)-20):])
	}
	if got := readNative(t, frames[4]); got != "__exception__\r\n"+divisionText+"\n" || !r.Cut {
		t.Errorf("exception frame %q, cut %v", got, r.Cut)
	}
}

// With compress=1 each block is a frame of its own, as ClickHouse 25.8
// frames it (numbers(30) in blocks of 10: frames of 20, 30 and 30 bytes).
func TestCompressedBodyHasAFramePerBlock(t *testing.T) {
	fe, _ := streamExec(func(context.Context) *fakeStream {
		return &fakeStream{blocks: numberBlocks(3, 10)}
	})
	_, ts := newTestServer(t, fe, nil)
	r := post(t, ts.URL+"/?compress=1", "SELECT n")
	var sizes []int
	for _, f := range nativeFrames(t, []byte(r.Body)) {
		sizes = append(sizes, len(readNative(t, f)))
	}
	if r.Status != 200 || fmt.Sprint(sizes) != "[20 30 30]" {
		t.Errorf("status %d, frame sizes %v", r.Status, sizes)
	}
}

// nativeFrames splits a compress=1 body into its frames.
func nativeFrames(t *testing.T, b []byte) [][]byte {
	t.Helper()
	var out [][]byte
	for len(b) > 0 {
		if len(b) < nativeFrameHeader {
			t.Fatalf("%d trailing bytes", len(b))
		}
		n := nativeChecksumSize + int(binary.LittleEndian.Uint32(b[nativeChecksumSize+1:]))
		out = append(out, b[:n])
		b = b[n:]
	}
	return out
}

// A stream that reaches max_execution_time after the commit ends the body
// with 159 and is closed.
func TestStreamTimeoutAfterCommit(t *testing.T) {
	fe, last := streamExec(func(ctx context.Context) *fakeStream {
		return &fakeStream{blocks: numberBlocks(1, 2), end: func() error {
			<-ctx.Done()
			return cherr.Wrap(cherr.Unavailable, ctx.Err(), "cannot query DynamoDB table")
		}}
	})
	_, ts := newTestServer(t, fe, func(c *Config) { c.ResponseBufferSize = 1 })
	r := post(t, ts.URL+"/?max_execution_time=0.2", "SELECT n")
	head, exc, _ := strings.Cut(r.Body, "__exception__\r\n")
	if r.Status != 200 || !r.Cut || head != "0\n1\n" || !strings.HasPrefix(exc, "Code: 159. DB::Exception: Timeout exceeded: elapsed ") ||
		!strings.HasSuffix(exc, " ms, maximum: 200 ms. (TIMEOUT_EXCEEDED) (version 25.8.1.1)\n") {
		t.Errorf("%d %q", r.Status, r.Body)
	}
	wantClosed(t, last)
}

// The statistics and a whole-body summary have the stream's final counts;
// an early commit sends the rows read so far and no result counts, as
// ClickHouse does.
func TestStreamCountsComeFromItsStats(t *testing.T) {
	newStream := func(context.Context) *fakeStream {
		return &fakeStream{
			blocks: numberBlocks(1, 3),
			live:   exec.Stats{ReadRows: 7, ReadBytes: 56, ResultRows: 3},
			final:  exec.Stats{ReadRows: 100, ReadBytes: 800, ResultRows: 3, RowsBeforeLimit: 50, HasRowsBeforeLimit: true},
		}
	}
	fe, _ := streamExec(newStream)
	_, ts := newTestServer(t, fe, nil)
	r := post(t, ts.URL+"/?default_format=JSONCompact&wait_end_of_query=1", "SELECT n")
	for _, want := range []string{"\"rows\": 3,", "\"rows_before_limit_at_least\": 50,", "\"rows_read\": 100,", "\"bytes_read\": 800\n"} {
		if !strings.Contains(r.Body, want) {
			t.Errorf("JSON lacks %q: %s", want, r.Body)
		}
	}
	if s := r.Header.Get("X-ClickHouse-Summary"); !strings.HasPrefix(s, `{"read_rows":"100","read_bytes":"800","written_rows":"0","written_bytes":"0","total_rows_to_read":"100","result_rows":"3","result_bytes":"`+fmt.Sprint(len(r.Body))+`"`) {
		t.Errorf("final summary %s", s)
	}

	_, early := newTestServer(t, fe, func(c *Config) { c.ResponseBufferSize = 1 })
	r = post(t, early.URL+"/", "SELECT n")
	if s := r.Header.Get("X-ClickHouse-Summary"); !strings.HasPrefix(s, `{"read_rows":"7","read_bytes":"56","written_rows":"0","written_bytes":"0","total_rows_to_read":"7","result_rows":"0","result_bytes":"0"`) {
		t.Errorf("early summary %s", s)
	}
}

// A row the encoder rejects ends the query, and the stream is closed.
func TestStreamClosedOnEncoderFailure(t *testing.T) {
	fe, last := streamExec(func(context.Context) *fakeStream {
		return &fakeStream{blocks: [][][]any{{{uint64(1)}}, {{"not a number"}}}}
	})
	_, ts := newTestServer(t, fe, nil)
	r := post(t, ts.URL+"/?default_format=Native", "SELECT n")
	if r.Status != 500 || r.Header.Get("X-ClickHouse-Exception-Code") != "49" {
		t.Errorf("%d %v %q", r.Status, r.Header, r.Body)
	}
	wantClosed(t, last)
}

// A client that goes away mid-result ends the query, and the stream is
// closed.
func TestStreamClosedWhenTheClientDisconnects(t *testing.T) {
	last := &atomic.Pointer[fakeStream]{}
	fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) {
		s := &endlessStream{block: numberBlocks(1, 1000)[0]}
		last.Store(&s.fakeStream)
		return &exec.Result{Columns: numberColumn, Stream: s}, nil
	}}
	_, ts := newTestServer(t, fe, nil)
	res, err := http.Post(ts.URL+"/", "text/plain", strings.NewReader("SELECT number FROM system.numbers"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.CopyN(io.Discard, res.Body, 1<<20); err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	wantClosed(t, last)
}

// A client that goes away before anything is sent (during a long sort,
// say) is logged with the cancellation's status, not 200.
func TestClientGoneBeforeTheCommitIsNotLoggedAsSent(t *testing.T) {
	fe, last := streamExec(func(ctx context.Context) *fakeStream {
		return &fakeStream{end: func() error {
			<-ctx.Done()
			return ctx.Err()
		}}
	})
	logs := make(chan map[string]any, 16)
	_, ts := newTestServer(t, fe, func(c *Config) { c.Logger = recordLogger(logs) })
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/", strings.NewReader("SELECT n"))
	if err != nil {
		t.Fatal(err)
	}
	if res, err := http.DefaultClient.Do(req); err == nil {
		res.Body.Close()
		t.Fatalf("got HTTP %d", res.StatusCode)
	}
	for {
		select {
		case rec := <-logs:
			if rec["msg"] != "query failed" {
				continue
			}
			if rec["code"] != int64(cherr.QueryWasCancelled) || rec["status"] == int64(http.StatusOK) {
				t.Errorf("request log %v", rec)
			}
			wantClosed(t, last)
			return
		case <-time.After(10 * time.Second):
			t.Fatal("no request log")
		}
	}
}

// endlessStream yields the same block forever.
type endlessStream struct {
	fakeStream
	block [][]any
}

func (e *endlessStream) Next(int) ([][]any, error) { return e.block, nil }

// newEngineServer serves the real engine over memstore with a server
// memory limit of maxMem bytes.
func newEngineServer(t *testing.T, maxMem int64, mod func(*Config)) string {
	t.Helper()
	cat := catalog.NewMemory()
	eng, err := exec.New(exec.Config{Store: memstore.New(), Catalog: cat, Timezone: "UTC", MaxMemoryUsage: maxMem})
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Executor: eng, Catalog: cat, ServerVersion: "25.8.1.1", Logger: quietLogger()}
	if mod != nil {
		mod(&cfg)
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return httpTestServer(t, s)
}

// The response wait_end_of_query buffers counts against max_memory_usage;
// a streamed one holds a block at a time.
func TestWaitEndOfQueryBufferCountsAgainstTheMemoryLimit(t *testing.T) {
	guardServerTest(t)
	base := newEngineServer(t, 0, nil)
	const sql = "SELECT number FROM numbers(1000000) SETTINGS max_memory_usage = 4000000" // 6.9 MB of TSV
	small := url.Values{"max_block_size": {"1000"}}
	r := query(t, base, sql, small)
	if r.Status != 200 || len(r.Body) != 6888890 {
		t.Fatalf("streamed: HTTP %d, %d bytes", r.Status, len(r.Body))
	}
	small.Set("wait_end_of_query", "1")
	wantException(t, "wait_end_of_query", query(t, base, sql, small), "241", 500, "Query memory limit exceeded: would use ")
	wantOK(t, "wait_end_of_query under the limit", query(t, base, "SELECT number FROM numbers(100000) SETTINGS max_memory_usage = 4000000", small))
}

// max_result_rows is checked per block (max_block_size), as in ClickHouse
// 25.8: throw sends the blocks within the limit and then the exception;
// break ends with the block that reaches it.
func TestMaxResultRowsPerBlock(t *testing.T) {
	base := newEngineServer(t, 0, func(c *Config) { c.ResponseBufferSize = 1 })
	blocks := url.Values{"max_block_size": {"10"}}
	r := query(t, base, "SELECT number FROM numbers(1000) SETTINGS max_result_rows = 100", blocks)
	head, exc, _ := strings.Cut(r.Body, "__exception__\r\n")
	if r.Status != 200 || strings.Count(head, "\n") != 100 ||
		exc != "Code: 396. DB::Exception: Limit for result exceeded, max rows: 100.00, current rows: 110.00. (TOO_MANY_ROWS_OR_BYTES) (version 25.8.1.1)\n" {
		t.Errorf("throw: %d %q", r.Status, r.Body)
	}
	r = query(t, base, "SELECT number FROM numbers(1000) SETTINGS max_result_rows = 15, result_overflow_mode = 'break'", blocks)
	if r.Status != 200 || strings.Count(r.Body, "\n") != 20 {
		t.Errorf("break: %d %q", r.Status, r.Body)
	}
	r = query(t, base, "SELECT number FROM numbers(1000) SETTINGS max_result_rows = 5", blocks)
	wantException(t, "first block", r, "396", 500, "max rows: 5.00, current rows: 10.00")
}

// max_block_size in the query's SETTINGS clause sets the blocks the result
// limits count, and Native's blocks, as in ClickHouse 25.8.
func TestBlockSizeFromTheSettingsClause(t *testing.T) {
	base := newEngineServer(t, 0, func(c *Config) { c.ResponseBufferSize = 1 })
	r := query(t, base, "SELECT number FROM numbers(1000) SETTINGS max_block_size = 10, max_result_rows = 15, result_overflow_mode = 'break'", nil)
	if r.Status != 200 || strings.Count(r.Body, "\n") != 20 {
		t.Errorf("break: %d %q", r.Status, r.Body)
	}
	r = query(t, base, "SELECT number FROM numbers(1000) SETTINGS max_block_size = 10, max_result_rows = 100", nil)
	head, exc, _ := strings.Cut(r.Body, "__exception__\r\n")
	if strings.Count(head, "\n") != 100 || !strings.Contains(exc, "max rows: 100.00, current rows: 110.00") {
		t.Errorf("throw: %d %q", r.Status, r.Body)
	}
	r = query(t, base, "SELECT number FROM numbers(1000) SETTINGS max_block_size = 10, max_result_bytes = 100", nil)
	head, exc, _ = strings.Cut(r.Body, "__exception__\r\n")
	if strings.Count(head, "\n") != 10 || !strings.Contains(exc, "Code: 396. DB::Exception: Limit for result exceeded, max bytes: 100.00 B, current bytes: 160.00 B.") {
		t.Errorf("bytes: %d %q", r.Status, r.Body)
	}
	if r = query(t, base, "SELECT number FROM numbers(100) FORMAT Native SETTINGS max_block_size = 10", nil); len(r.Body) != 960 {
		t.Errorf("Native: %d bytes, want 10 blocks of 96", len(r.Body))
	}
}

// failsAt250k fails at row 250000, after about 1.9 MB of TSV.
const failsAt250k = "SELECT number, intDiv(1, toInt64(number) - 250000) FROM numbers(300000)"

// http_response_buffer_size sets how much is buffered before the response
// is committed, so whether a late error is an HTTP error; what a streamed
// result buffers past ResponseBufferSize counts against its memory.
func TestHTTPResponseBufferSizeSetsTheCommitPoint(t *testing.T) {
	guardServerTest(t)
	base := newEngineServer(t, 0, nil)
	r := query(t, base, failsAt250k, nil)
	if r.Status != 200 || !r.Cut || !strings.Contains(r.Body, "\n__exception__\r\nCode: 153.") {
		t.Errorf("default: %d cut %v, %d bytes", r.Status, r.Cut, len(r.Body))
	}
	wantException(t, "10 MB buffer", query(t, base, failsAt250k, url.Values{"http_response_buffer_size": {"10000000"}}), "153", 500, "Division by zero")
	r = query(t, base, "SELECT number, intDiv(1, toInt64(number) - 5000) FROM numbers(6000)",
		url.Values{"http_response_buffer_size": {"100"}, "max_block_size": {"1000"}})
	if r.Status != 200 || !r.Cut || !strings.HasPrefix(r.Body, "0\t0\n") {
		t.Errorf("100 B buffer: %d cut %v %q", r.Status, r.Cut, r.Body[:min(len(r.Body), 100)])
	}
	big := url.Values{"http_response_buffer_size": {"100000000"}, "max_block_size": {"1000"}}
	wantException(t, "charged", query(t, base, "SELECT number FROM numbers(1000000) SETTINGS max_memory_usage = 4000000", big),
		"241", 500, "Query memory limit exceeded: would use ")
	wantException(t, "invalid", query(t, base, "SELECT 1", url.Values{"http_response_buffer_size": {"1KB"}}),
		"27", 400, "Cannot parse input: expected 'eof' before: 'B'")
}

// A client that stops reading ends the query after http_send_timeout,
// logged as SOCKET_TIMEOUT (209), as in ClickHouse.
func TestSendTimeoutEndsAStalledResponse(t *testing.T) {
	guardServerTest(t)
	logs := make(chan map[string]any, 16)
	base := newEngineServer(t, 0, func(c *Config) { c.Logger = recordLogger(logs) })
	conn, err := net.Dial("tcp", strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	sql := "SELECT number FROM system.numbers"
	fmt.Fprintf(conn, "POST /?http_send_timeout=0.2 HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n%s", len(sql), sql)
	rec := nextLog(t, logs)
	if rec["code"] != int64(cherr.SocketTimeout) || !strings.HasPrefix(fmt.Sprint(rec["error"]), "Timeout exceeded while writing to socket (") {
		t.Errorf("request log %v", rec)
	}
	wantException(t, "invalid", query(t, base, "SELECT 1", url.Values{"http_send_timeout": {"abc"}}),
		"27", 400, "Cannot parse input: expected 'eof' before: 'abc'")
}

// The send deadline ends with the response: a kept-alive connection
// serves the next request after the timeout has passed.
func TestSendTimeoutDoesNotOutliveTheResponse(t *testing.T) {
	guardServerTest(t)
	base := newEngineServer(t, 0, nil)
	client := &http.Client{Transport: &http.Transport{MaxConnsPerHost: 1}}
	reused := false
	get := func(u string) {
		t.Helper()
		trace := &httptrace.ClientTrace{GotConn: func(i httptrace.GotConnInfo) { reused = i.Reused }}
		req, _ := http.NewRequestWithContext(httptrace.WithClientTrace(context.Background(), trace), http.MethodGet, u, nil)
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if _, err := io.Copy(io.Discard, res.Body); err != nil || res.StatusCode != 200 {
			t.Fatalf("%s: HTTP %d %v", u, res.StatusCode, err)
		}
	}
	get(base + "/?http_send_timeout=0.05&query=SELECT+number+FROM+numbers(100000)")
	time.Sleep(150 * time.Millisecond)
	get(base + "/ping")
	if !reused {
		t.Error("the connection was not kept alive")
	}
}

// Setting up the output (format, date_time_output_format) fails before
// the query's rows are read, as in ClickHouse 25.8.
func TestOutputErrorsComeBeforeQueryErrors(t *testing.T) {
	base := newEngineServer(t, 0, nil)
	wantException(t, "format", query(t, base, "SELECT intDiv(1, number - 5) FROM numbers(10) FORMAT NoSuchFormat", nil),
		"73", 404, "Unknown format NoSuchFormat")
	wantException(t, "date_time_output_format", query(t, base, "SELECT now(), intDiv(1, number - 5) FROM numbers(10)",
		url.Values{"date_time_output_format": {"bogus"}}), "36", 400, "")
}

// isoRowThenException is a whole TSV row in date_time_output_format=iso
// followed by the exception.
var isoRowThenException = regexp.MustCompile(`\n1970-01-02T\d\d:\d\d:\d\dZ\t0\n__exception__\r\nCode: 153\. [^\n]*\n$`)

// date_time_output_format wraps the encoder; an error after commit still
// follows every row returned (the failing block is dropped), inside the
// format for JSON.
func TestErrorAfterCommitWithDateTimeOutput(t *testing.T) {
	guardServerTest(t)
	base := newEngineServer(t, 0, func(c *Config) { c.ResponseBufferSize = 1 })
	const sql = "SELECT toDateTime(number, 'UTC') AS t, intDiv(1, toInt64(number) - 150000) AS x FROM numbers(200000)"
	iso := url.Values{"date_time_output_format": {"iso"}}
	r := query(t, base, sql+" FORMAT TSV", iso)
	if !r.Cut || !isoRowThenException.MatchString(r.Body) {
		t.Errorf("TSV: cut %v ...%q", r.Cut, r.Body[max(0, len(r.Body)-200):])
	}
	r = query(t, base, sql+" FORMAT JSONCompact", iso)
	var v struct {
		Data      [][]any `json:"data"`
		Rows      int     `json:"rows"`
		Exception string  `json:"exception"`
	}
	if err := json.Unmarshal([]byte(r.Body), &v); err != nil || r.Cut || v.Rows == 0 || len(v.Data) != v.Rows ||
		!strings.HasPrefix(v.Exception, "Code: 153.") || !strings.HasSuffix(fmt.Sprint(v.Data[v.Rows-1][0]), "Z") {
		t.Errorf("JSONCompact (%v): rows %d ...%q", err, v.Rows, r.Body[max(0, len(r.Body)-200):])
	}
}

// A plain SELECT is not collected: its rows go out as they are read.
func TestSelectStreamsTheResult(t *testing.T) {
	guardServerTest(t)
	base := newEngineServer(t, 2_000_000, nil)
	r := query(t, base, "SELECT number, toString(number) FROM numbers(300000)", nil)
	if r.Status != 200 || !bytes.HasSuffix([]byte(r.Body), []byte("299999\t299999\n")) {
		t.Errorf("HTTP %d, %d bytes", r.Status, len(r.Body))
	}
}

// recordLogger sends the attributes of each log record to ch.
func recordLogger(ch chan map[string]any) *slog.Logger { return slog.New(recordHandler{ch}) }

type recordHandler struct{ ch chan map[string]any }

func (h recordHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h recordHandler) Handle(_ context.Context, r slog.Record) error {
	m := map[string]any{"msg": r.Message}
	r.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value.Any()
		return true
	})
	select {
	case h.ch <- m:
	default:
	}
	return nil
}

func (h recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h recordHandler) WithGroup(string) slog.Handler      { return h }

// nextLog waits for the next request log record.
func nextLog(t *testing.T, logs chan map[string]any) map[string]any {
	t.Helper()
	select {
	case rec := <-logs:
		return rec
	case <-time.After(10 * time.Second):
		t.Fatal("no request was logged")
		return nil
	}
}

// The request log has the rows returned and the query's peak memory.
func TestRequestLogHasResultRowsAndPeakMemory(t *testing.T) {
	guardServerTest(t)
	logs := make(chan map[string]any, 16)
	base := newEngineServer(t, 64_000_000, func(c *Config) { c.Logger = recordLogger(logs) })
	wantOK(t, "sort", query(t, base, "SELECT number FROM numbers(100000) ORDER BY number DESC LIMIT 99999, 3", nil))
	rec := nextLog(t, logs)
	if rec["result_rows"] != uint64(1) || rec["read_rows"] != uint64(100000) {
		t.Errorf("request log %v", rec)
	}
	if p, _ := rec["peak_memory"].(int64); p < 1_000_000 {
		t.Errorf("peak_memory %v", rec["peak_memory"])
	}
}

// Cutting a response off after a late error ends the request cleanly with
// the default send timeout: the connection is taken over once, and
// nothing writes to it afterwards.
func TestCutOffResponseEndsCleanly(t *testing.T) {
	fe, _ := streamExec(failingAfter(2))
	logs := make(chan map[string]any, 16)
	_, ts := newTestServer(t, fe, func(c *Config) { c.ResponseBufferSize, c.Logger = 1, recordLogger(logs) })
	for _, f := range []string{"TSV", "Native"} {
		if r := post(t, ts.URL+"/?default_format="+f, "SELECT n"); r.Status != 200 || !r.Cut {
			t.Errorf("%s: %d cut %v", f, r.Status, r.Cut)
		}
		for rec := nextLog(t, logs); rec["msg"] != "query failed"; rec = nextLog(t, logs) {
			if rec["msg"] == "panic serving query" {
				t.Errorf("%s: %v", f, rec["panic"])
			}
		}
	}
}

// http_send_timeout bounds a stalled write, not a long transfer: a client
// that reads slowly but steadily gets a whole wait_end_of_query response
// that takes several timeouts to send.
func TestSendTimeoutAllowsASlowSteadyReader(t *testing.T) {
	guardServerTest(t)
	logs := make(chan map[string]any, 16)
	base := newEngineServer(t, 0, func(c *Config) { c.Logger = recordLogger(logs) })
	d := net.Dialer{Control: func(_, _ string, rc syscall.RawConn) error {
		return rc.Control(func(fd uintptr) { _ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 16<<10) })
	}}
	conn, err := d.Dial("tcp", strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	sql := "SELECT number, toString(number) FROM numbers(400000)" // 5.38 MB
	fmt.Fprintf(conn, "POST /?http_send_timeout=0.5&wait_end_of_query=1 HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Length: %d\r\n\r\n%s", len(sql), sql)
	start, total, buf := time.Now(), 0, make([]byte, 32<<10)
	for {
		n, err := conn.Read(buf)
		total += n
		if err != nil {
			break
		}
		time.Sleep(10 * time.Millisecond) // about 3 MB/s
	}
	if took := time.Since(start); total < 5_378_000 || took < time.Second {
		t.Errorf("read %d bytes in %v", total, took)
	}
	if rec := nextLog(t, logs); rec["code"] != nil {
		t.Errorf("request log %v", rec)
	}
}

// gatedStream yields its blocks, then waits until gate is closed (or 10 s
// pass) before it ends.
type gatedStream struct {
	fakeStream
	gate chan struct{}
}

func (g *gatedStream) Next(n int) ([][]any, error) {
	if len(g.blocks) == 0 {
		select {
		case <-g.gate:
		case <-time.After(10 * time.Second):
		}
	}
	return g.fakeStream.Next(n)
}

// Once the response is committed each block reaches the client, status
// and headers first, before the next block is read, as ClickHouse 25.8
// flushes each block: a client sees the rows of a slow query as they
// come. Two blocks in a row are flushed together by the flush timer.
func TestCommittedBlocksReachTheClientAtOnce(t *testing.T) {
	for _, c := range []struct {
		blocks int
		want   string
	}{
		{1, "0\n1\n2\n"},
		{2, "0\n1\n2\n3\n4\n5\n"},
	} {
		gate := make(chan struct{})
		fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) {
			s := &gatedStream{fakeStream: fakeStream{blocks: numberBlocks(c.blocks, 3)}, gate: gate}
			return &exec.Result{Columns: numberColumn, Stream: s}, nil
		}}
		_, ts := newTestServer(t, fe, nil)
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/?http_response_buffer_size=0&query=SELECT+n", nil)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			cancel()
			t.Fatalf("%d blocks: no headers before the query ended: %v", c.blocks, err)
		}
		got := make([]byte, len(c.want))
		_, err = io.ReadFull(res.Body, got)
		if err != nil || string(got) != c.want || time.Since(start) > 4*time.Second {
			t.Errorf("%d blocks: read %q (%v) after %v, before the query ended", c.blocks, got, err, time.Since(start))
		}
		close(gate)
		rest, _ := io.ReadAll(res.Body)
		res.Body.Close()
		cancel()
		if res.StatusCode != 200 || len(rest) != 0 {
			t.Errorf("%d blocks: HTTP %d, then %q", c.blocks, res.StatusCode, rest)
		}
	}
}
