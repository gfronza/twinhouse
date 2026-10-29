package server

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/exec"
	"github.com/gfronza/twinhouse/internal/memtrack"
)

// commitCase is a request and whether its late error must be an HTTP
// error (the body buffered past it) or end a committed 200.
type commitCase struct {
	params string
	status int
}

// wantCommit checks each case against a stream of two 3-row blocks that
// then fails, served with a ResponseBufferSize of free bytes.
func wantCommit(t *testing.T, free int, cases []commitCase) {
	t.Helper()
	fe, _ := streamExec(failingAfter(2))
	fe.fn = sessionAware(fe.fn)
	_, ts := newTestServer(t, fe, func(c *Config) { c.ResponseBufferSize = free })
	for _, c := range cases {
		r := post(t, ts.URL+"/?"+c.params, "SELECT n")
		if r.Status != c.status {
			t.Errorf("%s: HTTP %d, want %d (%q)", c.params, r.Status, c.status, r.Body)
			continue
		}
		if c.status == 200 && (!r.Cut || !strings.Contains(r.Body, "__exception__\r\n"+divisionText)) {
			t.Errorf("%s: cut %v %q", c.params, r.Cut, r.Body)
		}
	}
}

// sessionAware answers SET with an empty result, so a session can hold
// settings, and every other query with fn.
func sessionAware(fn func(context.Context, *exec.Request) (*exec.Result, error)) func(context.Context, *exec.Request) (*exec.Result, error) {
	return func(ctx context.Context, req *exec.Request) (*exec.Result, error) {
		if strings.HasPrefix(req.Query, "SET ") {
			return &exec.Result{}, nil
		}
		return fn(ctx, req)
	}
}

// The buffer_size URL parameter sets the commit point when
// http_response_buffer_size is not set, which wins over it, as in
// ClickHouse 25.8; it is parsed as an optional '+' and plain digits, and
// http_response_buffer_size as the engine reads the setting ('+' and a
// size suffix). A given 0 commits at the first block.
func TestBufferSizeParameterSetsTheCommitPoint(t *testing.T) {
	wantCommit(t, 1, []commitCase{
		{params: "", status: 200},
		{params: "buffer_size=4000000", status: 500},
		{params: "buffer_size=0", status: 200},
		{params: "http_response_buffer_size=4&buffer_size=4000000", status: 200},
		{params: "http_response_buffer_size=4000000&buffer_size=4", status: 500},
		{params: "http_response_buffer_size=4&buffer_size=abc", status: 200},
	})
	wantCommit(t, 1<<20, []commitCase{
		{params: "", status: 500},
		{params: "http_response_buffer_size=0", status: 200},
		{params: "buffer_size=0", status: 200},
		{params: "http_response_buffer_size=0&buffer_size=4000000", status: 200},
		{params: "http_response_buffer_size=%2B4", status: 200},
		{params: "http_response_buffer_size=%2B0", status: 200},
		{params: "http_response_buffer_size=%2B1K", status: 500},
		{params: "buffer_size=%2B4", status: 200},
		{params: "buffer_size=%2B0", status: 200},
	})
	fe, _ := streamExec(failingAfter(2))
	_, ts := newTestServer(t, fe, nil)
	for _, c := range []struct{ value, code, msg string }{
		{"1K", "27", "Cannot parse input: expected 'eof' before: 'K'"},
		{"abc", "27", "Cannot parse input: expected 'eof' before: 'abc'"},
		{"", "32", "Attempt to read after eof"},
		{"-1", "72", "Unsigned type must not contain '-' symbol"},
		{"+", "72", "Cannot parse number without any digits"},
		{"+ 16", "72", "Cannot parse number without any digits"},
		{"++1", "72", "Cannot parse number with multiple sign (+/-) characters"},
		{"+-1", "72", "Cannot parse number with multiple sign (+/-) characters"},
		{"+1K", "27", "Cannot parse input: expected 'eof' before: 'K'"},
		{" +16", "27", "Cannot parse input: expected 'eof' before: ' +16'"},
	} {
		status := 400
		if c.code == "32" {
			status = 500
		}
		wantException(t, "buffer_size="+c.value, post(t, ts.URL+"/?buffer_size="+url.QueryEscape(c.value), "SELECT n"), c.code, status, c.msg)
	}
}

// http_wait_end_of_query (URL or session) buffers the whole body like the
// wait_end_of_query URL parameter, and either one is enough, as in
// ClickHouse 25.8; in the query's SETTINGS clause it has no effect there.
func TestHTTPWaitEndOfQueryBuffersTheWholeBody(t *testing.T) {
	wantCommit(t, 1, []commitCase{
		{params: "wait_end_of_query=1", status: 500},
		{params: "http_wait_end_of_query=1", status: 500},
		{params: "http_wait_end_of_query=true", status: 500},
		{params: "wait_end_of_query=0&http_wait_end_of_query=1", status: 500},
		{params: "http_wait_end_of_query=0", status: 200},
	})
	fe, _ := streamExec(failingAfter(2))
	fe.fn = sessionAware(fe.fn)
	_, ts := newTestServer(t, fe, func(c *Config) { c.ResponseBufferSize = 1 })
	wantOK(t, "SET", post(t, ts.URL+"/?session_id=w", "SET http_wait_end_of_query = 1"))
	if r := post(t, ts.URL+"/?session_id=w", "SELECT n"); r.Status != 500 {
		t.Errorf("session: HTTP %d %q", r.Status, r.Body)
	}
	if r := post(t, ts.URL+"/", "SELECT n SETTINGS http_wait_end_of_query = 1"); r.Status != 200 || !r.Cut {
		t.Errorf("SETTINGS clause: HTTP %d cut %v", r.Status, r.Cut)
	}
}

// wait_end_of_query is read as ClickHouse 25.8's readBoolText reads it
// (any first character but 0 is true; t and f must spell true or false),
// and http_wait_end_of_query as a Bool setting (0, 1, true, false), with
// 25.8's errors.
func TestWaitEndOfQueryValuesParseLikeClickHouse(t *testing.T) {
	wantCommit(t, 1, []commitCase{
		{params: "wait_end_of_query=2", status: 500},
		{params: "wait_end_of_query=t", status: 500},
		{params: "wait_end_of_query=f", status: 500},
		{params: "wait_end_of_query=tRuE", status: 500},
		{params: "wait_end_of_query=False", status: 200},
		{params: "wait_end_of_query=0", status: 200},
		{params: "http_wait_end_of_query=TRUE", status: 500},
		{params: "http_wait_end_of_query=False", status: 200},
	})
	fe, _ := streamExec(failingAfter(2))
	_, ts := newTestServer(t, fe, nil)
	for _, c := range []struct {
		params, code string
		status       int
		msg          string
	}{
		{"wait_end_of_query=yes", "27", 400, "Cannot parse input: expected 'eof' before: 'es'"},
		{"wait_end_of_query=10", "27", 400, "Cannot parse input: expected 'eof' before: '0'"},
		{"wait_end_of_query=tru", "27", 400, "Cannot parse input: expected 'rue' at end of stream."},
		{"wait_end_of_query=falsx", "27", 400, "Cannot parse input: expected 'alse' before: 'x'"},
		{"wait_end_of_query=", "32", 500, "Attempt to read after eof"},
		{"http_wait_end_of_query=2", "467", 500, "Cannot parse bool from string '2'"},
		{"http_wait_end_of_query=yes&wait_end_of_query=1", "467", 500, "Cannot parse bool from string 'yes'"},
	} {
		wantException(t, c.params, post(t, ts.URL+"/?"+c.params, "SELECT n"), c.code, c.status, c.msg)
	}
}

// The summary has the result's rows and bytes for a body sent at once
// when the client set its buffering (wait_end_of_query, a buffer size the
// body fits in) or compression, when the server buffers every response,
// or when the body is empty (FORMAT Null), as in ClickHouse 25.8; with the
// default buffer, or once a body is committed early, result_rows and
// result_bytes are 0.
func TestSummaryHasResultCountsForABufferedBody(t *testing.T) {
	fe, _ := streamExec(func(context.Context) *fakeStream {
		return &fakeStream{blocks: numberBlocks(1, 3), live: exec.Stats{ReadRows: 3, ReadBytes: 24},
			final: exec.Stats{ReadRows: 3, ReadBytes: 24, ResultRows: 3}}
	})
	_, ts := newTestServer(t, fe, nil)
	_, buffered := newTestServer(t, fe, func(c *Config) { c.BufferResponses = true })
	const none, counts = `"result_rows":"0","result_bytes":"0"`, `"result_rows":"3","result_bytes":"6"`
	for _, c := range []struct{ url, encoding, counts string }{
		{ts.URL + "/", "", none},
		{ts.URL + "/?wait_end_of_query=1", "", counts},
		{ts.URL + "/?http_wait_end_of_query=1", "", counts},
		{ts.URL + "/?http_response_buffer_size=1000", "", counts},
		{ts.URL + "/?buffer_size=6", "", counts},
		{ts.URL + "/?http_response_buffer_size=5", "", none},
		{ts.URL + "/?http_response_buffer_size=0", "", none},
		{ts.URL + "/?compress=1", "", counts},
		{ts.URL + "/?enable_http_compression=1", "gzip", counts},
		{ts.URL + "/?default_format=Null", "", `"result_rows":"3","result_bytes":"0"`},
		{buffered.URL + "/", "", counts},
	} {
		req, _ := http.NewRequest(http.MethodPost, c.url, strings.NewReader("SELECT n"))
		if c.encoding != "" {
			req.Header.Set("Accept-Encoding", c.encoding)
		}
		r := do(t, req)
		if s := r.Header.Get("X-ClickHouse-Summary"); !strings.Contains(s, `"read_rows":"3"`) || !strings.Contains(s, c.counts) {
			t.Errorf("%s %s: summary %s, want %s", c.url, c.encoding, s, c.counts)
		}
	}
}

// chargingStream yields the same block forever, charging it to its
// tracker until the next call, as the engine's stream does.
type chargingStream struct {
	fakeStream
	block   [][]any
	blocks  atomic.Int64
	tracker *memtrack.Tracker
	held    *memtrack.Account
}

func (c *chargingStream) Next(int) ([][]any, error) {
	c.held.Close()
	if err := c.held.Grow(1 << 20); err != nil {
		return nil, err
	}
	c.blocks.Add(1)
	return c.block, nil
}

func (c *chargingStream) Close() {
	c.held.Close()
	c.fakeStream.Close()
}

// A HEAD request is answered once the first block is encoded and the
// query stops there, as ClickHouse 25.8 answers it, so HEAD on an endless
// result returns at once; the stream is closed and its memory released.
func TestHeadEndsAfterTheFirstBlock(t *testing.T) {
	last := &atomic.Pointer[chargingStream]{}
	fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) {
		tr := memtrack.New(0)
		s := &chargingStream{block: numberBlocks(1, 1000)[0], tracker: tr, held: tr.NewAccount()}
		last.Store(s)
		return &exec.Result{Columns: numberColumn, Stream: s}, nil
	}}
	logs := make(chan map[string]any, 16)
	_, ts := newTestServer(t, fe, func(c *Config) { c.Logger = recordLogger(logs) })
	req, _ := http.NewRequest(http.MethodHead, ts.URL+"/?query=SELECT+number+FROM+system.numbers", nil)
	r := do(t, req)
	if r.Status != 200 || r.Header.Get("X-ClickHouse-Format") != "TabSeparated" {
		t.Errorf("%d %v", r.Status, r.Header)
	}
	if rec := nextLog(t, logs); rec["code"] != nil {
		t.Errorf("request log %v", rec)
	}
	s := last.Load()
	if !s.closed.Load() || s.blocks.Load() != 1 || s.tracker.Used() != 0 {
		t.Errorf("closed %v after %d blocks, %d bytes still tracked", s.closed.Load(), s.blocks.Load(), s.tracker.Used())
	}
}

// HEAD over the engine: an endless SELECT returns promptly, with a
// selective filter too (its first block ends with its source block); an
// error in the first block is an HTTP error, one after it is not reached,
// as in ClickHouse 25.8 without parallel formatting.
func TestHeadOverTheEngine(t *testing.T) {
	guardServerTest(t)
	logs := make(chan map[string]any, 16)
	base := newEngineServer(t, 0, func(c *Config) { c.Logger = recordLogger(logs) })
	head := func(sql string) resp {
		req, _ := http.NewRequest(http.MethodHead, base+"/?max_execution_time=30&query="+url.QueryEscape(sql), nil)
		return do(t, req)
	}
	start := time.Now()
	if r := head("SELECT number FROM system.numbers"); r.Status != 200 || time.Since(start) > 5*time.Second {
		t.Errorf("system.numbers: HTTP %d after %v", r.Status, time.Since(start))
	}
	if rec := nextLog(t, logs); rec["code"] != nil {
		t.Errorf("request log %v", rec)
	}
	start = time.Now()
	if r := head("SELECT number FROM system.numbers WHERE number % 100000 = 0"); r.Status != 200 || time.Since(start) > 5*time.Second {
		t.Errorf("selective filter: HTTP %d after %v", r.Status, time.Since(start))
	}
	if rec := nextLog(t, logs); rec["code"] != nil {
		t.Errorf("request log %v", rec)
	}
	if r := head("SELECT intDiv(1, toInt64(number) - 300000) FROM numbers(400000)"); r.Status != 200 {
		t.Errorf("late error: HTTP %d", r.Status)
	}
	nextLog(t, logs)
	if r := head("SELECT intDiv(1, toInt64(number) - 5) FROM numbers(10)"); r.Status != 500 || r.Header.Get("X-ClickHouse-Exception-Code") != "153" {
		t.Errorf("error in the first block: HTTP %d %v", r.Status, r.Header)
	}
}

// panickingStream yields its blocks, then panics.
type panickingStream struct{ fakeStream }

func (p *panickingStream) Next(n int) ([][]any, error) {
	if len(p.blocks) == 0 {
		panic("broken encoder")
	}
	return p.fakeStream.Next(n)
}

// A panic while a committed result is encoded ends the body like any late
// error: __exception__ with a LOGICAL_ERROR, then the transfer is cut off;
// before the commit it is an HTTP error.
func TestPanicWhileEncodingEndsTheBody(t *testing.T) {
	var closed atomic.Bool
	fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) {
		s := &panickingStream{fakeStream{blocks: numberBlocks(2, 3)}}
		return &exec.Result{Columns: numberColumn, Stream: closeSpy{s, &closed}}, nil
	}}
	_, early := newTestServer(t, fe, func(c *Config) { c.ResponseBufferSize = 1 })
	r := post(t, early.URL+"/", "SELECT n")
	if r.Status != 200 || !r.Cut || !strings.HasPrefix(r.Body, "0\n1\n2\n3\n4\n5\n__exception__\r\nCode: 49. DB::Exception: Internal error: broken encoder") {
		t.Errorf("committed: %d cut %v %q", r.Status, r.Cut, r.Body)
	}
	if !closed.Load() {
		t.Error("the stream was not closed")
	}
	_, ts := newTestServer(t, fe, nil)
	wantException(t, "before the commit", post(t, ts.URL+"/", "SELECT n"), "49", 500, "Internal error: broken encoder")
}

// closeSpy records that its stream was closed.
type closeSpy struct {
	exec.ResultStream
	closed *atomic.Bool
}

func (c closeSpy) Close() {
	c.ResultStream.Close()
	c.closed.Store(true)
}
