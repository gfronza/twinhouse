package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-lambda-go/events"
	"github.com/klauspost/compress/zstd"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
	"dynamodb-eventstream/internal/exec"
	"dynamodb-eventstream/internal/format"
	"dynamodb-eventstream/internal/server"
	"dynamodb-eventstream/internal/storage/memstore"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// countingCatalog counts Refresh calls.
type countingCatalog struct {
	catalog.Catalog
	refreshes int
	fail      error
}

func (c *countingCatalog) Refresh(ctx context.Context) error {
	c.refreshes++
	if c.fail != nil {
		return c.fail
	}
	return c.Catalog.Refresh(ctx)
}

// stack is the server wired to an in-memory store, reachable both through
// the Lambda handler and directly over HTTP (for byte-for-byte comparison).
type stack struct {
	srv    *server.Server
	app    *App
	handle Handler
	direct string
}

// newStack wires the engine and server to cat; app (its Catalog may wrap
// cat to observe the refreshes App makes, which the engine's own misses do
// not go through) defaults to a one-second MaxAge.
func newStack(t *testing.T, cat catalog.Catalog, app *App) *stack {
	t.Helper()
	eng, err := exec.New(exec.Config{
		Store: memstore.New(), Catalog: cat, Region: "local", InlineViews: true,
		ServerVersion: "25.8.1.1", ServerRevision: server.ServerRevision, DisplayName: "lambda-test", Timezone: "UTC",
	})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := server.New(server.Config{
		Executor: eng, Catalog: cat, ServerVersion: "25.8.1.1", DisplayName: "lambda-test",
		Logger: quietLogger(), RejectSessions: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if app == nil {
		app = &App{Catalog: cat, MaxAge: time.Second}
	}
	app.Logger = quietLogger()
	app.ServerVersion = "25.8.1.1"
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return &stack{srv: srv, app: app, handle: NewHandler(app.Middleware(srv)), direct: ts.URL}
}

// event builds an API Gateway REST proxy event the way API Gateway does
// with BinaryMediaTypes */*: every non-empty body base64-encoded, unless
// text is set (API Gateway sends text bodies when no binary media type
// matches; sam local does the same).
type event struct {
	method  string
	path    string
	query   url.Values
	headers http.Header
	body    []byte
	text    bool
	// singleValue uses QueryStringParameters instead of the multi-value map.
	singleValue bool
}

func (e event) proxy() events.APIGatewayProxyRequest {
	ev := events.APIGatewayProxyRequest{
		HTTPMethod: e.method, Path: e.path, Headers: map[string]string{}, MultiValueHeaders: map[string][]string{},
		RequestContext: events.APIGatewayProxyRequestContext{DomainName: "x.execute-api.us-east-1.amazonaws.com", Stage: "prod"},
	}
	if e.path == "" {
		ev.Path = "/"
	}
	if len(e.query) > 0 {
		if e.singleValue {
			ev.QueryStringParameters = map[string]string{}
			for k, v := range e.query {
				ev.QueryStringParameters[k] = v[0]
			}
		} else {
			ev.MultiValueQueryStringParameters = e.query
		}
	}
	for k, v := range e.headers {
		ev.Headers[k] = v[0]
		ev.MultiValueHeaders[k] = v
	}
	if len(e.body) > 0 {
		if e.text {
			ev.Body = string(e.body)
		} else {
			ev.Body = base64.StdEncoding.EncodeToString(e.body)
			ev.IsBase64Encoded = true
		}
	}
	return ev
}

type result struct {
	status int
	header http.Header
	body   []byte
	b64    bool
}

func (s *stack) invoke(t *testing.T, e event) result {
	t.Helper()
	resp, err := s.handle(context.Background(), e.proxy())
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	r := result{status: resp.StatusCode, header: http.Header(resp.MultiValueHeaders), body: []byte(resp.Body), b64: resp.IsBase64Encoded}
	if resp.IsBase64Encoded {
		b, err := base64.StdEncoding.DecodeString(resp.Body)
		if err != nil {
			t.Fatalf("response body is not base64: %v", err)
		}
		r.body = b
	}
	return r
}

func (s *stack) directly(t *testing.T, e event) result {
	t.Helper()
	u := s.direct + e.path
	if e.path == "" {
		u += "/"
	}
	if len(e.query) > 0 {
		u += "?" + e.query.Encode()
	}
	req, err := http.NewRequest(e.method, u, bytes.NewReader(e.body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range e.headers {
		req.Header[k] = v
	}
	res, err := (&http.Client{Transport: &http.Transport{DisableCompression: true}}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return result{status: res.StatusCode, header: res.Header, body: b}
}

// both runs e through the Lambda and directly, requiring equal status and
// body (byte for byte) and the same values of the given headers.
func (s *stack) both(t *testing.T, e event, headers ...string) result {
	t.Helper()
	l, d := s.invoke(t, e), s.directly(t, e)
	if l.status != d.status {
		t.Errorf("%s %v: lambda status %d, direct %d\nlambda: %q\ndirect: %q", e.method, e.query, l.status, d.status, l.body, d.body)
	}
	if !bytes.Equal(l.body, d.body) {
		t.Errorf("%s %v: bodies differ\nlambda (%d bytes): %q\ndirect (%d bytes): %q", e.method, e.query, len(l.body), l.body, len(d.body), d.body)
	}
	for _, h := range headers {
		if l.header.Get(h) != d.header.Get(h) {
			t.Errorf("%s %v: header %s: lambda %q, direct %q", e.method, e.query, h, l.header.Get(h), d.header.Get(h))
		}
	}
	return l
}

func q(kv ...string) url.Values {
	v := url.Values{}
	for i := 0; i+1 < len(kv); i += 2 {
		v.Add(kv[i], kv[i+1])
	}
	return v
}

func hdr(kv ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Set(kv[i], kv[i+1])
	}
	return h
}

var cols = []format.Column{
	{Name: "tenant", Type: chtype.TString},
	{Name: "id", Type: chtype.TUInt64},
	{Name: "amount", Type: chtype.TFloat64},
}

func nativeBlock(t *testing.T, rows [][]any) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := format.NewEncoder("Native", &buf, cols, format.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finish(format.Stats{}); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func gzipped(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func gunzip(t *testing.T, b []byte) []byte {
	t.Helper()
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	return out
}

func TestHandlerRoundTrips(t *testing.T) {
	inner := catalog.NewMemory()
	cat := &countingCatalog{Catalog: inner}
	s := newStack(t, inner, &App{Catalog: cat, MaxAge: time.Second})

	// Health checks and the help page.
	if r := s.both(t, event{method: "GET", path: "/ping"}, "Content-Type"); r.status != 200 || string(r.body) != "Ok.\n" || r.b64 {
		t.Fatalf("ping: %d %q b64=%v", r.status, r.body, r.b64)
	}
	if r := s.both(t, event{method: "GET", path: "/nope"}); r.status != 404 || !strings.Contains(string(r.body), "There is no handle /nope") {
		t.Fatalf("404: %d %q", r.status, r.body)
	}

	// DDL as a text body (API Gateway sends text when it does not treat the
	// body as binary; sam local does this too).
	r := s.invoke(t, event{method: "POST", body: []byte("CREATE TABLE ev (tenant String, id UInt64, amount Float64) ENGINE = MergeTree ORDER BY (tenant, id)"), text: true})
	if r.status != 200 {
		t.Fatalf("create: %d %s", r.status, r.body)
	}

	// INSERT a Native block (what clickhouse-go sends), base64-encoded.
	rows := [][]any{{"a", uint64(1), 1.5}, {"b", uint64(2), 2.5}, {"a", uint64(3), 3.5}}
	r = s.invoke(t, event{method: "POST", query: q("query", "INSERT INTO ev FORMAT Native"), body: nativeBlock(t, rows)})
	if r.status != 200 {
		t.Fatalf("native insert: %d %s", r.status, r.body)
	}
	// INSERT gzip-compressed JSONEachRow (Content-Encoding).
	jsonRows := []byte(`{"tenant":"c","id":4,"amount":4.5}` + "\n" + `{"tenant":"b","id":5,"amount":5.5}` + "\n")
	r = s.invoke(t, event{method: "POST", query: q("query", "INSERT INTO ev FORMAT JSONEachRow"),
		headers: hdr("Content-Encoding", "gzip"), body: gzipped(t, jsonRows)})
	if r.status != 200 {
		t.Fatalf("gzip insert: %d %s", r.status, r.body)
	}
	// INSERT with inline data in the body, and an explicit Content-Length.
	body := []byte("INSERT INTO ev FORMAT JSONEachRow\n" + `{"tenant":"d","id":6,"amount":6.5}` + "\n")
	r = s.invoke(t, event{method: "POST", body: body, headers: hdr("Content-Length", "71")})
	if r.status != 200 {
		t.Fatalf("inline insert: %d %s", r.status, r.body)
	}
	// INSERT native-compressed data (decompress=1): the client sends LZ4
	// frames. Produced by the server itself (compress=1), see below.

	const sel = "SELECT tenant, id, amount FROM ev ORDER BY id"
	// JSON: text, not base64.
	r = s.both(t, event{method: "GET", query: q("query", sel+" FORMAT JSON", "query_id", "j1", "output_format_write_statistics", "0")}, "Content-Type", "X-ClickHouse-Format")
	if r.status != 200 || r.b64 {
		t.Fatalf("json: %d b64=%v %s", r.status, r.b64, r.body)
	}
	var js struct {
		Rows int `json:"rows"`
	}
	if err := json.Unmarshal(r.body, &js); err != nil || js.Rows != 6 {
		t.Fatalf("json rows %d err %v: %s", js.Rows, err, r.body)
	}
	// TSV with a single-value query map (older event shape).
	r = s.both(t, event{method: "GET", query: q("query", sel+" FORMAT TSV"), singleValue: true})
	if r.status != 200 || r.b64 || string(r.body) != "a\t1\t1.5\nb\t2\t2.5\na\t3\t3.5\nc\t4\t4.5\nb\t5\t5.5\nd\t6\t6.5\n" {
		t.Fatalf("tsv: %d b64=%v %q", r.status, r.b64, r.body)
	}
	// Native: binary, base64.
	r = s.both(t, event{method: "POST", body: []byte(sel + " FORMAT Native"), text: true}, "Content-Type", "X-ClickHouse-Format")
	if r.status != 200 || !r.b64 || r.header.Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("native: %d b64=%v ct=%q", r.status, r.b64, r.header.Get("Content-Type"))
	}
	dec, err := format.NewDecoder("Native", bytes.NewReader(r.body), cols, format.Options{})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for {
		row, err := dec.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode native row %d: %v", n, err)
		}
		if n == 0 && (row[0] != "a" || row[1] != uint64(1) || row[2] != 1.5) {
			t.Errorf("first native row %v", row)
		}
		n++
	}
	if n != 6 {
		t.Errorf("native rows %d", n)
	}
	// compress=1: ClickHouse-native LZ4 frames around the TSV payload.
	r = s.both(t, event{method: "GET", query: q("query", sel+" FORMAT TSV", "compress", "1")})
	if r.status != 200 || !r.b64 || utf8.Valid(r.body) {
		t.Fatalf("compress=1: %d b64=%v valid-utf8=%v", r.status, r.b64, utf8.Valid(r.body))
	}
	lz4Body := r.body
	// ... which the server accepts back as INSERT data with decompress=1.
	r = s.invoke(t, event{method: "POST", query: q("query", "INSERT INTO ev FORMAT TSV", "decompress", "1"), body: lz4Body})
	if r.status != 200 {
		t.Fatalf("decompress insert: %d %s", r.status, r.body)
	}
	if r = s.invoke(t, event{method: "GET", query: q("query", "SELECT count() FROM ev")}); string(r.body) != "12\n" {
		t.Fatalf("count after decompress insert: %q", r.body)
	}

	// HTTP compression of the response (gzip, zstd): Content-Encoding set,
	// body base64, identical to the direct response.
	for _, enc := range []string{"gzip", "zstd"} {
		r = s.both(t, event{method: "GET", query: q("query", sel+" FORMAT JSONEachRow", "enable_http_compression", "1"),
			headers: hdr("Accept-Encoding", enc)}, "Content-Encoding", "Content-Type")
		if r.status != 200 || !r.b64 || r.header.Get("Content-Encoding") != enc {
			t.Fatalf("%s: %d b64=%v enc=%q", enc, r.status, r.b64, r.header.Get("Content-Encoding"))
		}
		var plain []byte
		if enc == "gzip" {
			plain = gunzip(t, r.body)
		} else {
			zr, err := zstd.NewReader(bytes.NewReader(r.body))
			if err != nil {
				t.Fatal(err)
			}
			plain, err = io.ReadAll(zr)
			zr.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
		if lines := strings.Count(string(plain), "\n"); lines != 12 {
			t.Errorf("%s: %d lines: %s", enc, lines, plain)
		}
	}

	// Errors keep their ClickHouse shape and headers.
	r = s.both(t, event{method: "GET", query: q("query", "SELECT * FROM nope")}, "X-ClickHouse-Exception-Code")
	if r.status != 404 || r.header.Get("X-ClickHouse-Exception-Code") != "60" {
		t.Errorf("unknown table: %d %q %s", r.status, r.header.Get("X-ClickHouse-Exception-Code"), r.body)
	}
	// Sessions are rejected.
	r = s.both(t, event{method: "POST", query: q("session_id", "s1"), body: []byte("SELECT 1"), text: true}, "X-ClickHouse-Exception-Code")
	if r.status != 501 || r.header.Get("X-ClickHouse-Exception-Code") != "48" ||
		string(r.body) != "Code: 48. DB::Exception: sessions are not supported by this deployment. (NOT_IMPLEMENTED) (version 25.8.1.1)\n" {
		t.Errorf("session: %d %s", r.status, r.body)
	}
	// Auth failures too (403 before anything else).
	r = s.invoke(t, event{method: "GET", query: q("query", "SELECT 1", "user", "nobody")})
	if r.status != 403 || r.header.Get("X-ClickHouse-Exception-Code") != "516" {
		t.Errorf("auth: %d %s", r.status, r.body)
	}
	// The catalog was refreshed at most once per MaxAge (1s): far fewer
	// refreshes than requests.
	if cat.refreshes == 0 || cat.refreshes > 2 {
		t.Errorf("catalog refreshes %d", cat.refreshes)
	}
}

func TestPrepareRefreshesWhenStale(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	inner := catalog.NewMemory()
	cat := &countingCatalog{Catalog: inner}
	ensureErr := errors.New("dynamodb: connection refused")
	ensures := 0
	app := &App{
		Catalog: cat, MaxAge: 2 * time.Second, Now: func() time.Time { return now },
		Ensure: func(context.Context) error {
			ensures++
			return ensureErr
		},
	}
	s := newStack(t, inner, app)
	ping := event{method: "GET", query: q("query", "SELECT 1")}

	// Ensure fails: the request fails with Unavailable, nothing is refreshed,
	// and the next request retries.
	r := s.invoke(t, ping)
	if r.status != cherr.HTTPStatus(cherr.Unavailable) || r.header.Get("X-ClickHouse-Exception-Code") != "999" ||
		!strings.Contains(string(r.body), "Code: 999. DB::Exception: system tables not ready") ||
		!strings.HasSuffix(string(r.body), "(KEEPER_EXCEPTION) (version 25.8.1.1)\n") {
		t.Fatalf("ensure failure: %d %s", r.status, r.body)
	}
	if ensures != 1 || cat.refreshes != 0 {
		t.Fatalf("ensures %d refreshes %d", ensures, cat.refreshes)
	}
	ensureErr = nil
	if r = s.invoke(t, ping); r.status != 200 || string(r.body) != "1\n" {
		t.Fatalf("after ensure: %d %s", r.status, r.body)
	}
	if ensures != 2 || cat.refreshes != 1 {
		t.Fatalf("ensures %d refreshes %d", ensures, cat.refreshes)
	}
	// Within MaxAge: no refresh, and Ensure is not called again.
	now = now.Add(2 * time.Second)
	s.invoke(t, ping)
	if ensures != 2 || cat.refreshes != 1 {
		t.Fatalf("fresh snapshot: ensures %d refreshes %d", ensures, cat.refreshes)
	}
	// Older than MaxAge: refreshed.
	now = now.Add(time.Millisecond)
	s.invoke(t, ping)
	if cat.refreshes != 2 {
		t.Fatalf("stale snapshot: refreshes %d", cat.refreshes)
	}
	// A failing refresh keeps serving the last snapshot and is retried on
	// the next request.
	cat.fail = errors.New("throttled")
	now = now.Add(3 * time.Second)
	if r = s.invoke(t, ping); r.status != 200 || cat.refreshes != 3 {
		t.Fatalf("failed refresh: %d %s refreshes %d", r.status, r.body, cat.refreshes)
	}
	if r = s.invoke(t, ping); r.status != 200 || cat.refreshes != 4 {
		t.Fatalf("failed refresh retry: %d refreshes %d", r.status, cat.refreshes)
	}
	cat.fail = nil
	s.invoke(t, ping)
	if cat.refreshes != 5 {
		t.Fatalf("recovered refresh: refreshes %d", cat.refreshes)
	}
	now = now.Add(time.Second)
	s.invoke(t, ping)
	if cat.refreshes != 5 {
		t.Fatalf("after recovery the snapshot is fresh again: refreshes %d", cat.refreshes)
	}

	// Without a snapshot a failing refresh fails the request.
	inner2 := catalog.NewMemory()
	cat2 := &countingCatalog{Catalog: inner2, fail: errors.New("no table")}
	s2 := newStack(t, inner2, &App{Catalog: cat2, MaxAge: time.Second})
	if r = s2.invoke(t, ping); r.status != cherr.HTTPStatus(cherr.Unavailable) || !strings.Contains(string(r.body), "cannot load catalog: no table") {
		t.Fatalf("first refresh failure: %d %s", r.status, r.body)
	}
}

func TestFinalizeForcesBase64ForBinaryContent(t *testing.T) {
	text := events.APIGatewayProxyResponse{StatusCode: 200, Body: "a\t1\n",
		MultiValueHeaders: map[string][]string{"Content-Type": {"text/tab-separated-values; charset=UTF-8"}}}
	if got := finalize(text); got.IsBase64Encoded || got.Body != text.Body {
		t.Errorf("text changed: %+v", got)
	}
	jsonResp := events.APIGatewayProxyResponse{StatusCode: 200, Body: "{}",
		MultiValueHeaders: map[string][]string{"Content-Type": {"application/json; charset=UTF-8"}}}
	if got := finalize(jsonResp); got.IsBase64Encoded {
		t.Errorf("json changed: %+v", got)
	}
	ndjson := events.APIGatewayProxyResponse{StatusCode: 200, Body: "{}\n",
		MultiValueHeaders: map[string][]string{"Content-Type": {"application/x-ndjson; charset=UTF-8"}}}
	if got := finalize(ndjson); got.IsBase64Encoded {
		t.Errorf("ndjson changed: %+v", got)
	}
	// Valid UTF-8 that is binary by content type (a short Native block).
	native := events.APIGatewayProxyResponse{StatusCode: 200, Body: "\x01\x01\x01x\x05UInt8\x07",
		MultiValueHeaders: map[string][]string{"Content-Type": {"application/octet-stream"}}}
	got := finalize(native)
	if !got.IsBase64Encoded || got.Body != base64.StdEncoding.EncodeToString([]byte(native.Body)) {
		t.Errorf("native not encoded: %+v", got)
	}
	// Compressed text that happens to be valid UTF-8, via the Headers map.
	gz := events.APIGatewayProxyResponse{StatusCode: 200, Body: "abc",
		Headers: map[string]string{"content-type": "text/plain", "content-encoding": "gzip"}}
	if got := finalize(gz); !got.IsBase64Encoded {
		t.Errorf("gzip not encoded: %+v", got)
	}
	// Already encoded, empty or untyped bodies are left alone.
	already := events.APIGatewayProxyResponse{StatusCode: 200, Body: "AQ==", IsBase64Encoded: true,
		MultiValueHeaders: map[string][]string{"Content-Type": {"application/octet-stream"}}}
	if got := finalize(already); !got.IsBase64Encoded || got.Body != already.Body {
		t.Errorf("already encoded changed: %+v", got)
	}
	empty := events.APIGatewayProxyResponse{StatusCode: 200, MultiValueHeaders: map[string][]string{"Content-Type": {"application/octet-stream"}}}
	if got := finalize(empty); got.IsBase64Encoded {
		t.Errorf("empty changed: %+v", got)
	}
	untyped := events.APIGatewayProxyResponse{StatusCode: 204, Body: "x"}
	if got := finalize(untyped); got.IsBase64Encoded {
		t.Errorf("untyped changed: %+v", got)
	}
}

func TestMiddlewareRestoresContentLength(t *testing.T) {
	var seen string
	var seenLen int64
	h := (&App{Catalog: catalog.NewMemory(), MaxAge: time.Second}).Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen, seenLen = r.Header.Get("Content-Length"), r.ContentLength
		w.WriteHeader(200)
	}))
	handle := NewHandler(h)
	if _, err := handle(context.Background(), event{method: "POST", body: []byte("SELECT 1"), text: true}.proxy()); err != nil {
		t.Fatal(err)
	}
	if seen != "8" || seenLen != 8 {
		t.Errorf("missing header: Content-Length %q (%d)", seen, seenLen)
	}
	if _, err := handle(context.Background(), event{method: "POST", body: []byte("SELECT 1"), text: true, headers: hdr("Content-Length", "8")}.proxy()); err != nil {
		t.Fatal(err)
	}
	if seen != "8" {
		t.Errorf("explicit header: Content-Length %q", seen)
	}
	// An empty POST body is still a POST without Content-Length to the
	// server, which answers 411 like ClickHouse... unless the header says 0.
	if _, err := handle(context.Background(), event{method: "POST"}.proxy()); err != nil {
		t.Fatal(err)
	}
	if seen != "0" || seenLen != 0 {
		t.Errorf("empty body: Content-Length %q (%d)", seen, seenLen)
	}
	// GET requests are untouched.
	if _, err := handle(context.Background(), event{method: "GET", query: q("query", "SELECT 1")}.proxy()); err != nil {
		t.Fatal(err)
	}
	if seen != "" {
		t.Errorf("GET: Content-Length %q", seen)
	}
}
