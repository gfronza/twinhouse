//go:build e2e

package e2e

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var (
	baseURL = strings.TrimRight(envOr("TWINHOUSE_URL", "http://localhost:8123"), "/")
	runID   = newRunID()

	reachOnce sync.Once
	reachErr  error
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func newRunID() string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%d_%s", time.Now().Unix()%1000000, hex.EncodeToString(b))
}

// hostPort returns host:port of TWINHOUSE_URL for clickhouse-go's Addr.
func hostPort() string {
	u, err := url.Parse(baseURL)
	if err != nil || u.Host == "" {
		return strings.TrimPrefix(strings.TrimPrefix(baseURL, "http://"), "https://")
	}
	return u.Host
}

// lambdaMode is true when TWINHOUSE_URL is the serverless deployment (the API
// Lambda behind API Gateway, or its `sam local start-api` emulation): HTTP
// sessions are rejected with NOT_IMPLEMENTED (48) and bodies are bounded by
// the Lambda payload limit (6 MB). Detected by probe (detect), or forced
// with E2E_LAMBDA=1. Tests of those features skip (skipInLambda) instead of
// weakening their assertions.
var lambdaMode bool

// detect pings the target once and probes for Lambda mode.
func detect() {
	reachOnce.Do(func() {
		c := &http.Client{Timeout: 3 * time.Second}
		resp, err := c.Get(baseURL + "/ping")
		if err != nil {
			reachErr = err
			return
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 || string(b) != "Ok.\n" {
			reachErr = fmt.Errorf("ping: status %d body %q", resp.StatusCode, b)
			return
		}
		if os.Getenv("E2E_LAMBDA") == "1" {
			lambdaMode = true
			return
		}
		pr, err := c.Get(baseURL + "/?" + url.Values{"session_id": {"e2e-probe-" + runID}, "query": {"SELECT 1"}}.Encode())
		if err != nil {
			return
		}
		defer pr.Body.Close()
		_, _ = io.Copy(io.Discard, pr.Body)
		lambdaMode = pr.Header.Get("X-ClickHouse-Exception-Code") == "48"
	})
}

// need skips the test when the target server is unreachable.
func need(t testing.TB) {
	t.Helper()
	detect()
	if reachErr != nil {
		t.Skipf("chserver at %s unreachable: %v", baseURL, reachErr)
	}
}

// skipInLambda skips a test of a feature the serverless deployment does not
// offer (sessions, bodies above the Lambda payload limit).
func skipInLambda(t testing.TB, feature string) {
	t.Helper()
	need(t)
	if lambdaMode {
		t.Skipf("%s: not available in Lambda mode (serverless deployment)", feature)
	}
}

// acceptAll sets Accept: */* in Lambda mode when the request has no Accept:
// API Gateway (BinaryMediaTypes */*) and its emulator base64-decode a binary
// response only for a request whose Accept matches a binary media type, and
// Go clients send no Accept by default (curl and Python requests send */*).
func acceptAll(h http.Header) {
	if lambdaMode && h.Get("Accept") == "" {
		h.Set("Accept", "*/*")
	}
}

// ---------------------------------------------------------------------------
// Raw HTTP helpers

// Generous timeouts: DynamoDB Local is slow under concurrent load (inline
// materialized views do several writes per inserted row).
const opTimeout = 5 * time.Minute

var httpClient = &http.Client{Timeout: opTimeout, Transport: &http.Transport{DisableCompression: true}}

type httpResp struct {
	Status int
	Header http.Header
	Body   string
}

// req is a raw HTTP request description.
type req struct {
	Method  string // default POST
	Path    string // default "/"
	Params  url.Values
	Body    io.Reader
	Headers map[string]string
}

func doHTTP(t testing.TB, r req) httpResp {
	t.Helper()
	resp, err := doHTTPErr(r)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// doHTTPErr is doHTTP without testing.TB (safe in goroutines).
func doHTTPErr(r req) (httpResp, error) {
	if r.Method == "" {
		r.Method = http.MethodPost
	}
	if r.Path == "" {
		r.Path = "/"
	}
	u := baseURL + r.Path
	if len(r.Params) > 0 {
		u += "?" + r.Params.Encode()
	}
	hr, err := http.NewRequest(r.Method, u, r.Body)
	if err != nil {
		return httpResp{}, fmt.Errorf("new request: %w", err)
	}
	for k, v := range r.Headers {
		hr.Header.Set(k, v)
	}
	detect()
	acceptAll(hr.Header)
	resp, err := httpClient.Do(hr)
	if err != nil {
		return httpResp{}, fmt.Errorf("%s %s: %w", r.Method, abbrev(u), err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return httpResp{}, fmt.Errorf("read body: %w", err)
	}
	return httpResp{Status: resp.StatusCode, Header: resp.Header, Body: string(b)}, nil
}

// post sends sql as the POST body (optionally in database db) and returns the response.
func post(t testing.TB, db, sqlText string) httpResp {
	t.Helper()
	p := url.Values{}
	if db != "" {
		p.Set("database", db)
	}
	return doHTTP(t, req{Params: p, Body: strings.NewReader(sqlText)})
}

// mustExec runs a statement over raw HTTP and fails the test on error.
func mustExec(t testing.TB, db, sqlText string) {
	t.Helper()
	r := post(t, db, sqlText)
	if r.Status != 200 {
		t.Fatalf("exec %q: HTTP %d: %s", abbrev(sqlText), r.Status, r.Body)
	}
}

// tsv runs a query and returns its TabSeparated output.
func tsv(t testing.TB, db, sqlText string) string {
	t.Helper()
	r := post(t, db, sqlText)
	if r.Status != 200 {
		t.Fatalf("query %q: HTTP %d: %s", abbrev(sqlText), r.Status, r.Body)
	}
	return r.Body
}

func abbrev(s string) string {
	if len(s) > 300 {
		return s[:300] + "..."
	}
	return s
}

// newDB creates a uniquely named database for this test and drops it at the end.
func newDB(t testing.TB, name string) string {
	t.Helper()
	need(t)
	db := fmt.Sprintf("e2e_%s_%s", runID, name)
	mustExec(t, "", "CREATE DATABASE "+db)
	t.Cleanup(func() {
		r := post(t, "", "DROP DATABASE IF EXISTS "+db)
		if r.Status != 200 {
			t.Errorf("cleanup drop %s: HTTP %d: %s", db, r.Status, r.Body)
		}
	})
	return db
}

// ---------------------------------------------------------------------------
// clickhouse-go helpers

func baseOptions(db string) *clickhouse.Options {
	detect()
	o := &clickhouse.Options{
		Addr:     []string{hostPort()},
		Protocol: clickhouse.HTTP,
		Auth: clickhouse.Auth{
			Database: db,
			Username: "default",
			Password: "",
		},
		DialTimeout: 10 * time.Second,
		ReadTimeout: opTimeout,
	}
	if lambdaMode {
		// See acceptAll: clickhouse-go sends no Accept header by default.
		o.HttpHeaders = map[string]string{"Accept": "*/*"}
	}
	return o
}

// openSQL opens a database/sql handle via clickhouse.OpenDB.
func openSQL(t testing.TB, db string, mod ...func(*clickhouse.Options)) *sql.DB {
	t.Helper()
	need(t)
	opts := baseOptions(db)
	for _, m := range mod {
		m(opts)
	}
	conn := clickhouse.OpenDB(opts)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// openNative opens the clickhouse-go native API (driver.Conn) over HTTP.
func openNative(t testing.TB, db string, mod ...func(*clickhouse.Options)) driver.Conn {
	t.Helper()
	need(t)
	opts := baseOptions(db)
	for _, m := range mod {
		m(opts)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		t.Fatalf("clickhouse.Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func ctxT(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 2*opTimeout)
	t.Cleanup(cancel)
	return ctx
}

// wantCode asserts err is (or wraps) a *clickhouse.Exception with the code.
func wantCode(t testing.TB, err error, code int32, what string) {
	t.Helper()
	if err == nil {
		t.Errorf("%s: expected exception code %d, got nil error", what, code)
		return
	}
	var ex *clickhouse.Exception
	if !errors.As(err, &ex) {
		t.Errorf("%s: expected *clickhouse.Exception code %d, got %T: %v", what, code, err, err)
		return
	}
	if ex.Code != code {
		t.Errorf("%s: exception code = %d (%s), want %d; err=%v", what, ex.Code, ex.Name, code, err)
	}
}

func gzipBytes(t testing.TB, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// eventually polls f until it returns nil or the timeout expires.
func eventually(t testing.TB, timeout time.Duration, f func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var err error
	for {
		if err = f(); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s: %v", timeout, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// heavySlots bounds how many write-heavy tests run at once: DynamoDB Local
// is single-process and its throughput collapses under many concurrent
// writers, which only makes every heavy test slower.
var heavySlots = make(chan struct{}, 2)

// heavy marks a write-heavy test; call it right after t.Parallel().
func heavy(t testing.TB) {
	t.Helper()
	need(t)
	heavySlots <- struct{}{}
	t.Cleanup(func() { <-heavySlots })
}

// workerSettle is how long tests give the stream worker to (re)process the
// records of an INSERT before asserting that MV output did not change. A
// worker lagging more than this can only hide bugs (false passes), never
// cause false failures. Override with E2E_WORKER_SETTLE (Go duration), e.g.
// on a heavily loaded stack whose mvworker reports a large lag_ms.
var workerSettle = func() time.Duration {
	if d, err := time.ParseDuration(os.Getenv("E2E_WORKER_SETTLE")); err == nil {
		return d
	}
	return 15 * time.Second
}()

// staysEqual polls a TabSeparated query for workerSettle and reports the
// first time its output differs from want.
func staysEqual(t testing.TB, db, query, want, label string) {
	t.Helper()
	deadline := time.Now().Add(workerSettle)
	for {
		if got := tsv(t, db, query); got != want {
			t.Errorf("%s:\n%s\nwant\n%s", label, got, want)
			return
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}
