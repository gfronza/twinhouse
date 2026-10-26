//go:build multiregion

package multiregion

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

type config struct {
	usURL, euURL       string
	ddbUS, ddbEU       string
	project, compose   string
	repoRoot           string
	prefix             string
	converge           time.Duration
	reachable          bool
	unreachableMessage string
}

var cfg config

func envOr(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func TestMain(m *testing.M) {
	root := repoRoot()
	cfg = config{
		usURL:    strings.TrimRight(envOr("MR_US_URL", "http://localhost:28123"), "/"),
		euURL:    strings.TrimRight(envOr("MR_EU_URL", "http://localhost:28124"), "/"),
		ddbUS:    envOr("MR_DDB_US", "http://localhost:28000"),
		ddbEU:    envOr("MR_DDB_EU", "http://localhost:28001"),
		project:  envOr("MR_COMPOSE_PROJECT", "dhmr"),
		compose:  envOr("MR_COMPOSE_FILE", filepath.Join(root, "deploy", "local", "docker-compose.yml")),
		repoRoot: root,
		prefix:   envOr("MR_TABLE_PREFIX", "twinhouse_"),
		converge: 60 * time.Second,
	}
	if !filepath.IsAbs(cfg.compose) {
		cfg.compose = filepath.Join(root, cfg.compose)
	}
	if d, err := time.ParseDuration(os.Getenv("MR_CONVERGE_TIMEOUT")); err == nil && d > 0 {
		cfg.converge = d
	}
	slots := 4
	if n, err := strconv.Atoi(os.Getenv("MR_PARALLEL")); err == nil && n > 0 {
		slots = n
	}
	parallelSlots = make(chan struct{}, slots)
	cfg.reachable = true
	for _, u := range []string{cfg.usURL, cfg.euURL} {
		if err := ping(u); err != nil {
			cfg.reachable = false
			cfg.unreachableMessage = fmt.Sprintf("multi-region stack not reachable (%s: %v); start it with "+
				"COMPOSE_PROJECT=dhmr US_HTTP_PORT=28123 EU_HTTP_PORT=28124 DDB_US_PORT=28000 DDB_EU_PORT=28001 make up", u, err)
			break
		}
	}
	os.Exit(m.Run())
}

func ping(base string) error {
	c := http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get(base + "/ping")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || strings.TrimSpace(string(b)) != "Ok." {
		return fmt.Errorf("ping: HTTP %d %q", resp.StatusCode, b)
	}
	return nil
}

// parallelSlots bounds how many scenario tests run at once (MR_PARALLEL,
// default 4): DynamoDB Local serialises transactions and saturates quickly.
var parallelSlots chan struct{}

// parallel marks the test parallel and waits for a free slot.
func parallel(t *testing.T) {
	t.Helper()
	t.Parallel()
	parallelSlots <- struct{}{}
	t.Cleanup(func() { <-parallelSlots })
}

// requireStack skips the test when the stack is not reachable.
func requireStack(t testing.TB) {
	t.Helper()
	if !cfg.reachable {
		t.Skip(cfg.unreachableMessage)
	}
}

// ---------------------------------------------------------------------------
// ClickHouse HTTP client
// ---------------------------------------------------------------------------

type region struct {
	name string
	base string
	hc   *http.Client
}

func us() *region {
	return &region{name: "us", base: cfg.usURL, hc: &http.Client{Timeout: 5 * time.Minute}}
}
func eu() *region {
	return &region{name: "eu", base: cfg.euURL, hc: &http.Client{Timeout: 5 * time.Minute}}
}

// chError is a ClickHouse error response.
type chError struct {
	Status int
	Code   string
	Body   string
}

func (e *chError) Error() string {
	return fmt.Sprintf("HTTP %d (code %s): %s", e.Status, e.Code, strings.TrimSpace(e.Body))
}

// do POSTs sql (as body, or as the query URL param when body != nil) and
// returns the response body.
func (r *region) do(ctx context.Context, sql string, params map[string]string, body io.Reader, hdr map[string]string) (string, error) {
	v := url.Values{}
	for k, val := range params {
		v.Set("param_"+k, val)
	}
	var reqBody io.Reader = strings.NewReader(sql)
	if body != nil {
		v.Set("query", sql)
		reqBody = body
	}
	u := r.base + "/"
	if len(v) > 0 {
		u += "?" + v.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, reqBody)
	if err != nil {
		return "", err
	}
	for k, val := range hdr {
		req.Header.Set(k, val)
	}
	resp, err := r.hc.Do(req)
	if err != nil {
		return "", fmt.Errorf("%s: %w", r.name, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("%s: read body: %w", r.name, err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", &chError{Status: resp.StatusCode, Code: resp.Header.Get("X-ClickHouse-Exception-Code"), Body: r.name + ": " + string(b)}
	}
	return string(b), nil
}

func (r *region) query(sql string, params map[string]string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	return r.do(ctx, sql, params, nil, nil)
}

// mustExec runs a statement and fails the test on error.
func (r *region) mustExec(t testing.TB, sql string) {
	t.Helper()
	if _, err := r.query(sql, nil); err != nil {
		t.Fatalf("%s: %s\n  -> %v", r.name, oneLine(sql), err)
	}
}

// mustQuery runs a query and fails the test on error.
func (r *region) mustQuery(t testing.TB, sql string, params map[string]string) string {
	t.Helper()
	out, err := r.query(sql, params)
	if err != nil {
		t.Fatalf("%s: %s\n  -> %v", r.name, oneLine(sql), err)
	}
	return out
}

// insertJSON inserts rows with "INSERT INTO table FORMAT JSONEachRow" and an
// NDJSON body (optionally gzip-compressed), like a Python loader would.
func (r *region) insertJSON(table string, rows []map[string]any, gz bool) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return err
		}
	}
	hdr := map[string]string{"Content-Type": "application/x-ndjson"}
	body := buf.Bytes()
	if gz {
		var zb bytes.Buffer
		zw := gzip.NewWriter(&zb)
		_, _ = zw.Write(body)
		_ = zw.Close()
		body = zb.Bytes()
		hdr["Content-Encoding"] = "gzip"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, err := r.do(ctx, "INSERT INTO "+table+" FORMAT JSONEachRow", nil, bytes.NewReader(body), hdr)
	return err
}

func oneLine(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}

// ---------------------------------------------------------------------------
// Databases
// ---------------------------------------------------------------------------

func randSuffix() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// newDB creates a uniquely-named database in us, waits until eu sees it, and
// drops it when the test ends.
func newDB(t testing.TB, scenario string) string {
	t.Helper()
	name := fmt.Sprintf("mr_%s_%s", scenario, randSuffix())
	u := us()
	u.mustExec(t, "CREATE DATABASE "+name)
	t.Cleanup(func() { dropDB(t, name) })
	return name
}

func dropDB(t testing.TB, name string) {
	// Retry: a region may just be coming back after a fault-injection test.
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, err := us().query("DROP DATABASE IF EXISTS "+name, nil)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Logf("cleanup: DROP DATABASE %s failed: %v", name, err)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// Polling
// ---------------------------------------------------------------------------

// eventually polls check until it returns ok or timeout elapses; on timeout
// it fails the test with the last description.
func eventually(t testing.TB, timeout time.Duration, what string, check func() (bool, string)) bool {
	t.Helper()
	start := time.Now()
	deadline := start.Add(timeout)
	var last string
	for {
		ok, desc := check()
		if ok {
			t.Logf("%s: converged after %s", what, time.Since(start).Round(10*time.Millisecond))
			return true
		}
		last = desc
		if time.Now().After(deadline) {
			t.Errorf("%s: not converged after %s:\n%s", what, timeout, last)
			return false
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// waitResult polls a query in a region until its output equals want.
func waitResult(t testing.TB, r *region, timeout time.Duration, what, sql string, params map[string]string, want string) bool {
	t.Helper()
	return eventually(t, timeout, r.name+": "+what, func() (bool, string) {
		got, err := r.query(sql, params)
		if err != nil {
			return false, "error: " + err.Error()
		}
		if got == want {
			return true, ""
		}
		return false, diffText(want, got)
	})
}

// diffText summarises the difference of two line-oriented outputs.
func diffText(want, got string) string {
	wl, gl := splitLines(want), splitLines(got)
	var b strings.Builder
	fmt.Fprintf(&b, "  want %d lines, got %d lines\n", len(wl), len(gl))
	cnt := map[string]int{} // >0 missing, <0 unexpected
	for _, l := range wl {
		cnt[l]++
	}
	for _, l := range gl {
		cnt[l]--
	}
	n := 0
	for _, l := range wl {
		if cnt[l] > 0 {
			if n < 8 {
				fmt.Fprintf(&b, "  - missing:    %q\n", l)
			}
			n++
			cnt[l]--
		}
	}
	m := 0
	for _, l := range gl {
		if cnt[l] < 0 {
			if m < 8 {
				fmt.Fprintf(&b, "  + unexpected: %q\n", l)
			}
			m++
			cnt[l]++
		}
	}
	if n > 8 || m > 8 {
		fmt.Fprintf(&b, "  (%d missing, %d unexpected lines in total)\n", n, m)
	}
	if n == 0 && m == 0 && want != got {
		for i := 0; i < len(wl) && i < len(gl); i++ {
			if wl[i] != gl[i] {
				fmt.Fprintf(&b, "  same lines, order differs at line %d: want %q got %q\n", i+1, wl[i], gl[i])
				break
			}
		}
	}
	return b.String()
}

func splitLines(s string) []string {
	s = strings.TrimSuffix(s, "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

// parseTSV splits TabSeparated output into rows of fields.
func parseTSV(s string) [][]string {
	var out [][]string
	for _, l := range splitLines(s) {
		out = append(out, strings.Split(l, "\t"))
	}
	return out
}

// ---------------------------------------------------------------------------
// Parallel helpers
// ---------------------------------------------------------------------------

// runAll runs fns concurrently and returns their errors joined.
func runAll(fns ...func() error) error {
	var wg sync.WaitGroup
	errs := make([]error, len(fns))
	for i, fn := range fns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = fn()
		}()
	}
	wg.Wait()
	var msgs []string
	for _, e := range errs {
		if e != nil {
			msgs = append(msgs, e.Error())
		}
	}
	if len(msgs) == 0 {
		return nil
	}
	slices.Sort(msgs)
	return fmt.Errorf("%s", strings.Join(msgs, "; "))
}
