package server

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/exec"
)

// stallingExec waits for ctx like a storage call to an unresponsive
// DynamoDB endpoint, and reports the cancellation the way the storage layer
// wraps it.
func stallingExec() *fakeExec {
	return &fakeExec{fn: func(ctx context.Context, _ *exec.Request) (*exec.Result, error) {
		select {
		case <-ctx.Done():
			return nil, cherr.Wrap(cherr.Unavailable, ctx.Err(), "cannot query DynamoDB table")
		case <-time.After(10 * time.Second):
			return oneRow(), nil
		}
	}}
}

func TestMaxExecutionTime(t *testing.T) {
	_, ts := newTestServer(t, stallingExec(), nil)
	cases := []struct{ name, url, body string }{
		{"url setting", ts.URL + "/?max_execution_time=0.2", "SELECT count() FROM t"},
		{"settings clause", ts.URL + "/", "SELECT count() FROM t SETTINGS max_execution_time=0.2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			r := post(t, tc.url, tc.body)
			if el := time.Since(start); el > 5*time.Second {
				t.Fatalf("took %s", el)
			}
			if r.Status != 408 || r.Header.Get("X-ClickHouse-Exception-Code") != "159" {
				t.Fatalf("status %d code %q body %q", r.Status, r.Header.Get("X-ClickHouse-Exception-Code"), r.Body)
			}
			if !strings.HasPrefix(r.Body, "Code: 159. DB::Exception: Timeout exceeded: elapsed ") ||
				!strings.Contains(r.Body, " ms, maximum: 200 ms. (TIMEOUT_EXCEEDED)") {
				t.Fatalf("body %q", r.Body)
			}
		})
	}
}

func TestMaxExecutionTimeServerDefault(t *testing.T) {
	_, ts := newTestServer(t, stallingExec(), func(c *Config) { c.MaxExecutionTime = 100 * time.Millisecond })
	r := post(t, ts.URL+"/", "SELECT 1")
	if r.Status != 408 || !strings.Contains(r.Body, "maximum: 100 ms") {
		t.Fatalf("server default: %d %q", r.Status, r.Body)
	}
	// An explicit 0 disables the default (fast executor: no timeout).
	fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) { return oneRow(), nil }}
	_, ts2 := newTestServer(t, fe, func(c *Config) { c.MaxExecutionTime = time.Nanosecond })
	if r := post(t, ts2.URL+"/?max_execution_time=0", "SELECT 1"); r.Status != 200 {
		t.Fatalf("max_execution_time=0: %d %q", r.Status, r.Body)
	}
}

func TestMaxExecutionTimeParse(t *testing.T) {
	fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) { return oneRow(), nil }}
	_, ts := newTestServer(t, fe, nil)
	for v, want := range map[string]string{
		"abc": "Code: 27. DB::Exception: Cannot parse input: expected 'eof' before: 'abc'. (CANNOT_PARSE_INPUT_ASSERTION_FAILED)",
		"2s":  "Code: 27. DB::Exception: Cannot parse input: expected 'eof' before: 's'. (CANNOT_PARSE_INPUT_ASSERTION_FAILED)",
		"":    "Code: 72. DB::Exception: Cannot read floating point value. (CANNOT_PARSE_NUMBER)",
		"-1":  "",
		"1.5": "",
	} {
		r := post(t, ts.URL+"/?max_execution_time="+url.QueryEscape(v), "SELECT 1")
		if want == "" {
			if r.Status != 200 {
				t.Errorf("%q: %d %q", v, r.Status, r.Body)
			}
			continue
		}
		if r.Status != 400 || !strings.HasPrefix(r.Body, want) {
			t.Errorf("%q: %d %q", v, r.Status, r.Body)
		}
	}
}

// With timeout_overflow_mode = 'break' a streamed SELECT that reaches
// max_execution_time ends normally after the rows sent so far, as in
// ClickHouse; with throw the rows sent are followed by 159 (a selective
// filter's blocks end with their source blocks, so rows go out before the
// deadline; ClickHouse 25.8 also sends 200, rows, then 159).
func TestTimeoutBreakEndsTheResult(t *testing.T) {
	guardServerTest(t)
	base := newEngineServer(t, 0, func(c *Config) { c.ResponseBufferSize = 1 })
	const sql = "SELECT number FROM system.numbers WHERE number % 100000 = 0 SETTINGS max_execution_time = 0.3, timeout_overflow_mode = "
	r := query(t, base, sql+"'break'", nil)
	if r.Status != 200 || r.Cut || !strings.HasPrefix(r.Body, "0\n") || strings.Contains(r.Body, "Code:") {
		t.Errorf("break: %d cut %v %q", r.Status, r.Cut, r.Body[max(0, len(r.Body)-200):])
	}
	r = query(t, base, sql+"'throw'", nil)
	if r.Status != 200 || !r.Cut || !strings.HasPrefix(r.Body, "0\n") || !strings.Contains(r.Body, "\n__exception__\r\nCode: 159. DB::Exception: Timeout exceeded") {
		t.Errorf("throw: %d cut %v %q", r.Status, r.Cut, r.Body[max(0, len(r.Body)-200):])
	}
}
