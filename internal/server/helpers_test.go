package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"dynamodb-eventstream/internal/chtype"
	"dynamodb-eventstream/internal/exec"
	"dynamodb-eventstream/internal/format"
	"dynamodb-eventstream/internal/sqlparse"
)

// seenRequest is a copy of an exec.Request with Data read into memory.
type seenRequest struct {
	exec.Request
	DataBytes []byte
	HasData   bool
}

// fakeExec is a scriptable Executor that records requests.
type fakeExec struct {
	mu   sync.Mutex
	reqs []seenRequest
	fn   func(ctx context.Context, req *exec.Request) (*exec.Result, error)
	// keepData leaves req.Data unread for fn.
	keepData bool
}

func (f *fakeExec) Execute(ctx context.Context, req *exec.Request) (*exec.Result, error) {
	sr := seenRequest{Request: *req}
	if req.Data != nil && !f.keepData {
		b, err := io.ReadAll(req.Data)
		if err != nil {
			return nil, err
		}
		sr.DataBytes, sr.HasData = b, true
		sr.Data = nil
	}
	f.mu.Lock()
	f.reqs = append(f.reqs, sr)
	f.mu.Unlock()
	if f.fn == nil {
		return &exec.Result{}, nil
	}
	return f.fn(ctx, req)
}

func (f *fakeExec) last(t *testing.T) seenRequest {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.reqs) == 0 {
		t.Fatal("executor was not called")
	}
	return f.reqs[len(f.reqs)-1]
}

func (f *fakeExec) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reqs)
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newTestServer(t *testing.T, fe *fakeExec, mod func(*Config)) (*Server, *httptest.Server) {
	t.Helper()
	cfg := Config{
		Executor:      fe,
		ServerVersion: "25.8.1.1",
		DisplayName:   "test-node",
		Timezone:      "UTC",
		Logger:        quietLogger(),
	}
	if mod != nil {
		mod(&cfg)
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s)
	t.Cleanup(ts.Close)
	return s, ts
}

type resp struct {
	Status int
	Header http.Header
	Body   string
}

func do(t *testing.T, req *http.Request) resp {
	t.Helper()
	tr := &http.Transport{DisableCompression: true}
	res, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp{Status: res.StatusCode, Header: res.Header, Body: string(b)}
}

func get(t *testing.T, url string, hdr ...string) resp {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	setHeaders(req, hdr)
	return do(t, req)
}

func post(t *testing.T, url, body string, hdr ...string) resp {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	setHeaders(req, hdr)
	return do(t, req)
}

func setHeaders(req *http.Request, kv []string) {
	for i := 0; i+1 < len(kv); i += 2 {
		req.Header.Set(kv[i], kv[i+1])
	}
}

// oneRow is a result with a single UInt8 column "1".
func oneRow() *exec.Result {
	return &exec.Result{
		Columns: []format.Column{{Name: "1", Type: chtype.TUInt8}},
		Rows:    [][]any{{uint64(1)}},
	}
}

func stringRows(name string, vals ...string) *exec.Result {
	r := &exec.Result{Columns: []format.Column{{Name: name, Type: chtype.TString}}}
	for _, v := range vals {
		r.Rows = append(r.Rows, []any{v})
	}
	return r
}

func formatCanonical(name string) (string, bool) { return format.Canonical(name) }

// parserReady reports whether sqlparse is implemented (it is developed in
// parallel; tests that need it skip until then).
func parserReady() bool {
	_, err := sqlparse.Parse("SET a = 1")
	return err == nil
}

// encodersReady reports whether TSV output works end to end.
func encodersReady() bool {
	var b strings.Builder
	enc, err := format.NewEncoder("TabSeparated", &b, []format.Column{{Name: "x", Type: chtype.TString}}, format.Options{})
	if err != nil {
		return false
	}
	if enc.WriteRows([][]any{{"a"}}) != nil || enc.Finish(format.Stats{}) != nil {
		return false
	}
	return b.String() == "a\n"
}

func requireEncoders(t *testing.T) {
	t.Helper()
	if !encodersReady() {
		t.Skip("format/chtype encoders not implemented yet")
	}
}

func httpTestServer(t *testing.T, h http.Handler) string {
	t.Helper()
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts.URL
}
