package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime/metrics"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec"
	"github.com/gfronza/twinhouse/internal/format"
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
	// Cut is set when the server closed the connection before the end of
	// the body.
	Cut bool
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
	cut := errors.Is(err, io.ErrUnexpectedEOF)
	if err != nil && !cut {
		t.Fatal(err)
	}
	return resp{Status: res.StatusCode, Header: res.Header, Body: string(b), Cut: cut}
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

func httpTestServer(t *testing.T, h http.Handler) string {
	t.Helper()
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts.URL
}

// guardServerTest skips under -short and makes a regression fail fast
// instead of exhausting the machine: the process exits once the heap
// passes 1 GiB.
func guardServerTest(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("heavy: skipped with -short")
	}
	name := t.Name()
	done := make(chan struct{})
	go func() {
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				if heapObjects() > 1<<30 {
					fmt.Fprintf(os.Stderr, "%s: heap passed 1 GiB\n", name)
					os.Exit(3)
				}
			}
		}
	}()
	t.Cleanup(func() { close(done) })
}

func heapObjects() uint64 {
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}
