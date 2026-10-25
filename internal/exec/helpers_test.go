package exec

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/format"
	"dynamodb-eventstream/internal/storage"
	"dynamodb-eventstream/internal/storage/memstore"
)

// testEnv is an engine over memstore and the in-memory catalog.
type testEnv struct {
	t     testing.TB
	e     *Engine
	store *memstore.Store
	spy   *spyStore
	cat   catalog.Catalog
}

func newTestEnv(t testing.TB) *testEnv {
	t.Helper()
	ms := memstore.New()
	spy := &spyStore{Store: ms}
	cat := catalog.NewMemory()
	e, err := New(Config{Store: spy, Catalog: cat, InlineViews: true, ConsistentReads: true, Timezone: "UTC",
		ServerVersion: "25.8.1.1", ServerRevision: 54477, DisplayName: "test"})
	if err != nil {
		t.Fatal(err)
	}
	e.impl.fresh.interval = 0 // the memory catalog's Refresh is free
	return &testEnv{t: t, e: e, store: ms, spy: spy, cat: cat}
}

// exec runs one statement and fails the test on error.
func (te *testEnv) exec(sql string, params ...string) *Result {
	te.t.Helper()
	res, err := te.run(sql, params...)
	if err != nil {
		te.t.Fatalf("query %q failed: %v", sql, err)
	}
	return res
}

// run executes sql with params given as name, value pairs.
func (te *testEnv) run(sql string, params ...string) (*Result, error) {
	return te.runReq(&Request{Query: sql}, params...)
}

func (te *testEnv) runReq(req *Request, params ...string) (*Result, error) {
	if len(params)%2 != 0 {
		te.t.Fatal("params must be name/value pairs")
	}
	if req.Params == nil {
		req.Params = map[string]string{}
	}
	for i := 0; i < len(params); i += 2 {
		req.Params[params[i]] = params[i+1]
	}
	return te.e.Execute(context.Background(), req)
}

// tsv runs a query and renders it as TabSeparatedWithNamesAndTypes.
func (te *testEnv) tsv(sql string, params ...string) string {
	te.t.Helper()
	return renderTSV(te.t, te.exec(sql, params...))
}

func renderTSV(t testing.TB, res *Result) string {
	t.Helper()
	var buf bytes.Buffer
	enc, err := format.NewEncoder("TabSeparatedWithNamesAndTypes", &buf, res.Columns, format.Options{Timezone: "UTC"})
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.WriteRows(res.Rows); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finish(format.Stats{}); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

// rows renders only the data rows (TabSeparated).
func (te *testEnv) rows(sql string, params ...string) string {
	te.t.Helper()
	res := te.exec(sql, params...)
	var buf bytes.Buffer
	enc, err := format.NewEncoder("TabSeparated", &buf, res.Columns, format.Options{Timezone: "UTC"})
	if err != nil {
		te.t.Fatal(err)
	}
	if err := enc.WriteRows(res.Rows); err != nil {
		te.t.Fatal(err)
	}
	if err := enc.Finish(format.Stats{}); err != nil {
		te.t.Fatal(err)
	}
	return buf.String()
}

func (te *testEnv) expectRows(sql, want string, params ...string) {
	te.t.Helper()
	got := te.rows(sql, params...)
	if strings.TrimRight(got, "\n") != strings.TrimRight(want, "\n") {
		te.t.Errorf("query %s\n got: %q\nwant: %q", sql, got, want)
	}
}

func (te *testEnv) expectErr(sql string, code int, params ...string) {
	te.t.Helper()
	_, err := te.run(sql, params...)
	if err == nil {
		te.t.Errorf("query %s: expected error code %d, got success", sql, code)
		return
	}
	if !cherr.Is(err, code) {
		te.t.Errorf("query %s: expected error code %d, got %v", sql, code, err)
	}
}

// spyStore records scan requests.
type spyStore struct {
	*memstore.Store
	mu    sync.Mutex
	scans []storage.ScanRequest
}

func (s *spyStore) Scan(ctx context.Context, t *catalog.Table, req storage.ScanRequest) (storage.RowIterator, error) {
	s.mu.Lock()
	s.scans = append(s.scans, req)
	s.mu.Unlock()
	return s.Store.Scan(ctx, t, req)
}

func (s *spyStore) lastScan() storage.ScanRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.scans) == 0 {
		return storage.ScanRequest{}
	}
	return s.scans[len(s.scans)-1]
}
