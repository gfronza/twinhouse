package server

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/storage"
)

// fakeCatalog serves a settable table list; only the methods the reconciler
// uses are functional.
type fakeCatalog struct {
	catalog.Catalog // nil: other methods panic if called
	mu              sync.Mutex
	dbs             []string
	tables          map[string][]*catalog.Table
	refreshErr      error
	refreshes       int
	// onRefresh, when set, runs (unlocked) at the start of every Refresh
	// with the 1-based refresh count.
	onRefresh func(n int)
	// refreshBlock, when set, makes Refresh wait for ctx to end.
	refreshBlock bool
	quarantine   []catalog.Quarantined
}

func (f *fakeCatalog) Quarantined() []catalog.Quarantined {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.quarantine)
}

func (f *fakeCatalog) Refresh(ctx context.Context) error {
	f.mu.Lock()
	f.refreshes++
	n, hook, block := f.refreshes, f.onRefresh, f.refreshBlock
	f.mu.Unlock()
	if hook != nil {
		hook(n)
	}
	if block {
		<-ctx.Done()
		return ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refreshErr
}

func (f *fakeCatalog) TableByPhysicalName(physical string) (*catalog.Table, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ts := range f.tables {
		for _, t := range ts {
			if t.PhysicalName == physical {
				return t, true
			}
		}
	}
	return nil, false
}

func (f *fakeCatalog) Databases() []*catalog.Database {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*catalog.Database
	for _, d := range f.dbs {
		out = append(out, &catalog.Database{Name: d})
	}
	return out
}

func (f *fakeCatalog) Tables(db string) []*catalog.Table {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tables[db]
}

func (f *fakeCatalog) set(dbs []string, tables ...*catalog.Table) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dbs = dbs
	f.tables = map[string][]*catalog.Table{}
	for _, t := range tables {
		f.tables[t.Database] = append(f.tables[t.Database], t)
	}
}

type fakeStore struct {
	storage.Store // nil: other methods panic if called
	mu            sync.Mutex
	ensured       []string
	dropped       []string
	ensureErr     map[string]error
	dropErr       error
	// block makes EnsureTable/DropTable wait for ctx to end.
	block bool
}

func (f *fakeStore) EnsureTable(ctx context.Context, t *catalog.Table) error {
	if f.block {
		<-ctx.Done()
		return ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.ensureErr[t.PhysicalName]; err != nil {
		return err
	}
	f.ensured = append(f.ensured, t.PhysicalName+"@"+itoa(t.Version))
	return nil
}

func (f *fakeStore) DropTable(_ context.Context, t *catalog.Table) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dropErr != nil {
		return f.dropErr
	}
	f.dropped = append(f.dropped, t.PhysicalName)
	return nil
}

func (f *fakeStore) snapshot() (ensured, dropped []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.ensured), slices.Clone(f.dropped)
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

func tbl(db, name string, version int64) *catalog.Table {
	return &catalog.Table{Database: db, Name: name, PhysicalName: "p_" + db + "_" + name, Version: version}
}

func TestReconciler(t *testing.T) {
	ctx := context.Background()
	cat := &fakeCatalog{}
	st := &fakeStore{ensureErr: map[string]error{}}
	r := NewReconciler(ReconcilerConfig{Catalog: cat, Store: st, Logger: quietLogger()})

	cat.set([]string{"default", "d1"}, tbl("default", "a", 1), tbl("d1", "b", 1))
	r.RunOnce(ctx)
	ensured, dropped := st.snapshot()
	slices.Sort(ensured)
	if want := []string{"p_d1_b@1", "p_default_a@1"}; !slices.Equal(ensured, want) || len(dropped) != 0 {
		t.Fatalf("round 1: ensured %v dropped %v", ensured, dropped)
	}

	// Unchanged versions are not ensured again; a new version is.
	cat.set([]string{"d1"}, tbl("default", "a", 2), tbl("d1", "b", 1))
	r.RunOnce(ctx)
	ensured, _ = st.snapshot()
	if len(ensured) != 3 || ensured[2] != "p_default_a@2" {
		t.Fatalf("round 2: %v", ensured)
	}

	// A failing EnsureTable is retried next round.
	cat.set([]string{"d1"}, tbl("default", "a", 2), tbl("d1", "b", 1), tbl("d1", "c", 1))
	st.ensureErr["p_d1_c"] = errors.New("throttled")
	r.RunOnce(ctx)
	delete(st.ensureErr, "p_d1_c")
	r.RunOnce(ctx)
	ensured, _ = st.snapshot()
	if ensured[len(ensured)-1] != "p_d1_c@1" || len(ensured) != 4 {
		t.Fatalf("retry: %v", ensured)
	}

	// A table that disappears is dropped only after DropAfter (2) rounds.
	cat.set([]string{"d1"}, tbl("default", "a", 2), tbl("d1", "c", 1))
	r.RunOnce(ctx)
	if _, dropped := st.snapshot(); len(dropped) != 0 {
		t.Fatalf("dropped too early: %v", dropped)
	}
	r.RunOnce(ctx)
	if _, dropped := st.snapshot(); !slices.Equal(dropped, []string{"p_d1_b"}) {
		t.Fatalf("drop: %v", dropped)
	}
	r.RunOnce(ctx)
	if _, dropped := st.snapshot(); len(dropped) != 1 {
		t.Fatalf("dropped twice: %v", dropped)
	}

	// A table that reappears before the threshold is kept.
	cat.set([]string{"d1"}, tbl("default", "a", 2))
	r.RunOnce(ctx)
	cat.set([]string{"d1"}, tbl("default", "a", 2), tbl("d1", "c", 1))
	r.RunOnce(ctx)
	r.RunOnce(ctx)
	if _, dropped := st.snapshot(); len(dropped) != 1 {
		t.Fatalf("reappeared table dropped: %v", dropped)
	}

	// Refresh failures never drop anything.
	cat.set(nil)
	cat.refreshErr = errors.New("dynamodb down")
	for i := 0; i < 5; i++ {
		r.RunOnce(ctx)
	}
	if _, dropped := st.snapshot(); len(dropped) != 1 {
		t.Fatalf("dropped on refresh failure: %v", dropped)
	}

	// Drop failures are retried.
	cat.refreshErr = nil
	st.dropErr = errors.New("in use")
	r.RunOnce(ctx)
	r.RunOnce(ctx)
	st.dropErr = nil
	r.RunOnce(ctx)
	if _, dropped := st.snapshot(); len(dropped) != 3 {
		t.Fatalf("drop retry: %v", dropped)
	}
}

func TestReconcilerRunStops(t *testing.T) {
	cat := &fakeCatalog{}
	st := &fakeStore{}
	r := NewReconciler(ReconcilerConfig{Catalog: cat, Store: st, Interval: 5 * time.Millisecond, Logger: quietLogger()})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
	cat.mu.Lock()
	defer cat.mu.Unlock()
	if cat.refreshes < 2 {
		t.Errorf("refreshes %d", cat.refreshes)
	}
}

func TestReconcilerSurvivesPanic(t *testing.T) {
	cat := &fakeCatalog{}
	cat.set([]string{"d"}, tbl("d", "x", 1))
	r := NewReconciler(ReconcilerConfig{Catalog: cat, Store: panicStore{}, Logger: quietLogger()})
	r.RunOnce(context.Background()) // must not panic
}

type panicStore struct{ storage.Store }

func (panicStore) EnsureTable(context.Context, *catalog.Table) error { panic("boom") }

// A table dropped and created again under the same physical name restarts at
// version 1; the new incarnation (CreatedAt) must still be ensured.
func TestReconcilerReensuresNewIncarnation(t *testing.T) {
	ctx := context.Background()
	cat := &fakeCatalog{}
	st := &fakeStore{ensureErr: map[string]error{}}
	r := NewReconciler(ReconcilerConfig{Catalog: cat, Store: st, Logger: quietLogger()})

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a := tbl("d", "a", 1)
	a.CreatedAt = t0
	cat.set([]string{"d"}, a)
	r.RunOnce(ctx)
	r.RunOnce(ctx)
	if ensured, _ := st.snapshot(); len(ensured) != 1 {
		t.Fatalf("ensured %v", ensured)
	}
	b := tbl("d", "a", 1)
	b.CreatedAt = t0.Add(time.Second)
	cat.set([]string{"d"}, b)
	r.RunOnce(ctx)
	if ensured, dropped := st.snapshot(); len(ensured) != 2 || len(dropped) != 0 {
		t.Fatalf("new incarnation: ensured %v dropped %v", ensured, dropped)
	}
}

// Concurrent DDL in two regions can leave a different definition at the same
// catalog version (last-writer-wins); it must be ensured again.
func TestReconcilerReensuresSameVersionNewDefinition(t *testing.T) {
	ctx := context.Background()
	cat := &fakeCatalog{}
	st := &fakeStore{ensureErr: map[string]error{}}
	r := NewReconciler(ReconcilerConfig{Catalog: cat, Store: st, Logger: quietLogger()})

	mk := func(col string) *catalog.Table {
		x := tbl("d", "a", 2)
		x.Columns = []catalog.Column{{Name: "k", TypeText: "String"}, {Name: col, TypeText: "String"}}
		return x
	}
	cat.set([]string{"d"}, mk("c_us"))
	r.RunOnce(ctx)
	r.RunOnce(ctx)
	cat.set([]string{"d"}, mk("c_eu"))
	r.RunOnce(ctx)
	if ensured, _ := st.snapshot(); len(ensured) != 2 {
		t.Fatalf("same version, new definition: ensured %v", ensured)
	}
}

// A table created again (same physical name) right after its drop candidate
// scan must not be dropped: the catalog is re-read before every drop.
func TestReconcilerRechecksCatalogBeforeDrop(t *testing.T) {
	ctx := context.Background()
	cat := &fakeCatalog{}
	st := &fakeStore{ensureErr: map[string]error{}}
	r := NewReconciler(ReconcilerConfig{Catalog: cat, Store: st, Logger: quietLogger()})

	cat.set([]string{"d"}, tbl("d", "a", 1))
	r.RunOnce(ctx) // refresh 1
	cat.set([]string{"d"})
	r.RunOnce(ctx) // refresh 2: missing 1
	recreated := tbl("d", "a", 1)
	recreated.CreatedAt = time.Now()
	cat.onRefresh = func(n int) {
		if n == 4 { // the re-read just before the drop of round 3
			cat.set([]string{"d"}, recreated)
		}
	}
	r.RunOnce(ctx) // refresh 3: missing 2 -> candidate; refresh 4 sees it again
	ensured, dropped := st.snapshot()
	if len(dropped) != 0 {
		t.Fatalf("recreated table dropped: %v", dropped)
	}
	if len(ensured) != 2 {
		t.Fatalf("recreated table not ensured: %v", ensured)
	}
	r.RunOnce(ctx)
	r.RunOnce(ctx)
	if _, dropped := st.snapshot(); len(dropped) != 0 {
		t.Fatalf("recreated table dropped later: %v", dropped)
	}

	// Still dropped when it is really gone; a failing re-read drops nothing.
	cat.onRefresh = nil
	cat.set([]string{"d"})
	r.RunOnce(ctx)
	cat.onRefresh = func(int) {
		cat.mu.Lock()
		cat.refreshErr = errors.New("throttled")
		cat.mu.Unlock()
	}
	r.RunOnce(ctx) // candidate, but the re-read fails
	if _, dropped := st.snapshot(); len(dropped) != 0 {
		t.Fatalf("dropped although the re-read failed: %v", dropped)
	}
	cat.onRefresh = nil
	cat.refreshErr = nil
	r.RunOnce(ctx)
	if _, dropped := st.snapshot(); !slices.Equal(dropped, []string{"p_d_a"}) {
		t.Fatalf("drop: %v", dropped)
	}
}

// An unresponsive DynamoDB must not stall a round forever.
func TestReconcilerCallTimeouts(t *testing.T) {
	cat := &fakeCatalog{refreshBlock: true}
	r := NewReconciler(ReconcilerConfig{Catalog: cat, Store: &fakeStore{}, Logger: quietLogger(),
		RefreshTimeout: 20 * time.Millisecond, TableTimeout: 20 * time.Millisecond})
	done := make(chan struct{})
	go func() { r.RunOnce(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh without deadline")
	}

	cat = &fakeCatalog{}
	cat.set([]string{"d"}, tbl("d", "a", 1))
	st := &fakeStore{block: true}
	r = NewReconciler(ReconcilerConfig{Catalog: cat, Store: st, Logger: quietLogger(),
		RefreshTimeout: 20 * time.Millisecond, TableTimeout: 20 * time.Millisecond})
	done = make(chan struct{})
	go func() { r.RunOnce(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("EnsureTable without deadline")
	}
}

// A table whose catalog entry is quarantined (undecodable) is never
// dropped; an undecodable entry of unknown physical name blocks every drop.
func TestReconcilerKeepsQuarantinedTables(t *testing.T) {
	ctx := context.Background()
	cat := &fakeCatalog{}
	st := &fakeStore{}
	r := NewReconciler(ReconcilerConfig{Catalog: cat, Store: st, Logger: quietLogger()})
	cat.set([]string{"d"}, tbl("d", "a", 1), tbl("d", "b", 1), tbl("d", "c", 1))
	r.RunOnce(ctx)
	// a becomes undecodable (newer build), b is dropped for real.
	cat.set([]string{"d"}, tbl("d", "c", 1))
	cat.mu.Lock()
	cat.quarantine = []catalog.Quarantined{{Key: "table#d.a", Kind: catalog.KindTable, PhysicalName: "p_d_a", Err: errors.New("bad type")}}
	cat.mu.Unlock()
	for range 3 {
		r.RunOnce(ctx)
	}
	if _, dropped := st.snapshot(); !slices.Equal(dropped, []string{"p_d_b"}) {
		t.Fatalf("dropped %v, want only p_d_b", dropped)
	}
	// Unknown physical name: nothing is dropped.
	cat.set([]string{"d"})
	cat.mu.Lock()
	cat.quarantine = []catalog.Quarantined{{Key: "junk", Kind: catalog.KindMalformed}}
	cat.mu.Unlock()
	for range 3 {
		r.RunOnce(ctx)
	}
	if _, dropped := st.snapshot(); len(dropped) != 1 {
		t.Fatalf("dropped while a malformed entry exists: %v", dropped)
	}
	cat.mu.Lock()
	cat.quarantine = nil
	cat.mu.Unlock()
	r.RunOnce(ctx)
	if _, dropped := st.snapshot(); !slices.Contains(dropped, "p_d_c") || !slices.Contains(dropped, "p_d_a") {
		t.Fatalf("after the quarantine cleared: %v", dropped)
	}
}
