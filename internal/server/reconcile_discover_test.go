package server

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/storage"
)

// discoverStore is a fakeStore that also lists and drops physical tables.
type discoverStore struct {
	*fakeStore
	physical        []string
	listErr         error
	lists           int
	droppedPhysical []string
}

func (d *discoverStore) ListPhysicalTables(context.Context) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lists++
	return slices.Clone(d.physical), d.listErr
}

func (d *discoverStore) DropPhysicalTable(_ context.Context, name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.droppedPhysical = append(d.droppedPhysical, name)
	d.physical = slices.DeleteFunc(d.physical, func(p string) bool { return p == name })
	return nil
}

// gcConsumers is a fakeConsumers that also lists the consumed tables.
type gcConsumers struct {
	*fakeConsumers
	consumed []string
}

func (g *gcConsumers) ConsumedTables(context.Context) ([]string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.consumed), nil
}

func (g *gcConsumers) RemoveConsumers(ctx context.Context, physical, streamARN string) error {
	if err := g.fakeConsumers.RemoveConsumers(ctx, physical, streamARN); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.consumed = slices.DeleteFunc(g.consumed, func(p string) bool { return p == physical })
	return nil
}

// TestReconcilerDropsOrphansItNeverObserved: a reconciler that starts after
// a DROP (a new Lambda execution environment) never had the table in its
// catalog snapshot; the physical table and its stream consumers are found
// anyway and removed with the usual protections.
func TestReconcilerDropsOrphansItNeverObserved(t *testing.T) {
	ctx := context.Background()
	cat := &fakeCatalog{}
	a := tbl("d", "a", 1)
	a.Engine = catalog.EngineMergeTree
	cat.set([]string{"d"}, a)
	st := &discoverStore{
		fakeStore: &fakeStore{ensureErr: map[string]error{}, streams: map[string]string{a.PhysicalName: streamOf(a.PhysicalName)}},
		physical:  []string{a.PhysicalName, "p_d_orphan"},
	}
	cons := &gcConsumers{fakeConsumers: &fakeConsumers{ensureErr: map[string]error{}},
		consumed: []string{a.PhysicalName, "p_d_orphan", "p_d_gone"}}
	r := NewReconciler(ReconcilerConfig{Catalog: cat, Store: st, Consumers: cons, Logger: quietLogger()})

	r.RunOnce(ctx) // discovers the orphans; missing 1
	if _, dropped := st.snapshot(); len(dropped) != 0 || len(st.droppedPhysical) != 0 {
		t.Fatalf("dropped on the first round: %v %v", dropped, st.droppedPhysical)
	}
	r.RunOnce(ctx) // missing 2 = DropAfter: re-read, unwire, drop
	_, removed := cons.snapshot()
	slices.Sort(removed)
	if want := []string{"p_d_gone=", "p_d_orphan="}; !slices.Equal(removed, want) {
		t.Fatalf("consumers removed %v, want %v", removed, want)
	}
	slices.Sort(st.droppedPhysical)
	if want := []string{"p_d_gone", "p_d_orphan"}; !slices.Equal(st.droppedPhysical, want) {
		t.Fatalf("physical drops %v, want %v", st.droppedPhysical, want)
	}
	if _, dropped := st.snapshot(); len(dropped) != 0 {
		t.Fatalf("catalog-based drop of a discovered table: %v", dropped)
	}
	if ensured, _ := st.snapshot(); !slices.Equal(ensured, []string{"p_d_a@1"}) {
		t.Fatalf("the catalog table must stay: %v", ensured)
	}
	// Nothing is listed again within DiscoverInterval.
	r.RunOnce(ctx)
	if st.lists != 1 {
		t.Fatalf("lists %d", st.lists)
	}

	// A discovered table that the catalog names (again) before the drop is
	// kept and ensured, not dropped.
	r.lastDiscover = time.Time{}
	st.mu.Lock()
	st.physical = append(st.physical, "p_d_late")
	st.mu.Unlock()
	r.RunOnce(ctx) // discovered, missing 1
	late := &catalog.Table{Database: "d", Name: "late", PhysicalName: "p_d_late", Version: 1, Engine: catalog.EngineMergeTree}
	cat.set([]string{"d"}, a, late)
	r.RunOnce(ctx)
	r.RunOnce(ctx)
	ensured, dropped := st.snapshot()
	if slices.Contains(st.droppedPhysical, "p_d_late") || len(dropped) != 0 || !slices.Contains(ensured, "p_d_late@1") {
		t.Fatalf("late table: physical %v dropped %v ensured %v", st.droppedPhysical, dropped, ensured)
	}

	// A failing listing is retried on the next round; a negative interval
	// disables discovery.
	r.lastDiscover = time.Time{}
	st.mu.Lock()
	st.listErr = errors.New("throttled")
	st.physical = append(st.physical, "p_d_orphan2")
	st.mu.Unlock()
	r.RunOnce(ctx)
	st.mu.Lock()
	st.listErr = nil
	st.mu.Unlock()
	r.RunOnce(ctx)
	if _, ok := r.known["p_d_orphan2"]; !ok {
		t.Fatal("listing not retried after a failure")
	}
	r2 := NewReconciler(ReconcilerConfig{Catalog: cat, Store: st, Consumers: cons, Logger: quietLogger(), DiscoverInterval: -1})
	r2.RunOnce(ctx)
	if st.lists != 4 {
		t.Fatalf("discovery not disabled: %d lists", st.lists)
	}
}

// TestReconcilerDeferredDrop: a deferred physical deletion (DynamoDB's
// 24 hour restriction) is retried every round without re-removing the
// stream consumers, and completes once the store allows it.
func TestReconcilerDeferredDrop(t *testing.T) {
	ctx := context.Background()
	cat := &fakeCatalog{}
	a := tbl("d", "a", 1)
	a.Engine = catalog.EngineMergeTree
	st := &fakeStore{ensureErr: map[string]error{}, streams: map[string]string{a.PhysicalName: streamOf(a.PhysicalName)}}
	cons := &fakeConsumers{ensureErr: map[string]error{}}
	r := NewReconciler(ReconcilerConfig{Catalog: cat, Store: st, Consumers: cons, Logger: quietLogger(), DiscoverInterval: -1})
	cat.set([]string{"d"}, a)
	r.RunOnce(ctx)
	cat.set([]string{"d"})
	st.mu.Lock()
	st.dropErr = &cherr.Error{Code: cherr.Unavailable, Cause: storage.ErrDeleteDeferred, Msg: "within 24 hours"}
	st.mu.Unlock()
	for i := 0; i < 4; i++ {
		r.RunOnce(ctx)
	}
	_, removed := cons.snapshot()
	if !slices.Equal(removed, []string{a.PhysicalName + "=" + streamOf(a.PhysicalName)}) {
		t.Fatalf("consumers removed %v, want exactly once", removed)
	}
	if _, dropped := st.snapshot(); len(dropped) != 0 {
		t.Fatalf("dropped while deferred: %v", dropped)
	}
	st.mu.Lock()
	st.dropErr = nil
	st.mu.Unlock()
	r.RunOnce(ctx)
	if _, dropped := st.snapshot(); !slices.Equal(dropped, []string{a.PhysicalName}) {
		t.Fatalf("drop after the window: %v", dropped)
	}
	// Recreated under the same physical name after the drop: wired again.
	cat.set([]string{"d"}, a)
	r.RunOnce(ctx)
	if ensured, _ := cons.snapshot(); len(ensured) != 2 {
		t.Fatalf("not re-wired: %v", ensured)
	}
}
