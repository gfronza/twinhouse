package exec

import (
	"context"
	"errors"
	"testing"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/storage"
	"github.com/gfronza/twinhouse/internal/storage/memstore"
)

// deferringStore drops like memstore but reports the deletion as deferred
// (what dynamo.Store does under DynamoDB's 24 hour restriction, or with
// Options.AsyncDDL for a table with replicas), or fails it.
type deferringStore struct {
	*memstore.Store
	dropErr error
	drops   int
}

func (d *deferringStore) DropTable(ctx context.Context, t *catalog.Table) error {
	d.drops++
	if err := d.Store.DropTable(ctx, t); err != nil {
		return err
	}
	return d.dropErr
}

func newDeferringEnv(t *testing.T) (*testEnv, *deferringStore) {
	t.Helper()
	ds := &deferringStore{Store: memstore.New(), dropErr: &cherr.Error{Code: cherr.Unavailable, Cause: storage.ErrDeleteDeferred,
		Msg: "DynamoDB table cannot be deleted now: within 24 hours of adding a replica"}}
	cat := catalog.NewMemory()
	e, err := New(Config{Store: ds, Catalog: cat, InlineViews: true, ConsistentReads: true, Timezone: "UTC",
		ServerVersion: "25.8.1.1", ServerRevision: 54477, DisplayName: "test"})
	if err != nil {
		t.Fatal(err)
	}
	e.impl.fresh.interval = 0
	return &testEnv{t: t, e: e, cat: cat}, ds
}

// TestDropDeferredIsSuccess: once the catalog entry is gone, a physical
// deletion the store defers is not an error for the client (the reconciler
// completes it); DROP TABLE, CREATE OR REPLACE and DROP DATABASE all take
// that path, and any other store error still surfaces.
func TestDropDeferredIsSuccess(t *testing.T) {
	te, ds := newDeferringEnv(t)
	te.exec("CREATE TABLE t (k String) ENGINE = MergeTree ORDER BY k")
	te.exec("DROP TABLE t")
	if _, err := te.cat.Table("default", "t"); !cherr.Is(err, cherr.UnknownTable) {
		t.Fatalf("catalog entry after deferred DROP: %v", err)
	}
	if ds.drops != 1 {
		t.Fatalf("drops %d", ds.drops)
	}
	te.exec("CREATE TABLE t (k String) ENGINE = MergeTree ORDER BY k") // a new incarnation
	te.exec("CREATE OR REPLACE TABLE t (k String, v UInt8) ENGINE = MergeTree ORDER BY k")
	if ds.drops != 2 {
		t.Fatalf("OR REPLACE drops %d", ds.drops)
	}
	te.exec("CREATE DATABASE d")
	te.exec("CREATE TABLE d.a (k String) ENGINE = MergeTree ORDER BY k")
	te.exec("DROP DATABASE d")
	if _, err := te.cat.Database("d"); !cherr.Is(err, cherr.UnknownDatabase) {
		t.Fatalf("database after deferred drops: %v", err)
	}

	ds.dropErr = errors.New("AccessDeniedException")
	if _, err := te.run("DROP TABLE t"); !errors.Is(err, ds.dropErr) {
		t.Fatalf("a real store error must surface, got %v", err)
	}
}
