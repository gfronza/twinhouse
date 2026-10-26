package dynamo

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/storage"
)

func asyncDDL(o *Options) { o.AsyncDDL = true }

func globalTable(name string) *catalog.Table {
	return &catalog.Table{Database: "default", Name: name, Engine: catalog.EngineMergeTree,
		Columns: cols("k", "String"), OrderBy: []string{"k"}, Replication: catalog.ReplicationGlobal,
		PhysicalName: "p_default." + name + ".abcdefgh", HomeRegion: "us-east-1", CreatedAt: time.Now()}
}

// TestAsyncDDLCreateDoesNotWaitForReplicas: the API Lambda's request budget
// (29 s) cannot cover adding a global-table replica (minutes), so with
// Options.AsyncDDL CREATE TABLE waits for the table itself only and the
// home region's reconciler (a store without the option) adds the replica.
func TestAsyncDDLCreateDoesNotWaitForReplicas(t *testing.T) {
	w := newFakeWorld(0)
	w.replicaDelay = time.Hour // the source table stays UPDATING "for minutes"
	api := awsStore(t, w, "us-east-1", asyncDDL)
	tbl := globalTable("events")

	// Control: a waiting store under a request deadline times out, which
	// is what the API Lambda used to answer (TIMEOUT_EXCEEDED after the
	// catalog entry was committed).
	sync := awsStore(t, w, "us-east-1")
	sctx, scancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	err := sync.EnsureTable(sctx, globalTable("control"))
	scancel()
	if !cherr.Is(err, cherr.Timeout) {
		t.Fatalf("waiting store under a request deadline: want TIMEOUT, got %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	if err := api.EnsureTable(ctx, tbl); err != nil {
		t.Fatalf("async CREATE: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("async CREATE took %v", d)
	}
	w.mu.Lock()
	us, eu := w.tables["us-east-1"][tbl.PhysicalName], w.tables["eu-west-1"][tbl.PhysicalName]
	w.mu.Unlock()
	if us == nil || eu != nil {
		t.Fatalf("async CREATE must create the table here and no replica: us %v eu %v", us != nil, eu != nil)
	}
	// IF NOT EXISTS re-runs EnsureTable: still no wait.
	if err := api.EnsureTable(ctx, tbl); err != nil {
		t.Fatalf("async CREATE again: %v", err)
	}

	// The reconciler (waiting store) completes the replica later.
	w.mu.Lock()
	w.replicaDelay = 0
	w.mu.Unlock()
	if err := sync.EnsureTable(context.Background(), tbl); err != nil {
		t.Fatalf("reconciler EnsureTable: %v", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if eu = w.tables["eu-west-1"][tbl.PhysicalName]; eu == nil || len(*eu.group) != 2 {
		t.Fatalf("replica not added by the reconciler: %v", eu)
	}
}

// TestAsyncDDLDropDefersToReconciler: DROP TABLE of a table with replicas
// is left to the reconciler (ErrDeleteDeferred, success for the client);
// a table without replicas is deleted without waiting.
func TestAsyncDDLDropDefersToReconciler(t *testing.T) {
	w := newFakeWorld(0)
	api, sync := awsStore(t, w, "us-east-1", asyncDDL), awsStore(t, w, "us-east-1")
	ctx := context.Background()
	global := globalTable("events")
	if err := sync.EnsureTable(ctx, global); err != nil {
		t.Fatal(err)
	}
	err := api.DropTable(ctx, global)
	if !errors.Is(err, storage.ErrDeleteDeferred) || !cherr.Is(err, cherr.Unavailable) {
		t.Fatalf("async DROP of a replicated table: want ErrDeleteDeferred/UNAVAILABLE, got %v", err)
	}
	w.mu.Lock()
	kept := w.tables["us-east-1"][global.PhysicalName] != nil && w.tables["eu-west-1"][global.PhysicalName] != nil
	w.mu.Unlock()
	if !kept {
		t.Fatal("the deferred table must be left in place for the reconciler")
	}

	regional := globalTable("roll")
	regional.Engine, regional.Replication = catalog.EngineSummingMergeTree, catalog.ReplicationRegional
	regional.PartitionKey = []string{"k"}
	if err := api.EnsureTable(ctx, regional); err != nil {
		t.Fatal(err)
	}
	if err := api.DropTable(ctx, regional); err != nil {
		t.Fatalf("async DROP of a regional table: %v", err)
	}
	w.mu.Lock()
	deletes := slices.Clone(w.deletes)
	w.mu.Unlock()
	if !slices.Contains(deletes, "us-east-1:"+regional.PhysicalName) {
		t.Fatalf("regional table not deleted: %v", deletes)
	}

	// The reconciler's DropTable removes the replica and the source.
	if err := sync.DropTable(ctx, global); err != nil {
		t.Fatalf("reconciler DROP: %v", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.tables["us-east-1"][global.PhysicalName] != nil || w.tables["eu-west-1"][global.PhysicalName] != nil {
		t.Fatal("global table not removed everywhere")
	}
}

// TestDeleteRestrictedIsDeferred: DynamoDB refuses to delete the source
// table of a global table within 24 hours of adding a replica; the store
// reports ErrDeleteDeferred (the catalog entry is gone, the reconciler
// retries) instead of BAD_ARGUMENTS.
func TestDeleteRestrictedIsDeferred(t *testing.T) {
	for _, msg := range []string{
		"Table default.events cannot be deleted because it was the source of a replica created within the last 24 hours",
		"Replica cannot be deleted within 24 hrs of creation",
	} {
		if !isDeleteRestricted(&validationErr{msg}) {
			t.Errorf("not recognised: %s", msg)
		}
	}
	if isDeleteRestricted(&validationErr{"One or more parameter values were invalid"}) || isDeleteRestricted(errors.New("24 hours")) {
		t.Error("recognised an unrelated error")
	}

	w := newFakeWorld(0)
	s := awsStore(t, w, "us-east-1")
	ctx := context.Background()
	tbl := globalTable("events")
	if err := s.EnsureTable(ctx, tbl); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	w.deleteErr = &validationErr{"Table cannot be deleted within 24 hours of being the source of a replica creation"}
	w.mu.Unlock()
	err := s.DropTable(ctx, tbl)
	if !errors.Is(err, storage.ErrDeleteDeferred) || !cherr.Is(err, cherr.Unavailable) {
		t.Fatalf("want ErrDeleteDeferred/UNAVAILABLE, got %v", err)
	}
	// The replica was removed anyway and the drop succeeds once allowed.
	w.mu.Lock()
	eu := w.tables["eu-west-1"][tbl.PhysicalName]
	w.deleteErr = nil
	w.mu.Unlock()
	if eu != nil {
		t.Fatal("replica kept")
	}
	if err := s.DropTable(ctx, tbl); err != nil {
		t.Fatalf("retry: %v", err)
	}
}

// TestListAndDropPhysicalTables covers the reconciler's discovery of
// physical tables without a catalog entry: only row tables of this
// deployment in the per-incarnation shape are listed (paginated), and
// DropPhysicalTable drops the row table with its side table.
func TestListAndDropPhysicalTables(t *testing.T) {
	w := newFakeWorld(0)
	w.listPage = 2
	s, err := newStoreWithAPI(w.region("us-east-1"), "us-east-1", Options{Prefix: "p_"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, name := range []string{
		"p_default.events.abcdefgh", "p_default.events.abcdefgh.members", "p_db1.t.zyxw2345",
		"p___catalog", "p___mv_markers", "p___dead_letters", // system tables
		"p_default.old",        // an older build's table: no incarnation suffix
		"p_x",                  // not <db>.<table>
		"q_default.t.abcdefgh", // another prefix
		"p_default.t.ABCDEFGH", // not an incarnation id
	} {
		if err := s.impl.ensurePhysical(ctx, tableSpec{name: name, keyType: types.ScalarAttributeTypeB, rangeKey: true}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.ListPhysicalTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"p_db1.t.zyxw2345", "p_default.events.abcdefgh"}; !slices.Equal(got, want) {
		t.Fatalf("listed %v, want %v", got, want)
	}
	if err := s.DropPhysicalTable(ctx, "p_default.events.abcdefgh"); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	_, row := w.tables["us-east-1"]["p_default.events.abcdefgh"]
	_, members := w.tables["us-east-1"]["p_default.events.abcdefgh.members"]
	w.mu.Unlock()
	if row || members {
		t.Fatalf("row %v members %v still exist", row, members)
	}
	if err := s.DropPhysicalTable(ctx, "p_default.events.abcdefgh"); err != nil {
		t.Fatalf("not idempotent: %v", err)
	}
	for _, name := range []string{"p___catalog", "q_default.t.abcdefgh", ""} {
		if err := s.DropPhysicalTable(ctx, name); !cherr.Is(err, cherr.BadArguments) {
			t.Errorf("DropPhysicalTable(%q) = %v, want BAD_ARGUMENTS", name, err)
		}
	}
}

// TestCheckSystemTables: the API Lambda only checks the system tables
// exist; a table being updated (a replica added) is ready, a missing or
// CREATING one is UNAVAILABLE, and nothing is created or awaited.
func TestCheckSystemTables(t *testing.T) {
	w := newFakeWorld(0)
	api, rec := awsStore(t, w, "us-east-1", asyncDDL), awsStore(t, w, "us-east-1")
	ctx := context.Background()
	err := api.CheckSystemTables(ctx)
	if !cherr.Is(err, cherr.Unavailable) || !strings.Contains(err.Error(), "p___catalog") {
		t.Fatalf("missing tables: %v", err)
	}
	w.mu.Lock()
	creates := len(w.creates)
	w.mu.Unlock()
	if creates != 0 {
		t.Fatal("CheckSystemTables created a table")
	}
	if err := rec.EnsureSystemTables(ctx); err != nil {
		t.Fatal(err)
	}
	if err := api.CheckSystemTables(ctx); err != nil {
		t.Fatalf("after EnsureSystemTables: %v", err)
	}
	w.mu.Lock()
	w.tables["us-east-1"]["p___catalog"].updating = time.Now().Add(time.Hour)
	w.mu.Unlock()
	start := time.Now()
	if err := api.CheckSystemTables(ctx); err != nil || time.Since(start) > time.Second {
		t.Fatalf("UPDATING catalog table: err %v after %v", err, time.Since(start))
	}
	w.mu.Lock()
	w.tables["us-east-1"]["p___checkpoints"].ready = time.Now().Add(time.Hour)
	w.mu.Unlock()
	if err := api.CheckSystemTables(ctx); !cherr.Is(err, cherr.Unavailable) || time.Since(start) > time.Second {
		t.Fatalf("CREATING table: err %v after %v", err, time.Since(start))
	}
}
