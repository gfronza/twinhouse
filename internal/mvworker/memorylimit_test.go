package mvworker

import (
	"context"
	"testing"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/exec"
	"github.com/gfronza/twinhouse/internal/storage"
	"github.com/gfronza/twinhouse/internal/storage/memstore"
)

// A view whose output passes ViewMemoryUsage fails ApplyViews with 241. The
// error is permanent, so the worker dead-letters the record instead of
// retrying it forever.
func TestViewMemoryLimitErrorIsPermanent(t *testing.T) {
	if testing.Short() {
		t.Skip("applies a view under a limit")
	}
	ctx := context.Background()
	cat := catalog.NewMemory()
	eng, err := exec.New(exec.Config{Store: memstore.New(), Catalog: cat, ViewMemoryUsage: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"CREATE TABLE src (n UInt64) ENGINE = MergeTree ORDER BY n",
		"CREATE TABLE dst (xs Array(UInt64)) ENGINE = MergeTree ORDER BY tuple()",
		"CREATE MATERIALIZED VIEW mv TO dst AS SELECT range(n) AS xs FROM src",
	} {
		if _, err := eng.Execute(ctx, &exec.Request{Query: q, Database: "default"}); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	src, err := cat.Table("default", "src")
	if err != nil {
		t.Fatal(err)
	}
	err = eng.ApplyViews(ctx, src, []storage.Row{{uint64(100_000)}}, []string{"r1"})
	if !cherr.Is(err, cherr.MemoryLimitExceeded) {
		t.Fatalf("ApplyViews of a 100000-element output row at a 1 MiB limit: %v", err)
	}
	if isTransient(err, false) {
		t.Fatalf("%v is transient", err)
	}
}
