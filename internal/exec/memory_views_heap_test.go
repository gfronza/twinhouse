//go:build !race

package exec

import (
	"context"
	"testing"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/storage"
)

// discardMerges drops view output, so only the view's own memory is live.
type discardMerges struct{ storage.Store }

func (discardMerges) Merge(context.Context, *catalog.Table, []storage.MergeRow) error { return nil }

// A million view rows from one source row never sit in memory together
// (collected first, they peaked near 300 MB).
func TestViewFanOutHeapStaysBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("applies a million view rows")
	}
	guardFunctionTest(t)
	te, spy, src := newFanOutEnv(t, 16_000_000)
	spy.Store = discardMerges{spy.Store}
	row := fanOutRow()
	peak := peakHeapObjects(func() {
		if err := te.e.ApplyViews(context.Background(), src, []storage.Row{row}, []string{"r1"}); err != nil {
			t.Fatal(err)
		}
	})
	if peak > 32<<20 {
		t.Fatalf("heap peaked at %d MB", peak>>20)
	}
}
