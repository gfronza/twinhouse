//go:build !race

package exec

import (
	"runtime"
	"runtime/metrics"
	"sync"
	"testing"
	"time"
)

// peakHeapDuring samples the heap every millisecond while fn runs and
// returns the largest growth over the heap before it.
func peakHeapDuring(fn func()) uint64 {
	runtime.GC()
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	base := s[0].Value.Uint64()
	var peak uint64
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		sample := []metrics.Sample{{Name: s[0].Name}}
		for {
			metrics.Read(sample)
			if v := sample[0].Value.Uint64(); v > base && v-base > peak {
				peak = v - base
			}
			select {
			case <-done:
				return
			case <-tick.C:
			}
		}
	}()
	fn()
	close(done)
	wg.Wait()
	return peak
}

func TestOrderByLimitKeepsBoundedRows(t *testing.T) {
	if testing.Short() {
		t.Skip("reads 3M rows")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	var res *Result
	peak := peakHeapDuring(func() {
		res = te.exec(`SELECT number FROM numbers(3000000) ORDER BY number DESC LIMIT 10`)
	})
	eq(t, res.Rows[0], []any{uint64(2999999)})
	eq(t, res.Rows[9], []any{uint64(2999990)})
	eq(t, res.RowsBeforeLimit, uint64(3000000))
	if peak > 20<<20 {
		t.Errorf("heap grew by %d MiB, want under 20 MiB", peak>>20)
	}
}
