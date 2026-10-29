//go:build !race

package server

import (
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// liveHeap is the heap left after a collection.
func liveHeap() int64 {
	runtime.GC()
	return int64(heapObjects())
}

// Reading an endless SELECT holds one block on the server, and a client
// that goes away ends the query.
func TestEndlessSelectStreamsInBoundedMemory(t *testing.T) {
	guardServerTest(t)
	logs := make(chan map[string]any, 16)
	base := newEngineServer(t, 0, func(c *Config) { c.Logger = recordLogger(logs) })
	before := liveHeap()
	res, err := http.Post(base+"/", "text/plain", strings.NewReader("SELECT number FROM system.numbers"))
	if err != nil {
		t.Fatal(err)
	}
	peak := int64(0)
	for range 10 {
		if _, err := io.CopyN(io.Discard, res.Body, 1<<20); err != nil {
			t.Fatal(err)
		}
		peak = max(peak, liveHeap()-before)
	}
	res.Body.Close()
	select {
	case rec := <-logs:
		if rec["code"] != int64(cherr.QueryWasCancelled) {
			t.Errorf("request log %v", rec)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the query did not end after the client went away")
	}
	if peak > 32<<20 {
		t.Errorf("server heap grew by %d MiB while streaming", peak>>20)
	}
	if after := liveHeap() - before; after > 8<<20 {
		t.Errorf("server heap %d MiB above the start once the query ended", after>>20)
	}
}

// peakHeapDuring returns how far the live heap rose above its start while
// fn ran, sampled every 20 ms.
func peakHeapDuring(fn func()) int64 {
	before := liveHeap()
	done, peak := make(chan struct{}), make(chan int64)
	go func() {
		p := int64(0)
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-done:
				peak <- p
				return
			case <-tick.C:
				p = max(p, liveHeap()-before)
			}
		}
	}()
	fn()
	close(done)
	return <-peak
}

// Pretty with output_format_pretty_max_rows = 0 keeps no rows, as in
// ClickHouse, so a large result renders in bounded memory.
func TestPrettyWithoutRowsStaysInBoundedMemory(t *testing.T) {
	guardServerTest(t)
	base := newEngineServer(t, 0, nil)
	var r resp
	peak := peakHeapDuring(func() {
		r = query(t, base, "SELECT number, toString(number) FROM numbers(1000000) FORMAT PrettyCompact",
			url.Values{"output_format_pretty_max_rows": {"0"}})
	})
	if r.Status != 200 || r.Body != "Showed 0 out of 1000000 rows.\n" {
		t.Errorf("%d %q", r.Status, r.Body[:min(len(r.Body), 200)])
	}
	if peak > 32<<20 {
		t.Errorf("server heap grew by %d MiB", peak>>20)
	}
}
