package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/exec"
	"github.com/gfronza/twinhouse/internal/storage/memstore"
)

// BenchmarkSelectStream serves a 1M-row SELECT as TSV into a discarding
// response, without a memory limit and with an 8 GiB one.
func BenchmarkSelectStream(b *testing.B) {
	for _, c := range []struct{ name, settings string }{
		{"limit=0", ""},
		{"limit=8GiB", " SETTINGS max_memory_usage = 8589934592"},
	} {
		b.Run(c.name, func(b *testing.B) {
			cat := catalog.NewMemory()
			eng, err := exec.New(exec.Config{Store: memstore.New(), Catalog: cat, Timezone: "UTC"})
			if err != nil {
				b.Fatal(err)
			}
			s, err := New(Config{Executor: eng, Catalog: cat, Logger: quietLogger()})
			if err != nil {
				b.Fatal(err)
			}
			sql := "SELECT number, toString(number) FROM numbers(1000000)" + c.settings
			b.ReportAllocs()
			for b.Loop() {
				w := &discardResponse{header: http.Header{}}
				s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(sql)))
				if w.status != http.StatusOK {
					b.Fatalf("HTTP %d", w.status)
				}
			}
		})
	}
}

// discardResponse is an http.ResponseWriter that drops the body.
type discardResponse struct {
	header http.Header
	status int
}

func (d *discardResponse) Header() http.Header { return d.header }

func (d *discardResponse) Write(p []byte) (int, error) {
	if d.status == 0 {
		d.status = http.StatusOK
	}
	return len(p), nil
}

func (d *discardResponse) WriteHeader(status int) { d.status = status }
