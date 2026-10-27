package server

import (
	"net/url"
	"strings"
	"testing"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/exec"
	"github.com/gfronza/twinhouse/internal/storage/memstore"
)

// newMemoryLimitServer serves the real engine with a server memory limit
// of maxMem bytes.
func newMemoryLimitServer(t *testing.T, maxMem int64) string {
	t.Helper()
	cat := catalog.NewMemory()
	eng, err := exec.New(exec.Config{Store: memstore.New(), Catalog: cat, Timezone: "UTC", MaxMemoryUsage: maxMem})
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{Executor: eng, Catalog: cat, ServerVersion: "25.8.1.1", Logger: quietLogger()})
	if err != nil {
		t.Fatal(err)
	}
	return httpTestServer(t, s)
}

// query posts sql with the given URL parameters.
func query(t *testing.T, base, sql string, params url.Values) resp {
	t.Helper()
	return post(t, base+"/?"+params.Encode(), sql)
}

func wantException(t *testing.T, what string, r resp, code string, status int, msg string) {
	t.Helper()
	if got := r.Header.Get("X-ClickHouse-Exception-Code"); got != code || r.Status != status || !strings.Contains(r.Body, msg) {
		t.Errorf("%s: HTTP %d code %q body %q; want HTTP %d code %s containing %q", what, r.Status, got, r.Body, status, code, msg)
	}
}

func wantOK(t *testing.T, what string, r resp) {
	t.Helper()
	if r.Status != 200 {
		t.Errorf("%s: HTTP %d %s", what, r.Status, r.Body)
	}
}

// 100000 collected rows estimate about 5 MB: under the 32 MB server limit,
// over a 1 MB one set by any source.
const collect100k = "SELECT number FROM numbers(100000)"

func TestMaxMemoryUsageFromEverySource(t *testing.T) {
	base := newMemoryLimitServer(t, 32_000_000)
	const exceeded = "Query memory limit exceeded: would use "
	low := url.Values{"max_memory_usage": {"1000000"}}

	wantOK(t, "server default", query(t, base, collect100k, nil))
	wantException(t, "URL", query(t, base, collect100k, low), "241", 500, exceeded)
	wantException(t, "SETTINGS", query(t, base, collect100k+" SETTINGS max_memory_usage = 1000000", nil), "241", 500, exceeded)

	sess := url.Values{"session_id": {"mem"}}
	wantOK(t, "SET in a session", query(t, base, "SET max_memory_usage = 1e6", sess))
	wantException(t, "session", query(t, base, collect100k, sess), "241", 500, exceeded)
	wantOK(t, "SETTINGS over the session", query(t, base, collect100k+" SETTINGS max_memory_usage = 32000000", sess))
}

func TestMaxMemoryUsageAboveServerLimitIsRejected(t *testing.T) {
	base := newMemoryLimitServer(t, 32_000_000)
	const violation = "Setting max_memory_usage shouldn't be greater than 32000000. (SETTING_CONSTRAINT_VIOLATION)"
	for _, v := range []string{"64000000", "0", "1G"} {
		wantException(t, "URL "+v, query(t, base, "SELECT 1", url.Values{"max_memory_usage": {v}}), "452", 500, violation)
		wantException(t, "SETTINGS "+v, query(t, base, "SELECT 1 SETTINGS max_memory_usage = '"+v+"'", nil), "452", 500, violation)
	}
	sess := url.Values{"session_id": {"raise"}}
	wantOK(t, "lowering SET", query(t, base, "SET max_memory_usage = 1000000", sess))
	wantException(t, "raising SET", query(t, base, "SET max_memory_usage = 0", sess), "452", 500, violation)
	wantException(t, "session keeps the lower limit", query(t, base, collect100k, sess), "241", 500, "Query memory limit exceeded")
}

func TestMaxMemoryUsageWithoutServerLimit(t *testing.T) {
	base := newMemoryLimitServer(t, 0)
	wantOK(t, "unlimited", query(t, base, collect100k, url.Values{"max_memory_usage": {"0"}}))
	wantOK(t, "any value", query(t, base, "SELECT 1", url.Values{"max_memory_usage": {"10Gi"}}))
	wantException(t, "bad text", query(t, base, "SELECT 1", url.Values{"max_memory_usage": {"abc"}}),
		"27", 400, "Cannot parse input: expected 'eof' before: 'abc'")
	wantException(t, "negative literal", query(t, base, "SELECT 1 SETTINGS max_memory_usage = -1", nil),
		"70", 500, "Field value -1 is out of range of unsigned long type")
}

// SET ... = DEFAULT drops the session's value, so the server default
// applies again.
func TestMaxMemoryUsageDefaultResetsTheSession(t *testing.T) {
	base := newMemoryLimitServer(t, 32_000_000)
	const exceeded = "Query memory limit exceeded: would use "
	sess := url.Values{"session_id": {"reset"}}
	wantOK(t, "lowering SET", query(t, base, "SET max_memory_usage = 1000000", sess))
	wantException(t, "lowered session", query(t, base, collect100k, sess), "241", 500, exceeded)
	wantOK(t, "SET DEFAULT", query(t, base, "SET max_memory_usage = DEFAULT", sess))
	wantOK(t, "session after DEFAULT", query(t, base, collect100k, sess))
	wantOK(t, "SET lower then DEFAULT", query(t, base, "SET max_memory_usage = 1000000, max_memory_usage = DEFAULT", sess))
	wantOK(t, "session after lower then DEFAULT", query(t, base, collect100k, sess))
	// As in ClickHouse, DEFAULT wins within one SET, wherever it is written.
	wantOK(t, "SET DEFAULT then lower", query(t, base, "SET max_memory_usage = DEFAULT, max_memory_usage = 1000000", sess))
	wantOK(t, "session after DEFAULT then lower", query(t, base, collect100k, sess))
	low := url.Values{"max_memory_usage": {"1000000"}}
	wantOK(t, "SETTINGS DEFAULT over the URL", query(t, base, collect100k+" SETTINGS max_memory_usage = DEFAULT", low))
}
