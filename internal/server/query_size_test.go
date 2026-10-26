package server

import (
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/exec"
	"github.com/gfronza/twinhouse/internal/storage/memstore"
)

// selectOfSize returns a SELECT of exactly n bytes.
func selectOfSize(n int) string {
	const head = "SELECT ''"
	return "SELECT '" + strings.Repeat("a", n-len(head)) + "'"
}

func TestMaxQuerySize(t *testing.T) {
	fe := &fakeExec{}
	_, ts := newTestServer(t, fe, nil)
	values := strings.Repeat("(1,'x'),", 600<<10/8)
	cases := []struct {
		name, url, body string
		code            string // "" = runs
	}{
		{"default fits", "/", selectOfSize(defaultMaxQuerySize), ""},
		{"default exceeded", "/", selectOfSize(defaultMaxQuerySize + 1), "62"},
		{"query parameter and body exceeded", "/?query=SELECT", strings.Repeat(" ", defaultMaxQuerySize), "62"},
		{"raised by the URL", "/?max_query_size=600000", selectOfSize(500000), ""},
		{"lowered by the URL", "/?max_query_size=10", "SELECT 1234", "62"},
		{"zero means the server ceiling", "/?max_query_size=0", selectOfSize(500000), ""},
		{"malformed", "/?max_query_size=1x", "SELECT 1", "27"},
		{"decimal size suffix", "/?max_query_size=1M", selectOfSize(900000), ""},
		{"binary size suffix", "/?max_query_size=1Mi", selectOfSize(1 << 20), ""},
		{"binary size suffix exceeded", "/?max_query_size=1Ki", selectOfSize(1<<10 + 1), "62"},
		{"suffix overflow", "/?max_query_size=20000000T", "SELECT 1", "72"},
		{"inline VALUES are data", "/", "INSERT INTO t VALUES " + values, ""},
		{"inline FORMAT data is data", "/?max_query_size=40", "INSERT INTO t FORMAT TSV\n" + strings.Repeat("1\n", 100), ""},
		{"INSERT header is counted", "/?max_query_size=20", "INSERT INTO t (a, b, c) VALUES (1, 2, 3)", "62"},
		{"INSERT SELECT is counted", "/?max_query_size=30", "INSERT INTO t SELECT 1234567890", "62"},
		{"INSERT parameter, body is data", "/?max_query_size=40&query=" + url.QueryEscape("INSERT INTO t VALUES"), values, ""},
		{"INSERT parameter is counted", "/?max_query_size=10&query=" + url.QueryEscape("INSERT INTO t VALUES"), "(1)", "62"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := post(t, ts.URL+tc.url, tc.body)
			if got := r.Header.Get("X-ClickHouse-Exception-Code"); got != tc.code {
				t.Fatalf("code %q, want %q: %d %.300s", got, tc.code, r.Status, r.Body)
			}
		})
	}
}

func TestMaxQuerySizeIsCappedByServer(t *testing.T) {
	fe := &fakeExec{}
	_, ts := newTestServer(t, fe, func(c *Config) { c.MaxQuerySize = 1000 })
	if r := post(t, ts.URL+"/?max_query_size=1000000", selectOfSize(1001)); r.Header.Get("X-ClickHouse-Exception-Code") != "62" {
		t.Fatalf("%d %.300s", r.Status, r.Body)
	}
	if r := post(t, ts.URL+"/", selectOfSize(1000)); r.Status != 200 {
		t.Fatalf("%d %.300s", r.Status, r.Body)
	}
}

func TestMaxQuerySizeFromSession(t *testing.T) {
	fe := &fakeExec{}
	_, ts := newTestServer(t, fe, nil)
	sess := ts.URL + "/?session_id=" + url.QueryEscape(t.Name())
	if r := post(t, sess, "SET max_query_size = '1K'"); r.Status != 200 {
		t.Fatalf("%d %.300s", r.Status, r.Body)
	}
	if r := post(t, sess, selectOfSize(1000)); r.Status != 200 {
		t.Fatalf("%d %.300s", r.Status, r.Body)
	}
	if r := post(t, sess, selectOfSize(1001)); r.Header.Get("X-ClickHouse-Exception-Code") != "62" {
		t.Fatalf("%d %.300s", r.Status, r.Body)
	}
	if r := post(t, sess, "SET max_query_size = 10"); r.Status != 200 {
		t.Fatalf("%d %.300s", r.Status, r.Body)
	}
	if r := post(t, sess, "SELECT 1234"); r.Header.Get("X-ClickHouse-Exception-Code") != "62" {
		t.Fatalf("%d %.300s", r.Status, r.Body)
	}
}

func TestSetMaxQuerySizeIsValidated(t *testing.T) {
	fe := &fakeExec{}
	_, ts := newTestServer(t, fe, nil)
	sess := ts.URL + "/?session_id=" + url.QueryEscape(t.Name())
	for q, code := range map[string]string{
		"SET max_query_size = 'abc'":                  "72",
		"SET max_query_size = '1x'":                   "27",
		"SET max_query_size = '20000000T'":            "72",
		"SET max_threads = 2, max_query_size = 'abc'": "72",
	} {
		if r := post(t, sess, q); r.Header.Get("X-ClickHouse-Exception-Code") != code {
			t.Errorf("%s: code %q, want %s: %d %.300s", q, r.Header.Get("X-ClickHouse-Exception-Code"), code, r.Status, r.Body)
		}
	}
	if r := post(t, sess, "SELECT 1"); r.Status != 200 {
		t.Fatalf("after the rejected SETs: %d %.300s", r.Status, r.Body)
	}
}

func TestBadSessionMaxQuerySizeAllowsItsSet(t *testing.T) {
	fe := &fakeExec{}
	s, ts := newTestServer(t, fe, nil)
	id := t.Name()
	sess := ts.URL + "/?session_id=" + url.QueryEscape(id)
	if r := post(t, sess, "SELECT 1"); r.Status != 200 {
		t.Fatalf("%d %.300s", r.Status, r.Body)
	}
	s.sessions.mu.Lock()
	s.sessions.sessions[sessionKey{"default", id}].settings["max_query_size"] = "abc"
	s.sessions.mu.Unlock()
	if r := post(t, sess, "SELECT 1"); r.Header.Get("X-ClickHouse-Exception-Code") != "72" {
		t.Fatalf("SELECT with a bad stored value: %d %.300s", r.Status, r.Body)
	}
	if r := post(t, sess, "SET max_query_size = 1000"); r.Status != 200 {
		t.Fatalf("SET: %d %.300s", r.Status, r.Body)
	}
	if r := post(t, sess, "SELECT 2"); r.Status != 200 {
		t.Fatalf("SELECT after SET: %d %.300s", r.Status, r.Body)
	}
}

// TestInlineValuesOverMaxQuerySizeInsert checks that inline VALUES rows,
// which the engine parses with the query, are not counted.
func TestInlineValuesOverMaxQuerySizeInsert(t *testing.T) {
	cat := catalog.NewMemory()
	eng, err := exec.New(exec.Config{Store: memstore.New(), Catalog: cat, Region: "local", ServerVersion: "25.8.1.1", Timezone: "UTC"})
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{Executor: eng, Catalog: cat, Logger: quietLogger()})
	if err != nil {
		t.Fatal(err)
	}
	base := httpTestServer(t, s)
	if r := post(t, base+"/", "CREATE TABLE t (a UInt32, s String) ENGINE = MergeTree ORDER BY a"); r.Status != 200 {
		t.Fatalf("%d %s", r.Status, r.Body)
	}
	const rows = 30000
	var b strings.Builder
	b.WriteString("INSERT INTO t VALUES ")
	for i := range rows {
		b.WriteString("(" + strconv.Itoa(i) + ",'xxxxxxx'),")
	}
	if b.Len() <= defaultMaxQuerySize || b.Len() > bodySplitThreshold {
		t.Fatalf("body of %d bytes does not exercise the inline path", b.Len())
	}
	if r := post(t, base+"/", b.String()); r.Status != 200 {
		t.Fatalf("%d %.300s", r.Status, r.Body)
	}
	if r := post(t, base+"/", "SELECT count() FROM t"); r.Body != strconv.Itoa(rows)+"\n" {
		t.Fatalf("count: %d %q", r.Status, r.Body)
	}
}
