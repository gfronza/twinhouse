package server

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/exec"
	"github.com/gfronza/twinhouse/internal/format"
)

func TestHealthEndpoints(t *testing.T) {
	fe := &fakeExec{}
	_, ts := newTestServer(t, fe, nil)

	for _, path := range []string{"/ping", "/", "/replicas_status"} {
		r := get(t, ts.URL+path)
		if r.Status != 200 || r.Body != "Ok.\n" {
			t.Errorf("GET %s = %d %q", path, r.Status, r.Body)
		}
	}
	if ct := get(t, ts.URL+"/ping").Header.Get("Content-Type"); ct != "text/html; charset=UTF-8" {
		t.Errorf("ping content-type %q", ct)
	}
	req, _ := http.NewRequest(http.MethodHead, ts.URL+"/ping", nil)
	if r := do(t, req); r.Status != 200 || r.Body != "" {
		t.Errorf("HEAD /ping = %d %q", r.Status, r.Body)
	}
	// Health checks do not authenticate (like ClickHouse).
	if r := get(t, ts.URL+"/ping", "Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("nobody:x"))); r.Status != 200 {
		t.Errorf("ping with bad auth = %d", r.Status)
	}
	if fe.calls() != 0 {
		t.Errorf("health checks reached the executor")
	}

	r := get(t, ts.URL+"/nonexistent")
	if r.Status != 404 || !strings.HasPrefix(r.Body, "There is no handle /nonexistent\n\nUse / or /ping") {
		t.Errorf("unknown path = %d %q", r.Status, r.Body)
	}
	r = post(t, ts.URL+"/ping", "x")
	if r.Status != 404 {
		t.Errorf("POST /ping = %d", r.Status)
	}
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/?query=SELECT+1", nil)
	if r := do(t, req); r.Status != 501 {
		t.Errorf("PUT = %d", r.Status)
	}
	req, _ = http.NewRequest(http.MethodOptions, ts.URL+"/", nil)
	r = do(t, req)
	if r.Status != 204 || r.Header.Get("Access-Control-Allow-Origin") != "*" ||
		r.Header.Get("Access-Control-Allow-Methods") != "POST, GET, OPTIONS" {
		t.Errorf("OPTIONS = %d %v", r.Status, r.Header)
	}
	// "/?" carries an (empty) query: Empty query, not Ok.
	r = get(t, ts.URL+"/?")
	if r.Status != 400 || !strings.Contains(r.Body, "Empty query") {
		t.Errorf("GET /? = %d %q", r.Status, r.Body)
	}
}

func TestQueryAndDataSources(t *testing.T) {
	big := strings.Repeat(`{"a":1}`+"\n", (bodySplitThreshold/8)+10)
	bigValues := strings.Repeat("(1,'x'),", bodySplitThreshold/8+10)
	cases := []struct {
		name      string
		method    string
		url       string
		body      string
		wantQuery string
		wantData  *string
	}{
		{name: "GET query param", method: "GET", url: "/?query=SELECT+1", wantQuery: "SELECT 1"},
		{name: "POST body", method: "POST", url: "/", body: "SELECT 2", wantQuery: "SELECT 2"},
		{name: "POST empty query param uses body", method: "POST", url: "/?query=", body: "SELECT 3", wantQuery: "SELECT 3"},
		{name: "POST param + body concatenated", method: "POST", url: "/?query=SELECT", body: "1", wantQuery: "SELECT\n1"},
		{name: "POST param, empty body", method: "POST", url: "/?query=SELECT+4", body: "", wantQuery: "SELECT 4"},
		{name: "insert param, body is data", method: "POST", url: "/?query=" + url.QueryEscape("INSERT INTO t FORMAT JSONEachRow"),
			body: "{\"a\":1}\n", wantQuery: "INSERT INTO t FORMAT JSONEachRow", wantData: ptr("{\"a\":1}\n")},
		{name: "insert param with comment", method: "POST", url: "/?query=" + url.QueryEscape("/* c */ insert into t values"),
			body: "(1)", wantQuery: "/* c */ insert into t values", wantData: ptr("(1)")},
		{name: "small inline insert stays in query", method: "POST", url: "/", body: "INSERT INTO t FORMAT TSV\n1\tx\n",
			wantQuery: "INSERT INTO t FORMAT TSV\n1\tx\n"},
		{name: "large inline FORMAT insert is streamed", method: "POST", url: "/", body: "INSERT INTO db.t (a) FORMAT JSONEachRow  \n" + big,
			wantQuery: "INSERT INTO db.t (a) FORMAT JSONEachRow", wantData: ptr(big)},
		{name: "large inline VALUES insert is streamed", method: "POST", url: "/", body: "INSERT INTO t (a, `format`) VALUES " + bigValues,
			wantQuery: "INSERT INTO t (a, `format`) VALUES", wantData: ptr(" " + bigValues)},
		{name: "large non-insert read whole", method: "POST", url: "/", body: "SELECT '" + big + "'",
			wantQuery: "SELECT '" + big + "'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fe := &fakeExec{}
			_, ts := newTestServer(t, fe, nil)
			req, _ := http.NewRequest(tc.method, ts.URL+tc.url, strings.NewReader(tc.body))
			if r := do(t, req); r.Status != 200 {
				t.Fatalf("status %d: %s", r.Status, r.Body)
			}
			got := fe.last(t)
			if got.Query != tc.wantQuery {
				t.Errorf("query = %.80q, want %.80q", got.Query, tc.wantQuery)
			}
			if tc.wantData == nil {
				if got.HasData {
					t.Errorf("unexpected data %.80q", got.DataBytes)
				}
			} else if string(got.DataBytes) != *tc.wantData {
				t.Errorf("data = %.80q (len %d), want %.80q (len %d)", got.DataBytes, len(got.DataBytes), *tc.wantData, len(*tc.wantData))
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

func TestEmptyQueryAndLengthRequired(t *testing.T) {
	fe := &fakeExec{}
	_, ts := newTestServer(t, fe, nil)
	for _, body := range []string{"", "   \n\t", "-- just a comment\n"} {
		r := post(t, ts.URL+"/", body)
		if r.Status != 400 || r.Body != "Code: 62. DB::Exception: Empty query. (SYNTAX_ERROR) (version 25.8.1.1)\n" {
			t.Errorf("body %q: %d %q", body, r.Status, r.Body)
		}
	}
	// POST without Content-Length and not chunked (Go's client always sends
	// one, so write the request by hand).
	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "POST /?query=SELECT+1 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	res, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode != 411 || res.Header.Get("X-ClickHouse-Exception-Code") != "381" {
		t.Errorf("no content-length: %d %q", res.StatusCode, b)
	}
	r := post(t, ts.URL+"/", "--x\r\n", "Content-Type", "multipart/form-data; boundary=x")
	if r.Status != 501 || r.Header.Get("X-ClickHouse-Exception-Code") != "48" {
		t.Errorf("multipart: %d %q", r.Status, r.Body)
	}
	if fe.calls() != 0 {
		t.Error("executor called")
	}
}

func TestSettingsParamsAndQueryID(t *testing.T) {
	fe := &fakeExec{}
	_, ts := newTestServer(t, fe, nil)
	q := url.Values{
		"query":                   {"SELECT {x:UInt8}"},
		"param_x":                 {"7"},
		"param_name":              {"a b"},
		"max_threads":             {"3"},
		"custom_unknown":          {"z"},
		"database":                {"db1"},
		"default_format":          {"JSON"},
		"query_id":                {"qid-1"},
		"client_protocol_version": {"54460"},
		"compress":                {"0"},
		"wait_end_of_query":       {"1"},
		"enable_http_compression": {"0"},
		"user":                    {"default"},
		"password":                {""},
	}
	r := post(t, ts.URL+"/?"+q.Encode(), "")
	if r.Status != 200 {
		t.Fatalf("%d %s", r.Status, r.Body)
	}
	got := fe.last(t)
	wantSettings := map[string]string{"max_threads": "3", "custom_unknown": "z"}
	if fmt.Sprint(got.Settings) != fmt.Sprint(wantSettings) {
		t.Errorf("settings = %v, want %v", got.Settings, wantSettings)
	}
	if fmt.Sprint(got.Params) != fmt.Sprint(map[string]string{"x": "7", "name": "a b"}) {
		t.Errorf("params = %v", got.Params)
	}
	if got.Database != "db1" || got.QueryID != "qid-1" || got.User != "default" {
		t.Errorf("db=%q qid=%q user=%q", got.Database, got.QueryID, got.User)
	}
	if r.Header.Get("X-ClickHouse-Query-Id") != "qid-1" {
		t.Errorf("query id header %q", r.Header.Get("X-ClickHouse-Query-Id"))
	}
	if got.FormatOptions.ClientProtocolVersion != 0 || got.FormatOptions.Timezone != "UTC" {
		t.Errorf("input format options %+v", got.FormatOptions)
	}

	// Generated query ids are UUIDs.
	r = post(t, ts.URL+"/", "SELECT 1")
	id := r.Header.Get("X-ClickHouse-Query-Id")
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(id) || fe.last(t).QueryID != id {
		t.Errorf("generated query id %q", id)
	}
	// session_timezone overrides the reported timezone.
	r = post(t, ts.URL+"/?session_timezone=Europe/Berlin", "SELECT 1")
	if r.Header.Get("X-ClickHouse-Timezone") != "Europe/Berlin" || fe.last(t).FormatOptions.Timezone != "Europe/Berlin" {
		t.Errorf("timezone header %q", r.Header.Get("X-ClickHouse-Timezone"))
	}
}

func TestDatabaseResolution(t *testing.T) {
	cat := catalog.NewMemory()
	if err := cat.CreateDatabase(context.Background(), &catalog.Database{Name: "d1"}, false); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateDatabase(context.Background(), &catalog.Database{Name: "d2"}, false); err != nil {
		t.Fatal(err)
	}
	fe := &fakeExec{}
	_, ts := newTestServer(t, fe, func(c *Config) { c.Catalog = cat })

	cases := []struct {
		url, header, want string
	}{
		{"/", "", "default"},
		{"/?database=d1", "", "d1"},
		{"/", "d2", "d2"},
		{"/?database=d1", "d2", "d2"}, // header wins, like ClickHouse
		// Virtual databases exist without a catalog entry (ClickHouse:
		// SELECT currentDatabase() with database=system returns "system").
		{"/?database=system", "", "system"},
		{"/", "system", "system"},
		{"/?database=information_schema", "", "information_schema"},
		{"/?database=INFORMATION_SCHEMA", "", "INFORMATION_SCHEMA"},
	}
	for _, tc := range cases {
		var hdr []string
		if tc.header != "" {
			hdr = []string{"X-ClickHouse-Database", tc.header}
		}
		r := post(t, ts.URL+tc.url, "SELECT 1", hdr...)
		if r.Status != 200 || fe.last(t).Database != tc.want {
			t.Errorf("%s hdr=%q: status %d db %q, want %q", tc.url, tc.header, r.Status, fe.last(t).Database, tc.want)
		}
	}
	n := fe.calls()
	r := post(t, ts.URL+"/?database=nodb", "SELECT 1")
	if r.Status != 404 || r.Header.Get("X-ClickHouse-Exception-Code") != "81" || fe.calls() != n {
		t.Errorf("unknown database: %d %q", r.Status, r.Body)
	}
	// Virtual database names are case-sensitive, as in ClickHouse.
	for _, db := range []string{"SYSTEM", "Information_Schema"} {
		r := post(t, ts.URL+"/?database="+db, "SELECT 1")
		if r.Status != 404 || r.Header.Get("X-ClickHouse-Exception-Code") != "81" {
			t.Errorf("database %s: %d %q", db, r.Status, r.Body)
		}
	}
}

func TestAuthentication(t *testing.T) {
	users := map[string]string{"default": "", "alice": "secret", "bob": "pw:with:colons"}
	basic := func(u, p string) string { return "Basic " + base64.StdEncoding.EncodeToString([]byte(u+":"+p)) }
	cases := []struct {
		name     string
		query    string
		hdr      []string
		status   int
		code     string
		msg      string
		wantUser string
	}{
		{name: "anonymous is default", wantUser: "default", status: 200},
		{name: "headers", hdr: []string{"X-ClickHouse-User", "alice", "X-ClickHouse-Key", "secret"}, status: 200, wantUser: "alice"},
		{name: "basic", hdr: []string{"Authorization", basic("alice", "secret")}, status: 200, wantUser: "alice"},
		{name: "basic password with colons", hdr: []string{"Authorization", basic("bob", "pw:with:colons")}, status: 200, wantUser: "bob"},
		{name: "params", query: "user=alice&password=secret", status: 200, wantUser: "alice"},
		{name: "basic default empty password", hdr: []string{"Authorization", basic("default", "")}, status: 200, wantUser: "default"},
		{name: "wrong password", hdr: []string{"Authorization", basic("alice", "nope")}, status: 403, code: "516",
			msg: "Code: 516. DB::Exception: alice: Authentication failed: password is incorrect, or there is no user with such name. (AUTHENTICATION_FAILED) (version 25.8.1.1)\n"},
		{name: "unknown user", hdr: []string{"X-ClickHouse-User", "nobody"}, status: 403, code: "516"},
		{name: "default wrong password", query: "password=x", status: 401, code: "194"},
		{name: "headers and basic", hdr: []string{"X-ClickHouse-User", "alice", "Authorization", basic("alice", "secret")}, status: 403, code: "516",
			msg: "Code: 516. DB::Exception: Invalid authentication: it is not allowed to use X-ClickHouse HTTP headers and Authorization HTTP header simultaneously. (AUTHENTICATION_FAILED) (version 25.8.1.1)\n"},
		{name: "headers and params", query: "user=alice", hdr: []string{"X-ClickHouse-User", "alice"}, status: 403, code: "516"},
		{name: "basic and params", query: "user=alice", hdr: []string{"Authorization", basic("alice", "secret")}, status: 403, code: "516"},
		{name: "key header only", hdr: []string{"X-ClickHouse-Key", "secret"}, status: 403, code: "516",
			msg: "Code: 516. DB::Exception: Got an empty user name from X-ClickHouse HTTP headers. (AUTHENTICATION_FAILED) (version 25.8.1.1)\n"},
		{name: "empty user param", query: "user=", status: 403, code: "516"},
		{name: "bearer", hdr: []string{"Authorization", "Bearer abc"}, status: 403, code: "516",
			msg: "Code: 516. DB::Exception: Invalid authentication: 'Bearer' HTTP Authorization scheme is not supported. (AUTHENTICATION_FAILED) (version 25.8.1.1)\n"},
		{name: "basic garbage", hdr: []string{"Authorization", "Basic !!!"}, status: 403, code: "516"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fe := &fakeExec{}
			_, ts := newTestServer(t, fe, func(c *Config) { c.Users = users })
			u := ts.URL + "/"
			if tc.query != "" {
				u += "?" + tc.query
			}
			r := post(t, u, "SELECT 1", tc.hdr...)
			if r.Status != tc.status {
				t.Fatalf("status %d (%s), want %d", r.Status, r.Body, tc.status)
			}
			if tc.status == 200 {
				if fe.last(t).User != tc.wantUser {
					t.Errorf("user %q, want %q", fe.last(t).User, tc.wantUser)
				}
				return
			}
			if fe.calls() != 0 {
				t.Error("executor called on auth failure")
			}
			if got := r.Header.Get("X-ClickHouse-Exception-Code"); got != tc.code {
				t.Errorf("code %q, want %q", got, tc.code)
			}
			if tc.msg != "" && r.Body != tc.msg {
				t.Errorf("body %q\nwant %q", r.Body, tc.msg)
			}
			if tc.code == "194" {
				if r.Header.Get("WWW-Authenticate") != `Basic realm="ClickHouse server HTTP API"` || !strings.Contains(r.Body, "(REQUIRED_PASSWORD)") {
					t.Errorf("194 response: %v %q", r.Header, r.Body)
				}
			}
			// Like ClickHouse, auth failures carry no query id.
			if r.Header.Get("X-ClickHouse-Query-Id") != "" {
				t.Error("query id on auth failure")
			}
		})
	}
}

func TestReadonly(t *testing.T) {
	cases := []struct {
		name   string
		method string
		query  string // URL params (besides query)
		sql    string
		code   string
		msg    string
	}{
		{name: "GET select", method: "GET", sql: "SELECT 1"},
		{name: "GET with comment and parens", method: "GET", sql: "/* x */ (SELECT 1)"},
		{name: "GET show", method: "GET", sql: "SHOW TABLES"},
		{name: "GET describe", method: "GET", sql: "DESC t"},
		{name: "GET exists", method: "GET", sql: "EXISTS t"},
		{name: "GET set allowed (readonly=2)", method: "GET", sql: "SET max_threads = 1"},
		{name: "GET use", method: "GET", sql: "USE db"},
		{name: "GET insert", method: "GET", sql: "INSERT INTO t VALUES (1)", code: "164",
			msg: "Code: 164. DB::Exception: default: Cannot execute query in readonly mode. For queries over HTTP, method GET implies readonly. You should use method POST for modifying queries. (READONLY) (version 25.8.1.1)\n"},
		{name: "GET create", method: "GET", sql: "CREATE TABLE t (a UInt8) ENGINE = Memory", code: "164"},
		{name: "GET drop", method: "GET", sql: "drop table t", code: "164"},
		{name: "GET alter", method: "GET", sql: "ALTER TABLE t DELETE WHERE 1", code: "164"},
		{name: "GET optimize", method: "GET", sql: "OPTIMIZE TABLE t", code: "164"},
		{name: "GET readonly=1 param", method: "GET", query: "readonly=1", sql: "SELECT 1", code: "164",
			msg: "Code: 164. DB::Exception: Cannot modify 'readonly' setting in readonly mode. (READONLY) (version 25.8.1.1)\n"},
		{name: "GET readonly=2 param", method: "GET", query: "readonly=2", sql: "SELECT 1"},
		{name: "POST readonly=1 select", method: "POST", query: "readonly=1", sql: "SELECT 1"},
		{name: "POST readonly=1 insert", method: "POST", query: "readonly=1", sql: "INSERT INTO t VALUES (1)", code: "164",
			msg: "Code: 164. DB::Exception: default: Cannot execute query in readonly mode. (READONLY) (version 25.8.1.1)\n"},
		{name: "POST readonly=1 set", method: "POST", query: "readonly=1", sql: "SET max_threads=1", code: "164",
			msg: "Code: 164. DB::Exception: Cannot modify 'max_threads' setting in readonly mode. (READONLY) (version 25.8.1.1)\n"},
		{name: "POST readonly=2 set", method: "POST", query: "readonly=2", sql: "SET max_threads=1"},
		{name: "POST insert", method: "POST", sql: "INSERT INTO t VALUES (1)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fe := &fakeExec{}
			_, ts := newTestServer(t, fe, nil)
			u := ts.URL + "/?query=" + url.QueryEscape(tc.sql)
			if tc.query != "" {
				u += "&" + tc.query
			}
			req, _ := http.NewRequest(tc.method, u, nil)
			if tc.method == "POST" {
				req.ContentLength = 0
				req.Header.Set("Content-Length", "0")
				req.Body = http.NoBody
			}
			r := do(t, req)
			if tc.code == "" {
				if r.Status != 200 {
					t.Fatalf("status %d: %s", r.Status, r.Body)
				}
				if tc.method == "GET" && fe.last(t).Settings["readonly"] != "2" {
					t.Errorf("GET must pass readonly=2 to the engine, got %v", fe.last(t).Settings)
				}
				return
			}
			if r.Status != 500 || r.Header.Get("X-ClickHouse-Exception-Code") != tc.code {
				t.Fatalf("got %d code %s: %s", r.Status, r.Header.Get("X-ClickHouse-Exception-Code"), r.Body)
			}
			if tc.msg != "" && r.Body != tc.msg {
				t.Errorf("body %q\nwant %q", r.Body, tc.msg)
			}
			if fe.calls() != 0 {
				t.Error("executor called")
			}
		})
	}
}

func TestErrorRendering(t *testing.T) {
	cases := []struct {
		err    error
		status int
		body   string
	}{
		{cherr.New(cherr.UnknownTable, "Table default.t does not exist"), 404,
			"Code: 60. DB::Exception: Table default.t does not exist. (UNKNOWN_TABLE) (version 25.8.1.1)\n"},
		{cherr.New(cherr.UnknownFormat, "Unknown format a."), 404,
			"Code: 73. DB::Exception: Unknown format a. (UNKNOWN_FORMAT) (version 25.8.1.1)\n"},
		{cherr.New(cherr.UnknownTable, "Table default.nope does not exist. Maybe you meant system.one?"), 404,
			"Code: 60. DB::Exception: Table default.nope does not exist. Maybe you meant system.one?. (UNKNOWN_TABLE) (version 25.8.1.1)\n"},
		{cherr.New(cherr.SyntaxError, "Syntax error"), 400, ""},
		{cherr.New(cherr.NotImplemented, "x"), 501, ""},
		{cherr.New(cherr.Timeout, "x"), 408, ""},
		{cherr.New(cherr.ReadonlyQuery, "x"), 500, ""},
		{cherr.New(cherr.UnknownQueryParameter, "x"), 500, ""},
		{cherr.New(cherr.UnknownSetting, "x"), 404, ""},
		{cherr.New(cherr.Unavailable, "x"), 500, ""},
		{cherr.New(cherr.NotAnAggregate, "x"), 500, ""},
		{cherr.New(cherr.TypeMismatch, "x"), 400, ""},
		{fmt.Errorf("plain failure"), 500, "Code: 1001. DB::Exception: plain failure. (STD_EXCEPTION) (version 25.8.1.1)\n"},
		{fmt.Errorf("wrapped: %w", cherr.New(cherr.UnknownDatabase, "Database x does not exist")), 404,
			"Code: 81. DB::Exception: Database x does not exist. (UNKNOWN_DATABASE) (version 25.8.1.1)\n"},
		{context.DeadlineExceeded, 408, ""},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.err), func(t *testing.T) {
			fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) { return nil, tc.err }}
			_, ts := newTestServer(t, fe, nil)
			r := post(t, ts.URL+"/?query_id=q1", "SELECT 1")
			if r.Status != tc.status {
				t.Errorf("status %d, want %d", r.Status, tc.status)
			}
			ce := toCHError(tc.err)
			if r.Header.Get("X-ClickHouse-Exception-Code") != fmt.Sprint(ce.Code) {
				t.Errorf("code header %q", r.Header.Get("X-ClickHouse-Exception-Code"))
			}
			if tc.body != "" && r.Body != tc.body {
				t.Errorf("body %q\nwant %q", r.Body, tc.body)
			}
			if !strings.HasPrefix(r.Body, "Code: ") || !strings.HasSuffix(r.Body, "(version 25.8.1.1)\n") {
				t.Errorf("body shape %q", r.Body)
			}
			h := r.Header
			if h.Get("Content-Type") != "text/plain; charset=UTF-8" || h.Get("X-ClickHouse-Query-Id") != "q1" ||
				h.Get("X-ClickHouse-Timezone") != "UTC" || h.Get("X-ClickHouse-Server-Display-Name") != "test-node" ||
				h.Get("X-ClickHouse-Format") != "" || h.Get("X-ClickHouse-Summary") == "" {
				t.Errorf("headers %v", h)
			}
		})
	}
}

func TestJSONFormatException(t *testing.T) {
	failing := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) {
		return nil, cherr.New(cherr.UnknownTable, `Table "x/y" does not exist`)
	}}
	_, ts := newTestServer(t, failing, nil)
	text := `Code: 60. DB::Exception: Table \"x\/y\" does not exist. (UNKNOWN_TABLE) (version 25.8.1.1)`
	cases := []struct {
		format string
		body   string
	}{
		{"JSON", "{\n\t\"meta\":\n\t[\n\n\t],\n\n\t\"data\":\n\t[\n\n\t],\n\n\t\"rows\": 0,\n\n\t\"exception\": \"" + text + "\"\n}\n"},
		{"JSONEachRow", "{\"exception\": \"" + text + "\"}\n"},
		{"JSONCompactEachRow", "[\"" + text + "\"]\n"},
		{"JSONStringsEachRow", "{\"exception\": \"" + text + "\"}\n"},
	}
	for _, tc := range cases {
		canon, ok := formatCanonical(tc.format)
		if !ok {
			t.Errorf("format %s unknown to the format package", tc.format)
			continue
		}
		r := post(t, ts.URL+"/?default_format="+tc.format, "SELECT * FROM x")
		if r.Status != 404 || r.Body != tc.body {
			t.Errorf("%s: %d %q\nwant %q", tc.format, r.Status, r.Body, tc.body)
		}
		if r.Header.Get("X-ClickHouse-Format") != tc.format || r.Header.Get("Content-Type") != format.ContentType(canon) {
			t.Errorf("%s: headers %v", tc.format, r.Header)
		}
	}
	// X-ClickHouse-Format request header behaves like default_format.
	r := post(t, ts.URL+"/", "SELECT * FROM x", "X-ClickHouse-Format", "JSONEachRow")
	if r.Body != cases[1].body {
		t.Errorf("header format: %q", r.Body)
	}
	// Plain-text formats keep the text body.
	r = post(t, ts.URL+"/?default_format=CSV", "SELECT * FROM x")
	if !strings.HasPrefix(r.Body, "Code: 60.") || r.Header.Get("Content-Type") != "text/plain; charset=UTF-8" {
		t.Errorf("CSV error: %q %v", r.Body, r.Header)
	}
}

func TestSessions(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return now }
	fe := &fakeExec{fn: func(_ context.Context, req *exec.Request) (*exec.Result, error) {
		if strings.HasPrefix(req.Query, "USE ") {
			return &exec.Result{Database: strings.TrimPrefix(req.Query, "USE ")}, nil
		}
		return &exec.Result{}, nil
	}}
	s, ts := newTestServer(t, fe, func(c *Config) {
		c.Now = clock
		c.Users = map[string]string{"default": "", "u2": ""}
	})

	post(t, ts.URL+"/?session_id=s1", "USE d2")
	post(t, ts.URL+"/?session_id=s1", "SELECT 1")
	if db := fe.last(t).Database; db != "d2" {
		t.Errorf("session database %q", db)
	}
	// Explicit database applies to one query only.
	post(t, ts.URL+"/?session_id=s1&database=other", "SELECT 1")
	if db := fe.last(t).Database; db != "other" {
		t.Errorf("explicit db %q", db)
	}
	post(t, ts.URL+"/?session_id=s1", "SELECT 1")
	if db := fe.last(t).Database; db != "d2" {
		t.Errorf("session database after override %q", db)
	}
	// Without session_id, USE has no lasting effect.
	post(t, ts.URL+"/", "USE d3")
	post(t, ts.URL+"/", "SELECT 1")
	if db := fe.last(t).Database; db != "default" {
		t.Errorf("sessionless db %q", db)
	}
	// Sessions are per user.
	post(t, ts.URL+"/?session_id=s1&user=u2", "SELECT 1")
	if db := fe.last(t).Database; db != "default" {
		t.Errorf("other user's session db %q", db)
	}

	errCases := []struct {
		q, code string
	}{
		{"session_id=", "744"},
		{"session_id=x&session_timeout=abc", "374"},
		{"session_id=x&session_timeout=-1", "374"},
		{"session_id=x&session_timeout=100000", "374"},
		{"session_id=missing&session_check=1", "372"},
	}
	for _, ec := range errCases {
		r := post(t, ts.URL+"/?"+ec.q, "SELECT 1")
		if r.Status != 500 || r.Header.Get("X-ClickHouse-Exception-Code") != ec.code {
			t.Errorf("%s: %d %s", ec.q, r.Status, r.Body)
		}
	}
	if r := post(t, ts.URL+"/?session_id=s1&session_check=1", "SELECT 1"); r.Status != 200 {
		t.Errorf("session_check on live session: %d %s", r.Status, r.Body)
	}

	// Expiry after the inactivity timeout.
	post(t, ts.URL+"/?session_id=s5&session_timeout=10", "USE d5")
	now = now.Add(5 * time.Second)
	post(t, ts.URL+"/?session_id=s5", "SELECT 1")
	if db := fe.last(t).Database; db != "d5" {
		t.Errorf("s5 db %q", db)
	}
	now = now.Add(61 * time.Second) // default timeout of the last request is 60s
	if r := post(t, ts.URL+"/?session_id=s5&session_check=1", "SELECT 1"); r.Header.Get("X-ClickHouse-Exception-Code") != "372" {
		t.Errorf("expired session still alive: %d", r.Status)
	}
	// session_timeout=0 closes the session after the request.
	post(t, ts.URL+"/?session_id=s6&session_timeout=0", "USE d6")
	if r := post(t, ts.URL+"/?session_id=s6&session_check=1", "SELECT 1"); r.Header.Get("X-ClickHouse-Exception-Code") != "372" {
		t.Errorf("session with timeout 0 survived")
	}
	_ = s
}

func TestSessionLocked(t *testing.T) {
	started := make(chan struct{})
	unblock := make(chan struct{})
	fe := &fakeExec{fn: func(_ context.Context, req *exec.Request) (*exec.Result, error) {
		if req.Query == "SLOW" {
			close(started)
			<-unblock
		}
		return &exec.Result{}, nil
	}}
	_, ts := newTestServer(t, fe, nil)
	done := make(chan resp)
	go func() { done <- post(t, ts.URL+"/?session_id=lk", "SLOW") }()
	<-started
	r := post(t, ts.URL+"/?session_id=lk", "SELECT 1")
	if r.Status != 500 || r.Body != "Code: 373. DB::Exception: Session lk is locked by a concurrent client. (SESSION_IS_LOCKED) (version 25.8.1.1)\n" {
		t.Errorf("concurrent use: %d %q", r.Status, r.Body)
	}
	close(unblock)
	if r := <-done; r.Status != 200 {
		t.Errorf("slow query: %d", r.Status)
	}
	if r := post(t, ts.URL+"/?session_id=lk", "SELECT 1"); r.Status != 200 {
		t.Errorf("after release: %d %s", r.Status, r.Body)
	}
}

func TestSessionSet(t *testing.T) {
	fe := &fakeExec{}
	_, ts := newTestServer(t, fe, nil)
	r := post(t, ts.URL+"/?session_id=st", "SET max_threads = 3, my_s = 'abc', neg = -2")
	if r.Status != 200 {
		t.Fatalf("%d %s", r.Status, r.Body)
	}
	post(t, ts.URL+"/?session_id=st&neg=5", "SELECT 1")
	got := fe.last(t).Settings
	if !parserReady() {
		t.Skip("sqlparse not implemented yet")
	}
	if got["max_threads"] != "3" || got["my_s"] != "abc" || got["neg"] != "5" {
		t.Errorf("session settings %v", got)
	}
}

func TestSummaryAndResultHeadersNoOutput(t *testing.T) {
	fe := &fakeExec{fn: func(_ context.Context, req *exec.Request) (*exec.Result, error) {
		return &exec.Result{WrittenRows: 2}, nil
	}}
	_, ts := newTestServer(t, fe, nil)
	body := "{\"a\":1}\n{\"a\":2}\n"
	r := post(t, ts.URL+"/?query="+url.QueryEscape("INSERT INTO t FORMAT JSONEachRow"), body)
	if r.Status != 200 || r.Body != "" {
		t.Fatalf("%d %q", r.Status, r.Body)
	}
	if r.Header.Get("Content-Type") != "text/plain; charset=UTF-8" || r.Header.Get("X-ClickHouse-Format") != "" {
		t.Errorf("headers %v", r.Header)
	}
	sum := r.Header.Get("X-ClickHouse-Summary")
	re := regexp.MustCompile(`^\{"read_rows":"2","read_bytes":"16","written_rows":"2","written_bytes":"16","total_rows_to_read":"0","result_rows":"2","result_bytes":"16","elapsed_ns":"\d+"\}$`)
	if !re.MatchString(sum) {
		t.Errorf("summary %s", sum)
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(sum), &m); err != nil {
		t.Errorf("summary is not JSON: %v", err)
	}
}

func TestPanicInExecutor(t *testing.T) {
	fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) { panic("boom") }}
	_, ts := newTestServer(t, fe, nil)
	r := post(t, ts.URL+"/", "SELECT 1")
	if r.Status != 500 || r.Header.Get("X-ClickHouse-Exception-Code") != "49" {
		t.Errorf("%d %q", r.Status, r.Body)
	}
}

func TestRejectSessions(t *testing.T) {
	fe := &fakeExec{fn: func(_ context.Context, req *exec.Request) (*exec.Result, error) {
		if strings.HasPrefix(req.Query, "USE ") {
			return &exec.Result{Database: strings.TrimPrefix(req.Query, "USE ")}, nil
		}
		if req.Query == "SELECT 1" {
			return oneRow(), nil
		}
		return &exec.Result{}, nil
	}}
	_, ts := newTestServer(t, fe, func(c *Config) {
		c.RejectSessions = true
		c.Users = map[string]string{"default": "", "u2": "pw"}
	})

	const want = "Code: 48. DB::Exception: sessions are not supported by this deployment. (NOT_IMPLEMENTED) (version 25.8.1.1)\n"
	for _, q := range []string{
		"session_id=s1",
		"session_id=",
		"session_check=1",
		"session_timeout=10",
		"session_id=s1&session_timeout=10&session_check=1",
		"session_id=s1&user=u2&password=pw",
	} {
		before := fe.calls()
		r := post(t, ts.URL+"/?"+q, "SELECT 1")
		if r.Status != 501 || r.Header.Get("X-ClickHouse-Exception-Code") != "48" || r.Body != want {
			t.Errorf("%s: status %d code %q body %q", q, r.Status, r.Header.Get("X-ClickHouse-Exception-Code"), r.Body)
		}
		if fe.calls() != before {
			t.Errorf("%s: executor was called", q)
		}
		// GET too. Like the other session errors, this one is raised before
		// default_format is known, so it is always plain text.
		r = get(t, ts.URL+"/?"+q+"&query=SELECT+1&default_format=JSONEachRow")
		if r.Status != 501 || r.Header.Get("X-ClickHouse-Exception-Code") != "48" || r.Body != want {
			t.Errorf("GET %s: status %d body %q", q, r.Status, r.Body)
		}
	}
	// Authentication still comes first.
	if r := post(t, ts.URL+"/?session_id=s1&user=u2&password=wrong", "SELECT 1"); r.Status != 403 || r.Header.Get("X-ClickHouse-Exception-Code") != "516" {
		t.Errorf("bad password: %d %s", r.Status, r.Body)
	}

	// Everything else is unaffected: queries, settings, database, query_id,
	// and a session-less USE (no lasting effect, as without sessions).
	requireEncoders(t)
	r := post(t, ts.URL+"/?query_id=q1&max_block_size=10&database=default", "SELECT 1")
	if r.Status != 200 || r.Body != "1\n" || r.Header.Get("X-ClickHouse-Query-Id") != "q1" {
		t.Errorf("plain query: %d %q %v", r.Status, r.Body, r.Header)
	}
	if got := fe.last(t).Settings["max_block_size"]; got != "10" {
		t.Errorf("settings %v", fe.last(t).Settings)
	}
	post(t, ts.URL+"/", "USE d3")
	post(t, ts.URL+"/", "SELECT 1")
	if db := fe.last(t).Database; db != "default" {
		t.Errorf("sessionless db %q", db)
	}
	if r := get(t, ts.URL+"/ping"); r.Status != 200 || r.Body != "Ok.\n" {
		t.Errorf("ping: %d %q", r.Status, r.Body)
	}
}
