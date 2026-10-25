//go:build e2e

package e2e

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"
)

// The fixture rows of http_metrics and their rendering in each output
// format, as produced by ClickHouse 25.8 (verified against the reference).
const httpFixture = `{"tenant_id":"acme","day":"2024-05-06","clicks":10,"ctr":0.25,"note":null,"tags":["a","b"],"ts":"2024-05-06 01:02:03.456"}
{"tenant_id":"glo\tbex","day":"2024-05-07","clicks":18446744073709551615,"ctr":1.5,"note":"he said \"hi\"","tags":[],"ts":"2024-05-07 00:00:00.000"}
`

var httpFormats = map[string]string{
	"TabSeparated": "acme\t2024-05-06\t10\t0.25\t\\N\t['a','b']\t2024-05-06 01:02:03.456\n" +
		"glo\\tbex\t2024-05-07\t18446744073709551615\t1.5\the said \"hi\"\t[]\t2024-05-07 00:00:00.000\n",
	"TSV": "acme\t2024-05-06\t10\t0.25\t\\N\t['a','b']\t2024-05-06 01:02:03.456\n" +
		"glo\\tbex\t2024-05-07\t18446744073709551615\t1.5\the said \"hi\"\t[]\t2024-05-07 00:00:00.000\n",
	"TSVWithNames": "tenant_id\tday\tclicks\tctr\tnote\ttags\tts\n" +
		"acme\t2024-05-06\t10\t0.25\t\\N\t['a','b']\t2024-05-06 01:02:03.456\n" +
		"glo\\tbex\t2024-05-07\t18446744073709551615\t1.5\the said \"hi\"\t[]\t2024-05-07 00:00:00.000\n",
	"TSVWithNamesAndTypes": "tenant_id\tday\tclicks\tctr\tnote\ttags\tts\n" +
		"String\tDate\tUInt64\tFloat64\tNullable(String)\tArray(String)\tDateTime64(3)\n" +
		"acme\t2024-05-06\t10\t0.25\t\\N\t['a','b']\t2024-05-06 01:02:03.456\n" +
		"glo\\tbex\t2024-05-07\t18446744073709551615\t1.5\the said \"hi\"\t[]\t2024-05-07 00:00:00.000\n",
	"CSV": `"acme","2024-05-06",10,0.25,\N,"['a','b']","2024-05-06 01:02:03.456"` + "\n" +
		`"glo	bex","2024-05-07",18446744073709551615,1.5,"he said ""hi""","[]","2024-05-07 00:00:00.000"` + "\n",
	"CSVWithNames": `"tenant_id","day","clicks","ctr","note","tags","ts"` + "\n" +
		`"acme","2024-05-06",10,0.25,\N,"['a','b']","2024-05-06 01:02:03.456"` + "\n" +
		`"glo	bex","2024-05-07",18446744073709551615,1.5,"he said ""hi""","[]","2024-05-07 00:00:00.000"` + "\n",
	"JSONEachRow": `{"tenant_id":"acme","day":"2024-05-06","clicks":10,"ctr":0.25,"note":null,"tags":["a","b"],"ts":"2024-05-06 01:02:03.456"}` + "\n" +
		`{"tenant_id":"glo\tbex","day":"2024-05-07","clicks":18446744073709551615,"ctr":1.5,"note":"he said \"hi\"","tags":[],"ts":"2024-05-07 00:00:00.000"}` + "\n",
	"JSONCompactEachRow": `["acme", "2024-05-06", 10, 0.25, null, ["a","b"], "2024-05-06 01:02:03.456"]` + "\n" +
		`["glo\tbex", "2024-05-07", 18446744073709551615, 1.5, "he said \"hi\"", [], "2024-05-07 00:00:00.000"]` + "\n",
	"Values": `('acme','2024-05-06',10,0.25,NULL,['a','b'],'2024-05-06 01:02:03.456'),('glo\tbex','2024-05-07',18446744073709551615,1.5,'he said "hi"',[],'2024-05-07 00:00:00.000')`,
}

var contentTypes = map[string]string{
	"TabSeparated": "text/tab-separated-values; charset=UTF-8",
	"CSV":          "text/csv; charset=UTF-8; header=absent",
	"JSONEachRow":  "application/x-ndjson; charset=UTF-8",
	"JSON":         "application/json; charset=UTF-8",
	"JSONCompact":  "application/json; charset=UTF-8",
}

var errBodyRe = regexp.MustCompile(`^Code: (\d+)\. DB::Exception: .+\. \(([A-Z_]+)\) \(version [0-9.]+[^)]*\)\n$`)

func TestHTTPLoader(t *testing.T) {
	t.Parallel()
	db := newDB(t, "http")
	mustExec(t, db, `CREATE TABLE http_metrics (tenant_id String, day Date, clicks UInt64, ctr Float64,
		note Nullable(String), tags Array(String), ts DateTime64(3)) ENGINE = MergeTree ORDER BY (tenant_id, day)`)

	// Python-style loader: query in URL, NDJSON in body.
	r := doHTTP(t, req{
		Params: url.Values{"query": {"INSERT INTO " + db + ".http_metrics FORMAT JSONEachRow"}},
		Body:   strings.NewReader(httpFixture),
	})
	if r.Status != 200 || r.Body != "" {
		t.Fatalf("JSONEachRow insert: %d %q", r.Status, r.Body)
	}
	var sum map[string]string
	if err := json.Unmarshal([]byte(r.Header.Get("X-ClickHouse-Summary")), &sum); err != nil {
		t.Errorf("X-ClickHouse-Summary %q: %v", r.Header.Get("X-ClickHouse-Summary"), err)
	} else if sum["written_rows"] != "2" {
		t.Errorf("summary written_rows = %q, want 2 (%v)", sum["written_rows"], sum)
	}
	if r.Header.Get("X-ClickHouse-Query-Id") == "" {
		t.Errorf("missing X-ClickHouse-Query-Id")
	}

	t.Run("formats", func(t *testing.T) {
		for f, want := range httpFormats {
			r := post(t, db, "SELECT * FROM http_metrics ORDER BY tenant_id FORMAT "+f)
			if r.Status != 200 || r.Body != want {
				t.Errorf("FORMAT %s:\n got  %q\n want %q", f, r.Body, want)
			}
			// ClickHouse echoes the format name as written (TSV stays TSV).
			if got := r.Header.Get("X-ClickHouse-Format"); got != f {
				t.Errorf("FORMAT %s: X-ClickHouse-Format = %q", f, got)
			}
			if ct, ok := contentTypes[f]; ok && r.Header.Get("Content-Type") != ct {
				t.Errorf("FORMAT %s: Content-Type = %q, want %q", f, r.Header.Get("Content-Type"), ct)
			}
		}
	})

	t.Run("json", func(t *testing.T) {
		for _, f := range []string{"JSON", "JSONCompact"} {
			r := post(t, db, "SELECT tenant_id, clicks, ctr, note, tags FROM http_metrics ORDER BY tenant_id FORMAT "+f)
			if r.Status != 200 {
				t.Fatalf("%s: %d %s", f, r.Status, r.Body)
			}
			if r.Header.Get("Content-Type") != contentTypes[f] {
				t.Errorf("%s Content-Type = %q", f, r.Header.Get("Content-Type"))
			}
			var doc struct {
				Meta []struct{ Name, Type string } `json:"meta"`
				Data json.RawMessage               `json:"data"`
				Rows int                           `json:"rows"`
				Stat map[string]any                `json:"statistics"`
			}
			dec := json.NewDecoder(strings.NewReader(r.Body))
			dec.UseNumber()
			if err := dec.Decode(&doc); err != nil {
				t.Fatalf("%s: invalid JSON: %v\n%s", f, err, r.Body)
			}
			var mt []string
			for _, m := range doc.Meta {
				mt = append(mt, m.Name+":"+m.Type)
			}
			if strings.Join(mt, ",") != "tenant_id:String,clicks:UInt64,ctr:Float64,note:Nullable(String),tags:Array(String)" {
				t.Errorf("%s meta = %v", f, mt)
			}
			if doc.Rows != 2 || doc.Stat == nil {
				t.Errorf("%s rows=%d statistics=%v", f, doc.Rows, doc.Stat)
			}
			var data any
			d2 := json.NewDecoder(bytes.NewReader(doc.Data))
			d2.UseNumber()
			_ = d2.Decode(&data)
			got, _ := json.Marshal(data)
			want := `[{"clicks":10,"ctr":0.25,"note":null,"tags":["a","b"],"tenant_id":"acme"},{"clicks":18446744073709551615,"ctr":1.5,"note":"he said \"hi\"","tags":[],"tenant_id":"glo\tbex"}]`
			if f == "JSONCompact" {
				want = `[["acme",10,0.25,null,["a","b"]],["glo\tbex",18446744073709551615,1.5,"he said \"hi\"",[]]]`
			}
			if string(got) != want {
				t.Errorf("%s data = %s\nwant %s", f, got, want)
			}
		}
	})

	t.Run("gzip_insert_and_query", func(t *testing.T) {
		rows := `{"tenant_id":"zeta","day":"2024-06-01","clicks":5,"ctr":0.5,"note":"gz","tags":["z"],"ts":"2024-06-01 00:00:00.001"}` + "\n"
		r := doHTTP(t, req{
			Params:  url.Values{"query": {"INSERT INTO http_metrics FORMAT JSONEachRow"}, "database": {db}},
			Body:    bytes.NewReader(gzipBytes(t, []byte(rows))),
			Headers: map[string]string{"Content-Encoding": "gzip"},
		})
		if r.Status != 200 {
			t.Fatalf("gzip insert: %d %s", r.Status, r.Body)
		}
		// Query itself in a gzip body.
		r = doHTTP(t, req{
			Params:  url.Values{"database": {db}},
			Body:    bytes.NewReader(gzipBytes(t, []byte("SELECT note, ts FROM http_metrics WHERE tenant_id = 'zeta'"))),
			Headers: map[string]string{"Content-Encoding": "gzip"},
		})
		if r.Status != 200 || r.Body != "gz\t2024-06-01 00:00:00.001\n" {
			t.Errorf("gzip query body: %d %q", r.Status, r.Body)
		}
		// Compressed response.
		hr, _ := http.NewRequest("GET", baseURL+"/?"+url.Values{"enable_http_compression": {"1"}, "database": {db},
			"query": {"SELECT tenant_id FROM http_metrics ORDER BY tenant_id"}}.Encode(), nil)
		hr.Header.Set("Accept-Encoding", "gzip")
		resp, err := httpClient.Do(hr)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.Header.Get("Content-Encoding") != "gzip" {
			t.Fatalf("response Content-Encoding = %q", resp.Header.Get("Content-Encoding"))
		}
		zr, err := gzip.NewReader(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(zr)
		if string(b) != "acme\nglo\\tbex\nzeta\n" {
			t.Errorf("gzip response = %q", b)
		}
	})

	t.Run("json_loader_edge_cases", func(t *testing.T) {
		// Omitted columns take DEFAULTs, unknown keys are skipped, numbers may
		// be quoted, DateTime64 accepts unix millis, JSON arrays of objects
		// and blank lines are accepted, null in a non-Nullable column is the
		// default (all verified against ClickHouse 25.8).
		mustExec(t, db, `CREATE TABLE loose (tenant_id String, day Date, clicks UInt64 DEFAULT 7, note Nullable(String), tags Array(String),
			ts DateTime64(3), lc LowCardinality(String), flag Bool, u UUID, f Float64) ENGINE = MergeTree ORDER BY tenant_id`)
		bodies := []string{
			`{"tenant_id":"a","day":"2024-01-01"}` + "\n",
			`{"tenant_id":"b","day":"2024-01-02","unknown_field":1,"clicks":"12","ts":1714986900123,"flag":true,"f":"1.5","tags":null}` + "\n",
			`[{"tenant_id":"d","day":"2024-01-04"},{"tenant_id":"e","day":"2024-01-05"}]`,
			`{"tenant_id":"f","day":"2024-01-06","ts":"2024-05-06 09:15:00"}` + "\n\n" + `{"tenant_id":"g","day":"2024-01-07","clicks":null}` + "\n",
		}
		for _, b := range bodies {
			r := doHTTP(t, req{Params: url.Values{"query": {"INSERT INTO loose FORMAT JSONEachRow"}, "database": {db}}, Body: strings.NewReader(b)})
			if r.Status != 200 {
				t.Errorf("insert %s: %d %s", b, r.Status, r.Body)
			}
		}
		// ISO-8601 with Z is rejected without date_time_input_format=best_effort.
		r := doHTTP(t, req{Params: url.Values{"query": {"INSERT INTO loose FORMAT JSONEachRow"}, "database": {db}},
			Body: strings.NewReader(`{"tenant_id":"c","day":"2024-01-03","ts":"2024-05-06T09:15:00.120Z"}`)})
		if r.Status == 200 {
			t.Errorf("ISO-8601 'Z' timestamp accepted without best_effort")
		}
		zero := "\tfalse\t00000000-0000-0000-0000-000000000000\t0\n"
		want := "a\t2024-01-01\t7\t\\N\t[]\t1970-01-01 00:00:00.000\t" + zero +
			"b\t2024-01-02\t12\t\\N\t[]\t2024-05-06 09:15:00.123\t\ttrue\t00000000-0000-0000-0000-000000000000\t1.5\n" +
			"d\t2024-01-04\t7\t\\N\t[]\t1970-01-01 00:00:00.000\t" + zero +
			"e\t2024-01-05\t7\t\\N\t[]\t1970-01-01 00:00:00.000\t" + zero +
			"f\t2024-01-06\t7\t\\N\t[]\t2024-05-06 09:15:00.000\t" + zero +
			"g\t2024-01-07\t7\t\\N\t[]\t1970-01-01 00:00:00.000\t" + zero
		if got := tsv(t, db, "SELECT * FROM loose ORDER BY tenant_id"); got != want {
			t.Errorf("loose JSONEachRow:\n%q\nwant\n%q", got, want)
		}
	})

	t.Run("other_input_formats", func(t *testing.T) {
		mustExec(t, db, "CREATE TABLE loads (k String, v UInt64, s Nullable(String)) ENGINE = MergeTree ORDER BY k")
		cases := []struct{ query, body string }{
			{"INSERT INTO loads FORMAT CSV", "\"c1\",1,\"x,y\"\nc2,2,\\N\n"},
			{"INSERT INTO loads FORMAT TabSeparated", "t1\t3\ta\\tb\nt2\t4\t\\N\n"},
			{"INSERT INTO loads FORMAT TSVWithNames", "v\tk\n5\tn1\n"},
			{"INSERT INTO loads FORMAT JSONCompactEachRow", "[\"j1\", 6, null]\n"},
			{"INSERT INTO loads (k, v) VALUES", "('v1', 7), ('v2', 8)"},
		}
		for _, c := range cases {
			r := doHTTP(t, req{Params: url.Values{"query": {c.query}, "database": {db}}, Body: strings.NewReader(c.body)})
			if r.Status != 200 {
				t.Errorf("%s: %d %s", c.query, r.Status, r.Body)
			}
		}
		// Inline data after FORMAT in the body (no query param).
		r := post(t, db, "INSERT INTO loads FORMAT JSONEachRow\n{\"k\":\"b1\",\"v\":9,\"s\":\"inline\"}\n")
		if r.Status != 200 {
			t.Errorf("inline JSONEachRow: %d %s", r.Status, r.Body)
		}
		got := tsv(t, db, "SELECT k, v, s FROM loads ORDER BY k")
		want := "b1\t9\tinline\nc1\t1\tx,y\nc2\t2\t\\N\nj1\t6\t\\N\nn1\t5\t\\N\nt1\t3\ta\\tb\nt2\t4\t\\N\nv1\t7\t\\N\nv2\t8\t\\N\n"
		if got != want {
			t.Errorf("loads =\n%s\nwant\n%s", got, want)
		}
		// Malformed input fails without partial garbage.
		r = doHTTP(t, req{Params: url.Values{"query": {"INSERT INTO loads FORMAT JSONEachRow"}, "database": {db}},
			Body: strings.NewReader("{\"k\":\"bad\",\"v\":\"not-a-number\"}\n")})
		if r.Status == 200 {
			t.Errorf("malformed JSONEachRow accepted")
		}
		if out := tsv(t, db, "SELECT count() FROM loads WHERE k = 'bad'"); out != "0\n" {
			t.Errorf("malformed row stored: %q", out)
		}
	})

	t.Run("query_placement", func(t *testing.T) {
		// GET with query in URL.
		r := doHTTP(t, req{Method: "GET", Params: url.Values{"query": {"SELECT count() FROM http_metrics WHERE tenant_id = 'acme'"}, "database": {db}}})
		if r.Status != 200 || r.Body != "1\n" {
			t.Errorf("GET: %d %q", r.Status, r.Body)
		}
		// POST with query in body.
		if r := post(t, db, "SELECT clicks FROM http_metrics WHERE tenant_id = 'acme'"); r.Body != "10\n" {
			t.Errorf("POST body: %q", r.Body)
		}
		// POST with query in URL and empty body.
		r = doHTTP(t, req{Params: url.Values{"query": {"SELECT 1 + 1"}}})
		if r.Status != 200 || r.Body != "2\n" {
			t.Errorf("POST url query: %d %q", r.Status, r.Body)
		}
		// Query split between URL and body (ClickHouse concatenates them).
		r = doHTTP(t, req{Params: url.Values{"query": {"SELECT "}}, Body: strings.NewReader("40 + 2")})
		if r.Status != 200 || r.Body != "42\n" {
			t.Errorf("URL+body query: %d %q", r.Status, r.Body)
		}
		// GET is readonly.
		for _, q := range []string{"INSERT INTO " + db + ".http_metrics (tenant_id) VALUES ('x')", "CREATE TABLE " + db + ".ro (x String) ENGINE = MergeTree ORDER BY x", "DROP TABLE " + db + ".http_metrics"} {
			r := doHTTP(t, req{Method: "GET", Params: url.Values{"query": {q}}})
			if r.Status == 200 || r.Header.Get("X-ClickHouse-Exception-Code") != "164" {
				t.Errorf("GET %q: %d code=%q %s", q, r.Status, r.Header.Get("X-ClickHouse-Exception-Code"), r.Body)
			}
		}
		if out := tsv(t, db, "SELECT count() FROM http_metrics"); out != "3\n" && out != "2\n" {
			t.Errorf("readonly GET modified data: count = %q", out)
		}
	})

	t.Run("headers_and_defaults", func(t *testing.T) {
		// default_format.
		r := doHTTP(t, req{Method: "GET", Params: url.Values{"default_format": {"JSONEachRow"}, "database": {db},
			"query": {"SELECT tenant_id, clicks FROM http_metrics WHERE tenant_id = 'acme'"}}})
		if r.Body != "{\"tenant_id\":\"acme\",\"clicks\":10}\n" || r.Header.Get("X-ClickHouse-Format") != "JSONEachRow" {
			t.Errorf("default_format: %q %q", r.Body, r.Header.Get("X-ClickHouse-Format"))
		}
		// FORMAT clause beats default_format.
		r = doHTTP(t, req{Method: "GET", Params: url.Values{"default_format": {"JSONEachRow"}, "query": {"SELECT 1 AS x FORMAT CSV"}}})
		if r.Body != "1\n" {
			t.Errorf("FORMAT over default_format: %q", r.Body)
		}
		// Query id echo.
		r = doHTTP(t, req{Method: "GET", Params: url.Values{"query_id": {"e2e-" + runID}, "query": {"SELECT 1"}}})
		if r.Header.Get("X-ClickHouse-Query-Id") != "e2e-"+runID {
			t.Errorf("X-ClickHouse-Query-Id = %q", r.Header.Get("X-ClickHouse-Query-Id"))
		}
		if r.Header.Get("X-ClickHouse-Timezone") == "" || r.Header.Get("X-ClickHouse-Server-Display-Name") == "" {
			t.Errorf("missing timezone/display-name headers: %v", r.Header)
		}
		// Summary on SELECT.
		r = post(t, db, "SELECT * FROM http_metrics WHERE tenant_id = 'acme'")
		var sum map[string]string
		if err := json.Unmarshal([]byte(r.Header.Get("X-ClickHouse-Summary")), &sum); err != nil {
			t.Errorf("summary: %v", err)
		} else if sum["read_rows"] == "" || sum["written_rows"] != "0" {
			t.Errorf("SELECT summary = %v", sum)
		}
		// Database via X-ClickHouse-Database header and database param.
		r = doHTTP(t, req{Body: strings.NewReader("SELECT currentDatabase(), count() FROM http_metrics"), Headers: map[string]string{"X-ClickHouse-Database": db}})
		if r.Status != 200 || !strings.HasPrefix(r.Body, db+"\t") {
			t.Errorf("X-ClickHouse-Database: %d %q", r.Status, r.Body)
		}
		r = doHTTP(t, req{Method: "GET", Params: url.Values{"database": {db}, "query": {"SELECT currentDatabase()"}}})
		if r.Body != db+"\n" {
			t.Errorf("database param: %q", r.Body)
		}
		r = doHTTP(t, req{Method: "GET", Params: url.Values{"database": {"e2e_nope_" + runID}, "query": {"SELECT 1"}}})
		if r.Header.Get("X-ClickHouse-Exception-Code") != "81" {
			t.Errorf("unknown database param: %d %q", r.Status, r.Body)
		}
		// Unknown settings as URL params are accepted (ClickHouse rejects truly
		// unknown ones, but common real settings must work).
		r = doHTTP(t, req{Method: "GET", Params: url.Values{"max_result_rows": {"1000"}, "output_format_json_quote_64bit_integers": {"0"},
			"wait_end_of_query": {"1"}, "send_progress_in_http_headers": {"0"}, "query": {"SELECT 7"}}})
		if r.Status != 200 || r.Body != "7\n" {
			t.Errorf("settings params: %d %q", r.Status, r.Body)
		}
	})

	t.Run("session", func(t *testing.T) {
		sid := "e2e-session-" + runID
		r := doHTTP(t, req{Params: url.Values{"session_id": {sid}}, Body: strings.NewReader("USE " + db)})
		if r.Status != 200 {
			t.Fatalf("USE in session: %d %s", r.Status, r.Body)
		}
		r = doHTTP(t, req{Params: url.Values{"session_id": {sid}}, Body: strings.NewReader("SELECT currentDatabase(), count() > 0 FROM http_metrics")})
		if r.Status != 200 || r.Body != db+"\t1\n" {
			t.Errorf("session keeps USE: %d %q", r.Status, r.Body)
		}
		r = doHTTP(t, req{Params: url.Values{"session_id": {sid}}, Body: strings.NewReader("SET max_result_rows = 1")})
		if r.Status != 200 {
			t.Fatalf("SET in session: %d %s", r.Status, r.Body)
		}
		r = doHTTP(t, req{Params: url.Values{"session_id": {sid}}, Body: strings.NewReader("SELECT number FROM numbers(5)")})
		if r.Header.Get("X-ClickHouse-Exception-Code") != "396" {
			t.Errorf("session keeps SET max_result_rows: %d %q", r.Status, r.Body)
		}
		// Another session (or none) is unaffected.
		if r := doHTTP(t, req{Body: strings.NewReader("SELECT count() FROM numbers(5)")}); r.Body != "5\n" {
			t.Errorf("sessionless query: %q", r.Body)
		}
	})

	t.Run("system_database", func(t *testing.T) {
		// Tools often connect with database=system.
		r := doHTTP(t, req{Method: "GET", Params: url.Values{"database": {"system"}, "query": {"SELECT currentDatabase()"}}})
		if r.Status != 200 || r.Body != "system\n" {
			t.Errorf("database=system: %d %q", r.Status, r.Body)
		}
		r = doHTTP(t, req{Method: "GET", Params: url.Values{"database": {"system"}, "query": {"SELECT count() > 0 FROM databases"}}})
		if r.Status != 200 || r.Body != "1\n" {
			t.Errorf("database=system unqualified system table: %d %q", r.Status, r.Body)
		}
	})

	t.Run("errors", func(t *testing.T) {
		cases := []struct {
			q    string
			code string
			name string
		}{
			{"SELECT * FROM no_such_table", "60", "UNKNOWN_TABLE"},
			{"SELECT FROM WHERE", "62", "SYNTAX_ERROR"},
			{"SELECT * FROM e2e_nodb_" + runID + ".t", "81", "UNKNOWN_DATABASE"},
			{"SELECT nope FROM http_metrics", "47", "UNKNOWN_IDENTIFIER"},
			{"SELECT 1 FORMAT NoSuchFormat", "73", "UNKNOWN_FORMAT"},
			{"SELECT sum(count()) FROM numbers(1)", "184", "ILLEGAL_AGGREGATION"},
			{"SELECT intDiv(1, 0)", "153", "ILLEGAL_DIVISION"},
			{"SELECT (SELECT number FROM numbers(2))", "125", "INCORRECT_RESULT_OF_SCALAR_SUBQUERY"},
		}
		for _, c := range cases {
			r := post(t, db, c.q)
			if r.Status == 200 {
				t.Errorf("%q succeeded", c.q)
				continue
			}
			if r.Header.Get("X-ClickHouse-Exception-Code") != c.code {
				t.Errorf("%q: X-ClickHouse-Exception-Code = %q, want %s", c.q, r.Header.Get("X-ClickHouse-Exception-Code"), c.code)
			}
			m := errBodyRe.FindStringSubmatch(r.Body)
			if m == nil || m[1] != c.code || m[2] != c.name {
				t.Errorf("%q: error body %q does not match 'Code: %s. DB::Exception: ... (%s) (version ...)'", c.q, r.Body, c.code, c.name)
			}
		}
		if r := post(t, db, "SELECT * FROM no_such_table"); r.Status != 404 {
			t.Errorf("UNKNOWN_TABLE status = %d, want 404", r.Status)
		}
		if r := post(t, db, "SELEC 1"); r.Status != 400 {
			t.Errorf("SYNTAX_ERROR status = %d, want 400", r.Status)
		}
	})

	t.Run("auth", func(t *testing.T) {
		q := url.Values{"query": {"SELECT currentUser()"}}
		basic := "Basic " + base64.StdEncoding.EncodeToString([]byte("default:"))
		for name, rq := range map[string]req{
			"basic":   {Method: "GET", Params: q, Headers: map[string]string{"Authorization": basic}},
			"headers": {Method: "GET", Params: q, Headers: map[string]string{"X-ClickHouse-User": "default", "X-ClickHouse-Key": ""}},
			"params":  {Method: "GET", Params: url.Values{"query": q["query"], "user": {"default"}, "password": {""}}},
			"none":    {Method: "GET", Params: q},
		} {
			r := doHTTP(t, rq)
			if r.Status != 200 || r.Body != "default\n" {
				t.Errorf("auth %s: %d %q", name, r.Status, r.Body)
			}
		}
		bad := "Basic " + base64.StdEncoding.EncodeToString([]byte("default:wrong"))
		r := doHTTP(t, req{Method: "GET", Params: q, Headers: map[string]string{"Authorization": bad}})
		if r.Status != 401 {
			t.Errorf("wrong password: %d %q", r.Status, r.Body)
		}
		r = doHTTP(t, req{Method: "GET", Params: q, Headers: map[string]string{"X-ClickHouse-User": "nobody_" + runID}})
		if r.Status != 403 || r.Header.Get("X-ClickHouse-Exception-Code") != "516" {
			t.Errorf("unknown user: %d %q %q", r.Status, r.Header.Get("X-ClickHouse-Exception-Code"), r.Body)
		}
	})

	t.Run("ping", func(t *testing.T) {
		for _, p := range []string{"/ping", "/", "/replicas_status"} {
			r := doHTTP(t, req{Method: "GET", Path: p})
			if r.Status != 200 || r.Body != "Ok.\n" {
				t.Errorf("GET %s: %d %q", p, r.Status, r.Body)
			}
		}
		r := doHTTP(t, req{Method: "HEAD", Path: "/ping"})
		if r.Status != 200 {
			t.Errorf("HEAD /ping: %d", r.Status)
		}
	})
}
