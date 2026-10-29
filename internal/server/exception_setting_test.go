package server

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/gfronza/twinhouse/internal/exec"
)

// With http_write_exception_in_output_format = 0 (URL or SETTINGS clause)
// an error after the commit ends every format with __exception__ and cuts
// the transfer off, as ClickHouse 25.8 does: the JSON formats stop after
// the last whole row (JSON's rows end without their separator there).
func TestExceptionOutsideTheFormatAfterCommit(t *testing.T) {
	fe, _ := streamExec(failingAfter(2))
	_, ts := newTestServer(t, fe, func(c *Config) { c.ResponseBufferSize = 1 })
	exc := "__exception__\r\n" + divisionText + "\n"
	for _, c := range []struct{ format, end string }{
		{"JSON", "\t\t{\n\t\t\t\"n\": 5\n\t\t}" + exc},
		{"JSONCompact", "\t\t[5]" + exc},
		{"JSONEachRow", "{\"n\":5}\n" + exc},
		{"JSONCompactEachRow", "[5]\n" + exc},
	} {
		for _, q := range []struct{ url, sql string }{
			{"/?http_write_exception_in_output_format=0&default_format=" + c.format, "SELECT n"},
			{"/?default_format=" + c.format, "SELECT n SETTINGS http_write_exception_in_output_format = 0"},
		} {
			r := post(t, ts.URL+q.url, q.sql)
			if r.Status != 200 || !r.Cut || !strings.HasSuffix(r.Body, c.end) || strings.Count(r.Body, "__exception__") != 1 {
				t.Errorf("%s %s: %d cut %v ...%q", c.format, q.sql, r.Status, r.Cut, r.Body[max(0, len(r.Body)-80):])
			}
		}
	}
}

// Before the commit the exception is plain text with the format's
// Content-Type and X-ClickHouse-Format, also for an error raised before
// the output is set up.
func TestExceptionOutsideTheFormatBeforeCommit(t *testing.T) {
	fe, _ := streamExec(failingAfter(2))
	_, ts := newTestServer(t, fe, nil)
	r := post(t, ts.URL+"/?http_write_exception_in_output_format=0&default_format=JSON", "SELECT n")
	if r.Status != 500 || r.Body != divisionText+"\n" || r.Header.Get("Content-Type") != "application/json; charset=UTF-8" ||
		r.Header.Get("X-ClickHouse-Format") != "JSON" {
		t.Errorf("stream: %d %v %q", r.Status, r.Header, r.Body)
	}
	planErr := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) { return nil, errDivision }}
	_, ts = newTestServer(t, planErr, nil)
	r = post(t, ts.URL+"/?http_write_exception_in_output_format=0", "SELECT intDiv(1, 0) FORMAT JSONEachRow")
	if r.Status != 500 || r.Body != divisionText+"\n" || r.Header.Get("Content-Type") != "application/x-ndjson; charset=UTF-8" ||
		r.Header.Get("X-ClickHouse-Format") != "JSONEachRow" {
		t.Errorf("planning: %d %v %q", r.Status, r.Header, r.Body)
	}
}

// clickhouse-go's QueryFormat sets http_write_exception_in_output_format
// to 0 and reports a query that fails after the commit as an error.
func TestQueryFormatReportsALateError(t *testing.T) {
	base := newEngineServer(t, 0, func(c *Config) { c.ResponseBufferSize = 1 })
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{strings.TrimPrefix(base, "http://")}, Protocol: clickhouse.HTTP})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{"max_block_size": 100}))
	for _, f := range []string{"JSONEachRow", "JSON", "TSV"} {
		rc, err := conn.QueryFormat(ctx, f, "SELECT number, intDiv(1, toInt64(number) - 2500) FROM numbers(5000)")
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		n, err := io.Copy(io.Discard, rc)
		rc.Close()
		var ex *clickhouse.Exception
		if !errors.As(err, &ex) || ex.Code != 153 {
			t.Errorf("%s: read %d bytes, err %v", f, n, err)
		}
	}
}

// Execution errors, those raised while the plan reads its input too (a
// sort, GROUP BY), come after the output is set up: an unknown format
// wins, and JSON has the result's meta and the rows read, as in
// ClickHouse 25.8 (which counts whole blocks read: 10 there).
func TestExecutionErrorsComeAfterTheOutput(t *testing.T) {
	base := newEngineServer(t, 0, nil)
	json := url.Values{"default_format": {"JSONCompact"}}
	for _, sql := range []string{
		"SELECT intDiv(1, toInt64(number) - 5) AS x FROM numbers(10) ORDER BY number",
		"SELECT intDiv(1, toInt64(number) - 5) AS x FROM numbers(10) GROUP BY x",
	} {
		r := query(t, base, sql, json)
		if r.Status != 500 || !strings.Contains(r.Body, "\"name\": \"x\"") || !strings.Contains(r.Header.Get("X-ClickHouse-Summary"), `"read_rows":"6"`) {
			t.Errorf("%s: %d %v %q", sql, r.Status, r.Header, r.Body)
		}
		wantException(t, sql, query(t, base, sql+" FORMAT NoSuchFormat", nil), "73", 404, "Unknown format NoSuchFormat")
	}
}

// An error in a constant expression is raised when the query is planned,
// before the output is set up, as ClickHouse raises it when it analyzes
// the query: JSON has an empty meta, and it wins over an unknown format.
func TestConstantErrorsComeBeforeTheOutput(t *testing.T) {
	base := newEngineServer(t, 0, nil)
	r := query(t, base, "SELECT intDiv(1, 0)", url.Values{"default_format": {"JSONCompact"}})
	if r.Status != 500 || !strings.HasPrefix(r.Body, "{\n\t\"meta\":\n\t[\n\n\t],") {
		t.Errorf("JSON: %d %q", r.Status, r.Body)
	}
	wantException(t, "format", query(t, base, "SELECT toUInt8('abc') FROM numbers(3) FORMAT NoSuchFormat", nil), "6", 400, "")
}
