//go:build compat

package compat

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"
	"testing"
)

// TestMaxResultRowsCountsBlocks pins max_result_rows on whole blocks of
// max_block_size rows: break returns the block that reaches the limit,
// throw fails the block that passes it. Under a filter a block holds the
// rows of one source block that pass.
func TestMaxResultRowsCountsBlocks(t *testing.T) {
	ref, our := requireTargets(t)
	ctx := context.Background()
	o := reqOpts{settings: []kv{{"max_block_size", "10"}}}
	for _, c := range []struct {
		filter, settings string
		rows             int
		msg              string
	}{
		{settings: "max_result_rows = 5, result_overflow_mode = 'break'", rows: 10},
		{settings: "max_result_rows = 10, result_overflow_mode = 'break'", rows: 10},
		{settings: "max_result_rows = 11, result_overflow_mode = 'break'", rows: 20},
		{settings: "max_result_rows = 5", msg: "Limit for result exceeded, max rows: 5.00, current rows: 10.00. (TOO_MANY_ROWS_OR_BYTES)"},
		// A filtered block is its source block's rows that pass.
		{filter: "number % 2 = 0", settings: "max_result_rows = 4, result_overflow_mode = 'break'", rows: 5},
		{filter: "number % 2 = 0", settings: "max_result_rows = 6, result_overflow_mode = 'break'", rows: 10},
		{filter: "number % 7 = 0", settings: "max_result_rows = 4, result_overflow_mode = 'break'", rows: 5},
		{filter: "number % 7 = 0", settings: "max_result_rows = 6, result_overflow_mode = 'break'", rows: 6},
		{filter: "number % 13 = 0", settings: "max_result_rows = 2, result_overflow_mode = 'break'", rows: 2},
	} {
		sql := "SELECT number FROM numbers(1000) SETTINGS " + c.settings
		if c.filter != "" {
			sql = "SELECT number FROM numbers(1000) WHERE " + c.filter + " SETTINGS max_threads = 1, " + c.settings
		}
		for _, tg := range []*target{ref, our} {
			r := tg.do(ctx, sql, o)
			switch {
			case c.msg != "" && (r.code != 396 || r.status != 500 || exceptionMessage(r) != c.msg):
				t.Errorf("%s: %s: %s", tg.name, sql, r.describe())
			case c.msg == "" && (r.failed() || bytes.Count(r.body, []byte("\n")) != c.rows):
				t.Errorf("%s: %s: want %d rows, got %s", tg.name, sql, c.rows, r.describe())
			}
		}
	}
}

// failsMidStream fails at row 150000, past the reference's first blocks,
// so both servers have sent their headers.
const failsMidStream = "SELECT toString(number) AS s, repeat('x', 20) AS p, intDiv(1, toInt64(number) - 150000) AS d FROM numbers(160000) SETTINGS max_threads = 1"

var (
	tsvException  = regexp.MustCompile(`\n__exception__\r\nCode: 153\. DB::Exception: Division by zero[^\n]*\(ILLEGAL_DIVISION\) \(version [^\n]+\)\n$`)
	exceptionLine = regexp.MustCompile(`^\{"exception": "Code: 153\. DB::Exception: Division by zero[^\n]*\(ILLEGAL_DIVISION\) \(version [^\n]+\)"\}$`)
)

// TestErrorAfterCommitEndsTheBody pins how a committed response ends when
// the query fails: an early summary without result counts, then the
// exception after the rows (a trailer, or inside the JSON formats). The
// reference sends fewer rows first (it drops its unsent buffer).
func TestErrorAfterCommitEndsTheBody(t *testing.T) {
	ref, our := requireTargets(t)
	ctx := context.Background()
	for _, tg := range []*target{ref, our} {
		// The transfer is cut off after the exception.
		r := tg.do(ctx, failsMidStream+" FORMAT TSV", reqOpts{})
		if r.status != 200 || r.code != 153 || !tsvException.Match(r.body) || !errors.Is(r.err, io.ErrUnexpectedEOF) {
			t.Errorf("%s TSV (%v): %s", tg.name, r.err, tail(r))
		}
		if !strings.Contains(r.summary, `"result_rows":"0","result_bytes":"0"`) {
			t.Errorf("%s: early summary %s", tg.name, r.summary)
		}

		r = tg.do(ctx, failsMidStream+" FORMAT JSONCompact", reqOpts{})
		var v struct {
			Data       [][]any        `json:"data"`
			Rows       int            `json:"rows"`
			Exception  string         `json:"exception"`
			Statistics map[string]any `json:"statistics"`
		}
		if err := json.Unmarshal(r.body, &v); err != nil || r.err != nil || r.status != 200 || v.Rows != len(v.Data) || v.Rows == 0 ||
			!strings.HasPrefix(v.Exception, "Code: 153. DB::Exception: Division by zero") || v.Statistics != nil {
			t.Errorf("%s JSONCompact (%v): %s", tg.name, err, tail(r))
		}

		r = tg.do(ctx, failsMidStream+" FORMAT JSONEachRow", reqOpts{})
		lines := strings.Split(strings.TrimSuffix(string(r.body), "\n"), "\n")
		if r.status != 200 || len(lines) < 2 || !exceptionLine.MatchString(lines[len(lines)-1]) ||
			!strings.HasPrefix(lines[len(lines)-2], `{"s":"`) {
			t.Errorf("%s JSONEachRow: %s", tg.name, tail(r))
		}
	}
}

// With gzip the formats without an in-format exception are cut off with no
// exception text; JSON ends its stream with the exception. Both servers
// commit early: the reference without parallel formatting, twinhouse with
// a 1-byte buffer (the compressed result is under 1 MiB).
func TestErrorAfterCommitWithGzip(t *testing.T) {
	ref, our := requireTargets(t)
	ctx := context.Background()
	for _, tg := range []*target{ref, our} {
		gz := reqOpts{settings: []kv{{"enable_http_compression", "1"}, {"output_format_parallel_formatting", "0"}}, acceptGzip: true}
		if tg == our {
			gz.settings = append(gz.settings, kv{"http_response_buffer_size", "1"})
		}
		r := tg.do(ctx, failsMidStream+" FORMAT TSV", gz)
		text, err := gunzip(r.body)
		if r.status != 200 || !errors.Is(r.err, io.ErrUnexpectedEOF) || err == nil || bytes.Contains(text, []byte("__exception__")) {
			t.Errorf("%s TSV: HTTP %d, transfer %v, gzip %v", tg.name, r.status, r.err, err)
		}
		r = tg.do(ctx, failsMidStream+" FORMAT JSONEachRow", gz)
		text, err = gunzip(r.body)
		lines := strings.Split(strings.TrimSuffix(string(text), "\n"), "\n")
		if r.status != 200 || r.err != nil || err != nil || !exceptionLine.MatchString(lines[len(lines)-1]) {
			t.Errorf("%s JSONEachRow: HTTP %d, transfer %v, gzip %v", tg.name, r.status, r.err, err)
		}
	}
}

func gunzip(b []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(zr)
}

// A query error before the response is committed keeps the result's
// format: its Content-Type, and for JSON the result's meta.
func TestErrorBeforeCommitKeepsTheFormat(t *testing.T) {
	ref, our := requireTargets(t)
	ctx := context.Background()
	const sql = "SELECT number, intDiv(1, toInt64(number) - 5) AS d FROM numbers(10) FORMAT "
	o := reqOpts{settings: []kv{{"max_block_size", "3"}}}
	for _, tg := range []*target{ref, our} {
		// The reference commits a small result early in some runs.
		r := tg.do(ctx, sql+"CSV", o)
		if (tg == our || r.status == 500) && (r.status != 500 || r.code != 153 || r.ctype != "text/csv; charset=UTF-8; header=absent" || r.xformat != "CSV") {
			t.Errorf("%s CSV: %s, Content-Type %q, format %q", tg.name, r.describe(), r.ctype, r.xformat)
		}
		r = tg.do(ctx, sql+"JSON", o)
		var v struct {
			Meta []struct{ Name, Type string } `json:"meta"`
		}
		if err := json.Unmarshal(r.body, &v); r.status == 500 && (err != nil || len(v.Meta) != 2 || v.Meta[1].Name != "d" || v.Meta[1].Type != "Int8") {
			t.Errorf("%s JSON (%v): %s", tg.name, err, r.describe())
		}
	}
}

// http_response_buffer_size moves the commit point: a 1.9 MB result that
// then fails is an HTTP error with a 10 MB buffer.
func TestResponseBufferSizeMovesTheCommitPoint(t *testing.T) {
	ref, our := requireTargets(t)
	ctx := context.Background()
	o := reqOpts{settings: []kv{{"http_response_buffer_size", "10000000"}}}
	for _, tg := range []*target{ref, our} {
		if r := tg.do(ctx, "SELECT number, intDiv(1, toInt64(number) - 250000) FROM numbers(300000)", o); r.status != 500 || r.code != 153 {
			t.Errorf("%s: %s", tg.name, r.describe())
		}
	}
}

// Result limits and max_block_size set in the query's SETTINGS clause
// count the same blocks as when set in the URL.
func TestResultLimitsFromTheSettingsClause(t *testing.T) {
	ref, our := requireTargets(t)
	ctx := context.Background()
	for _, c := range []struct {
		settings string
		rows     int
		msg      string
	}{
		{settings: "max_block_size = 10, max_result_rows = 15, result_overflow_mode = 'break'", rows: 20},
		{settings: "max_block_size = 10, max_result_bytes = 100, result_overflow_mode = 'break'", rows: 20},
		{settings: "max_block_size = 10, max_result_rows = 100", msg: "Limit for result exceeded, max rows: 100.00, current rows: 110.00. (TOO_MANY_ROWS_OR_BYTES)"},
		{settings: "max_block_size = 10, max_result_bytes = 100", msg: "Limit for result exceeded, max bytes: 100.00 B, current bytes: 160.00 B. (TOO_MANY_ROWS_OR_BYTES)"},
	} {
		sql := "SELECT number FROM numbers(1000) SETTINGS " + c.settings
		for _, tg := range []*target{ref, our} {
			// The status of a limit error depends on when the reference
			// commits; the code and message do not.
			r := tg.do(ctx, sql, reqOpts{})
			switch {
			case c.msg != "" && (r.code != 396 || exceptionMessage(r) != c.msg):
				t.Errorf("%s: %s: %s", tg.name, sql, r.describe())
			case c.msg == "" && (r.failed() || bytes.Count(r.body, []byte("\n")) != c.rows):
				t.Errorf("%s: %s: want %d rows, got %s", tg.name, sql, c.rows, r.describe())
			}
		}
	}
	for _, tg := range []*target{ref, our} {
		if r := tg.do(ctx, "SELECT number FROM numbers(100) FORMAT Native SETTINGS max_block_size = 10", reqOpts{}); r.failed() || len(r.body) != 960 {
			t.Errorf("%s Native: %d bytes, %s", tg.name, len(r.body), r.describe())
		}
	}
}

var jsonRowThenException = regexp.MustCompile(`\}__exception__\r\nCode: 153\. DB::Exception: Division by zero[^\n]*\(ILLEGAL_DIVISION\) \(version [^\n]+\)\n$`)

// With http_write_exception_in_output_format = 0 (clickhouse-go's
// QueryFormat sets it) the JSON formats end like the others: the
// __exception__ trailer after the rows and a cut-off transfer; before the
// commit the error is plain text with the format's Content-Type.
func TestExceptionOutsideTheFormat(t *testing.T) {
	ref, our := requireTargets(t)
	ctx := context.Background()
	o := reqOpts{settings: []kv{{"http_write_exception_in_output_format", "0"}}}
	for _, tg := range []*target{ref, our} {
		r := tg.do(ctx, failsMidStream+" FORMAT JSONEachRow", o)
		if r.status != 200 || !tsvException.Match(r.body) || !errors.Is(r.err, io.ErrUnexpectedEOF) {
			t.Errorf("%s JSONEachRow (%v): %s", tg.name, r.err, tail(r))
		}
		r = tg.do(ctx, failsMidStream+" FORMAT JSON", o)
		if r.status != 200 || !jsonRowThenException.Match(r.body) || !errors.Is(r.err, io.ErrUnexpectedEOF) {
			t.Errorf("%s JSON (%v): %s", tg.name, r.err, tail(r))
		}
		before := reqOpts{settings: append([]kv{{"wait_end_of_query", "1"}}, o.settings...)}
		r = tg.do(ctx, "SELECT number, intDiv(1, toInt64(number) - 5) AS d FROM numbers(10) FORMAT JSON", before)
		if r.status != 500 || r.code != 153 || !bytes.HasPrefix(r.body, []byte("Code: 153.")) ||
			r.ctype != "application/json; charset=UTF-8" || r.xformat != "JSON" {
			t.Errorf("%s before the commit: %s, Content-Type %q, format %q", tg.name, r.describe(), r.ctype, r.xformat)
		}
	}
}

// An error in a constant expression is an analysis error: it comes before
// the output is set up (JSON has no meta, an unknown format loses). An
// error while a sort reads its input is an execution error: the format is
// checked first and JSON has the result's meta.
func TestErrorsBeforeAndAfterTheOutputIsSetUp(t *testing.T) {
	ref, our := requireTargets(t)
	ctx := context.Background()
	const sorted = "SELECT intDiv(1, toInt64(number) - 5) AS x FROM numbers(10) ORDER BY number"
	for _, tg := range []*target{ref, our} {
		var v struct {
			Meta []struct{ Name string } `json:"meta"`
		}
		r := tg.do(ctx, "SELECT intDiv(1, 0) FORMAT JSONCompact", reqOpts{})
		if err := json.Unmarshal(r.body, &v); err != nil || r.code != 153 || len(v.Meta) != 0 {
			t.Errorf("%s constant (%v): %s", tg.name, err, r.describe())
		}
		if r = tg.do(ctx, "SELECT toUInt8('abc') FROM numbers(3) FORMAT NoSuchFormat", reqOpts{}); r.code != 6 {
			t.Errorf("%s constant, unknown format: %s", tg.name, r.describe())
		}
		r = tg.do(ctx, sorted+" FORMAT JSONCompact", reqOpts{})
		if err := json.Unmarshal(r.body, &v); err != nil || r.code != 153 || len(v.Meta) != 1 || v.Meta[0].Name != "x" {
			t.Errorf("%s sort (%v): %s", tg.name, err, r.describe())
		}
		if r = tg.do(ctx, sorted+" FORMAT NoSuchFormat", reqOpts{}); r.code != 73 {
			t.Errorf("%s sort, unknown format: %s", tg.name, r.describe())
		}
	}
}

// output_format_pretty_max_rows = 0 shows no rows, only the count.
func TestPrettyMaxRowsZero(t *testing.T) {
	ref, our := requireTargets(t)
	ctx := context.Background()
	for _, tg := range []*target{ref, our} {
		r := tg.do(ctx, "SELECT number FROM numbers(5) FORMAT PrettyCompact SETTINGS output_format_pretty_max_rows = 0", reqOpts{})
		if r.failed() || string(r.body) != "Showed 0 out of 5 rows.\n" {
			t.Errorf("%s: %s", tg.name, r.describe())
		}
	}
}

// With timeout_overflow_mode = 'break' a SELECT that reaches
// max_execution_time ends normally with the rows read so far.
func TestTimeoutBreakEndsTheResult(t *testing.T) {
	ref, our := requireTargets(t)
	ctx := context.Background()
	const sql = "SELECT number FROM system.numbers WHERE number % 1000000 = 0 SETTINGS max_execution_time = 0.3, timeout_overflow_mode = 'break'"
	for _, tg := range []*target{ref, our} {
		if r := tg.do(ctx, sql, reqOpts{}); r.failed() || !bytes.HasPrefix(r.body, []byte("0\n")) {
			t.Errorf("%s: %s", tg.name, r.describe())
		}
	}
}

// tail describes r by the end of its body.
func tail(r httpResult) string {
	b := r.body[max(0, len(r.body)-400):]
	r.body = nil
	return r.describe() + " ...tail " + string(b)
}

// The branch of if (or ?:) that a constant UInt8, Bool or NULL condition
// rules out is never evaluated, so its constant errors are not raised;
// other condition types, multiIf, CASE, OR, ifNull and coalesce raise
// them. materialize() makes its argument a
// per-row value, so no error is raised without rows.
func TestConstantConditionSkipsTheOtherBranch(t *testing.T) {
	ref, our := requireTargets(t)
	ctx := context.Background()
	for _, tg := range []*target{ref, our} {
		for _, c := range []struct{ sql, want string }{
			{"SELECT if(1, 1, intDiv(1, 0))", "1\n"},
			{"SELECT if(0, intDiv(1, 0), 1)", "1\n"},
			{"SELECT if(1, 1, toUInt8('abc'))", "1\n"},
			{"SELECT 1 ? 2 : intDiv(1, 0)", "2\n"},
			{"SELECT if(NULL, intDiv(1, 0), 1)", "1\n"},
			{"WITH 0 AS z SELECT if(z, intDiv(1, 0), 1)", "1\n"},
			{"SELECT if(0, if(1, intDiv(1, 0), 2), 3)", "3\n"},
			{"SELECT number FROM numbers(3) WHERE if(0, intDiv(1, 0), 1) = 1", "0\n1\n2\n"},
			{"SELECT intDiv(materialize(1), 0) FROM numbers(3) WHERE 0", ""},
			{"SELECT if(true, 1, intDiv(1, 0))", "1\n"},
			{"SELECT if(toNullable(1), 1, intDiv(1, 0))", "1\n"},
			{"SELECT if(toLowCardinality(toUInt8(1)), 1, intDiv(1, 0))", "1\n"},
		} {
			if r := tg.do(ctx, c.sql, reqOpts{}); r.failed() || string(r.body) != c.want {
				t.Errorf("%s %s: %s", tg.name, c.sql, r.describe())
			}
		}
		for _, c := range []struct {
			sql  string
			code int
		}{
			{"SELECT multiIf(1, 1, intDiv(1, 0))", 153},
			{"SELECT CASE WHEN 0 THEN toUInt8('abc') ELSE 1 END", 6},
			{"SELECT 1 OR intDiv(1, 0) = 1", 153},
			{"SELECT ifNull(1, intDiv(1, 0))", 153},
			{"SELECT coalesce(1, intDiv(1, 0))", 153},
			{"SELECT materialize(intDiv(1, 0)) FROM numbers(0)", 153},
			{"SELECT if(toInt8(1), 1, intDiv(1, 0))", 153},
			{"SELECT if(toUInt16(1), 1, intDiv(1, 0))", 153},
			{"SELECT if(-1, 1, intDiv(1, 0))", 153},
			{"SELECT if(1.5, 1, intDiv(1, 0))", 153},
			{"SELECT if(toNullable(toInt8(1)), 1, intDiv(1, 0))", 153},
		} {
			if r := tg.do(ctx, c.sql, reqOpts{}); r.code != c.code {
				t.Errorf("%s %s: %s", tg.name, c.sql, r.describe())
			}
		}
	}
}

// With result_overflow_mode = 'break' ending the result early,
// rows_before_limit_at_least is still the whole input of a sort or
// GROUP BY.
func TestRowsBeforeLimitWithBreak(t *testing.T) {
	ref, our := requireTargets(t)
	ctx := context.Background()
	o := reqOpts{settings: []kv{{"max_result_rows", "5"}, {"result_overflow_mode", "break"}, {"max_block_size", "10"}}}
	for _, tg := range []*target{ref, our} {
		for _, c := range []struct {
			sql  string
			want int
		}{
			{"SELECT number FROM numbers(100) ORDER BY number LIMIT 50", 100},
			{"SELECT number % 50 AS k FROM numbers(1000) GROUP BY k LIMIT 40", 50},
			{"SELECT number % 50 AS k FROM numbers(1000) GROUP BY k ORDER BY k LIMIT 40", 50},
		} {
			r := tg.do(ctx, c.sql+" FORMAT JSONCompact", o)
			var v struct {
				Before int `json:"rows_before_limit_at_least"`
			}
			if err := json.Unmarshal(r.body, &v); err != nil || r.failed() || v.Before != c.want {
				t.Errorf("%s %s (%v): %s", tg.name, c.sql, err, r.describe())
			}
		}
	}
}

// buffer_size and http_wait_end_of_query set how much is buffered before
// the headers, so whether a late error is an HTTP error (a given 0 commits
// at the first block); the summary has result counts for a body sent at
// once when the client set its buffering or compression.
func TestResponseBufferParameters(t *testing.T) {
	ref, our := requireTargets(t)
	ctx := context.Background()
	const sql = "SELECT number, intDiv(10, toInt64(number) - 25) AS d FROM numbers(40) FORMAT TSV"
	base := []kv{{"max_block_size", "10"}, {"output_format_parallel_formatting", "0"}}
	for _, tg := range []*target{ref, our} {
		for _, c := range []struct {
			params []kv
			status int
		}{
			{[]kv{{"http_response_buffer_size", "16"}}, 200},
			{[]kv{{"http_response_buffer_size", "16"}, {"http_wait_end_of_query", "1"}}, 500},
			{[]kv{{"http_response_buffer_size", "16"}, {"buffer_size", "4000000"}}, 200},
			{[]kv{{"http_response_buffer_size", "4000000"}, {"buffer_size", "16"}}, 500},
			{[]kv{{"http_response_buffer_size", "0"}}, 200},
			{[]kv{{"buffer_size", "0"}}, 200},
			{[]kv{{"http_response_buffer_size", "0"}, {"buffer_size", "4000000"}}, 200},
			{[]kv{{"http_response_buffer_size", "+16"}}, 200},
			{[]kv{{"http_response_buffer_size", "+0"}}, 200},
			{[]kv{{"http_response_buffer_size", "+4000000"}}, 500},
			{[]kv{{"buffer_size", "+16"}}, 200},
			{[]kv{{"buffer_size", "+0"}}, 200},
			{[]kv{{"buffer_size", "+4000000"}}, 500},
		} {
			r := tg.do(ctx, sql, reqOpts{settings: append(append([]kv{}, base...), c.params...)})
			if r.status != c.status || r.code != 153 {
				t.Errorf("%s %v: %s", tg.name, c.params, r.describe())
			}
		}
		for _, c := range []struct {
			params []kv
			code   int
		}{
			{[]kv{{"buffer_size", "1K"}}, 27},
			{[]kv{{"buffer_size", "+"}}, 72},
			{[]kv{{"buffer_size", "++1"}}, 72},
			{[]kv{{"buffer_size", "+1K"}}, 27},
			{[]kv{{"http_response_buffer_size", "+"}}, 72},
			{[]kv{{"http_response_buffer_size", "++1"}}, 72},
			{[]kv{{"wait_end_of_query", "yes"}}, 27},
			{[]kv{{"http_wait_end_of_query", "2"}}, 467},
		} {
			if r := tg.do(ctx, "SELECT 1", reqOpts{settings: c.params}); r.code != c.code {
				t.Errorf("%s %v: %s", tg.name, c.params, r.describe())
			}
		}
		for _, c := range []struct {
			params []kv
			gzip   bool
			counts string
		}{
			{nil, false, `"result_rows":"0","result_bytes":"0"`},
			{[]kv{{"wait_end_of_query", "1"}}, false, `"result_rows":"3"`},
			{[]kv{{"http_wait_end_of_query", "1"}}, false, `"result_rows":"3"`},
			{[]kv{{"http_response_buffer_size", "1000"}}, false, `"result_rows":"3"`},
			{[]kv{{"buffer_size", "1000000"}}, false, `"result_rows":"3"`},
			{[]kv{{"http_response_buffer_size", "0"}}, false, `"result_rows":"0","result_bytes":"0"`},
			{[]kv{{"compress", "1"}}, false, `"result_rows":"3"`},
			{[]kv{{"enable_http_compression", "1"}}, true, `"result_rows":"3"`},
		} {
			r := tg.do(ctx, "SELECT number FROM numbers(3)", reqOpts{settings: c.params, acceptGzip: c.gzip})
			if r.status != 200 || !strings.Contains(r.summary, c.counts) {
				t.Errorf("%s %v: HTTP %d summary %s", tg.name, c.params, r.status, r.summary)
			}
		}
	}
}
