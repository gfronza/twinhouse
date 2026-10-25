package format

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Golden text-format tests: testdata/text/<case>/<format>.txt holds what
// real ClickHouse 25.8 returns for the case's query in that format
// (statistics normalised). Regenerate with FORMAT_GOLDEN_CH set (see
// native_golden_test.go).

type textCase struct {
	name     string
	query    string // without FORMAT
	settings map[string]string
	formats  []string
	cols     []Column
	rows     [][]any
	stats    Stats
	tz       string // Options.Timezone (default UTC)
}

var allTextFormats = []string{
	"TabSeparated", "TabSeparatedRaw", "TabSeparatedWithNames", "TabSeparatedWithNamesAndTypes",
	"CSV", "CSVWithNames", "CSVWithNamesAndTypes",
	"JSON", "JSONCompact", "JSONStrings", "JSONCompactStrings",
	"JSONEachRow", "JSONCompactEachRow", "JSONStringsEachRow", "JSONCompactStringsEachRow",
	"Values", "PrettyCompact", "Pretty",
}

var jsonFormats = []string{"JSON", "JSONCompact", "JSONEachRow", "JSONCompactEachRow"}

func textCases() []textCase {
	mixedCols := []Column{
		col("n", "UInt64"), col("i", "Int64"), col("s", "String"), col("f", "Float64"), col("b", "Bool"),
		col("ns", "Nullable(String)"), col("a", "Array(Nullable(UInt8))"), col("t", "Tuple(UInt8, String)"),
		col("nt", "Tuple(x UInt8, y Array(String))"), col("m", "Map(String, Array(UInt8))"),
		col("d", "Date"), col("dt", "DateTime('UTC')"), col("dt64", "DateTime64(3, 'UTC')"),
		col("u", "UUID"), col("e", "Enum8('a' = 1, 'b' = 2)"), col("lc", "LowCardinality(String)"),
		col("fs", "FixedString(3)"),
	}
	mixedRows := [][]any{
		{uint64(0), int64(0), "tab\there \"q\" 'sq' back\\slash /", 1.5, true, nil, []any{uint64(1), nil},
			[]any{uint64(1), "x"}, []any{uint64(2), []any{"p", "q"}}, []any{[]any{"k", []any{uint64(1), uint64(2)}}},
			day(2024, 1, 2), utc(2024, 1, 2, 3, 4, 5, 0), utc(2024, 1, 2, 3, 4, 5, 123000000),
			"00112233-4455-6677-8899-aabbccddeeff", "a", "low", "ab\x00"},
		{uint64(18446744073709551615), int64(-9223372036854775808), "line\nbreak,comma", -0.25, false, "x", []any{},
			[]any{uint64(255), ""}, []any{uint64(0), []any{}}, []any{},
			day(1970, 1, 1), utc(1970, 1, 1, 0, 0, 0, 0), utc(2299, 12, 31, 23, 59, 59, 999000000),
			"ffffffff-ffff-ffff-ffff-ffffffffffff", "b", "", "abc"},
	}
	mixedQuery := `SELECT * FROM (SELECT
		toUInt64(0) n, toInt64(0) i, 'tab\there "q" \'sq\' back\\slash /' s, 1.5 f, true b, CAST(NULL AS Nullable(String)) ns,
		CAST([1, NULL] AS Array(Nullable(UInt8))) a, CAST((1, 'x') AS Tuple(UInt8, String)) t,
		CAST((2, ['p', 'q']) AS Tuple(x UInt8, y Array(String))) nt, CAST(map('k', [1, 2]) AS Map(String, Array(UInt8))) m,
		toDate('2024-01-02') d, toDateTime('2024-01-02 03:04:05', 'UTC') dt, toDateTime64('2024-01-02 03:04:05.123', 3, 'UTC') dt64,
		toUUID('00112233-4455-6677-8899-aabbccddeeff') u, CAST('a' AS Enum8('a' = 1, 'b' = 2)) e, toLowCardinality('low') lc,
		toFixedString('ab', 3) fs
	UNION ALL SELECT
		toUInt64(18446744073709551615), toInt64(-9223372036854775808), 'line\nbreak,comma', -0.25, false, 'x',
		[], (255, ''), CAST((0, []) AS Tuple(x UInt8, y Array(String))), map(),
		toDate('1970-01-01'), toDateTime(0, 'UTC'), toDateTime64('2299-12-31 23:59:59.999', 3, 'UTC'),
		toUUID('ffffffff-ffff-ffff-ffff-ffffffffffff'), CAST('b' AS Enum8('a' = 1, 'b' = 2)), toLowCardinality(''),
		toFixedString('abc', 3)) ORDER BY n`

	floatsCols := []Column{col("a", "Float64"), col("b", "Float64"), col("c", "Float64"), col("d", "Float32"), col("e", "Float64"), col("f", "Float64")}
	floatsRows := [][]any{{chNaN, math.Inf(1), math.Inf(-1), 0.1, 1e100, 0.30000000000000004}}
	floatsQuery := `SELECT nan a, inf b, -inf c, toFloat32(0.1) d, 1e100 e, 0.1 + 0.2 f`

	numbersCols := []Column{col("number", "UInt64")}
	numbersRows := [][]any{{uint64(0)}, {uint64(1)}, {uint64(2)}}

	return []textCase{
		{name: "mixed", query: mixedQuery, formats: allTextFormats, cols: mixedCols, rows: mixedRows,
			stats: Stats{RowsRead: 2, BytesRead: 2}},
		{name: "mixed_quote64", query: mixedQuery, formats: jsonFormats, cols: mixedCols, rows: mixedRows,
			settings: map[string]string{"output_format_json_quote_64bit_integers": "1"}},
		{name: "mixed_compact_json", query: mixedQuery, formats: []string{"JSON"}, cols: mixedCols, rows: mixedRows,
			settings: map[string]string{"output_format_json_pretty_print": "0", "output_format_write_statistics": "0",
				"output_format_json_named_tuples_as_objects": "0"}},
		{name: "mixed_semicolon", query: mixedQuery, formats: []string{"CSV", "CSVWithNames"}, cols: mixedCols, rows: mixedRows,
			settings: map[string]string{"format_csv_delimiter": ";"}},
		{name: "floats", query: floatsQuery, formats: allTextFormats, cols: floatsCols, rows: floatsRows},
		{name: "floats_denormals", query: floatsQuery, formats: jsonFormats, cols: floatsCols, rows: floatsRows,
			settings: map[string]string{"output_format_json_quote_denormals": "1", "output_format_json_quote_64bit_floats": "1"}},
		{name: "empty", query: `SELECT toUInt8(1) a, 'x' b WHERE 0`, formats: allTextFormats,
			cols: []Column{col("a", "UInt8"), col("b", "String")}},
		{name: "limit", query: `SELECT number FROM numbers(10) LIMIT 3`, formats: []string{"JSON", "JSONCompact"},
			cols: numbersCols, rows: numbersRows, stats: Stats{HasRowsBeforeLimit: true, RowsBeforeLimit: 3}},
		{name: "array_of_rows", query: `SELECT number FROM numbers(3)`, formats: []string{"JSONEachRow"},
			cols: numbersCols, rows: numbersRows, settings: map[string]string{"output_format_json_array_of_rows": "1"}},
		{name: "timezone", query: `SELECT toDateTime('2024-01-02 03:04:05', 'UTC') AS a, CAST(a AS DateTime) b, CAST(a AS DateTime64(3)) c, [b] d`,
			formats: []string{"TSVWithNamesAndTypes", "CSV", "JSONEachRow", "Values"}, tz: "Asia/Tokyo",
			settings: map[string]string{"session_timezone": "Asia/Tokyo"},
			cols:     []Column{col("a", "DateTime('UTC')"), col("b", "DateTime"), col("c", "DateTime64(3)"), col("d", "Array(DateTime)")},
			rows:     [][]any{{utc(2024, 1, 2, 3, 4, 5, 0), utc(2024, 1, 2, 3, 4, 5, 0), utc(2024, 1, 2, 3, 4, 5, 0), []any{utc(2024, 1, 2, 3, 4, 5, 0)}}}},
		{name: "pretty_multiline", query: `SELECT 1 a, 'x\ny' b, 1000000000 big, 'é' u`, formats: []string{"PrettyCompact", "Pretty"},
			cols: []Column{col("a", "UInt8"), col("b", "String"), col("big", "UInt32"), col("u", "String")},
			rows: [][]any{{uint64(1), "x\ny", uint64(1000000000), "é"}}},
		{name: "pretty_many", query: `SELECT number FROM numbers(1005)`, formats: []string{"PrettyCompact"},
			cols: numbersCols, rows: seqRows(1005)},
		{name: "pretty_footer", query: `SELECT number, toString(number) s FROM numbers(60)`, formats: []string{"PrettyCompact", "Pretty"},
			cols: []Column{col("number", "UInt64"), col("s", "String")}, rows: seqStrRows(60)},
	}
}

func seqRows(n int) [][]any {
	out := make([][]any, n)
	for i := range out {
		out[i] = []any{uint64(i)}
	}
	return out
}

func seqStrRows(n int) [][]any {
	out := make([][]any, n)
	for i := range out {
		out[i] = []any{uint64(i), fmt.Sprint(i)}
	}
	return out
}

var statsRe = regexp.MustCompile(`("elapsed": )[0-9.e+-]+|("rows_read": )[0-9]+|("bytes_read": )[0-9]+`)

func normalizeStats(s string) string {
	return statsRe.ReplaceAllStringFunc(s, func(m string) string {
		return m[:strings.Index(m, ": ")+2] + "0"
	})
}

func fetchText(t *testing.T, base string, c textCase, format string) string {
	t.Helper()
	q := url.Values{}
	for k, v := range c.settings {
		q.Set(k, v)
	}
	resp, err := http.Post(base+"/?"+q.Encode(), "text/plain", strings.NewReader(c.query+" FORMAT "+format))
	if err != nil {
		t.Fatalf("query ClickHouse: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ClickHouse %d: %s", resp.StatusCode, body)
	}
	return normalizeStats(string(body))
}

func TestTextGolden(t *testing.T) {
	for _, c := range textCases() {
		for _, f := range c.formats {
			t.Run(c.name+"/"+f, func(t *testing.T) {
				path := filepath.Join("testdata", "text", c.name, f+".txt")
				var want string
				if base := os.Getenv(goldenEnv); base != "" {
					want = fetchText(t, base, c, f)
					if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, []byte(want), 0o644); err != nil {
						t.Fatal(err)
					}
				} else {
					raw, err := os.ReadFile(path)
					if err != nil {
						t.Fatalf("missing golden file %s: %v", path, err)
					}
					want = string(raw)
				}
				var buf bytes.Buffer
				tz := c.tz
				if tz == "" {
					tz = "UTC"
				}
				enc, err := NewEncoder(f, &buf, c.cols, Options{Settings: c.settings, Timezone: tz})
				if err != nil {
					t.Fatal(err)
				}
				if len(c.rows) > 0 {
					// Split into two blocks to exercise streaming.
					half := len(c.rows) / 2
					if err := enc.WriteRows(c.rows[:half]); err != nil {
						t.Fatal(err)
					}
					if err := enc.WriteRows(c.rows[half:]); err != nil {
						t.Fatal(err)
					}
				}
				if err := enc.Finish(c.stats); err != nil {
					t.Fatal(err)
				}
				got := normalizeStats(buf.String())
				if got != want {
					t.Fatalf("%s output differs from ClickHouse\n--- got ---\n%s\n--- want ---\n%s", f, got, want)
				}
			})
		}
	}
}
