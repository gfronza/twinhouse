package format

import (
	"errors"
	"io"
	"strings"
	"testing"

	"dynamodb-eventstream/internal/cherr"
)

type decodeCase struct {
	name     string
	format   string
	input    string
	settings map[string]string
	cols     []Column
	want     [][]any
	errCode  int // expected *cherr.Error code (0: success, -1: any error)
}

func runDecodeCases(t *testing.T, cases []decodeCase) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dec, err := NewDecoder(c.format, strings.NewReader(c.input), c.cols, Options{Settings: c.settings})
			var rows [][]any
			for err == nil {
				var r []any
				r, err = dec.Next()
				if err == nil {
					rows = append(rows, r)
				}
			}
			if errors.Is(err, io.EOF) {
				err = nil
			}
			if c.errCode != 0 {
				if err == nil {
					t.Fatalf("expected error code %d, got rows %#v", c.errCode, rows)
				}
				if c.errCode > 0 && !cherr.Is(err, c.errCode) {
					t.Fatalf("expected error code %d, got %v", c.errCode, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRows(t, rows, c.want)
		})
	}
}

func TestDecodeTSV(t *testing.T) {
	two := []Column{col("a", "UInt32"), col("s", "String")}
	nullable := []Column{col("a", "Nullable(String)"), col("b", "UInt8")}
	runDecodeCases(t, []decodeCase{
		{name: "basic", format: "TSV", input: "1\tx\n2\ty\n", cols: two,
			want: [][]any{{uint64(1), "x"}, {uint64(2), "y"}}},
		{name: "no trailing newline", format: "TabSeparated", input: "1\tx\n2\ty", cols: two,
			want: [][]any{{uint64(1), "x"}, {uint64(2), "y"}}},
		{name: "escapes", format: "TSV", input: "1\ta\\tb\\nc\\\\d\\'e\n", cols: two,
			want: [][]any{{uint64(1), "a\tb\nc\\d'e"}}},
		{name: "escaped literal newline", format: "TSV", input: "1\ta\\\nb\n", cols: two,
			want: [][]any{{uint64(1), "a\nb"}}},
		{name: "null", format: "TSV", input: "\\N\t\\N\n", cols: nullable,
			want: [][]any{{nil, Missing}}},
		{name: "null not as default", format: "TSV", input: "\\N\t\\N\n", cols: nullable,
			settings: map[string]string{"input_format_null_as_default": "0"}, errCode: cherr.TypeMismatch},
		{name: "empty number is zero", format: "TSV", input: "\t\n", cols: []Column{col("a", "UInt8"), col("s", "String")},
			want: [][]any{{uint64(0), ""}}},
		{name: "empty as default", format: "TSV", input: "\t\n", cols: two,
			settings: map[string]string{"input_format_tsv_empty_as_default": "1"}, want: [][]any{{Missing, Missing}}},
		{name: "wrong field count", format: "TSV", input: "1\n", cols: two, errCode: cherr.CannotParseText},
		// ClickHouse's row readers fail on the delimiter after the number
		// prefix: CANNOT_PARSE_INPUT_ASSERTION_FAILED (27), for ints and floats.
		{name: "bad number", format: "TSV", input: "x\ty\n", cols: two, errCode: cherr.CannotParseInputAssertionFailed},
		{name: "bad float", format: "TSV", input: "abc\n", cols: []Column{col("f", "Float64")}, errCode: cherr.CannotParseInputAssertionFailed},
		{name: "minus into unsigned", format: "TSV", input: "-1\tx\n", cols: two, errCode: cherr.CannotParseNumber},
		{name: "custom null representation", format: "TSV", input: "NULL\tNULL\n", cols: nullable,
			settings: map[string]string{"format_tsv_null_representation": "NULL"}, want: [][]any{{nil, Missing}}},
		{name: "raw keeps backslashes", format: "TSVRaw", input: "1\ta\\tb\n", cols: two,
			want: [][]any{{uint64(1), "a\\tb"}}},
		{name: "array and date", format: "TSV", input: "[1,2]\t2024-01-02\n",
			cols: []Column{col("a", "Array(UInt8)"), col("d", "Date")},
			want: [][]any{{[]any{uint64(1), uint64(2)}, day(2024, 1, 2)}}},
		{name: "with names reordered", format: "TSVWithNames", input: "s\ta\nx\t1\n", cols: two,
			want: [][]any{{uint64(1), "x"}}},
		{name: "with names subset", format: "TabSeparatedWithNames", input: "s\nx\n", cols: two,
			want: [][]any{{Missing, "x"}}},
		{name: "with names unknown skipped", format: "TSVWithNames", input: "zz\ta\nq\t5\n", cols: two,
			want: [][]any{{uint64(5), Missing}}},
		{name: "with names unknown error", format: "TSVWithNames", input: "zz\ta\nq\t5\n", cols: two,
			settings: map[string]string{"input_format_skip_unknown_fields": "0"}, errCode: cherr.IncorrectData},
		{name: "with names header only", format: "TSVWithNames", input: "a\ts\n", cols: two},
		{name: "empty input", format: "TSVWithNames", input: "", cols: two},
		{name: "with names and types converts", format: "TSVWithNamesAndTypes",
			input: "a\ts\nUInt8\tString\n7\tz\n", cols: two, want: [][]any{{uint64(7), "z"}}},
		{name: "names header ignored", format: "TSVWithNames", input: "x\ty\n1\tq\n", cols: two,
			settings: map[string]string{"input_format_with_names_use_header": "0"}, want: [][]any{{uint64(1), "q"}}},
	})
}

func TestDecodeCSV(t *testing.T) {
	two := []Column{col("a", "UInt32"), col("s", "String")}
	defaults := []Column{col("a", "Nullable(String)"), col("b", "UInt8"), col("c", "Nullable(UInt8)"), col("d", "String")}
	runDecodeCases(t, []decodeCase{
		{name: "basic", format: "CSV", input: "1,x\n2,\"y, \"\"z\"\"\"\n", cols: two,
			want: [][]any{{uint64(1), "x"}, {uint64(2), "y, \"z\""}}},
		{name: "crlf", format: "CSV", input: "1,x\r\n2,\"y\"\r\n", cols: two,
			want: [][]any{{uint64(1), "x"}, {uint64(2), "y"}}},
		{name: "newline in quotes", format: "CSV", input: "1,\"a\nb\"\n", cols: two,
			want: [][]any{{uint64(1), "a\nb"}}},
		{name: "backslash is literal", format: "CSV", input: "1,a\\tb\n", cols: two,
			want: [][]any{{uint64(1), "a\\tb"}}},
		{name: "quoted number", format: "CSV", input: "\"3\",x\n", cols: two,
			want: [][]any{{uint64(3), "x"}}},
		{name: "empty and null defaults", format: "CSV", input: ",,,\n\"\",,,\"\"\n\\N,\\N,\\N,\\N\n", cols: defaults,
			want: [][]any{
				{Missing, Missing, Missing, Missing},
				{"", Missing, Missing, ""},
				{nil, Missing, nil, Missing},
			}},
		{name: "delimiter", format: "CSV", input: "1;x\n", cols: two,
			settings: map[string]string{"format_csv_delimiter": ";"}, want: [][]any{{uint64(1), "x"}}},
		{name: "bad delimiter setting", format: "CSV", input: "", cols: two,
			settings: map[string]string{"format_csv_delimiter": ";;"}, errCode: cherr.BadArguments},
		{name: "array", format: "CSV", input: "\"[1,2]\",\"['a','b']\"\n",
			cols: []Column{col("a", "Array(UInt8)"), col("s", "Array(String)")},
			want: [][]any{{[]any{uint64(1), uint64(2)}, []any{"a", "b"}}}},
		{name: "with names", format: "CSVWithNames", input: "\"s\",\"a\"\nx,1\n", cols: two,
			want: [][]any{{uint64(1), "x"}}},
		{name: "with names and types", format: "CSVWithNamesAndTypes", input: "a,s\nUInt8,String\n1,x\n", cols: two,
			want: [][]any{{uint64(1), "x"}}},
		{name: "not a number", format: "CSV", input: "notanumber,x\n", cols: two, errCode: cherr.CannotParseInputAssertionFailed},
		{name: "quoted not a number", format: "CSV", input: "\"abc\",x\n", cols: two, errCode: cherr.CannotParseInputAssertionFailed},
		{name: "custom csv null", format: "CSV", input: "NULL\n", cols: []Column{col("n", "Nullable(UInt8)")},
			settings: map[string]string{"format_csv_null_representation": "NULL"}, want: [][]any{{nil}}},
		{name: "unterminated quote", format: "CSV", input: "1,\"x\n", cols: two, errCode: cherr.CannotParseText},
		{name: "garbage after quote", format: "CSV", input: "1,\"x\"y\n", cols: two, errCode: cherr.CannotParseText},
	})
}

func TestDecodeValues(t *testing.T) {
	cols := []Column{col("a", "UInt32"), col("s", "String"), col("arr", "Array(Tuple(UInt8, String))"), col("n", "Nullable(Int8)")}
	runDecodeCases(t, []decodeCase{
		{name: "basic", format: "Values", input: "(1, 'x', [(1, 'a')], -1), (2,'y)\\'z',[],NULL)", cols: cols,
			want: [][]any{
				{uint64(1), "x", []any{[]any{uint64(1), "a"}}, int64(-1)},
				{uint64(2), "y)'z", []any{}, nil},
			}},
		{name: "multiline and semicolon", format: "Values", input: "\n(1,\n 'x',\n [], 2)\n,\n(3, 'q', [], NULL);\n", cols: cols,
			want: [][]any{
				{uint64(1), "x", []any{}, int64(2)},
				{uint64(3), "q", []any{}, nil},
			}},
		{name: "no commas between rows", format: "Values", input: "(1,'x',[],1) (2,'y',[],2)", cols: cols,
			want: [][]any{
				{uint64(1), "x", []any{}, int64(1)},
				{uint64(2), "y", []any{}, int64(2)},
			}},
		{name: "default keyword", format: "Values", input: "(DEFAULT, 'x', [], default)", cols: cols,
			want: [][]any{{Missing, "x", []any{}, Missing}}},
		{name: "null into non-nullable", format: "Values", input: "(NULL, 'x', [], 1)", cols: cols,
			want: [][]any{{Missing, "x", []any{}, int64(1)}}},
		{name: "empty", format: "Values", input: "  \n", cols: cols},
		{name: "too few", format: "Values", input: "(1, 'x')", cols: cols, errCode: cherr.CannotParseText},
		{name: "too many", format: "Values", input: "(1, 'x', [], 1, 2)", cols: cols, errCode: cherr.CannotParseText},
		{name: "unterminated", format: "Values", input: "(1, 'x', [], 1", cols: cols, errCode: cherr.CannotParseText},
		{name: "not a tuple", format: "Values", input: "1, 2", cols: cols, errCode: cherr.CannotParseText},
		{name: "expression", format: "Values", input: "(now(), 'x', [], 1)", cols: cols, errCode: cherr.NotImplemented},
		{name: "data after semicolon", format: "Values", input: "(1,'x',[],1); (2,'y',[],2)", cols: cols, errCode: cherr.CannotParseText},
	})
}

func TestDecodeJSONEachRow(t *testing.T) {
	cols := []Column{col("id", "UInt64"), col("s", "String"), col("ts", "DateTime64(3, 'UTC')"),
		col("n", "Nullable(Int32)"), col("tags", "Array(String)"), col("d", "UInt8")}
	ts := utc(2024, 1, 2, 3, 4, 5, 123000000)
	runDecodeCases(t, []decodeCase{
		{name: "basic", format: "JSONEachRow",
			input: `{"id":1,"s":"a","ts":"2024-01-02 03:04:05.123","n":-5,"tags":["x","y"],"d":3}` + "\n" +
				`{"id":"2","s":"b","ts":1704164645.123,"n":null,"tags":[],"d":true}`,
			cols: cols, want: [][]any{
				{uint64(1), "a", ts, int64(-5), []any{"x", "y"}, uint64(3)},
				{uint64(2), "b", ts, nil, []any{}, uint64(1)},
			}},
		{name: "missing keys", format: "JSONEachRow", input: `{"s":"only"}`, cols: cols,
			want: [][]any{{Missing, "only", Missing, Missing, Missing, Missing}}},
		{name: "null into non-nullable is default", format: "JSONEachRow", input: `{"id":null,"s":null}`, cols: cols,
			want: [][]any{{Missing, Missing, Missing, Missing, Missing, Missing}}},
		{name: "number into string", format: "JSONEachRow", input: `{"s":12.5}`, cols: cols,
			want: [][]any{{Missing, "12.5", Missing, Missing, Missing, Missing}}},
		{name: "unknown skipped", format: "JSONEachRow", input: `{"id":1,"zz":{"a":[1,"}"]}}`, cols: cols,
			want: [][]any{{uint64(1), Missing, Missing, Missing, Missing, Missing}}},
		{name: "unknown error", format: "JSONEachRow", input: `{"id":1,"zz":2}`, cols: cols,
			settings: map[string]string{"input_format_skip_unknown_fields": "0"}, errCode: cherr.IncorrectData},
		{name: "array wrapped", format: "JSONEachRow", input: "[\n {\"id\":1},\n {\"id\":2}\n]\n", cols: cols,
			want: [][]any{
				{uint64(1), Missing, Missing, Missing, Missing, Missing},
				{uint64(2), Missing, Missing, Missing, Missing, Missing},
			}},
		{name: "comma separated", format: "JSONEachRow", input: `{"id":1},{"id":2}`, cols: cols,
			want: [][]any{
				{uint64(1), Missing, Missing, Missing, Missing, Missing},
				{uint64(2), Missing, Missing, Missing, Missing, Missing},
			}},
		{name: "escaped strings", format: "JSONEachRow", input: `{"s":"a\"}\\\u00e9\n"}`, cols: cols,
			want: [][]any{{Missing, "a\"}\\é\n", Missing, Missing, Missing, Missing}}},
		{name: "empty", format: "JSONEachRow", input: "\n\n", cols: cols},
		// ClickHouse silently wraps 300 into UInt8; we fail loudly instead.
		{name: "out of range", format: "JSONEachRow", input: `{"d":300}`, cols: cols, errCode: -1},
		// Structural errors: codes as ClickHouse 25.8 reports them.
		{name: "not an object", format: "JSONEachRow", input: `1`, cols: cols, errCode: cherr.CannotParseInputAssertionFailed},
		{name: "truncated", format: "JSONEachRow", input: `{"id":1`, cols: cols, errCode: cherr.CannotReadAllData},
		{name: "unterminated array", format: "JSONEachRow", input: `[{"id":1}`, cols: cols, errCode: cherr.CannotParseInputAssertionFailed},
		{name: "invalid json", format: "JSONEachRow", input: `{"id":}`, cols: cols, errCode: cherr.CannotParseInputAssertionFailed},
		{name: "bare word", format: "JSONEachRow", input: `{"id":abc}`, cols: cols, errCode: cherr.CannotParseInputAssertionFailed},
		{name: "number field not a number", format: "JSONEachRow", input: `{"id":"not a number"}`, cols: cols, errCode: cherr.CannotParseInputAssertionFailed},
	})
}

func TestDecodeJSONCompactEachRow(t *testing.T) {
	cols := []Column{col("id", "UInt64"), col("s", "Nullable(String)"), col("a", "Array(Array(UInt8))")}
	runDecodeCases(t, []decodeCase{
		{name: "basic", format: "JSONCompactEachRow", input: "[1, \"x\", [[1],[2,3]]]\n[2, null, []]\n", cols: cols,
			want: [][]any{
				{uint64(1), "x", []any{[]any{uint64(1)}, []any{uint64(2), uint64(3)}}},
				{uint64(2), nil, []any{}},
			}},
		{name: "wrong arity", format: "JSONCompactEachRow", input: "[1, \"x\"]\n", cols: cols, errCode: cherr.IncorrectData},
		{name: "not an array", format: "JSONCompactEachRow", input: "{\"id\":1}\n", cols: cols, errCode: cherr.CannotParseInputAssertionFailed},
	})
}

func TestDecoderErrors(t *testing.T) {
	cols := []Column{col("a", "UInt8")}
	if _, err := NewDecoder("NoSuchFormat", strings.NewReader(""), cols, Options{}); !cherr.Is(err, cherr.UnknownFormat) {
		t.Fatalf("unknown format: got %v", err)
	}
	for _, f := range []string{"Parquet", "RowBinary", "Avro", "TSKV"} {
		if _, err := NewDecoder(f, strings.NewReader(""), cols, Options{}); !cherr.Is(err, cherr.NotImplemented) {
			t.Fatalf("known unsupported format %s: got %v", f, err)
		}
	}
	if _, err := NewDecoder("JSONEachRow", strings.NewReader(""), cols, Options{Settings: map[string]string{"date_time_input_format": "bogus"}}); !cherr.Is(err, cherr.BadArguments) {
		t.Fatalf("bad date_time_input_format: got %v", err)
	}
	if _, err := NewDecoder("JSON", strings.NewReader(""), cols, Options{}); !cherr.Is(err, cherr.NotImplemented) {
		t.Fatalf("output-only format: got %v", err)
	}
	agg := []Column{col("st", "AggregateFunction(uniqExact, String)")}
	if _, err := NewDecoder("Native", strings.NewReader(""), agg, Options{}); !cherr.Is(err, cherr.NotImplemented) {
		t.Fatalf("aggregate input: got %v", err)
	}
}
