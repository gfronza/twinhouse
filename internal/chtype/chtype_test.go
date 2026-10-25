package chtype

import (
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"dynamodb-eventstream/internal/cherr"
)

func TestInferLiteral(t *testing.T) {
	// Expected types observed with SELECT toTypeName(<literal>) in ClickHouse 25.8.
	cases := []struct {
		v       any
		isTuple bool
		want    string
	}{
		{uint64(0), false, "UInt8"},
		{uint64(255), false, "UInt8"},
		{uint64(256), false, "UInt16"},
		{uint64(65536), false, "UInt32"},
		{uint64(4294967296), false, "UInt64"},
		{uint64(math.MaxUint64), false, "UInt64"},
		{int64(0), false, "UInt8"},
		{int64(-1), false, "Int8"},
		{int64(-128), false, "Int8"},
		{int64(-129), false, "Int16"},
		{int64(-32769), false, "Int32"},
		{int64(-2147483649), false, "Int64"},
		{int64(math.MinInt64), false, "Int64"},
		{1.5, false, "Float64"},
		{math.Inf(1), false, "Float64"},
		{"x", false, "String"},
		{true, false, "Bool"},
		{nil, false, "Nullable(Nothing)"},
		{[]any{uint64(1), uint64(2)}, false, "Array(UInt8)"},
		{[]any{uint64(1), int64(-1)}, false, "Array(Int16)"},
		{[]any{uint64(1), uint64(256)}, false, "Array(UInt16)"},
		{[]any{uint64(1), 1.5}, false, "Array(Float64)"},
		{[]any{nil, uint64(1)}, false, "Array(Nullable(UInt8))"},
		{[]any{}, false, "Array(Nothing)"},
		{[]any{[]any{}}, false, "Array(Array(Nothing))"},
		{[]any{[]any{}, []any{uint64(1)}}, false, "Array(Array(UInt8))"},
		{[]any{"a", nil}, false, "Array(Nullable(String))"},
		{[]any{nil}, false, "Array(Nullable(Nothing))"},
		{[]any{int64(-1), uint64(4294967296)}, false, "Array(Int64)"},
		{[]any{uint64(1), "a"}, false, "Invalid"}, // no supertype
		{[]any{uint64(1), "a"}, true, "Tuple(UInt8, String)"},
		{[]any{uint64(1), nil}, true, "Tuple(UInt8, Nullable(Nothing))"},
		{[]any{}, true, "Tuple()"},
		{7, false, "UInt8"},
		{-7, false, "Int8"},
	}
	for _, c := range cases {
		if got := InferLiteral(c.v, c.isTuple).String(); got != c.want {
			t.Errorf("InferLiteral(%#v, %v) = %s, want %s", c.v, c.isTuple, got, c.want)
		}
	}
}

func TestCompare(t *testing.T) {
	ts := func(s string) time.Time { tm, _ := time.Parse(time.RFC3339Nano, s); return tm }
	cases := []struct {
		a, b any
		want int
	}{
		{nil, nil, 0},
		{nil, uint64(0), -1},
		{uint64(0), nil, 1},
		{uint64(1), int64(1), 0},
		{uint64(1), 1.0, 0},
		{int64(-1), uint64(0), -1},
		{int64(-1), -1.0, 0},
		{int64(-2), -1.5, -1},
		{uint64(math.MaxUint64), int64(-1), 1},
		{uint64(math.MaxUint64), float64(math.MaxUint64), -1}, // float rounds up to 2^64
		{uint64(1 << 53), float64(1 << 53), 0},
		{uint64(1<<53 + 1), float64(1 << 53), 1},
		{int64(math.MinInt64), float64(math.MinInt64), 0},
		{uint64(5), math.Inf(1), -1},
		{uint64(5), math.Inf(-1), 1},
		{math.NaN(), math.NaN(), 0},
		{math.NaN(), math.Inf(1), 1},
		{uint64(5), math.NaN(), -1},
		{0.0, math.Copysign(0, -1), 0},
		{true, uint64(1), 0},
		{false, true, -1},
		{"a", "b", -1},
		{"b", "a", 1},
		{"", "", 0},
		{"\xff", "a", 1},
		{ts("2024-01-01T00:00:00Z"), ts("2024-01-01T00:00:00.000000001Z"), -1},
		{ts("2024-01-01T01:00:00+01:00"), ts("2024-01-01T00:00:00Z"), 0},
		{[]any{uint64(1), "a"}, []any{uint64(1), "b"}, -1},
		{[]any{uint64(1)}, []any{uint64(1), uint64(0)}, -1},
		{[]any{}, []any{}, 0},
		// Nested NULL sorts after every value (ClickHouse array/map
		// comparison): [1] < [1,2] < [1,nan] < [1,NULL] < [NULL].
		{[]any{nil}, []any{uint64(0)}, 1},
		{[]any{nil}, []any{nil}, 0},
		{[]any{uint64(1), nil}, []any{uint64(1), uint64(2)}, 1},
		{[]any{uint64(1), nil}, []any{uint64(1), math.NaN()}, 1},
		{[]any{uint64(1), math.NaN()}, []any{uint64(1), uint64(2)}, 1},
		{[]any{uint64(1)}, []any{uint64(1), nil}, -1},
		{[]any{nil}, []any{uint64(1), nil}, 1},
		{[]any{math.NaN()}, []any{nil}, -1},
		{[]any{[]any{nil}}, []any{[]any{uint64(1)}}, 1},
		{[]any{[]any{uint64(1), nil}}, []any{[]any{uint64(1), uint64(2)}}, 1}, // map {1:NULL} > {1:2}
		{uint64(1), "1", -1},                                                  // incomparable kinds order by rank
		{&AggPartial{}, &AggPartial{Count: 1}, 0},
		{7, int64(7), 0},
		{float32(1.5), 1.5, 0},
	}
	for _, c := range cases {
		if got := Compare(c.a, c.b); got != c.want {
			t.Errorf("Compare(%#v, %#v) = %d, want %d", c.a, c.b, got, c.want)
		}
		if got := Compare(c.b, c.a); got != -c.want {
			t.Errorf("Compare(%#v, %#v) = %d, want %d (antisymmetry)", c.b, c.a, got, -c.want)
		}
		if Equal(c.a, c.b) != (c.want == 0) {
			t.Errorf("Equal(%#v, %#v) inconsistent with Compare", c.a, c.b)
		}
	}
}

// TestCompareHint checks the IColumn::compareAt-style order: NULL and NaN at
// any depth go after all values for hint > 0 and before them for hint < 0,
// NULL beyond NaN. Expected orders are ClickHouse 25.8's ORDER BY results.
func TestCompareHint(t *testing.T) {
	n := math.NaN()
	vals := []any{
		[]any{uint64(1), nil}, []any{uint64(1), n}, []any{nil}, []any{uint64(1)},
		[]any{n}, []any{math.Inf(1)}, []any{uint64(1), uint64(2)}, []any{},
	}
	sorted := func(hint, dir int) string {
		vs := slices.Clone(vals)
		slices.SortStableFunc(vs, func(a, b any) int { return dir * CompareHint(a, b, hint) })
		parts := make([]string, len(vs))
		for i, v := range vs {
			parts[i] = FormatQuoted(Type{Kind: Array, Elem: &Type{Kind: Nullable, Elem: &TFloat64}}, v)
		}
		return strings.Join(parts, " ")
	}
	cases := []struct {
		name      string
		hint, dir int
		want      string
	}{
		{"ASC", 1, 1, "[] [1] [1,2] [1,nan] [1,NULL] [inf] [nan] [NULL]"},
		{"DESC", -1, -1, "[inf] [1,2] [1,nan] [1,NULL] [1] [nan] [NULL] []"},
		{"ASC NULLS FIRST", -1, 1, "[] [NULL] [nan] [1] [1,NULL] [1,nan] [1,2] [inf]"},
		{"DESC NULLS FIRST", 1, -1, "[NULL] [nan] [inf] [1,NULL] [1,nan] [1,2] [1] []"},
	}
	for _, c := range cases {
		if got := sorted(c.hint, c.dir); got != c.want {
			t.Errorf("ORDER BY x %s:\n got  %s\n want %s", c.name, got, c.want)
		}
	}
	// Top level follows the hint too (unlike Compare, where nil is first).
	for _, c := range []struct {
		a, b       any
		hint, want int
	}{
		{nil, uint64(1), 1, 1}, {nil, uint64(1), -1, -1}, {nil, nil, 1, 0},
		{n, uint64(1), 1, 1}, {n, uint64(1), -1, -1}, {n, n, -1, 0},
		{nil, n, 1, 1}, {nil, n, -1, -1}, {math.Inf(1), n, -1, 1},
		{int64(-1), uint64(1), -1, -1},
		// tuple (1,NULL) vs (1,2): max picks (1,2) with hint -1, min with +1.
		{[]any{uint64(1), nil}, []any{uint64(1), uint64(2)}, -1, -1},
		{[]any{uint64(1), nil}, []any{uint64(1), uint64(2)}, 1, 1},
	} {
		if got := CompareHint(c.a, c.b, c.hint); got != c.want {
			t.Errorf("CompareHint(%#v, %#v, %d) = %d, want %d", c.a, c.b, c.hint, got, c.want)
		}
		if got := CompareHint(c.b, c.a, c.hint); got != -c.want {
			t.Errorf("CompareHint(%#v, %#v, %d) = %d, want %d (antisymmetry)", c.b, c.a, c.hint, got, -c.want)
		}
	}
}

func TestZero(t *testing.T) {
	cases := []struct {
		typ  string
		want string // FormatQuoted of the zero value
	}{
		{"UInt8", "0"}, {"Int64", "0"}, {"Float32", "0"}, {"Bool", "false"},
		{"String", "''"}, {"FixedString(3)", `'\0\0\0'`},
		{"UUID", "'00000000-0000-0000-0000-000000000000'"},
		{"Date", "'1970-01-01'"}, {"Date32", "'1900-01-01'"},
		{"DateTime", "'1970-01-01 00:00:00'"}, {"DateTime64(3)", "'1970-01-01 00:00:00.000'"},
		{"Enum8('b' = 2, 'a' = 1)", "'a'"}, {"Enum8('b' = -2, 'a' = 1)", "'b'"},
		{"Nullable(String)", "NULL"}, {"LowCardinality(Nullable(String))", "NULL"},
		{"LowCardinality(String)", "''"},
		{"Array(String)", "[]"}, {"Map(String, UInt8)", "{}"},
		{"Tuple(UInt8, String, Array(UInt8))", "(0,'',[])"},
		{"SimpleAggregateFunction(sum, UInt64)", "0"},
		{"Nothing", "NULL"},
	}
	for _, c := range cases {
		typ := MustParse(c.typ)
		z := Zero(typ)
		checkCanonical(t, typ, z)
		if got := FormatQuoted(typ, z); got != c.want {
			t.Errorf("Zero(%s) = %s, want %s", c.typ, got, c.want)
		}
	}
	agg := Zero(MustParse("AggregateFunction(sumIf, Int32, UInt8)")).(*AggPartial)
	if agg.Func != "sum" || len(agg.ArgTypes) != 1 || agg.Sum != int64(0) || !agg.SetLoaded {
		t.Errorf("Zero(AggregateFunction(sumIf, Int32, UInt8)) = %+v", agg)
	}
	agg = Zero(MustParse("AggregateFunction(avg, Float64)")).(*AggPartial)
	if agg.Func != "avg" || agg.Sum != float64(0) {
		t.Errorf("Zero(avg) = %+v", agg)
	}
	agg = Zero(MustParse("AggregateFunction(uniqExact, String)")).(*AggPartial)
	if agg.Func != "uniqExact" || agg.Set != nil || !agg.SetLoaded {
		t.Errorf("Zero(uniqExact) = %+v", agg)
	}
}

func TestBaseAggFunc(t *testing.T) {
	for in, want := range map[string]string{
		"sum": "sum", "sumIf": "sum", "uniqExactIf": "uniqExact", "sumState": "sum",
		"sumMergeState": "sum", "countIf": "count", "argMax": "argMax", "SUM": "sum", "If": "If",
	} {
		if got := BaseAggFunc(in); got != want {
			t.Errorf("BaseAggFunc(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTypeHelpers(t *testing.T) {
	lcn := MustParse("LowCardinality(Nullable(String))")
	if !lcn.IsNullable() || !lcn.IsString() || lcn.Unwrap().Kind != String {
		t.Errorf("LowCardinality(Nullable(String)) helpers wrong: %v %v %v", lcn.IsNullable(), lcn.IsString(), lcn.Unwrap())
	}
	if MustParse("LowCardinality(String)").IsNullable() || MustParse("Array(Nullable(UInt8))").IsNullable() {
		t.Error("IsNullable true for non-nullable type")
	}
	saf := MustParse("SimpleAggregateFunction(sum, UInt64)")
	if !saf.IsNumeric() || !saf.IsInteger() || saf.IsSigned() || saf.Unwrap().Kind != UInt64 {
		t.Error("SimpleAggregateFunction classifiers wrong")
	}
	for s, want := range map[string][4]bool{ // numeric, integer, signed, time
		"Int8": {true, true, true, false}, "UInt64": {true, true, false, false},
		"Float32": {true, false, true, false}, "Bool": {false, false, false, false},
		"String": {false, false, false, false}, "Nullable(DateTime64(3))": {false, false, false, true},
		"Date32": {false, false, false, true}, "Enum8('a' = 1)": {false, false, false, false},
	} {
		typ := MustParse(s)
		got := [4]bool{typ.IsNumeric(), typ.IsInteger(), typ.IsSigned(), typ.IsTime()}
		if got != want {
			t.Errorf("%s classifiers = %v, want %v", s, got, want)
		}
	}
	eq := []struct {
		a, b string
		want bool
	}{
		{"Enum8('a' = 1, 'b' = 2)", "Enum8('b' = 2, 'a' = 1)", true},
		{"Enum8('a' = 1)", "Enum16('a' = 1)", false},
		{"DateTime", "DateTime('UTC')", false},
		{"DateTime64(3)", "DateTime64(6)", false},
		{"Tuple(a UInt8)", "Tuple(b UInt8)", false},
		{"Tuple(a UInt8)", "Tuple(UInt8)", false},
		{"Map(String, Array(UInt8))", "Map(String, Array(UInt8))", true},
		{"AggregateFunction(quantile(0.5), Float64)", "AggregateFunction(quantile(0.50), Float64)", true},
		{"AggregateFunction(quantile(0.5), Float64)", "AggregateFunction(quantile(0.9), Float64)", false},
		{"FixedString(2)", "FixedString(3)", false},
		{"INT", "Int32", true},
	}
	for _, c := range eq {
		if got := MustParse(c.a).Equal(MustParse(c.b)); got != c.want {
			t.Errorf("%s.Equal(%s) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

func TestParseTypeStructure(t *testing.T) {
	typ := MustParse("AggregateFunction(quantiles(0.5, 0.9), Float64)")
	if typ.AggFunc != "quantiles" || len(typ.AggParams) != 2 || typ.AggParams[0] != 0.5 || typ.AggArgs[0].Kind != Float64 {
		t.Errorf("parametric aggregate parsed as %+v", typ)
	}
	typ = MustParse("AggregateFunction(topK(10), String)")
	if typ.AggParams[0] != uint64(10) {
		t.Errorf("topK(10) param = %#v", typ.AggParams[0])
	}
	typ = MustParse("SimpleAggregateFunction(anyLast, Nullable(String))")
	if typ.AggFunc != "anyLast" || typ.Elem.String() != "Nullable(String)" || len(typ.AggArgs) != 0 {
		t.Errorf("SimpleAggregateFunction parsed as %+v", typ)
	}
	typ = MustParse("Tuple(qualification_id UInt32, precodes Array(String))")
	if len(typ.Fields) != 2 || typ.Fields[0].Name != "qualification_id" || typ.Fields[1].Type.String() != "Array(String)" {
		t.Errorf("named tuple parsed as %+v", typ)
	}
	typ = MustParse("Enum8('b' = 2, 'a' = 1)")
	if typ.EnumValues[0].Name != "b" || typ.String() != "Enum8('a' = 1, 'b' = 2)" {
		t.Errorf("enum keeps declaration order and prints sorted: %+v %s", typ.EnumValues, typ)
	}
	typ = MustParse("DateTime64(9, 'Asia/Kolkata')")
	if typ.Precision != 9 || typ.TZ != "Asia/Kolkata" {
		t.Errorf("DateTime64 parsed as %+v", typ)
	}
	if MustParse("Map(String, UInt64)").Key.Kind != String {
		t.Error("Map key not set")
	}
	for _, bad := range []string{"", "   ", "Array(String", "DateTime('utc')", "DateTime('Local')", "DateTime64(3, 'Mars/Olympus')"} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("Parse(%q) succeeded", bad)
		} else if !errors.As(err, new(*cherr.Error)) {
			t.Errorf("Parse(%q) error is not a *cherr.Error: %T", bad, err)
		}
	}
}

func TestPrettyString(t *testing.T) {
	// Expected strings from ClickHouse DESCRIBE (print_pretty_type_names=1).
	cases := map[string]string{
		"Tuple(a UInt32, b String)":                          "Tuple(\n    a UInt32,\n    b String)",
		"Tuple(UInt32, String)":                              "Tuple(UInt32, String)",
		"Tuple(a Array(Tuple(b UInt8)))":                     "Tuple(\n    a Array(Tuple(\n        b UInt8)))",
		"Tuple(Tuple(a UInt8), String)":                      "Tuple(Tuple(\n    a UInt8), String)",
		"Map(String, Tuple(a UInt8, b Tuple(c String)))":     "Map(String, Tuple(\n    a UInt8,\n    b Tuple(\n        c String)))",
		"Tuple(a Tuple(UInt8, Tuple(b UInt8)))":              "Tuple(\n    a Tuple(UInt8, Tuple(\n        b UInt8)))",
		"AggregateFunction(argMax, Tuple(a UInt8), UInt8)":   "AggregateFunction(argMax, Tuple(a UInt8), UInt8)",
		"Tuple(`a.b` UInt8, `1` UInt8, _x UInt8, `é` UInt8)": "Tuple(\n    `a.b` UInt8,\n    `1` UInt8,\n    _x UInt8,\n    `é` UInt8)",
		"LowCardinality(Nullable(String))":                   "LowCardinality(Nullable(String))",
	}
	for in, want := range cases {
		if got := MustParse(in).PrettyString(); got != want {
			t.Errorf("PrettyString(%s) =\n%s\nwant\n%s", in, got, want)
		}
	}
}

func TestTimezones(t *testing.T) {
	berlin := MustParse("DateTime('Europe/Berlin')")
	v, err := ParseText(berlin, "2024-07-01 12:00:00")
	if err != nil {
		t.Fatal(err)
	}
	tm := v.(time.Time)
	if tm.Location() != time.UTC || tm.Hour() != 10 {
		t.Errorf("Berlin wall clock 12:00 in July should be 10:00 UTC, got %v", tm)
	}
	if got := FormatText(berlin, v); got != "2024-07-01 12:00:00" {
		t.Errorf("FormatText = %q", got)
	}
	if got := FormatText(TDateTime, v); got != "2024-07-01 10:00:00" {
		t.Errorf("FormatText without TZ = %q", got)
	}
	// Unix timestamps are absolute regardless of the column timezone.
	v, _ = ParseText(MustParse("DateTime('Asia/Kolkata')"), "1136214245")
	if v.(time.Time).Unix() != 1136214245 {
		t.Errorf("timestamp shifted by timezone: %v", v)
	}
	// Date -> DateTime('Asia/Tokyo') is midnight Tokyo time.
	d, _ := ParseText(TDate, "2024-01-02")
	c, err := Convert(d, TDate, MustParse("DateTime('Asia/Tokyo')"))
	if err != nil || !c.(time.Time).Equal(time.Date(2024, 1, 1, 15, 0, 0, 0, time.UTC)) {
		t.Errorf("Date->DateTime(Tokyo) = %v, %v", c, err)
	}
}

func TestDateTime64NanosecondRoundTrip(t *testing.T) {
	typ := NewDateTime64(9, "")
	for _, s := range []string{
		"2006-01-02 15:04:05.000000000", "2006-01-02 15:04:05.123456789",
		"1900-01-01 00:00:00.000000001", "2262-04-11 23:47:16.854775807",
		"1969-12-31 23:59:59.999999999",
	} {
		v, err := ParseText(typ, s)
		if err != nil {
			t.Fatalf("ParseText(%q): %v", s, err)
		}
		if got := FormatText(typ, v); got != s {
			t.Errorf("round trip %q -> %q", s, got)
		}
		q, n, err := ParseQuoted(typ, FormatQuoted(typ, v))
		if err != nil || n != len(s)+2 || !q.(time.Time).Equal(v.(time.Time)) {
			t.Errorf("quoted round trip of %q: %v %d %v", s, q, n, err)
		}
	}
	if _, err := ParseText(typ, "2262-04-11 23:47:16.854775808"); err == nil {
		t.Error("expected overflow past the int64 nanosecond range")
	}
	// Precision truncates (never rounds).
	v, _ := ParseText(NewDateTime64(3, ""), "2006-01-02 15:04:05.9999")
	if got := v.(time.Time).Nanosecond(); got != 999000000 {
		t.Errorf("DateTime64(3) truncation: got %d ns", got)
	}
}

func TestParseQuotedStream(t *testing.T) {
	typ := MustParse("Array(Tuple(UInt32, Array(String)))")
	in := "  [(12, ['1','3'])], (next row)"
	v, n, err := ParseQuoted(typ, in)
	if err != nil {
		t.Fatal(err)
	}
	if in[n:] != ", (next row)" {
		t.Errorf("consumed %d bytes, rest %q", n, in[n:])
	}
	if got := FormatQuoted(typ, v); got != "[(12,['1','3'])]" {
		t.Errorf("value %s", got)
	}
	for _, c := range []struct{ typ, in, rest string }{
		{"String", "'a''b' x", " x"},
		{"UInt8", "42)", ")"},
		{"Nullable(Int8)", "NULL,", ","},
		{"Float64", "-1.5e3,", ","},
		{"DateTime64(3)", "1136214245123,", ","},
		{"Map(String, UInt8)", "{'a':1})", ")"},
	} {
		_, n, err := ParseQuoted(MustParse(c.typ), c.in)
		if err != nil || c.in[n:] != c.rest {
			t.Errorf("ParseQuoted(%s, %q): n=%d err=%v", c.typ, c.in, n, err)
		}
	}
	for _, c := range []struct{ typ, in string }{
		{"String", "abc"}, {"UInt8", "x"}, {"Array(UInt8)", "[1,2"}, {"Tuple(UInt8, UInt8)", "(1)"},
		{"UUID", "'nope'"}, {"Date", "2006-01-02"}, {"AggregateFunction(count)", "1"},
	} {
		if _, n, err := ParseQuoted(MustParse(c.typ), c.in); err == nil && n == len(c.in) {
			t.Errorf("ParseQuoted(%s, %q) succeeded", c.typ, c.in)
		}
	}
}

type myStringer struct{}

func TestNormalize(t *testing.T) {
	berlin := time.FixedZone("CET", 3600)
	u := uuid.MustParse("61f0c404-5cb3-11e7-907b-a6006ad3dba0")
	str := "p"
	cases := []struct {
		typ  string
		in   any
		want string // FormatQuoted of the result
	}{
		{"UInt8", 7, "7"},
		{"UInt8", int32(255), "255"},
		{"UInt64", uint64(math.MaxUint64), "18446744073709551615"},
		{"Int8", int8(-128), "-128"},
		{"Int64", 3.0, "3"},
		{"UInt16", json.Number("65535"), "65535"},
		{"UInt8", "300", "44"}, // strings follow CAST rules
		{"UInt8", true, "1"},
		{"Float32", 0.1, "0.1"},
		{"Float32", float32(0.1), "0.1"},
		{"Float64", 3, "3"},
		{"Float64", json.Number("1e3"), "1000"},
		{"Float64", "inf", "inf"},
		{"Bool", true, "true"},
		{"Bool", 1, "true"},
		{"Bool", "false", "false"},
		{"String", "x", "'x'"},
		{"String", []byte("b\x00"), `'b\0'`},
		{"String", &str, "'p'"},
		{"FixedString(3)", "ab", `'ab\0'`},
		{"UUID", "61F0C4045CB311E7907BA6006AD3DBA0", "'61f0c404-5cb3-11e7-907b-a6006ad3dba0'"},
		{"UUID", u, "'61f0c404-5cb3-11e7-907b-a6006ad3dba0'"},
		{"UUID", [16]byte(u), "'61f0c404-5cb3-11e7-907b-a6006ad3dba0'"},
		{"Enum8('a' = 1, 'b' = 2)", "b", "'b'"},
		{"Enum8('a' = 1, 'b' = 2)", 1, "'a'"},
		{"Date", time.Date(2024, 3, 1, 23, 30, 0, 0, berlin), "'2024-03-01'"},
		{"Date", "2024-03-01", "'2024-03-01'"},
		{"Date", 19000, "'2022-01-08'"},
		{"DateTime", time.Date(2024, 3, 1, 23, 30, 5, 999, berlin), "'2024-03-01 22:30:05'"},
		{"DateTime", 1136214245, "'2006-01-02 15:04:05'"},
		{"DateTime", "2006-01-02 15:04:05", "'2006-01-02 15:04:05'"},
		{"DateTime64(9)", time.Date(2024, 3, 1, 0, 0, 0, 123456789, time.UTC), "'2024-03-01 00:00:00.123456789'"},
		{"DateTime64(3)", time.Date(2024, 3, 1, 0, 0, 0, 123456789, time.UTC), "'2024-03-01 00:00:00.123'"},
		{"DateTime64(3)", 1136214245, "'2006-01-02 15:04:05.000'"},
		{"DateTime64(3)", int64(1136214245123), "'2006-01-02 15:04:05.123'"},
		{"DateTime64(3)", 1136214245.5, "'2006-01-02 15:04:05.500'"},
		{"DateTime64(3)", json.Number("1136214245.25"), "'2006-01-02 15:04:05.250'"},
		{"Nullable(String)", nil, "NULL"},
		{"String", nil, "''"}, // NULL into non-Nullable inserts the default
		{"UInt8", nil, "0"},
		{"Array(String)", []string{"a", "b"}, "['a','b']"},
		{"Array(UInt8)", []int{1, 2}, "[1,2]"},
		{"Array(Nullable(UInt8))", []any{nil, 1}, "[NULL,1]"},
		{"Array(String)", "['x']", "['x']"},
		{"Tuple(a UInt8, b String)", []any{1, "x"}, "(1,'x')"},
		{"Tuple(a UInt8, b String)", map[string]any{"b": "y"}, "(0,'y')"},
		{"Tuple(UInt32, Array(String))", []any{12, []string{"1", "3"}}, "(12,['1','3'])"},
		{"Map(String, UInt8)", map[string]int{"b": 2, "a": 1}, "{'a':1,'b':2}"},
		{"Map(String, UInt8)", []any{[]any{"z", 1}, []any{"a", 2}}, "{'z':1,'a':2}"},
		{"Map(String, UInt8)", "{'k':3}", "{'k':3}"},
		{"LowCardinality(Nullable(String))", "v", "'v'"},
		{"SimpleAggregateFunction(sum, UInt64)", 5, "5"},
	}
	for _, c := range cases {
		typ := MustParse(c.typ)
		got, err := Normalize(typ, c.in)
		if err != nil {
			t.Errorf("Normalize(%s, %#v): %v", c.typ, c.in, err)
			continue
		}
		checkCanonical(t, typ, got)
		if s := FormatQuoted(typ, got); s != c.want {
			t.Errorf("Normalize(%s, %#v) = %s, want %s", c.typ, c.in, s, c.want)
		}
	}
	p := &AggPartial{Func: "count", Count: 3}
	if got, err := Normalize(MustParse("AggregateFunction(count)"), p); err != nil || got != p {
		t.Errorf("Normalize(*AggPartial) = %v, %v", got, err)
	}
	bad := []struct {
		typ string
		in  any
	}{
		{"UInt8", 256}, {"UInt8", -1}, {"Int8", 128}, {"UInt64", -1}, {"Int64", uint64(math.MaxUint64)},
		{"UInt8", 1.5}, {"Int32", math.NaN()}, {"String", 5}, {"String", 1.5}, {"String", true},
		{"Bool", 2}, {"Enum8('a' = 1)", "z"}, {"Enum8('a' = 1)", 9}, {"UUID", "nope"}, {"UUID", 5},
		{"FixedString(1)", "ab"}, {"Date", "garbage"}, {"DateTime", -1}, {"DateTime", time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"DateTime64(9)", time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)}, {"Array(UInt8)", 5},
		{"Tuple(UInt8, UInt8)", []any{1}}, {"Tuple(a UInt8)", map[string]any{"zz": 1}},
		{"Map(String, UInt8)", []any{[]any{"only-key"}}}, {"AggregateFunction(count)", 1},
		{"Nothing", 1}, {"String", myStringer{}},
	}
	for _, c := range bad {
		if v, err := Normalize(MustParse(c.typ), c.in); err == nil {
			t.Errorf("Normalize(%s, %#v) = %#v, want error", c.typ, c.in, v)
		} else if !errors.As(err, new(*cherr.Error)) {
			t.Errorf("Normalize(%s, %#v) error is %T, want *cherr.Error", c.typ, c.in, err)
		}
	}
}

func TestConvertNullAndComposite(t *testing.T) {
	if v, err := Convert(nil, MustParse("Nullable(UInt8)"), MustParse("Nullable(String)")); err != nil || v != nil {
		t.Errorf("NULL -> Nullable: %v %v", v, err)
	}
	if _, err := Convert(nil, MustParse("Nullable(UInt8)"), TUInt8); !cherr.Is(err, cherr.CannotConvertType) {
		t.Errorf("NULL -> UInt8: %v", err)
	}
	src := []any{[]any{"a", uint64(1)}, []any{"b", uint64(300)}}
	v, err := Convert(src, MustParse("Map(String, UInt16)"), MustParse("Map(String, UInt8)"))
	if err != nil || FormatQuoted(MustParse("Map(String, UInt8)"), v) != "{'a':1,'b':44}" {
		t.Errorf("Map conversion: %v %v", v, err)
	}
	// Untyped source (Type{}) infers the type from the Go value.
	if v, err := Convert(uint64(5), Type{}, TString); err != nil || v != "5" {
		t.Errorf("Convert untyped: %v %v", v, err)
	}
	if _, err := Convert(&AggPartial{}, MustParse("AggregateFunction(count)"), TUInt64); err == nil {
		t.Error("AggregateFunction -> UInt64 should fail")
	}
}

func TestFormattersMisc(t *testing.T) {
	if err := CheckFormattable(MustParse("Array(Tuple(String, AggregateFunction(count)))")); !cherr.Is(err, cherr.NotImplemented) {
		t.Errorf("CheckFormattable: %v", err)
	}
	if err := CheckFormattable(MustParse("Map(String, Array(Nullable(UInt8)))")); err != nil {
		t.Errorf("CheckFormattable: %v", err)
	}
	tup := MustParse("Tuple(String, Tuple(UInt8, String))")
	if got := string(AppendCSV(nil, tup, []any{"a", []any{uint64(1), "b"}}, ';')); got != `"a";1;"b"` {
		t.Errorf("AppendCSV custom delimiter: %q", got)
	}
	if got := string(AppendJSONWith(nil, TString, "a\xff\xfeb", JSONOptions{ValidateUTF8: true})); got != "\"a�b\"" {
		t.Errorf("ValidateUTF8: %q", got)
	}
	if got := string(AppendJSON(nil, TString, "a\xffb", false, false)); got != "\"a\xffb\"" {
		t.Errorf("raw invalid UTF-8 should pass through: %q", got)
	}
	// Non-canonical Go values never crash the formatters.
	if got := FormatText(TUInt8, 7); got != "7" {
		t.Errorf("FormatText(int) = %q", got)
	}
	if got := FormatText(TFloat64, float32(1.5)); got != "1.5" {
		t.Errorf("FormatText(float32) = %q", got)
	}
}

func TestFloatFormatting(t *testing.T) {
	// Expected strings observed from ClickHouse TabSeparated output.
	cases := []struct {
		f    float64
		bits int
		want string
	}{
		{1, 64, "1"}, {0.1, 64, "0.1"}, {1e-7, 64, "1e-7"}, {1.5e-7, 64, "1.5e-7"}, {1e-6, 64, "0.000001"},
		{1.234e-6, 64, "0.000001234"}, {1e21, 64, "1e21"}, {1e20, 64, "100000000000000000000"},
		{9.99e20, 64, "999000000000000000000"}, {1.234e21, 64, "1.234e21"}, {123456789.123, 64, "123456789.123"},
		{1.2345678901234568e22, 64, "1.2345678901234568e22"}, {123e-20, 64, "1.23e-18"}, {1e100, 64, "1e100"},
		{-1e-7, 64, "-1e-7"}, {-1e21, 64, "-1e21"}, {math.Copysign(0, -1), 64, "-0"}, {0, 64, "0"},
		{math.Inf(1), 64, "inf"}, {math.Inf(-1), 64, "-inf"}, {math.NaN(), 64, "nan"},
		{float64(float32(123456789.123)), 32, "123456790"}, {float64(float32(1.0 / 3)), 32, "0.33333334"},
		{float64(float32(16777217)), 32, "16777216"}, {float64(float32(12345678901234567890.0)), 32, "12345679000000000000"},
		{0.00012345678901234567, 64, "0.00012345678901234567"}, {float64(float32(0.000123456789012345678)), 32, "0.00012345679"},
		{5e-324, 64, "5e-324"}, {1.7976931348623157e308, 64, "1.7976931348623157e308"},
	}
	for _, c := range cases {
		if got := formatFloat(c.f, c.bits); got != c.want {
			t.Errorf("formatFloat(%v, %d) = %q, want %q", c.f, c.bits, got, c.want)
		}
	}
}

func TestSupertypeEdges(t *testing.T) {
	if st, err := Supertype(); err != nil || st.Kind != Nothing {
		t.Errorf("Supertype() = %v, %v", st, err)
	}
	if st, err := Supertype(TString); err != nil || st.Kind != String {
		t.Errorf("Supertype(String) = %v, %v", st, err)
	}
	if st, err := Supertype(TNothing, TUInt8, TNothing); err != nil || st.Kind != UInt8 {
		t.Errorf("Nothing identity: %v, %v", st, err)
	}
	if _, err := Supertype(TUInt64, TInt8); !cherr.Is(err, codeNoCommonType) {
		t.Errorf("UInt64+Int8: %v", err)
	}
}

func TestTimezoneNames(t *testing.T) {
	for _, tz := range []string{"UTC", "Etc/UTC", "GMT", "Europe/Berlin", "America/Argentina/Buenos_Aires", "Asia/Kolkata", "Australia/Lord_Howe"} {
		if err := checkTimezone(tz); err != nil {
			t.Errorf("checkTimezone(%q): %v", tz, err)
		}
	}
	for _, tz := range []string{"utc", "europe/berlin", "Europe/berlin", "Local", "Nowhere/Land", "../etc/passwd"} {
		if err := checkTimezone(tz); err == nil {
			t.Errorf("checkTimezone(%q) succeeded", tz)
		}
	}
}
