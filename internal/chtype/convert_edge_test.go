package chtype

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// TestCastCompositeCodes checks error codes and results of casts involving
// Array/Tuple/Map against ClickHouse 25.8 (want "" means an error with code).
func TestCastCompositeCodes(t *testing.T) {
	cases := []struct {
		from, src, to, want string
		code                int
	}{
		// composite -> scalar: ILLEGAL_TYPE_OF_ARGUMENT (43), except Enum (70)
		// and FixedString (48).
		{"Array(UInt8)", "[1]", "UInt8", "", 43},
		{"Array(UInt8)", "[1]", "Nullable(UInt8)", "", 43},
		{"Array(UInt8)", "[1]", "Bool", "", 43},
		{"Array(UInt8)", "[1]", "Float64", "", 43},
		{"Array(UInt8)", "[1]", "Date", "", 43},
		{"Array(UInt8)", "[1]", "DateTime", "", 43},
		{"Array(UInt8)", "[1]", "UUID", "", 43},
		{"Tuple(UInt8, UInt8)", "(1,2)", "UInt8", "", 43},
		{"Map(UInt8, UInt8)", "{1:2}", "UInt8", "", 43},
		{"Array(UInt8)", "[1]", "Enum8('a' = 1)", "", 70},
		{"Array(UInt8)", "[1]", "Enum16('a' = 1)", "", 70},
		{"Array(UInt8)", "[1]", "DateTime64(3)", "", 70},
		{"Tuple(UInt8, UInt8)", "(1,2)", "DateTime64(3)", "", 70},
		{"Array(UInt8)", "[1]", "Date32", "", 43},
		{"Array(UInt8)", "[1]", "DateTime('UTC')", "", 43},
		{"Array(UInt8)", "[1]", "Int64", "", 43},
		{"Map(UInt8, UInt8)", "{1:2}", "Bool", "", 43},
		{"Map(UInt8, Array(UInt8))", "{1:[2]}", "Map(UInt8, UInt8)", "", 43},
		{"Map(UInt8, UInt8)", "{1:2}", "Map(UInt8, Array(UInt8))", "", 53},
		{"Array(UInt8)", "[1]", "FixedString(3)", "", 48},
		{"Array(UInt8)", "[1]", "String", "'[1]'", 0},
		{"Map(UInt8, UInt8)", "{1:2}", "String", "'{1:2}'", 0},
		// Array dimensions must match (TYPE_MISMATCH), except from Array(Nothing).
		{"Array(Array(UInt8))", "[[1]]", "Array(UInt8)", "", 53},
		{"Array(UInt8)", "[1,2]", "Array(Array(UInt8))", "", 53},
		{"Array(Nothing)", "[]", "Array(Array(UInt8))", "[]", 0},
		{"UInt8", "1", "Array(UInt8)", "", 53},
		{"Tuple(UInt8, UInt8)", "(1,2)", "Array(UInt8)", "", 53},
		// Map <-> Array(Tuple(K, V)) and Tuple(Array(K), Array(V)) -> Map.
		{"Map(UInt8, UInt8)", "{1:2}", "Array(Tuple(UInt8, UInt8))", "[(1,2)]", 0},
		{"Map(UInt8, UInt8)", "{1:2}", "Array(Tuple(String, UInt16))", "[('1',2)]", 0},
		{"Map(UInt8, UInt8)", "{1:2}", "Array(UInt8)", "", 43},
		{"Array(Tuple(UInt8, UInt8))", "[(1,2),(3,4)]", "Map(UInt8, UInt8)", "{1:2,3:4}", 0},
		{"Array(Tuple(UInt8, UInt8))", "[(1,2)]", "Map(String, UInt8)", "{'1':2}", 0},
		{"Array(Nothing)", "[]", "Map(UInt8, UInt8)", "{}", 0},
		{"Tuple(Array(UInt8), Array(UInt8))", "([1],[2])", "Map(UInt8, UInt8)", "{1:2}", 0},
		{"Tuple(UInt8, UInt8)", "(1,2)", "Map(UInt8, UInt8)", "", 53},
		{"Array(Tuple(UInt8, UInt8, UInt8))", "[(1,2,3)]", "Map(UInt8, UInt8)", "", 53},
		{"Array(UInt8)", "[1]", "Map(UInt8, UInt8)", "", 53},
		{"UInt8", "1", "Map(UInt8, UInt8)", "", 53},
		{"Array(UInt8)", "[1]", "Tuple(UInt8)", "", 53},
		// String -> integer keeps CANNOT_PARSE_NUMBER for a sign on unsigned.
		{"String", "'-1'", "UInt8", "", 72},
		{"String", "'300'", "UInt8", "44", 0},
		{"String", "'1.5'", "UInt8", "", 6},
		// Empty / sign-only strings (ClickHouse 25.8).
		{"String", "''", "UInt8", "", 32},
		{"String", "''", "Int64", "", 32},
		{"String", "'-'", "Int64", "", 72},
		{"String", "'+'", "Int32", "", 72},
		{"String", "''", "Float64", "", 72},
		{"String", "''", "Date", "", 38},
		{"String", "''", "DateTime", "", 41},
		{"String", "''", "DateTime64(3)", "'1970-01-01 00:00:00.000'", 0},
		{"String", "' 1'", "Int64", "", 6},
		{"String", "'1 '", "Int64", "", 6},
		// String -> date/time: trailing data after a valid prefix is
		// CANNOT_PARSE_TEXT (6); an unparsable value keeps 38/41.
		{"String", "'2024-01-05T10:20:30Z'", "DateTime", "", 6},
		{"String", "'2024-01-05 10:20:30x'", "Date", "", 6},
		{"String", "'2024-01-05x'", "Date", "", 6},
		{"String", "'2024-01-05 10:20:30.5x'", "DateTime64(3)", "", 6},
		{"String", "'garbage'", "DateTime", "", 41},
		{"String", "'garbage'", "DateTime64(3)", "", 41},
		{"String", "'garbage'", "Date", "", 38},
		{"String", "'2024-01-05 10:20:30'", "Date", "'2024-01-05'", 0},
		{"String", "'20240105'", "Date", "'2024-01-05'", 0},
		{"String", "'2024-01-05T10:20:30'", "DateTime", "'2024-01-05 10:20:30'", 0},
	}
	for _, c := range cases {
		t.Run(c.from+"|"+c.src+"|"+c.to, func(t *testing.T) {
			from, to := MustParse(c.from), MustParse(c.to)
			v, n, err := ParseQuoted(from, c.src)
			if err != nil || n != len(c.src) {
				t.Fatalf("ParseQuoted(%s, %q) = %v, %d, %v", c.from, c.src, v, n, err)
			}
			got, err := Convert(v, from, to)
			if c.code != 0 {
				if code := errCodeOf(err); code != c.code {
					t.Fatalf("got code %d (err %v, value %v), want %d", code, err, got, c.code)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			checkCanonical(t, to, got)
			if s := FormatQuoted(to, got); s != c.want {
				t.Fatalf("got %s, want %s", s, c.want)
			}
		})
	}
}

func TestCastIllegalTypeMessage(t *testing.T) {
	_, err := Convert([]any{uint64(1)}, MustParse("Array(UInt8)"), MustParse("UInt8"))
	want := "Code: 43. DB::Exception: Illegal type Array(UInt8) of argument of function toUInt8. (ILLEGAL_TYPE_OF_ARGUMENT)"
	if err == nil || err.Error() != want {
		t.Fatalf("got %v, want %s", err, want)
	}
}

func TestCastTrailingDataMessage(t *testing.T) {
	_, err := Convert("2024-01-05T10:20:30Z", TString, MustParse("DateTime"))
	want := "Code: 6. DB::Exception: Cannot parse string '2024-01-05T10:20:30Z' as DateTime: syntax error at position 19 (parsed just '2024-01-05T10:20:30'). (CANNOT_PARSE_TEXT)"
	if err == nil || err.Error() != want {
		t.Fatalf("got %v, want %s", err, want)
	}
	// Input-format parsing keeps CANNOT_PARSE_DATETIME, as a plain *cherr.Error.
	_, err = ParseText(MustParse("DateTime"), "2024-01-05T10:20:30Z")
	if _, ok := err.(*cherr.Error); !ok || errCodeOf(err) != cherr.CannotParseDateTime {
		t.Fatalf("ParseText: got %T %v, want *cherr.Error code 41", err, err)
	}
	// JSON input (Normalize): CANNOT_PARSE_INPUT_ASSERTION_FAILED like
	// ClickHouse's JSONEachRow.
	for _, c := range [][2]string{{"DateTime", "2024-01-05T10:20:30Z"}, {"Date", "2024-01-05x"}, {"DateTime64(3)", "2024-01-05 10:20:30.5x"}} {
		_, err = Normalize(MustParse(c[0]), c[1])
		if _, ok := err.(*cherr.Error); !ok || errCodeOf(err) != 27 {
			t.Fatalf("Normalize(%s, %q): got %T %v, want *cherr.Error code 27", c[0], c[1], err, err)
		}
	}
	if _, err = Normalize(MustParse("DateTime"), "garbage"); errCodeOf(err) != cherr.CannotParseDateTime {
		t.Fatalf("Normalize(DateTime, garbage): got %v, want code 41", err)
	}
	if _, err = Normalize(MustParse("UInt64"), "not a number"); errCodeOf(err) != 27 {
		t.Fatalf("Normalize(UInt64, 'not a number'): got %v, want code 27", err)
	}
}

func TestZeroDate32(t *testing.T) {
	// SELECT defaultValueOfTypeName('Date32') -> 1900-01-01
	want := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	if z, ok := Zero(Type{Kind: Date32}).(time.Time); !ok || !z.Equal(want) {
		t.Fatalf("Zero(Date32) = %v, want %v", Zero(Type{Kind: Date32}), want)
	}
	if z := Zero(MustParse("Tuple(Date32, LowCardinality(String))")).([]any); !z[0].(time.Time).Equal(want) {
		t.Fatalf("Zero(Tuple(Date32,...)) = %v", z)
	}
	if z := Zero(MustParse("Date")).(time.Time); !z.Equal(Epoch) {
		t.Fatalf("Zero(Date) = %v", z)
	}
}

func TestParseNothingComposites(t *testing.T) {
	// toTypeName(map()) / toTypeName([]) in ClickHouse.
	for _, s := range []string{"Map(Nothing, Nothing)", "Array(Nothing)", "Map(String, Nothing)", "Array(Map(Nothing, Nothing))"} {
		ty, err := Parse(s)
		if err != nil {
			t.Fatalf("Parse(%q): %v", s, err)
		}
		if ty.String() != s {
			t.Fatalf("Parse(%q).String() = %q", s, ty.String())
		}
	}
	if _, err := Parse("Map(Nullable(String), UInt8)"); errCodeOf(err) != cherr.BadArguments {
		t.Fatalf("Map(Nullable(String), UInt8): got %v, want code 36", err)
	}
	v, err := Convert([]any{}, MustParse("Map(Nothing, Nothing)"), MustParse("Map(String, UInt8)"))
	if err != nil || len(v.([]any)) != 0 {
		t.Fatalf("CAST(map() AS Map(String, UInt8)) = %v, %v", v, err)
	}
}

// TestNormalizeJSONIntegers checks INSERT ... FORMAT JSONEachRow number
// handling for integer columns. Error codes match ClickHouse 25.8 where it
// also rejects the input: a '-' on an unsigned type is CANNOT_PARSE_NUMBER
// (72) and a fractional number CANNOT_PARSE_INPUT_ASSERTION_FAILED (27).
// Out-of-range integers are rejected (69) on purpose where ClickHouse wraps
// them (300 into UInt8 -> 44).
func TestNormalizeJSONIntegers(t *testing.T) {
	cases := []struct {
		typ  string
		in   any
		want string
		code int
	}{
		{"UInt8", json.Number("300"), "", 69},
		{"UInt8", "300", "44", 0}, // string -> CAST-from-String rules (wraps, as ClickHouse)
		{"Int8", json.Number("200"), "", 69},
		{"UInt64", json.Number("18446744073709551616"), "", 69},
		{"Int64", json.Number("9223372036854775808"), "", 69},
		{"UInt64", json.Number("18446744073709551615"), "18446744073709551615", 0},
		{"Int64", json.Number("-9223372036854775808"), "-9223372036854775808", 0},
		{"Array(UInt8)", []any{json.Number("255"), json.Number("7")}, "[255,7]", 0},
		{"UInt8", json.Number("-1"), "", 72},
		{"UInt8", "-1", "", 72},
		{"Array(UInt8)", []any{json.Number("-0")}, "", 72},
		{"UInt8", json.Number("1.5"), "", 27},
		{"UInt8", json.Number("1.0"), "1", 0},
		{"UInt8", json.Number("7"), "7", 0},
		{"Int32", json.Number("-7"), "-7", 0},
		{"UInt8", true, "1", 0},
		{"UInt8", 300, "", 69},
	}
	for _, c := range cases {
		ty := MustParse(c.typ)
		got, err := Normalize(ty, c.in)
		if c.code != 0 {
			if code := errCodeOf(err); code != c.code {
				t.Errorf("Normalize(%s, %v): got code %d (%v, %v), want %d", c.typ, c.in, code, got, err, c.code)
			}
			continue
		}
		if err != nil {
			t.Errorf("Normalize(%s, %v): %v", c.typ, c.in, err)
			continue
		}
		checkCanonical(t, ty, got)
		if s := FormatQuoted(ty, got); s != c.want {
			t.Errorf("Normalize(%s, %v) = %s, want %s", c.typ, c.in, s, c.want)
		}
	}
}

// TestDateTimeSaturation checks ClickHouse 25.8's range handling for
// DateTime/DateTime64: CAST from String saturates DateTime at
// 2106-02-07 06:28:15 (input formats and query parameters keep rejecting,
// a documented deviation from ClickHouse's modulo-2^32 wrap), and every
// DateTime64 path clamps below 1900-01-01 (the local calendar date for
// text, the UTC instant for numbers).
func TestDateTimeSaturation(t *testing.T) {
	cast := []struct{ to, src, want string }{
		{"DateTime", "2200-01-01 00:00:00", "'2106-02-07 06:28:15'"},
		{"DateTime", "9999-12-31", "'2106-02-07 06:28:15'"},
		{"DateTime", "2106-02-07 06:28:16", "'2106-02-07 06:28:15'"},
		{"DateTime", "2106-02-07 06:28:15", "'2106-02-07 06:28:15'"},
		{"DateTime", "4294967296", "'2106-02-07 06:28:15'"},
		{"DateTime('Asia/Tokyo')", "2200-01-01 00:00:00", "'2106-02-07 15:28:15'"},
		{"DateTime('America/New_York')", "2200-01-01 00:00:00", "'2106-02-07 01:28:15'"},
		{"Array(DateTime)", "['2200-01-01 00:00:00']", "['2106-02-07 06:28:15']"},
		{"DateTime64(3)", "1800-01-01 00:00:00", "'1900-01-01 00:00:00.000'"},
		{"DateTime64(3)", "1899-12-31 23:59:59.999", "'1900-01-01 23:59:59.999'"},
		{"DateTime64(3, 'Asia/Tokyo')", "1800-01-01 00:00:00", "'1900-01-01 00:00:00.000'"},
		{"DateTime64(3, 'Asia/Tokyo')", "1900-01-01 00:00:00", "'1900-01-01 00:00:00.000'"},
		{"DateTime64(0, 'Asia/Tokyo')", "1900-01-01", "'1900-01-01 00:00:00'"},
		{"DateTime64(0, 'America/New_York')", "1899-12-31 23:00:00", "'1900-01-01 23:00:00'"},
		{"DateTime64(3)", "-3000000000", "'1900-01-01 00:00:00.000'"},
		{"DateTime64(3, 'Asia/Tokyo')", "-3000000000", "'1900-01-01 00:00:00.000'"},
		{"DateTime64(3)", "2200-01-01 00:00:00", "'2200-01-01 00:00:00.000'"},
	}
	for _, c := range cast {
		to := MustParse(c.to)
		got, err := Convert(c.src, TString, to)
		if err != nil {
			t.Errorf("CAST(%q AS %s): %v", c.src, c.to, err)
			continue
		}
		checkCanonical(t, to, got)
		if s := FormatQuoted(to, got); s != c.want {
			t.Errorf("CAST(%q AS %s) = %s, want %s", c.src, c.to, s, c.want)
		}
	}
	// Overflowing digits: CANNOT_PARSE_NUMBER like ClickHouse.
	if _, err := Convert("99999999999999999999", TString, MustParse("DateTime")); errCodeOf(err) != 72 {
		t.Errorf("CAST('99999999999999999999' AS DateTime): got %v, want code 72", err)
	}
	// Beyond the representable DateTime64 range (int64 nanoseconds) stays an
	// error; ClickHouse would saturate at 2299-12-31.
	if _, err := Convert("2300-01-01 00:00:00", TString, MustParse("DateTime64(3)")); errCodeOf(err) != 407 {
		t.Errorf("CAST('2300-01-01' AS DateTime64(3)): got %v, want code 407", err)
	}

	// Numbers clamp to 1900-01-01 00:00:00 UTC.
	for _, c := range []struct {
		to   string
		num  any
		want string
	}{
		{"DateTime64(3)", int64(-3000000000), "'1900-01-01 00:00:00.000'"},
		{"DateTime64(3, 'Asia/Tokyo')", int64(-3000000000), "'1900-01-01 09:00:00.000'"},
		{"DateTime64(0)", int64(-2208988801), "'1900-01-01 00:00:00'"},
		{"DateTime64(0)", int64(-2208988800), "'1900-01-01 00:00:00'"},
		{"DateTime64(3)", float64(-3e9), "'1900-01-01 00:00:00.000'"},
	} {
		to := MustParse(c.to)
		got, err := Convert(c.num, TInt64, to)
		if err != nil {
			t.Errorf("CAST(%v AS %s): %v", c.num, c.to, err)
			continue
		}
		if s := FormatQuoted(to, got); s != c.want {
			t.Errorf("CAST(%v AS %s) = %s, want %s", c.num, c.to, s, c.want)
		}
	}
	// Date32 1900-01-01 as a Tokyo DateTime64 is 1899-12-31 15:00 UTC.
	to := MustParse("DateTime64(0, 'Asia/Tokyo')")
	got, err := Convert(minDate32, MustParse("Date32"), to)
	if err != nil || FormatQuoted(to, got) != "'1900-01-01 00:00:00'" {
		t.Errorf("CAST(toDate32('1900-01-01') AS %s) = %v, %v", to, got, err)
	}
	if _, err := Normalize(to, got); err != nil {
		t.Errorf("Normalize(%s, %v): %v", to, got, err)
	}

	// Input formats and parameters: DateTime overflow is still rejected (69),
	// DateTime64 text below 1900 is clamped like ClickHouse.
	for _, s := range []string{"2200-01-01 00:00:00", "5000000000"} {
		if _, err := ParseText(MustParse("DateTime"), s); errCodeOf(err) != 69 {
			t.Errorf("ParseText(DateTime, %q): got %v, want code 69", s, err)
		}
		if _, err := Normalize(MustParse("DateTime"), s); errCodeOf(err) != 69 {
			t.Errorf("Normalize(DateTime, %q): got %v, want code 69", s, err)
		}
	}
	for _, f := range []func() (any, error){
		func() (any, error) { return ParseText(MustParse("DateTime64(3)"), "1800-01-01 00:00:00") },
		func() (any, error) { return Normalize(MustParse("DateTime64(3)"), "1800-01-01 00:00:00") },
		func() (any, error) {
			v, _, err := ParseQuoted(MustParse("DateTime64(3)"), "'1800-01-01 00:00:00'")
			return v, err
		},
	} {
		v, err := f()
		if err != nil || FormatQuoted(MustParse("DateTime64(3)"), v) != "'1900-01-01 00:00:00.000'" {
			t.Errorf("DateTime64 input '1800-01-01 00:00:00' = %v, %v; want 1900-01-01 00:00:00.000", v, err)
		}
	}
}
