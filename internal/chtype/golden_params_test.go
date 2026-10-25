package chtype

import "testing"

type paramCase struct {
	typ, in, want string
	code          int
}

// paramDeviations lists inputs ClickHouse accepts with a garbage or
// silently-defaulted result that this implementation rejects on purpose
// (correctness over compatibility for malformed input).
var paramDeviations = map[string]string{
	"DateTime|2006-01-02 1a:04:05":              "non-digit in time; ClickHouse computes garbage",
	"DateTime|2006-13-01 00:00:00":              "invalid month; ClickHouse returns the epoch",
	"DateTime|2006-00-10 00:00:00":              "invalid month; ClickHouse returns the epoch",
	"DateTime|2006-01-32 00:00:00":              "invalid day; ClickHouse returns the epoch",
	"DateTime|4294967296":                       "timestamp beyond DateTime; ClickHouse wraps modulo 2^32",
	"DateTime|2106-02-07 06:28:16":              "beyond DateTime range; ClickHouse wraps",
	"DateTime|2200-01-01 00:00:00":              "beyond DateTime range; ClickHouse wraps",
	"DateTime|20060102150405":                   "timestamp beyond DateTime; ClickHouse wraps",
	"Date|2006-13-01":                           "invalid month; ClickHouse returns the epoch",
	"Date|2006-01-0":                            "invalid day; ClickHouse returns the epoch",
	"DateTime64(3)|2300-01-01 00:00:00":         "beyond range; ClickHouse clamps the year",
	"DateTime64(3)|2299-12-31 23:59:59.999":     "beyond the int64-nanosecond cap (2262-04-11)",
	"Date32|2299-12-31":                         "beyond the int64-nanosecond cap (2262-04-11)",
	"Date32|2300-01-01":                         "beyond the int64-nanosecond cap (2262-04-11)",
	"String|\\xZZ":                              "invalid hex escape; ClickHouse decodes garbage",
	"UUID|zzf0c404-5cb3-11e7-907b-a6006ad3dba0": "invalid hex digits; ClickHouse decodes garbage",
	"Float64|.":                                 "no digits; ClickHouse returns 0",
	"Float64|e5":                                "no mantissa; ClickHouse returns 0",
	"Float64|1e+":                               "empty exponent; ClickHouse ignores it",
}

// paramValueDeviations: inputs where this implementation is more precise.
var paramValueDeviations = map[string]string{
	// ClickHouse's Float32 text parser rounds to the float32 below FLT_MAX.
	"Float32|3.4028235e38": "3.4028235e38",
}

// TestParamGolden checks ParseText against ClickHouse's parsing of HTTP
// query parameter values (param_x=...).
func TestParamGolden(t *testing.T) {
	for _, c := range paramGolden {
		t.Run(c.typ+"|"+c.in, func(t *testing.T) {
			typ := MustParse(c.typ)
			v, err := ParseText(typ, c.in)
			if why, ok := paramDeviations[c.typ+"|"+c.in]; ok {
				if err == nil {
					t.Fatalf("expected deliberate rejection (%s), got %#v", why, v)
				}
				return
			}
			if c.code != 0 {
				if err == nil {
					t.Fatalf("ClickHouse rejects this (code %d); got value %q", c.code, FormatQuoted(typ, v))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v (ClickHouse: %q)", err, c.want)
			}
			checkCanonical(t, typ, v)
			want := c.want
			if w, ok := paramValueDeviations[c.typ+"|"+c.in]; ok {
				want = w
			}
			if got := FormatQuoted(typ, v); got != want {
				t.Fatalf("got %q, want %q", got, want)
			}
		})
	}
}
