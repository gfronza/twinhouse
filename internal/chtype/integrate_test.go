package chtype

import (
	"math"
	"testing"

	"dynamodb-eventstream/internal/cherr"
)

// TestConvertNumberErrorCodes: CAST from String to a number reports the
// codes ClickHouse 25.8 gives (verified against the reference server).
func TestConvertNumberErrorCodes(t *testing.T) {
	cases := []struct {
		s, typ string
		code   int
	}{
		{"", "UInt64", 32},
		{"-", "Int8", 72},
		{"+", "UInt64", 72},
		{"", "Float64", 72},
		{"-", "Float64", 72},
		{"1e", "Float64", 72},
		{"2.5E+", "Float32", 72},
		{"abc", "Float64", 6},
		{"1.5x", "Float64", 6},
		{"abc", "UInt8", 6},
	}
	for _, c := range cases {
		_, err := Convert(c.s, TString, MustParse(c.typ))
		var ce *cherr.Error
		if !asCherr(err, &ce) || ce.Code != c.code {
			t.Errorf("CAST(%q AS %s): got %v, want code %d", c.s, c.typ, err, c.code)
		}
	}
}

// TestNegativeNaNKeepsSign: "-nan" parses with the sign bit set (text and
// quoted parsers), while text output still prints nan for both signs.
func TestNegativeNaNKeepsSign(t *testing.T) {
	for _, typ := range []Type{MustParse("Float32"), MustParse("Float64")} {
		v, err := ParseText(typ, "-nan")
		if err != nil {
			t.Fatal(err)
		}
		if f := v.(float64); !math.IsNaN(f) || !math.Signbit(f) {
			t.Errorf("ParseText(%s, -nan) = %v, sign lost", typ, v)
		}
		if v, _ := ParseText(typ, "nan"); math.Signbit(v.(float64)) {
			t.Errorf("ParseText(%s, nan) has the sign bit", typ)
		}
		q, _, err := ParseQuoted(typ, "-nan")
		if err != nil {
			t.Fatal(err)
		}
		if f, ok := q.(float64); !ok || !math.IsNaN(f) || !math.Signbit(f) {
			t.Errorf("ParseQuoted(%s, -nan) = %#v, sign lost", typ, q)
		}
		if got := FormatText(typ, math.Copysign(math.NaN(), -1)); got != "nan" {
			t.Errorf("FormatText(-nan) = %q, want nan", got)
		}
	}
}

func asCherr(err error, out **cherr.Error) bool {
	ce, ok := err.(*cherr.Error)
	if ok {
		*out = ce
	}
	return ok
}
