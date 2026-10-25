package chtype

import "testing"

type castCase struct {
	from, src, to, want string
	code                int
}

// castDeviations: conversions ClickHouse performs with a wrapped/garbage
// result that this implementation rejects on purpose.
var castDeviations = map[string]string{
	"DateTime64(3, 'UTC')|'1960-01-02 03:04:05.999'|DateTime": "before the DateTime range; ClickHouse wraps modulo 2^32",
	"DateTime64(3, 'UTC')|'1960-01-02 03:04:05.000'|Date":     "before the Date range; ClickHouse wraps modulo 2^16 days",
	"Date32|'1950-01-02'|Date":                                "before the Date range; ClickHouse wraps modulo 2^16 days",
	"UInt64|1700000000123|DateTime64(3)":                      "seconds beyond the DateTime64 range; ClickHouse clamps to 2299-12-31",
}

// TestCastGolden checks Convert against ClickHouse CAST results.
func TestCastGolden(t *testing.T) {
	for _, c := range castGolden {
		key := c.from + "|" + c.src + "|" + c.to
		t.Run(key, func(t *testing.T) {
			from, to := MustParse(c.from), MustParse(c.to)
			v, n, err := ParseQuoted(from, c.src)
			if err != nil || n != len(c.src) {
				t.Fatalf("ParseQuoted(%s, %q) = %v, %d, %v", c.from, c.src, v, n, err)
			}
			got, err := Convert(v, from, to)
			if why, ok := castDeviations[key]; ok {
				if err == nil {
					t.Fatalf("expected deliberate rejection (%s), got %q", why, FormatQuoted(to, got))
				}
				return
			}
			if c.code != 0 {
				if err == nil {
					t.Fatalf("ClickHouse rejects this (code %d); got %q", c.code, FormatQuoted(to, got))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v (ClickHouse: %q)", err, c.want)
			}
			checkCanonical(t, to, got)
			if s := FormatQuoted(to, got); s != c.want {
				t.Fatalf("got %q, want %q", s, c.want)
			}
		})
	}
}
