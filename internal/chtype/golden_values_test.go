package chtype

import "testing"

type valuesCase struct {
	typ, in, want string
	code          int
}

// valuesDeviations: VALUES inputs ClickHouse accepts only through its
// expression evaluator (identifiers, arithmetic) which ParseQuoted
// rejects so that the caller can fall back to evaluating the expression.
var valuesDeviations = map[string]string{}

// TestValuesGolden checks ParseQuoted against the value ClickHouse stores
// for INSERT ... VALUES (<literal>).
func TestValuesGolden(t *testing.T) {
	for _, c := range valuesGolden {
		key := c.typ + "|" + c.in
		t.Run(key, func(t *testing.T) {
			typ := MustParse(c.typ)
			v, n, err := ParseQuoted(typ, c.in)
			if err == nil && n != len(c.in) {
				err = errf(0, "consumed %d of %d bytes", n, len(c.in))
			}
			if why, ok := valuesDeviations[key]; ok {
				if err == nil {
					t.Fatalf("expected deliberate rejection (%s), got %q", why, FormatQuoted(typ, v))
				}
				return
			}
			if c.code != 0 {
				if err == nil {
					t.Fatalf("ClickHouse rejects this (code %d); got %q", c.code, FormatQuoted(typ, v))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v (ClickHouse: %q)", err, c.want)
			}
			checkCanonical(t, typ, v)
			if s := FormatQuoted(typ, v); s != c.want {
				t.Fatalf("got %q, want %q", s, c.want)
			}
		})
	}
}
