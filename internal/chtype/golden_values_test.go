package chtype

import "testing"

type valuesCase struct {
	typ, in, want string
	code          int
}

// TestValuesGolden checks ParseQuoted against the value ClickHouse stores
// for INSERT ... VALUES (<literal>).
func TestValuesGolden(t *testing.T) {
	for _, c := range valuesGolden {
		t.Run(c.typ+"|"+c.in, func(t *testing.T) {
			typ := MustParse(c.typ)
			v, n, err := ParseQuoted(typ, c.in)
			if err == nil && n != len(c.in) {
				err = errf(0, "consumed %d of %d bytes", n, len(c.in))
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
