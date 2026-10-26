package chtype

import (
	"strings"
	"testing"

	"github.com/gfronza/twinhouse/internal/cherr"
)

type supertypeCase struct {
	in   []string
	want string
	code int
}

// TestSupertypeGolden checks Supertype against ClickHouse's
// getLeastSupertype as observed through array literal typing.
func TestSupertypeGolden(t *testing.T) {
	for _, c := range supertypeGolden {
		t.Run(strings.Join(c.in, "|"), func(t *testing.T) {
			ts := make([]Type, len(c.in))
			for i, s := range c.in {
				ts[i] = MustParse(s)
			}
			got, err := Supertype(ts...)
			if c.code != 0 {
				if err == nil {
					t.Fatalf("ClickHouse has no supertype (code %d); got %s", c.code, got)
				}
				if c.code == codeNoCommonType && !cherr.Is(err, codeNoCommonType) {
					t.Errorf("error code: got %v, want %d", err, c.code)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v (ClickHouse: %s)", err, c.want)
			}
			if got.String() != c.want {
				t.Fatalf("got %s, want %s", got, c.want)
			}
		})
	}
}
