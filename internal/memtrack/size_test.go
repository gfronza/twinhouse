package memtrack

import (
	"testing"

	"github.com/gfronza/twinhouse/internal/chtype"
)

func TestAllocMatchesSizeClasses(t *testing.T) {
	for _, c := range []struct{ n, want int64 }{
		{0, 0}, {1, 8}, {8, 8}, {9, 16}, {17, 24}, {25, 32}, {33, 48}, {129, 144},
		{1024, 1024}, {1025, 1152}, {3073, 3200}, {32768, 32768}, {32769, 40960}, {1 << 20, 1 << 20},
	} {
		if got := Alloc(c.n); got != c.want {
			t.Errorf("Alloc(%d) = %d, want %d", c.n, got, c.want)
		}
	}
}

func TestAggPartialBytesCoversTheStruct(t *testing.T) {
	if AggPartialBytes < 176 {
		t.Fatalf("AggPartialBytes = %d", AggPartialBytes)
	}
}

func TestFixedValueCoversFixedSizeTypes(t *testing.T) {
	for _, c := range []struct {
		t     chtype.Type
		n     int64
		fixed bool
	}{
		{chtype.TUInt64, 16, true},
		{chtype.NewNullable(chtype.TFloat64), 16, true},
		{chtype.TBool, 0, true},
		{chtype.TDateTime, 24, true},
		{chtype.TString, 0, false},
		{chtype.NewArray(chtype.TUInt64), 0, false},
		{chtype.TUUID, 0, false},
	} {
		if n, ok := FixedValue(c.t); n != c.n || ok != c.fixed {
			t.Errorf("FixedValue(%v) = %d, %v", c.t.Kind, n, ok)
		}
	}
}

func TestShortValuesCountTheirTinyBlock(t *testing.T) {
	for _, c := range []struct {
		v    any
		want int64
	}{
		{uint64(5), 16}, {int64(-5), 16}, {0.5, 16},
		{"", 0}, {"a", StringHeader + 16}, {"15 bytes string", StringHeader + 16}, {"16 bytes string!", StringHeader + 16},
		{"17 bytes string!!", StringHeader + 24},
	} {
		if got := Value(c.v); got != c.want {
			t.Errorf("Value(%#v) = %d, want %d", c.v, got, c.want)
		}
	}
}
