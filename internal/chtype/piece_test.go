package chtype

import (
	"runtime"
	"strings"
	"testing"
	"unsafe"
)

// A piece stays a view only when it leaves under 64 B of its source
// unused and is at least 4/5 of it; every other piece is copied.
func TestDetachValueKeepsOnlyNearWholePiecesAsViews(t *testing.T) {
	src := strings.Repeat("x", 1000)
	for _, c := range []struct {
		part, s string
		copied  bool
	}{
		{src[:999], src, false},
		{src[1:], src, false},
		{src[:937], src, false},
		{src[:936], src, true},
		{src[:2], src[:50], true},
		{src[:4], src[:5], false},
		{src[:3], src[:5], true},
		{src, src, false},
		{"", src, false},
	} {
		got := DetachValue(c.part, c.s).(string)
		copied := got != "" && unsafe.StringData(got) != unsafe.StringData(c.part)
		if got != c.part || copied != c.copied {
			t.Errorf("%d of %d bytes: copied %v, want %v", len(c.part), len(c.s), copied, c.copied)
		}
	}
}

// eface is the layout Go uses for an empty interface holding a string.
func TestEfaceIsTheEmptyInterfaceLayout(t *testing.T) {
	v := any("layout")
	e := (*eface)(unsafe.Pointer(&v))
	if unsafe.Sizeof(v) != unsafe.Sizeof(*e) || *(*string)(e.data) != "layout" {
		t.Fatal("an interface value is not a type word and a pointer to the boxed string")
	}
}

// A piece of up to 48 B is boxed with its bytes in one allocation, and
// stays intact after the source is gone and the GC has run.
func TestDetachValueBoxesShortPiecesInOneAllocation(t *testing.T) {
	src := strings.Repeat("abcdefgh", 20)
	for _, n := range []int{1, 16, 17, 32, 33, 48} {
		if a := testing.AllocsPerRun(100, func() { _ = DetachValue(src[1:1+n], src) }); a != 1 {
			t.Errorf("DetachValue of %d bytes: %v allocations, want 1", n, a)
		}
	}
	held := make([]any, 0, 64)
	for n := 1; n <= 64; n++ {
		s := strings.Repeat("y", 1) + strings.Repeat("z", 100)
		held = append(held, DetachValue(s[1:1+n], s))
	}
	runtime.GC()
	for i, v := range held {
		if want := strings.Repeat("z", i+1); v.(string) != want {
			t.Fatalf("piece %d is %q after GC", i+1, v)
		}
	}
}

// CopyValue always copies, in one allocation up to 48 B.
func TestCopyValueCopiesInOneAllocationUpTo48B(t *testing.T) {
	src := strings.Repeat("abcdefgh", 20)
	for _, n := range []int{0, 1, 48, 49, 160} {
		got := CopyValue(src[:n]).(string)
		if got != src[:n] || n > 0 && unsafe.StringData(got) == unsafe.StringData(src) {
			t.Errorf("CopyValue of %d bytes: %q is not a copy", n, got)
		}
		want := 1.0
		if n > 48 {
			want = 2
		}
		if a := testing.AllocsPerRun(100, func() { _ = CopyValue(src[:n]) }); a != want {
			t.Errorf("CopyValue of %d bytes: %v allocations, want %v", n, a, want)
		}
	}
}
