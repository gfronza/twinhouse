package chtype

import (
	"strings"
	"unsafe"
)

// A Go substring shares the memory of its source, so a kept piece holds
// all of the source while it is charged its own bytes. DetachValue keeps
// a piece as a view only when it leaves under 64 B of its source unused
// and is at least 4/5 of it; any other piece is copied, as ClickHouse
// copies too. So a kept view wastes under 64 B and at most 1.25x its
// bytes, and a chain of d cuts under 64*d B.

// copiesPiece reports whether part, a piece of s, is copied out of it.
func copiesPiece(part, s string) bool {
	return part != "" && (len(s)-len(part) >= 64 || 5*len(part) < 4*len(s))
}

// DetachValue returns part, a piece of s (or a result built in a buffer
// the size of s), as a boxed result: copied out (CopyValue) unless
// copiesPiece keeps it.
func DetachValue(part, s string) any {
	if !copiesPiece(part, s) {
		return part
	}
	return CopyValue(part)
}

// CopyValue returns a copy of s as a boxed result. A short string's bytes
// go in the same allocation as its boxed header, so the copy costs no
// extra allocation.
func CopyValue(s string) any {
	switch n := len(s); {
	case n <= 16:
		p := new(piece16)
		return boxPiece(&p.s, p.b[:n], s)
	case n <= 32:
		p := new(piece32)
		return boxPiece(&p.s, p.b[:n], s)
	case n <= 48:
		p := new(piece48)
		return boxPiece(&p.s, p.b[:n], s)
	}
	return strings.Clone(s)
}

// A pieceN holds a boxed string header and the bytes it points to. Their
// sizes are Go size classes (32, 48 and 64 B).
type (
	piece16 struct {
		s string
		b [16]byte
	}
	piece32 struct {
		s string
		b [32]byte
	}
	piece48 struct {
		s string
		b [48]byte
	}
)

// eface is the layout of an empty interface value: its type and a pointer
// to the boxed value (a string is always boxed by pointer). The exec row
// sizer reads the same layout.
type eface struct {
	typ, data unsafe.Pointer
}

// boxPiece copies part into b and returns the string header at hdr, which
// points at b, as an interface value. hdr and b are in one object, so the
// GC keeps both alive while the value is.
func boxPiece(hdr *string, b []byte, part string) any {
	copy(b, part)
	*hdr = unsafe.String(unsafe.SliceData(b), len(b))
	v := any("")
	(*eface)(unsafe.Pointer(&v)).data = unsafe.Pointer(hdr)
	return v
}
