package memtrack

import (
	"time"
	"unsafe"

	"github.com/gfronza/twinhouse/internal/chtype"
)

// Size estimates cover the canonical value set (chtype/type.go). They
// round up to the Go allocator's size classes, so they stay at or above
// what the heap really holds for the value.

// sizeClasses are the Go runtime's small-object size classes
// (internal/runtime/gc/sizeclasses.go).
var sizeClasses = [...]int64{8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224,
	240, 256, 288, 320, 352, 384, 416, 448, 480, 512, 576, 640, 704, 768, 896, 1024, 1152, 1280, 1408,
	1536, 1792, 2048, 2304, 2688, 3072, 3200, 3456, 4096, 4864, 5376, 6144, 6528, 6784, 6912, 8192,
	9472, 9728, 10240, 10880, 12288, 13568, 14336, 16384, 18432, 19072, 20480, 21760, 24576, 27264,
	28672, 32768}

const (
	maxSmallSize = 32768
	pageSize     = 8192
	// Objects holding pointers above mallocHeaderMin bytes carry an
	// 8-byte type header.
	mallocHeaderMin = 512
)

// classBySize maps (n+7)/8 to n's size class, for n up to 1024; classBy128
// maps (n-1024+127)/128 above that.
var classBySize, classBy128 = sizeClassTables()

func sizeClassTables() (small [1024/8 + 1]int64, large [(maxSmallSize-1024)/128 + 1]int64) {
	c := 0
	for i := range small {
		for sizeClasses[c] < int64(i*8) {
			c++
		}
		small[i] = sizeClasses[c]
	}
	for i := range large {
		for sizeClasses[c] < int64(1024+i*128) {
			c++
		}
		large[i] = sizeClasses[c]
	}
	return small, large
}

// Alloc is the heap bytes an allocation of n bytes takes.
func Alloc(n int64) int64 {
	switch {
	case n <= 0:
		return 0
	case n <= 1024:
		return classBySize[(n+7)/8]
	case n <= maxSmallSize:
		return classBy128[(n-1024+127)/128]
	}
	return (n + pageSize - 1) / pageSize * pageSize
}

// TinyBlock is the block of Go's tiny allocator. Pointer-free objects under
// 16 bytes (a boxed number, short string bytes) are packed into one, often
// next to garbage, and a live one keeps the whole block alive.
const TinyBlock = 16

// Bytes is Alloc for a pointer-free object, counting a tiny one as its
// whole block.
func Bytes(n int64) int64 {
	if n > 0 && n < TinyBlock {
		return TinyBlock
	}
	return Alloc(n)
}

// allocPointers is Alloc for an object that holds pointers.
func allocPointers(n int64) int64 {
	if n > mallocHeaderMin {
		n += 8
	}
	return Alloc(n)
}

// Slice is the bytes of a boxed []any of capacity c: its header and
// backing array, not the elements' own allocations.
func Slice(c int) int64 { return 24 + allocPointers(16*int64(c)) }

// Value is the heap bytes reachable from v beyond its own 16-byte slot.
func Value(v any) int64 {
	switch x := v.(type) {
	case nil, bool:
		return 0
	case uint64, int64, float64:
		return TinyBlock
	case string:
		return String(x)
	case time.Time:
		return 24
	case []any:
		return Slice(cap(x)) + values(x)
	case *chtype.AggPartial:
		return aggPartial(x)
	}
	return 32
}

// StringHeader is the boxed header of a non-empty string value.
const StringHeader = 16

// String is Value of a string: its boxed header and bytes.
func String(s string) int64 {
	if s == "" {
		return 0
	}
	return StringHeader + Bytes(int64(len(s)))
}

// FixedValue is Value of every value of a column of type t, when that does
// not depend on the value: numbers, Bool and dates and times (NULL takes
// less).
func FixedValue(t chtype.Type) (int64, bool) {
	switch k := t.Unwrap().Kind; {
	case k == chtype.Bool || k == chtype.Nothing:
		return 0, true
	case t.IsNumeric():
		return TinyBlock, true
	case t.IsTime():
		return 24, true
	}
	return 0, false
}

// Row is the bytes a row keeps alive: its slice header (in the slice that
// holds it), backing array and values.
func Row(r []any) int64 { return Slice(cap(r)) + values(r) }

func values(vs []any) int64 {
	n := int64(0)
	for _, v := range vs {
		n += Value(v)
	}
	return n
}

// AggPartialBytes is the size of an aggregate state's struct.
var AggPartialBytes = allocPointers(int64(unsafe.Sizeof(chtype.AggPartial{})))

func aggPartial(p *chtype.AggPartial) int64 {
	n := AggPartialBytes + Value(p.Sum) + Value(p.Value) + Value(p.Key)
	if p.LoadSet != nil && !p.SetLoaded {
		// The store sizes what its closure holds.
		n += int64(p.LoadSetBytes)
	}
	if p.Set != nil {
		n += allocPointers(16*int64(cap(p.Set))) + values(p.Set)
	}
	if p.List != nil {
		n += allocPointers(16*int64(cap(p.List))) + values(p.List)
	}
	return n
}

// MapEntry is the bytes one entry takes in a Go map whose key and value
// slot is slot bytes: the slot and its control byte at the lowest load a
// table has after it grows (7/16).
func MapEntry(slot int64) int64 { return (slot+1)*16/7 + 1 }
