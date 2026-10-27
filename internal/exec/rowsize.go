package exec

import (
	"unsafe"

	"github.com/gfronza/twinhouse/internal/format"
	"github.com/gfronza/twinhouse/internal/memtrack"
)

// sliceHeader is the size of a []any header.
const sliceHeader = 24

// rowSizer estimates the rows one holder keeps, like memtrack.Row. Columns
// of fixed-size types cost the same in every row, so they are summed once.
// A value a recent row holds too (the same allocation: an ARRAY JOIN source
// column, a folded constant, one of if's branches, a shared state) is
// charged only its slot. Only append-only holders may use it: dropping the
// row that owned a shared value would leave the estimate short.
type rowSizer struct {
	width int
	// fixed is the fixed-size columns' values; base adds a row of width
	// columns' slice.
	fixed, base int64
	// strs are the String-like columns, others the rest.
	strs, others []sizedColumn
}

// recentValues is how many boxes a column remembers. It must be 4, because
// recent compares them unrolled.
const recentValues = 4

// sizedColumn is a column whose values differ in size. It remembers the
// boxes (interface data words) of its recent values and, for a String-like
// column, the last long value's bytes, apart from the box that holds them.
// They are only compared, so they are not pointers the GC sees.
type sizedColumn struct {
	i     int
	boxes [recentValues]uintptr
	data  uintptr
	n     int
}

func newRowSizer(cols []format.Column) *rowSizer {
	s := &rowSizer{width: len(cols)}
	for i, c := range cols {
		if n, ok := memtrack.FixedValue(c.Type); ok {
			s.fixed += n
		} else if c.Type.IsString() {
			s.strs = append(s.strs, sizedColumn{i: i})
		} else {
			s.others = append(s.others, sizedColumn{i: i})
		}
	}
	s.base = memtrack.Slice(s.width) + s.fixed
	return s
}

// size is memtrack.Row(r), with fixed-size columns at their type's size,
// less what r shares with recent rows. It runs for every row a holder
// keeps, so the String-like columns are written out here.
func (s *rowSizer) size(r []any) int64 {
	n := s.base
	if cap(r) != s.width {
		n = memtrack.Slice(cap(r)) + s.fixed
	}
	for k := range s.strs {
		c := &s.strs[k]
		v := &r[c.i]
		box, held := c.recent(v)
		if held {
			continue
		}
		c.remember(box)
		str, _ := (*v).(string) // NULL sizes as ""
		if len(str) < memtrack.TinyBlock {
			// Its bytes take a tiny block: the memo would save no
			// more than that.
			n += memtrack.String(str)
			continue
		}
		data := uintptr(unsafe.Pointer(unsafe.StringData(str)))
		if data == c.data && len(str) == c.n {
			// A new box over the last value's bytes (a function
			// that returns its argument).
			n += memtrack.StringHeader
			continue
		}
		c.data, c.n = data, len(str)
		n += memtrack.String(str)
	}
	if len(s.others) > 0 {
		n += s.othersSize(r)
	}
	return n
}

// othersSize is size's part for the columns that are not String-like.
func (s *rowSizer) othersSize(r []any) int64 {
	n := int64(0)
	for k := range s.others {
		c := &s.others[k]
		if box, held := c.recent(&r[c.i]); !held {
			c.remember(box)
			n += memtrack.Value(r[c.i])
		}
	}
	return n
}

// recent returns v's box (the interface's data word) and whether a recent
// row holds it.
func (c *sizedColumn) recent(v *any) (uintptr, bool) {
	box, b := (*[2]uintptr)(unsafe.Pointer(v))[1], &c.boxes
	return box, b[0] == box || b[1] == box || b[2] == box || b[3] == box
}

// remember adds box as the most recent one, dropping the oldest.
func (c *sizedColumn) remember(box uintptr) {
	b := &c.boxes
	b[0], b[1], b[2], b[3] = box, b[0], b[1], b[2]
}

// forget drops what s remembers of earlier rows, once the holder has
// released them (their memory may be reused).
func (s *rowSizer) forget() {
	if s == nil {
		return
	}
	for _, cs := range [][]sizedColumn{s.strs, s.others} {
		for k := range cs {
			cs[k] = sizedColumn{i: cs[k].i}
		}
	}
}

// rowCharge is where a holder charges the rows it keeps: an account and a
// sizer for its rows. Both are nil for an unlimited query.
type rowCharge struct {
	acct *memtrack.Account
	sz   *rowSizer
}

func newRowCharge(acct *memtrack.Account, cols []format.Column) rowCharge {
	if acct == nil {
		return rowCharge{}
	}
	return rowCharge{acct: acct, sz: newRowSizer(cols)}
}

// grow appends row to a full rows and charges it. The header array append
// allocates is charged first, while the old one is still live, and the old
// one is released after the copy. A row that fits is appended inline
// (collectFirst), so that charging it makes a single call (size).
func (ch rowCharge) grow(rows [][]any, row []any) ([][]any, error) {
	if ch.acct == nil {
		return append(rows, row), nil
	}
	old, next := cap(rows), grownCap(cap(rows))
	if err := ch.acct.Grow(sliceHeader * int64(next)); err != nil {
		return rows, err
	}
	rows = append(rows, row)
	// Release the old array, less any size-class rounding of the new.
	ch.acct.Shrink(sliceHeader * int64(old+next-cap(rows)))
	return rows, ch.acct.Grow(ch.sz.size(row) - sliceHeader)
}

// grownCap is the capacity append gives a full slice of capacity c when it
// adds one element, before rounding up to a size class.
func grownCap(c int) int {
	if c < 256 {
		return max(2*c, 1)
	}
	return c + (c+3*256)/4
}
