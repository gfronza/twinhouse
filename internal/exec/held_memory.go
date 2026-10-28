package exec

import (
	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/format"
	"github.com/gfronza/twinhouse/internal/memtrack"
)

// Helpers the holders share to charge what they keep: rows, arrays that
// grow, and the entries of maps keyed by strings.

// fixedRow returns what size returns for every row of the sizer's width,
// when all its columns have fixed-size types; ok is false otherwise.
func (s *rowSizer) fixedRow() (n int64, ok bool) { return s.base, s.fixedOnly() }

// fixedOnly reports whether all the columns have fixed-size types.
func (s *rowSizer) fixedOnly() bool { return len(s.strs) == 0 && len(s.others) == 0 }

// slots is what r's slice and fixed-size values take.
func (s *rowSizer) slots(r []any) int64 {
	if cap(r) != s.width {
		return memtrack.Slice(cap(r)) + s.fixed
	}
	return s.base
}

// appendCharged appends v to s. When s is full, the array append allocates
// (slot bytes an element) is charged first, while the old one is still
// live, and the old one is released after the copy. What v itself holds
// is the caller's to charge.
func appendCharged[T any](acct *memtrack.Account, s []T, v T, slot int64) ([]T, error) {
	if len(s) < cap(s) || acct == nil {
		return append(s, v), nil
	}
	old, next := cap(s), grownCap(cap(s))
	if err := acct.Grow(slot * int64(next)); err != nil {
		return s, err
	}
	s = append(s, v)
	// Release the old array, less any size-class rounding of the new.
	acct.Shrink(slot * int64(old+next-cap(s)))
	return s, nil
}

// charge changes acct by n bytes, either way.
func charge(acct *memtrack.Account, n int64) error {
	if n < 0 {
		acct.Shrink(-n)
		return nil
	}
	return acct.Grow(n)
}

// The entries of the maps holders key by strings: a set (an empty value,
// which a slot pads to 8 bytes) and an index (a number or pointer).
var (
	setEntry   = memtrack.MapEntry(memtrack.StringHeader, 0)
	indexEntry = memtrack.MapEntry(memtrack.StringHeader, 8)
	// newSet is a set's map before its first entry.
	newSet = memtrack.NewMap(memtrack.StringHeader, 0)
)

// keyEntry is what a new key of n bytes takes in a map whose entries take
// entry bytes: the entry and the key's bytes.
func keyEntry(entry int64, n int) int64 {
	return entry + memtrack.Bytes(int64(n))
}

// heldColumns are the columns of the rows a sort holds, the values of
// xs: the selected ones, then the hidden ORDER BY and LIMIT BY values.
type heldColumns struct {
	cols []format.Column
	// skip marks the columns a sort does not size: constants, as all rows
	// hold the plan's value, which the plan is charged (constCharge), and
	// a column that returns the same input value as an earlier one
	// (`SELECT s ... ORDER BY s`), which holds the same box.
	skip []bool
}

func newHeldColumns(xs ...[]*expr) heldColumns {
	var h heldColumns
	seen := map[int]bool{}
	for _, list := range xs {
		for _, x := range list {
			h.cols = append(h.cols, format.Column{Type: x.t})
			h.skip = append(h.skip, x.konst || seen[x.slot])
			if x.slot > 0 {
				seen[x.slot] = true
			}
		}
	}
	return h
}

// charge returns where a sort charges its rows to acct.
func (h heldColumns) charge(acct *memtrack.Account) rowCharge {
	if acct == nil {
		return rowCharge{}
	}
	return rowCharge{acct: acct, sz: newRowSizerOf(h.cols, h.skip)}
}

// tableColumns are t's columns as a row sizer sees them.
func tableColumns(t *catalog.Table) []format.Column {
	cols := make([]format.Column, len(t.Columns))
	for i, c := range t.Columns {
		cols[i] = format.Column{Name: c.Name, Type: c.Type}
	}
	return cols
}
