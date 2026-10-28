package exec

import (
	"io"
	"unsafe"

	"github.com/gfronza/twinhouse/internal/memtrack"
)

// sortedRow is a row a limited full sort holds, with its own charge: what
// it was charged beyond its header, less what it shares with later rows.
type sortedRow struct {
	row     []any
	charged int64
}

// sortedRowSlot is a sortedRow's slot in a sort's array.
const sortedRowSlot = int64(unsafe.Sizeof(sortedRow{}))

// sortCharge charges a limited full sort's rows, like rowSizer.size. Each
// row is charged its own part, which it releases when it is read out. A
// value the memo finds again in a later row is shared: its charge moves
// from its first holder to the sort, which releases it on close.
type sortCharge struct {
	rowCharge
	// memo follows each sized column's memo (strs, then others).
	memo []heldMemo
}

// heldMemo is, for a column's remembered boxes, the row each was charged
// to (noRow once its charge moved to the sort) and that charge; and for a
// String-like column the row charged the last long value's bytes.
type heldMemo struct {
	rows    [recentValues]int
	sizes   [recentValues]int64
	dataRow int
}

// noRow is a heldMemo row whose charge is the sort's.
const noRow = -1

func newSortCharge(ch rowCharge) *sortCharge {
	s := &sortCharge{rowCharge: ch}
	if ch.sz != nil {
		s.memo = make([]heldMemo, len(ch.sz.strs)+len(ch.sz.others))
		for k := range s.memo {
			s.memo[k] = heldMemo{rows: [recentValues]int{noRow, noRow, noRow, noRow}, dataRow: noRow}
		}
	}
	return s
}

// hold appends row to the sort's rows and charges it.
func (s *sortCharge) hold(rows []sortedRow, row []any) ([]sortedRow, error) {
	sz, at := s.sz, len(rows)
	n := sz.slots(row) - sliceHeader
	for k := range sz.strs {
		n += s.str(rows, &sz.strs[k], &s.memo[k], &row[sz.strs[k].i], at)
	}
	for k := range sz.others {
		n += s.other(rows, &sz.others[k], &s.memo[len(sz.strs)+k], &row[sz.others[k].i], at)
	}
	rows, err := appendCharged(s.acct, rows, sortedRow{row: row, charged: n}, sortedRowSlot)
	if err != nil {
		return rows, err
	}
	return rows, s.acct.Grow(n)
}

// str is what row at is charged for the String-like value v.
func (s *sortCharge) str(rows []sortedRow, c *sizedColumn, m *heldMemo, v *any, at int) int64 {
	box, j := c.find(v)
	if j >= 0 {
		m.share(rows, j)
		return 0
	}
	str, _ := (*v).(string) // NULL sizes as ""
	n := memtrack.String(str)
	if len(str) >= memtrack.TinyBlock {
		data := uintptr(unsafe.Pointer(unsafe.StringData(str)))
		if data == c.data && len(str) == c.n {
			// A new box over the last value's bytes: they are shared.
			m.shareData(rows, n-memtrack.StringHeader)
			n = memtrack.StringHeader
		} else {
			c.data, c.n, m.dataRow = data, len(str), at
		}
	}
	c.remember(box)
	m.remember(at, n)
	return n
}

// other is what row at is charged for v, of a column not String-like.
func (s *sortCharge) other(rows []sortedRow, c *sizedColumn, m *heldMemo, v *any, at int) int64 {
	box, j := c.find(v)
	if j >= 0 {
		m.share(rows, j)
		return 0
	}
	n := memtrack.Value(*v)
	c.remember(box)
	m.remember(at, n)
	return n
}

// find returns v's box and its position among c's recent boxes, or -1.
func (c *sizedColumn) find(v *any) (uintptr, int) {
	box := (*[2]uintptr)(unsafe.Pointer(v))[1]
	for j, b := range c.boxes {
		if b == box {
			return box, j
		}
	}
	return box, -1
}

// remember adds the charge of a box just remembered, to row.
func (m *heldMemo) remember(row int, n int64) {
	r, z := &m.rows, &m.sizes
	r[0], r[1], r[2], r[3] = row, r[0], r[1], r[2]
	z[0], z[1], z[2], z[3] = n, z[0], z[1], z[2]
}

// share moves the charge of remembered box j to the sort, once.
func (m *heldMemo) share(rows []sortedRow, j int) {
	r := m.rows[j]
	if r == noRow {
		return
	}
	rows[r].charged -= m.sizes[j]
	m.rows[j] = noRow
	if r == m.dataRow {
		// The value's bytes went with it.
		m.dataRow = noRow
	}
}

// shareData moves the n bytes of the column's last long value to the
// sort, once.
func (m *heldMemo) shareData(rows []sortedRow, n int64) {
	r := m.dataRow
	if r == noRow {
		return
	}
	m.dataRow = noRow
	rows[r].charged -= n
	// The first box over them no longer holds their charge.
	for j := range m.rows {
		if m.rows[j] == r {
			m.sizes[j] -= n
		}
	}
}

// sortedIter returns a limited full sort's rows in order. Each row it
// returns is the reader's: the iterator drops it and releases its own
// charge at the next call, as a reader that streams (ARRAY JOIN, a join
// probe) may hold it until then. So a reader that keeps the rows (a
// collect) does not hold them charged twice. close releases the values
// rows shared.
type sortedIter struct {
	rows []sortedRow
	pos  int
	acct *memtrack.Account
	// out is the charge of the row returned last.
	out int64
}

func (s *sortedIter) next() ([]any, error) {
	s.acct.Shrink(s.out)
	s.out = 0
	if s.pos >= len(s.rows) {
		return nil, io.EOF
	}
	r := &s.rows[s.pos]
	row := r.row
	s.out = r.charged
	*r = sortedRow{}
	s.pos++
	return row, nil
}

func (s *sortedIter) close() { s.acct.Close() }

// topNIter returns a limited top-N's kept rows in order, dropping each
// from the top-N at the next call (see sortedIter), so its charge is
// released exactly.
type topNIter struct {
	rows [][]any
	pos  int
	t    *topN
	// out is the row returned last.
	out []any
}

func (it *topNIter) next() ([]any, error) {
	it.release()
	if it.pos >= len(it.rows) {
		return nil, io.EOF
	}
	row := it.rows[it.pos]
	it.rows[it.pos], it.out = nil, row
	it.pos++
	return row, nil
}

// release drops the row returned last.
func (it *topNIter) release() {
	if t := it.t; it.out != nil && t.fixed {
		t.ch.acct.Shrink(t.ch.sz.slots(it.out) - sliceHeader)
	} else if it.out != nil {
		t.drop(it.out)
	}
	it.out = nil
}

// skip passes over up to n rows (an OFFSET) and drops them at once.
func (it *topNIter) skip(n uint64) uint64 {
	it.release()
	rows := it.rows[it.pos:]
	rows = rows[:min(n, uint64(len(rows)))]
	if t := it.t; t.fixed {
		var b int64
		for _, r := range rows {
			b += t.ch.sz.slots(r) - sliceHeader
		}
		t.ch.acct.Shrink(b)
	} else {
		for _, r := range rows {
			t.drop(r)
		}
	}
	clear(rows)
	it.pos += len(rows)
	return uint64(len(rows))
}

func (it *topNIter) close() { it.t.ch.acct.Close() }

// sharedValue is a value held top-N rows may share: how many hold it, and
// what it is charged.
type sharedValue struct {
	refs, size int64
}

// sharedEntry is a sharedValue's entry in its map, keyed by the value's box.
var sharedEntry = memtrack.MapEntry(8, int64(unsafe.Sizeof(sharedValue{})))

// charge charges what row holds beyond its header: its slice, fixed-size
// values and strings (chargeStrings), and each array, tuple or map once,
// with the first held row that holds it; drop releases it with the last.
// So values rows share (a folded constant, an ARRAY JOIN source array) are
// not charged per row, and an evicted row never takes a value others still
// hold with it.
func (t *topN) charge(row []any) error {
	if t.ch.sz == nil {
		return nil
	}
	n, err := t.chargeStrings(row)
	if err != nil {
		return err
	}
	if err := t.ch.acct.Grow(t.ch.sz.slots(row) - sliceHeader + n); err != nil {
		return err
	}
	return t.shareValues(row)
}

// chargeStrings returns what row's strings are charged to it. A string a
// recent held row holds too (the column's memo) is shared instead: counted
// in t.strs and charged once (shareString).
func (t *topN) chargeStrings(row []any) (int64, error) {
	n := int64(0)
	for k := range t.ch.sz.strs {
		c := &t.ch.sz.strs[k]
		s, _ := row[c.i].(string) // NULL sizes as ""
		if len(s) < memtrack.TinyBlock {
			n += memtrack.String(s)
			continue
		}
		box, recent := c.recent(&row[c.i])
		if !recent && !t.sharedString(box) {
			c.remember(box)
			n += memtrack.String(s)
			continue
		}
		if err := t.shareString(box, s); err != nil {
			return 0, err
		}
	}
	return n, nil
}

// sharedString reports whether held rows share the string whose box is box.
func (t *topN) sharedString(box uintptr) bool {
	if len(t.strs) == 0 {
		return false
	}
	_, ok := t.strs[box]
	return ok
}

// shareString counts one more held row holding s, whose box is box. The
// first time, the row the memo remembers it from, still held, was charged
// s: that charge moves to the count.
func (t *topN) shareString(box uintptr, s string) error {
	if t.strs == nil {
		t.strs = map[uintptr]sharedValue{}
	}
	e, ok := t.strs[box]
	if !ok {
		e = sharedValue{refs: 1, size: memtrack.String(s) + sharedEntry}
		if err := t.ch.acct.Grow(sharedEntry); err != nil {
			return err
		}
	}
	e.refs++
	t.strs[box] = e
	return nil
}

// dropStrings returns what row's strings release: those charged to it.
func (t *topN) dropStrings(row []any) int64 {
	n := int64(0)
	for k := range t.ch.sz.strs {
		c := &t.ch.sz.strs[k]
		s, _ := row[c.i].(string)
		if len(s) < memtrack.TinyBlock {
			n += memtrack.String(s)
			continue
		}
		box, _ := shareable(&row[c.i])
		if t.sharedString(box) {
			if t.unshare(t.strs, box) {
				t.forgetString(box)
			}
			continue
		}
		t.forgetString(box)
		n += memtrack.String(s)
	}
	return n
}

// forgetString drops box from every String column's memo once a held row
// it was charged to, or its count, goes: no memo may remember a box no
// held row was charged for (its memory may be reused), whichever column
// charged it.
func (t *topN) forgetString(box uintptr) {
	for k := range t.ch.sz.strs {
		t.ch.sz.strs[k].forget(box)
	}
}

// shareValues counts row as one more holder of each value it may share.
func (t *topN) shareValues(row []any) error {
	for _, c := range t.ch.sz.others {
		if box, ok := shareable(&row[c.i]); ok {
			if err := t.share(box, row[c.i]); err != nil {
				return err
			}
		}
	}
	return nil
}

// share counts one more held row holding v, whose box is box.
func (t *topN) share(box uintptr, v any) error {
	if t.shared == nil {
		t.shared = map[uintptr]sharedValue{}
	}
	e, ok := t.shared[box]
	if !ok {
		e.size = memtrack.Value(v) + sharedEntry
		if err := t.ch.acct.Grow(e.size); err != nil {
			return err
		}
	}
	e.refs++
	t.shared[box] = e
	return nil
}

// drop releases a row the top-N no longer holds (evicted, or read out): its
// own part, and the values no other held row holds.
func (t *topN) drop(row []any) {
	if t.ch.sz == nil {
		return
	}
	t.ch.acct.Shrink(t.ch.sz.slots(row) - sliceHeader + t.dropStrings(row))
	for _, c := range t.ch.sz.others {
		if box, ok := shareable(&row[c.i]); ok {
			t.unshare(t.shared, box)
		}
	}
}

// unshare counts one held row fewer holding the value of m whose box is
// box, and reports whether that was the last.
func (t *topN) unshare(m map[uintptr]sharedValue, box uintptr) bool {
	e := m[box]
	if e.refs--; e.refs > 0 {
		m[box] = e
		return false
	}
	delete(m, box)
	t.ch.acct.Shrink(e.size)
	return true
}

// shareable returns the box (the interface's data word) of a value rows
// may share, and false for NULL.
func shareable(v *any) (uintptr, bool) {
	box := (*[2]uintptr)(unsafe.Pointer(v))[1]
	return box, box != 0
}
