package exec

import (
	"unsafe"

	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/memtrack"
)

// constCharge charges the constants a query's plans hold: literals and
// folded expressions, which live in the compiled plan until the query ends.
// Many large ones (`has(range(1000000), x) AND has(range(1000001), x)`)
// then fail with 241 while they are planned. A value several constants
// share (one box) is charged once. The values charged are kept here, so a
// box is never reused while it counts as charged. Constant IN sets are
// kept with the plan that built them too, so the filter and key pushdown
// share one, and a plan compiled again (a view, for each block) does not
// add one each time.
type constCharge struct {
	acct *memtrack.Account
	// held are the boxes charged, innermost expression last.
	held   []uintptr
	values map[uintptr]heldConst
	// sets are the constant IN sets held, newest last, and byKey finds
	// them.
	sets  []plannedSet
	byKey map[inSetKey]*valueSet
}

// constMark is a position release goes back to.
type constMark struct{ held, sets int }

// plannedSet is a constant IN set a plan holds, and what it is charged.
type plannedSet struct {
	key  inSetKey
	size int64
}

// heldConst is a charged constant: its value, how many held entries name
// it, and its charge.
type heldConst struct {
	v          any
	refs, size int64
}

// constEntry is a heldConst's entry in its map, keyed by the value's box.
var constEntry = memtrack.MapEntry(8, int64(unsafe.Sizeof(heldConst{})))

// mark returns the position release goes back to.
func (c *constCharge) mark() constMark { return constMark{held: len(c.held), sets: len(c.sets)} }

// settle accounts x for the constants and sets charged since mark: those
// of its arguments. Any expression other than a constant holds them. A
// constant was folded from them, so it replaces them.
func (c *constCharge) settle(q *query, mark constMark, x *expr) error {
	if !x.konst {
		return nil
	}
	n := len(c.held)
	if err := c.hold(q, x.val); err != nil {
		return err
	}
	for _, box := range c.held[mark.held:n] {
		c.unref(box)
	}
	c.held = append(c.held[:mark.held], c.held[n:]...)
	c.dropSets(mark.sets)
	return nil
}

// hold charges v, unless its box is charged already.
func (c *constCharge) hold(q *query, v any) error {
	box := boxOf(v)
	if box == 0 || !c.start(q) {
		return nil
	}
	e, ok := c.values[box]
	if !ok {
		e = heldConst{v: v, size: memtrack.Value(v) + constEntry}
		if err := c.acct.Grow(e.size); err != nil {
			return err
		}
	}
	e.refs++
	c.values[box] = e
	c.held = append(c.held, box)
	return nil
}

// pin marks v as charged elsewhere (a scalar subquery's value, charged
// with the subquery's cached rows), so constants with its box are not
// charged.
func (c *constCharge) pin(q *query, v any) {
	box := boxOf(v)
	if box == 0 || !c.start(q) {
		return
	}
	if _, ok := c.values[box]; !ok {
		c.values[box] = heldConst{v: v, refs: 1}
	}
}

// start readies c to charge, and reports false for an unlimited query.
func (c *constCharge) start(q *query) bool {
	if c.acct == nil {
		if c.acct = q.lifetimeAccount(); c.acct == nil {
			return false
		}
		c.values = map[uintptr]heldConst{}
	}
	return true
}

// boxOf is v's box: its interface data word.
func boxOf(v any) uintptr { return (*[2]uintptr)(unsafe.Pointer(&v))[1] }

// unref drops one entry naming box, and its charge with the last.
func (c *constCharge) unref(box uintptr) {
	e := c.values[box]
	if e.refs--; e.refs > 0 {
		c.values[box] = e
		return
	}
	delete(c.values, box)
	c.acct.Shrink(e.size)
}

// release drops the constants and sets charged since mark, for a caller
// that drops what it compiled then.
func (c *constCharge) release(mark constMark) {
	for _, box := range c.held[mark.held:] {
		c.unref(box)
	}
	c.held = c.held[:mark.held]
	c.dropSets(mark.sets)
}

// set returns the constant IN set of key that a plan holds, or builds one
// of type to with fill, charged and held from now on.
func (c *constCharge) set(q *query, key inSetKey, to chtype.Type, fill func(*valueSet) error) (*valueSet, error) {
	if s, ok := c.byKey[key]; ok {
		return s, nil
	}
	var acct *memtrack.Account
	if c.start(q) {
		acct = c.acct
	}
	s := newValueSet(to, acct)
	before := acct.Charged()
	if err := fill(s); err != nil {
		acct.Shrink(acct.Charged() - before)
		return nil, err
	}
	if c.byKey == nil {
		c.byKey = map[inSetKey]*valueSet{}
	}
	c.byKey[key] = s
	c.sets = append(c.sets, plannedSet{key: key, size: acct.Charged() - before})
	return s, nil
}

// dropSets drops the sets held since position from.
func (c *constCharge) dropSets(from int) {
	for _, p := range c.sets[from:] {
		delete(c.byKey, p.key)
		c.acct.Shrink(p.size)
	}
	c.sets = c.sets[:from]
}
