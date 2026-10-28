package exec

import (
	"unsafe"

	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec/aggstate"
	"github.com/gfronza/twinhouse/internal/memtrack"
)

// What GROUP BY charges: each group's own bytes while it waits to be
// emitted, the groups map until the input ends, and the states' growth
// (set members, list elements, kept values) with their group.

// stateBytes is an acc and its AggPartial.
var stateBytes = memtrack.Alloc(int64(unsafe.Sizeof(acc{}))) + memtrack.AggPartialBytes

// aggCharge charges one aggregation's groups to its account. Its methods do
// nothing for an unlimited query (nil account).
type aggCharge struct {
	acct *memtrack.Account
	// group is a new group's bytes that do not depend on its keys.
	group int64
	// index is what the groups map holds: entries and key strings.
	index int64
}

func newAggCharge(acct *memtrack.Account, ac *aggContext) aggCharge {
	if acct == nil {
		return aggCharge{}
	}
	n := memtrack.Alloc(int64(unsafe.Sizeof(group{}))) + memtrack.Slice(len(ac.keys)) - sliceHeader +
		memtrack.Pointers(len(ac.calls))
	for _, c := range ac.calls {
		n += stateBytes + c.kept
	}
	return aggCharge{acct: acct, group: n}
}

// newGroup charges g, and the growth of the order array from capacity
// from to to.
func (c *aggCharge) newGroup(g *group, from, to int) error {
	if c.acct == nil {
		return nil
	}
	g.mem = c.group
	for _, k := range g.keys {
		g.mem += memtrack.Value(k)
	}
	return c.acct.Grow(g.mem + memtrack.Pointers(to) - memtrack.Pointers(from))
}

// indexed charges a groups map entry whose key is keyLen bytes long.
func (c *aggCharge) indexed(keyLen int) error {
	if c.acct == nil {
		return nil
	}
	n := keyEntry(indexEntry, keyLen)
	c.index += n
	return c.acct.Grow(n)
}

// built releases the groups map, which the aggregation drops once its
// input ends.
func (c *aggCharge) built() {
	c.acct.Shrink(c.index)
	c.index = 0
}

// bytes is what g and its states are charged.
func (g *group) bytes() int64 {
	n := g.mem
	for _, st := range g.accs {
		n += st.bytes
	}
	return n
}

// fixedSize reports whether every value of each type takes the same
// memory.
func fixedSize(ts []chtype.Type) bool {
	for _, t := range ts {
		if _, ok := memtrack.FixedValue(t); !ok {
			return false
		}
	}
	return true
}

// fixedKeptBytes is what a new state of a is charged for the values it
// keeps when their size does not vary: a boxed sum, or the kept value and
// key of fixed-size arguments. Values that vary are charged by keep.
func (a *aggCall) fixedKeptBytes() int64 {
	switch a.spec.base {
	case "sum", "avg":
		return memtrack.TinyBlock
	case "min", "max", "any", "anyLast", "argMin", "argMax":
		if a.sized {
			return 0
		}
		n := int64(0)
		for _, t := range a.argTypes {
			v, _ := memtrack.FixedValue(t)
			n += v
		}
		return n
	}
	return 0
}

// keptBytes is the size of the value and key p keeps, when it varies.
func (a *aggCall) keptBytes(p *chtype.AggPartial) int64 {
	if !a.sized {
		return 0
	}
	return memtrack.Value(p.Value) + memtrack.Value(p.Key)
}

// keep makes v (with key k) the state's kept value and charges the change
// in size.
func (a *aggCall) keep(st *acc, v, k any) error {
	p := st.p
	if st.mem == nil || !a.sized {
		p.Value, p.Key, p.HasValue = v, k, true
		return nil
	}
	old := a.keptBytes(p)
	p.Value, p.Key, p.HasValue = v, k, true
	return st.charge(a.keptBytes(p) - old)
}

// mergeValues merges a state of a function without a set into st and
// charges what that adds: list elements, or a kept value of another size.
func (a *aggCall) mergeValues(st *acc, in *chtype.AggPartial) error {
	p := st.p
	if st.mem == nil {
		return aggstate.Merge(a.ctx, p, in)
	}
	n, c, old := len(p.List), cap(p.List), a.keptBytes(p)
	if err := aggstate.Merge(a.ctx, p, in); err != nil {
		return err
	}
	added := a.keptBytes(p) - old + memtrack.Slice(cap(p.List)) - memtrack.Slice(c)
	for _, v := range p.List[n:] {
		added += memtrack.Value(v)
	}
	return st.charge(added)
}

// charge adds n (which may be negative) to what st holds.
func (st *acc) charge(n int64) error {
	st.bytes += n
	return charge(st.mem, n)
}

// appendList appends v to a groupArray state.
func (st *acc) appendList(v any) (err error) {
	if st.mem == nil {
		st.p.List = append(st.p.List, v)
		return nil
	}
	st.p.List, err = st.appendValue(st.p.List, v)
	return err
}

// addMembers adds loaded set members to st. The loaded slice is charged
// while they are copied in.
func (st *acc) addMembers(m []any) error {
	held := int64(0)
	if st.mem != nil {
		held = memtrack.Row(m)
		if err := st.mem.Grow(held); err != nil {
			return err
		}
	}
	for _, v := range m {
		if err := st.addMember(v); err != nil {
			return err
		}
	}
	st.mem.Shrink(held)
	return nil
}

// appendValue appends v to s, a slice of st's state, and charges st v and
// the growth of s's array. A grown array is charged before append
// allocates it, while the old one is still live, and the old one is
// released after the copy.
func (st *acc) appendValue(s []any, v any) ([]any, error) {
	n := memtrack.Value(v)
	if len(s) < cap(s) {
		return append(s, v), st.charge(n)
	}
	old, next := memtrack.Slice(cap(s)), memtrack.Slice(grownCap(cap(s)))
	if err := st.charge(next); err != nil {
		return s, err
	}
	s = append(s, v)
	return s, st.charge(n + memtrack.Slice(cap(s)) - next - old)
}
