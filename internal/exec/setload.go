package exec

import (
	"context"

	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec/aggstate"
	"github.com/gfronza/twinhouse/internal/memtrack"
)

// memberLoaders bounds how many stored sets one aggregation loads at once.
// Loads overlap their storage reads; their members are added one set at a
// time, in input order.
const memberLoaders = 8

// unknownMemberBytes estimates a loaded member whose type does not fix its
// size (a string, a tuple of several arguments).
const unknownMemberBytes = 32

// maxEstimatedMembers caps the member count an estimate multiplies, so a
// corrupt SetSize cannot overflow it (the reservation still fails).
const maxEstimatedMembers = 1 << 40

// memberLoad is one pending set state being loaded.
type memberLoad struct {
	call *aggCall
	st   *acc
	i    int // index in st.pending
	p    *chtype.AggPartial
	held int64 // what queue charged for p
	est  int64 // reserved for the loaded members until they are added
	m    []any
	err  error
	done chan struct{}
}

// loadMembers loads the stored set members that the groups' results need.
// A load failure then fails the query before any row is out, even when the
// consumer stops early or the failing group lies past the LIMIT.
func loadMembers(ac *aggContext, order []*group) error {
	var loads []*memberLoad
	for _, g := range order {
		for j, c := range ac.calls {
			st := g.accs[j]
			if !aggstate.IsSetFunc(c.spec.base) || c.countsPending(st) {
				continue
			}
			for i, p := range st.pending {
				if p != nil {
					loads = append(loads, &memberLoad{call: c, st: st, i: i, p: p})
				}
			}
		}
	}
	if len(loads) > 1 {
		if err := runMemberLoads(loads); err != nil {
			return err
		}
	}
	// Loads a lone state, and releases the emptied pending lists.
	for _, g := range order {
		for j, c := range ac.calls {
			if err := c.load(g.accs[j]); err != nil {
				return err
			}
		}
	}
	return nil
}

// runMemberLoads loads at most memberLoaders sets at once and adds each to
// its group in input order. Before a load starts, the bytes its members
// will take are estimated from SetSize and reserved, so the sets loaded
// and not yet added count against the memory limit. It returns once no
// load is running.
func runMemberLoads(loads []*memberLoad) (err error) {
	ctx, cancel := context.WithCancel(loads[0].call.ctx)
	started := 0
	defer func() {
		cancel()
		for _, l := range loads[:started] {
			<-l.done
		}
		for _, l := range loads {
			l.st.mem.Shrink(l.est)
			l.est = 0
		}
	}()
	start := func() error {
		l := loads[started]
		if l.st.mem != nil {
			l.held = memtrack.Value(l.p)
			l.est = l.call.loadedSetBytes(l.p)
			if err := l.st.mem.Grow(l.est); err != nil {
				return err
			}
		}
		l.done = make(chan struct{})
		started++
		go l.run(ctx)
		return nil
	}
	for next, l := range loads {
		for started < len(loads) && started-next < memberLoaders {
			if err := start(); err != nil {
				return err
			}
		}
		<-l.done
		if l.err != nil {
			return l.err
		}
		l.st.mem.Shrink(l.est)
		l.est = 0
		l.st.pending[l.i] = nil
		m := l.m
		l.m = nil
		if err := l.st.addMembers(m); err != nil {
			return err
		}
		if err := l.st.charge(-l.held); err != nil {
			return err
		}
	}
	return nil
}

func (l *memberLoad) run(ctx context.Context) {
	defer close(l.done)
	defer recoverInternal(&l.err)
	l.m, l.err = aggstate.Members(ctx, l.p)
}

// loadedSetBytes estimates the bytes p's members take once loaded, as
// addMembers charges them: the slice and each member's value.
func (a *aggCall) loadedSetBytes(p *chtype.AggPartial) int64 {
	per := int64(unknownMemberBytes)
	if len(a.argTypes) == 1 {
		if n, ok := memtrack.FixedValue(a.argTypes[0]); ok {
			per = n
		}
	}
	n := min(p.SetSize, maxEstimatedMembers)
	return memtrack.Slice(int(n)) + int64(n)*per
}
