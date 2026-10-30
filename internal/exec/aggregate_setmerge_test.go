package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec/aggstate"
)

// setMergeCall compiles name (an -Merge of a set function over String
// members) and counts the LoadSet calls of the states it makes.
type setMergeCall struct {
	call  *aggCall
	loads []string
}

func newSetMergeCall(t *testing.T, name, state string) *setMergeCall {
	t.Helper()
	te := newTestEnv(t)
	q := te.e.newQuery(context.Background(), &Request{})
	spec, _ := parseAggName(name)
	call, err := newAggCall(q.ctx, spec, name, []*expr{{t: chtype.MustParse("AggregateFunction(" + state + ", String)")}})
	if err != nil {
		t.Fatal(err)
	}
	return &setMergeCall{call: call}
}

// unloaded is a stored state of members whose LoadSet records id.
func (s *setMergeCall) unloaded(id string, members ...any) *chtype.AggPartial {
	p := aggstate.New(s.call.spec.base, []chtype.Type{chtype.TString})
	p.SetSize, p.SetLoaded = uint64(len(members)), false
	p.LoadSet = func(context.Context) ([]any, error) {
		s.loads = append(s.loads, id)
		return append([]any(nil), members...), nil
	}
	return p
}

func (s *setMergeCall) loaded(members ...any) *chtype.AggPartial {
	p := aggstate.New(s.call.spec.base, []chtype.Type{chtype.TString})
	p.Set, p.SetSize, p.SetLoaded = members, uint64(len(members)), true
	return p
}

func (s *setMergeCall) merge(t *testing.T, states ...*chtype.AggPartial) *acc {
	t.Helper()
	st := s.call.newAcc()
	for _, p := range states {
		if err := s.call.mergeState(st, p); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

func (s *setMergeCall) result(t *testing.T, st *acc) string {
	t.Helper()
	if err := s.call.load(st); err != nil {
		t.Fatal(err)
	}
	v, err := s.call.result(st)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprint(v)
}

// Unloaded states merged into a group wait until the result needs their
// members, then load once each, in input order.
func TestUnloadedSetStatesLoadWhenResultNeedsThem(t *testing.T) {
	s := newSetMergeCall(t, "groupUniqArrayMerge", "groupUniqArray")
	st := s.merge(t, s.unloaded("a", "x", "y"), s.unloaded("b", "y", "z"), s.unloaded("c", "w"))
	if len(s.loads) != 0 {
		t.Fatalf("merging loaded %v, want no loads before the result", s.loads)
	}
	if got, want := s.result(t, st), "[x y z w]"; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
	if got := fmt.Sprint(s.loads); got != "[a b c]" {
		t.Errorf("loads %s, want [a b c]", got)
	}
}

// A loaded state between unloaded ones keeps its place in the members'
// order.
func TestLoadedSetStateKeepsInputOrder(t *testing.T) {
	s := newSetMergeCall(t, "groupUniqArrayMerge", "groupUniqArray")
	st := s.merge(t, s.unloaded("a", "x"), s.loaded("y"), s.unloaded("c", "z"))
	if got, want := s.result(t, st), "[x y z]"; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

// A group that merged a single stored state is counted from its SetSize;
// any second state, loaded or not, makes the count a union.
func TestSingleSetStateCountsWithoutLoading(t *testing.T) {
	for _, c := range []struct {
		name   string
		states func(s *setMergeCall) []*chtype.AggPartial
		want   string
		loads  int
	}{
		{"one unloaded", func(s *setMergeCall) []*chtype.AggPartial {
			return []*chtype.AggPartial{s.unloaded("a", "x", "y")}
		}, "2", 0},
		{"loaded then unloaded", func(s *setMergeCall) []*chtype.AggPartial {
			return []*chtype.AggPartial{s.loaded("x", "w"), s.unloaded("a", "x", "y")}
		}, "3", 1},
		{"two unloaded", func(s *setMergeCall) []*chtype.AggPartial {
			return []*chtype.AggPartial{s.unloaded("a", "x", "y"), s.unloaded("b", "y")}
		}, "2", 2},
	} {
		s := newSetMergeCall(t, "uniqExactMerge", "uniqExact")
		st := s.merge(t, c.states(s)...)
		if got := s.result(t, st); got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
		if len(s.loads) != c.loads {
			t.Errorf("%s: %d loads, want %d", c.name, len(s.loads), c.loads)
		}
	}
}
