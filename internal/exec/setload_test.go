package exec

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec/aggstate"
)

// loadSpy makes stored groupUniqArray states whose loads it tracks.
type loadSpy struct {
	running, peak, calls atomic.Int64
	// load runs inside each load, after it counts as running.
	load func(ctx context.Context, members []any) ([]any, error)
}

func (s *loadSpy) state(members ...any) *chtype.AggPartial {
	p := aggstate.New("groupUniqArray", []chtype.Type{chtype.TString})
	p.SetSize, p.SetLoaded = uint64(len(members)), false
	p.LoadSet = func(ctx context.Context) ([]any, error) {
		s.calls.Add(1)
		n := s.running.Add(1)
		defer s.running.Add(-1)
		for {
			old := s.peak.Load()
			if n <= old || s.peak.CompareAndSwap(old, n) {
				break
			}
		}
		if s.load != nil {
			return s.load(ctx, members)
		}
		return append([]any(nil), members...), nil
	}
	return p
}

// runSetMerge aggregates groupUniqArrayMerge over input rows (key, state).
func runSetMerge(t *testing.T, settings map[string]string, input [][]any) ([][]any, error) {
	t.Helper()
	te := newTestEnv(t)
	q := te.e.newQuery(context.Background(), &Request{Settings: settings})
	ac := compileAggregation(t, q, []string{"a"}, nil)
	merge := &aggCall{spec: aggSpec{base: "groupUniqArray", merge: true}, name: "groupUniqArrayMerge",
		argTypes: []chtype.Type{chtype.TString},
		args:     []*expr{{fn: func(row []any) (any, error) { return row[1], nil }}}, ctx: q.ctx}
	ac.calls = []*aggCall{merge}
	out, err := runAggregation(q, &sliceIter{rows: input}, ac)
	if err != nil {
		return nil, err
	}
	return drainRows(t, out), nil
}

// The stored sets of all groups load concurrently, at most memberLoaders
// at once, and each group's members keep their input order.
func TestMemberLoadsOverlapWithinBound(t *testing.T) {
	spy := &loadSpy{load: func(_ context.Context, m []any) ([]any, error) {
		time.Sleep(10 * time.Millisecond)
		return append([]any(nil), m...), nil
	}}
	var input [][]any
	for i := range 30 {
		input = append(input, []any{int64(i % 3), spy.state(fmt.Sprint(i), "shared")})
	}
	rows, err := runSetMerge(t, nil, input)
	if err != nil {
		t.Fatal(err)
	}
	if p := spy.peak.Load(); p < 2 || p > memberLoaders {
		t.Errorf("peak concurrent loads %d, want 2..%d", p, memberLoaders)
	}
	eq(t, spy.calls.Load(), int64(30))
	got := fmt.Sprint(rows)
	want := "[[0 [0 shared 3 6 9 12 15 18 21 24 27]] [1 [1 shared 4 7 10 13 16 19 22 25 28]] [2 [2 shared 5 8 11 14 17 20 23 26 29]]]"
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

// A failed load fails the aggregation, cancels the loads in flight and
// returns only once none is running.
func TestMemberLoadErrorCancelsTheOthers(t *testing.T) {
	loadErr := cherr.New(cherr.LogicalError, "member load failed")
	spy := &loadSpy{load: func(ctx context.Context, m []any) ([]any, error) {
		if m[0] == "bad" {
			return nil, loadErr
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
			return m, nil
		}
	}}
	var input [][]any
	for i := range 20 {
		m := fmt.Sprint(i)
		if i == 0 {
			m = "bad"
		}
		input = append(input, []any{int64(i), spy.state(m)})
	}
	start := time.Now()
	if _, err := runSetMerge(t, nil, input); !errors.Is(err, loadErr) {
		t.Fatalf("got %v, want the member load error", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("took %v: the other loads were not cancelled", d)
	}
	eq(t, spy.running.Load(), int64(0))
	if c := spy.calls.Load(); c > memberLoaders {
		t.Errorf("%d loads started, want at most %d", c, memberLoaders)
	}
}

// A panic inside a load fails the query with LOGICAL_ERROR instead of
// killing the process.
func TestMemberLoadPanicIsLogicalError(t *testing.T) {
	spy := &loadSpy{load: func(_ context.Context, m []any) ([]any, error) {
		if m[0] == "boom" {
			panic("decode exploded")
		}
		return m, nil
	}}
	input := [][]any{{int64(1), spy.state("a")}, {int64(2), spy.state("boom")}}
	_, err := runSetMerge(t, nil, input)
	if ce := cherr.As(err); ce == nil || ce.Code != cherr.LogicalError || !strings.Contains(ce.Msg, "decode exploded") {
		t.Fatalf("got %v, want LOGICAL_ERROR from the panic", err)
	}
}

// The members a load will add are reserved from SetSize before it starts,
// so sets too large for the limit fail before any is read.
func TestMemberLoadReservesBeforeReading(t *testing.T) {
	spy := &loadSpy{}
	big := func() *chtype.AggPartial {
		p := spy.state("x")
		p.SetSize = 50_000_000
		return p
	}
	input := [][]any{{int64(1), big()}, {int64(2), big()}}
	_, err := runSetMerge(t, map[string]string{"max_memory_usage": "100000000"}, input)
	if ce := cherr.As(err); ce == nil || ce.Code != cherr.MemoryLimitExceeded {
		t.Fatalf("got %v, want MEMORY_LIMIT_EXCEEDED", err)
	}
	eq(t, spy.calls.Load(), int64(0))
}
