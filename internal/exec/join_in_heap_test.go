//go:build !race

package exec

import (
	"context"
	"runtime"
	"testing"

	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/memtrack"
	"github.com/gfronza/twinhouse/internal/sqlparse"
)

// checkEstimate fails unless the tracked bytes cover the live heap growth
// and are at most 1.6 times it. Up to one Chunk may still be pending.
func checkEstimate(t *testing.T, what string, used, live int64) {
	t.Helper()
	t.Logf("%s: estimate %d, live %d (%.2f)", what, used, live, float64(used)/float64(live))
	if used+memtrack.Chunk < live || float64(used) > 1.6*float64(live) {
		t.Errorf("%s: estimate not between 1x and 1.6x the live heap", what)
	}
}

// TestInSetEstimateCoversLiveHeap checks the charge of IN subquery sets
// against the live heap they hold.
func TestInSetEstimateCoversLiveHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("builds sets of about 50 MB")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	tuple := chtype.NewTuple(chtype.Field{Type: chtype.TUInt64}, chtype.Field{Type: chtype.TString})
	for _, c := range []struct {
		sql string
		t   chtype.Type
	}{
		{"SELECT number FROM numbers(300000)", chtype.TUInt64},
		{"SELECT number % 1000 FROM numbers(300000)", chtype.TUInt64},
		{"SELECT toString(number) FROM numbers(300000)", chtype.TString},
		{"SELECT concat(repeat('x', 100), toString(number)) FROM numbers(200000)", chtype.TString},
		{"SELECT concat(repeat('x', 1000), toString(number)) FROM numbers(30000)", chtype.TString},
		{"SELECT number, toString(number) FROM numbers(200000)", tuple},
		{"SELECT toUInt8(number % 200) FROM numbers(300000)", chtype.TUInt64},
	} {
		sel, err := parseSelect(c.sql)
		if err != nil {
			t.Fatal(err)
		}
		q := te.e.newQuery(context.Background(), &Request{})
		cc := &compiler{q: q, env: &planEnv{}}
		before := liveHeapBytes()
		set, err := cc.inSet(c.t, &sqlparse.SubqueryExpr{Select: sel})
		if err != nil {
			t.Fatal(err)
		}
		live := liveHeapBytes() - before
		checkEstimate(t, c.sql, q.mem.Used(), live)
		runtime.KeepAlive(set)
		q.finish()
	}
}

// TestJoinBuildEstimateCoversLiveHeap checks the charge of join builds
// against the live heap they hold.
func TestJoinBuildEstimateCoversLiveHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("builds joins of about 50 MB")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	same := func(v any) (any, error) { return v, nil }
	for _, c := range []struct {
		sql   string
		keyed bool
	}{
		{"SELECT number, toString(number) FROM numbers(200000)", true},
		{"SELECT number % 1000, toString(number) FROM numbers(200000)", true},
		{"SELECT number, concat(repeat('x', 100), toString(number)) FROM numbers(100000)", true},
		{"SELECT toString(number), number FROM numbers(200000)", true},
		{"SELECT number, toString(number) FROM numbers(200000)", false},
		{"SELECT number, if(number % 2 = 0, NULL, number) FROM numbers(200000)", false},
		{"SELECT number, number * 2 FROM numbers(200000)", true},
		{"SELECT number FROM numbers(300000)", false},
		{"SELECT number FROM numbers(1500)", false},
	} {
		q := te.e.newQuery(context.Background(), &Request{})
		sel, err := parseSelect(c.sql)
		if err != nil {
			t.Fatal(err)
		}
		rel, err := q.planSelect(sel, &planEnv{})
		if err != nil {
			t.Fatal(err)
		}
		spec := &joinSpec{kind: "INNER", leftW: 1, rightW: len(rel.cols), rightCols: rel.cols}
		if c.keyed {
			spec.rightKeys = []*expr{slotExpr(1, rel.cols[0].Type)}
			spec.keyConvR = []func(any) (any, error){same}
		}
		it, err := rel.open()
		if err != nil {
			t.Fatal(err)
		}
		before := liveHeapBytes()
		b, err := spec.build(q, it)
		if err != nil {
			t.Fatal(err)
		}
		live := liveHeapBytes() - before
		it.close()
		checkEstimate(t, c.sql, q.mem.Used(), live)
		runtime.KeepAlive(b)
		b.ch.acct.Close()
		q.finish()
	}
}

func TestJoinBuildStopsAtMemoryLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("builds joins up to 32 MB")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	wantLimitBeforeHeap(t, te, "SELECT count() FROM numbers(10) AS a CROSS JOIN system.numbers AS b", 32000000)
	wantLimitBeforeHeap(t, te, "SELECT count() FROM numbers(10) AS a JOIN system.numbers AS b ON a.number = b.number", 32000000)
}

func TestInSetsStopAtMemoryLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("builds sets up to 32 MB")
	}
	guardFunctionTest(t)
	te := newTestEnv(t)
	wantLimitBeforeHeap(t, te, "SELECT count() FROM numbers(10) WHERE number IN (SELECT number FROM system.numbers)", 32000000)
	// The rows alone would fit; the set they make does not.
	wantLimitBeforeHeap(t, te, "SELECT count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(400000))", 32000000)
	wantLimitBeforeHeap(t, te, "SELECT count() FROM numbers(10) WHERE (number, toString(number)) IN "+
		"(SELECT number, toString(number) FROM system.numbers)", 32000000)
	wantLimitBeforeHeap(t, te, "SELECT count() FROM numbers(10) WHERE number IN range(400000)", 32000000)
}
