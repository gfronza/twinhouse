//go:build !race

package memtrack

// The race detector pads allocations, so the live heap matches the
// estimates only without it.

import (
	"context"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec/aggstate"
)

// liveHeap is the live heap after a full collection.
func liveHeap() int64 {
	runtime.GC()
	s := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(s)
	return int64(s[0].Value.Uint64())
}

// measure returns the live bytes build allocates and its estimate.
func measure(build func() (any, int64)) (measured, estimate int64) {
	before := liveHeap()
	v, est := build()
	measured = liveHeap() - before
	runtime.KeepAlive(v)
	return measured, est
}

// boxed builds n values with mk into a []any and estimates them as the
// engine does: one 16-byte slot each plus Value.
func boxed(n int, mk func(i int) any) func() (any, int64) {
	return func() (any, int64) {
		vs := make([]any, n)
		for i := range vs {
			vs[i] = mk(i)
		}
		return vs, Slice(n) - 24 + values(vs)
	}
}

func rows(n, width int) func() (any, int64) {
	return func() (any, int64) {
		out := make([][]any, n)
		est := int64(0)
		for i := range out {
			r := make([]any, width)
			for j := range r {
				if j%2 == 0 {
					r[j] = uint64(1_000_000 + i*width + j)
				} else {
					r[j] = strconv.Itoa(100_000_000 + i)
				}
			}
			out[i] = r
			est += Row(r)
		}
		return out, est
	}
}

// mapOf builds a map of n string keys the way GROUP BY and DISTINCT do (no
// size hint) and estimates MapEntry(slot) plus each key's bytes.
func mapOf[V any](n int, slot int64, val V) func() (any, int64) {
	return func() (any, int64) {
		m := map[string]V{}
		est := int64(0)
		for i := 0; i < n; i++ {
			k := "key-" + strconv.Itoa(1_000_000+i)
			m[k] = val
			est += MapEntry(slot) + Alloc(int64(len(k)))
		}
		return m, est
	}
}

type state struct{ n int }

// unloadedState is a set state as DynamoDB decodes it: no members, and a
// LoadSet closure over three pointers and copies of the row's two keys of
// keyLen bytes, sized as the store sizes it.
func unloadedState(store, layout, col *state, keyLen int) func(i int) any {
	return func(i int) any {
		key := []byte(strconv.Itoa(100_000_000+i) + strings.Repeat("k", keyLen-9))
		pk, sk := append([]byte(nil), key...), append([]byte(nil), key...)
		p := &chtype.AggPartial{Func: "uniqExact", SetSize: 8}
		p.LoadSet = func(context.Context) ([]any, error) { return loadMembers(store, layout, pk, sk, col), nil }
		p.LoadSetBytes = int32(Alloc(4*8+2*24) + Bytes(int64(cap(pk))) + Bytes(int64(cap(sk))))
		return p
	}
}

func loadMembers(store, layout *state, pk, sk []byte, col *state) []any {
	return []any{store.n + layout.n + col.n, len(pk) + len(sk)}
}

// garbage receives a value allocated just before each live one, as the
// engine allocates a result right after its temporaries.
var garbage any

// afterGarbage is mk with a short-lived 8-byte box allocated before each
// value, so a tiny value shares its block with garbage.
func afterGarbage(mk func(i int) any) func(i int) any {
	return func(i int) any {
		garbage = uint64(1_000_000 + i)
		return mk(i)
	}
}

// engineState is an aggregate state as the engine builds a -State result:
// aggstate.New, filled, then cloned for the output row.
func engineState(f string, args []chtype.Type, fill func(p *chtype.AggPartial, i int)) func(i int) any {
	return func(i int) any {
		p := aggstate.New(f, args)
		fill(p, i)
		return aggstate.Clone(p)
	}
}

// heapNoise is how far the live heap moves between two collections without
// the test allocating anything (runtime and testing bookkeeping).
const heapNoise = 64 << 10

// TestEstimatesCoverLiveHeap checks every value shape the engine holds:
// the estimate is never below the live heap the shape takes, and at most
// 1.6 times it.
func TestEstimatesCoverLiveHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates about 400 MB")
	}
	const n = 100_000
	kib := strings.Repeat("k", 1<<10)
	mib := strings.Repeat("m", 1<<20)
	shared := &state{}
	for _, c := range []struct {
		name  string
		build func() (any, int64)
	}{
		{"uint64", boxed(n, func(i int) any { return uint64(1_000_000 + i) })},
		{"int64", boxed(n, func(i int) any { return int64(-1_000_000 - i) })},
		{"float64", boxed(n, func(i int) any { return float64(i) + 0.5 })},
		{"empty string", boxed(n, func(int) any { return "" })},
		{"9 B string", boxed(n, func(i int) any { return strconv.Itoa(100_000_000 + i) })},
		{"1 KiB string", boxed(n, func(i int) any { return strconv.Itoa(i) + kib[len(strconv.Itoa(i)):] })},
		{"1 MiB string", boxed(64, func(i int) any { return strconv.Itoa(i) + mib[len(strconv.Itoa(i)):] })},
		{"time", boxed(n, func(i int) any { return time.Unix(int64(i), 0).UTC() })},
		{"nested array", boxed(n, func(i int) any {
			return []any{[]any{uint64(1000 + i), uint64(2000 + i)}, []any{uint64(3000 + i)}}
		})},
		{"tuple", boxed(n, func(i int) any { return []any{uint64(1000 + i), strconv.Itoa(100_000_000 + i), 0.5 + float64(i)} })},
		{"AggPartial set", boxed(n/10, func(i int) any {
			set := make([]any, 0, 8)
			for j := 0; j < 8; j++ {
				set = append(set, strconv.Itoa(100_000_000+i*8+j))
			}
			return &chtype.AggPartial{Func: "uniqExact", Set: set, SetSize: 8, SetLoaded: true}
		})},
		{"AggPartial list", boxed(n/10, func(i int) any {
			var list []any
			for j := 0; j < 10; j++ {
				list = append(list, uint64(1000+i*10+j))
			}
			return &chtype.AggPartial{Func: "groupArray", List: list, Count: 10}
		})},
		{"unloaded set, 12 B keys", boxed(n/10, unloadedState(shared, &state{}, &state{}, 12))},
		{"unloaded set, 32 B keys", boxed(n/10, unloadedState(shared, &state{}, &state{}, 32))},
		{"unloaded set, 128 B keys", boxed(n/10, unloadedState(shared, &state{}, &state{}, 128))},
		{"unloaded set, 512 B keys", boxed(n/10, unloadedState(shared, &state{}, &state{}, 512))},
		{"uint64 after garbage", boxed(n, afterGarbage(func(i int) any { return uint64(2_000_000 + i) }))},
		{"float64 after garbage", boxed(n, afterGarbage(func(i int) any { return float64(i) + 0.5 }))},
		{"7 B string after garbage", boxed(n, afterGarbage(func(i int) any { return strconv.Itoa(1_000_000 + i) }))},
		{"sumState", boxed(n/10, engineState("sum", []chtype.Type{chtype.TUInt64}, func(p *chtype.AggPartial, i int) {
			p.Sum = uint64(1000 + i)
		}))},
		{"argMaxState", boxed(n/10, engineState("argMax", []chtype.Type{chtype.TString, chtype.TUInt64}, func(p *chtype.AggPartial, i int) {
			p.Value, p.Key, p.HasValue = strconv.Itoa(100_000_000+i), uint64(1000+i), true
		}))},
		{"row width 1", rows(n, 1)},
		{"row width 4", rows(n, 4)},
		{"row width 16", rows(n, 16)},
		{"map[string]struct{}", mapOf(n, 16, struct{}{})},
		{"map[string]*T", mapOf(n, 24, shared)},
		{"map[string][]int", mapOf(n, 40, []int(nil))},
	} {
		measured, est := measure(c.build)
		t.Logf("%-20s measured %10d estimate %10d ratio %.2f", c.name, measured, est, float64(est)/float64(max(measured, 1)))
		if est < measured-heapNoise || float64(est) > 1.6*float64(measured) {
			t.Errorf("%s: estimate %d for a live heap of %d (want between 1x and 1.6x)", c.name, est, measured)
		}
	}
}
