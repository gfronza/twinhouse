//go:build !race

package dynamo

// The race detector pads allocations, so the live heap matches the
// estimates only without it.

import (
	"bytes"
	"runtime"
	"runtime/metrics"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/memtrack"
)

func liveHeap() int64 {
	runtime.GC()
	s := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(s)
	return int64(s[0].Value.Uint64())
}

// TestLoadSetBytesCoversTheClosure checks that an unloaded set state's
// estimate covers the live heap decodeAggState leaves, for short and long
// keys, and is at most 1.25 times it.
func TestLoadSetBytesCoversTheClosure(t *testing.T) {
	s := &storeImpl{}
	c := &colSpec{name: "u", agg: catalog.AggregateSpec{Func: catalog.AggUniqExact, Args: []chtype.Type{chtype.TUInt64}}}
	l := &layout{}
	item := map[string]types.AttributeValue{"u" + sufSize: &types.AttributeValueMemberN{Value: "8"}}
	for _, keyLen := range []int{12, 128, 512} {
		key := bytes.Repeat([]byte("k"), keyLen)
		states := make([]any, 20000)
		before := liveHeap()
		est := int64(0)
		for i := range states {
			p, err := s.decodeAggState(l, c, item, key, key)
			if err != nil {
				t.Fatal(err)
			}
			states[i] = p
			est += memtrack.Value(p)
		}
		measured := liveHeap() - before
		runtime.KeepAlive(states)
		t.Logf("%d B keys: measured %d estimate %d ratio %.2f", keyLen, measured, est, float64(est)/float64(measured))
		if est < measured-64<<10 || float64(est) > 1.25*float64(measured) {
			t.Errorf("%d B keys: estimate %d for a live heap of %d", keyLen, est, measured)
		}
	}
}
