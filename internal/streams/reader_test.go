package streams

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/smithy-go"
)

const (
	testTable = "tbl"
	arn1      = "arn:aws:dynamodb:local:0:table/tbl/stream/1"
	arn2      = "arn:aws:dynamodb:local:0:table/tbl/stream/2"
)

// collector records delivered keys per shard, in delivery order.
type collector struct {
	mu      sync.Mutex
	keys    []string
	byShard map[string][]string
	fail    func(b *Batch) error
}

func newCollector() *collector { return &collector{byShard: map[string][]string{}} }

func (c *collector) handle(_ context.Context, b *Batch) error {
	if c.fail != nil {
		if err := c.fail(b); err != nil {
			return err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range b.Records {
		k := keyOf(r)
		c.keys = append(c.keys, k)
		c.byShard[b.ShardID] = append(c.byShard[b.ShardID], k)
	}
	return nil
}

func (c *collector) all() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.keys)
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.keys)
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testConfig(f *fakeAWS, owner string, h Handler) Config {
	return Config{
		Streams:           f,
		DynamoDB:          f,
		Table:             testTable,
		CheckpointTable:   "ckpt",
		Consumer:          "test",
		Owner:             owner,
		Handler:           h,
		PollInterval:      2 * time.Millisecond,
		MaxIdleBackoff:    10 * time.Millisecond,
		LeaseDuration:     300 * time.Millisecond,
		DiscoveryInterval: 10 * time.Millisecond,
		MaxRetryBackoff:   10 * time.Millisecond,
		Logger:            quietLogger(),
	}
}

// startReader runs a reader in the background; the returned stop func
// cancels it and waits for Run to return.
func startReader(t *testing.T, cfg Config, leases leaseStore) (*Reader, func()) {
	t.Helper()
	r, err := newReader(cfg, leases)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Run returned %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("reader did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return r, stop
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func keys(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return out
}

func TestNewValidates(t *testing.T) {
	f := newFakeAWS(testTable)
	base := testConfig(f, "o", func(context.Context, *Batch) error { return nil })
	cases := map[string]func(*Config){
		"no streams":    func(c *Config) { c.Streams = nil },
		"no dynamodb":   func(c *Config) { c.DynamoDB = nil },
		"no table":      func(c *Config) { c.Table = "" },
		"no checkpoint": func(c *Config) { c.CheckpointTable = "" },
		"no consumer":   func(c *Config) { c.Consumer = "" },
		"no owner":      func(c *Config) { c.Owner = "" },
		"no handler":    func(c *Config) { c.Handler = nil },
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			c := base
			mut(&c)
			if _, err := New(c); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if _, err := New(base); err != nil {
		t.Fatal(err)
	}
}

func TestReaderDeliversInOrderAndCheckpoints(t *testing.T) {
	f := newFakeAWS(testTable)
	f.createStream(arn1)
	f.addShard(arn1, "s0", "")
	want := keys("k", 2500) // > BatchSize: several batches
	seqs := f.put(arn1, "s0", want...)
	c := newCollector()
	leases := newMemLeases()
	r, _ := startReader(t, testConfig(f, "a", c.handle), leases)

	waitFor(t, "all records", func() bool { return c.count() == len(want) })
	if got := c.all(); !slices.Equal(got, want) {
		t.Fatalf("delivered out of order or with duplicates")
	}
	key := CheckpointKey("test", arn1, "s0")
	waitFor(t, "checkpoint", func() bool { return leases.snapshot(key).Seq == seqs[len(seqs)-1] })
	st := r.Stats()
	if st.Records != 2500 || st.Batches < 3 || st.ActiveShards != 1 {
		t.Fatalf("stats = %+v", st)
	}
	// New records on the open shard keep flowing.
	f.put(arn1, "s0", "late")
	waitFor(t, "late record", func() bool { return c.count() == len(want)+1 })
}

func TestReaderLineageParentsFirst(t *testing.T) {
	f := newFakeAWS(testTable)
	f.createStream(arn1)
	// Children are listed before their parents to make sure ordering comes
	// from lineage, not from list order.
	f.addShard(arn1, "g", "c1")
	f.addShard(arn1, "c1", "p")
	f.addShard(arn1, "c2", "p")
	f.addShard(arn1, "p", "")
	f.addShard(arn1, "orphan", "trimmed-parent") // parent no longer listed
	f.put(arn1, "p", keys("p", 3)...)
	f.put(arn1, "c1", keys("c1-", 2)...)
	f.put(arn1, "c2", keys("c2-", 2)...)
	f.put(arn1, "g", keys("g", 2)...)
	f.put(arn1, "orphan", "o0")
	for _, s := range []string{"p", "c1"} {
		f.closeShard(arn1, s)
	}
	c := newCollector()
	leases := newMemLeases()
	startReader(t, testConfig(f, "a", c.handle), leases)
	waitFor(t, "all records", func() bool { return c.count() == 10 })

	got := c.all()
	idx := func(k string) int { return slices.Index(got, k) }
	for _, child := range []string{"c1-0", "c1-1", "c2-0", "c2-1", "g0", "g1"} {
		if idx(child) < idx("p2") {
			t.Errorf("%s delivered before parent shard finished: %v", child, got)
		}
	}
	for _, g := range []string{"g0", "g1"} {
		if idx(g) < idx("c1-1") {
			t.Errorf("%s delivered before c1 finished: %v", g, got)
		}
	}
	for _, s := range []string{"p", "c1"} {
		if cp := leases.snapshot(CheckpointKey("test", arn1, s)); !cp.Done || cp.Owner != "" {
			t.Errorf("shard %s checkpoint = %+v, want done and released", s, cp)
		}
	}
	if cp := leases.snapshot(CheckpointKey("test", arn1, "c2")); cp.Done {
		t.Errorf("open shard c2 marked done")
	}
}

func TestReaderPaginatesDescribeStream(t *testing.T) {
	f := newFakeAWS(testTable)
	f.pageSize = 2
	f.createStream(arn1)
	for i := range 5 {
		id := fmt.Sprintf("s%d", i)
		f.addShard(arn1, id, "")
		f.put(arn1, id, id)
	}
	c := newCollector()
	startReader(t, testConfig(f, "a", c.handle), newMemLeases())
	waitFor(t, "records of all pages", func() bool { return c.count() == 5 })
}

func TestReaderResumesFromCheckpoint(t *testing.T) {
	f := newFakeAWS(testTable)
	f.createStream(arn1)
	f.addShard(arn1, "s0", "")
	f.put(arn1, "s0", "a", "b", "c")
	leases := newMemLeases()

	c1 := newCollector()
	_, stop := startReader(t, testConfig(f, "w1", c1.handle), leases)
	waitFor(t, "first records", func() bool { return c1.count() == 3 })
	stop()
	key := CheckpointKey("test", arn1, "s0")
	if cp := leases.snapshot(key); cp.Owner != "" {
		t.Fatalf("lease not released on graceful stop: %+v", cp)
	}

	f.put(arn1, "s0", "d", "e")
	c2 := newCollector()
	startReader(t, testConfig(f, "w2", c2.handle), leases)
	waitFor(t, "resumed records", func() bool { return c2.count() == 2 })
	time.Sleep(30 * time.Millisecond)
	if got := c2.all(); !slices.Equal(got, []string{"d", "e"}) {
		t.Fatalf("resumed reader got %v, want [d e]", got)
	}
}

func TestReaderAtLeastOnceOnHandlerFailure(t *testing.T) {
	f := newFakeAWS(testTable)
	f.createStream(arn1)
	f.addShard(arn1, "s0", "")
	f.put(arn1, "s0", "a", "b")
	leases := newMemLeases()
	key := CheckpointKey("test", arn1, "s0")

	var calls atomic.Int32
	c := newCollector()
	c.fail = func(b *Batch) error {
		if calls.Add(1) <= 3 {
			if cp := leases.snapshot(key); cp.Seq != "" {
				t.Errorf("checkpoint advanced to %s before handler success", cp.Seq)
			}
			return errors.New("boom")
		}
		return nil
	}
	r, _ := startReader(t, testConfig(f, "a", c.handle), leases)
	waitFor(t, "delivery after retries", func() bool { return c.count() == 2 })
	if got := c.all(); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("got %v", got)
	}
	if st := r.Stats(); st.HandlerErrors != 3 {
		t.Fatalf("HandlerErrors = %d, want 3", st.HandlerErrors)
	}
	waitFor(t, "checkpoint", func() bool { return leases.snapshot(key).Seq != "" })
}

func TestReaderExpiredIteratorResumesFromCheckpoint(t *testing.T) {
	f := newFakeAWS(testTable)
	f.createStream(arn1)
	f.addShard(arn1, "s0", "")
	f.put(arn1, "s0", "a", "b")
	c := newCollector()
	startReader(t, testConfig(f, "a", c.handle), newMemLeases())
	waitFor(t, "first batch", func() bool { return c.count() == 2 })

	f.mu.Lock()
	f.expireNext = 2
	f.mu.Unlock()
	f.put(arn1, "s0", "c", "d")
	waitFor(t, "after expiry", func() bool { return c.count() == 4 })
	time.Sleep(30 * time.Millisecond)
	if got := c.all(); !slices.Equal(got, []string{"a", "b", "c", "d"}) {
		t.Fatalf("got %v (lost or duplicated records)", got)
	}
}

func TestReaderTrimmedCheckpointRestartsAtTrimHorizon(t *testing.T) {
	f := newFakeAWS(testTable)
	f.createStream(arn1)
	f.addShard(arn1, "s0", "")
	seqs := f.put(arn1, "s0", "a", "b", "c", "d")
	f.trim(arn1, "s0", 2) // a, b are gone
	leases := newMemLeases()
	leases.set(CheckpointKey("test", arn1, "s0"), checkpoint{Seq: seqs[0]})
	c := newCollector()
	startReader(t, testConfig(f, "a", c.handle), leases)
	waitFor(t, "records after trim horizon", func() bool { return c.count() == 2 })
	if got := c.all(); !slices.Equal(got, []string{"c", "d"}) {
		t.Fatalf("got %v", got)
	}
}

func TestReaderTrimmedDuringGetRecords(t *testing.T) {
	f := newFakeAWS(testTable)
	f.createStream(arn1)
	f.addShard(arn1, "s0", "")
	f.put(arn1, "s0", "a")
	f.mu.Lock()
	f.trimNext = 1
	f.mu.Unlock()
	c := newCollector()
	startReader(t, testConfig(f, "a", c.handle), newMemLeases())
	waitFor(t, "record", func() bool { return c.count() == 1 })
}

func TestReaderFollowsRecreatedTable(t *testing.T) {
	f := newFakeAWS(testTable)
	c := newCollector()
	startReader(t, testConfig(f, "a", c.handle), newMemLeases())
	time.Sleep(20 * time.Millisecond) // table does not exist yet: reader waits

	f.createStream(arn1)
	f.addShard(arn1, "s0", "")
	f.put(arn1, "s0", "old")
	waitFor(t, "first stream", func() bool { return c.count() == 1 })

	f.createStream(arn2) // table recreated: new latest stream
	f.addShard(arn2, "s0", "")
	f.put(arn2, "s0", "new1", "new2")
	waitFor(t, "second stream", func() bool { return c.count() == 3 })
	if got := c.all(); !slices.Equal(got, []string{"old", "new1", "new2"}) {
		t.Fatalf("got %v", got)
	}
}

func TestReaderStealsExpiredLease(t *testing.T) {
	f := newFakeAWS(testTable)
	f.createStream(arn1)
	f.addShard(arn1, "s0", "")
	seqs := f.put(arn1, "s0", "a", "b", "c")
	leases := newMemLeases()
	key := CheckpointKey("test", arn1, "s0")
	expires := time.Now().Add(150 * time.Millisecond)
	// A crashed reader holds the lease and had processed "a".
	leases.set(key, checkpoint{Owner: "dead", Expires: expires, Seq: seqs[0]})

	c := newCollector()
	startReader(t, testConfig(f, "live", c.handle), leases)
	time.Sleep(50 * time.Millisecond)
	if n := c.count(); n != 0 {
		t.Fatalf("delivered %d records while another owner held the lease", n)
	}
	waitFor(t, "stolen lease", func() bool { return c.count() == 2 })
	if time.Now().Before(expires) {
		t.Fatal("lease stolen before expiry")
	}
	if got := c.all(); !slices.Equal(got, []string{"b", "c"}) {
		t.Fatalf("got %v, want resume after checkpoint", got)
	}
	if cp := leases.snapshot(key); cp.Owner != "live" {
		t.Fatalf("owner = %q", cp.Owner)
	}
}

func TestReaderStopsWhenLeaseLost(t *testing.T) {
	f := newFakeAWS(testTable)
	f.createStream(arn1)
	f.addShard(arn1, "s0", "")
	f.put(arn1, "s0", "a")
	leases := newMemLeases()
	key := CheckpointKey("test", arn1, "s0")
	c := newCollector()
	r, _ := startReader(t, testConfig(f, "a", c.handle), leases)
	waitFor(t, "first record", func() bool { return c.count() == 1 })

	// Another reader takes over (e.g. after a long pause of this one).
	leases.set(key, checkpoint{Owner: "b", Expires: time.Now().Add(time.Hour), Seq: leases.snapshot(key).Seq})
	waitFor(t, "lease loss detected", func() bool { return r.Stats().ActiveShards == 0 })
	f.put(arn1, "s0", "b")
	time.Sleep(50 * time.Millisecond)
	if n := c.count(); n != 1 {
		t.Fatalf("reader kept processing after losing its lease (%d records)", n)
	}
}

func TestTwoReadersShareShardsWithoutDuplicates(t *testing.T) {
	f := newFakeAWS(testTable)
	f.createStream(arn1)
	const shards = 6
	for i := range shards {
		f.addShard(arn1, fmt.Sprintf("s%d", i), "")
	}
	leases := newMemLeases()
	c := newCollector()
	startReader(t, testConfig(f, "r1", c.handle), leases)
	startReader(t, testConfig(f, "r2", c.handle), leases)
	var want []string
	for i := range shards {
		ks := keys(fmt.Sprintf("s%d-", i), 50)
		want = append(want, ks...)
		f.put(arn1, fmt.Sprintf("s%d", i), ks...)
	}
	waitFor(t, "all records", func() bool { return c.count() == len(want) })
	time.Sleep(30 * time.Millisecond)
	got := c.all()
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("records lost or duplicated: got %d want %d", len(got), len(want))
	}
	owners := map[string]int{}
	for i := range shards {
		owners[leases.snapshot(CheckpointKey("test", arn1, fmt.Sprintf("s%d", i))).Owner]++
	}
	if owners[""] != 0 || len(owners) == 0 {
		t.Fatalf("unexpected owners %v", owners)
	}
}

func TestBackoff(t *testing.T) {
	b := backoff{min: 10 * time.Millisecond, max: 50 * time.Millisecond}
	want := []time.Duration{10, 20, 40, 50, 50}
	for i, w := range want {
		if got := b.next(); got != w*time.Millisecond {
			t.Fatalf("step %d: got %v want %v", i, got, w*time.Millisecond)
		}
	}
	b.reset()
	if got := b.next(); got != 10*time.Millisecond {
		t.Fatalf("after reset: %v", got)
	}
}

func TestReaderTrimmedIteratorResumesFromValidCheckpoint(t *testing.T) {
	f := newFakeAWS(testTable)
	f.createStream(arn1)
	f.addShard(arn1, "s0", "")
	f.put(arn1, "s0", "a", "b")
	c := newCollector()
	startReader(t, testConfig(f, "a", c.handle), newMemLeases())
	waitFor(t, "first batch", func() bool { return c.count() == 2 })

	f.mu.Lock()
	f.trimNext = 1 // spurious trim of the iterator position; checkpoint still valid
	f.mu.Unlock()
	f.put(arn1, "s0", "c")
	waitFor(t, "after trim error", func() bool { return c.count() == 3 })
	time.Sleep(30 * time.Millisecond)
	if got := c.all(); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("got %v (redelivered from TRIM_HORIZON?)", got)
	}
}

// A shard that is still empty when the reader starts (a table created moments
// ago) must be read cleanly once records arrive, without going through the
// TrimmedDataAccessException recovery path (DynamoDB Local invalidates the
// iterators of empty shards as other tables' records advance its global
// sequence numbers).
func TestReaderFreshEmptyShardAvoidsStaleIterators(t *testing.T) {
	f := newFakeAWS(testTable)
	f.staleEmptyIters = true
	f.createStream(arn1)
	f.addShard(arn1, "s0", "")
	c := newCollector()
	startReader(t, testConfig(f, "a", c.handle), newMemLeases())
	waitFor(t, "a few empty polls", func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.getRecords >= 3
	})
	want := keys("k", 5)
	f.put(arn1, "s0", want...)
	waitFor(t, "records", func() bool { return c.count() == len(want) })
	if got := c.all(); !slices.Equal(got, want) {
		t.Fatalf("got %v", got)
	}
	// Once the shard has records, the reader goes back to NextShardIterator.
	f.mu.Lock()
	before := f.trimHorizonIters
	f.mu.Unlock()
	f.put(arn1, "s0", "late")
	waitFor(t, "late record", func() bool { return c.count() == len(want)+1 })
	time.Sleep(30 * time.Millisecond)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.staleTrimmed != 0 {
		t.Fatalf("GetRecords failed %d times with a stale iterator", f.staleTrimmed)
	}
	if f.trimHorizonIters != before {
		t.Fatalf("reader kept re-acquiring TRIM_HORIZON iterators after the shard had records")
	}
}

// A failing batch is redelivered with its attempt number, the time it
// started failing and the handler's scratch state, and the reader reports it
// (with the age of its oldest record) until it succeeds.
func TestReaderRetryMetadataAndPendingStats(t *testing.T) {
	f := newFakeAWS(testTable)
	f.createStream(arn1)
	f.addShard(arn1, "s0", "")
	f.put(arn1, "s0", "a", "b")
	var (
		mu       sync.Mutex
		attempts []int
		since    []time.Time
		scratch  []any
		release  atomic.Bool
	)
	c := newCollector()
	c.fail = func(b *Batch) error {
		mu.Lock()
		attempts = append(attempts, b.Attempt)
		since = append(since, b.FailingSince)
		scratch = append(scratch, b.Scratch)
		mu.Unlock()
		b.Scratch = b.Attempt
		if !release.Load() {
			return errors.New("poison")
		}
		return nil
	}
	r, _ := startReader(t, testConfig(f, "a", c.handle), newMemLeases())
	waitFor(t, "pending stats", func() bool {
		s := r.Stats()
		return s.RetryingBatches == 1 && !s.OldestPendingAt.IsZero() && s.HandlerErrors >= 3
	})
	if age := time.Since(r.Stats().OldestPendingAt); age < 0 || age > time.Minute {
		t.Fatalf("pending age %v", age)
	}
	release.Store(true)
	waitFor(t, "delivery", func() bool { return c.count() == 2 })
	waitFor(t, "pending cleared", func() bool { return r.Stats().RetryingBatches == 0 })
	if !r.Stats().OldestPendingAt.IsZero() {
		t.Fatal("OldestPendingAt not cleared")
	}
	mu.Lock()
	defer mu.Unlock()
	for i, a := range attempts {
		if a != i+1 {
			t.Fatalf("attempts = %v", attempts)
		}
		if i == 0 && (!since[0].IsZero() || scratch[0] != nil) {
			t.Fatalf("first attempt: since=%v scratch=%v", since[0], scratch[0])
		}
		if i > 0 && (since[i].IsZero() || !since[i].Equal(since[1]) || scratch[i] != i) {
			t.Fatalf("attempt %d: since=%v (first %v) scratch=%v", a, since[i], since[1], scratch[i])
		}
	}
}

// DescribeStream throttling (about 10 TPS per account) makes discovery back
// off instead of polling at DiscoveryInterval.
func TestReaderDiscoveryBacksOffOnThrottling(t *testing.T) {
	f := newFakeAWS(testTable)
	f.createStream(arn1)
	f.addShard(arn1, "s0", "")
	f.describeErr = &smithy.GenericAPIError{Code: "LimitExceededException", Message: "rate exceeded"}
	cfg := testConfig(f, "a", newCollector().handle)
	cfg.DiscoveryInterval = 5 * time.Millisecond
	cfg.MaxDiscoveryBackoff = 80 * time.Millisecond
	_, stop := startReader(t, cfg, newMemLeases())
	time.Sleep(400 * time.Millisecond)
	f.mu.Lock()
	n := f.describeCalls
	f.mu.Unlock()
	stop()
	// Without backoff: ~80 calls. With it: 5,10,20,40,80,80,... ms -> ~8.
	if n > 20 {
		t.Fatalf("DescribeStream called %d times in 400ms despite throttling", n)
	}
	if n < 2 {
		t.Fatalf("DescribeStream called %d times", n)
	}

	// Recovery: discovery resumes and the shard is read.
	f2 := newFakeAWS(testTable)
	f2.createStream(arn1)
	f2.addShard(arn1, "s0", "")
	f2.put(arn1, "s0", "x")
	f2.describeErr = &smithy.GenericAPIError{Code: "LimitExceededException", Message: "rate exceeded"}
	c := newCollector()
	cfg2 := testConfig(f2, "a", c.handle)
	cfg2.MaxDiscoveryBackoff = 40 * time.Millisecond
	startReader(t, cfg2, newMemLeases())
	time.Sleep(50 * time.Millisecond)
	f2.mu.Lock()
	f2.describeErr = nil
	f2.mu.Unlock()
	waitFor(t, "delivery after throttling ends", func() bool { return c.count() == 1 })
}
