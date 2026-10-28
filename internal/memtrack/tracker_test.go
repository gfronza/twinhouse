package memtrack

import (
	"slices"
	"sync"
	"testing"

	"github.com/gfronza/twinhouse/internal/cherr"
)

func TestTrackerReserveFailsPastLimitAndAddsNothing(t *testing.T) {
	tr := New(100)
	if err := tr.Reserve(60); err != nil {
		t.Fatal(err)
	}
	err := tr.Reserve(50)
	if ce := cherr.As(err); ce == nil || ce.Code != cherr.MemoryLimitExceeded {
		t.Fatalf("Reserve past the limit: %v", err)
	}
	if tr.Used() != 60 {
		t.Fatalf("used after a failed reserve = %d, want 60", tr.Used())
	}
	if err := tr.Reserve(40); err != nil {
		t.Fatalf("reserve up to the limit: %v", err)
	}
	tr.Release(100)
	if tr.Used() != 0 {
		t.Fatalf("used after release = %d", tr.Used())
	}
}

func TestTrackerZeroLimitIsUnlimited(t *testing.T) {
	tr := New(0)
	if err := tr.Reserve(1 << 60); err != nil {
		t.Fatal(err)
	}
	tr.SetLimit(10)
	if err := tr.Reserve(1); err == nil {
		t.Fatal("reserve after lowering the limit succeeded")
	}
}

func TestNilTrackerAndAccountDoNothing(t *testing.T) {
	var tr *Tracker
	if err := tr.Reserve(1 << 40); err != nil {
		t.Fatal(err)
	}
	tr.Release(5)
	tr.SetLimit(5)
	if tr.Used() != 0 || tr.Limit() != 0 || tr.NewAccount() != nil {
		t.Fatal("nil tracker is not inert")
	}
	var a *Account
	if err := a.Grow(1 << 40); err != nil {
		t.Fatal(err)
	}
	a.Shrink(1)
	a.Close()
}

func TestUnlimitedTrackerGivesNilAccount(t *testing.T) {
	if New(0).NewAccount() != nil {
		t.Fatal("an unlimited tracker returned an account")
	}
	if New(1).NewAccount() == nil {
		t.Fatal("a limited tracker returned no account")
	}
}

func TestAccountReservesOnceAChunkIsPending(t *testing.T) {
	tr := New(1 << 30)
	a := tr.NewAccount()
	if err := a.Grow(Chunk - 1); err != nil {
		t.Fatal(err)
	}
	if tr.Used() != 0 {
		t.Fatalf("used with less than a chunk pending = %d", tr.Used())
	}
	if err := a.Grow(1); err != nil {
		t.Fatal(err)
	}
	if tr.Used() != Chunk {
		t.Fatalf("used after a full chunk = %d, want %d", tr.Used(), Chunk)
	}
}

func TestAccountChecksOneLargeGrowAtOnce(t *testing.T) {
	tr := New(1 << 20)
	a := tr.NewAccount()
	if err := a.Grow(10 << 20); err == nil {
		t.Fatal("a 10 MiB grow passed a 1 MiB limit")
	}
	if tr.Used() != 0 {
		t.Fatalf("used after a failed grow = %d", tr.Used())
	}
}

func TestAccountShrinkReleasesChunksButNeverBelowZero(t *testing.T) {
	tr := New(1 << 30)
	a := tr.NewAccount()
	if err := a.Grow(4 * Chunk); err != nil {
		t.Fatal(err)
	}
	a.Shrink(Chunk / 2)
	if tr.Used() != 4*Chunk {
		t.Fatalf("used after a small shrink = %d", tr.Used())
	}
	a.Shrink(Chunk)
	if want := int64(4*Chunk - Chunk - Chunk/2); tr.Used() != want {
		t.Fatalf("used after shrinking past a chunk = %d, want %d", tr.Used(), want)
	}
	a.Shrink(100 * Chunk)
	if tr.Used() != 0 {
		t.Fatalf("used after shrinking past everything = %d", tr.Used())
	}
	a.Close()
	if tr.Used() != 0 {
		t.Fatalf("used after close = %d", tr.Used())
	}
}

func TestAccountCloseIsIdempotentAndReusable(t *testing.T) {
	tr := New(1 << 30)
	a := tr.NewAccount()
	if err := a.Grow(3 * Chunk); err != nil {
		t.Fatal(err)
	}
	a.Close()
	a.Close()
	if tr.Used() != 0 {
		t.Fatalf("used after close = %d", tr.Used())
	}
	if err := a.Grow(Chunk); err != nil {
		t.Fatal(err)
	}
	if tr.Used() != Chunk {
		t.Fatalf("used after reuse = %d", tr.Used())
	}
	a.Close()
}

func TestTrackerConcurrentReserveRelease(t *testing.T) {
	tr := New(1 << 20)
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Go(func() {
			for i := 0; i < 10000; i++ {
				if tr.Reserve(4096) == nil {
					tr.Release(4096)
				}
			}
		})
	}
	wg.Wait()
	if tr.Used() != 0 {
		t.Fatalf("used after balanced reserves = %d", tr.Used())
	}
}

func TestReadableSize(t *testing.T) {
	for _, c := range []struct {
		n    int64
		want string
	}{
		{0, "0.00 B"},
		{1023, "1023.00 B"},
		{1024, "1.00 KiB"},
		{1536, "1.50 KiB"},
		{1152, "1.13 KiB"}, // 1.125 rounds half up, as ClickHouse does
		{20000000, "19.07 MiB"},
		{1 << 30, "1.00 GiB"},
		{1<<40 - 1, "1024.00 GiB"},
		{1 << 62, "4.00 EiB"},
	} {
		if got := ReadableSize(c.n); got != c.want {
			t.Errorf("ReadableSize(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

func TestSampleHookFiresOnEachStepPassed(t *testing.T) {
	tr := New(100)
	var got []int64
	tr.SetSampleHook(10, func(used int64) { got = append(got, used) })
	for _, n := range []int64{4, 4, 4, 25, 2} {
		if err := tr.Reserve(n); err != nil {
			t.Fatal(err)
		}
	}
	tr.Release(30)
	_ = tr.Reserve(200) // over the limit: not reserved, no sample
	if err := tr.Reserve(5); err != nil {
		t.Fatal(err)
	}
	if want := []int64{12, 37, 14}; !slices.Equal(got, want) {
		t.Fatalf("samples %v, want %v", got, want)
	}
}

func TestAccountChargedCountsPendingBytes(t *testing.T) {
	a := New(1 << 30).NewAccount()
	if err := a.Grow(Chunk + 10); err != nil {
		t.Fatal(err)
	}
	if err := a.Grow(7); err != nil {
		t.Fatal(err)
	}
	a.Shrink(3)
	if got := a.Charged(); got != Chunk+14 {
		t.Fatalf("Charged() = %d, want %d", got, Chunk+14)
	}
	a.Close()
	if got := a.Charged(); got != 0 {
		t.Fatalf("after Close: %d", got)
	}
}

func TestLimitExceededMessage(t *testing.T) {
	err := LimitExceeded(22408069, 4372562, 20000000)
	if err.Code != 241 || err.Name() != "MEMORY_LIMIT_EXCEEDED" {
		t.Fatalf("code %d %s", err.Code, err.Name())
	}
	const want = "Query memory limit exceeded: would use 21.37 MiB (attempt to allocate chunk of 4.17 MiB bytes), maximum: 19.07 MiB"
	if err.Msg != want {
		t.Fatalf("message\n got: %s\nwant: %s", err.Msg, want)
	}
}
