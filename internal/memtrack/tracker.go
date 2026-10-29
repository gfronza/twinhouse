// Package memtrack accounts the memory one query keeps alive, for
// max_memory_usage. A query has one Tracker; each structure that grows with
// its input (a collected result, a sort buffer, a hash table) charges its
// estimated bytes through an Account and releases them when it dies. The
// process heap is never read, so a query fails at the same point whatever
// else the process holds.
package memtrack

import "sync/atomic"

// Tracker counts one query's tracked bytes against its limit. It is safe for
// concurrent use. A nil Tracker tracks nothing.
type Tracker struct {
	limit atomic.Int64
	used  atomic.Int64
	peak  atomic.Int64
	// sample and sampleStep are a test hook (SetSampleHook).
	sample     func(used int64)
	sampleStep int64
}

// SetSampleHook makes Reserve call f with the reserved bytes each time they
// pass a multiple of step. It is for tests, which set it before the tracker
// is used; f may be called from several goroutines at once.
func (t *Tracker) SetSampleHook(step int64, f func(used int64)) {
	t.sample, t.sampleStep = f, step
}

// New returns a tracker with the given limit in bytes (0 = unlimited, as in
// ClickHouse).
func New(limit int64) *Tracker {
	t := &Tracker{}
	t.limit.Store(limit)
	return t
}

// SetLimit changes the limit (0 = unlimited).
func (t *Tracker) SetLimit(n int64) {
	if t != nil {
		t.limit.Store(n)
	}
}

// Limit returns the limit in bytes (0 = unlimited).
func (t *Tracker) Limit() int64 {
	if t == nil {
		return 0
	}
	return t.limit.Load()
}

// Used returns the bytes currently reserved.
func (t *Tracker) Used() int64 {
	if t == nil {
		return 0
	}
	return t.used.Load()
}

// Reserve adds n bytes, or fails with MEMORY_LIMIT_EXCEEDED (241) and adds
// nothing when that would pass the limit.
func (t *Tracker) Reserve(n int64) error {
	if t == nil {
		return nil
	}
	u := t.used.Add(n)
	if l := t.limit.Load(); l > 0 && u > l {
		t.used.Add(-n)
		return LimitExceeded(u, n, l)
	}
	t.notePeak(u)
	if t.sample != nil && u/t.sampleStep > (u-n)/t.sampleStep {
		t.sample(u)
	}
	return nil
}

// notePeak raises the peak to u.
func (t *Tracker) notePeak(u int64) {
	for p := t.peak.Load(); u > p; p = t.peak.Load() {
		if t.peak.CompareAndSwap(p, u) {
			return
		}
	}
}

// Peak returns the most bytes reserved at once.
func (t *Tracker) Peak() int64 {
	if t == nil {
		return 0
	}
	return t.peak.Load()
}

// Release returns n reserved bytes.
func (t *Tracker) Release(n int64) {
	if t != nil {
		t.used.Add(-n)
	}
}
