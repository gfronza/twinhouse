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
	return nil
}

// Release returns n reserved bytes.
func (t *Tracker) Release(n int64) {
	if t != nil {
		t.used.Add(-n)
	}
}
