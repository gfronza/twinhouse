package memtrack

// Chunk is how many bytes an Account gathers before it reserves them, so a
// holder touches the shared tracker about once per Chunk.
const Chunk = 64 << 10

// Account is one holder's share of a Tracker. It is used by one goroutine. A
// nil Account (from an unlimited query) does nothing, so holders charge it
// without checking.
type Account struct {
	t        *Tracker
	reserved int64
	pending  int64
}

// NewAccount returns an account on t, or nil when t is nil or unlimited.
func (t *Tracker) NewAccount() *Account {
	if t.Limit() == 0 {
		return nil
	}
	return &Account{t: t}
}

// Grow charges n bytes. They are reserved once Chunk bytes are pending, so a
// single large value is checked at once.
func (a *Account) Grow(n int64) error {
	if a == nil {
		return nil
	}
	a.pending += n
	if a.pending < Chunk {
		return nil
	}
	return a.flush()
}

// flush reserves the pending bytes.
func (a *Account) flush() error {
	if err := a.t.Reserve(a.pending); err != nil {
		return err
	}
	a.reserved += a.pending
	a.pending = 0
	return nil
}

// Shrink uncharges n bytes. They are released once Chunk bytes are pending,
// never more than the account reserved.
func (a *Account) Shrink(n int64) {
	if a == nil {
		return
	}
	a.pending -= n
	if a.pending > -Chunk {
		return
	}
	r := min(-a.pending, a.reserved)
	a.t.Release(r)
	a.reserved -= r
	a.pending = 0
}

// Charged returns the bytes charged and not yet uncharged, reserved or
// pending.
func (a *Account) Charged() int64 {
	if a == nil {
		return 0
	}
	return a.reserved + a.pending
}

// Close releases everything the account reserved. It may be called more
// than once, and the account may be charged again afterwards.
func (a *Account) Close() {
	if a == nil {
		return
	}
	a.t.Release(a.reserved)
	a.reserved, a.pending = 0, 0
}
