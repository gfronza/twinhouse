package exec

import (
	"context"
	"sync"
	"time"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
)

// missRefreshInterval is the minimum time between two catalog reloads
// triggered by lookup misses, so a flood of queries naming missing tables
// cannot turn into a stream of catalog scans.
const missRefreshInterval = 50 * time.Millisecond

// freshCatalog reloads the engine's catalog when a lookup of a database,
// table or view missing from the in-memory snapshot reloads the snapshot
// (strongly consistent, see catalog.Catalog.Refresh) once and retries. The
// snapshot is otherwise refreshed only periodically, so with several
// servers behind a load balancer a CREATE executed on one server would be
// unknown to the others for up to CATALOG_REFRESH.
//
// Reloads are single-flight: concurrent misses share one in-flight reload,
// and a miss is only satisfied by a reload that started after it (so it
// sees every DDL that committed before the lookup). Reload starts are at
// least missRefreshInterval apart.
type freshCatalog struct {
	cat      catalog.Catalog
	interval time.Duration

	mu        sync.Mutex
	inflight  *refreshFlight
	lastStart time.Time
	lastDone  time.Time // start time of the last completed reload
}

type refreshFlight struct {
	start time.Time
	done  chan struct{}
	err   error
}

func newFreshCatalog(c catalog.Catalog) *freshCatalog {
	return &freshCatalog{cat: c, interval: missRefreshInterval}
}

// refreshSince ensures a reload that started at or after since has
// completed.
func (f *freshCatalog) refreshSince(ctx context.Context, since time.Time) error {
	for {
		f.mu.Lock()
		if fl := f.inflight; fl != nil {
			f.mu.Unlock()
			select {
			case <-fl.done:
			case <-ctx.Done():
				return ctx.Err()
			}
			if !fl.start.Before(since) {
				return fl.err
			}
			continue
		}
		if !f.lastDone.IsZero() && !f.lastDone.Before(since) {
			f.mu.Unlock()
			return nil
		}
		if wait := f.interval - time.Since(f.lastStart); wait > 0 && !f.lastStart.IsZero() {
			f.mu.Unlock()
			t := time.NewTimer(wait)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			}
			continue
		}
		fl := &refreshFlight{start: time.Now(), done: make(chan struct{})}
		f.inflight, f.lastStart = fl, fl.start
		f.mu.Unlock()

		fl.err = f.cat.Refresh(ctx)

		f.mu.Lock()
		f.inflight = nil
		if fl.err == nil && fl.start.After(f.lastDone) {
			f.lastDone = fl.start
		}
		f.mu.Unlock()
		close(fl.done)
		return fl.err
	}
}

func isMiss(err error) bool {
	return cherr.Is(err, cherr.UnknownTable) || cherr.Is(err, cherr.UnknownDatabase)
}

// queryCatalog is the catalog as one statement sees it: lookups that miss
// the snapshot are retried after a reload that started after the statement
// did, so the statement sees every DDL committed before it started.
type queryCatalog struct {
	catalog.Catalog
	f     *freshCatalog
	ctx   context.Context
	since time.Time
}

func retryLookup[T any](c *queryCatalog, lookup func() (T, error)) (T, error) {
	v, err := lookup()
	if err == nil || !isMiss(err) || c.f == nil {
		return v, err
	}
	if rerr := c.f.refreshSince(c.ctx, c.since); rerr != nil {
		return v, err // keep the miss
	}
	return lookup()
}

func (c *queryCatalog) Database(name string) (*catalog.Database, error) {
	return retryLookup(c, func() (*catalog.Database, error) { return c.Catalog.Database(name) })
}

func (c *queryCatalog) Table(db, name string) (*catalog.Table, error) {
	t, err := c.Catalog.Table(db, name)
	if err == nil || !isMiss(err) {
		return t, err
	}
	// The name is a view in the snapshot: callers look up tables first
	// and then views, so this is not a stale miss.
	if _, verr := c.Catalog.View(db, name); verr == nil {
		return t, err
	}
	return retryLookup(c, func() (*catalog.Table, error) { return c.Catalog.Table(db, name) })
}

func (c *queryCatalog) View(db, name string) (*catalog.View, error) {
	v, err := c.Catalog.View(db, name)
	if err == nil || !isMiss(err) {
		return v, err
	}
	if _, terr := c.Catalog.Table(db, name); terr == nil {
		return v, err
	}
	return retryLookup(c, func() (*catalog.View, error) { return c.Catalog.View(db, name) })
}

// refreshTable reloads the snapshot (a reload started after the statement)
// and returns the table's current entry; used when a statement finds the
// snapshot's entry stale (e.g. a column added by ALTER on another server).
func (c *queryCatalog) refreshTable(db, name string) (*catalog.Table, error) {
	if c.f == nil {
		return nil, cherr.New(cherr.LogicalError, "catalog reload unavailable")
	}
	if err := c.f.refreshSince(c.ctx, c.since); err != nil {
		return nil, err
	}
	return c.Catalog.Table(db, name)
}
