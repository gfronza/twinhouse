package server

import (
	"context"
	"errors"
	"hash/fnv"
	"log/slog"
	"time"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/storage"
)

// ReconcilerConfig configures a Reconciler.
type ReconcilerConfig struct {
	Catalog catalog.Catalog
	Store   storage.Store
	// Interval between rounds (CATALOG_REFRESH; default 2s).
	Interval time.Duration
	// DropAfter is how many consecutive successful refreshes a table must be
	// missing from the catalog before its physical table is dropped
	// (default 2), so a transient catalog glitch never deletes data.
	DropAfter int
	// RefreshTimeout bounds one catalog refresh (default 1m), so an
	// unresponsive DynamoDB endpoint cannot stall the reconciler forever.
	RefreshTimeout time.Duration
	// TableTimeout bounds one EnsureTable/DropTable call (default 10m: on AWS
	// creating a table with global-table replicas can take minutes).
	TableTimeout time.Duration
	Logger       *slog.Logger
}

// Reconciler keeps this region's physical tables in line with the global
// catalog: it periodically refreshes the catalog snapshot, ensures every
// catalog table exists physically (Store.EnsureTable, once per table
// definition), and drops the physical tables of catalog entries that
// disappeared (e.g. a DROP TABLE executed in another region).
//
// Only tables this process has observed in the catalog are ever dropped,
// and only after the catalog was re-read right before the drop and still
// does not contain them; errors are logged and retried on the next round.
type Reconciler struct {
	cfg ReconcilerConfig
	log *slog.Logger
	// ensured maps physical name -> definition successfully ensured.
	ensured map[string]tableIdentity
	// known maps physical name -> last catalog definition seen.
	known map[string]*catalog.Table
	// missing counts consecutive rounds a known table was absent.
	missing map[string]int
	// quarantineLogged records the quarantined catalog keys already logged.
	quarantineLogged map[string]bool
}

// tableIdentity identifies one catalog definition of a physical table. The
// version alone is not enough: a table dropped and created again starts over
// at version 1 (a new incarnation, told apart by CreatedAt), and two regions
// that change the same table concurrently can both write the same version
// with different definitions (catalog last-writer-wins), so the definition
// itself is fingerprinted too.
type tableIdentity struct {
	version int64
	created int64
	def     uint64
}

func identityOf(t *catalog.Table) tableIdentity {
	id := tableIdentity{version: t.Version, created: t.CreatedAt.UnixNano()}
	if b, err := catalog.EncodeTable(t); err == nil {
		h := fnv.New64a()
		_, _ = h.Write(b)
		id.def = h.Sum64()
	}
	return id
}

// NewReconciler creates a Reconciler.
func NewReconciler(cfg ReconcilerConfig) *Reconciler {
	if cfg.Interval <= 0 {
		cfg.Interval = 2 * time.Second
	}
	if cfg.DropAfter <= 0 {
		cfg.DropAfter = 2
	}
	if cfg.RefreshTimeout <= 0 {
		cfg.RefreshTimeout = time.Minute
	}
	if cfg.TableTimeout <= 0 {
		cfg.TableTimeout = 10 * time.Minute
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Reconciler{
		cfg:     cfg,
		log:     log.With("component", "reconciler"),
		ensured: map[string]tableIdentity{},
		known:   map[string]*catalog.Table{},
		missing: map[string]int{},

		quarantineLogged: map[string]bool{},
	}
}

// Run reconciles immediately and then every Interval until ctx is done.
func (r *Reconciler) Run(ctx context.Context) {
	t := time.NewTicker(r.cfg.Interval)
	defer t.Stop()
	for {
		r.RunOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (r *Reconciler) refresh(ctx context.Context) error {
	rctx, cancel := context.WithTimeout(ctx, r.cfg.RefreshTimeout)
	defer cancel()
	return r.cfg.Catalog.Refresh(rctx)
}

// RunOnce performs one reconciliation round. It never panics on store or
// catalog errors; they are logged.
func (r *Reconciler) RunOnce(ctx context.Context) {
	defer func() {
		if p := recover(); p != nil {
			r.log.Error("reconcile round panicked", "panic", p)
		}
	}()
	if err := r.refresh(ctx); err != nil {
		if ctx.Err() == nil {
			r.log.Warn("catalog refresh failed", "error", err)
		}
		return
	}

	r.quarantined() // logs newly quarantined catalog entries

	current := r.currentTables()
	for name, t := range current {
		if ctx.Err() != nil {
			return
		}
		r.observe(name, t)
		r.ensure(ctx, name, t)
	}

	var candidates []string
	for name := range r.known {
		if _, ok := current[name]; ok {
			continue
		}
		r.missing[name]++
		if r.missing[name] >= r.cfg.DropAfter {
			candidates = append(candidates, name)
		}
	}
	if len(candidates) == 0 || ctx.Err() != nil {
		return
	}
	// Re-read the catalog right before dropping: a table created (in this
	// or another process) since the scan above, reusing a physical name
	// that was missing for DropAfter rounds, must not lose its new physical
	// table.
	if err := r.refresh(ctx); err != nil {
		if ctx.Err() == nil {
			r.log.Warn("catalog refresh before drop failed", "error", err)
		}
		return
	}
	protected, all := r.quarantined()
	if all {
		// An undecodable entry whose physical table is unknown could be
		// any of the candidates: drop nothing until it is readable.
		return
	}
	for _, name := range candidates {
		if ctx.Err() != nil {
			return
		}
		if protected[name] {
			continue
		}
		if t, ok := r.cfg.Catalog.TableByPhysicalName(name); ok && t != nil {
			delete(r.missing, name)
			r.observe(name, t)
			r.ensure(ctx, name, t)
			continue
		}
		t := r.known[name]
		dctx, cancel := context.WithTimeout(ctx, r.cfg.TableTimeout)
		err := r.cfg.Store.DropTable(dctx, t)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				r.log.Warn("drop table failed", "table", t.FullName(), "physical", name, "error", err)
			}
			continue
		}
		r.log.Info("dropped physical table removed from catalog", "table", t.FullName(), "physical", name)
		delete(r.known, name)
		delete(r.ensured, name)
		delete(r.missing, name)
	}
}

// quarantined returns the physical names of quarantined (undecodable)
// table entries, which must never be dropped, and whether an undecodable
// entry's physical table is unknown (then nothing may be dropped). Each
// quarantined entry is logged once.
func (r *Reconciler) quarantined() (protected map[string]bool, all bool) {
	qs := catalog.QuarantinedEntries(r.cfg.Catalog)
	protected = map[string]bool{}
	for _, q := range qs {
		if !r.quarantineLogged[q.Key] {
			r.quarantineLogged[q.Key] = true
			r.log.Error("catalog entry cannot be decoded; it is ignored and its physical table is kept (upgrade this server?)",
				"key", q.Key, "kind", q.Kind, "physical", q.PhysicalName, "error", q.Err)
		}
		switch q.Kind {
		case catalog.KindTable:
			if q.PhysicalName == "" {
				all = true
			}
			protected[q.PhysicalName] = true
		case catalog.KindMalformed:
			all = true
		}
	}
	return protected, all
}

// currentTables maps physical name -> catalog table for the current snapshot.
func (r *Reconciler) currentTables() map[string]*catalog.Table {
	current := map[string]*catalog.Table{}
	for _, db := range r.databases() {
		for _, t := range r.cfg.Catalog.Tables(db) {
			if t == nil || t.PhysicalName == "" {
				continue
			}
			current[t.PhysicalName] = t
		}
	}
	return current
}

// observe records t as the current definition of physical table name.
func (r *Reconciler) observe(name string, t *catalog.Table) {
	if old := r.known[name]; old != nil && !old.CreatedAt.IsZero() && !t.CreatedAt.Equal(old.CreatedAt) {
		// The table was dropped and created again under the same physical
		// name between two rounds. The physical table cannot be safely
		// replaced here (other processes of this region, the stream worker
		// and the replicator may already be writing the new incarnation into
		// it), so rows of the previous incarnation stay; per-incarnation
		// physical names avoid the reuse.
		r.log.Warn("table recreated under the same physical name; rows of the previous incarnation may remain in this region",
			"table", t.FullName(), "physical", name,
			"old_created_at", old.CreatedAt, "new_created_at", t.CreatedAt)
	}
	r.known[name] = t
	delete(r.missing, name)
}

// ensure runs Store.EnsureTable unless this exact definition was ensured.
func (r *Reconciler) ensure(ctx context.Context, name string, t *catalog.Table) {
	id := identityOf(t)
	if v, ok := r.ensured[name]; ok && v == id {
		return
	}
	ectx, cancel := context.WithTimeout(ctx, r.cfg.TableTimeout)
	err := r.cfg.Store.EnsureTable(ectx, t)
	cancel()
	if err != nil {
		switch {
		case ctx.Err() != nil:
		case errors.Is(err, storage.ErrAwaitingReplica):
			// Expected while the home region adds this region's replica.
			r.log.Debug("ensure table pending", "table", t.FullName(), "physical", name, "error", err)
		default:
			r.log.Warn("ensure table failed", "table", t.FullName(), "physical", name, "error", err)
		}
		return
	}
	r.ensured[name] = id
	r.log.Debug("table ensured", "table", t.FullName(), "physical", name, "version", t.Version)
}

// databases lists catalog databases; "default" always exists.
func (r *Reconciler) databases() []string {
	names := []string{"default"}
	for _, d := range r.cfg.Catalog.Databases() {
		if d != nil && d.Name != "default" {
			names = append(names, d.Name)
		}
	}
	return names
}
