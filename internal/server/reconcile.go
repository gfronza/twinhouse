package server

import (
	"context"
	"errors"
	"hash/fnv"
	"log/slog"
	"time"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/storage"
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

	// Consumers, when set, wires the consumers of every table's DynamoDB
	// stream (Lambda event-source mappings in the serverless deployment,
	// internal/esm): EnsureConsumers runs after a table was ensured, for
	// tables whose stream ARN is known, and RemoveConsumers right before a
	// physical table is dropped (the drop waits for it to succeed, so no
	// mapping outlives its table). nil: streams are consumed by long-running
	// workers that discover them from the catalog themselves.
	Consumers StreamConsumers
	// StreamARN resolves a physical table's latest stream ARN ("" when the
	// table has no stream). Default: Store when it implements StreamARNs
	// (dynamo.Store does); required otherwise when Consumers is set.
	StreamARN func(ctx context.Context, physical string) (string, error)

	// Physical, when set, lets the reconciler find physical tables whose
	// catalog entry is gone although no round of this process observed the
	// entry (the process started after the DROP: a Lambda execution
	// environment, a restarted server), and drop them. Default: Store when
	// it implements PhysicalTables (dynamo.Store does). The same applies
	// to the tables whose streams still have consumers (Consumers
	// implementing ConsumedTables). Both are polled every DiscoverInterval.
	Physical PhysicalTables
	// DiscoverInterval is how often the physical tables and the consumed
	// streams are listed (default 1m; a Lambda reconciler, invoked about
	// once a minute, lists them every round). 0 is the default; a negative
	// value disables discovery.
	DiscoverInterval time.Duration
}

// PhysicalTables is the optional Store capability the reconciler uses to
// find and drop this region's physical row tables that no catalog entry
// names (dynamo.Store implements it). ListPhysicalTables returns only row
// tables of this deployment (no system or side tables; dynamo.Store also
// requires the per-incarnation name shape, so tables of another deployment
// whose prefix starts with this one's are never listed). DropPhysicalTable
// drops a row table with its replicas and side tables (idempotent).
type PhysicalTables interface {
	ListPhysicalTables(ctx context.Context) ([]string, error)
	DropPhysicalTable(ctx context.Context, physical string) error
}

// ConsumedTables is the optional StreamConsumers capability listing the
// physical tables whose streams currently have consumers (esm.Consumers
// implements it), so the consumers of tables that vanished from the
// catalog while no reconciler observed them are removed too.
type ConsumedTables interface {
	ConsumedTables(ctx context.Context) ([]string, error)
}

// StreamConsumers wires the consumers of a table's DynamoDB stream. Both
// methods must be idempotent: the reconciler calls them again on failure
// (next round) and after every change of the table's definition.
type StreamConsumers interface {
	// EnsureConsumers makes t's stream (streamARN, non-empty) consumed by
	// whatever should consume it (the implementation decides from the
	// table's engine and the deployment, and may do nothing).
	EnsureConsumers(ctx context.Context, t *catalog.Table, streamARN string) error
	// RemoveConsumers removes every consumer of the stream of the physical
	// table physicalName; streamARN is "" when it could not be resolved
	// (e.g. the table is already gone).
	RemoveConsumers(ctx context.Context, physicalName string, streamARN string) error
}

// StreamARNs is the optional Store capability the reconciler uses to find
// a table's stream for Consumers (dynamo.Store implements it).
type StreamARNs interface {
	StreamARN(ctx context.Context, physical string) (string, error)
}

// Reconciler keeps this region's physical tables in line with the global
// catalog: it periodically refreshes the catalog snapshot, ensures every
// catalog table exists physically (Store.EnsureTable, once per table
// definition), and drops the physical tables of catalog entries that
// disappeared (e.g. a DROP TABLE executed in another region).
//
// A physical table is dropped only when it was observed (in the catalog by
// this process, or, with PhysicalTables / ConsumedTables, as an existing
// table or a consumed stream of this deployment), has been missing from
// the catalog for DropAfter rounds, and the catalog was re-read right
// before the drop and still does not contain it; errors are logged and
// retried on the next round.
type Reconciler struct {
	cfg ReconcilerConfig
	log *slog.Logger
	// ensured maps physical name -> definition successfully ensured.
	ensured map[string]tableIdentity
	// wired maps physical name -> definition whose stream consumers were
	// ensured (only used with cfg.Consumers).
	wired map[string]tableIdentity
	// unwired records the physical names whose consumers were removed in
	// preparation of a drop that has not completed yet, so the removal
	// is not repeated every round while the drop is deferred.
	unwired map[string]bool
	// known maps physical name -> last catalog definition seen; nil for a
	// table discovered physically (or by its consumers) whose definition
	// this process never saw.
	known map[string]*catalog.Table
	// missing counts consecutive rounds a known table was absent.
	missing map[string]int
	// quarantineLogged records the quarantined catalog keys already logged.
	quarantineLogged map[string]bool
	// lastDiscover is when the physical tables were last listed.
	lastDiscover time.Time
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
	if cfg.StreamARN == nil {
		if s, ok := cfg.Store.(StreamARNs); ok {
			cfg.StreamARN = s.StreamARN
		}
	}
	if cfg.Consumers != nil && cfg.StreamARN == nil {
		log.Error("reconciler: Consumers set but the store cannot resolve stream ARNs; stream consumers are not managed")
		cfg.Consumers = nil
	}
	if cfg.Physical == nil {
		if p, ok := cfg.Store.(PhysicalTables); ok {
			cfg.Physical = p
		}
	}
	if cfg.DiscoverInterval == 0 {
		cfg.DiscoverInterval = time.Minute
	}
	return &Reconciler{
		cfg:     cfg,
		log:     log.With("component", "reconciler"),
		ensured: map[string]tableIdentity{},
		wired:   map[string]tableIdentity{},
		unwired: map[string]bool{},
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
	r.discover(ctx, current)

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
		if !r.unwire(ctx, name, t) {
			continue // retried next round: no drop while consumers remain
		}
		if err := r.drop(ctx, name, t); err != nil {
			switch {
			case ctx.Err() != nil:
			case errors.Is(err, storage.ErrDeleteDeferred):
				// DynamoDB refuses to delete the source table of a global
				// table within 24 hours of adding a replica: retried every
				// round until the window passes.
				r.log.Debug("drop table deferred", "table", fullName(t), "physical", name, "error", err)
			default:
				r.log.Warn("drop table failed", "table", fullName(t), "physical", name, "error", err)
			}
			continue
		}
		r.log.Info("dropped physical table removed from catalog", "table", fullName(t), "physical", name)
		r.forget(name)
	}
}

// drop deletes the physical table name: through the store with its catalog
// definition t when this process saw one, else by physical name
// (PhysicalTables). Without that capability a discovered table is only
// unwired and forgotten.
func (r *Reconciler) drop(ctx context.Context, name string, t *catalog.Table) error {
	dctx, cancel := context.WithTimeout(ctx, r.cfg.TableTimeout)
	defer cancel()
	if t != nil {
		return r.cfg.Store.DropTable(dctx, t)
	}
	if r.cfg.Physical == nil {
		return nil
	}
	return r.cfg.Physical.DropPhysicalTable(dctx, name)
}

// forget clears every record of a dropped physical table.
func (r *Reconciler) forget(name string) {
	delete(r.known, name)
	delete(r.ensured, name)
	delete(r.wired, name)
	delete(r.unwired, name)
	delete(r.missing, name)
}

// fullName renders t for logs ("" for a table known only physically).
func fullName(t *catalog.Table) string {
	if t == nil {
		return ""
	}
	return t.FullName()
}

// discover adds to the drop candidates this region's physical row tables
// and consumed streams that the current catalog does not name, so tables
// dropped while no round of this process observed their catalog entry
// (the process started after the DROP) are cleaned up too. They enter the
// same path as a vanished catalog entry: DropAfter rounds missing, the
// catalog re-read right before the drop, quarantine protection.
func (r *Reconciler) discover(ctx context.Context, current map[string]*catalog.Table) {
	if r.cfg.DiscoverInterval < 0 || (r.cfg.Physical == nil && r.cfg.Consumers == nil) {
		return
	}
	if !r.lastDiscover.IsZero() && time.Since(r.lastDiscover) < r.cfg.DiscoverInterval {
		return
	}
	add := func(kind string, names []string) {
		for _, name := range names {
			if name == "" {
				continue
			}
			if _, ok := current[name]; ok {
				continue
			}
			if _, ok := r.known[name]; ok {
				continue
			}
			r.known[name] = nil
			r.log.Info("physical table without a catalog entry found; dropped unless the catalog names it again",
				"physical", name, "found_by", kind)
		}
	}
	done := true
	if r.cfg.Physical != nil {
		lctx, cancel := context.WithTimeout(ctx, r.cfg.RefreshTimeout)
		names, err := r.cfg.Physical.ListPhysicalTables(lctx)
		cancel()
		if err != nil {
			done = false
			if ctx.Err() == nil {
				r.log.Warn("cannot list physical tables", "error", err)
			}
		} else {
			add("list-tables", names)
		}
	}
	if c, ok := r.cfg.Consumers.(ConsumedTables); ok {
		lctx, cancel := context.WithTimeout(ctx, r.cfg.RefreshTimeout)
		names, err := c.ConsumedTables(lctx)
		cancel()
		if err != nil {
			done = false
			if ctx.Err() == nil {
				r.log.Warn("cannot list consumed streams", "error", err)
			}
		} else {
			add("stream-consumers", names)
		}
	}
	if done {
		r.lastDiscover = time.Now()
	}
}

// streamARN resolves the stream ARN of physical table name, bounded like a
// catalog refresh (it is one DescribeTable call).
func (r *Reconciler) streamARN(ctx context.Context, name string) (string, error) {
	sctx, cancel := context.WithTimeout(ctx, r.cfg.RefreshTimeout)
	defer cancel()
	return r.cfg.StreamARN(sctx, name)
}

// wire ensures the stream consumers of t (definition id) unless already
// done for this definition. A table without a stream needs none.
func (r *Reconciler) wire(ctx context.Context, name string, t *catalog.Table, id tableIdentity) {
	if r.cfg.Consumers == nil {
		return
	}
	if v, ok := r.wired[name]; ok && v == id {
		return
	}
	arn, err := r.streamARN(ctx, name)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("cannot resolve table stream", "table", t.FullName(), "physical", name, "error", err)
		}
		return
	}
	if arn != "" {
		cctx, cancel := context.WithTimeout(ctx, r.cfg.TableTimeout)
		err = r.cfg.Consumers.EnsureConsumers(cctx, t, arn)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				r.log.Warn("ensure stream consumers failed", "table", t.FullName(), "physical", name, "stream", arn, "error", err)
			}
			return
		}
	}
	r.wired[name] = id
	r.log.Debug("stream consumers ensured", "table", t.FullName(), "physical", name, "stream", arn)
}

// unwire removes the stream consumers of the physical table name before it
// is dropped; it reports whether the drop may proceed.
func (r *Reconciler) unwire(ctx context.Context, name string, t *catalog.Table) bool {
	if r.cfg.Consumers == nil || r.unwired[name] {
		return true
	}
	arn, err := r.streamARN(ctx, name)
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		arn = "" // unknown (the table may be gone already): remove by table name
	}
	cctx, cancel := context.WithTimeout(ctx, r.cfg.TableTimeout)
	err = r.cfg.Consumers.RemoveConsumers(cctx, name, arn)
	cancel()
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("remove stream consumers failed; drop postponed", "table", fullName(t), "physical", name, "stream", arn, "error", err)
		}
		return false
	}
	delete(r.wired, name)
	r.unwired[name] = true
	return true
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
	delete(r.unwired, name)
}

// ensure runs Store.EnsureTable unless this exact definition was ensured,
// then wires the table's stream consumers (cfg.Consumers).
func (r *Reconciler) ensure(ctx context.Context, name string, t *catalog.Table) {
	id := identityOf(t)
	if v, ok := r.ensured[name]; !ok || v != id {
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
	r.wire(ctx, name, t, id)
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
