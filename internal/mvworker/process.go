package mvworker

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/storage"
)

// Stream event names.
const (
	eventInsert = "INSERT"
	eventModify = "MODIFY"
	eventRemove = "REMOVE"
)

// applyChunk bounds the rows handed to one ApplyViews call.
const applyChunk = 256

// change is a stream record normalised from either the Streams API or a
// Lambda event.
type change struct {
	EventName string
	NewImage  map[string]ddbtypes.AttributeValue
	Seq       string
	Created   time.Time // approximate creation time (zero if unknown)
	// WriteTime is the row's write time (_ts of the new image; zero if the
	// image has none). It decides which views see the row and is preserved
	// by replication, so every region makes the same decision.
	WriteTime time.Time
}

// recordDecoder is the part of *dynamo.Store the processor needs.
type recordDecoder interface {
	DecodeRecord(t *catalog.Table, eventName string, image map[string]ddbtypes.AttributeValue) (*storage.StreamRecord, error)
}

// viewApplier is the part of *exec.Engine the processor needs.
type viewApplier interface {
	ApplyViews(ctx context.Context, t *catalog.Table, rows []storage.Row, rowIDs []string) error
}

// viewListApplier is optionally implemented by the applier: it applies only
// the given views of t (each with source t) to the rows. Without it, rows
// that some but not all of the table's views must see (written between two
// views' creation) go to every view.
type viewListApplier interface {
	ApplyViewList(ctx context.Context, t *catalog.Table, views []*catalog.View, rows []storage.Row, rowIDs []string) error
}

// processor turns source-table changes into ApplyViews calls.
type processor struct {
	cat catalog.Catalog
	dec recordDecoder
	app viewApplier
	log *slog.Logger
	now func() time.Time // nil: time.Now

	// Poison handling (poison.go): policy is PoisonDeadLetter or
	// PoisonBlock, poisonAfter how long a deterministic failure must last
	// before a record is set aside, dlq where it goes (nil: logged only),
	// origin the entry point recorded with it.
	policy      string
	poisonAfter time.Duration
	dlq         deadLetterSink
	origin      string

	// refreshMu serialises catalog reloads; loadedAt is when the newest
	// successful reload started (every DDL committed before it is visible),
	// lastReload when the newest attempt started. minReload spaces the
	// reloads ensureFresh triggers (0: no spacing).
	refreshMu  sync.Mutex
	loadedAt   time.Time
	lastReload time.Time
	minReload  time.Duration

	applied, skipped, errs, deadLettered, reloads atomic.Uint64
	lagMillis                                     atomic.Int64
	mixedWarned                                   atomic.Bool
	mergingWarned                                 sync.Map // table full name -> logged once
}

// processorOptions configures newProcessor.
type processorOptions struct {
	policy      string
	poisonAfter time.Duration
	minReload   time.Duration
	deadLetters DeadLetterAPI // nil: dead letters are only logged
	dlqTable    string
	origin      string
}

func newProcessor(cat catalog.Catalog, dec recordDecoder, app viewApplier, log *slog.Logger, o processorOptions) (*processor, error) {
	policy, after, minReload, err := resolvePoison(o.policy, o.poisonAfter, o.minReload)
	if err != nil {
		return nil, err
	}
	p := &processor{cat: cat, dec: dec, app: app, log: log, policy: policy, poisonAfter: after, minReload: minReload, origin: o.origin}
	if o.deadLetters != nil && o.dlqTable != "" {
		p.dlq = &dynamoDeadLetters{ddb: o.deadLetters, table: o.dlqTable}
	}
	return p, nil
}

func (p *processor) clock() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
}

// markLoaded records that the catalog snapshot reflects every DDL committed
// before t.
func (p *processor) markLoaded(t time.Time) {
	p.refreshMu.Lock()
	if t.After(p.loadedAt) {
		p.loadedAt = t
	}
	p.refreshMu.Unlock()
}

// refresh reloads the catalog snapshot (strongly consistent) and records
// when the reload started.
func (p *processor) refresh(ctx context.Context) error {
	p.refreshMu.Lock()
	defer p.refreshMu.Unlock()
	return p.refreshLocked(ctx)
}

func (p *processor) refreshLocked(ctx context.Context) error {
	start := p.clock()
	p.lastReload = start
	p.reloads.Add(1)
	if err := p.cat.Refresh(ctx); err != nil {
		return err
	}
	if start.After(p.loadedAt) {
		p.loadedAt = start
	}
	return nil
}

// ensureFresh reloads the catalog unless the snapshot was loaded after
// newest (the latest row write time of a batch). Without it, a record
// written right after DROP MATERIALIZED VIEW (or CREATE) could be processed
// with a snapshot that still has (or lacks) the view, until the periodic
// refresh. Concurrent callers share reloads: a reload that started after
// their newest record satisfies them.
//
// A reload is a strongly consistent Scan of the whole catalog, and under
// steady writes nearly every batch has a record newer than the last reload.
// So reloads are spaced at least minReload apart: a caller that needs one
// sooner waits (holding refreshMu) until minReload after the previous one
// started, and every batch that queued meanwhile is covered by that single
// reload. That bounds catalog Scans to about one per minReload per process,
// however many tables and shards it reads, for at most minReload of extra
// view latency.
func (p *processor) ensureFresh(ctx context.Context, newest time.Time) error {
	if newest.IsZero() {
		return nil
	}
	p.refreshMu.Lock()
	defer p.refreshMu.Unlock()
	if p.loadedAt.After(newest) {
		return nil
	}
	if !p.lastReload.IsZero() && p.minReload > 0 {
		if wait := p.lastReload.Add(p.minReload).Sub(p.clock()); wait > 0 {
			if err := sleepCtx(ctx, wait); err != nil {
				return err
			}
		}
	}
	if err := p.refreshLocked(ctx); err != nil {
		return fmt.Errorf("refresh catalog: %w", err)
	}
	return nil
}

// table resolves a physical table name, refreshing the catalog once on a
// miss (the table may have been created moments ago in another process).
func (p *processor) table(ctx context.Context, physical string) (*catalog.Table, error) {
	if t, ok := p.cat.TableByPhysicalName(physical); ok {
		return t, nil
	}
	if err := p.cat.Refresh(ctx); err != nil {
		return nil, fmt.Errorf("refresh catalog: %w", err)
	}
	t, _ := p.cat.TableByPhysicalName(physical)
	return t, nil
}

// process applies the views of physical's table to the inserted/modified
// rows among changes. It is idempotent (exactly-once markers downstream),
// so callers retry the whole slice on error.
func (p *processor) process(ctx context.Context, physical string, changes []change) error {
	err := p.processOnce(ctx, physical, changes)
	if err != nil {
		p.errs.Add(1)
	}
	return err
}

func (p *processor) processOnce(ctx context.Context, physical string, changes []change) error {
	var newestWrite time.Time
	for _, c := range changes {
		if (c.EventName == eventInsert || c.EventName == eventModify) && c.WriteTime.After(newestWrite) {
			newestWrite = c.WriteTime
		}
	}
	if err := p.ensureFresh(ctx, newestWrite); err != nil {
		return err
	}
	t, err := p.table(ctx, physical)
	if err != nil {
		return err
	}
	if t == nil {
		if q, ok := quarantinedTable(p.cat, physical); ok {
			// Not dropped but undecodable by this build: retry (blocking
			// the shard) rather than lose its rows' view output.
			return errQuarantined(q)
		}
		// Dropped table: it can have no views left.
		p.log.Warn("skipping records of a table that is not in the catalog", "table", physical, "records", len(changes))
		p.skipped.Add(uint64(len(changes)))
		return nil
	}
	views := p.cat.ViewsFrom(t.Database, t.Name)
	if q, ok := quarantinedViewOf(p.cat, t); ok {
		return errQuarantined(q)
	}
	if len(views) == 0 {
		// Its last view was dropped (the reader stops at the next reconcile).
		p.skipped.Add(uint64(len(changes)))
		return nil
	}
	if t.Engine == catalog.EngineSummingMergeTree || t.Engine == catalog.EngineAggregatingMergeTree {
		// The engine ignores views reading FROM a merging table (its stream
		// carries merged state, not the inserted rows; such views are
		// rejected at CREATE, a legacy one is never applied).
		if _, warned := p.mergingWarned.LoadOrStore(t.FullName(), true); !warned {
			p.log.Warn("skipping records of a SummingMergeTree/AggregatingMergeTree view source: its views are not applied",
				"table", t.FullName(), "views", len(views))
		}
		p.skipped.Add(uint64(len(changes)))
		return nil
	}
	var (
		rows    []storage.Row
		ids     []string
		masks   []viewMask
		skipped uint64
		newest  time.Time
	)
	for _, c := range changes {
		if c.Created.After(newest) {
			newest = c.Created
		}
		if c.EventName != eventInsert && c.EventName != eventModify {
			skipped++
			continue
		}
		rec, err := p.dec.DecodeRecord(t, c.EventName, c.NewImage)
		if err != nil {
			return &decodeError{fmt.Errorf("decode %s record %s of %s: %w", c.EventName, c.Seq, t.FullName(), err)}
		}
		if rec == nil || rec.IsMVOutput {
			skipped++
			continue
		}
		m := eligibleViews(views, c.WriteTime)
		if m.none() {
			// Written before every view of the table existed: like
			// ClickHouse, a view only sees inserts made after its creation
			// (older rows are POPULATE's job).
			skipped++
			continue
		}
		rows = append(rows, rec.Row)
		ids = append(ids, rec.RowID)
		masks = append(masks, m)
	}
	// Consecutive rows that go to the same views are applied together,
	// keeping stream order.
	for start := 0; start < len(rows); {
		end := start + 1
		for end < len(rows) && end-start < applyChunk && masks[end].equal(masks[start]) {
			end++
		}
		if err := p.apply(ctx, t, views, masks[start], rows[start:end], ids[start:end]); err != nil {
			return fmt.Errorf("apply views of %s: %w", t.FullName(), err)
		}
		start = end
	}
	p.applied.Add(uint64(len(rows)))
	p.skipped.Add(skipped)
	if !newest.IsZero() {
		p.lagMillis.Store(time.Since(newest).Milliseconds())
	}
	return nil
}

// apply applies the views selected by m to rows.
func (p *processor) apply(ctx context.Context, t *catalog.Table, views []*catalog.View, m viewMask, rows []storage.Row, ids []string) error {
	if m.all() {
		return p.app.ApplyViews(ctx, t, rows, ids)
	}
	subset := m.selectFrom(views)
	if la, ok := p.app.(viewListApplier); ok {
		return la.ApplyViewList(ctx, t, subset, rows, ids)
	}
	if !p.mixedWarned.Swap(true) {
		p.log.Warn("engine cannot apply a subset of a table's views; rows written before a newer view was created are applied to it too",
			"source", t.FullName(), "rows", len(rows))
	}
	return p.app.ApplyViews(ctx, t, rows, ids)
}

// quarantinedTable finds a quarantined (undecodable) catalog entry that
// may be the table stored as physical: a table entry of that physical name
// or of unknown physical name, or a malformed item.
func quarantinedTable(cat catalog.Catalog, physical string) (catalog.Quarantined, bool) {
	for _, q := range catalog.QuarantinedEntries(cat) {
		switch q.Kind {
		case catalog.KindTable:
			if q.PhysicalName == physical || q.PhysicalName == "" {
				return q, true
			}
		case catalog.KindMalformed:
			return q, true
		}
	}
	return catalog.Quarantined{}, false
}

// quarantinedViewOf finds a quarantined view entry that may read FROM t
// (its source is t or unknown), or a malformed item.
func quarantinedViewOf(cat catalog.Catalog, t *catalog.Table) (catalog.Quarantined, bool) {
	for _, q := range catalog.QuarantinedEntries(cat) {
		switch q.Kind {
		case catalog.KindView:
			if q.SourceTable == "" || q.SourceDatabase == t.Database && q.SourceTable == t.Name {
				return q, true
			}
		case catalog.KindMalformed:
			return q, true
		}
	}
	return catalog.Quarantined{}, false
}

// errQuarantined is retried (UNAVAILABLE, never dead-lettered): the records
// wait until a build that can decode the entry processes them.
func errQuarantined(q catalog.Quarantined) error {
	return cherr.Wrap(cherr.Unavailable, q.Err, "catalog entry %s cannot be decoded by this build; its records wait", q.Key)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// viewMask marks, per view of a source table, whether a row goes to it
// (nil: every view).
type viewMask []bool

// eligibleViews selects the views created at or before the row's write time.
// Rows without a write time, and views without a creation time (catalog
// entries written by older versions), are not filtered.
func eligibleViews(views []*catalog.View, written time.Time) viewMask {
	if written.IsZero() {
		return nil
	}
	var m viewMask
	for i, v := range views {
		ok := v.CreatedAt.IsZero() || !written.Before(v.CreatedAt)
		if !ok && m == nil {
			m = make(viewMask, len(views))
			for j := range i {
				m[j] = true
			}
		}
		if m != nil {
			m[i] = ok
		}
	}
	return m
}

func (m viewMask) all() bool { return m == nil }

func (m viewMask) none() bool {
	if m == nil {
		return false
	}
	for _, ok := range m {
		if ok {
			return false
		}
	}
	return true
}

func (m viewMask) equal(o viewMask) bool {
	if m.all() || o.all() {
		return m.all() == o.all()
	}
	return slices.Equal(m, o)
}

func (m viewMask) selectFrom(views []*catalog.View) []*catalog.View {
	var out []*catalog.View
	for i, v := range views {
		if m[i] {
			out = append(out, v)
		}
	}
	return out
}
