package mvworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/streams"
)

// Consumer is the checkpoint consumer name of the stream worker.
const Consumer = "mvworker"

type workerImpl struct {
	cfg  Config
	proc *processor
	log  *slog.Logger

	mu      sync.Mutex
	readers map[string]*tableReader // by physical table name
}

type tableReader struct {
	reader *streams.Reader
	cancel context.CancelFunc
	done   chan struct{}
}

func newWorker(cfg Config) (*Worker, error) {
	switch {
	case cfg.Clients == nil || cfg.Clients.DynamoDB == nil || cfg.Clients.Streams == nil:
		return nil, errors.New("mvworker: Clients are required")
	case cfg.Store == nil:
		return nil, errors.New("mvworker: Store is required")
	case cfg.Catalog == nil:
		return nil, errors.New("mvworker: Catalog is required")
	case cfg.Engine == nil:
		return nil, errors.New("mvworker: Engine is required")
	}
	if cfg.Owner == "" {
		cfg.Owner = DefaultOwner()
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 250 * time.Millisecond
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.CatalogRefresh <= 0 {
		cfg.CatalogRefresh = 2 * time.Second
	}
	log := slog.Default().With("component", "mvworker")
	proc, err := newProcessor(cfg.Catalog, cfg.Store, cfg.Engine, log, processorOptions{
		policy: cfg.PoisonPolicy, poisonAfter: cfg.PoisonAfter, minReload: cfg.CatalogMinReload,
		deadLetters: cfg.Clients.DynamoDB, dlqTable: DeadLetterTableName(cfg.Store.CheckpointsTableName()), origin: "stream",
	})
	if err != nil {
		return nil, err
	}
	return &Worker{impl: &workerImpl{
		cfg:     cfg,
		log:     log,
		proc:    proc,
		readers: map[string]*tableReader{},
	}}, nil
}

// DefaultOwner returns "hostname:pid", a lease owner id unique per process.
func DefaultOwner() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		h = "unknown"
	}
	return fmt.Sprintf("%s:%d", h, os.Getpid())
}

func (w *Worker) run(ctx context.Context) error {
	wi := w.impl
	defer wi.stopAll()
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		if err := wi.proc.refresh(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			wi.log.Warn("catalog refresh failed; using last snapshot", "err", err)
		}
		wi.reconcile(ctx, sourceTables(wi.cfg.Catalog))
		t.Reset(wi.cfg.CatalogRefresh)
	}
}

// sourceTables returns the physical names of tables that have materialized
// views, sorted. Summing/AggregatingMergeTree tables are skipped: the engine
// ignores views on merging sources (a legacy catalog may still hold one),
// and their regional tables have no stream to read.
func sourceTables(cat catalog.Catalog) []string {
	var out []string
	for _, db := range cat.Databases() {
		for _, t := range cat.Tables(db.Name) {
			if t.Engine == catalog.EngineSummingMergeTree || t.Engine == catalog.EngineAggregatingMergeTree {
				continue
			}
			if len(cat.ViewsFrom(t.Database, t.Name)) > 0 && t.PhysicalName != "" {
				out = append(out, t.PhysicalName)
			}
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// reconcile runs one stream reader per source table.
func (wi *workerImpl) reconcile(ctx context.Context, tables []string) {
	want := map[string]bool{}
	for _, t := range tables {
		want[t] = true
	}
	wi.mu.Lock()
	defer wi.mu.Unlock()
	for name, tr := range wi.readers {
		if !want[name] {
			wi.log.Info("table no longer has materialized views; stopping its stream", "table", name)
			tr.cancel()
			<-tr.done
			delete(wi.readers, name)
		}
	}
	for name := range want {
		if _, ok := wi.readers[name]; ok {
			continue
		}
		r, err := streams.New(streams.Config{
			Streams:         wi.cfg.Clients.Streams,
			DynamoDB:        wi.cfg.Clients.DynamoDB,
			Table:           name,
			CheckpointTable: wi.cfg.Store.CheckpointsTableName(),
			Consumer:        Consumer,
			Owner:           wi.cfg.Owner,
			Handler:         wi.handler(name),
			PollInterval:    wi.cfg.PollInterval,
			MaxIdleBackoff:  max(4*wi.cfg.PollInterval, time.Second),
			LeaseDuration:   wi.cfg.LeaseDuration,
			// DescribeStream is limited to ~10 TPS per account: rediscover
			// shards (which split every few hours) at a modest pace.
			DiscoveryInterval: max(wi.cfg.CatalogRefresh, 5*time.Second),
			Logger:            wi.log,
		})
		if err != nil {
			wi.log.Error("cannot create stream reader", "table", name, "err", err)
			continue
		}
		rctx, cancel := context.WithCancel(ctx)
		tr := &tableReader{reader: r, cancel: cancel, done: make(chan struct{})}
		wi.readers[name] = tr
		wi.log.Info("applying materialized views from stream", "table", name)
		go func() {
			defer close(tr.done)
			_ = r.Run(rctx)
		}()
	}
}

func (wi *workerImpl) stopAll() {
	wi.mu.Lock()
	defer wi.mu.Unlock()
	for name, tr := range wi.readers {
		tr.cancel()
		<-tr.done
		delete(wi.readers, name)
	}
}

func (wi *workerImpl) handler(physical string) streams.Handler {
	return func(ctx context.Context, b *streams.Batch) error {
		changes, err := changesFromStream(b.Records)
		if err != nil {
			wi.proc.errs.Add(1)
			return err
		}
		return wi.proc.handle(ctx, physical, changes, &b.Scratch)
	}
}

func (w *Worker) stats() Stats {
	wi := w.impl
	s := Stats{
		RecordsApplied: wi.proc.applied.Load(),
		RecordsSkipped: wi.proc.skipped.Load(),
		Errors:         wi.proc.errs.Load(),
		LagMillis:      wi.proc.lagMillis.Load(),
		DeadLettered:   wi.proc.deadLettered.Load(),
		CatalogReloads: wi.proc.reloads.Load(),
	}
	var oldest time.Time
	wi.mu.Lock()
	for _, tr := range wi.readers {
		rs := tr.reader.Stats()
		s.Shards += int(rs.ActiveShards)
		s.RetryingBatches += rs.RetryingBatches
		if !rs.OldestPendingAt.IsZero() && (oldest.IsZero() || rs.OldestPendingAt.Before(oldest)) {
			oldest = rs.OldestPendingAt
		}
	}
	wi.mu.Unlock()
	if !oldest.IsZero() {
		s.PendingAgeMillis = max(time.Since(oldest).Milliseconds(), 0)
	}
	return s
}
