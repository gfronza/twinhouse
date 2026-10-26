// Package mvworker tails the DynamoDB streams of materialized-view source
// tables and applies the views to region-local target tables (DESIGN.md §5).
package mvworker

import (
	"context"
	"time"

	"github.com/gfronza/twinhouse/internal/awsx"
	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/exec"
	"github.com/gfronza/twinhouse/internal/storage/dynamo"
)

// Config configures a Worker.
type Config struct {
	Clients *awsx.Clients
	Store   *dynamo.Store
	Catalog catalog.Catalog
	Engine  *exec.Engine
	// Owner identifies this worker for shard leases (e.g. hostname:pid).
	Owner string
	// PollInterval is the idle wait between GetRecords calls (default 250ms).
	PollInterval time.Duration
	// LeaseDuration is the shard lease TTL (default 30s).
	LeaseDuration time.Duration
	// CatalogRefresh is how often to rediscover source tables (default 2s).
	CatalogRefresh time.Duration
	// CatalogMinReload spaces the catalog reloads a batch newer than the
	// snapshot triggers (default CATALOG_MIN_RELOAD or 1s; DESIGN.md §5).
	CatalogMinReload time.Duration
	// PoisonPolicy is PoisonDeadLetter (default, or MV_POISON_POLICY) or
	// PoisonBlock; PoisonAfter how long a record must keep failing with a
	// deterministic error before it is dead-lettered (default
	// MV_POISON_AFTER or 30s).
	PoisonPolicy string
	PoisonAfter  time.Duration
}

// Worker applies materialized views from DynamoDB streams.
type Worker struct {
	impl *workerImpl
}

// New creates a worker.
func New(cfg Config) (*Worker, error) { return newWorker(cfg) }

// Run blocks until ctx is cancelled, processing every view-source table's stream.
func (w *Worker) Run(ctx context.Context) error { return w.run(ctx) }

// Stats is a snapshot of worker progress (for tests and /metrics-style logs).
type Stats struct {
	RecordsApplied uint64
	RecordsSkipped uint64
	Errors         uint64
	Shards         int
	// LagMillis is the age of the newest processed record when processed.
	LagMillis int64
	// DeadLettered counts records set aside after failing deterministically.
	DeadLettered uint64
	// CatalogReloads counts catalog reloads (strongly consistent Scans).
	CatalogReloads uint64
	// RetryingBatches is the number of batches being retried after a
	// failure; PendingAgeMillis the age of the oldest record among them (0
	// when none). Alert well before it reaches the 24h stream retention.
	RetryingBatches  int
	PendingAgeMillis int64
}

// Stats returns current counters.
func (w *Worker) Stats() Stats { return w.stats() }
