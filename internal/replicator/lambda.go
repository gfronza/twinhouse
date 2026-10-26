package replicator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/mvworker"
)

// TableLookup resolves a physical table name to its replication spec. ok is
// false for a table the replicator must not copy (unknown, dropped, or not a
// replicator-copied table); an error (e.g. the catalog cannot be read) makes
// the records wait.
type TableLookup func(ctx context.Context, physical string) (spec TableSpec, ok bool, err error)

// CatalogLookup returns a TableLookup over cat for GLOBAL_MODE=aws: it
// resolves global ReplacingMergeTree tables (catalog.CopiedByReplicator),
// the ones real global tables do not replicate, with their version column.
// The snapshot is refreshed when older than maxAge (default 5s) and once
// more when a table is missing from it (a table created just before its
// first rows, in case the periodic refresh has not seen it yet). A table
// still missing then was dropped, unless a quarantined catalog entry may be
// it: those records wait (UNAVAILABLE) for a build that can decode it.
func CatalogLookup(cat catalog.Catalog, maxAge time.Duration, log *slog.Logger) TableLookup {
	if maxAge <= 0 {
		maxAge = 5 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	var mu sync.Mutex
	var refreshed time.Time
	refresh := func(ctx context.Context) error {
		if err := cat.Refresh(ctx); err != nil {
			return err
		}
		refreshed = time.Now()
		return nil
	}
	return func(ctx context.Context, physical string) (TableSpec, bool, error) {
		mu.Lock()
		defer mu.Unlock()
		fresh := false
		if time.Since(refreshed) > maxAge {
			if err := refresh(ctx); err != nil {
				if refreshed.IsZero() {
					return TableSpec{}, false, fmt.Errorf("load catalog: %w", err)
				}
				log.Warn("catalog refresh failed; using last snapshot", "err", err)
			} else {
				fresh = true
			}
		}
		t, ok := cat.TableByPhysicalName(physical)
		if !ok && !fresh {
			if err := refresh(ctx); err != nil {
				return TableSpec{}, false, fmt.Errorf("load catalog: %w", err)
			}
			t, ok = cat.TableByPhysicalName(physical)
		}
		if !ok {
			if q, quarantined := quarantinedTable(cat, physical); quarantined {
				return TableSpec{}, false, cherr.Wrap(cherr.Unavailable, q.Err,
					"catalog entry %s cannot be decoded by this build; its records wait", q.Key)
			}
			return TableSpec{}, false, nil
		}
		if t.Replication != catalog.ReplicationGlobal || !catalog.CopiedByReplicator(t) {
			return TableSpec{}, false, nil
		}
		return TableSpec{Name: physical, VersionColumn: t.VersionColumn}, true, nil
	}
}

// quarantinedTable finds a quarantined catalog entry that may be the table
// stored as physical (as the stream worker does).
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

// Target is one region a Lambda copies rows to.
type Target struct {
	Name string
	// Client is the region's DynamoDB client (awsx.Clients.DynamoDB).
	Client WriteAPI
}

// LambdaConfig configures the Lambda entry point.
type LambdaConfig struct {
	// Region is this region: the Lambda consumes the streams of this
	// region's ReplacingMergeTree tables and copies the rows written here
	// (_origin == Region) to every target; replicas written by the other
	// regions' replicators are skipped.
	Region string
	// Targets are the other regions (REPLICA_REGIONS) with their clients.
	Targets []Target
	// Tables resolves a stream's table (CatalogLookup).
	Tables TableLookup
	// OnConflict is as in Config.
	OnConflict ConflictHook
	// Parallelism bounds concurrent writes per invocation (default 8);
	// records of the same item are always applied in stream order.
	Parallelism int
	Logger      *slog.Logger
}

// Lambda replicates ReplacingMergeTree tables from a DynamoDB stream
// event-source mapping (GLOBAL_MODE=aws), one function per region, where
// the poller (Replicator) would need a long-running process.
type Lambda struct {
	cfg LambdaConfig
	log *slog.Logger
	counters
}

// NewLambda validates cfg and returns the entry point; pass Handle to the
// Lambda runtime.
func NewLambda(cfg LambdaConfig) (*Lambda, error) {
	if cfg.Region == "" {
		return nil, errors.New("replicator: Lambda needs its Region")
	}
	if len(cfg.Targets) == 0 {
		return nil, errors.New("replicator: Lambda needs at least one target region")
	}
	seen := map[string]bool{cfg.Region: true}
	for _, t := range cfg.Targets {
		if t.Name == "" || t.Client == nil {
			return nil, errors.New("replicator: every target region needs a name and a DynamoDB client")
		}
		if seen[t.Name] {
			return nil, fmt.Errorf("replicator: duplicate region %q", t.Name)
		}
		seen[t.Name] = true
	}
	if cfg.Tables == nil {
		return nil, errors.New("replicator: Lambda needs a TableLookup")
	}
	if cfg.Parallelism <= 0 {
		cfg.Parallelism = 8
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Lambda{cfg: cfg, log: cfg.Logger}, nil
}

// Stats returns a snapshot of the counters (Pipes is 0).
func (l *Lambda) Stats() Stats { return l.stats() }

// Handle consumes one events.DynamoDBEvent batch from an event-source
// mapping configured with ReportBatchItemFailures. Records are applied to
// every target; records of the same item in stream order, distinct items
// concurrently. On a failure it reports the earliest record that was not
// applied everywhere as a batch item failure, so Lambda retries from there:
// every record before it is applied, and re-applying a later one is a no-op
// under the replication conditions.
func (l *Lambda) Handle(ctx context.Context, ev events.DynamoDBEvent) (events.DynamoDBEventResponse, error) {
	var resp events.DynamoDBEventResponse
	fail := func(i int, err error) (events.DynamoDBEventResponse, error) {
		l.log.Error("replication failed; reporting batch item failure", "seq", ev.Records[i].Change.SequenceNumber, "err", err)
		resp.BatchItemFailures = []events.DynamoDBBatchItemFailure{{ItemIdentifier: ev.Records[i].Change.SequenceNumber}}
		return resp, nil
	}
	// Consecutive records of one table are applied together.
	for start := 0; start < len(ev.Records); {
		table, ok := mvworker.TableFromStreamARN(ev.Records[start].EventSourceArn)
		if !ok {
			l.errs.Add(1)
			return fail(start, errors.New("cannot parse table from eventSourceARN "+ev.Records[start].EventSourceArn))
		}
		end := start + 1
		for ; end < len(ev.Records); end++ {
			if t, _ := mvworker.TableFromStreamARN(ev.Records[end].EventSourceArn); t != table {
				break
			}
		}
		spec, found, err := l.cfg.Tables(ctx, table)
		if err != nil {
			l.errs.Add(1)
			return fail(start, fmt.Errorf("resolve table %s: %w", table, err))
		}
		if !found {
			l.log.Warn("skipping records of a table the replicator does not copy", "table", table, "records", end-start)
			l.skipInvalid.Add(uint64(end - start))
		} else if i, err := l.applyGroup(ctx, spec, ev.Records[start:end]); err != nil {
			return fail(start+i, err)
		}
		start = end
	}
	return resp, nil
}

// applyGroup applies one table's records to every target. It returns the
// index of the earliest record that failed (every earlier record was
// applied to every target) and its error.
func (l *Lambda) applyGroup(ctx context.Context, spec TableSpec, recs []events.DynamoDBEventRecord) (int, error) {
	appliers := make([]*Applier, len(l.cfg.Targets))
	for i, t := range l.cfg.Targets {
		a, err := NewApplier(ApplierConfig{Source: l.cfg.Region, Target: t.Name, Client: t.Client, Table: spec,
			OnConflict: l.cfg.OnConflict, Logger: l.log.With("table", spec.Name, "to", t.Name)})
		if err != nil {
			return 0, err
		}
		appliers[i] = a
	}
	// Records are laned by item key, so one item's records stay ordered;
	// the key conversion is cheap and never fails for stream records.
	n := min(l.cfg.Parallelism, len(recs))
	lanes := make([][]int, n)
	for i, r := range recs {
		keys, _ := mvworker.FromLambdaItem(r.Change.Keys)
		lane := laneOf(keys, n)
		lanes[lane] = append(lanes[lane], i)
	}
	failed := make([]int, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for lane, idx := range lanes {
		failed[lane] = -1
		if len(idx) == 0 {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, i := range idx {
				if err := l.applyOne(ctx, appliers, recs[i]); err != nil {
					failed[lane], errs[lane] = i, err
					return
				}
			}
		}()
	}
	wg.Wait()
	first := -1
	for lane, i := range failed {
		if i >= 0 && (first < 0 || i < failed[first]) {
			first = lane
		}
	}
	if first < 0 {
		return -1, nil
	}
	return failed[first], errs[first]
}

// applyOne applies one record to every target.
func (l *Lambda) applyOne(ctx context.Context, appliers []*Applier, r events.DynamoDBEventRecord) error {
	rec, err := RecordFromLambda(r)
	if err != nil {
		l.errs.Add(1)
		return err
	}
	for _, a := range appliers {
		out, err := a.Apply(ctx, rec)
		l.record(out, err)
		if err != nil {
			return err
		}
	}
	return nil
}
