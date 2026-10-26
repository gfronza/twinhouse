// Package replicator emulates DynamoDB global tables between independent
// DynamoDB endpoints (e.g. one DynamoDB Local per "region"), as described in
// DESIGN.md §6.
//
// For every ordered region pair A→B and every replicated table it tails A's
// stream (consumer "replicator-<B>", checkpoints in A's checkpoints table)
// and applies to B:
//
//   - INSERT/MODIFY whose new image has _origin == A: PutItem of the full
//     image under a last-writer-wins condition on (_ts, _origin). For
//     ReplacingMergeTree(ver) tables (TableSpec.VersionColumn) the version
//     is compared first, so the highest version wins in every region
//     regardless of write order, like ClickHouse; (_ts, _origin) only breaks
//     version ties. Records whose origin is another region are replicas
//     written by this replicator and are skipped, which is what prevents
//     ping-pong loops.
//   - REMOVE (any origin): DeleteItem conditioned on the target's _ts being
//     <= the deleted image's _ts. A delete applied by the replicator produces
//     a REMOVE in the target region's stream too; replaying it back finds the
//     item already gone (or newer) and fails its condition, so deletes
//     terminate after one bounce. Filtering deletes by _origin instead would
//     never replicate the deletion of a row written in another region.
//
// A failed condition means the target already has the same or newer data and
// the record is not written. For an INSERT/MODIFY the losing row version is
// then handed to Config.OnConflict, which applies the table's materialized
// views to it in the target region: the version never enters that region's
// stream, but the origin region applied its views to it, and rollups only
// converge if every region's views see the same row versions (DESIGN.md §1).
//
// The per-record logic lives in Applier, shared by the stream poller
// (Replicator, one pipe per table and ordered region pair) and the Lambda
// entry point (Lambda, GLOBAL_MODE=aws: one function per region fed by the
// event-source mappings of that region's ReplacingMergeTree tables, copying
// its rows to every other region).
package replicator

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/gfronza/twinhouse/internal/awsx"
	"github.com/gfronza/twinhouse/internal/streams"
)

// System attributes used for conflict resolution (DESIGN.md §4).
const (
	attrPK     = "_pk"
	attrTS     = "_ts"
	attrOrigin = "_origin"
	attrRID    = "_rid"
)

// Region is one emulated region.
type Region struct {
	Name    string
	Clients *awsx.Clients
}

// TableSpec is one replicated table.
type TableSpec struct {
	// Name is the physical DynamoDB table name.
	Name string
	// VersionColumn is the ReplacingMergeTree version column (the item
	// attribute of the same name), "" for plain last-writer-wins.
	VersionColumn string
}

// TableSource returns the row tables to replicate (tables whose catalog
// Replication is global). The catalog table itself is replicated unless
// Config.SkipCatalog is set and need not be returned.
type TableSource func(ctx context.Context) ([]TableSpec, error)

// Config configures a Replicator.
type Config struct {
	Regions []Region
	// Prefix is the physical table prefix (config.TablePrefix); it names the
	// catalog ({prefix}__catalog) and checkpoints ({prefix}__checkpoints) tables.
	Prefix string
	Tables TableSource
	// SkipCatalog leaves the catalog table out (GLOBAL_MODE=aws, where it is
	// a real global table and only ReplacingMergeTree(ver) tables are copied
	// by the replicator).
	SkipCatalog bool
	// Delay is an artificial replication lag: a record is applied no sooner
	// than Delay after it was written (its _ts) or, for deletes, received.
	Delay time.Duration
	// Owner identifies this process for shard leases.
	Owner string
	// PollInterval is the stream idle poll interval (default 250ms).
	PollInterval time.Duration
	// LeaseDuration is the shard lease TTL (default 30s).
	LeaseDuration time.Duration
	// DiscoveryInterval is how often Tables is consulted and target tables
	// are probed (default 2s).
	DiscoveryInterval time.Duration
	// StreamDiscoveryInterval is how often each pipe's stream reader
	// re-describes its stream (DescribeTable + DescribeStream). DescribeStream
	// is limited to about 10 TPS per account and every (table, from, to) pipe
	// polls it, so it is at least MinStreamDiscoveryInterval (default 5s).
	StreamDiscoveryInterval time.Duration
	// OnConflict, when set, is called for every replicated INSERT/MODIFY
	// image that the target region rejected because it already holds the
	// same or a newer version of the item (region is the target region,
	// table the physical table, seq identifies the record). It applies
	// materialized views to the losing version (mvworker.ImageApplier); an
	// error fails the batch, which is retried, so it must be idempotent.
	OnConflict ConflictHook
	// Parallelism bounds concurrent writes per batch (default 8); records of
	// the same item are always applied in stream order.
	Parallelism int
	Logger      *slog.Logger
}

// Stats are replication counters (all pipes).
type Stats struct {
	Put             uint64 // items written to a target
	Deleted         uint64 // items deleted in a target
	SkippedOrigin   uint64 // records originating elsewhere (replicas)
	SkippedConflict uint64 // records superseded by newer target data
	SkippedInvalid  uint64 // records without the system attributes
	ConflictViews   uint64 // superseded versions handed to OnConflict
	Errors          uint64 // failed writes (retried)
	Pipes           int    // running (table, from, to) pipes
}

// Replicator runs every replication pipe.
type Replicator struct {
	cfg Config
	log *slog.Logger
	counters

	mu    sync.Mutex
	pipes map[pipeKey]context.CancelFunc
}

// pipeKey identifies a pipe; a table recreated with another version column
// gets a new pipe.
type pipeKey struct{ table, ver, from, to string }

// New validates cfg and creates a Replicator.
func New(cfg Config) (*Replicator, error) {
	if len(cfg.Regions) < 2 {
		return nil, errors.New("replicator: at least two regions are required")
	}
	seen := map[string]bool{}
	for _, r := range cfg.Regions {
		if r.Name == "" || r.Clients == nil {
			return nil, errors.New("replicator: every region needs a name and clients")
		}
		if seen[r.Name] {
			return nil, fmt.Errorf("replicator: duplicate region %q", r.Name)
		}
		seen[r.Name] = true
	}
	if cfg.Owner == "" {
		return nil, errors.New("replicator: Owner is required")
	}
	if cfg.Tables == nil {
		cfg.Tables = func(context.Context) ([]TableSpec, error) { return nil, nil }
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 250 * time.Millisecond
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.DiscoveryInterval <= 0 {
		cfg.DiscoveryInterval = 2 * time.Second
	}
	cfg.StreamDiscoveryInterval = max(cfg.StreamDiscoveryInterval, MinStreamDiscoveryInterval)
	if cfg.Parallelism <= 0 {
		cfg.Parallelism = 8
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Replicator{cfg: cfg, log: cfg.Logger, pipes: map[pipeKey]context.CancelFunc{}}, nil
}

// MinStreamDiscoveryInterval is the lowest StreamDiscoveryInterval (the
// stream worker uses the same floor).
const MinStreamDiscoveryInterval = 5 * time.Second

// CatalogTable is the catalog table name (replicated unless SkipCatalog).
func (r *Replicator) CatalogTable() string { return r.cfg.Prefix + "__catalog" }

// CheckpointTable is the per-region checkpoints table name.
func (r *Replicator) CheckpointTable() string { return r.cfg.Prefix + "__checkpoints" }

// Stats returns a snapshot of the counters.
func (r *Replicator) Stats() Stats {
	s := r.stats()
	r.mu.Lock()
	s.Pipes = len(r.pipes)
	r.mu.Unlock()
	return s
}

// Run replicates until ctx is cancelled.
func (r *Replicator) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	listFailing := false
	defer func() {
		r.mu.Lock()
		for k, cancel := range r.pipes {
			cancel()
			delete(r.pipes, k)
		}
		r.mu.Unlock()
		wg.Wait()
	}()
	for {
		var tables []TableSpec
		if !r.cfg.SkipCatalog {
			tables = append(tables, TableSpec{Name: r.CatalogTable()})
		}
		extra, err := r.cfg.Tables(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Expected until a server has created the catalog: log changes only.
			if !listFailing {
				r.log.Warn("listing replicated tables failed; keeping current set", "err", err)
				listFailing = true
			}
			r.mu.Lock()
			for k := range r.pipes {
				tables = append(tables, TableSpec{Name: k.table, VersionColumn: k.ver})
			}
			r.mu.Unlock()
		}
		if err == nil && listFailing {
			r.log.Info("listing replicated tables recovered")
			listFailing = false
		}
		tables = append(tables, extra...)
		r.reconcile(ctx, tables, &wg)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(r.cfg.DiscoveryInterval):
		}
	}
}

// reconcile starts missing pipes and stops pipes of tables no longer listed.
func (r *Replicator) reconcile(ctx context.Context, tables []TableSpec, wg *sync.WaitGroup) {
	want := map[pipeKey]bool{}
	for _, t := range tables {
		for _, from := range r.cfg.Regions {
			for _, to := range r.cfg.Regions {
				if from.Name != to.Name {
					want[pipeKey{t.Name, t.VersionColumn, from.Name, to.Name}] = true
				}
			}
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, cancel := range r.pipes {
		if !want[k] {
			r.log.Info("stopping replication", "table", k.table, "from", k.from, "to", k.to)
			cancel()
			delete(r.pipes, k)
		}
	}
	for k := range want {
		if _, ok := r.pipes[k]; ok {
			continue
		}
		pctx, cancel := context.WithCancel(ctx)
		r.pipes[k] = cancel
		// The stream reader adds "table" (and "consumer") itself; giving it
		// the pipe's logger would repeat the key in every reader line.
		readerLog := r.log.With("from", k.from, "to", k.to)
		p := &pipe{r: r, key: k, from: r.region(k.from), to: r.region(k.to),
			log: readerLog.With("table", k.table), readerLog: readerLog}
		hook := r.cfg.OnConflict
		if k.table == r.CatalogTable() {
			hook = nil // catalog entries feed no views
		}
		var err error
		p.applier, err = NewApplier(ApplierConfig{Source: k.from, Target: k.to, Client: p.to.Clients.DynamoDB,
			Table: TableSpec{Name: k.table, VersionColumn: k.ver}, OnConflict: hook, Logger: p.log})
		if err != nil {
			// Unreachable: New validated the regions and the table has a name.
			r.log.Error("cannot build applier", "table", k.table, "from", k.from, "to", k.to, "err", err)
			cancel()
			delete(r.pipes, k)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.run(pctx)
		}()
	}
}

func (r *Replicator) region(name string) Region {
	for _, reg := range r.cfg.Regions {
		if reg.Name == name {
			return reg
		}
	}
	panic("replicator: unknown region " + name)
}

// pipe replicates one table from one region to another.
type pipe struct {
	r         *Replicator
	key       pipeKey
	from, to  Region
	applier   *Applier
	log       *slog.Logger
	readerLog *slog.Logger // log without the table attribute (streams adds it)
}

func (p *pipe) run(ctx context.Context) {
	if err := p.waitReady(ctx); err != nil {
		return
	}
	reader, err := streams.New(streams.Config{
		Streams:           p.from.Clients.Streams,
		DynamoDB:          p.from.Clients.DynamoDB,
		Table:             p.key.table,
		CheckpointTable:   p.r.CheckpointTable(),
		Consumer:          "replicator-" + p.to.Name,
		Owner:             p.r.cfg.Owner,
		Handler:           p.handle,
		PollInterval:      p.r.cfg.PollInterval,
		MaxIdleBackoff:    max(p.r.cfg.PollInterval*4, time.Second),
		LeaseDuration:     p.r.cfg.LeaseDuration,
		DiscoveryInterval: p.r.cfg.StreamDiscoveryInterval,
		Logger:            p.readerLog,
	})
	if err != nil {
		p.log.Error("cannot start stream reader", "err", err)
		return
	}
	p.log.Info("replicating")
	_ = reader.Run(ctx)
}

// waitReady creates the source checkpoints table if needed and waits until
// the target table exists (each region's server reconciler creates it).
func (p *pipe) waitReady(ctx context.Context) error {
	logged := false
	for {
		err := streams.EnsureCheckpointTable(ctx, p.from.Clients.DynamoDB, p.r.CheckpointTable())
		if err == nil {
			_, err = p.to.Clients.DynamoDB.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(p.key.table)})
		}
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !logged {
			p.log.Info("waiting for tables", "reason", err)
			logged = true
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(p.r.cfg.DiscoveryInterval):
		}
	}
}

// handle applies a batch. Records of the same item keep their stream order;
// distinct items are written concurrently. Any write error fails the whole
// batch, which the reader retries: every write is idempotent under its
// condition.
func (p *pipe) handle(ctx context.Context, b *streams.Batch) error {
	received := time.Now()
	recs := make([]Record, len(b.Records))
	for i, sr := range b.Records {
		rec, err := RecordFromStream(sr)
		if err != nil {
			p.r.errs.Add(1)
			return err
		}
		recs[i] = rec
	}
	n := min(p.r.cfg.Parallelism, len(recs))
	lanes := make([][]Record, n)
	for _, rec := range recs {
		i := laneOf(rec.Keys, n)
		lanes[i] = append(lanes[i], rec)
	}
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i, lane := range lanes {
		if len(lane) == 0 {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, rec := range lane {
				if err := p.apply(ctx, rec, received); err != nil {
					errs[i] = err
					return
				}
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// laneOf hashes the record's key attributes into one of n lanes.
func laneOf(keys map[string]ddbtypes.AttributeValue, n int) int {
	if n <= 1 || len(keys) == 0 {
		return 0
	}
	h := fnv.New32a()
	names := make([]string, 0, len(keys))
	for k := range keys {
		names = append(names, k)
	}
	slices.Sort(names)
	for _, k := range names {
		h.Write([]byte(k))
		switch v := keys[k].(type) {
		case *ddbtypes.AttributeValueMemberS:
			h.Write([]byte(v.Value))
		case *ddbtypes.AttributeValueMemberB:
			h.Write(v.Value)
		case *ddbtypes.AttributeValueMemberN:
			h.Write([]byte(v.Value))
		}
		h.Write([]byte{0})
	}
	return int(h.Sum32() % uint32(n))
}

// apply holds the record back for the artificial lag, then applies it.
func (p *pipe) apply(ctx context.Context, rec Record, received time.Time) error {
	if p.r.cfg.Delay > 0 {
		switch rec.EventName {
		case EventInsert, EventModify:
			// Only images this pipe will write are delayed (by their write
			// time); replicas from elsewhere are skipped right away.
			if origin, ok := stringAttr(rec.NewImage, attrOrigin); ok && origin == p.from.Name {
				if ts, ok := numberAttr(rec.NewImage, attrTS); ok {
					if nanos, err := strconv.ParseInt(ts, 10, 64); err == nil {
						if err := p.wait(ctx, time.Unix(0, nanos).Add(p.r.cfg.Delay)); err != nil {
							return err
						}
					}
				}
			}
		case EventRemove:
			if err := p.wait(ctx, received.Add(p.r.cfg.Delay)); err != nil {
				return err
			}
		}
	}
	out, err := p.applier.Apply(ctx, rec)
	p.r.record(out, err)
	return err
}

// putCondition builds the replication write condition for an image with
// write time ts and origin. Without a version column it is last-writer-wins
// on (_ts, _origin). With one (ReplacingMergeTree(ver)) the higher version
// wins and (_ts, _origin) only breaks ties, mirroring the local write path
// (storage/dynamo versionCondition): versions are N (integers; unix nanos for
// dates) and NULL or a missing attribute is the smallest version.
func putCondition(item map[string]ddbtypes.AttributeValue, ts, origin, verCol string) (string, map[string]string, map[string]ddbtypes.AttributeValue) {
	names := map[string]string{"#pk": attrPK, "#ts": attrTS, "#o": attrOrigin}
	values := map[string]ddbtypes.AttributeValue{
		":ts": &ddbtypes.AttributeValueMemberN{Value: ts},
		":o":  &ddbtypes.AttributeValueMemberS{Value: origin},
	}
	const lww = "(#ts < :ts OR (#ts = :ts AND #o < :o))"
	if verCol == "" {
		return "attribute_not_exists(#pk) OR " + lww, names, values
	}
	names["#ver"] = verCol
	values[":null"] = &ddbtypes.AttributeValueMemberS{Value: "NULL"}
	const targetNull = "(attribute_not_exists(#ver) OR attribute_type(#ver, :null))"
	ver, ok := item[verCol].(*ddbtypes.AttributeValueMemberN)
	if !ok {
		// The new version is NULL/absent (the smallest): it only replaces a
		// target whose version is NULL/absent too, by write time.
		return "attribute_not_exists(#pk) OR (" + targetNull + " AND " + lww + ")", names, values
	}
	values[":ver"] = ver
	return "attribute_not_exists(#pk) OR " + targetNull + " OR #ver < :ver OR (#ver = :ver AND " + lww + ")", names, values
}

// sameRow reports whether a failed conditional put found the very row
// version it tried to write (same _rid): a redelivery whose first put
// succeeded, so the version is already in the target region's stream. When
// the existing item is unknown it reports false (the caller's hook is
// idempotent).
func sameRow(err error, item map[string]ddbtypes.AttributeValue) bool {
	var ccf *ddbtypes.ConditionalCheckFailedException
	if !errors.As(err, &ccf) || ccf.Item == nil {
		return false
	}
	have, ok1 := ccf.Item[attrRID].(*ddbtypes.AttributeValueMemberS)
	want, ok2 := item[attrRID].(*ddbtypes.AttributeValueMemberS)
	return ok1 && ok2 && have.Value == want.Value
}

// wait sleeps until t (artificial lag).
func (p *pipe) wait(ctx context.Context, t time.Time) error {
	if p.r.cfg.Delay <= 0 {
		return nil
	}
	d := time.Until(t)
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func stringAttr(img map[string]ddbtypes.AttributeValue, name string) (string, bool) {
	v, ok := img[name].(*ddbtypes.AttributeValueMemberS)
	if !ok {
		return "", false
	}
	return v.Value, true
}

func numberAttr(img map[string]ddbtypes.AttributeValue, name string) (string, bool) {
	v, ok := img[name].(*ddbtypes.AttributeValueMemberN)
	if !ok {
		return "", false
	}
	return v.Value, true
}
