// Package streams is a reusable DynamoDB Streams consumer: it tails the
// stream of one table, processes parent shards before their children, keeps
// per-shard checkpoints and leases in a checkpoints table (so several
// readers can share a stream safely), and delivers records to a handler with
// at-least-once semantics: a batch's checkpoint advances only after the
// handler returned nil for it.
package streams

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	sttypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

// StreamsAPI is the subset of *dynamodbstreams.Client used by the reader.
type StreamsAPI interface {
	DescribeStream(ctx context.Context, in *dynamodbstreams.DescribeStreamInput, optFns ...func(*dynamodbstreams.Options)) (*dynamodbstreams.DescribeStreamOutput, error)
	GetShardIterator(ctx context.Context, in *dynamodbstreams.GetShardIteratorInput, optFns ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetShardIteratorOutput, error)
	GetRecords(ctx context.Context, in *dynamodbstreams.GetRecordsInput, optFns ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetRecordsOutput, error)
}

// DynamoDBAPI is the subset of *dynamodb.Client used by the reader
// (stream discovery and the checkpoints table).
type DynamoDBAPI interface {
	DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}

// TableAdmin is the subset of *dynamodb.Client used by EnsureCheckpointTable.
type TableAdmin interface {
	DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	CreateTable(ctx context.Context, in *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
}

// Batch is a group of consecutive records of one shard.
type Batch struct {
	Table     string
	StreamARN string
	ShardID   string
	Records   []sttypes.Record
	// Attempt is 1 on the first delivery of the batch and grows by one on
	// every retry.
	Attempt int
	// FailingSince is when the first failed delivery of this batch returned
	// (zero on the first attempt).
	FailingSince time.Time
	// Scratch is handler-owned state kept across the retries of this batch
	// (the reader never reads or resets it), e.g. which records a handler
	// has already applied or isolated as poison.
	Scratch any
}

// Handler processes a batch. Returning an error makes the reader retry the
// same batch (with backoff) without advancing the checkpoint; the reader
// itself never skips records. Handlers must therefore be idempotent, and a
// handler that can fail deterministically on some record (a "poison"
// record) must decide itself when to set it aside (e.g. dead-letter it) and
// return nil, or the shard stops advancing until the stream trims it.
type Handler func(ctx context.Context, b *Batch) error

// Config configures a Reader.
type Config struct {
	Streams  StreamsAPI
	DynamoDB DynamoDBAPI
	// Table is the physical DynamoDB table whose stream is read.
	Table string
	// CheckpointTable stores checkpoints and leases (_pk S).
	CheckpointTable string
	// Consumer names the logical consumer (e.g. "mvworker",
	// "replicator-eu-west-1"); each consumer has independent checkpoints.
	Consumer string
	// Owner identifies this reader instance for leases (e.g. host:pid).
	Owner   string
	Handler Handler

	// PollInterval is the first idle wait after an empty GetRecords
	// (default 250ms); consecutive empty polls back off up to MaxIdleBackoff
	// (default 2s).
	PollInterval   time.Duration
	MaxIdleBackoff time.Duration
	// LeaseDuration is the shard lease TTL (default 30s); leases are renewed
	// every LeaseDuration/3 while a shard is processed.
	LeaseDuration time.Duration
	// DiscoveryInterval is how often the stream's shards and the table's
	// latest stream ARN are re-read (default 5s).
	DiscoveryInterval time.Duration
	// BatchSize is the GetRecords limit (default and maximum 1000).
	BatchSize int32
	// MaxRetryBackoff caps the backoff of failed handler calls and API
	// errors (default 10s).
	MaxRetryBackoff time.Duration
	// MaxDiscoveryBackoff caps the DescribeTable/DescribeStream interval
	// after consecutive throttled or failed discoveries (default
	// max(DiscoveryInterval, 60s)).
	MaxDiscoveryBackoff time.Duration
	// StuckAlertAge: a batch whose oldest record is older than this while
	// its handler keeps failing is logged as a wedged shard that will lose
	// data when the stream trims it (default 1h; streams keep 24h).
	StuckAlertAge time.Duration
	Logger        *slog.Logger
}

// Stats are reader counters.
type Stats struct {
	Records       uint64 // records delivered successfully
	Batches       uint64
	HandlerErrors uint64
	ReadErrors    uint64
	ActiveShards  int64 // shards currently leased by this reader
	ShardsDone    uint64
	// LastRecordAt is the ApproximateCreationDateTime of the newest
	// delivered record, LastDeliveredAt when it was delivered.
	LastRecordAt    time.Time
	LastDeliveredAt time.Time
	// RetryingBatches is the number of batches whose handler failed and is
	// being retried; OldestPendingAt is the ApproximateCreationDateTime of
	// the oldest record among them (zero when none). Alert well before
	// now-OldestPendingAt reaches the stream retention (24h): records older
	// than that are trimmed and lost.
	RetryingBatches int
	OldestPendingAt time.Time
}

// Reader consumes one table's stream.
type Reader struct {
	cfg    Config
	leases leaseStore
	log    *slog.Logger
	now    func() time.Time

	records, batches, handlerErrs, readErrs, shardsDone atomic.Uint64
	active                                              atomic.Int64
	lastRecord, lastDelivered                           atomic.Int64 // unix nanos

	pendingMu sync.Mutex
	pending   map[string]time.Time // shard -> oldest record of its retried batch
}

// New validates cfg and creates a Reader.
func New(cfg Config) (*Reader, error) {
	return newReader(cfg, &dynamoLeases{ddb: cfg.DynamoDB, table: cfg.CheckpointTable, now: time.Now})
}

func newReader(cfg Config, leases leaseStore) (*Reader, error) {
	switch {
	case cfg.Streams == nil || cfg.DynamoDB == nil:
		return nil, errors.New("streams: Streams and DynamoDB clients are required")
	case cfg.Table == "":
		return nil, errors.New("streams: Table is required")
	case cfg.CheckpointTable == "":
		return nil, errors.New("streams: CheckpointTable is required")
	case cfg.Consumer == "":
		return nil, errors.New("streams: Consumer is required")
	case cfg.Owner == "":
		return nil, errors.New("streams: Owner is required")
	case cfg.Handler == nil:
		return nil, errors.New("streams: Handler is required")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 250 * time.Millisecond
	}
	if cfg.MaxIdleBackoff <= 0 {
		cfg.MaxIdleBackoff = 2 * time.Second
	}
	if cfg.MaxIdleBackoff < cfg.PollInterval {
		cfg.MaxIdleBackoff = cfg.PollInterval
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.DiscoveryInterval <= 0 {
		cfg.DiscoveryInterval = 5 * time.Second
	}
	if cfg.BatchSize <= 0 || cfg.BatchSize > 1000 {
		cfg.BatchSize = 1000
	}
	if cfg.MaxRetryBackoff <= 0 {
		cfg.MaxRetryBackoff = 10 * time.Second
	}
	if cfg.MaxDiscoveryBackoff < cfg.DiscoveryInterval {
		cfg.MaxDiscoveryBackoff = max(cfg.DiscoveryInterval, time.Minute)
	}
	if cfg.StuckAlertAge <= 0 {
		cfg.StuckAlertAge = time.Hour
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Reader{
		cfg:     cfg,
		leases:  leases,
		now:     time.Now,
		log:     cfg.Logger.With("table", cfg.Table, "consumer", cfg.Consumer),
		pending: map[string]time.Time{},
	}, nil
}

// Stats returns a snapshot of the counters.
func (r *Reader) Stats() Stats {
	s := Stats{
		Records:       r.records.Load(),
		Batches:       r.batches.Load(),
		HandlerErrors: r.handlerErrs.Load(),
		ReadErrors:    r.readErrs.Load(),
		ActiveShards:  r.active.Load(),
		ShardsDone:    r.shardsDone.Load(),
	}
	if n := r.lastRecord.Load(); n != 0 {
		s.LastRecordAt = time.Unix(0, n)
	}
	if n := r.lastDelivered.Load(); n != 0 {
		s.LastDeliveredAt = time.Unix(0, n)
	}
	r.pendingMu.Lock()
	for _, t := range r.pending {
		s.RetryingBatches++
		if s.OldestPendingAt.IsZero() || t.Before(s.OldestPendingAt) {
			s.OldestPendingAt = t
		}
	}
	r.pendingMu.Unlock()
	return s
}

// Run reads the table's stream until ctx is cancelled. It waits for the table
// and its stream to exist, and follows the table's latest stream when the
// table is recreated or its stream re-enabled. It returns ctx.Err() (or nil)
// on cancellation; transient AWS errors are retried internally.
func (r *Reader) Run(ctx context.Context) error {
	var waitingLogged bool
	for {
		arn, err := r.latestStreamARN(ctx)
		if err != nil || arn == "" {
			if ctx.Err() != nil {
				return nil
			}
			if !waitingLogged {
				r.log.Info("waiting for table stream", "reason", reasonOf(err, "stream disabled"))
				waitingLogged = true
			}
			if err != nil && !isTableNotFound(err) {
				r.readErrs.Add(1)
			}
			// The table is often created moments later (by a reconciler):
			// probe more often than shards are rediscovered.
			if err := sleep(ctx, min(r.cfg.DiscoveryInterval, time.Second)); err != nil {
				return nil
			}
			continue
		}
		waitingLogged = false
		r.log.Info("reading stream", "stream", arn)
		r.runStream(ctx, arn)
		if ctx.Err() != nil {
			return nil
		}
	}
}

func reasonOf(err error, def string) string {
	if err != nil {
		return err.Error()
	}
	return def
}

// latestStreamARN returns the table's LatestStreamArn if its stream is
// enabled, "" if streams are disabled.
func (r *Reader) latestStreamARN(ctx context.Context) (string, error) {
	cctx, cancel := callCtx(ctx)
	out, err := r.cfg.DynamoDB.DescribeTable(cctx, &dynamodb.DescribeTableInput{TableName: &r.cfg.Table})
	cancel()
	if err != nil {
		return "", err
	}
	t := out.Table
	if t == nil || t.LatestStreamArn == nil {
		return "", nil
	}
	if t.StreamSpecification != nil && t.StreamSpecification.StreamEnabled != nil && !*t.StreamSpecification.StreamEnabled {
		return "", nil
	}
	return *t.LatestStreamArn, nil
}

// shardResult is reported by a shard goroutine when it stops.
type shardResult struct {
	id   string
	done bool // shard closed and fully processed
	err  error
}

// runStream processes one stream until ctx is cancelled, the stream
// disappears or the table's latest stream changes.
func (r *Reader) runStream(ctx context.Context, arn string) {
	sctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()

	results := make(chan shardResult)
	active := map[string]bool{}
	done := map[string]bool{}
	timer := time.NewTimer(0)
	defer timer.Stop()
	// Consecutive discovery failures (typically DescribeStream throttling,
	// ~10 TPS per account) back off; a finished parent still rescans at once.
	discWait := backoff{min: r.cfg.DiscoveryInterval, max: r.cfg.MaxDiscoveryBackoff}

	for {
		select {
		case <-ctx.Done():
			return
		case res := <-results:
			delete(active, res.id)
			if res.done {
				done[res.id] = true
				r.shardsDone.Add(1)
				// A finished parent unblocks its children: rescan at once.
				resetTimer(timer, 0)
			}
			if errors.Is(res.err, errStreamGone) {
				return
			}
			continue
		case <-timer.C:
		}

		latest, err := r.latestStreamARN(sctx)
		switch {
		case err == nil && latest != arn:
			r.log.Warn("table stream changed; switching", "old", arn, "new", latest)
			return
		case isTableNotFound(err):
			r.log.Warn("table deleted; stopping stream", "stream", arn)
			return
		case err != nil:
			if sctx.Err() != nil {
				return
			}
			r.readErrs.Add(1)
			wait := discWait.next()
			r.log.Warn("describe table failed", "err", err, "retry_in", wait)
			resetTimer(timer, wait)
			continue
		}

		shards, err := r.describeShards(sctx, arn)
		switch {
		case isStreamNotFound(err):
			r.log.Warn("stream not found", "stream", arn)
			return
		case err != nil:
			if sctx.Err() != nil {
				return
			}
			r.readErrs.Add(1)
			wait := discWait.next()
			r.log.Warn("describe stream failed", "stream", arn, "err", err, "retry_in", wait)
			resetTimer(timer, wait)
			continue
		}
		discWait.reset()
		r.startEligible(sctx, arn, shards, active, done, results, &wg)
		resetTimer(timer, r.cfg.DiscoveryInterval)
	}
}

func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// describeShards lists every shard of the stream (paginated).
func (r *Reader) describeShards(ctx context.Context, arn string) ([]sttypes.Shard, error) {
	var shards []sttypes.Shard
	var start *string
	for {
		cctx, cancel := callCtx(ctx)
		out, err := r.cfg.Streams.DescribeStream(cctx, &dynamodbstreams.DescribeStreamInput{
			StreamArn:             &arn,
			ExclusiveStartShardId: start,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		if out.StreamDescription == nil {
			return shards, nil
		}
		shards = append(shards, out.StreamDescription.Shards...)
		start = out.StreamDescription.LastEvaluatedShardId
		if start == nil {
			return shards, nil
		}
	}
}

// startEligible leases and starts every shard that is not finished, not
// already running here, and whose parent (if still listed) is finished.
func (r *Reader) startEligible(ctx context.Context, arn string, shards []sttypes.Shard,
	active, done map[string]bool, results chan<- shardResult, wg *sync.WaitGroup) {
	listed := make(map[string]bool, len(shards))
	for _, s := range shards {
		listed[deref(s.ShardId)] = true
	}
	for _, s := range shards {
		id := deref(s.ShardId)
		if id == "" || done[id] || active[id] {
			continue
		}
		if parent := deref(s.ParentShardId); parent != "" && listed[parent] && !done[parent] {
			cp, found, err := r.leases.get(ctx, CheckpointKey(r.cfg.Consumer, arn, parent))
			if err != nil {
				r.readErrs.Add(1)
				r.log.Warn("read parent checkpoint failed", "shard", parent, "err", err)
				continue
			}
			if !found || !cp.Done {
				continue // parent not finished yet (here or by another reader)
			}
			done[parent] = true
		}
		key := CheckpointKey(r.cfg.Consumer, arn, id)
		cp, ok, err := r.leases.acquire(ctx, key, r.cfg.Owner, r.cfg.LeaseDuration)
		if err != nil {
			r.readErrs.Add(1)
			r.log.Warn("acquire lease failed", "shard", id, "err", err)
			continue
		}
		if cp.Done {
			done[id] = true
			continue
		}
		if !ok {
			continue // leased by another live reader
		}
		active[id] = true
		r.active.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer r.active.Add(-1)
			res := r.runShard(ctx, arn, id, key, cp.Seq)
			select {
			case results <- res:
			case <-ctx.Done():
			}
		}()
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

var errStreamGone = errors.New("streams: stream or shard no longer exists")

// runShard owns the lease of one shard: it renews the lease in the
// background and processes the shard until it is closed, the lease is lost
// or ctx ends.
func (r *Reader) runShard(ctx context.Context, arn, shardID, key, seq string) shardResult {
	log := r.log.With("shard", shardID)
	shCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var lost atomic.Bool
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		t := time.NewTicker(r.cfg.LeaseDuration / 3)
		defer t.Stop()
		for {
			select {
			case <-shCtx.Done():
				return
			case <-t.C:
			}
			err := r.leases.renew(shCtx, key, r.cfg.Owner, r.cfg.LeaseDuration)
			if errors.Is(err, ErrLeaseLost) {
				log.Warn("shard lease lost")
				lost.Store(true)
				cancel()
				return
			}
			if err != nil && shCtx.Err() == nil {
				r.readErrs.Add(1)
				log.Warn("renew lease failed", "err", err)
			}
		}
	}()

	done, err := r.processShard(shCtx, log, arn, shardID, key, seq)
	cancel()
	<-renewDone
	if !done && !lost.Load() && !errors.Is(err, ErrLeaseLost) {
		// Graceful stop: hand the shard over immediately.
		rctx, rcancel := detached(ctx)
		if rerr := r.leases.release(rctx, key, r.cfg.Owner); rerr != nil {
			log.Warn("release lease failed", "err", rerr)
		}
		rcancel()
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Warn("shard stopped", "err", err)
	}
	return shardResult{id: shardID, done: done, err: err}
}

// processShard reads one shard from just after seq ("" = TRIM_HORIZON).
// It returns done=true once the shard is closed and fully processed.
func (r *Reader) processShard(ctx context.Context, log *slog.Logger, arn, shardID, key, seq string) (bool, error) {
	iter, err := r.iterator(ctx, log, arn, shardID, &seq)
	if err != nil {
		return false, err
	}
	idle := r.cfg.PollInterval
	errWait := backoff{min: 100 * time.Millisecond, max: r.cfg.MaxRetryBackoff}
	for {
		if iter == "" {
			// Closed shard: everything up to seq has been processed.
			fctx, fcancel := detached(ctx)
			err := r.finish(fctx, key, seq)
			fcancel()
			if err != nil {
				return false, err
			}
			log.Info("shard finished", "seq", seq)
			return true, nil
		}
		cctx, cancel := callCtx(ctx)
		out, err := r.cfg.Streams.GetRecords(cctx, &dynamodbstreams.GetRecordsInput{
			ShardIterator: &iter,
			Limit:         &r.cfg.BatchSize,
		})
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			var expired *sttypes.ExpiredIteratorException
			var trimmed *sttypes.TrimmedDataAccessException
			switch {
			case errors.As(err, &expired):
				log.Debug("shard iterator expired; re-acquiring from checkpoint", "seq", seq)
				iter, err = r.iterator(ctx, log, arn, shardID, &seq)
				if err != nil {
					return false, err
				}
				continue
			case errors.As(err, &trimmed):
				// The iterator's position is gone. Re-position from the
				// checkpoint; iterator() detects (and reports) a checkpoint
				// that is itself beyond the retention window.
				// Nothing read yet (seq ""): a fresh TRIM_HORIZON iterator
				// cannot skip anything (a residual DynamoDB Local quirk, see
				// the idle-poll comment below), so it is not worth an INFO.
				level := slog.LevelInfo
				if seq == "" {
					level = slog.LevelDebug
				}
				log.Log(ctx, level, "GetRecords hit trimmed data; re-acquiring iterator from checkpoint", "seq", seq, "err", err)
				iter, err = r.iterator(ctx, log, arn, shardID, &seq)
				if err != nil {
					return false, err
				}
				continue
			case isStreamNotFound(err):
				return false, errStreamGone
			}
			r.readErrs.Add(1)
			log.Warn("GetRecords failed", "err", err)
			if err := sleep(ctx, errWait.next()); err != nil {
				return false, err
			}
			continue
		}
		errWait.reset()
		if len(out.Records) > 0 {
			b := &Batch{Table: r.cfg.Table, StreamARN: arn, ShardID: shardID, Records: out.Records}
			if err := r.deliver(ctx, log, b); err != nil {
				return false, err
			}
			seq = deref(out.Records[len(out.Records)-1].Dynamodb.SequenceNumber)
			// The batch is applied: persist its checkpoint even if a shutdown
			// started meanwhile, so a graceful stop does not redeliver it.
			cctx, ccancel := detached(ctx)
			err := r.saveCheckpoint(cctx, log, key, seq)
			ccancel()
			if err != nil {
				return false, err
			}
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			idle = r.cfg.PollInterval
		}
		iter = deref(out.NextShardIterator)
		if iter != "" && len(out.Records) == 0 {
			if err := sleep(ctx, idle); err != nil {
				return false, err
			}
			idle = min(idle*2, r.cfg.MaxIdleBackoff)
			if seq == "" {
				// Nothing read from this shard yet: poll with a fresh
				// TRIM_HORIZON iterator instead of the one an empty
				// GetRecords returned. DynamoDB Local pins such iterators to
				// the empty shard's start, and once other tables' writes
				// advance its global sequence numbers the shard's first
				// record makes them fail with TrimmedDataAccessException.
				// TRIM_HORIZON cannot skip anything; on AWS it is merely one
				// extra call per idle poll of a never-written shard.
				if iter, err = r.iterator(ctx, log, arn, shardID, &seq); err != nil {
					return false, err
				}
			}
		}
	}
}

// iterator returns a shard iterator positioned after *seq (TRIM_HORIZON when
// empty). If the checkpoint is older than the stream's retention it logs the
// data loss, resets *seq and starts at TRIM_HORIZON. "" means the shard has
// no iterator (closed and exhausted).
func (r *Reader) iterator(ctx context.Context, log *slog.Logger, arn, shardID string, seq *string) (string, error) {
	wait := backoff{min: 100 * time.Millisecond, max: r.cfg.MaxRetryBackoff}
	for {
		in := &dynamodbstreams.GetShardIteratorInput{StreamArn: &arn, ShardId: &shardID}
		if *seq == "" {
			in.ShardIteratorType = sttypes.ShardIteratorTypeTrimHorizon
		} else {
			in.ShardIteratorType = sttypes.ShardIteratorTypeAfterSequenceNumber
			in.SequenceNumber = seq
		}
		cctx, cancel := callCtx(ctx)
		out, err := r.cfg.Streams.GetShardIterator(cctx, in)
		cancel()
		if err == nil {
			return deref(out.ShardIterator), nil
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		var trimmed *sttypes.TrimmedDataAccessException
		switch {
		case errors.As(err, &trimmed) && *seq != "":
			log.Error("DATA LOSS: checkpoint is beyond the stream's retention; restarting from TRIM_HORIZON", "seq", *seq)
			*seq = ""
			continue
		case isStreamNotFound(err):
			return "", errStreamGone
		}
		r.readErrs.Add(1)
		log.Warn("GetShardIterator failed", "err", err)
		if err := sleep(ctx, wait.next()); err != nil {
			return "", err
		}
	}
}

// deliver calls the handler until it succeeds (at-least-once; the reader
// never skips records: setting a poison record aside is the handler's call).
func (r *Reader) deliver(ctx context.Context, log *slog.Logger, b *Batch) error {
	wait := backoff{min: 100 * time.Millisecond, max: r.cfg.MaxRetryBackoff}
	oldest := r.now()
	if t := b.Records[0].Dynamodb.ApproximateCreationDateTime; t != nil {
		oldest = *t
	}
	defer r.clearPending(b.ShardID)
	for b.Attempt = 1; ; b.Attempt++ {
		err := r.cfg.Handler(ctx, b)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if b.FailingSince.IsZero() {
			b.FailingSince = r.now()
			r.setPending(b.ShardID, oldest)
		}
		r.handlerErrs.Add(1)
		age := r.now().Sub(oldest)
		attrs := []any{"attempt", b.Attempt, "records", len(b.Records),
			"first_seq", deref(b.Records[0].Dynamodb.SequenceNumber), "failing_for", r.now().Sub(b.FailingSince).Round(time.Millisecond),
			"record_age", age.Round(time.Second), "err", err}
		if age >= r.cfg.StuckAlertAge {
			log.Error("SHARD WEDGED: handler keeps failing on a batch; its records are lost once the stream trims them (24h retention)", attrs...)
		} else {
			log.Error("handler failed; retrying batch", attrs...)
		}
		if err := sleep(ctx, wait.next()); err != nil {
			return err
		}
	}
	r.records.Add(uint64(len(b.Records)))
	r.batches.Add(1)
	if t := b.Records[len(b.Records)-1].Dynamodb.ApproximateCreationDateTime; t != nil {
		r.lastRecord.Store(t.UnixNano())
	}
	r.lastDelivered.Store(r.now().UnixNano())
	return nil
}

func (r *Reader) setPending(shard string, oldest time.Time) {
	r.pendingMu.Lock()
	r.pending[shard] = oldest
	r.pendingMu.Unlock()
}

func (r *Reader) clearPending(shard string) {
	r.pendingMu.Lock()
	delete(r.pending, shard)
	r.pendingMu.Unlock()
}

// saveCheckpoint persists seq, retrying transient errors.
func (r *Reader) saveCheckpoint(ctx context.Context, log *slog.Logger, key, seq string) error {
	wait := backoff{min: 100 * time.Millisecond, max: r.cfg.MaxRetryBackoff}
	for {
		err := r.leases.checkpoint(ctx, key, r.cfg.Owner, seq, r.cfg.LeaseDuration)
		if err == nil || errors.Is(err, ErrLeaseLost) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.readErrs.Add(1)
		log.Warn("checkpoint failed", "seq", seq, "err", err)
		if err := sleep(ctx, wait.next()); err != nil {
			return err
		}
	}
}

func (r *Reader) finish(ctx context.Context, key, seq string) error {
	wait := backoff{min: 100 * time.Millisecond, max: r.cfg.MaxRetryBackoff}
	for {
		err := r.leases.markDone(ctx, key, r.cfg.Owner, seq)
		if err == nil || errors.Is(err, ErrLeaseLost) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.readErrs.Add(1)
		r.log.Warn("mark shard done failed", "key", key, "err", err)
		if err := sleep(ctx, wait.next()); err != nil {
			return err
		}
	}
}

// detached returns a context that survives ctx's cancellation for a short
// time, for bookkeeping writes that must complete during shutdown.
func detached(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
}

// backoff is a capped exponential backoff.
type backoff struct {
	min, max, cur time.Duration
}

func (b *backoff) next() time.Duration {
	if b.cur == 0 {
		b.cur = b.min
	} else {
		b.cur = min(b.cur*2, b.max)
	}
	return b.cur
}

func (b *backoff) reset() { b.cur = 0 }

// sleep waits d or until ctx is done (returning ctx.Err()).
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
