package mvworker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/exec"
	"github.com/gfronza/twinhouse/internal/storage/dynamo"
)

// LambdaConfig configures the Lambda entry point.
type LambdaConfig struct {
	Store   *dynamo.Store
	Catalog catalog.Catalog
	Engine  *exec.Engine
	Logger  *slog.Logger
	// CatalogRefresh is the maximum catalog snapshot age before an
	// invocation reloads it (default 5s), so ALTERs are picked up.
	CatalogRefresh time.Duration
	// DeadLetters, when set, stores poison records in the regional
	// dead-letter table (DeadLetterTableName); otherwise they are only
	// logged. PoisonPolicy, PoisonAfter and CatalogMinReload are as in
	// Config.
	DeadLetters      DeadLetterAPI
	PoisonPolicy     string
	PoisonAfter      time.Duration
	CatalogMinReload time.Duration
}

// LambdaHandler consumes events.DynamoDBEvent batches from a DynamoDB
// stream event-source mapping configured with ReportBatchItemFailures.
type LambdaHandler func(ctx context.Context, ev events.DynamoDBEvent) (events.DynamoDBEventResponse, error)

// NewLambdaHandler returns the Lambda handler. On failure it reports the
// first unprocessed record as a batch item failure, so Lambda retries from
// there (records before it are applied; re-application is a no-op thanks to
// exactly-once markers). A deterministic failure is isolated like in the
// stream worker: the other records of the group are applied, and the
// poison record is dead-lettered once it has kept failing for PoisonAfter
// (tracked per Lambda execution environment), after which the batch
// succeeds.
func NewLambdaHandler(cfg LambdaConfig) (LambdaHandler, error) {
	if cfg.Store == nil || cfg.Catalog == nil || cfg.Engine == nil {
		return nil, errors.New("mvworker: Store, Catalog and Engine are required")
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	if cfg.CatalogRefresh <= 0 {
		cfg.CatalogRefresh = 5 * time.Second
	}
	o := processorOptions{policy: cfg.PoisonPolicy, poisonAfter: cfg.PoisonAfter, minReload: cfg.CatalogMinReload, origin: "lambda"}
	if cfg.DeadLetters != nil {
		o.deadLetters, o.dlqTable = cfg.DeadLetters, DeadLetterTableName(cfg.Store.CheckpointsTableName())
	}
	p, err := newProcessor(cfg.Catalog, cfg.Store, cfg.Engine, log, o)
	if err != nil {
		return nil, err
	}
	h := newLambdaHandler(p)
	var mu sync.Mutex
	var refreshed time.Time
	return func(ctx context.Context, ev events.DynamoDBEvent) (events.DynamoDBEventResponse, error) {
		mu.Lock()
		if time.Since(refreshed) > cfg.CatalogRefresh {
			if err := p.refresh(ctx); err != nil {
				log.Warn("catalog refresh failed; using last snapshot", "err", err)
			} else {
				refreshed = time.Now()
			}
		}
		mu.Unlock()
		return h(ctx, ev)
	}, nil
}

func newLambdaHandler(p *processor) LambdaHandler {
	var scratch scratchTracker
	return func(ctx context.Context, ev events.DynamoDBEvent) (events.DynamoDBEventResponse, error) {
		var resp events.DynamoDBEventResponse
		fail := func(i int, err error) (events.DynamoDBEventResponse, error) {
			p.log.Error("materialized view application failed; reporting batch item failure",
				"seq", ev.Records[i].Change.SequenceNumber, "err", err)
			resp.BatchItemFailures = []events.DynamoDBBatchItemFailure{{ItemIdentifier: ev.Records[i].Change.SequenceNumber}}
			return resp, nil
		}
		// Consecutive records of one table are applied together, in order.
		for start := 0; start < len(ev.Records); {
			table, ok := TableFromStreamARN(ev.Records[start].EventSourceArn)
			if !ok {
				p.errs.Add(1)
				return fail(start, errors.New("cannot parse table from eventSourceARN "+ev.Records[start].EventSourceArn))
			}
			end := start
			var changes []change
			for ; end < len(ev.Records); end++ {
				t, _ := TableFromStreamARN(ev.Records[end].EventSourceArn)
				if t != table {
					break
				}
				c, err := changeFromLambda(ev.Records[end])
				if err != nil {
					p.errs.Add(1)
					return fail(start, err)
				}
				changes = append(changes, c)
			}
			key := table + "|" + changes[0].Seq
			st := scratch.get(key)
			err := p.handle(ctx, table, changes, &st)
			if err != nil {
				scratch.set(key, st)
				return fail(start, err)
			}
			scratch.set(key, nil)
			start = end
		}
		return resp, nil
	}
}
