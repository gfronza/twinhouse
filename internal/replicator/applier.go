package replicator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sttypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/gfronza/twinhouse/internal/mvworker"
	"github.com/gfronza/twinhouse/internal/streams"
)

// Stream record event names.
const (
	EventInsert = "INSERT"
	EventModify = "MODIFY"
	EventRemove = "REMOVE"
)

// Record is one source-stream record, normalised from either the DynamoDB
// Streams API (RecordFromStream) or a Lambda event (RecordFromLambda).
type Record struct {
	// EventName is INSERT, MODIFY or REMOVE.
	EventName string
	// Seq is the record's sequence number (logs, conflict hook).
	Seq string
	// Keys is the item key; NewImage the full item after an INSERT/MODIFY;
	// OldImage the item before a REMOVE.
	Keys, NewImage, OldImage map[string]ddbtypes.AttributeValue
}

// RecordFromStream converts a DynamoDB Streams record.
func RecordFromStream(rec sttypes.Record) (Record, error) {
	r := Record{EventName: string(rec.EventName)}
	sr := rec.Dynamodb
	if sr == nil {
		return r, nil
	}
	r.Seq = aws.ToString(sr.SequenceNumber)
	var err error
	if r.Keys, err = streams.ToDynamoDBItem(sr.Keys); err != nil {
		return r, fmt.Errorf("record %s: convert keys: %w", r.Seq, err)
	}
	if r.NewImage, err = streams.ToDynamoDBItem(sr.NewImage); err != nil {
		return r, fmt.Errorf("record %s: convert new image: %w", r.Seq, err)
	}
	if r.OldImage, err = streams.ToDynamoDBItem(sr.OldImage); err != nil {
		return r, fmt.Errorf("record %s: convert old image: %w", r.Seq, err)
	}
	return r, nil
}

// RecordFromLambda converts a record of a Lambda events.DynamoDBEvent.
func RecordFromLambda(rec events.DynamoDBEventRecord) (Record, error) {
	r := Record{EventName: rec.EventName, Seq: rec.Change.SequenceNumber}
	var err error
	if r.Keys, err = mvworker.FromLambdaItem(rec.Change.Keys); err != nil {
		return r, fmt.Errorf("record %s: convert keys: %w", r.Seq, err)
	}
	if r.NewImage, err = mvworker.FromLambdaItem(rec.Change.NewImage); err != nil {
		return r, fmt.Errorf("record %s: convert new image: %w", r.Seq, err)
	}
	if r.OldImage, err = mvworker.FromLambdaItem(rec.Change.OldImage); err != nil {
		return r, fmt.Errorf("record %s: convert old image: %w", r.Seq, err)
	}
	return r, nil
}

// Outcome is what Applier.Apply did with a record.
type Outcome int

const (
	// Invalid: the record lacks the system attributes (or has an unknown
	// event name) and was skipped.
	Invalid Outcome = iota
	// SkippedOrigin: the image originates in another region (a replica
	// written by a replicator) and was skipped.
	SkippedOrigin
	// Put: the image was written to the target.
	Put
	// Deleted: the item was deleted in the target.
	Deleted
	// Superseded: the target already holds the same or a newer version and
	// the record was not written.
	Superseded
	// SupersededViewsApplied: Superseded, and the conflict hook applied the
	// target region's materialized views to the losing image.
	SupersededViewsApplied
)

// String names the outcome.
func (o Outcome) String() string {
	switch o {
	case Invalid:
		return "invalid"
	case SkippedOrigin:
		return "skipped-origin"
	case Put:
		return "put"
	case Deleted:
		return "deleted"
	case Superseded:
		return "superseded"
	case SupersededViewsApplied:
		return "superseded-views-applied"
	}
	return fmt.Sprintf("Outcome(%d)", int(o))
}

// ConflictHook is called for a replicated INSERT/MODIFY image that the
// target region rejected because it already holds the same or a newer
// version of the item (region is the target region, table the physical
// table, seq identifies the record as "<origin>:<sequence number>"). It
// applies materialized views to the losing version (mvworker.ImageApplier);
// an error fails the record, which is retried, so it must be idempotent.
type ConflictHook func(ctx context.Context, region, table, seq string, image map[string]ddbtypes.AttributeValue) error

// WriteAPI is the part of the DynamoDB client an Applier uses (the target
// region's client).
type WriteAPI interface {
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(ctx context.Context, in *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

// ApplierConfig configures an Applier.
type ApplierConfig struct {
	// Source is the region whose stream the records come from: an
	// INSERT/MODIFY image whose _origin is another region is skipped.
	Source string
	// Target names the target region; Client is its DynamoDB client.
	Target string
	Client WriteAPI
	// Table is the replicated table (physical name and version column).
	Table TableSpec
	// OnConflict, when set, receives superseded images (see ConflictHook);
	// nil disables it (e.g. for the catalog table).
	OnConflict ConflictHook
	Logger     *slog.Logger
}

// Applier applies the records of one table's stream in one region to the
// same table in another region, with the replication conditions described
// in the package comment. It is independent of how the records were read
// (stream poller or Lambda event source mapping) and safe for concurrent
// use; records of the same item must be applied in stream order.
type Applier struct {
	cfg ApplierConfig
	log *slog.Logger
}

// NewApplier validates cfg and returns an Applier.
func NewApplier(cfg ApplierConfig) (*Applier, error) {
	switch {
	case cfg.Source == "" || cfg.Target == "":
		return nil, errors.New("replicator: applier needs Source and Target regions")
	case cfg.Source == cfg.Target:
		return nil, fmt.Errorf("replicator: applier source and target are both %q", cfg.Source)
	case cfg.Client == nil:
		return nil, errors.New("replicator: applier needs the target's DynamoDB client")
	case cfg.Table.Name == "":
		return nil, errors.New("replicator: applier needs a table")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Applier{cfg: cfg, log: cfg.Logger}, nil
}

// Apply applies one record to the target. An error means the write (or the
// conflict hook) failed and the record must be retried: every write is
// idempotent under its condition.
func (a *Applier) Apply(ctx context.Context, rec Record) (Outcome, error) {
	switch rec.EventName {
	case EventInsert, EventModify:
		return a.put(ctx, rec.Seq, rec.NewImage)
	case EventRemove:
		return a.delete(ctx, rec.Keys, rec.OldImage)
	default:
		return a.invalid("unknown event name " + rec.EventName), nil
	}
}

func (a *Applier) put(ctx context.Context, seq string, item map[string]ddbtypes.AttributeValue) (Outcome, error) {
	origin, ok := stringAttr(item, attrOrigin)
	if !ok {
		return a.invalid("image without _origin"), nil
	}
	if origin != a.cfg.Source {
		return SkippedOrigin, nil
	}
	ts, ok := numberAttr(item, attrTS)
	if !ok {
		return a.invalid("image without numeric _ts"), nil
	}
	cond, names, values := putCondition(item, ts, origin, a.cfg.Table.VersionColumn)
	_, err := a.cfg.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                 aws.String(a.cfg.Table.Name),
		Item:                      item,
		ConditionExpression:       aws.String(cond),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		// On a failed condition, return the item that won, to tell a
		// superseded version from a redelivery of this very version.
		ReturnValuesOnConditionCheckFailure: ddbtypes.ReturnValuesOnConditionCheckFailureAllOld,
	})
	switch {
	case err == nil:
		return Put, nil
	case !streams.IsConditionFailed(err):
		return Superseded, fmt.Errorf("put into %s/%s: %w", a.cfg.Target, a.cfg.Table.Name, err)
	case a.cfg.OnConflict == nil || sameRow(err, item):
		return Superseded, nil
	}
	// The target keeps its (newer) version, so this one never reaches the
	// target region's stream: feed it to the region's views here.
	if herr := a.cfg.OnConflict(ctx, a.cfg.Target, a.cfg.Table.Name, a.cfg.Source+":"+seq, item); herr != nil {
		return Superseded, fmt.Errorf("apply views of superseded %s row in %s: %w", a.cfg.Table.Name, a.cfg.Target, herr)
	}
	return SupersededViewsApplied, nil
}

func (a *Applier) delete(ctx context.Context, key, old map[string]ddbtypes.AttributeValue) (Outcome, error) {
	ts, ok := numberAttr(old, attrTS)
	if !ok || len(key) == 0 {
		return a.invalid("REMOVE without keys or old image _ts"), nil
	}
	_, err := a.cfg.Client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:                 aws.String(a.cfg.Table.Name),
		Key:                       key,
		ConditionExpression:       aws.String("#ts <= :ts"),
		ExpressionAttributeNames:  map[string]string{"#ts": attrTS},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":ts": &ddbtypes.AttributeValueMemberN{Value: ts}},
	})
	switch {
	case err == nil:
		return Deleted, nil
	case streams.IsConditionFailed(err):
		return Superseded, nil
	default:
		return Superseded, fmt.Errorf("delete into %s/%s: %w", a.cfg.Target, a.cfg.Table.Name, err)
	}
}

func (a *Applier) invalid(why string) Outcome {
	a.log.Warn("skipping record that cannot be replicated", "reason", why)
	return Invalid
}

// counters are the replication counters shared by the poller and the
// Lambda entry point.
type counters struct {
	put, deleted, skipOrigin, skipConflict, skipInvalid, conflictViews, errs atomic.Uint64
}

// record counts one Apply result.
func (c *counters) record(o Outcome, err error) {
	if err != nil {
		c.errs.Add(1)
		return
	}
	switch o {
	case Put:
		c.put.Add(1)
	case Deleted:
		c.deleted.Add(1)
	case SkippedOrigin:
		c.skipOrigin.Add(1)
	case Superseded:
		c.skipConflict.Add(1)
	case SupersededViewsApplied:
		c.skipConflict.Add(1)
		c.conflictViews.Add(1)
	default:
		c.skipInvalid.Add(1)
	}
}

func (c *counters) stats() Stats {
	return Stats{
		Put: c.put.Load(), Deleted: c.deleted.Load(),
		SkippedOrigin: c.skipOrigin.Load(), SkippedConflict: c.skipConflict.Load(),
		SkippedInvalid: c.skipInvalid.Load(), ConflictViews: c.conflictViews.Load(), Errors: c.errs.Load(),
	}
}
