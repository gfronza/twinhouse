package streams

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ErrLeaseLost is returned when a lease operation finds the shard owned by
// another reader (the lease expired and was taken over).
var ErrLeaseLost = errors.New("streams: shard lease lost")

// Checkpoint item attributes in the checkpoints table.
const (
	attrPK           = "_pk"
	attrSeq          = "seq"
	attrDone         = "done"
	attrLeaseOwner   = "lease_owner"
	attrLeaseExpires = "lease_expires" // unix millis
	attrUpdatedAt    = "updated_at"    // unix millis
	attrExp          = "_exp"          // unix seconds; TTL hint for finished shards
)

// doneRetention is how long a finished shard's checkpoint is kept (TTL hint).
// Closed shards stay listed by DescribeStream for 24h, so this must exceed it.
const doneRetention = 7 * 24 * time.Hour

// CheckpointKey returns the checkpoints-table key of (consumer, stream, shard).
func CheckpointKey(consumer, streamARN, shardID string) string {
	return consumer + "|" + streamARN + "|" + shardID
}

// checkpoint is the persisted state of one (consumer, shard).
type checkpoint struct {
	Seq     string // last fully processed sequence number ("" = none)
	Done    bool   // shard closed and fully processed
	Owner   string // current lease owner ("" = free)
	Expires time.Time
}

// leaseStore persists checkpoints and leases. Every mutation except acquire
// is conditional on the caller owning the lease and returns ErrLeaseLost
// otherwise.
type leaseStore interface {
	// acquire takes the lease if it is free, expired or already held by
	// owner, and the shard is not done. ok reports success; cp is the current
	// state either way.
	acquire(ctx context.Context, key, owner string, ttl time.Duration) (cp checkpoint, ok bool, err error)
	renew(ctx context.Context, key, owner string, ttl time.Duration) error
	checkpoint(ctx context.Context, key, owner, seq string, ttl time.Duration) error
	markDone(ctx context.Context, key, owner, seq string) error
	release(ctx context.Context, key, owner string) error
	get(ctx context.Context, key string) (cp checkpoint, found bool, err error)
}

// dynamoLeases implements leaseStore on a DynamoDB table keyed by _pk (S).
type dynamoLeases struct {
	ddb   DynamoDBAPI
	table string
	now   func() time.Time
}

func millis(t time.Time) string { return strconv.FormatInt(t.UnixMilli(), 10) }

func num(v string) *ddbtypes.AttributeValueMemberN { return &ddbtypes.AttributeValueMemberN{Value: v} }
func str(v string) *ddbtypes.AttributeValueMemberS { return &ddbtypes.AttributeValueMemberS{Value: v} }

func (d *dynamoLeases) key(k string) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{attrPK: str(k)}
}

func (d *dynamoLeases) acquire(ctx context.Context, key, owner string, ttl time.Duration) (checkpoint, bool, error) {
	now := d.now()
	cctx, cancel := callCtx(ctx)
	out, err := d.ddb.UpdateItem(cctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(d.table),
		Key:              d.key(key),
		UpdateExpression: aws.String("SET #o = :o, #e = :e, #u = :now"),
		ConditionExpression: aws.String("(attribute_not_exists(#d) OR #d = :f) AND " +
			"(attribute_not_exists(#o) OR #o = :o OR #e < :now)"),
		ExpressionAttributeNames: map[string]string{
			"#o": attrLeaseOwner, "#e": attrLeaseExpires, "#u": attrUpdatedAt, "#d": attrDone,
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":o":   str(owner),
			":e":   num(millis(now.Add(ttl))),
			":now": num(millis(now)),
			":f":   &ddbtypes.AttributeValueMemberBOOL{Value: false},
		},
		ReturnValues: ddbtypes.ReturnValueAllNew,
	})
	cancel()
	if err != nil {
		if isConditionFailed(err) {
			cp, _, gerr := d.get(ctx, key)
			return cp, false, gerr
		}
		return checkpoint{}, false, fmt.Errorf("acquire lease %s: %w", key, err)
	}
	return parseCheckpoint(out.Attributes), true, nil
}

func (d *dynamoLeases) owned(ctx context.Context, key, owner, update string, names map[string]string, values map[string]ddbtypes.AttributeValue) error {
	names["#o"] = attrLeaseOwner
	values[":o"] = str(owner)
	cctx, cancel := callCtx(ctx)
	_, err := d.ddb.UpdateItem(cctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(d.table),
		Key:                       d.key(key),
		UpdateExpression:          aws.String(update),
		ConditionExpression:       aws.String("#o = :o"),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	})
	cancel()
	if isConditionFailed(err) {
		return ErrLeaseLost
	}
	return err
}

func (d *dynamoLeases) renew(ctx context.Context, key, owner string, ttl time.Duration) error {
	now := d.now()
	return d.owned(ctx, key, owner, "SET #e = :e, #u = :now",
		map[string]string{"#e": attrLeaseExpires, "#u": attrUpdatedAt},
		map[string]ddbtypes.AttributeValue{":e": num(millis(now.Add(ttl))), ":now": num(millis(now))})
}

func (d *dynamoLeases) checkpoint(ctx context.Context, key, owner, seq string, ttl time.Duration) error {
	now := d.now()
	return d.owned(ctx, key, owner, "SET #s = :s, #e = :e, #u = :now",
		map[string]string{"#s": attrSeq, "#e": attrLeaseExpires, "#u": attrUpdatedAt},
		map[string]ddbtypes.AttributeValue{
			":s": str(seq), ":e": num(millis(now.Add(ttl))), ":now": num(millis(now)),
		})
}

func (d *dynamoLeases) markDone(ctx context.Context, key, owner, seq string) error {
	now := d.now()
	names := map[string]string{"#d": attrDone, "#u": attrUpdatedAt, "#x": attrExp, "#e": attrLeaseExpires}
	values := map[string]ddbtypes.AttributeValue{
		":t":   &ddbtypes.AttributeValueMemberBOOL{Value: true},
		":now": num(millis(now)),
		":x":   num(strconv.FormatInt(now.Add(doneRetention).Unix(), 10)),
	}
	set := "SET #d = :t, #u = :now, #x = :x"
	if seq != "" {
		names["#s"] = attrSeq
		values[":s"] = str(seq)
		set += ", #s = :s"
	}
	return d.owned(ctx, key, owner, set+" REMOVE #o, #e", names, values)
}

func (d *dynamoLeases) release(ctx context.Context, key, owner string) error {
	err := d.owned(ctx, key, owner, "REMOVE #o, #e", map[string]string{"#e": attrLeaseExpires}, map[string]ddbtypes.AttributeValue{})
	if errors.Is(err, ErrLeaseLost) {
		return nil
	}
	return err
}

func (d *dynamoLeases) get(ctx context.Context, key string) (checkpoint, bool, error) {
	cctx, cancel := callCtx(ctx)
	out, err := d.ddb.GetItem(cctx, &dynamodb.GetItemInput{
		TableName:      aws.String(d.table),
		Key:            d.key(key),
		ConsistentRead: aws.Bool(true),
	})
	cancel()
	if err != nil {
		return checkpoint{}, false, fmt.Errorf("read checkpoint %s: %w", key, err)
	}
	if out.Item == nil {
		return checkpoint{}, false, nil
	}
	return parseCheckpoint(out.Item), true, nil
}

func parseCheckpoint(item map[string]ddbtypes.AttributeValue) checkpoint {
	var cp checkpoint
	if v, ok := item[attrSeq].(*ddbtypes.AttributeValueMemberS); ok {
		cp.Seq = v.Value
	}
	if v, ok := item[attrDone].(*ddbtypes.AttributeValueMemberBOOL); ok {
		cp.Done = v.Value
	}
	if v, ok := item[attrLeaseOwner].(*ddbtypes.AttributeValueMemberS); ok {
		cp.Owner = v.Value
	}
	if v, ok := item[attrLeaseExpires].(*ddbtypes.AttributeValueMemberN); ok {
		if ms, err := strconv.ParseInt(v.Value, 10, 64); err == nil {
			cp.Expires = time.UnixMilli(ms)
		}
	}
	return cp
}

// EnsureCheckpointTable creates the checkpoints table (_pk S, on-demand) if
// it does not exist and waits until it is ACTIVE. Processes that may start
// before the server (which normally creates system tables) call this.
func EnsureCheckpointTable(ctx context.Context, ddb TableAdmin, table string) error {
	cctx, cancel := callCtx(ctx)
	_, err := ddb.DescribeTable(cctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
	cancel()
	if err == nil {
		return waitActive(ctx, ddb, table)
	}
	if !isTableNotFound(err) {
		return fmt.Errorf("describe %s: %w", table, err)
	}
	_, err = ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String(table),
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String(attrPK), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String(attrPK), KeyType: ddbtypes.KeyTypeHash}},
		BillingMode:          ddbtypes.BillingModePayPerRequest,
	})
	var inUse *ddbtypes.ResourceInUseException
	if err != nil && !errors.As(err, &inUse) {
		return fmt.Errorf("create %s: %w", table, err)
	}
	return waitActive(ctx, ddb, table)
}

func waitActive(ctx context.Context, ddb TableAdmin, table string) error {
	for {
		cctx, cancel := callCtx(ctx)
		out, err := ddb.DescribeTable(cctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
		cancel()
		if err == nil && out.Table != nil && out.Table.TableStatus == ddbtypes.TableStatusActive {
			return nil
		}
		if err != nil && !isTableNotFound(err) {
			return fmt.Errorf("describe %s: %w", table, err)
		}
		if err := sleep(ctx, 200*time.Millisecond); err != nil {
			return err
		}
	}
}

// callTimeout bounds one DynamoDB / DynamoDB Streams call (including the
// SDK's retries), so a stalled endpoint cannot hang a shard consumer or a
// lease renewal forever; the caller's retry loop then tries again.
var callTimeout = time.Minute

func callCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, callTimeout)
}
