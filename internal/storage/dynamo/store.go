package dynamo

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/gfronza/twinhouse/internal/awsx"
	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/config"
)

// ddbAPI is the subset of the DynamoDB client the store uses (an interface so
// tests can intercept calls DynamoDB Local cannot serve, e.g. replicas).
type ddbAPI interface {
	CreateTable(context.Context, *dynamodb.CreateTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	DescribeTable(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	UpdateTable(context.Context, *dynamodb.UpdateTableInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error)
	DeleteTable(context.Context, *dynamodb.DeleteTableInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error)
	ListTables(context.Context, *dynamodb.ListTablesInput, ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error)
	DescribeTimeToLive(context.Context, *dynamodb.DescribeTimeToLiveInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error)
	UpdateTimeToLive(context.Context, *dynamodb.UpdateTimeToLiveInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTimeToLiveOutput, error)
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	BatchWriteItem(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
	BatchGetItem(context.Context, *dynamodb.BatchGetItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
	TransactWriteItems(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
}

// Concurrency limits of a single store call.
const (
	writeParallelism = 16 // concurrent item writers / merge groups
	rangeParallelism = 16 // concurrent Query ranges of one Scan
	batchWriteSize   = 25 // BatchWriteItem limit
	batchGetSize     = 100
	memberTxChunk    = 50 // members per TransactWriteItems (plus the row update)

	maxPartitionKeyLen = 2048
	maxSortKeyLen      = 1024
	// maxItemSize is DynamoDB's item size limit (attribute names included).
	maxItemSize = 400 << 10
	// maxExpressionLen is DynamoDB's limit on each expression string.
	maxExpressionLen = 4096
)

type storeImpl struct {
	ddb  ddbAPI
	opts Options
	now  func() time.Time
	// tableWait bounds how long table creation/deletion is awaited.
	tableWait time.Duration
	// parseType parses catalog column types (nil: chtype.ParseStored); tests inject one.
	parseType func(string) (chtype.Type, error)
	// lastStamp is the newest write time handed out by stamp (unix nanos).
	lastStamp atomic.Int64
}

func (s *storeImpl) parse(text string) (chtype.Type, error) {
	if s.parseType != nil {
		return s.parseType(text)
	}
	return chtype.ParseStored(text)
}

// stamp returns n consecutive write times (unix nanos) starting at the
// returned value: the wall clock, but always after every time this store
// handed out before, so rows written in order by one process (several rows
// of one INSERT for the same ReplacingMergeTree key included) get strictly
// increasing _ts.
func (s *storeImpl) stamp(n int) int64 {
	if n < 1 {
		n = 1
	}
	for {
		last := s.lastStamp.Load()
		base := s.now().UnixNano()
		if base <= last {
			base = last + 1
		}
		if s.lastStamp.CompareAndSwap(last, base+int64(n)-1) {
			return base
		}
	}
}

// bumpStamp makes later stamps exceed ts (a stored write time seen in a
// condition failure).
func (s *storeImpl) bumpStamp(ts int64) {
	for {
		last := s.lastStamp.Load()
		if last >= ts || s.lastStamp.CompareAndSwap(last, ts) {
			return
		}
	}
}

func newStore(c *awsx.Clients, opts Options) (*Store, error) {
	if c == nil || c.DynamoDB == nil {
		return nil, fmt.Errorf("dynamo: nil DynamoDB client")
	}
	return newStoreWithAPI(c.DynamoDB, c.Region, opts)
}

func newStoreWithAPI(api ddbAPI, region string, opts Options) (*Store, error) {
	if opts.Region == "" {
		opts.Region = region
	}
	if opts.Region == "" {
		return nil, fmt.Errorf("dynamo: region is required")
	}
	switch opts.GlobalMode {
	case "":
		opts.GlobalMode = config.GlobalNone
	case config.GlobalNone, config.GlobalAWS, config.GlobalEmulated:
	default:
		return nil, fmt.Errorf("dynamo: unknown global mode %q", opts.GlobalMode)
	}
	if opts.ScanSegments <= 0 {
		opts.ScanSegments = 4
	}
	if opts.MarkerTTLDays <= 0 {
		opts.MarkerTTLDays = 7
	}
	return &Store{impl: &storeImpl{ddb: api, opts: opts, now: time.Now, tableWait: 5 * time.Minute}}, nil
}

func (s *Store) physicalName(db, table string) string { return s.impl.opts.Prefix + db + "." + table }
func (s *Store) catalogTableName() string             { return s.impl.opts.Prefix + "__catalog" }
func (s *Store) markersTableName() string             { return s.impl.opts.Prefix + "__mv_markers" }
func (s *Store) checkpointsTableName() string         { return s.impl.opts.Prefix + "__checkpoints" }

func (s *Store) newCatalog() catalog.Catalog {
	return catalog.New(&catalogBackend{s: s.impl, table: s.catalogTableName()},
		catalog.Options{PhysicalName: s.physicalName, ParseType: s.impl.parseType, Region: s.impl.opts.Region,
			CheckTable: s.impl.checkTable})
}

// physical returns t's DynamoDB table name.
func (s *storeImpl) physical(t *catalog.Table) string {
	if t.PhysicalName != "" {
		return t.PhysicalName
	}
	return s.opts.Prefix + t.Database + "." + t.Name
}

// membersSuffix marks the side table of an aggregate table's set members.
const membersSuffix = ".members"

func membersTable(physical string) string { return physical + membersSuffix }

func (s *storeImpl) markersTable() string { return s.opts.Prefix + "__mv_markers" }

// randomBytes returns n bytes from crypto/rand (whose Read never fails).
func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}
