package streams

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	sttypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

// fakeShard is an in-memory stream shard.
type fakeShard struct {
	id, parent string
	recs       []sttypes.Record
	closed     bool
	trimmed    int // records with index < trimmed are gone
}

// fakeAWS fakes DynamoDB Streams plus DescribeTable. Iterators are
// "arn|shard|pos" (pos = index of the next record).
type fakeAWS struct {
	mu          sync.Mutex
	table       string
	tableExists bool
	latest      string
	streams     map[string][]*fakeShard
	seq         int
	pageSize    int
	expireNext  int // fail this many GetRecords calls with ExpiredIterator
	trimNext    int // fail this many GetRecords calls with TrimmedDataAccess
	getRecords  int
	// staleEmptyIters emulates a DynamoDB Local quirk: the NextShardIterator
	// returned by an empty GetRecords on a shard that has never had a record
	// fails with TrimmedDataAccessException once the shard gets records
	// (DynamoDB Local's sequence numbers are global across tables).
	staleEmptyIters  bool
	staleTrimmed     int   // GetRecords calls failed by staleEmptyIters
	trimHorizonIters int   // TRIM_HORIZON iterators handed out
	describeErr      error // returned by DescribeStream while set
	describeCalls    int
}

func newFakeAWS(table string) *fakeAWS {
	return &fakeAWS{table: table, streams: map[string][]*fakeShard{}}
}

// createStream makes arn the table's latest stream.
func (f *fakeAWS) createStream(arn string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tableExists = true
	f.latest = arn
	if _, ok := f.streams[arn]; !ok {
		f.streams[arn] = nil
	}
}

func (f *fakeAWS) addShard(arn, id, parent string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.streams[arn] = append(f.streams[arn], &fakeShard{id: id, parent: parent})
}

func (f *fakeAWS) shard(arn, id string) *fakeShard {
	for _, s := range f.streams[arn] {
		if s.id == id {
			return s
		}
	}
	panic("no shard " + id)
}

// put appends records with the given keys and returns their sequence numbers.
func (f *fakeAWS) put(arn, shardID string, keys ...string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.shard(arn, shardID)
	var seqs []string
	for _, k := range keys {
		f.seq++
		seq := fmt.Sprintf("%021d", f.seq)
		seqs = append(seqs, seq)
		now := time.Now()
		s.recs = append(s.recs, sttypes.Record{
			EventName: sttypes.OperationTypeInsert,
			Dynamodb: &sttypes.StreamRecord{
				SequenceNumber:              aws.String(seq),
				ApproximateCreationDateTime: &now,
				Keys:                        map[string]sttypes.AttributeValue{"k": &sttypes.AttributeValueMemberS{Value: k}},
				NewImage:                    map[string]sttypes.AttributeValue{"k": &sttypes.AttributeValueMemberS{Value: k}},
			},
		})
	}
	return seqs
}

func (f *fakeAWS) closeShard(arn, id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shard(arn, id).closed = true
}

func (f *fakeAWS) trim(arn, id string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shard(arn, id).trimmed = n
}

func (f *fakeAWS) DescribeTable(_ context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.tableExists || *in.TableName != f.table {
		return nil, &ddbtypes.ResourceNotFoundException{Message: aws.String("table not found")}
	}
	td := &ddbtypes.TableDescription{TableName: in.TableName, TableStatus: ddbtypes.TableStatusActive}
	if f.latest != "" {
		td.LatestStreamArn = aws.String(f.latest)
		td.StreamSpecification = &ddbtypes.StreamSpecification{StreamEnabled: aws.Bool(true)}
	}
	return &dynamodb.DescribeTableOutput{Table: td}, nil
}

func (f *fakeAWS) GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	panic("fakeAWS.GetItem: leases are in memory")
}

func (f *fakeAWS) UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	panic("fakeAWS.UpdateItem: leases are in memory")
}

func (f *fakeAWS) DescribeStream(_ context.Context, in *dynamodbstreams.DescribeStreamInput, _ ...func(*dynamodbstreams.Options)) (*dynamodbstreams.DescribeStreamOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.describeCalls++
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	shards, ok := f.streams[*in.StreamArn]
	if !ok {
		return nil, &sttypes.ResourceNotFoundException{Message: aws.String("stream not found")}
	}
	start := 0
	if in.ExclusiveStartShardId != nil {
		for i, s := range shards {
			if s.id == *in.ExclusiveStartShardId {
				start = i + 1
			}
		}
	}
	end := len(shards)
	if f.pageSize > 0 && start+f.pageSize < end {
		end = start + f.pageSize
	}
	desc := &sttypes.StreamDescription{StreamArn: in.StreamArn}
	for _, s := range shards[start:end] {
		sh := sttypes.Shard{ShardId: aws.String(s.id)}
		if s.parent != "" {
			sh.ParentShardId = aws.String(s.parent)
		}
		desc.Shards = append(desc.Shards, sh)
	}
	if end < len(shards) {
		desc.LastEvaluatedShardId = aws.String(shards[end-1].id)
	}
	return &dynamodbstreams.DescribeStreamOutput{StreamDescription: desc}, nil
}

func (f *fakeAWS) GetShardIterator(_ context.Context, in *dynamodbstreams.GetShardIteratorInput, _ ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetShardIteratorOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.streams[*in.StreamArn]; !ok {
		return nil, &sttypes.ResourceNotFoundException{Message: aws.String("stream not found")}
	}
	s := f.shard(*in.StreamArn, *in.ShardId)
	pos := s.trimmed
	switch in.ShardIteratorType {
	case sttypes.ShardIteratorTypeTrimHorizon:
		f.trimHorizonIters++
	case sttypes.ShardIteratorTypeAfterSequenceNumber:
		want := *in.SequenceNumber
		found := false
		for i, r := range s.recs {
			if *r.Dynamodb.SequenceNumber == want {
				if i < s.trimmed {
					return nil, &sttypes.TrimmedDataAccessException{Message: aws.String("trimmed")}
				}
				pos, found = i+1, true
			}
		}
		if !found {
			return nil, fmt.Errorf("fake: unknown sequence number %s", want)
		}
	default:
		return nil, fmt.Errorf("fake: unsupported iterator type %s", in.ShardIteratorType)
	}
	return &dynamodbstreams.GetShardIteratorOutput{
		ShardIterator: aws.String(fmt.Sprintf("%s|%s|%d", *in.StreamArn, *in.ShardId, pos)),
	}, nil
}

func (f *fakeAWS) GetRecords(_ context.Context, in *dynamodbstreams.GetRecordsInput, _ ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetRecordsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getRecords++
	if f.expireNext > 0 {
		f.expireNext--
		return nil, &sttypes.ExpiredIteratorException{Message: aws.String("expired")}
	}
	if f.trimNext > 0 {
		f.trimNext--
		return nil, &sttypes.TrimmedDataAccessException{Message: aws.String("trimmed")}
	}
	parts := strings.Split(*in.ShardIterator, "|")
	arn, id := parts[0], parts[1]
	pos, _ := strconv.Atoi(parts[2])
	if _, ok := f.streams[arn]; !ok {
		return nil, &sttypes.ResourceNotFoundException{Message: aws.String("stream not found")}
	}
	s := f.shard(arn, id)
	if pos < s.trimmed {
		return nil, &sttypes.TrimmedDataAccessException{Message: aws.String("trimmed")}
	}
	if len(parts) > 3 && parts[3] == "stale" && len(s.recs) > 0 {
		f.staleTrimmed++
		return nil, &sttypes.TrimmedDataAccessException{Message: aws.String("The operation attempted to read past the oldest stream record in a shard.")}
	}
	end := min(len(s.recs), pos+int(aws.ToInt32(in.Limit)))
	out := &dynamodbstreams.GetRecordsOutput{Records: append([]sttypes.Record(nil), s.recs[pos:end]...)}
	if !s.closed || end < len(s.recs) {
		out.NextShardIterator = aws.String(fmt.Sprintf("%s|%s|%d", arn, id, end))
		if f.staleEmptyIters && len(s.recs) == 0 {
			*out.NextShardIterator += "|stale"
		}
	}
	return out, nil
}

// memLeases is an in-memory leaseStore with the same semantics as dynamoLeases.
type memLeases struct {
	mu    sync.Mutex
	items map[string]*checkpoint
	now   func() time.Time
}

func newMemLeases() *memLeases { return &memLeases{items: map[string]*checkpoint{}, now: time.Now} }

func (m *memLeases) acquire(_ context.Context, key, owner string, ttl time.Duration) (checkpoint, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp, ok := m.items[key]
	if !ok {
		cp = &checkpoint{}
		m.items[key] = cp
	}
	now := m.now()
	if cp.Done || (cp.Owner != "" && cp.Owner != owner && !cp.Expires.Before(now)) {
		return *cp, false, nil
	}
	cp.Owner, cp.Expires = owner, now.Add(ttl)
	return *cp, true, nil
}

func (m *memLeases) owned(key, owner string, fn func(*checkpoint)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp, ok := m.items[key]
	if !ok || cp.Owner != owner {
		return ErrLeaseLost
	}
	fn(cp)
	return nil
}

func (m *memLeases) renew(_ context.Context, key, owner string, ttl time.Duration) error {
	return m.owned(key, owner, func(cp *checkpoint) { cp.Expires = m.now().Add(ttl) })
}

func (m *memLeases) checkpoint(_ context.Context, key, owner, seq string, ttl time.Duration) error {
	return m.owned(key, owner, func(cp *checkpoint) { cp.Seq, cp.Expires = seq, m.now().Add(ttl) })
}

func (m *memLeases) markDone(_ context.Context, key, owner, seq string) error {
	return m.owned(key, owner, func(cp *checkpoint) {
		cp.Done, cp.Owner, cp.Expires = true, "", time.Time{}
		if seq != "" {
			cp.Seq = seq
		}
	})
}

func (m *memLeases) release(_ context.Context, key, owner string) error {
	err := m.owned(key, owner, func(cp *checkpoint) { cp.Owner, cp.Expires = "", time.Time{} })
	if err == ErrLeaseLost {
		return nil
	}
	return err
}

func (m *memLeases) get(_ context.Context, key string) (checkpoint, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp, ok := m.items[key]
	if !ok {
		return checkpoint{}, false, nil
	}
	return *cp, true, nil
}

func (m *memLeases) set(key string, cp checkpoint) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := cp
	m.items[key] = &c
}

func (m *memLeases) snapshot(key string) checkpoint {
	cp, _, _ := m.get(context.Background(), key)
	return cp
}

func keyOf(r sttypes.Record) string {
	return r.Dynamodb.Keys["k"].(*sttypes.AttributeValueMemberS).Value
}
