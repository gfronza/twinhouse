package streams

// Integration tests against DynamoDB Local. They run when DDB_TEST_ENDPOINT
// is set (e.g. http://localhost:18101) and are skipped otherwise.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"dynamodb-eventstream/internal/awsx"
)

func localClients(t *testing.T) *awsx.Clients {
	t.Helper()
	ep := os.Getenv("DDB_TEST_ENDPOINT")
	if ep == "" {
		t.Skip("DDB_TEST_ENDPOINT not set")
	}
	c, err := awsx.New(context.Background(), "us-east-1", ep)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func uniqueName(base string) string {
	return fmt.Sprintf("%s-%d", base, time.Now().UnixNano())
}

// createStreamTable creates a _pk S table with NEW_AND_OLD_IMAGES streams.
func createStreamTable(t *testing.T, c *awsx.Clients, name string) {
	t.Helper()
	ctx := context.Background()
	_, err := c.DynamoDB.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String(name),
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("k"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("k"), KeyType: ddbtypes.KeyTypeHash}},
		BillingMode:          ddbtypes.BillingModePayPerRequest,
		StreamSpecification: &ddbtypes.StreamSpecification{
			StreamEnabled: aws.Bool(true), StreamViewType: ddbtypes.StreamViewTypeNewAndOldImages,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = c.DynamoDB.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(name)})
	})
}

func putKeys(t *testing.T, c *awsx.Clients, table string, ks ...string) {
	t.Helper()
	for _, k := range ks {
		_, err := c.DynamoDB.PutItem(context.Background(), &dynamodb.PutItemInput{
			TableName: aws.String(table),
			Item:      map[string]ddbtypes.AttributeValue{"k": str(k)},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func localConfig(c *awsx.Clients, table, ckpt, owner string, h Handler) Config {
	return Config{
		Streams:           c.Streams,
		DynamoDB:          c.DynamoDB,
		Table:             table,
		CheckpointTable:   ckpt,
		Consumer:          "itest",
		Owner:             owner,
		Handler:           h,
		PollInterval:      10 * time.Millisecond,
		MaxIdleBackoff:    50 * time.Millisecond,
		LeaseDuration:     time.Second,
		DiscoveryInterval: 100 * time.Millisecond,
		MaxRetryBackoff:   50 * time.Millisecond,
		Logger:            quietLogger(),
	}
}

func setupLocal(t *testing.T) (*awsx.Clients, string, string) {
	c := localClients(t)
	table, ckpt := uniqueName("streams-src"), uniqueName("streams-ckpt")
	createStreamTable(t, c, table)
	if err := EnsureCheckpointTable(context.Background(), c.DynamoDB, ckpt); err != nil {
		t.Fatal(err)
	}
	// Idempotent.
	if err := EnsureCheckpointTable(context.Background(), c.DynamoDB, ckpt); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = c.DynamoDB.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(ckpt)})
	})
	return c, table, ckpt
}

func TestIntegrationDynamoLeases(t *testing.T) {
	c, _, ckpt := setupLocal(t)
	ctx := context.Background()
	now := time.Now()
	l := &dynamoLeases{ddb: c.DynamoDB, table: ckpt, now: func() time.Time { return now }}
	const key = "c|arn|shard"
	ttl := 10 * time.Second

	if _, found, err := l.get(ctx, key); err != nil || found {
		t.Fatalf("get missing: found=%v err=%v", found, err)
	}
	if cp, ok, err := l.acquire(ctx, key, "a", ttl); err != nil || !ok || cp.Owner != "a" || cp.Seq != "" {
		t.Fatalf("acquire a: %+v %v %v", cp, ok, err)
	}
	if _, ok, err := l.acquire(ctx, key, "a", ttl); err != nil || !ok {
		t.Fatalf("re-acquire by owner: %v %v", ok, err)
	}
	if cp, ok, err := l.acquire(ctx, key, "b", ttl); err != nil || ok || cp.Owner != "a" {
		t.Fatalf("acquire b while held: %+v %v %v", cp, ok, err)
	}
	if err := l.checkpoint(ctx, key, "a", "0001", ttl); err != nil {
		t.Fatal(err)
	}
	if err := l.checkpoint(ctx, key, "b", "0002", ttl); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("checkpoint by non-owner: %v", err)
	}
	if err := l.renew(ctx, key, "b", ttl); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("renew by non-owner: %v", err)
	}

	// Expiry: b steals and resumes from a's checkpoint; a is fenced off.
	now = now.Add(ttl + time.Millisecond)
	cp, ok, err := l.acquire(ctx, key, "b", ttl)
	if err != nil || !ok || cp.Seq != "0001" || cp.Owner != "b" {
		t.Fatalf("steal expired: %+v %v %v", cp, ok, err)
	}
	if err := l.checkpoint(ctx, key, "a", "0009", ttl); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("old owner checkpoint after steal: %v", err)
	}
	if err := l.renew(ctx, key, "b", ttl); err != nil {
		t.Fatal(err)
	}

	// Release lets anyone acquire immediately; releasing someone else's lease is a no-op.
	if err := l.release(ctx, key, "a"); err != nil {
		t.Fatal(err)
	}
	if err := l.release(ctx, key, "b"); err != nil {
		t.Fatal(err)
	}
	if cp, ok, err := l.acquire(ctx, key, "c", ttl); err != nil || !ok || cp.Seq != "0001" {
		t.Fatalf("acquire after release: %+v %v %v", cp, ok, err)
	}
	if err := l.markDone(ctx, key, "c", "0003"); err != nil {
		t.Fatal(err)
	}
	cp, _, _ = l.get(ctx, key)
	if !cp.Done || cp.Owner != "" || cp.Seq != "0003" {
		t.Fatalf("after markDone: %+v", cp)
	}
	if cp, ok, err := l.acquire(ctx, key, "d", ttl); err != nil || ok || !cp.Done {
		t.Fatalf("acquire done shard: %+v %v %v", cp, ok, err)
	}
}

func TestIntegrationReaderEventsAndResume(t *testing.T) {
	c, table, ckpt := setupLocal(t)
	ctx := context.Background()
	events := make(chan string, 100)
	h := func(_ context.Context, b *Batch) error {
		for _, r := range b.Records {
			events <- string(r.EventName) + ":" + keyOf(r)
		}
		return nil
	}
	r1, err := New(localConfig(c, table, ckpt, "w1", h))
	if err != nil {
		t.Fatal(err)
	}
	ctx1, stop1 := context.WithCancel(ctx)
	done1 := make(chan struct{})
	go func() { _ = r1.Run(ctx1); close(done1) }()

	putKeys(t, c, table, "a", "b")
	_, err = c.DynamoDB.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(table),
		Item: map[string]ddbtypes.AttributeValue{"k": str("a"), "v": num("2")}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.DynamoDB.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(table),
		Key: map[string]ddbtypes.AttributeValue{"k": str("b")}})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"INSERT:a", "INSERT:b", "MODIFY:a", "REMOVE:b"}
	got := collect(t, events, len(want))
	if !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	stop1()
	<-done1

	// A new reader instance of the same consumer resumes after the checkpoint.
	putKeys(t, c, table, "c")
	r2, err := New(localConfig(c, table, ckpt, "w2", h))
	if err != nil {
		t.Fatal(err)
	}
	ctx2, stop2 := context.WithCancel(ctx)
	defer stop2()
	go func() { _ = r2.Run(ctx2) }()
	if got := collect(t, events, 1); got[0] != "INSERT:c" {
		t.Fatalf("resumed reader redelivered %v", got)
	}
	select {
	case e := <-events:
		t.Fatalf("unexpected extra record %s", e)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestIntegrationTwoReadersAndLeaseSteal(t *testing.T) {
	c, table, ckpt := setupLocal(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Find the shard and pretend a crashed reader holds its lease.
	r0, _ := New(localConfig(c, table, ckpt, "probe", func(context.Context, *Batch) error { return nil }))
	arn, err := r0.latestStreamARN(ctx)
	if err != nil || arn == "" {
		t.Fatalf("stream arn: %q %v", arn, err)
	}
	putKeys(t, c, table, "first")
	shards, err := r0.describeShards(ctx, arn)
	if err != nil || len(shards) == 0 {
		t.Fatalf("shards: %v %v", shards, err)
	}
	dead := &dynamoLeases{ddb: c.DynamoDB, table: ckpt, now: time.Now}
	for _, s := range shards {
		if _, ok, err := dead.acquire(ctx, CheckpointKey("itest", arn, *s.ShardId), "dead", 800*time.Millisecond); err != nil || !ok {
			t.Fatalf("seed lease: %v %v", ok, err)
		}
	}
	seeded := time.Now()

	var total atomic.Int64
	seen := make(chan string, 1000)
	h := func(_ context.Context, b *Batch) error {
		for _, r := range b.Records {
			total.Add(1)
			seen <- keyOf(r)
		}
		return nil
	}
	for _, o := range []string{"r1", "r2"} {
		r, err := New(localConfig(c, table, ckpt, o, h))
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = r.Run(ctx) }()
	}
	first := collect(t, seen, 1)
	if time.Since(seeded) < 800*time.Millisecond {
		t.Fatal("lease stolen before expiry")
	}
	ks := keys("k", 40)
	putKeys(t, c, table, ks...)
	got := append(first, collect(t, seen, len(ks))...)
	time.Sleep(300 * time.Millisecond)
	if n := total.Load(); n != int64(len(ks)+1) {
		t.Fatalf("delivered %d records, want %d (duplicates)", n, len(ks)+1)
	}
	slices.Sort(got)
	want := append([]string{"first"}, ks...)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("got %v", got)
	}
}

func TestIntegrationAtLeastOnce(t *testing.T) {
	c, table, ckpt := setupLocal(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	seen := make(chan string, 10)
	h := func(_ context.Context, b *Batch) error {
		if calls.Add(1) <= 2 {
			return errors.New("transient")
		}
		for _, r := range b.Records {
			seen <- keyOf(r)
		}
		return nil
	}
	r, _ := New(localConfig(c, table, ckpt, "w", h))
	go func() { _ = r.Run(ctx) }()
	putKeys(t, c, table, "x")
	if got := collect(t, seen, 1); got[0] != "x" {
		t.Fatalf("got %v", got)
	}
	if st := r.Stats(); st.HandlerErrors != 2 || st.Records != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func collect(t *testing.T, ch <-chan string, n int) []string {
	t.Helper()
	var out []string
	timeout := time.After(15 * time.Second)
	for len(out) < n {
		select {
		case s := <-ch:
			out = append(out, s)
		case <-timeout:
			t.Fatalf("timed out after %d/%d records: %v", len(out), n, out)
		}
	}
	return out
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// A brand-new (empty) table's shard is read cleanly from TRIM_HORIZON even
// when other tables are written before its first record: DynamoDB Local used
// to fail the reader's empty-poll iterator with TrimmedDataAccessException.
func TestIntegrationFreshShardWithConcurrentWritesElsewhere(t *testing.T) {
	c, table, ckpt := setupLocal(t)
	other := uniqueName("streams-other")
	createStreamTable(t, c, other)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make(chan string, 100)
	var logs syncBuffer
	cfg := localConfig(c, table, ckpt, "w", func(_ context.Context, b *Batch) error {
		for _, r := range b.Records {
			seen <- keyOf(r)
		}
		return nil
	})
	cfg.Logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = r.Run(ctx) }()
	// Let the reader lease the shard and poll it empty a few times.
	deadline := time.Now().Add(10 * time.Second)
	for r.Stats().ActiveShards == 0 {
		if time.Now().After(deadline) {
			t.Fatal("reader never leased the shard")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	putKeys(t, c, other, "x1", "x2", "x3")
	putKeys(t, c, table, "a", "b")
	if got := collect(t, seen, 2); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("got %v", got)
	}
	putKeys(t, c, other, "y1", "y2")
	putKeys(t, c, table, "c")
	if got := collect(t, seen, 1); got[0] != "c" {
		t.Fatalf("got %v", got)
	}
	if out := logs.String(); strings.Contains(out, "trimmed") || strings.Contains(out, "Trimmed") {
		t.Fatalf("reader went through the trimmed-data path:\n%s", out)
	}
}
