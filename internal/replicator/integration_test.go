package replicator

// Integration tests with two DynamoDB Local instances standing in for two
// regions. They run when DDB_TEST_ENDPOINT and DDB_TEST_ENDPOINT_B are set.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	sttypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/gfronza/twinhouse/internal/awsx"
)

const (
	us = "us-east-1"
	eu = "eu-west-1"
)

type env struct {
	t        *testing.T
	prefix   string
	regions  map[string]*awsx.Clients
	events   string
	states   string // ReplacingMergeTree(ver)-style table
	logs     *syncBuffer
	catalog  string
	ckpt     string
	ordering []Region
}

func newEnv(t *testing.T) *env {
	t.Helper()
	a, b := os.Getenv("DDB_TEST_ENDPOINT"), os.Getenv("DDB_TEST_ENDPOINT_B")
	if a == "" || b == "" {
		t.Skip("DDB_TEST_ENDPOINT / DDB_TEST_ENDPOINT_B not set")
	}
	e := &env{t: t, prefix: fmt.Sprintf("rt%d_", time.Now().UnixNano()), regions: map[string]*awsx.Clients{}}
	for name, ep := range map[string]string{us: a, eu: b} {
		c, err := awsx.New(context.Background(), name, ep)
		if err != nil {
			t.Fatal(err)
		}
		e.regions[name] = c
	}
	e.ordering = []Region{{us, e.regions[us]}, {eu, e.regions[eu]}}
	e.events = e.prefix + "db.events"
	e.states = e.prefix + "db.states"
	e.catalog = e.prefix + "__catalog"
	e.ckpt = e.prefix + "__checkpoints"
	t.Cleanup(func() {
		for _, c := range e.regions {
			for _, tbl := range []string{e.events, e.states, e.catalog, e.ckpt} {
				_, _ = c.DynamoDB.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(tbl)})
			}
		}
	})
	return e
}

func (e *env) createRowTable(region, name string) {
	e.t.Helper()
	e.create(region, name, []string{"_pk", "_sk"}, ddbtypes.ScalarAttributeTypeB)
}

func (e *env) createCatalogTable(region string) {
	e.t.Helper()
	e.create(region, e.catalog, []string{"_pk"}, ddbtypes.ScalarAttributeTypeS)
}

func (e *env) create(region, name string, keys []string, typ ddbtypes.ScalarAttributeType) {
	in := &dynamodb.CreateTableInput{
		TableName:   aws.String(name),
		BillingMode: ddbtypes.BillingModePayPerRequest,
		StreamSpecification: &ddbtypes.StreamSpecification{
			StreamEnabled: aws.Bool(true), StreamViewType: ddbtypes.StreamViewTypeNewAndOldImages,
		},
	}
	for i, k := range keys {
		kt := ddbtypes.KeyTypeHash
		if i > 0 {
			kt = ddbtypes.KeyTypeRange
		}
		in.AttributeDefinitions = append(in.AttributeDefinitions, ddbtypes.AttributeDefinition{AttributeName: aws.String(k), AttributeType: typ})
		in.KeySchema = append(in.KeySchema, ddbtypes.KeySchemaElement{AttributeName: aws.String(k), KeyType: kt})
	}
	if _, err := e.regions[region].DynamoDB.CreateTable(context.Background(), in); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) start(delay time.Duration) *Replicator {
	return e.startWith(delay, nil)
}

type conflictHook = func(ctx context.Context, region, table, seq string, image map[string]ddbtypes.AttributeValue) error

func (e *env) startWith(delay time.Duration, onConflict conflictHook) *Replicator {
	e.t.Helper()
	e.logs = &syncBuffer{}
	e.t.Cleanup(func() { checkNoDuplicateLogKeys(e.t, e.logs.String()) })
	r, err := New(Config{
		Regions: e.ordering,
		Prefix:  e.prefix,
		Tables: func(context.Context) ([]TableSpec, error) {
			return []TableSpec{{Name: e.events}, {Name: e.states, VersionColumn: "ver"}}, nil
		},
		Delay:             delay,
		Owner:             "test",
		PollInterval:      10 * time.Millisecond,
		LeaseDuration:     2 * time.Second,
		DiscoveryInterval: 100 * time.Millisecond,
		OnConflict:        onConflict,
		Logger:            slog.New(slog.NewJSONHandler(e.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	if err != nil {
		e.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = r.Run(ctx); close(done) }()
	e.t.Cleanup(func() { cancel(); <-done })
	return r
}

func key(pk, sk string) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		"_pk": &ddbtypes.AttributeValueMemberB{Value: []byte(pk)},
		"_sk": &ddbtypes.AttributeValueMemberB{Value: []byte(sk)},
	}
}

// row builds an item with system attributes plus v.
func row(pk, sk string, ts int64, origin, v string) map[string]ddbtypes.AttributeValue {
	item := key(pk, sk)
	item["_ts"] = &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(ts, 10)}
	item["_origin"] = &ddbtypes.AttributeValueMemberS{Value: origin}
	item["_rid"] = &ddbtypes.AttributeValueMemberS{Value: "rid-" + pk + sk}
	item["v"] = &ddbtypes.AttributeValueMemberS{Value: v}
	return item
}

func (e *env) put(region, table string, item map[string]ddbtypes.AttributeValue) {
	e.t.Helper()
	_, err := e.regions[region].DynamoDB.PutItem(context.Background(), &dynamodb.PutItemInput{TableName: aws.String(table), Item: item})
	if err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) get(region, table string, k map[string]ddbtypes.AttributeValue) map[string]ddbtypes.AttributeValue {
	e.t.Helper()
	out, err := e.regions[region].DynamoDB.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(table), Key: k, ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return out.Item
}

func (e *env) eventually(what string, cond func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			e.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func vOf(item map[string]ddbtypes.AttributeValue) string {
	if v, ok := item["v"].(*ddbtypes.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}

// streamRecords counts every record currently in the table's stream.
func (e *env) streamRecords(region, table string) int {
	e.t.Helper()
	ctx := context.Background()
	c := e.regions[region]
	td, err := c.DynamoDB.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
	if err != nil {
		e.t.Fatal(err)
	}
	ds, err := c.Streams.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: td.Table.LatestStreamArn})
	if err != nil {
		e.t.Fatal(err)
	}
	n := 0
	for _, s := range ds.StreamDescription.Shards {
		it, err := c.Streams.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
			StreamArn: td.Table.LatestStreamArn, ShardId: s.ShardId, ShardIteratorType: sttypes.ShardIteratorTypeTrimHorizon,
		})
		if err != nil {
			e.t.Fatal(err)
		}
		iter := it.ShardIterator
		for iter != nil {
			out, err := c.Streams.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: iter})
			if err != nil {
				e.t.Fatal(err)
			}
			n += len(out.Records)
			if len(out.Records) == 0 {
				break
			}
			iter = out.NextShardIterator
		}
	}
	return n
}

// quiesce waits until the replicator's counters stop changing.
func quiesce(r *Replicator) {
	prev := r.Stats()
	for stable := 0; stable < 10; {
		time.Sleep(50 * time.Millisecond)
		cur := r.Stats()
		if cur == prev {
			stable++
		} else {
			stable = 0
		}
		prev = cur
	}
}

func TestIntegrationReplicatesBothWaysWithoutLoops(t *testing.T) {
	e := newEnv(t)
	for _, reg := range []string{us, eu} {
		e.createCatalogTable(reg)
		e.createRowTable(reg, e.events)
	}
	r := e.start(0)

	// us → eu, every attribute type preserved verbatim.
	x := row("x\x00", "1", time.Now().UnixNano(), us, "")
	x["bin"] = &ddbtypes.AttributeValueMemberB{Value: []byte{0, 0xff, 0}}
	x["list"] = &ddbtypes.AttributeValueMemberL{Value: []ddbtypes.AttributeValue{
		&ddbtypes.AttributeValueMemberN{Value: "1.5"}, &ddbtypes.AttributeValueMemberNULL{Value: true},
	}}
	x["map"] = &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{
		"b": &ddbtypes.AttributeValueMemberBOOL{Value: true},
	}}
	x["c#n"] = &ddbtypes.AttributeValueMemberN{Value: "3"}
	e.put(us, e.events, x)
	e.eventually("x in eu", func() bool { return e.get(eu, e.events, key("x\x00", "1")) != nil })
	if got := e.get(eu, e.events, key("x\x00", "1")); !reflect.DeepEqual(got, x) {
		t.Fatalf("replica differs:\n got %#v\nwant %#v", got, x)
	}

	// eu → us.
	e.put(eu, e.events, row("y", "1", time.Now().UnixNano(), eu, "from-eu"))
	e.eventually("y in us", func() bool { return vOf(e.get(us, e.events, key("y", "1"))) == "from-eu" })

	// A newer write (MODIFY) replicates too.
	e.put(us, e.events, row("x\x00", "1", time.Now().UnixNano(), us, "v2"))
	e.eventually("x v2 in eu", func() bool { return vOf(e.get(eu, e.events, key("x\x00", "1"))) == "v2" })

	// Deleting in us a row written in eu deletes it in eu.
	_, err := e.regions[us].DynamoDB.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: aws.String(e.events), Key: key("y", "1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	e.eventually("y deleted in eu", func() bool { return e.get(eu, e.events, key("y", "1")) == nil })

	// The catalog is always replicated.
	e.put(us, e.catalog, map[string]ddbtypes.AttributeValue{
		"_pk":     &ddbtypes.AttributeValueMemberS{Value: "table#db.events"},
		"def":     &ddbtypes.AttributeValueMemberS{Value: `{"Name":"events"}`},
		"kind":    &ddbtypes.AttributeValueMemberS{Value: "table"},
		"_ts":     &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().UnixNano(), 10)},
		"_origin": &ddbtypes.AttributeValueMemberS{Value: us},
	})
	catKey := map[string]ddbtypes.AttributeValue{"_pk": &ddbtypes.AttributeValueMemberS{Value: "table#db.events"}}
	e.eventually("catalog entry in eu", func() bool { return e.get(eu, e.catalog, catKey) != nil })

	// No ping-pong: each stream holds exactly the local writes plus one
	// replica write per remote write, and nothing more arrives later.
	quiesce(r)
	// us: x INSERT, y INSERT (replica), x MODIFY, y REMOVE.
	// eu: x INSERT (replica), y INSERT, x MODIFY (replica), y REMOVE (replica).
	for _, reg := range []string{us, eu} {
		if n := e.streamRecords(reg, e.events); n != 4 {
			t.Errorf("%s events stream has %d records, want 4", reg, n)
		}
		if n := e.streamRecords(reg, e.catalog); n != 1 {
			t.Errorf("%s catalog stream has %d records, want 1", reg, n)
		}
	}
	time.Sleep(500 * time.Millisecond)
	for _, reg := range []string{us, eu} {
		if n := e.streamRecords(reg, e.events); n != 4 {
			t.Errorf("%s events stream grew to %d records: replication loop", reg, n)
		}
	}
	st := r.Stats()
	if st.SkippedOrigin == 0 || st.Errors != 0 || st.SkippedInvalid != 0 {
		t.Errorf("stats %+v", st)
	}
	if st.Pipes != 6 {
		t.Errorf("pipes = %d, want 6 (3 tables x 2 directions)", st.Pipes)
	}
}

func TestIntegrationLastWriterWins(t *testing.T) {
	e := newEnv(t)
	for _, reg := range []string{us, eu} {
		e.createCatalogTable(reg)
		e.createRowTable(reg, e.events)
	}
	// Conflicting writes while "partitioned" (replicator not running yet).
	e.put(us, e.events, row("k", "newer-ts", 1000, us, "us"))
	e.put(eu, e.events, row("k", "newer-ts", 2000, eu, "eu"))
	e.put(us, e.events, row("k", "tie", 5000, us, "us")) // tie: greater origin wins
	e.put(eu, e.events, row("k", "tie", 5000, eu, "eu"))
	// d was replicated earlier (ts 100); then eu updated it while us deleted it.
	e.put(us, e.events, row("d", "1", 100, us, "old"))
	e.put(eu, e.events, row("d", "1", 300, eu, "updated"))
	_, err := e.regions[us].DynamoDB.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: aws.String(e.events), Key: key("d", "1"),
	})
	if err != nil {
		t.Fatal(err)
	}

	r := e.start(0)
	want := map[string]string{"newer-ts": "eu", "tie": "us"}
	for sk, v := range want {
		for _, reg := range []string{us, eu} {
			e.eventually(fmt.Sprintf("%s=%s in %s", sk, v, reg), func() bool {
				return vOf(e.get(reg, e.events, key("k", sk))) == v
			})
		}
	}
	for _, reg := range []string{us, eu} {
		e.eventually("d converged to the update in "+reg, func() bool {
			return vOf(e.get(reg, e.events, key("d", "1"))) == "updated"
		})
	}
	quiesce(r)
	for sk, v := range want {
		for _, reg := range []string{us, eu} {
			if got := vOf(e.get(reg, e.events, key("k", sk))); got != v {
				t.Errorf("%s in %s = %q after quiescence, want %q", sk, reg, got, v)
			}
		}
	}
	if st := r.Stats(); st.SkippedConflict == 0 {
		t.Errorf("expected conflicts to be skipped: %+v", st)
	}
}

func TestIntegrationWaitsForTargetTableAndDelays(t *testing.T) {
	e := newEnv(t)
	e.createCatalogTable(us)
	e.createCatalogTable(eu)
	e.createRowTable(us, e.events) // eu's copy is created later
	const delay = 800 * time.Millisecond
	written := time.Now()
	e.put(us, e.events, row("late", "1", written.UnixNano(), us, "v"))

	e.start(delay)
	time.Sleep(300 * time.Millisecond)
	e.createRowTable(eu, e.events)
	e.eventually("row in late-created table", func() bool { return e.get(eu, e.events, key("late", "1")) != nil })
	if lag := time.Since(written); lag < delay {
		t.Fatalf("replicated after %v, before the %v delay", lag, delay)
	}
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

// checkNoDuplicateLogKeys fails if a JSON log line repeats a top-level key
// (e.g. "table" added both by the pipe and by the stream reader).
func checkNoDuplicateLogKeys(t *testing.T, logs string) {
	t.Helper()
	lines := 0
	for _, line := range strings.Split(logs, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		lines++
		dec := json.NewDecoder(strings.NewReader(line))
		if _, err := dec.Token(); err != nil { // {
			t.Fatalf("bad log line %q: %v", line, err)
		}
		seen := map[string]bool{}
		for dec.More() {
			tok, err := dec.Token()
			if err != nil {
				t.Fatalf("bad log line %q: %v", line, err)
			}
			k := tok.(string)
			if seen[k] {
				t.Errorf("log line repeats key %q: %s", k, line)
			}
			seen[k] = true
			var v json.RawMessage
			if err := dec.Decode(&v); err != nil {
				t.Fatalf("bad log line %q: %v", line, err)
			}
		}
	}
	if lines == 0 {
		t.Error("replicator logged nothing")
	}
}

// state builds a ReplacingMergeTree-style item: system attributes plus a
// numeric version (ver "" = no version attribute, "NULL" = NULL version).
func state(pk string, ts int64, origin, ver, v string) map[string]ddbtypes.AttributeValue {
	item := row(pk, "1", ts, origin, v)
	switch ver {
	case "":
	case "NULL":
		item["ver"] = &ddbtypes.AttributeValueMemberNULL{Value: true}
	default:
		item["ver"] = &ddbtypes.AttributeValueMemberN{Value: ver}
	}
	return item
}

// ReplacingMergeTree(ver) conflicts resolve by version in every region,
// whatever the write order (ClickHouse keeps the max version); write time
// only breaks version ties.
func TestIntegrationReplacingHighestVersionWins(t *testing.T) {
	e := newEnv(t)
	for _, reg := range []string{us, eu} {
		e.createCatalogTable(reg)
		e.createRowTable(reg, e.events)
		e.createRowTable(reg, e.states)
	}
	// Conflicting writes while "partitioned" (replicator not running yet).
	// a: eu writes v2 first, us writes v1 later -> v2 (eu).
	e.put(eu, e.states, state("a", 1000, eu, "2", "eu-v2"))
	e.put(us, e.states, state("a", 2000, us, "1", "us-v1"))
	// b: the mirror image -> v2 (us).
	e.put(us, e.states, state("b", 1000, us, "2", "us-v2"))
	e.put(eu, e.states, state("b", 2000, eu, "1", "eu-v1"))
	// c: same version -> the later write wins.
	e.put(us, e.states, state("c", 3000, us, "7", "us-v7"))
	e.put(eu, e.states, state("c", 4000, eu, "7", "eu-v7"))
	// d: a NULL version is the smallest.
	e.put(us, e.states, state("d", 9000, us, "NULL", "us-null"))
	e.put(eu, e.states, state("d", 1, eu, "0", "eu-v0"))
	// f: a missing version attribute counts as NULL.
	e.put(eu, e.states, state("f", 9000, eu, "", "eu-none"))
	e.put(us, e.states, state("f", 1, us, "3", "us-v3"))
	// g: both NULL -> later write wins.
	e.put(us, e.states, state("g", 10, us, "NULL", "us-null"))
	e.put(eu, e.states, state("g", 20, eu, "NULL", "eu-null"))
	// h: large versions compare numerically (DateTime64 versions are unix nanos).
	e.put(us, e.states, state("h", 5, us, "1700000000000000001", "us-big"))
	e.put(eu, e.states, state("h", 6, eu, "999999999999999999", "eu-small"))
	// The plain LWW table is unaffected by version attributes.
	e.put(eu, e.events, state("lww", 1000, eu, "2", "eu"))
	e.put(us, e.events, state("lww", 2000, us, "1", "us"))

	r := e.start(0)
	want := map[string]string{"a": "eu-v2", "b": "us-v2", "c": "eu-v7", "d": "eu-v0", "f": "us-v3", "g": "eu-null", "h": "us-big"}
	for pk, v := range want {
		for _, reg := range []string{us, eu} {
			e.eventually(fmt.Sprintf("%s=%s in %s", pk, v, reg), func() bool {
				return vOf(e.get(reg, e.states, key(pk, "1"))) == v
			})
		}
	}
	for _, reg := range []string{us, eu} {
		e.eventually("lww row in "+reg, func() bool { return vOf(e.get(reg, e.events, key("lww", "1"))) == "us" })
	}
	quiesce(r)
	for pk, v := range want {
		for _, reg := range []string{us, eu} {
			if got := vOf(e.get(reg, e.states, key(pk, "1"))); got != v {
				t.Errorf("%s in %s = %q after quiescence, want %q", pk, reg, got, v)
			}
		}
	}
	// A later lower-version write while replicating is superseded everywhere.
	e.put(us, e.states, state("a", time.Now().UnixNano(), us, "1", "us-late-v1"))
	e.put(eu, e.states, state("b", time.Now().UnixNano(), eu, "3", "eu-v3"))
	for _, reg := range []string{us, eu} {
		e.eventually("b=v3 in "+reg, func() bool { return vOf(e.get(reg, e.states, key("b", "1"))) == "eu-v3" })
	}
	quiesce(r)
	if got := vOf(e.get(eu, e.states, key("a", "1"))); got != "eu-v2" {
		t.Errorf("a in eu = %q, want eu-v2 (lower version replicated over a higher one)", got)
	}
	if st := r.Stats(); st.Errors != 0 || st.SkippedConflict == 0 {
		t.Errorf("stats %+v", st)
	}
}

// A replicated row version the target rejects (it holds a newer version)
// never enters the target's stream, so the replicator hands it to
// OnConflict (which applies the region's materialized views to it): every
// region's views then see the same row versions. A failing hook fails the
// batch, which is retried.
func TestIntegrationConflictHookGetsSupersededVersions(t *testing.T) {
	e := newEnv(t)
	for _, reg := range []string{us, eu} {
		e.createCatalogTable(reg)
		e.createRowTable(reg, e.events)
		e.createRowTable(reg, e.states)
	}
	// Every inserted row version has its own row id (as in the store).
	version := func(item map[string]ddbtypes.AttributeValue) map[string]ddbtypes.AttributeValue {
		item["_rid"] = &ddbtypes.AttributeValueMemberS{Value: "rid-" + vOf(item)}
		return item
	}
	// Partitioned conflict: eu holds v2, us holds v1 (written later).
	e.put(eu, e.states, version(state("a", 1000, eu, "2", "eu-v2")))
	e.put(us, e.states, version(state("a", 2000, us, "1", "us-v1")))
	// No conflict.
	e.put(us, e.states, version(state("b", 1000, us, "1", "us-only")))

	var mu sync.Mutex
	var calls []string
	failures := 2
	hook := func(_ context.Context, region, table, seq string, image map[string]ddbtypes.AttributeValue) error {
		mu.Lock()
		defer mu.Unlock()
		if failures > 0 {
			failures--
			return errors.New("views unavailable")
		}
		if table == e.prefix+"__catalog" {
			t.Errorf("catalog item handed to OnConflict")
		}
		if !strings.HasPrefix(seq, us+":") {
			t.Errorf("seq %q does not name the source region", seq)
		}
		calls = append(calls, region+"/"+vOf(image))
		return nil
	}
	r := e.startWith(0, hook)
	for _, reg := range []string{us, eu} {
		e.eventually("a=v2 in "+reg, func() bool { return vOf(e.get(reg, e.states, key("a", "1"))) == "eu-v2" })
		e.eventually("b in "+reg, func() bool { return vOf(e.get(reg, e.states, key("b", "1"))) == "us-only" })
	}
	e.eventually("hook called", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(calls) > 0
	})
	quiesce(r)
	mu.Lock()
	defer mu.Unlock()
	// us-v1 was rejected in eu: eu's views must see it. eu-v2 was accepted
	// in us (its stream feeds us's views), so it is not handed over, and
	// nor are versions re-put by the retried batches (b, or a's us-v1 put
	// before a failed hook call), which the target already holds.
	if len(calls) != 1 || calls[0] != eu+"/us-v1" {
		t.Fatalf("OnConflict calls = %v, want [%s/us-v1]", calls, eu)
	}
	if st := r.Stats(); st.ConflictViews != 1 || st.Errors < 2 {
		t.Fatalf("stats %+v", st)
	}
}
