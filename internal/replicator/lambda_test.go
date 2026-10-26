package replicator

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sttypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/aws/smithy-go"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/chtype"
)

// fakeDDB records the writes of one target region. failPut/failDelete return
// an error for the given _rid / key; conditional-check failures carry the
// item the target "holds".
type fakeDDB struct {
	mu      sync.Mutex
	puts    []*dynamodb.PutItemInput
	deletes []*dynamodb.DeleteItemInput
	putErr  map[string]error // by _rid of the written item
	delErr  map[string]error // by _pk (S) of the key
}

func (f *fakeDDB) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if rid, ok := in.Item["_rid"].(*ddbtypes.AttributeValueMemberS); ok {
		if err := f.putErr[rid.Value]; err != nil {
			return nil, err
		}
	}
	f.puts = append(f.puts, in)
	return &dynamodb.PutItemOutput{}, nil
}

func (f *fakeDDB) DeleteItem(_ context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if pk, ok := in.Key["_pk"].(*ddbtypes.AttributeValueMemberS); ok {
		if err := f.delErr[pk.Value]; err != nil {
			return nil, err
		}
	}
	f.deletes = append(f.deletes, in)
	return &dynamodb.DeleteItemOutput{}, nil
}

func (f *fakeDDB) rids() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.puts))
	for _, p := range f.puts {
		out = append(out, p.Item["_rid"].(*ddbtypes.AttributeValueMemberS).Value)
	}
	sort.Strings(out)
	return out
}

// conditionFailed is what DynamoDB returns when a condition fails, holding
// the existing item (ReturnValuesOnConditionCheckFailure ALL_OLD).
func conditionFailed(existing map[string]ddbtypes.AttributeValue) error {
	return &ddbtypes.ConditionalCheckFailedException{Message: aws.String("The conditional request failed"), Item: existing}
}

// genericErr is a retryable service error.
type genericErr struct{ smithy.GenericAPIError }

func serviceErr(code string) error {
	return &genericErr{smithy.GenericAPIError{Code: code, Message: code}}
}

func item(rid, origin, ts, ver string) map[string]ddbtypes.AttributeValue {
	m := map[string]ddbtypes.AttributeValue{
		"_pk":     &ddbtypes.AttributeValueMemberS{Value: "k-" + rid},
		"_rid":    &ddbtypes.AttributeValueMemberS{Value: rid},
		"_origin": &ddbtypes.AttributeValueMemberS{Value: origin},
		"_ts":     &ddbtypes.AttributeValueMemberN{Value: ts},
	}
	if ver != "" {
		m["ver"] = &ddbtypes.AttributeValueMemberN{Value: ver}
	}
	return m
}

func lambdaImage(rid, origin, ts, ver string) map[string]events.DynamoDBAttributeValue {
	m := map[string]events.DynamoDBAttributeValue{
		"_pk":     events.NewStringAttribute("k-" + rid),
		"_rid":    events.NewStringAttribute(rid),
		"_origin": events.NewStringAttribute(origin),
		"_ts":     events.NewNumberAttribute(ts),
	}
	if ver != "" {
		m["ver"] = events.NewNumberAttribute(ver)
	}
	return m
}

func lambdaKeys(rid string) map[string]events.DynamoDBAttributeValue {
	return map[string]events.DynamoDBAttributeValue{"_pk": events.NewStringAttribute("k-" + rid)}
}

func lambdaPut(table, seq, rid, origin, ts, ver string) events.DynamoDBEventRecord {
	return events.DynamoDBEventRecord{
		EventName:      EventInsert,
		EventSourceArn: "arn:aws:dynamodb:us-east-1:1:table/" + table + "/stream/2024",
		Change:         events.DynamoDBStreamRecord{SequenceNumber: seq, Keys: lambdaKeys(rid), NewImage: lambdaImage(rid, origin, ts, ver)},
	}
}

func lambdaRemove(table, seq, rid, origin, ts string) events.DynamoDBEventRecord {
	return events.DynamoDBEventRecord{
		EventName:      EventRemove,
		EventSourceArn: "arn:aws:dynamodb:us-east-1:1:table/" + table + "/stream/2024",
		Change:         events.DynamoDBStreamRecord{SequenceNumber: seq, Keys: lambdaKeys(rid), OldImage: lambdaImage(rid, origin, ts, "")},
	}
}

// conflicts records conflict hook calls.
type conflicts struct {
	mu    sync.Mutex
	calls []string // "region table seq rid"
	err   error
}

func (c *conflicts) hook(_ context.Context, region, table, seq string, image map[string]ddbtypes.AttributeValue) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.calls = append(c.calls, strings.Join([]string{region, table, seq, image["_rid"].(*ddbtypes.AttributeValueMemberS).Value}, " "))
	return nil
}

func newApplier(t *testing.T, target *fakeDDB, ver string, hook ConflictHook) *Applier {
	t.Helper()
	a, err := NewApplier(ApplierConfig{Source: "us", Target: "eu", Client: target,
		Table: TableSpec{Name: "p_db.state", VersionColumn: ver}, OnConflict: hook})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestNewApplierValidates(t *testing.T) {
	f := &fakeDDB{}
	for name, cfg := range map[string]ApplierConfig{
		"no source":   {Target: "eu", Client: f, Table: TableSpec{Name: "t"}},
		"no target":   {Source: "us", Client: f, Table: TableSpec{Name: "t"}},
		"same region": {Source: "us", Target: "us", Client: f, Table: TableSpec{Name: "t"}},
		"no client":   {Source: "us", Target: "eu", Table: TableSpec{Name: "t"}},
		"no table":    {Source: "us", Target: "eu", Client: f},
	} {
		if _, err := NewApplier(cfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := NewApplier(ApplierConfig{Source: "us", Target: "eu", Client: f, Table: TableSpec{Name: "t"}}); err != nil {
		t.Fatal(err)
	}
}

func TestApplierPut(t *testing.T) {
	ctx := context.Background()
	t.Run("own origin is written with the version-aware condition", func(t *testing.T) {
		f := &fakeDDB{}
		a := newApplier(t, f, "ver", nil)
		out, err := a.Apply(ctx, Record{EventName: EventModify, Seq: "1", NewImage: item("r1", "us", "100", "7")})
		if err != nil || out != Put {
			t.Fatalf("out=%v err=%v", out, err)
		}
		if len(f.puts) != 1 {
			t.Fatalf("puts = %d", len(f.puts))
		}
		p := f.puts[0]
		if aws.ToString(p.TableName) != "p_db.state" || !strings.Contains(aws.ToString(p.ConditionExpression), "#ver < :ver") ||
			p.ReturnValuesOnConditionCheckFailure != ddbtypes.ReturnValuesOnConditionCheckFailureAllOld {
			t.Fatalf("put = %+v", p)
		}
		if v := p.ExpressionAttributeValues[":ver"].(*ddbtypes.AttributeValueMemberN).Value; v != "7" {
			t.Fatalf(":ver = %s", v)
		}
	})
	t.Run("foreign origin is skipped", func(t *testing.T) {
		f := &fakeDDB{}
		a := newApplier(t, f, "", nil)
		out, err := a.Apply(ctx, Record{EventName: EventInsert, Seq: "1", NewImage: item("r1", "eu", "100", "")})
		if err != nil || out != SkippedOrigin || len(f.puts) != 0 {
			t.Fatalf("out=%v err=%v puts=%d", out, err, len(f.puts))
		}
	})
	t.Run("images without system attributes are invalid", func(t *testing.T) {
		f := &fakeDDB{}
		a := newApplier(t, f, "", nil)
		noOrigin := item("r1", "us", "100", "")
		delete(noOrigin, "_origin")
		noTS := item("r1", "us", "100", "")
		noTS["_ts"] = &ddbtypes.AttributeValueMemberS{Value: "100"}
		for _, img := range []map[string]ddbtypes.AttributeValue{nil, noOrigin, noTS} {
			if out, err := a.Apply(ctx, Record{EventName: EventInsert, NewImage: img}); err != nil || out != Invalid {
				t.Fatalf("out=%v err=%v", out, err)
			}
		}
		if out, err := a.Apply(ctx, Record{EventName: "WEIRD"}); err != nil || out != Invalid {
			t.Fatalf("unknown event: out=%v err=%v", out, err)
		}
		if len(f.puts) != 0 {
			t.Fatal("invalid record written")
		}
	})
	t.Run("older version loses without error and feeds the views", func(t *testing.T) {
		c := &conflicts{}
		f := &fakeDDB{putErr: map[string]error{"old": conditionFailed(item("newer", "eu", "200", "9"))}}
		a := newApplier(t, f, "ver", c.hook)
		out, err := a.Apply(ctx, Record{EventName: EventInsert, Seq: "42", NewImage: item("old", "us", "100", "7")})
		if err != nil || out != SupersededViewsApplied {
			t.Fatalf("out=%v err=%v", out, err)
		}
		if want := []string{"eu p_db.state us:42 old"}; !reflect.DeepEqual(c.calls, want) {
			t.Fatalf("hook calls = %v", c.calls)
		}
	})
	t.Run("redelivery of the same row version skips the hook", func(t *testing.T) {
		c := &conflicts{}
		f := &fakeDDB{putErr: map[string]error{"r1": conditionFailed(item("r1", "us", "100", "7"))}}
		a := newApplier(t, f, "ver", c.hook)
		out, err := a.Apply(ctx, Record{EventName: EventInsert, Seq: "1", NewImage: item("r1", "us", "100", "7")})
		if err != nil || out != Superseded || len(c.calls) != 0 {
			t.Fatalf("out=%v err=%v calls=%v", out, err, c.calls)
		}
	})
	t.Run("condition failure without a hook", func(t *testing.T) {
		f := &fakeDDB{putErr: map[string]error{"r1": conditionFailed(nil)}}
		a := newApplier(t, f, "", nil)
		if out, err := a.Apply(ctx, Record{EventName: EventInsert, NewImage: item("r1", "us", "100", "")}); err != nil || out != Superseded {
			t.Fatalf("out=%v err=%v", out, err)
		}
	})
	t.Run("hook failure is an error", func(t *testing.T) {
		c := &conflicts{err: errors.New("views down")}
		f := &fakeDDB{putErr: map[string]error{"r1": conditionFailed(item("r2", "eu", "200", ""))}}
		a := newApplier(t, f, "", c.hook)
		if _, err := a.Apply(ctx, Record{EventName: EventInsert, NewImage: item("r1", "us", "100", "")}); err == nil || !strings.Contains(err.Error(), "views down") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("service error is returned", func(t *testing.T) {
		f := &fakeDDB{putErr: map[string]error{"r1": serviceErr("ProvisionedThroughputExceededException")}}
		a := newApplier(t, f, "", nil)
		if _, err := a.Apply(ctx, Record{EventName: EventInsert, NewImage: item("r1", "us", "100", "")}); err == nil || !strings.Contains(err.Error(), "put into eu/p_db.state") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestApplierDelete(t *testing.T) {
	ctx := context.Background()
	key := map[string]ddbtypes.AttributeValue{"_pk": &ddbtypes.AttributeValueMemberS{Value: "k"}}
	t.Run("delete is conditioned on the old write time", func(t *testing.T) {
		f := &fakeDDB{}
		a := newApplier(t, f, "ver", nil)
		out, err := a.Apply(ctx, Record{EventName: EventRemove, Keys: key, OldImage: item("r1", "eu", "150", "")})
		if err != nil || out != Deleted || len(f.deletes) != 1 {
			t.Fatalf("out=%v err=%v deletes=%d", out, err, len(f.deletes))
		}
		d := f.deletes[0]
		if aws.ToString(d.ConditionExpression) != "#ts <= :ts" || d.ExpressionAttributeValues[":ts"].(*ddbtypes.AttributeValueMemberN).Value != "150" ||
			!reflect.DeepEqual(d.Key, key) {
			t.Fatalf("delete = %+v", d)
		}
	})
	t.Run("already gone or newer is not an error", func(t *testing.T) {
		f := &fakeDDB{delErr: map[string]error{"k": conditionFailed(nil)}}
		a := newApplier(t, f, "", nil)
		if out, err := a.Apply(ctx, Record{EventName: EventRemove, Keys: key, OldImage: item("r1", "us", "150", "")}); err != nil || out != Superseded {
			t.Fatalf("out=%v err=%v", out, err)
		}
	})
	t.Run("without keys or old _ts is invalid", func(t *testing.T) {
		f := &fakeDDB{}
		a := newApplier(t, f, "", nil)
		if out, err := a.Apply(ctx, Record{EventName: EventRemove, OldImage: item("r1", "us", "150", "")}); err != nil || out != Invalid {
			t.Fatalf("no keys: out=%v err=%v", out, err)
		}
		if out, err := a.Apply(ctx, Record{EventName: EventRemove, Keys: key}); err != nil || out != Invalid {
			t.Fatalf("no old image: out=%v err=%v", out, err)
		}
	})
	t.Run("service error is returned", func(t *testing.T) {
		f := &fakeDDB{delErr: map[string]error{"k": serviceErr("InternalServerError")}}
		a := newApplier(t, f, "", nil)
		if _, err := a.Apply(ctx, Record{EventName: EventRemove, Keys: key, OldImage: item("r1", "us", "150", "")}); err == nil || !strings.Contains(err.Error(), "delete into eu/p_db.state") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestCountersRecord(t *testing.T) {
	var c counters
	for _, o := range []Outcome{Put, Put, Deleted, SkippedOrigin, Superseded, SupersededViewsApplied, Invalid} {
		c.record(o, nil)
	}
	c.record(Put, errors.New("x"))
	want := Stats{Put: 2, Deleted: 1, SkippedOrigin: 1, SkippedConflict: 2, SkippedInvalid: 1, ConflictViews: 1, Errors: 1}
	if got := c.stats(); got != want {
		t.Fatalf("stats = %+v, want %+v", got, want)
	}
	for _, o := range []Outcome{Invalid, SkippedOrigin, Put, Deleted, Superseded, SupersededViewsApplied, Outcome(99)} {
		if o.String() == "" {
			t.Fatal("unnamed outcome")
		}
	}
}

func TestRecordConversion(t *testing.T) {
	t.Run("stream", func(t *testing.T) {
		rec, err := RecordFromStream(sttypes.Record{EventName: sttypes.OperationTypeRemove, Dynamodb: &sttypes.StreamRecord{
			SequenceNumber: aws.String("7"),
			Keys:           map[string]sttypes.AttributeValue{"_pk": &sttypes.AttributeValueMemberS{Value: "k"}},
			OldImage:       map[string]sttypes.AttributeValue{"_ts": &sttypes.AttributeValueMemberN{Value: "1"}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		want := Record{EventName: EventRemove, Seq: "7",
			Keys:     map[string]ddbtypes.AttributeValue{"_pk": &ddbtypes.AttributeValueMemberS{Value: "k"}},
			OldImage: map[string]ddbtypes.AttributeValue{"_ts": &ddbtypes.AttributeValueMemberN{Value: "1"}}}
		if !reflect.DeepEqual(rec, want) {
			t.Fatalf("got %+v", rec)
		}
		if rec, err := RecordFromStream(sttypes.Record{EventName: sttypes.OperationTypeInsert}); err != nil || rec.EventName != EventInsert || rec.Keys != nil {
			t.Fatalf("record without Dynamodb: %+v %v", rec, err)
		}
		if _, err := RecordFromStream(sttypes.Record{Dynamodb: &sttypes.StreamRecord{NewImage: map[string]sttypes.AttributeValue{"x": nil}}}); err == nil {
			t.Fatal("nil attribute accepted")
		}
	})
	t.Run("lambda", func(t *testing.T) {
		rec, err := RecordFromLambda(lambdaRemove("t", "9", "r1", "us", "5"))
		if err != nil || rec.EventName != EventRemove || rec.Seq != "9" || rec.NewImage != nil {
			t.Fatalf("rec=%+v err=%v", rec, err)
		}
		if !reflect.DeepEqual(rec.Keys, map[string]ddbtypes.AttributeValue{"_pk": &ddbtypes.AttributeValueMemberS{Value: "k-r1"}}) ||
			!reflect.DeepEqual(rec.OldImage, item("r1", "us", "5", "")) {
			t.Fatalf("rec = %+v", rec)
		}
	})
}

// --- Lambda entry point ---

type lambdaEnv struct {
	eu, ap  *fakeDDB
	hook    *conflicts
	lookups map[string]TableSpec
	err     error // lookup error
	l       *Lambda
}

func newLambdaEnv(t *testing.T, parallelism int) *lambdaEnv {
	t.Helper()
	e := &lambdaEnv{eu: &fakeDDB{}, ap: &fakeDDB{}, hook: &conflicts{},
		lookups: map[string]TableSpec{"p_db.state": {Name: "p_db.state", VersionColumn: "ver"}, "p_db.plain": {Name: "p_db.plain"}}}
	l, err := NewLambda(LambdaConfig{
		Region:  "us",
		Targets: []Target{{Name: "eu", Client: e.eu}, {Name: "ap", Client: e.ap}},
		Tables: func(_ context.Context, physical string) (TableSpec, bool, error) {
			if e.err != nil {
				return TableSpec{}, false, e.err
			}
			s, ok := e.lookups[physical]
			return s, ok, nil
		},
		OnConflict:  e.hook.hook,
		Parallelism: parallelism,
	})
	if err != nil {
		t.Fatal(err)
	}
	e.l = l
	return e
}

func TestNewLambdaValidates(t *testing.T) {
	c := &fakeDDB{}
	lookup := func(context.Context, string) (TableSpec, bool, error) { return TableSpec{}, false, nil }
	for name, cfg := range map[string]LambdaConfig{
		"no region":        {Targets: []Target{{"eu", c}}, Tables: lookup},
		"no targets":       {Region: "us", Tables: lookup},
		"self target":      {Region: "us", Targets: []Target{{"us", c}}, Tables: lookup},
		"duplicate target": {Region: "us", Targets: []Target{{"eu", c}, {"eu", c}}, Tables: lookup},
		"no clients":       {Region: "us", Targets: []Target{{"eu", nil}}, Tables: lookup},
		"no lookup":        {Region: "us", Targets: []Target{{"eu", c}}},
	} {
		if _, err := NewLambda(cfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	l, err := NewLambda(LambdaConfig{Region: "us", Targets: []Target{{"eu", c}}, Tables: lookup})
	if err != nil || l.cfg.Parallelism != 8 {
		t.Fatalf("l=%+v err=%v", l, err)
	}
}

func TestLambdaReplicatesOwnRowsToEveryTarget(t *testing.T) {
	e := newLambdaEnv(t, 0)
	resp, err := e.l.Handle(context.Background(), events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
		lambdaPut("p_db.state", "1", "a", "us", "100", "1"),
		lambdaPut("p_db.state", "2", "b", "eu", "100", "1"), // replicated in: skipped
		lambdaPut("p_db.state", "3", "c", "us", "101", "2"),
		lambdaRemove("p_db.state", "4", "d", "ap", "90"),
		lambdaPut("p_db.plain", "5", "e", "us", "102", ""),
	}})
	if err != nil || len(resp.BatchItemFailures) != 0 {
		t.Fatalf("resp=%+v err=%v", resp, err)
	}
	for _, f := range []*fakeDDB{e.eu, e.ap} {
		if got := f.rids(); !reflect.DeepEqual(got, []string{"a", "c", "e"}) {
			t.Fatalf("puts = %v", got)
		}
		if len(f.deletes) != 1 || f.deletes[0].Key["_pk"].(*ddbtypes.AttributeValueMemberS).Value != "k-d" {
			t.Fatalf("deletes = %+v", f.deletes)
		}
		for _, p := range f.puts {
			ver := aws.ToString(p.TableName) == "p_db.state"
			if strings.Contains(aws.ToString(p.ConditionExpression), "#ver") != ver {
				t.Fatalf("condition of %s: %s", aws.ToString(p.TableName), aws.ToString(p.ConditionExpression))
			}
		}
	}
	want := Stats{Put: 6, Deleted: 2, SkippedOrigin: 2}
	if got := e.l.Stats(); got != want {
		t.Fatalf("stats = %+v, want %+v", got, want)
	}
}

func TestLambdaConflictFeedsTargetViews(t *testing.T) {
	e := newLambdaEnv(t, 0)
	// eu already holds a newer version of a; ap takes it.
	e.eu.putErr = map[string]error{"a": conditionFailed(item("a2", "eu", "200", "5"))}
	resp, err := e.l.Handle(context.Background(), events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
		lambdaPut("p_db.state", "11", "a", "us", "100", "1"),
	}})
	if err != nil || len(resp.BatchItemFailures) != 0 {
		t.Fatalf("resp=%+v err=%v", resp, err)
	}
	if want := []string{"eu p_db.state us:11 a"}; !reflect.DeepEqual(e.hook.calls, want) {
		t.Fatalf("hook calls = %v", e.hook.calls)
	}
	if got := e.ap.rids(); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("ap puts = %v", got)
	}
	want := Stats{Put: 1, SkippedConflict: 1, ConflictViews: 1}
	if got := e.l.Stats(); got != want {
		t.Fatalf("stats = %+v, want %+v", got, want)
	}
}

func TestLambdaReportsFirstFailedRecord(t *testing.T) {
	e := newLambdaEnv(t, 1) // one lane: strictly in order
	e.ap.putErr = map[string]error{"c": serviceErr("InternalServerError")}
	resp, err := e.l.Handle(context.Background(), events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
		lambdaPut("p_db.state", "1", "a", "us", "100", "1"),
		lambdaPut("p_db.state", "2", "b", "us", "100", "1"),
		lambdaPut("p_db.state", "3", "c", "us", "101", "2"),
		lambdaPut("p_db.state", "4", "d", "us", "102", "3"),
		lambdaPut("p_db.plain", "5", "e", "us", "102", ""),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if want := []events.DynamoDBBatchItemFailure{{ItemIdentifier: "3"}}; !reflect.DeepEqual(resp.BatchItemFailures, want) {
		t.Fatalf("failures = %+v", resp.BatchItemFailures)
	}
	// Records before the failure reached every target; eu even got c
	// (re-applying it on retry is a no-op); nothing after it was applied.
	if got := e.eu.rids(); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("eu puts = %v", got)
	}
	if got := e.ap.rids(); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("ap puts = %v", got)
	}
	if s := e.l.Stats(); s.Errors != 1 || s.Put != 5 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestLambdaParallelFailureReportsEarliest(t *testing.T) {
	e := newLambdaEnv(t, 8)
	e.eu.putErr = map[string]error{"b": serviceErr("InternalServerError"), "d": serviceErr("InternalServerError")}
	var recs []events.DynamoDBEventRecord
	for i, rid := range []string{"a", "b", "c", "d", "e", "f"} {
		recs = append(recs, lambdaPut("p_db.state", string(rune('1'+i)), rid, "us", "100", "1"))
	}
	resp, err := e.l.Handle(context.Background(), events.DynamoDBEvent{Records: recs})
	if err != nil {
		t.Fatal(err)
	}
	if want := []events.DynamoDBBatchItemFailure{{ItemIdentifier: "2"}}; !reflect.DeepEqual(resp.BatchItemFailures, want) {
		t.Fatalf("failures = %+v", resp.BatchItemFailures)
	}
	if got := e.eu.rids(); got[0] != "a" {
		t.Fatalf("eu puts = %v", got)
	}
}

func TestLambdaSkipsTablesItDoesNotCopy(t *testing.T) {
	e := newLambdaEnv(t, 0)
	resp, err := e.l.Handle(context.Background(), events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
		lambdaPut("p_db.dropped", "1", "a", "us", "100", ""),
		lambdaPut("p_db.dropped", "2", "b", "us", "100", ""),
		lambdaPut("p_db.plain", "3", "c", "us", "100", ""),
	}})
	if err != nil || len(resp.BatchItemFailures) != 0 {
		t.Fatalf("resp=%+v err=%v", resp, err)
	}
	if got := e.eu.rids(); !reflect.DeepEqual(got, []string{"c"}) {
		t.Fatalf("eu puts = %v", got)
	}
	if s := e.l.Stats(); s.SkippedInvalid != 2 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestLambdaFailsBatchOnBadInput(t *testing.T) {
	ctx := context.Background()
	t.Run("unparseable source ARN", func(t *testing.T) {
		e := newLambdaEnv(t, 0)
		r := lambdaPut("p_db.state", "1", "a", "us", "100", "1")
		r.EventSourceArn = "arn:aws:dynamodb:us-east-1:1:what"
		resp, err := e.l.Handle(ctx, events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{r}})
		if err != nil || !reflect.DeepEqual(resp.BatchItemFailures, []events.DynamoDBBatchItemFailure{{ItemIdentifier: "1"}}) {
			t.Fatalf("resp=%+v err=%v", resp, err)
		}
	})
	t.Run("catalog unavailable", func(t *testing.T) {
		e := newLambdaEnv(t, 0)
		e.err = errors.New("catalog down")
		resp, err := e.l.Handle(ctx, events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
			lambdaPut("p_db.state", "1", "a", "us", "100", "1"),
			lambdaPut("p_db.state", "2", "b", "us", "100", "1"),
		}})
		if err != nil || !reflect.DeepEqual(resp.BatchItemFailures, []events.DynamoDBBatchItemFailure{{ItemIdentifier: "1"}}) {
			t.Fatalf("resp=%+v err=%v", resp, err)
		}
		if len(e.eu.puts) != 0 {
			t.Fatal("records applied without a table spec")
		}
	})
	t.Run("empty event", func(t *testing.T) {
		e := newLambdaEnv(t, 0)
		if resp, err := e.l.Handle(ctx, events.DynamoDBEvent{}); err != nil || len(resp.BatchItemFailures) != 0 {
			t.Fatalf("resp=%+v err=%v", resp, err)
		}
	})
}

func TestCatalogLookup(t *testing.T) {
	ctx := context.Background()
	cat := catalog.NewMemory()
	if err := cat.CreateDatabase(ctx, &catalog.Database{Name: "db"}, true); err != nil {
		t.Fatal(err)
	}
	mk := func(name, engine, ver, repl string) {
		t.Helper()
		col := func(n string) catalog.Column { return catalog.Column{Name: n, Type: chtype.MustParse("UInt64")} }
		tb := &catalog.Table{Database: "db", Name: name, Engine: engine, Columns: []catalog.Column{col("k"), col("v")},
			OrderBy: []string{"k"}, PartitionKey: []string{"k"}, VersionColumn: ver, Replication: repl}
		if err := cat.CreateTable(ctx, tb, false); err != nil {
			t.Fatal(err)
		}
	}
	mk("state", catalog.EngineReplacingMergeTree, "v", catalog.ReplicationGlobal)
	mk("plain", catalog.EngineReplacingMergeTree, "", catalog.ReplicationGlobal)
	mk("events", catalog.EngineMergeTree, "", catalog.ReplicationGlobal)
	mk("local", catalog.EngineReplacingMergeTree, "v", catalog.ReplicationRegional)

	lookup := CatalogLookup(cat, time.Minute, nil)
	cases := []struct {
		physical string
		want     TableSpec
		ok       bool
	}{
		{"db.state", TableSpec{Name: "db.state", VersionColumn: "v"}, true},
		{"db.plain", TableSpec{Name: "db.plain"}, true},
		{"db.events", TableSpec{}, false}, // an AWS global table
		{"db.local", TableSpec{}, false},  // regional
		{"db.gone", TableSpec{}, false},   // dropped
	}
	for _, c := range cases {
		got, ok, err := lookup(ctx, c.physical)
		if err != nil || ok != c.ok || got != c.want {
			t.Errorf("lookup(%s) = %+v, %v, %v; want %+v, %v", c.physical, got, ok, err, c.want, c.ok)
		}
	}
	// A table created after the snapshot is found by the retry refresh.
	mk("late", catalog.EngineReplacingMergeTree, "v", catalog.ReplicationGlobal)
	if got, ok, err := lookup(ctx, "db.late"); err != nil || !ok || got.VersionColumn != "v" {
		t.Fatalf("late table: %+v %v %v", got, ok, err)
	}
}
