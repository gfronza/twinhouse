package esm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

const (
	streamA   = "arn:aws:dynamodb:eu-west-1:123456789012:table/chddb_default.events/stream/2026-01-01T00:00:00.000"
	streamB   = "arn:aws:dynamodb:eu-west-1:123456789012:table/chddb_default.other/stream/2026-01-01T00:00:00.000"
	mvworker  = "arn:aws:lambda:eu-west-1:123456789012:function:chddb-mvworker"
	replicatr = "arn:aws:lambda:eu-west-1:123456789012:function:chddb-replicator"
)

// fakeLambda is an in-memory Lambda event-source-mapping API.
type fakeLambda struct {
	mu       sync.Mutex
	mappings []types.EventSourceMappingConfiguration
	next     int
	calls    map[string]int
	// pageSize splits List responses (0: all at once).
	pageSize int
	// fail returns the given error for the next call of op (consumed).
	fail map[string][]error
	// createState is the State of a new mapping (default "Creating").
	createState string
}

func newFake() *fakeLambda { return &fakeLambda{calls: map[string]int{}, fail: map[string][]error{}} }

func (f *fakeLambda) failNext(op string, errs ...error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail[op] = append(f.fail[op], errs...)
}

func (f *fakeLambda) take(op string) error {
	f.calls[op]++
	if q := f.fail[op]; len(q) > 0 {
		f.fail[op] = q[1:]
		return q[0]
	}
	return nil
}

func (f *fakeLambda) add(stream, fn, state string, batch int32) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.addLocked(stream, fn, state, batch)
}

func (f *fakeLambda) addLocked(stream, fn, state string, batch int32) string {
	f.next++
	uuid := "uuid-" + strconv.Itoa(f.next)
	f.mappings = append(f.mappings, types.EventSourceMappingConfiguration{
		UUID: aws.String(uuid), EventSourceArn: aws.String(stream), FunctionArn: aws.String(fn), State: aws.String(state),
		BatchSize: aws.Int32(batch), BisectBatchOnFunctionError: aws.Bool(false), MaximumRetryAttempts: aws.Int32(-1),
		FunctionResponseTypes: []types.FunctionResponseType{types.FunctionResponseTypeReportBatchItemFailures},
		StartingPosition:      types.EventSourcePositionTrimHorizon,
	})
	return uuid
}

func (f *fakeLambda) ListEventSourceMappings(_ context.Context, in *lambda.ListEventSourceMappingsInput, _ ...func(*lambda.Options)) (*lambda.ListEventSourceMappingsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.take("List"); err != nil {
		return nil, err
	}
	var all []types.EventSourceMappingConfiguration
	for _, m := range f.mappings {
		if in.EventSourceArn != nil && *in.EventSourceArn != *m.EventSourceArn {
			continue
		}
		if in.FunctionName != nil && *in.FunctionName != *m.FunctionArn {
			continue
		}
		all = append(all, m)
	}
	start := 0
	if in.Marker != nil {
		start, _ = strconv.Atoi(*in.Marker)
	}
	end := len(all)
	if f.pageSize > 0 && start+f.pageSize < end {
		end = start + f.pageSize
	}
	out := &lambda.ListEventSourceMappingsOutput{EventSourceMappings: slices.Clone(all[start:end])}
	if end < len(all) {
		out.NextMarker = aws.String(strconv.Itoa(end))
	}
	return out, nil
}

func (f *fakeLambda) CreateEventSourceMapping(_ context.Context, in *lambda.CreateEventSourceMappingInput, _ ...func(*lambda.Options)) (*lambda.CreateEventSourceMappingOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.take("Create"); err != nil {
		return nil, err
	}
	for _, m := range f.mappings {
		if *m.EventSourceArn == *in.EventSourceArn && *m.FunctionArn == *in.FunctionName {
			return nil, &types.ResourceConflictException{Message: aws.String("The event source arn and function provided mapping already exists. Please update or delete the existing mapping with UUID " + *m.UUID)}
		}
	}
	state := f.createState
	if state == "" {
		state = "Creating"
	}
	uuid := f.addLocked(*in.EventSourceArn, *in.FunctionName, state, aws.ToInt32(in.BatchSize))
	m := &f.mappings[len(f.mappings)-1]
	m.StartingPosition = in.StartingPosition
	m.MaximumRetryAttempts = in.MaximumRetryAttempts
	m.BisectBatchOnFunctionError = in.BisectBatchOnFunctionError
	m.FunctionResponseTypes = in.FunctionResponseTypes
	return &lambda.CreateEventSourceMappingOutput{UUID: aws.String(uuid)}, nil
}

func (f *fakeLambda) UpdateEventSourceMapping(_ context.Context, in *lambda.UpdateEventSourceMappingInput, _ ...func(*lambda.Options)) (*lambda.UpdateEventSourceMappingOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.take("Update"); err != nil {
		return nil, err
	}
	for i := range f.mappings {
		m := &f.mappings[i]
		if *m.UUID != *in.UUID {
			continue
		}
		if in.BatchSize != nil {
			m.BatchSize = in.BatchSize
		}
		if in.Enabled != nil && *in.Enabled {
			m.State = aws.String("Enabled")
		}
		if in.MaximumRetryAttempts != nil {
			m.MaximumRetryAttempts = in.MaximumRetryAttempts
		}
		if in.BisectBatchOnFunctionError != nil {
			m.BisectBatchOnFunctionError = in.BisectBatchOnFunctionError
		}
		if in.FunctionResponseTypes != nil {
			m.FunctionResponseTypes = in.FunctionResponseTypes
		}
		return &lambda.UpdateEventSourceMappingOutput{UUID: m.UUID}, nil
	}
	return nil, &types.ResourceNotFoundException{Message: aws.String("not found")}
}

func (f *fakeLambda) DeleteEventSourceMapping(_ context.Context, in *lambda.DeleteEventSourceMappingInput, _ ...func(*lambda.Options)) (*lambda.DeleteEventSourceMappingOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.take("Delete"); err != nil {
		return nil, err
	}
	for i, m := range f.mappings {
		if *m.UUID == *in.UUID {
			f.mappings = slices.Delete(f.mappings, i, i+1)
			return &lambda.DeleteEventSourceMappingOutput{UUID: in.UUID}, nil
		}
	}
	return nil, &types.ResourceNotFoundException{Message: aws.String("not found")}
}

func (f *fakeLambda) snapshot() []types.EventSourceMappingConfiguration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.mappings)
}

func (f *fakeLambda) count(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[op]
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newManager(t *testing.T, f *fakeLambda) *Manager {
	t.Helper()
	m, err := NewManager(ManagerConfig{API: f, Logger: quiet(), RetryBase: time.Millisecond, RetryMax: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestEnsureCreatesOnce(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	m := newManager(t, f)
	if err := m.Ensure(ctx, streamA, mvworker, Options{}); err != nil {
		t.Fatal(err)
	}
	ms := f.snapshot()
	if len(ms) != 1 {
		t.Fatalf("mappings %v", ms)
	}
	got := ms[0]
	if aws.ToInt32(got.BatchSize) != DefaultBatchSize || got.StartingPosition != types.EventSourcePositionTrimHorizon ||
		aws.ToInt32(got.MaximumRetryAttempts) != -1 || aws.ToBool(got.BisectBatchOnFunctionError) ||
		!slices.Contains(got.FunctionResponseTypes, types.FunctionResponseTypeReportBatchItemFailures) {
		t.Errorf("created with %+v", got)
	}
	// Idempotent: the second Ensure only lists (the mapping is still Creating).
	if err := m.Ensure(ctx, streamA, mvworker, Options{}); err != nil {
		t.Fatal(err)
	}
	if f.count("Create") != 1 || f.count("Update") != 0 || f.count("List") != 2 {
		t.Errorf("calls: create %d update %d list %d", f.count("Create"), f.count("Update"), f.count("List"))
	}
	// Custom options.
	if err := m.Ensure(ctx, streamB, mvworker, Options{BatchSize: 25, StartingPosition: types.EventSourcePositionLatest, MaximumRetryAttempts: aws.Int32(3)}); err != nil {
		t.Fatal(err)
	}
	for _, g := range f.snapshot() {
		if *g.EventSourceArn == streamB && (aws.ToInt32(g.BatchSize) != 25 || g.StartingPosition != types.EventSourcePositionLatest || aws.ToInt32(g.MaximumRetryAttempts) != 3) {
			t.Errorf("options not applied: %+v", g)
		}
	}
	if err := m.Ensure(ctx, "", mvworker, Options{}); err == nil {
		t.Error("empty stream ARN accepted")
	}
}

func TestEnsureEnablesAndUpdates(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	m := newManager(t, f)

	// Disabled mapping -> enabled, settings kept.
	f.add(streamA, mvworker, "Disabled", DefaultBatchSize)
	if err := m.Ensure(ctx, streamA, mvworker, Options{}); err != nil {
		t.Fatal(err)
	}
	if ms := f.snapshot(); len(ms) != 1 || *ms[0].State != "Enabled" {
		t.Fatalf("not enabled: %+v", ms)
	}
	// Enabled mapping with another batch size -> updated.
	if err := m.Ensure(ctx, streamA, mvworker, Options{BatchSize: 500}); err != nil {
		t.Fatal(err)
	}
	if ms := f.snapshot(); aws.ToInt32(ms[0].BatchSize) != 500 {
		t.Fatalf("batch size not updated: %+v", ms)
	}
	// Matching mapping -> no update.
	before := f.count("Update")
	if err := m.Ensure(ctx, streamA, mvworker, Options{BatchSize: 500}); err != nil {
		t.Fatal(err)
	}
	if f.count("Update") != before || f.count("Create") != 0 {
		t.Errorf("unneeded calls: update %d create %d", f.count("Update")-before, f.count("Create"))
	}
	// Drifted settings (bisect on, bounded retries, no report) -> updated.
	f.mu.Lock()
	f.mappings[0].BisectBatchOnFunctionError = aws.Bool(true)
	f.mappings[0].MaximumRetryAttempts = aws.Int32(10)
	f.mappings[0].FunctionResponseTypes = nil
	f.mu.Unlock()
	if err := m.Ensure(ctx, streamA, mvworker, Options{BatchSize: 500}); err != nil {
		t.Fatal(err)
	}
	ms := f.snapshot()
	if aws.ToBool(ms[0].BisectBatchOnFunctionError) || aws.ToInt32(ms[0].MaximumRetryAttempts) != -1 || len(ms[0].FunctionResponseTypes) != 1 {
		t.Errorf("drift not fixed: %+v", ms[0])
	}
	// Lambda reports MaximumRetryAttempts nil for mappings created without it: same as -1.
	f.mu.Lock()
	f.mappings[0].MaximumRetryAttempts = nil
	f.mu.Unlock()
	before = f.count("Update")
	if err := m.Ensure(ctx, streamA, mvworker, Options{BatchSize: 500}); err != nil || f.count("Update") != before {
		t.Errorf("nil retries treated as drift: err %v updates %d", err, f.count("Update")-before)
	}
}

func TestEnsureTransitions(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	m := newManager(t, f)
	for _, state := range []string{"Disabling", "Updating", "Deleting"} {
		f.mu.Lock()
		f.mappings = nil
		f.mu.Unlock()
		f.add(streamA, mvworker, state, DefaultBatchSize)
		err := m.Ensure(ctx, streamA, mvworker, Options{})
		if !errors.Is(err, ErrInTransition) {
			t.Errorf("%s: err %v, want ErrInTransition", state, err)
		}
	}
	// Creating/Enabling with matching settings is fine; with differing settings, pending.
	for _, state := range []string{"Creating", "Enabling"} {
		f.mu.Lock()
		f.mappings = nil
		f.mu.Unlock()
		f.add(streamA, mvworker, state, DefaultBatchSize)
		if err := m.Ensure(ctx, streamA, mvworker, Options{}); err != nil {
			t.Errorf("%s matching: %v", state, err)
		}
		if err := m.Ensure(ctx, streamA, mvworker, Options{BatchSize: 7}); !errors.Is(err, ErrInTransition) {
			t.Errorf("%s differing: %v", state, err)
		}
	}
	if f.count("Update") != 0 {
		t.Errorf("updated a mapping in transition")
	}
	// An update racing a transition (ResourceInUseException) is pending too.
	f.mu.Lock()
	f.mappings = nil
	f.mu.Unlock()
	f.add(streamA, mvworker, "Disabled", DefaultBatchSize)
	f.failNext("Update", &types.ResourceInUseException{Message: aws.String("in use")})
	if err := m.Ensure(ctx, streamA, mvworker, Options{}); !errors.Is(err, ErrInTransition) {
		t.Errorf("in use: %v", err)
	}
	// A mapping deleted between List and Update is reported (not pending).
	f.failNext("Update", &types.ResourceNotFoundException{Message: aws.String("gone")})
	if err := m.Ensure(ctx, streamA, mvworker, Options{}); err == nil || errors.Is(err, ErrInTransition) {
		t.Errorf("not found on update: %v", err)
	}
}

func TestEnsureConcurrentCreate(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	m := newManager(t, f)
	// The List says nothing exists, then the Create conflicts because
	// another reconciler created the mapping meanwhile.
	f.failNext("Create", &types.ResourceConflictException{Message: aws.String("already exists")})
	f.add(streamA, mvworker, "Enabled", DefaultBatchSize)
	f.mu.Lock()
	f.fail["List"] = nil
	f.mu.Unlock()
	// Make the first List miss it: temporarily hide the mapping.
	hidden := f.snapshot()
	f.mu.Lock()
	f.mappings = nil
	f.mu.Unlock()
	listed := 0
	f.mu.Lock()
	f.fail["List"] = nil
	f.mu.Unlock()
	// Wrap: after the first List, restore the mapping.
	wrapped := &listHook{fakeLambda: f, onList: func() {
		listed++
		if listed == 1 {
			f.mu.Lock()
			f.mappings = hidden
			f.mu.Unlock()
		}
	}}
	m2, _ := NewManager(ManagerConfig{API: wrapped, Logger: quiet(), RetryBase: time.Millisecond})
	if err := m2.Ensure(ctx, streamA, mvworker, Options{}); err != nil {
		t.Fatalf("conflict not tolerated: %v", err)
	}
	if ms := f.snapshot(); len(ms) != 1 {
		t.Fatalf("mappings %v", ms)
	}
	// A conflict with nothing to find afterwards is an error.
	f.mu.Lock()
	f.mappings = nil
	f.mu.Unlock()
	f.failNext("Create", &types.ResourceConflictException{Message: aws.String("already exists")})
	if err := m.Ensure(ctx, streamA, mvworker, Options{}); err == nil {
		t.Error("phantom conflict accepted")
	}
	// Other create errors propagate.
	f.failNext("Create", &types.InvalidParameterValueException{Message: aws.String("bad")})
	if err := m.Ensure(ctx, streamA, mvworker, Options{}); err == nil {
		t.Error("create error swallowed")
	}
}

// listHook runs onList before every List call.
type listHook struct {
	*fakeLambda
	onList func()
}

func (h *listHook) ListEventSourceMappings(ctx context.Context, in *lambda.ListEventSourceMappingsInput, o ...func(*lambda.Options)) (*lambda.ListEventSourceMappingsOutput, error) {
	h.onList()
	return h.fakeLambda.ListEventSourceMappings(ctx, in, o...)
}

func TestEnsureRemovesDuplicates(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	m := newManager(t, f)
	f.add(streamA, mvworker, "Enabled", DefaultBatchSize)
	f.add(streamA, mvworker, "Enabled", DefaultBatchSize)
	if err := m.Ensure(ctx, streamA, mvworker, Options{}); err != nil {
		t.Fatal(err)
	}
	if ms := f.snapshot(); len(ms) != 1 || *ms[0].UUID != "uuid-1" {
		t.Errorf("duplicates: %+v", ms)
	}
}

func TestRemove(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	m := newManager(t, f)
	f.add(streamA, mvworker, "Enabled", DefaultBatchSize)
	f.add(streamA, replicatr, "Enabled", DefaultBatchSize)
	f.add(streamB, mvworker, "Enabled", DefaultBatchSize)
	if err := m.Remove(ctx, streamA, mvworker); err != nil {
		t.Fatal(err)
	}
	ms := f.snapshot()
	if len(ms) != 2 || *ms[0].FunctionArn != replicatr || *ms[1].EventSourceArn != streamB {
		t.Fatalf("after remove: %+v", ms)
	}
	// Missing mapping: no error, no delete.
	if err := m.Remove(ctx, streamA, mvworker); err != nil || f.count("Delete") != 1 {
		t.Errorf("remove missing: %v, deletes %d", err, f.count("Delete"))
	}
	// Already gone between List and Delete.
	f.failNext("Delete", &types.ResourceNotFoundException{Message: aws.String("gone")})
	if err := m.Remove(ctx, streamB, mvworker); err != nil {
		t.Errorf("not found on delete: %v", err)
	}
	// Being deleted: in transition.
	f.failNext("Delete", &types.ResourceInUseException{Message: aws.String("deleting")})
	if err := m.Remove(ctx, streamB, mvworker); !errors.Is(err, ErrInTransition) {
		t.Errorf("in use on delete: %v", err)
	}
	// A mapping already in state Deleting is left alone.
	f.mu.Lock()
	f.mappings[1].State = aws.String("Deleting")
	f.mu.Unlock()
	before := f.count("Delete")
	if err := m.Remove(ctx, streamB, mvworker); err != nil || f.count("Delete") != before {
		t.Errorf("deleting state: %v, deletes %d", err, f.count("Delete")-before)
	}
	if err := m.Remove(ctx, "", mvworker); err == nil {
		t.Error("empty stream accepted")
	}
}

func TestRemoveByTableAndPagination(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	f.pageSize = 2
	m := newManager(t, f)
	old := "arn:aws:dynamodb:eu-west-1:123456789012:table/chddb_default.events/stream/2025-01-01T00:00:00.000"
	f.add(streamB, mvworker, "Enabled", DefaultBatchSize)
	f.add(old, mvworker, "Enabled", DefaultBatchSize)
	f.add("arn:aws:dynamodb:eu-west-1:123456789012:table/chddb_default.events_2/stream/x", mvworker, "Enabled", DefaultBatchSize)
	f.add(streamA, mvworker, "Enabled", DefaultBatchSize)
	f.add(streamA, replicatr, "Enabled", DefaultBatchSize)
	if err := m.RemoveByTable(ctx, "chddb_default.events", mvworker); err != nil {
		t.Fatal(err)
	}
	var left []string
	for _, g := range f.snapshot() {
		left = append(left, *g.EventSourceArn+"->"+*g.FunctionArn)
	}
	want := []string{streamB + "->" + mvworker, "arn:aws:dynamodb:eu-west-1:123456789012:table/chddb_default.events_2/stream/x->" + mvworker, streamA + "->" + replicatr}
	if !slices.Equal(left, want) {
		t.Errorf("left %v\nwant %v", left, want)
	}
	if f.count("List") != 2 { // 4 of the function's mappings in pages of 2
		t.Errorf("pagination not followed: %d list calls, want 2", f.count("List"))
	}
	if TableOfStreamARN("junk") != "" || TableOfStreamARN(streamA) != "chddb_default.events" {
		t.Error("TableOfStreamARN")
	}
}

func TestThrottlingRetried(t *testing.T) {
	ctx := context.Background()
	f := newFake()
	m := newManager(t, f)
	throttle := &types.TooManyRequestsException{Message: aws.String("Rate exceeded")}
	f.failNext("List", throttle, throttle)
	f.failNext("Create", throttle)
	if err := m.Ensure(ctx, streamA, mvworker, Options{}); err != nil {
		t.Fatalf("throttling not retried: %v", err)
	}
	if f.count("List") != 3 || f.count("Create") != 2 {
		t.Errorf("calls: list %d create %d", f.count("List"), f.count("Create"))
	}
	// Persistent throttling gives up after RetryAttempts.
	f.mu.Lock()
	f.mappings = nil
	f.mu.Unlock()
	f.failNext("List", throttle, throttle, throttle, throttle, throttle, throttle, throttle)
	err := m.Ensure(ctx, streamA, mvworker, Options{})
	var tm *types.TooManyRequestsException
	if !errors.As(err, &tm) {
		t.Errorf("persistent throttling: %v", err)
	}
	// A cancelled context stops the retries at once.
	f.failNext("List", throttle, throttle, throttle)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := m.Ensure(cctx, streamA, mvworker, Options{}); err == nil {
		t.Error("cancelled context ignored")
	}
	// Non-throttling errors are not retried.
	f.mu.Lock()
	f.fail["List"] = nil // the cancelled run left queued throttles behind
	f.mu.Unlock()
	f.failNext("List", fmt.Errorf("boom"))
	before := f.count("List")
	if err := m.Ensure(ctx, streamA, mvworker, Options{}); err == nil || f.count("List") != before+1 {
		t.Errorf("non-throttling error: %v, %d calls", err, f.count("List")-before)
	}
}

func TestNewManagerRequiresAPI(t *testing.T) {
	if _, err := NewManager(ManagerConfig{}); err == nil {
		t.Error("nil API accepted")
	}
}
