package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

type fakeStore struct {
	mu        sync.Mutex
	ensures   int
	ensureErr error
	streamARN string
	streamErr error
}

func (f *fakeStore) EnsureSystemTables(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensures++
	return f.ensureErr
}

func (f *fakeStore) CatalogTableName() string { return "twinhouse___catalog" }

func (f *fakeStore) StreamARN(_ context.Context, physical string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if physical != "twinhouse___catalog" {
		return "", errors.New("unexpected table " + physical)
	}
	return f.streamARN, f.streamErr
}

type fakeReconciler struct {
	mu        sync.Mutex
	rounds    int
	deadlines []time.Time
}

func (f *fakeReconciler) RunOnce(ctx context.Context) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rounds++
	dl, _ := ctx.Deadline()
	f.deadlines = append(f.deadlines, dl)
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

const streamEvent = `{"Records":[{"eventID":"1","eventName":"INSERT","eventSource":"aws:dynamodb","awsRegion":"eu-west-1",
 "dynamodb":{"Keys":{"pk":{"S":"table#default.t"}},"SequenceNumber":"111","StreamViewType":"NEW_AND_OLD_IMAGES"},
 "eventSourceARN":"arn:aws:dynamodb:eu-west-1:123456789012:table/twinhouse___catalog/stream/2026-01-01T00:00:00.000"},
 {"eventID":"2","eventName":"REMOVE","eventSource":"aws:dynamodb","awsRegion":"eu-west-1",
 "dynamodb":{"Keys":{"pk":{"S":"table#default.u"}},"SequenceNumber":"112"},
 "eventSourceARN":"arn:aws:dynamodb:eu-west-1:123456789012:table/twinhouse___catalog/stream/2026-01-01T00:00:00.000"}]}`

const scheduleEvent = `{"version":"0","id":"53dc4d37-cffa-4f76-80c9-8b7d4a4d2eaa","detail-type":"Scheduled Event","source":"aws.events",
 "account":"123456789012","time":"2026-10-25T10:00:00Z","region":"eu-west-1",
 "resources":["arn:aws:events:eu-west-1:123456789012:rule/twinhouse-reconciler-schedule"],"detail":{}}`

func TestClassify(t *testing.T) {
	if k, n := classify(json.RawMessage(streamEvent)); k != kindCatalogStream || n != 2 {
		t.Errorf("stream event: %s %d", k, n)
	}
	if k, n := classify(json.RawMessage(scheduleEvent)); k != kindSchedule || n != 0 {
		t.Errorf("schedule event: %s %d", k, n)
	}
	// EventBridge Scheduler (not a rule) sends source "aws.scheduler" with an arbitrary detail-type.
	if k, _ := classify(json.RawMessage(`{"source":"aws.scheduler","detail-type":"Scheduled Event"}`)); k != kindSchedule {
		t.Errorf("scheduler event: %s", k)
	}
	if k, n := classify(json.RawMessage(`{"Records":[]}`)); k != kindCatalogStream || n != 0 {
		t.Errorf("empty batch: %s %d", k, n)
	}
	for _, raw := range []string{`{}`, `"string"`, `not json`, `{"hello":"world"}`} {
		if k, _ := classify(json.RawMessage(raw)); k != kindUnknown {
			t.Errorf("%s: %s", raw, k)
		}
	}
}

func TestHandleBothShapes(t *testing.T) {
	st := &fakeStore{streamARN: "arn:aws:dynamodb:eu-west-1:123456789012:table/twinhouse___catalog/stream/x"}
	rec := &fakeReconciler{}
	var wired []string
	h, err := newHandler(handlerConfig{Store: st, Reconciler: rec, Logger: quiet(), Margin: time.Second,
		WireCatalog: func(_ context.Context, arn string) error { wired = append(wired, arn); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{streamEvent, scheduleEvent, `{"hello":"world"}`} {
		if err := h.Handle(context.Background(), json.RawMessage(raw)); err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
	}
	if st.ensures != 3 || rec.rounds != 3 {
		t.Errorf("ensures %d rounds %d, want 3 each", st.ensures, rec.rounds)
	}
	// The catalog stream is wired once per environment.
	if len(wired) != 1 || wired[0] != st.streamARN {
		t.Errorf("wired %v", wired)
	}
	// Without a deadline the round is unbounded; with one, Margin is kept free.
	if !rec.deadlines[0].IsZero() {
		t.Errorf("deadline without one on the invocation: %v", rec.deadlines[0])
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.Handle(ctx, json.RawMessage(scheduleEvent)); err != nil {
		t.Fatal(err)
	}
	dl, _ := ctx.Deadline()
	got := rec.deadlines[3]
	if d := dl.Sub(got); d < 900*time.Millisecond || d > 1100*time.Millisecond {
		t.Errorf("round deadline %v before the invocation deadline, want ~1s", d)
	}
}

func TestHandleErrors(t *testing.T) {
	// System tables unavailable: the invocation fails (the batch is redelivered).
	st := &fakeStore{ensureErr: errors.New("awaiting the catalog replica")}
	rec := &fakeReconciler{}
	h, _ := newHandler(handlerConfig{Store: st, Reconciler: rec, Logger: quiet()})
	if err := h.Handle(context.Background(), json.RawMessage(streamEvent)); err == nil || rec.rounds != 0 {
		t.Errorf("err %v rounds %d", err, rec.rounds)
	}

	// Catalog wiring failures are retried on the next invocation and never
	// fail the round; a catalog without a stream is wired "once" (logged).
	st = &fakeStore{streamErr: errors.New("throttled")}
	calls := 0
	h, _ = newHandler(handlerConfig{Store: st, Reconciler: rec, Logger: quiet(),
		WireCatalog: func(context.Context, string) error { calls++; return errors.New("TooManyRequests") }})
	if err := h.Handle(context.Background(), json.RawMessage(scheduleEvent)); err != nil || calls != 0 || h.catalogWired {
		t.Errorf("stream lookup failure: err %v calls %d wired %v", err, calls, h.catalogWired)
	}
	st.mu.Lock()
	st.streamErr, st.streamARN = nil, "arn:stream"
	st.mu.Unlock()
	if err := h.Handle(context.Background(), json.RawMessage(scheduleEvent)); err != nil || calls != 1 || h.catalogWired {
		t.Errorf("wire failure: err %v calls %d wired %v", err, calls, h.catalogWired)
	}
	h.cfg.WireCatalog = func(context.Context, string) error { calls++; return nil }
	for range 2 {
		if err := h.Handle(context.Background(), json.RawMessage(scheduleEvent)); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 || !h.catalogWired {
		t.Errorf("wire retry: calls %d wired %v", calls, h.catalogWired)
	}
	if rec.rounds != 4 {
		t.Errorf("rounds %d", rec.rounds)
	}
	st = &fakeStore{} // no stream
	h, _ = newHandler(handlerConfig{Store: st, Reconciler: rec, Logger: quiet(),
		WireCatalog: func(context.Context, string) error { t.Error("wired a table without a stream"); return nil }})
	if err := h.Handle(context.Background(), json.RawMessage(scheduleEvent)); err != nil || !h.catalogWired {
		t.Errorf("no stream: err %v wired %v", err, h.catalogWired)
	}

	if _, err := newHandler(handlerConfig{Store: st}); err == nil {
		t.Error("missing reconciler accepted")
	}
}

func TestRunRefusesOutsideLambdaWithoutOnce(t *testing.T) {
	t.Setenv("REGION", "eu-west-1")
	t.Setenv("DYNAMODB_ENDPOINT", "")
	t.Setenv("MVWORKER_FUNCTION_ARN", "")
	t.Setenv("REPLICATOR_FUNCTION_ARN", "")
	if err := run(false, false); err == nil {
		t.Error("run outside Lambda without -once succeeded")
	}
	t.Setenv("RECONCILER_DEADLINE_MARGIN", "bogus")
	if err := run(true, false); err == nil {
		t.Error("invalid margin accepted")
	}
}
