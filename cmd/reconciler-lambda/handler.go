package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// systemTables is the part of dynamo.Store the handler uses.
type systemTables interface {
	EnsureSystemTables(ctx context.Context) error
	CatalogTableName() string
	StreamARN(ctx context.Context, physical string) (string, error)
}

// reconciler is the part of server.Reconciler the handler uses.
type reconciler interface {
	RunOnce(ctx context.Context)
}

// handlerConfig configures the Lambda handler.
type handlerConfig struct {
	Store      systemTables
	Reconciler reconciler
	// WireCatalog, when set, makes the {prefix}__catalog stream consumed
	// by the reconciler function (RECONCILER_FUNCTION_ARN): called with the
	// catalog stream ARN once per execution environment, until it succeeds.
	WireCatalog func(ctx context.Context, catalogStreamARN string) error
	// Margin is kept free before the invocation deadline (default 5s), so
	// the round ends, and logs, before Lambda kills the environment.
	Margin time.Duration
	Logger *slog.Logger
}

// handler runs one reconcile round per invocation.
type handler struct {
	cfg handlerConfig
	log *slog.Logger

	mu           sync.Mutex // invocations of one environment are sequential; kept for -once and tests
	catalogWired bool
}

func newHandler(cfg handlerConfig) (*handler, error) {
	if cfg.Store == nil || cfg.Reconciler == nil {
		return nil, errors.New("reconciler-lambda: Store and Reconciler are required")
	}
	if cfg.Margin <= 0 {
		cfg.Margin = 5 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &handler{cfg: cfg, log: cfg.Logger.With("component", "reconciler-lambda")}, nil
}

// eventKind is what triggered an invocation.
type eventKind string

const (
	kindCatalogStream eventKind = "catalog-stream" // a DynamoDB stream batch of {prefix}__catalog
	kindSchedule      eventKind = "schedule"       // the EventBridge safety-net schedule
	kindUnknown       eventKind = "unknown"
)

// classify tells a DynamoDB stream event from an EventBridge scheduled
// event by shape; records is the number of stream records.
func classify(raw json.RawMessage) (kind eventKind, records int) {
	var ev struct {
		Records    []json.RawMessage `json:"Records"`
		Source     string            `json:"source"`
		DetailType string            `json:"detail-type"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return kindUnknown, 0
	}
	switch {
	case ev.Records != nil:
		return kindCatalogStream, len(ev.Records)
	case ev.Source == "aws.events" || strings.EqualFold(ev.DetailType, "Scheduled Event"):
		return kindSchedule, 0
	}
	return kindUnknown, 0
}

// Handle is the Lambda entry point: any event (a catalog stream batch, the
// schedule, or anything else) triggers one full round: EnsureSystemTables,
// wiring of the catalog stream to this function, Reconciler.RunOnce. The
// round is bounded by the invocation deadline minus Margin. It returns an
// error only when the system tables are not available (so a stream batch
// is redelivered and a scheduled invocation retried); reconcile failures
// of individual tables are logged and retried on the next round.
func (h *handler) Handle(ctx context.Context, raw json.RawMessage) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	kind, records := classify(raw)
	if kind == kindUnknown {
		h.log.Warn("unrecognised event; running a reconcile round anyway", "event", truncate(raw, 512))
	}
	if dl, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, dl.Add(-h.cfg.Margin))
		defer cancel()
	}
	start := time.Now()
	if err := h.cfg.Store.EnsureSystemTables(ctx); err != nil {
		h.log.Warn("ensure system tables failed", "error", err)
		return fmt.Errorf("ensure system tables: %w", err)
	}
	h.wireCatalog(ctx)
	h.cfg.Reconciler.RunOnce(ctx)
	h.log.Info("reconcile round done", "trigger", kind, "records", records, "elapsed_ms", time.Since(start).Milliseconds())
	return nil
}

// wireCatalog makes the catalog stream consumed by this function, once per
// environment; failures are logged and retried on the next invocation.
func (h *handler) wireCatalog(ctx context.Context) {
	if h.cfg.WireCatalog == nil || h.catalogWired {
		return
	}
	name := h.cfg.Store.CatalogTableName()
	arn, err := h.cfg.Store.StreamARN(ctx, name)
	if err != nil {
		h.log.Warn("cannot resolve catalog stream", "table", name, "error", err)
		return
	}
	if arn == "" {
		h.log.Error("catalog table has no stream; DDL in other regions is only picked up by the schedule", "table", name)
		h.catalogWired = true
		return
	}
	if err := h.cfg.WireCatalog(ctx, arn); err != nil {
		h.log.Warn("cannot wire catalog stream to this function", "stream", arn, "error", err)
		return
	}
	h.catalogWired = true
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
