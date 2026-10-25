// Command mvworker applies materialized views from the DynamoDB streams of
// view-source tables to region-local target tables (DESIGN.md §5).
//
// It runs as a long-lived stream consumer, or as an AWS Lambda function
// consuming a DynamoDB stream event-source mapping when started with -lambda
// or when AWS_LAMBDA_RUNTIME_API is set.
//
// Configuration: the common environment variables of internal/config, plus
//
//	MVWORKER_OWNER         lease owner id (default hostname:pid)
//	STREAM_LEASE_DURATION  shard lease TTL, Go duration (default 30s)
//	STATS_INTERVAL         progress log interval, Go duration (default 30s; 0 disables)
//	MV_POISON_POLICY       deadletter (default): a record whose view application
//	                       keeps failing with a deterministic engine error is
//	                       written to {prefix}__dead_letters and skipped, so its
//	                       shard advances; block: retry it forever (DESIGN.md §5)
//	MV_POISON_AFTER        how long such a failure must persist first (default 30s)
//	CATALOG_MIN_RELOAD     minimum spacing of the catalog reloads triggered by
//	                       batches newer than the snapshot (default 1s)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-lambda-go/lambda"

	"dynamodb-eventstream/internal/awsx"
	"dynamodb-eventstream/internal/config"
	"dynamodb-eventstream/internal/exec"
	"dynamodb-eventstream/internal/mvworker"
	"dynamodb-eventstream/internal/storage/dynamo"
)

// serverRevision is the ClickHouse revision reported by revision().
const serverRevision = 54477

func main() {
	lambdaMode := flag.Bool("lambda", false, "run as an AWS Lambda DynamoDB-stream handler")
	flag.Parse()
	if os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		*lambdaMode = true
	}
	if err := run(*lambdaMode); err != nil {
		slog.Error("mvworker failed", "err", err)
		os.Exit(1)
	}
}

func run(lambdaMode bool) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	slog.SetDefault(newLogger(cfg.LogLevel))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	clients, err := awsx.New(ctx, cfg.Region, cfg.DynamoDBEndpoint)
	if err != nil {
		return fmt.Errorf("aws clients: %w", err)
	}
	store, err := dynamo.New(clients, dynamo.Options{
		Prefix:         cfg.TablePrefix,
		Region:         cfg.Region,
		GlobalMode:     cfg.GlobalMode,
		ReplicaRegions: cfg.ReplicaRegions,
		HomeRegion:     cfg.HomeRegion,
	})
	if err != nil {
		return fmt.Errorf("dynamo store: %w", err)
	}
	cat := store.NewCatalog()
	host, _ := os.Hostname()
	engine, err := exec.New(exec.Config{
		Store:           store,
		Catalog:         cat,
		Region:          cfg.Region,
		InlineViews:     false,
		ConsistentReads: cfg.ConsistentReads,
		ServerVersion:   cfg.ServerVersion,
		ServerRevision:  serverRevision,
		DisplayName:     host,
		Timezone:        cfg.Timezone,
		MultiRegion:     cfg.GlobalMode != config.GlobalNone,
	})
	if err != nil {
		return fmt.Errorf("engine: %w", err)
	}

	if lambdaMode {
		h, err := mvworker.NewLambdaHandler(mvworker.LambdaConfig{
			Store: store, Catalog: cat, Engine: engine, CatalogRefresh: cfg.CatalogRefresh,
			DeadLetters: clients.DynamoDB,
		})
		if err != nil {
			return err
		}
		lambda.StartWithOptions(h, lambda.WithContext(ctx))
		return nil
	}

	if err := retry(ctx, "ensure system tables", store.EnsureSystemTables); err != nil {
		return err
	}
	lease, err := envDuration("STREAM_LEASE_DURATION", 30*time.Second)
	if err != nil {
		return err
	}
	statsEvery, err := envDuration("STATS_INTERVAL", 30*time.Second)
	if err != nil {
		return err
	}
	w, err := mvworker.New(mvworker.Config{
		Clients:        clients,
		Store:          store,
		Catalog:        cat,
		Engine:         engine,
		Owner:          os.Getenv("MVWORKER_OWNER"),
		PollInterval:   cfg.StreamPollInterval,
		LeaseDuration:  lease,
		CatalogRefresh: cfg.CatalogRefresh,
	})
	if err != nil {
		return err
	}
	if statsEvery > 0 {
		go logStats(ctx, w, statsEvery)
	}
	slog.Info("mvworker started", "region", cfg.Region, "prefix", cfg.TablePrefix, "endpoint", cfg.DynamoDBEndpoint)
	err = w.Run(ctx)
	slog.Info("mvworker stopped", "stats", fmt.Sprintf("%+v", w.Stats()))
	return err
}

func logStats(ctx context.Context, w *mvworker.Worker, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s := w.Stats()
			attrs := []any{"applied", s.RecordsApplied, "skipped", s.RecordsSkipped, "errors", s.Errors,
				"dead_lettered", s.DeadLettered, "catalog_reloads", s.CatalogReloads, "shards", s.Shards,
				"lag_ms", s.LagMillis, "retrying_batches", s.RetryingBatches, "pending_age_ms", s.PendingAgeMillis}
			if s.PendingAgeMillis >= time.Hour.Milliseconds() {
				// Stream records are trimmed after 24h: alert long before.
				slog.Error("mvworker shard stuck: oldest pending record is old; it is lost when the stream trims it (24h)", attrs...)
			} else {
				slog.Info("mvworker progress", attrs...)
			}
		}
	}
}

// retry calls fn with capped exponential backoff until it succeeds, fails
// with a permanent error (awsx.PermanentError: bad request, credentials,
// layout mismatch) or ctx ends (DynamoDB may still be starting).
func retry(ctx context.Context, what string, fn func(context.Context) error) error {
	wait := 250 * time.Millisecond
	for {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return errors.Join(ctx.Err(), err)
		}
		if awsx.PermanentError(err) {
			return fmt.Errorf("%s: %w", what, err)
		}
		slog.Warn(what+" failed; retrying", "err", err, "in", wait)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		wait = min(wait*2, 5*time.Second)
	}
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}

func envDuration(k string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", k, err)
	}
	return d, nil
}
