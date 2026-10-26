// Command reconciler-lambda runs the catalog reconciler (DESIGN.md §2) as
// an AWS Lambda function of the serverless deployment (deploy/aws-lambda): each
// invocation ensures the system tables, wires the {prefix}__catalog stream
// to this function, and runs one reconcile round, which creates and drops
// this region's physical tables and manages the Lambda event-source
// mappings of their streams (internal/esm): every source table's stream to
// MVWORKER_FUNCTION_ARN and, with GLOBAL_MODE=aws, every ReplacingMergeTree
// table's stream to REPLICATOR_FUNCTION_ARN.
//
// It is invoked by the catalog stream itself (a DDL in any region) and by an
// EventBridge schedule (rate(1 minute)) as a safety net; both event shapes
// are accepted, and any other event also runs a round.
//
// Configuration: the common environment variables of internal/config
// (MVWORKER_FUNCTION_ARN, REPLICATOR_FUNCTION_ARN, RECONCILER_FUNCTION_ARN,
// ESM_BATCH_SIZE, ...), plus
//
//	RECONCILER_DEADLINE_MARGIN  time kept free before the invocation deadline,
//	                            Go duration (default 5s)
//
// Outside Lambda (AWS_LAMBDA_RUNTIME_API unset), -once runs a single round
// against the configured DynamoDB endpoint and exits (local smoke tests).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-lambda-go/lambda"

	"github.com/gfronza/twinhouse/internal/awsx"
	"github.com/gfronza/twinhouse/internal/config"
	"github.com/gfronza/twinhouse/internal/esm"
	"github.com/gfronza/twinhouse/internal/server"
	"github.com/gfronza/twinhouse/internal/storage/dynamo"
)

func main() {
	once := flag.Bool("once", false, "run one reconcile round and exit (outside Lambda)")
	flag.Parse()
	lambdaMode := os.Getenv("AWS_LAMBDA_RUNTIME_API") != ""
	if err := run(lambdaMode, *once); err != nil {
		slog.Error("reconciler-lambda failed", "err", err)
		os.Exit(1)
	}
}

func run(lambdaMode, once bool) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)
	slog.SetDefault(log)
	margin, err := envDuration("RECONCILER_DEADLINE_MARGIN", 5*time.Second)
	if err != nil {
		return err
	}
	if !lambdaMode && !once {
		return fmt.Errorf("not running inside AWS Lambda (AWS_LAMBDA_RUNTIME_API unset); use -once for a single local round")
	}

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
		return fmt.Errorf("dynamodb store: %w", err)
	}
	cat := store.NewCatalog()

	hcfg := handlerConfig{Store: store, Margin: margin, Logger: log}
	rcfg := server.ReconcilerConfig{Catalog: cat, Store: store, Logger: log}
	if cfg.ManagesEventSourceMappings() {
		lc, err := esm.NewClient(ctx, cfg.Region)
		if err != nil {
			return fmt.Errorf("lambda client: %w", err)
		}
		mgr, err := esm.NewManager(esm.ManagerConfig{API: lc, Logger: log})
		if err != nil {
			return err
		}
		opts := esm.Options{BatchSize: cfg.ESMBatchSize}
		cons, err := esm.NewConsumers(esm.ConsumersConfig{
			Manager: mgr, MVWorkerARN: cfg.MVWorkerFunctionARN, ReplicatorARN: cfg.ReplicatorFunctionARN,
			GlobalMode: cfg.GlobalMode, Options: opts, Logger: log,
		})
		if err != nil {
			return err
		}
		rcfg.Consumers = cons
		if self := cfg.ReconcilerFunctionARN; self != "" {
			hcfg.WireCatalog = func(ctx context.Context, streamARN string) error {
				return mgr.Ensure(ctx, streamARN, self, opts)
			}
		} else {
			log.Warn("RECONCILER_FUNCTION_ARN is empty; the catalog stream is not wired to the reconciler (DDL is picked up by the schedule only)")
		}
	} else {
		log.Info("MVWORKER_FUNCTION_ARN is empty; event-source mappings are not managed")
	}
	hcfg.Reconciler = server.NewReconciler(rcfg)
	h, err := newHandler(hcfg)
	if err != nil {
		return err
	}
	log.Info("reconciler-lambda starting", "region", cfg.Region, "prefix", cfg.TablePrefix, "global_mode", cfg.GlobalMode,
		"esm", cfg.ManagesEventSourceMappings(), "mvworker", cfg.MVWorkerFunctionARN, "replicator", cfg.ReplicatorFunctionARN,
		"reconciler", cfg.ReconcilerFunctionARN, "lambda", lambdaMode)
	if lambdaMode {
		lambda.StartWithOptions(h.Handle, lambda.WithContext(ctx))
		return nil
	}
	return h.Handle(ctx, json.RawMessage(`{"source":"local","detail-type":"Scheduled Event"}`))
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
	if err != nil || d < 0 {
		return 0, fmt.Errorf("%s: invalid duration %q", k, v)
	}
	return d, nil
}
