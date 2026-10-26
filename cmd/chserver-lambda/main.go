// Command chserver-lambda is the ClickHouse-compatible HTTP API (the same
// server as cmd/chserver) packaged as an AWS Lambda function behind an API
// Gateway REST API with Lambda proxy integration (deploy/aws-lambda).
//
// Differences from cmd/chserver: there are no background loops (the catalog
// snapshot is reloaded at the start of a request when it is older than
// CATALOG_REFRESH, and the reconciler and stream worker are separate
// Lambdas), HTTP sessions (session_id) are rejected with NOT_IMPLEMENTED
// (48) since no process outlives a request, and request/response bodies are
// bounded by the Lambda payload limit (6 MB). Configuration is the
// environment of internal/config (LISTEN_ADDR, RUN_STREAM_WORKER and
// MAX_EXECUTION_TIME aside: the API Gateway integration timeout bounds a
// query; set MAX_EXECUTION_TIME below it for a ClickHouse-style error).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"

	"github.com/gfronza/twinhouse/internal/awsx"
	"github.com/gfronza/twinhouse/internal/config"
	"github.com/gfronza/twinhouse/internal/exec"
	"github.com/gfronza/twinhouse/internal/server"
	"github.com/gfronza/twinhouse/internal/storage/dynamo"
)

func main() {
	h, err := build(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, "chserver-lambda:", err)
		os.Exit(1)
	}
	lambda.Start(h)
}

// build wires Store, Catalog, Engine and Server like cmd/chserver does and
// returns the Lambda handler. Nothing talks to DynamoDB here: the first
// invocation checks the system tables exist (the reconciler creates them)
// and loads the catalog (App.Prepare), so a cold start stays short and a
// slow DynamoDB fails one request, not the function's initialization.
func build(ctx context.Context) (Handler, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	log := newLogger(cfg.LogLevel)
	slog.SetDefault(log)

	clients, err := awsx.New(ctx, cfg.Region, cfg.DynamoDBEndpoint)
	if err != nil {
		return nil, fmt.Errorf("aws clients: %w", err)
	}
	store, err := dynamo.New(clients, dynamo.Options{
		Prefix:         cfg.TablePrefix,
		Region:         cfg.Region,
		GlobalMode:     cfg.GlobalMode,
		ReplicaRegions: cfg.ReplicaRegions,
		HomeRegion:     cfg.HomeRegion,
		// A request has the API Gateway integration timeout (29 s) as its
		// budget: CREATE TABLE waits for the table, not for its replicas,
		// and DROP TABLE does not wait for the deletion; the reconciler
		// converges both (DESIGN.md section 2).
		AsyncDDL: true,
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb store: %w", err)
	}
	cat := store.NewCatalog()

	engine, err := exec.New(exec.Config{
		Store:           store,
		Catalog:         cat,
		Region:          cfg.Region,
		InlineViews:     cfg.InlineViews,
		StreamViews:     cfg.StreamViews,
		Logger:          log,
		ConsistentReads: cfg.ConsistentReads,
		ServerVersion:   cfg.ServerVersion,
		ServerRevision:  server.ServerRevision,
		DisplayName:     cfg.DisplayName,
		Timezone:        cfg.Timezone,
		MultiRegion:     cfg.GlobalMode != config.GlobalNone,
	})
	if err != nil {
		return nil, fmt.Errorf("engine: %w", err)
	}
	srv, err := server.New(server.Config{
		Executor:         engine,
		Catalog:          cat,
		Users:            cfg.Users,
		ServerVersion:    cfg.ServerVersion,
		DisplayName:      cfg.DisplayName,
		Timezone:         cfg.Timezone,
		Logger:           log,
		MaxExecutionTime: cfg.MaxExecutionTime,
		RejectSessions:   true,
	})
	if err != nil {
		return nil, err
	}
	app := &App{
		Catalog: cat,
		MaxAge:  cfg.CatalogRefresh,
		// The reconciler creates and converges the system tables (adding
		// a global-table replica blocks for minutes); a request only
		// checks they exist and fails with UNAVAILABLE until they do.
		Ensure:        store.CheckSystemTables,
		ServerVersion: cfg.ServerVersion,
		Logger:        log,
	}
	log.Info("chserver-lambda ready", "region", cfg.Region, "display_name", cfg.DisplayName,
		"version", cfg.ServerVersion, "global_mode", cfg.GlobalMode, "catalog_refresh", cfg.CatalogRefresh)
	return NewHandler(app.Middleware(srv)), nil
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn", "warning":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}
