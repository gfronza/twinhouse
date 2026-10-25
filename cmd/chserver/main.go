// Command chserver is the ClickHouse-compatible HTTP server backed by
// DynamoDB (DESIGN.md §1–2). Configuration comes from the environment (see
// internal/config.Load, including RUN_STREAM_WORKER and DISPLAY_NAME).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"dynamodb-eventstream/internal/awsx"
	"dynamodb-eventstream/internal/config"
	"dynamodb-eventstream/internal/exec"
	"dynamodb-eventstream/internal/mvworker"
	"dynamodb-eventstream/internal/server"
	"dynamodb-eventstream/internal/storage/dynamo"
)

const shutdownTimeout = 15 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "chserver:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)
	slog.SetDefault(log)

	runWorker, displayName := cfg.RunStreamWorker, cfg.DisplayName

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
	// DynamoDB may start after us (compose): retry until it answers.
	if err := retry(ctx, log, "ensure system tables", func() error { return store.EnsureSystemTables(ctx) }); err != nil {
		return startupErr(ctx, err)
	}
	cat := store.NewCatalog()
	if err := retry(ctx, log, "load catalog", func() error { return cat.Refresh(ctx) }); err != nil {
		return startupErr(ctx, err)
	}

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
		DisplayName:     displayName,
		Timezone:        cfg.Timezone,
		MultiRegion:     cfg.GlobalMode != config.GlobalNone,
	})
	if err != nil {
		return fmt.Errorf("engine: %w", err)
	}

	srv, err := server.New(server.Config{
		Executor:      engine,
		Catalog:       cat,
		Users:         cfg.Users,
		ServerVersion: cfg.ServerVersion,
		DisplayName:   displayName,
		Timezone:      cfg.Timezone,
		Logger:        log,

		MaxExecutionTime: cfg.MaxExecutionTime,
	})
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	bg, cancelBG := context.WithCancel(ctx)
	defer cancelBG()

	rec := server.NewReconciler(server.ReconcilerConfig{
		Catalog: cat, Store: store, Interval: cfg.CatalogRefresh, Logger: log,
	})
	wg.Go(func() { rec.Run(bg) })

	if runWorker {
		host, _ := os.Hostname()
		w, err := mvworker.New(mvworker.Config{
			Clients:        clients,
			Store:          store,
			Catalog:        cat,
			Engine:         engine,
			Owner:          host + ":" + strconv.Itoa(os.Getpid()),
			PollInterval:   cfg.StreamPollInterval,
			CatalogRefresh: cfg.CatalogRefresh,
		})
		if err != nil {
			return fmt.Errorf("stream worker: %w", err)
		}
		wg.Go(func() {
			// The worker must not take the server down; restart it on failure.
			for bg.Err() == nil {
				if err := w.Run(bg); err != nil && bg.Err() == nil {
					log.Error("stream worker stopped; restarting", "error", err)
					sleep(bg, 2*time.Second)
				}
			}
		})
		log.Info("stream worker started")
	}

	httpSrv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           srv,
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	errc := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.ListenAddr, "region", cfg.Region, "display_name", displayName,
			"version", cfg.ServerVersion, "global_mode", cfg.GlobalMode, "stream_worker", runWorker)
		errc <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		cancelBG()
		wg.Wait()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down")
	shCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	// Stop the background loops (reconciler, stream worker) while the HTTP
	// server drains, not after: they stop promptly on cancellation, and
	// in-flight requests do not depend on them.
	cancelBG()
	err = httpSrv.Shutdown(shCtx)
	wg.Wait()
	if err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}

// startupErr treats a shutdown signal received during startup as a clean exit.
func startupErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		slog.Info("shutdown requested during startup")
		return nil
	}
	return err
}

// retry calls f with capped exponential backoff until it succeeds, fails
// with a permanent error, or ctx ends.
func retry(ctx context.Context, log *slog.Logger, what string, f func() error) error {
	delay := retryInitialDelay
	for attempt := 1; ; attempt++ {
		err := f()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("%s: %w", what, ctx.Err())
		}
		if permanentError(err) {
			return fmt.Errorf("%s: %w", what, err)
		}
		log.Warn("retrying", "op", what, "attempt", attempt, "error", err, "in", delay)
		if !sleep(ctx, delay) {
			return fmt.Errorf("%s: %w", what, ctx.Err())
		}
		delay = min(2*delay, 5*time.Second)
	}
}

// retryInitialDelay is the first startup retry backoff (tests shorten it).
var retryInitialDelay = 500 * time.Millisecond

// permanentError reports whether a startup error will not go away by
// retrying (see awsx.PermanentError).
func permanentError(err error) bool { return awsx.PermanentError(err) }

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
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
