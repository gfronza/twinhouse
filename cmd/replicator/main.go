// Command replicator emulates DynamoDB global tables between independent
// DynamoDB endpoints for local multi-region development, and copies the
// ReplacingMergeTree tables of a GLOBAL_MODE=aws deployment between its
// regions (DESIGN.md §6).
//
// It runs as a long-lived stream poller tailing every region it lists in
// REGIONS, or as an AWS Lambda function (-lambda, or automatically when
// AWS_LAMBDA_RUNTIME_API is set) consuming the DynamoDB stream event-source
// mappings of this region's ReplacingMergeTree tables and copying the rows
// written here to every region in REPLICA_REGIONS. Lambda mode requires
// GLOBAL_MODE=aws and uses REGION/REPLICA_REGIONS (and GLOBAL_HOME_REGION)
// instead of REGIONS; its catalog snapshot is refreshed when older than
// CATALOG_REFRESH.
//
// Configuration (environment):
//
//	REGIONS                "us-east-1=http://dynamodb-us:8000,eu-west-1=http://dynamodb-eu:8000"
//	                       (an empty endpoint means the real AWS region)
//	TABLE_PREFIX           physical table prefix (default "twinhouse_")
//	REPLICATION_DELAY      artificial lag, Go duration (default 0)
//	STREAM_POLL_INTERVAL   stream idle poll interval (default 250ms)
//	CATALOG_REFRESH        table discovery interval (default 2s)
//	STREAM_LEASE_DURATION  shard lease TTL (default 30s)
//	REPLICATOR_OWNER       lease owner id (default hostname:pid)
//	STREAM_DISCOVERY_INTERVAL  how often each pipe re-describes its stream
//	                       (default and minimum 5s: DescribeStream is limited
//	                       to ~10 TPS per account)
//	APPLY_CONFLICT_VIEWS   true (default) applies materialized views, in the
//	                       target region, to replicated row versions the target
//	                       rejects because it holds a newer version, so
//	                       regional rollups converge (DESIGN.md §1, §6)
//	MV_POISON_POLICY, MV_POISON_AFTER  as for cmd/mvworker (poison handling of
//	                       those view applications)
//	STATS_INTERVAL         progress log interval (default 30s; 0 disables)
//	LOG_LEVEL              debug|info|warn|error
//	GLOBAL_MODE            emulated (default behaviour): replicate the catalog
//	                       and every global table; aws: the regions use real
//	                       global tables, so replicate only the
//	                       ReplacingMergeTree(ver) tables, which are regional
//	                       in that mode (version-aware, DESIGN.md §1/§6)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/gfronza/twinhouse/internal/awsx"
	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/config"
	"github.com/gfronza/twinhouse/internal/exec"
	"github.com/gfronza/twinhouse/internal/mvworker"
	"github.com/gfronza/twinhouse/internal/replicator"
	"github.com/gfronza/twinhouse/internal/storage/dynamo"
)

func main() {
	lambdaMode := flag.Bool("lambda", false, "run as an AWS Lambda DynamoDB-stream handler (GLOBAL_MODE=aws)")
	flag.Parse()
	if os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		*lambdaMode = true
	}
	if err := run(*lambdaMode); err != nil {
		slog.Error("replicator failed", "err", err)
		os.Exit(1)
	}
}

// regionSpec is one REGIONS entry.
type regionSpec struct{ name, endpoint string }

// parseRegions parses "name=endpoint,name2=endpoint2".
func parseRegions(s string) ([]regionSpec, error) {
	var out []regionSpec
	seen := map[string]bool{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, ep, ok := strings.Cut(part, "=")
		name, ep = strings.TrimSpace(name), strings.TrimSpace(ep)
		if !ok || name == "" {
			return nil, fmt.Errorf("REGIONS: entry %q must be name=endpoint", part)
		}
		if seen[name] {
			return nil, fmt.Errorf("REGIONS: duplicate region %q", name)
		}
		seen[name] = true
		out = append(out, regionSpec{name, ep})
	}
	if len(out) < 2 {
		return nil, errors.New("REGIONS: at least two regions are required")
	}
	return out, nil
}

func run(lambdaMode bool) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	slog.SetDefault(newLogger(cfg.LogLevel))
	if lambdaMode {
		return runLambda(cfg)
	}
	specs, err := parseRegions(os.Getenv("REGIONS"))
	if err != nil {
		return err
	}
	delay, err := envDuration("REPLICATION_DELAY", 0)
	if err != nil {
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
	streamDiscovery, err := envDuration("STREAM_DISCOVERY_INTERVAL", replicator.MinStreamDiscoveryInterval)
	if err != nil {
		return err
	}
	conflictViews, err := envBool("APPLY_CONFLICT_VIEWS", true)
	if err != nil {
		return err
	}
	owner := os.Getenv("REPLICATOR_OWNER")
	if owner == "" {
		host, _ := os.Hostname()
		owner = fmt.Sprintf("%s:%d", host, os.Getpid())
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	versionedOnly := cfg.GlobalMode == config.GlobalAWS

	var regions []replicator.Region
	var catalogs []regionCatalog
	appliers := map[string]*mvworker.ImageApplier{}
	for _, s := range specs {
		c, err := awsx.New(ctx, s.name, s.endpoint)
		if err != nil {
			return fmt.Errorf("aws clients for %s: %w", s.name, err)
		}
		store, err := dynamo.New(c, dynamo.Options{Prefix: cfg.TablePrefix, Region: s.name, GlobalMode: config.GlobalEmulated})
		if err != nil {
			return fmt.Errorf("dynamo store for %s: %w", s.name, err)
		}
		cat := store.NewCatalog()
		regions = append(regions, replicator.Region{Name: s.name, Clients: c})
		catalogs = append(catalogs, regionCatalog{s.name, cat})
		if conflictViews {
			a, err := newConflictApplier(cfg, s.name, c, store, cat)
			if err != nil {
				return err
			}
			appliers[s.name] = a
		}
	}
	var onConflict replicator.ConflictHook
	if conflictViews {
		onConflict = func(ctx context.Context, region, table, seq string, image map[string]ddbtypes.AttributeValue) error {
			return appliers[region].Apply(ctx, table, seq, image)
		}
	}

	r, err := replicator.New(replicator.Config{
		Regions:           regions,
		Prefix:            cfg.TablePrefix,
		Tables:            globalTables(catalogs, versionedOnly),
		SkipCatalog:       versionedOnly,
		Delay:             delay,
		Owner:             owner,
		PollInterval:      cfg.StreamPollInterval,
		LeaseDuration:     lease,
		DiscoveryInterval: cfg.CatalogRefresh,
		// Table discovery keeps the CATALOG_REFRESH cadence; each pipe's
		// DescribeStream polling is floored (replicator.MinStreamDiscoveryInterval).
		StreamDiscoveryInterval: streamDiscovery,
		OnConflict:              onConflict,
	})
	if err != nil {
		return err
	}
	if statsEvery > 0 {
		go logStats(ctx, r, statsEvery)
	}
	names := make([]string, len(specs))
	for i, s := range specs {
		names[i] = s.name + "=" + s.endpoint
	}
	slog.Info("replicator started", "regions", strings.Join(names, ","), "prefix", cfg.TablePrefix, "delay", delay,
		"versioned_only", versionedOnly)
	err = r.Run(ctx)
	slog.Info("replicator stopped", "stats", fmt.Sprintf("%+v", r.Stats()))
	return err
}

// validateLambda checks the configuration of Lambda mode: GLOBAL_MODE=aws
// (the poller's REGIONS list and the emulated catalog copy have no Lambda
// equivalent) with the usual aws-mode checks (a real REGION, no
// DYNAMODB_ENDPOINT, at least one REPLICA_REGIONS entry).
func validateLambda(cfg config.Config) error {
	if cfg.GlobalMode != config.GlobalAWS {
		return fmt.Errorf("replicator: Lambda mode requires GLOBAL_MODE=aws, got %q", cfg.GlobalMode)
	}
	return cfg.Validate()
}

// runLambda serves this region's ReplacingMergeTree streams as a Lambda
// function: rows written in REGION are copied to every REPLICA_REGIONS
// region with the replicator's conditions; clients, catalogs and the
// conflict-view engines are built once per execution environment.
func runLambda(cfg config.Config) error {
	if err := validateLambda(cfg); err != nil {
		return err
	}
	conflictViews, err := envBool("APPLY_CONFLICT_VIEWS", true)
	if err != nil {
		return err
	}
	if os.Getenv("REGIONS") != "" {
		slog.Warn("REGIONS is ignored in Lambda mode; using REGION and REPLICA_REGIONS")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// This region's catalog resolves the streams' tables and their version
	// columns (the catalog is a real global table in this mode).
	local, err := awsx.New(ctx, cfg.Region, "")
	if err != nil {
		return fmt.Errorf("aws clients for %s: %w", cfg.Region, err)
	}
	store, err := dynamo.New(local, dynamo.Options{Prefix: cfg.TablePrefix, Region: cfg.Region, GlobalMode: config.GlobalEmulated})
	if err != nil {
		return fmt.Errorf("dynamo store for %s: %w", cfg.Region, err)
	}
	lookup := replicator.CatalogLookup(store.NewCatalog(), cfg.CatalogRefresh, slog.Default())

	var targets []replicator.Target
	appliers := map[string]*mvworker.ImageApplier{}
	for _, name := range cfg.ReplicaRegions {
		c, err := awsx.New(ctx, name, "")
		if err != nil {
			return fmt.Errorf("aws clients for %s: %w", name, err)
		}
		targets = append(targets, replicator.Target{Name: name, Client: c.DynamoDB})
		if conflictViews {
			st, err := dynamo.New(c, dynamo.Options{Prefix: cfg.TablePrefix, Region: name, GlobalMode: config.GlobalEmulated})
			if err != nil {
				return fmt.Errorf("dynamo store for %s: %w", name, err)
			}
			a, err := newConflictApplier(cfg, name, c, st, st.NewCatalog())
			if err != nil {
				return err
			}
			appliers[name] = a
		}
	}
	var onConflict replicator.ConflictHook
	if conflictViews {
		onConflict = func(ctx context.Context, region, table, seq string, image map[string]ddbtypes.AttributeValue) error {
			return appliers[region].Apply(ctx, table, seq, image)
		}
	}
	l, err := replicator.NewLambda(replicator.LambdaConfig{
		Region: cfg.Region, Targets: targets, Tables: lookup, OnConflict: onConflict,
		Logger: slog.Default().With("from", cfg.Region),
	})
	if err != nil {
		return err
	}
	slog.Info("replicator lambda ready", "region", cfg.Region, "targets", strings.Join(cfg.ReplicaRegions, ","),
		"prefix", cfg.TablePrefix, "conflict_views", conflictViews)
	lambda.StartWithOptions(l.Handle, lambda.WithContext(ctx))
	return nil
}

// newConflictApplier builds the engine that applies materialized views, in
// region, to replicated row versions that region rejected as superseded.
func newConflictApplier(cfg config.Config, region string, c *awsx.Clients, store *dynamo.Store, cat catalog.Catalog) (*mvworker.ImageApplier, error) {
	engine, err := exec.New(exec.Config{
		Store:           store,
		Catalog:         cat,
		Region:          region,
		InlineViews:     false,
		ConsistentReads: cfg.ConsistentReads,
		ServerVersion:   cfg.ServerVersion,
		Timezone:        cfg.Timezone,
		PhysicalName:    store.PhysicalName,
		MultiRegion:     true,
	})
	if err != nil {
		return nil, fmt.Errorf("engine for %s: %w", region, err)
	}
	a, err := mvworker.NewImageApplier(mvworker.ImageApplierConfig{
		Store: store, Catalog: cat, Engine: engine, DeadLetters: c.DynamoDB,
		Logger: slog.Default().With("component", "conflict-views", "region", region),
	})
	if err != nil {
		return nil, fmt.Errorf("conflict view applier for %s: %w", region, err)
	}
	return a, nil
}

type regionCatalog struct {
	region string
	cat    catalog.Catalog
}

// globalTables lists the physical tables with global replication, read from
// every region's catalog (the union covers catalog replication lag). With
// versionedOnly (GLOBAL_MODE=aws) it lists only ReplacingMergeTree tables
// (catalog.CopiedByReplicator), the ones real global tables do not
// replicate. It fails only if no region's catalog can be read.
func globalTables(cats []regionCatalog, versionedOnly bool) replicator.TableSource {
	return func(ctx context.Context) ([]replicator.TableSpec, error) {
		set := map[string]string{} // physical name -> version column
		var errs []error
		for _, rc := range cats {
			if err := rc.cat.Refresh(ctx); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", rc.region, err))
				continue
			}
			for _, db := range rc.cat.Databases() {
				for _, t := range rc.cat.Tables(db.Name) {
					if t.Replication != catalog.ReplicationGlobal || t.PhysicalName == "" {
						continue
					}
					if versionedOnly && !catalog.CopiedByReplicator(t) {
						continue
					}
					ver := ""
					if t.Engine == catalog.EngineReplacingMergeTree {
						ver = t.VersionColumn
					}
					if _, seen := set[t.PhysicalName]; !seen || ver != "" {
						set[t.PhysicalName] = ver
					}
				}
			}
		}
		if len(errs) == len(cats) {
			return nil, errors.Join(errs...)
		}
		out := make([]replicator.TableSpec, 0, len(set))
		for name, ver := range set {
			out = append(out, replicator.TableSpec{Name: name, VersionColumn: ver})
		}
		slices.SortFunc(out, func(a, b replicator.TableSpec) int { return strings.Compare(a.Name, b.Name) })
		return out, nil
	}
}

func logStats(ctx context.Context, r *replicator.Replicator, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s := r.Stats()
			slog.Info("replicator progress", "put", s.Put, "deleted", s.Deleted, "skipped_origin", s.SkippedOrigin,
				"skipped_conflict", s.SkippedConflict, "conflict_views", s.ConflictViews, "skipped_invalid", s.SkippedInvalid,
				"errors", s.Errors, "pipes", s.Pipes)
		}
	}
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}

func envBool(k string, def bool) (bool, error) {
	switch strings.ToLower(os.Getenv(k)) {
	case "":
		return def, nil
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	}
	return false, fmt.Errorf("%s: want true or false, got %q", k, os.Getenv(k))
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
