// Command chbench runs the same workload against twinhouse and ClickHouse
// and prints the comparison (docs/clickhouse-comparison.md). It expects the
// stack of `make chbench-up`; `make chbench` runs it with its endpoints.
//
// It is a measurement, not a test: it reports numbers and fails only when
// a request fails or the two servers return different results.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"
)

type config struct {
	rows, mvRows, smallInserts int
	rounds, warmup, conc       int
	seed                       uint64
	only                       map[string]bool
	skipLoad                   bool
	align                      bool
	eventsKey                  string
	setupOnly, skipSetup       bool
}

// wants reports whether step id (W1…W3, R1…R6) runs.
func (c config) wants(id string) bool { return len(c.only) == 0 || c.only[id] }

// wantsReads reports whether any read runs; reads need W1 and W2's data.
func (c config) wantsReads() bool {
	for _, r := range reads {
		if c.wants(r.id) {
			return true
		}
	}
	return false
}

func main() {
	var (
		thURL   = flag.String("twinhouse", env("CHBENCH_TWINHOUSE", "http://localhost:38125"), "twinhouse HTTP endpoint; http://user:password@host:port logs in (default from $CHBENCH_TWINHOUSE)")
		chURL   = flag.String("clickhouse", env("CHBENCH_CLICKHOUSE", "http://localhost:38123"), "ClickHouse HTTP endpoint, as -twinhouse (default from $CHBENCH_CLICKHOUSE)")
		db      = flag.String("db", "chbench", "database the benchmark creates (dropped first)")
		size    = flag.String("size", "S", "events to load: S (100k), M (300k) or a number")
		mvRows  = flag.Int("mv-rows", 200_000, "events loaded through the materialized view (W2), at most -size")
		small   = flag.Int("small-inserts", 2000, "single-row INSERTs per server (W3)")
		rounds  = flag.Int("rounds", 20, "timed rounds per read; a quarter (at least 5) when a warm-up run takes over 2s")
		warmup  = flag.Int("warmup", 3, "untimed rounds per read")
		conc    = flag.Int("conc", 8, "concurrent clients for W3 and the concurrent reads")
		seed    = flag.Uint64("seed", 1, "seed of the data and the query parameters")
		only    = flag.String("only", "", "comma-separated steps to run (W1,W2,W3,R1…R6); reads load the data they need")
		skip    = flag.Bool("skip-load", false, "reuse the data of a previous run (same -size and -seed): run only W3 and the reads")
		timeout = flag.Duration("timeout", 120*time.Second, "max_execution_time sent with every request")
		project = flag.String("compose-project", "twinhouse-bench", "compose project of the stack, for the idle wait and the run context")
		settleD = flag.Duration("settle", 0, "without Docker (a deployed stack): after a load, wait until ClickHouse has no merges and at least this long, instead of watching the containers' CPU")
		evKey   = flag.String("events-key", "tenant_id, ts", "sorting key of events (and its copies); another key turns the tenant reads into scans, so it needs -only with writes only")
		setOnly = flag.Bool("setup-only", false, "drop and create the schema, then stop (to change the tables before loading, e.g. their DynamoDB capacity)")
		noSetup = flag.Bool("skip-setup", false, "load into the tables of a previous -setup-only instead of recreating them")
		align   = flag.Bool("align-steps", false, "start each step on a minute boundary, so CloudWatch's per-minute DynamoDB capacity can be split by step")
		out     = flag.String("out", "", "raw results as JSON (default bin/chbench-<size>-<time>.json)")
	)
	flag.Parse()

	cfg := config{mvRows: *mvRows, smallInserts: *small, rounds: *rounds, warmup: *warmup, conc: *conc, seed: *seed, skipLoad: *skip, align: *align, eventsKey: strings.TrimSpace(*evKey), setupOnly: *setOnly, skipSetup: *noSetup}
	switch strings.ToUpper(*size) {
	case "S":
		cfg.rows = 100_000
	case "M":
		// Not 1M: DynamoDB Local's Scan slows with table size per page, so
		// a full scan of 1M events takes minutes (docs/clickhouse-comparison.md).
		cfg.rows = 300_000
	default:
		if _, err := fmt.Sscan(*size, &cfg.rows); err != nil || cfg.rows <= 0 {
			log.Fatalf("-size %q: want S, M or a positive number", *size)
		}
	}
	if *only != "" {
		cfg.only = map[string]bool{}
		for _, id := range strings.Split(*only, ",") {
			cfg.only[strings.ToUpper(strings.TrimSpace(id))] = true
		}
	}
	if "("+cfg.eventsKey+")" != defaultEventsKey && cfg.wantsReads() {
		log.Fatalf("-events-key %q: the reads assume (tenant_id, ts); add -only with writes only (W1,W2,W3)", cfg.eventsKey)
	}
	if cfg.setupOnly && (cfg.skipSetup || cfg.skipLoad) {
		log.Fatal("-setup-only cannot be combined with -skip-setup or -skip-load")
	}
	if *out == "" {
		*out = fmt.Sprintf("bin/chbench-%s-%s.json", strings.ToUpper(*size), time.Now().Format("20060102-150405"))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	logf := func(format string, args ...any) { log.Printf(format, args...) }
	b := &bench{
		th:      newServer("twinhouse", *thURL, *db, *timeout),
		ch:      newServer("clickhouse", *chURL, *db, *timeout),
		cfg:     cfg,
		logf:    logf,
		results: &results{},
	}
	var names []string
	if *settleD > 0 {
		b.quiet = &settle{ch: b.ch, min: *settleD, logf: logf}
	} else {
		names = []string{*project + "-chserver-single-1", *project + "-dynamodb-single-1", *project + "-clickhouse-1"}
		b.quiet = &quiescence{containers: names, maxCPU: 10, limit: 30 * time.Minute, logf: logf}
	}
	if cfg.setupOnly {
		if err := b.setup(ctx); err != nil {
			log.Fatal(err)
		}
		logf("schema created in database %s on both servers", *db)
		return
	}
	if err := run(ctx, b, names); err != nil {
		log.Fatal(err)
	}
	if err := b.results.writeJSON(*out); err != nil {
		log.Fatal(err)
	}
	b.results.markdown(os.Stdout)
	logf("raw results: %s", *out)
}

func run(ctx context.Context, b *bench, names []string) error {
	start := time.Now()
	rc := &b.results.Context
	rc.Date = start.UTC().Format("2006-01-02")
	rc.Commit = commit()
	rc.Host = hostDesc()
	rc.Rows, rc.Seed = b.cfg.rows, b.cfg.seed
	if "("+b.cfg.eventsKey+")" != defaultEventsKey {
		rc.EventsKey = b.cfg.eventsKey
	}
	rc.LoadStart = loadAvg()
	if len(names) > 0 {
		rc.Containers = inspect(ctx, names)
	}
	for _, s := range b.sides() {
		d := *s
		d.db = "default"
		v, err := d.text(ctx, "SELECT version()")
		if err != nil {
			return err
		}
		if s == b.th {
			rc.Twinhouse = v
		} else {
			rc.ClickHouse = v
		}
	}

	if b.cfg.skipLoad {
		if b.cfg.wants("W3") {
			if err := b.smallInserts(ctx); err != nil {
				return err
			}
		}
	} else {
		if !b.cfg.skipSetup {
			if err := b.setup(ctx); err != nil {
				return err
			}
		}
		if err := b.writes(ctx); err != nil {
			return err
		}
	}
	if b.cfg.wantsReads() {
		b.logf("waiting for both servers to go idle")
		_, _ = b.quiet.wait(ctx)
		b.logf("checking that both servers return the same results")
		done := b.step(ctx, "check")
		if err := b.gate(ctx); err != nil {
			return err
		}
		done()
		if err := b.readsPhase(ctx); err != nil {
			return err
		}
	}
	rc.LoadEnd = loadAvg()
	rc.Duration = time.Since(start).Round(time.Second).String()
	return nil
}

// env returns the environment variable k, or def when it is unset. The
// endpoints can carry a password, which stays out of shell history and the
// process list this way.
func env(k, def string) string {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		return v
	}
	return def
}
