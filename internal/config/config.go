// Package config loads process configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Global-table modes.
const (
	// GlobalNone: single region, no replication.
	GlobalNone = "none"
	// GlobalAWS: global tables are real DynamoDB global tables; replicas are
	// added with UpdateTable(ReplicaUpdates) for every region in ReplicaRegions.
	GlobalAWS = "aws"
	// GlobalEmulated: local development. Every region creates its own copy of
	// every table and cmd/replicator copies items of global tables between
	// regions, emulating DynamoDB global tables (last-writer-wins).
	GlobalEmulated = "emulated"
)

// Config is shared by all binaries; each uses the fields it needs.
type Config struct {
	// Region is the logical (and AWS) region name of this process.
	Region string
	// DynamoDBEndpoint overrides the DynamoDB endpoint (DynamoDB Local).
	DynamoDBEndpoint string
	// TablePrefix prefixes every physical DynamoDB table name.
	TablePrefix string
	// GlobalMode is GlobalNone | GlobalAWS | GlobalEmulated.
	GlobalMode string
	// ReplicaRegions are the other regions of a global deployment.
	ReplicaRegions []string
	// HomeRegion (GLOBAL_MODE=aws) is the region that creates the global
	// system tables and the global tables whose catalog entry names no home
	// region; "" picks the first of the sorted REGION + REPLICA_REGIONS,
	// which every region computes the same way.
	HomeRegion string

	// ListenAddr is the HTTP listen address of the ClickHouse-compatible server.
	ListenAddr string
	// Users maps user -> password ("" allowed). Default: {"default": ""}.
	Users map[string]string
	// InlineViews applies materialized views during INSERT (read-your-writes).
	InlineViews bool
	// StreamViews declares that a stream worker applies this deployment's
	// materialized views (cmd/mvworker, RUN_STREAM_WORKER or a Lambda, one
	// per region): an inline view application that fails after the source
	// rows are stored is then logged and the INSERT succeeds (the worker
	// applies it). false: such an INSERT fails (source rows stay written).
	StreamViews bool
	// ConsistentReads uses strongly consistent reads by default.
	ConsistentReads bool
	// CatalogRefresh is how often the catalog snapshot is reloaded.
	CatalogRefresh time.Duration
	// Timezone reported by timezone() and used to render DateTime.
	Timezone string
	// ServerVersion reported to clients.
	ServerVersion string
	// LogLevel: debug|info|warn|error.
	LogLevel string

	// StreamPollInterval is the stream worker / replicator idle poll interval.
	StreamPollInterval time.Duration

	// RunStreamWorker makes chserver run the materialized-view stream worker
	// in-process (instead of, or in addition to, cmd/mvworker).
	RunStreamWorker bool
	// DisplayName is reported by displayName() and the
	// X-ClickHouse-Server-Display-Name header.
	DisplayName string
	// MaxExecutionTime is chserver's default max_execution_time for every
	// query (0: unlimited); a query's own setting overrides it.
	MaxExecutionTime time.Duration
}

// Load reads configuration from the environment.
//
//	REGION                 (default: AWS_REGION, then AWS_DEFAULT_REGION, then "local")
//	DYNAMODB_ENDPOINT      (default "", i.e. AWS)
//	TABLE_PREFIX           (default "chddb_")
//	GLOBAL_MODE            none|aws|emulated (default none)
//	REPLICA_REGIONS        comma list
//	GLOBAL_HOME_REGION     GLOBAL_MODE=aws: region creating global tables (default:
//	                       first of the sorted REGION + REPLICA_REGIONS)
//	LISTEN_ADDR            (default ":8123")
//	CH_USERS               "user:password,user2:pw" (default "default:")
//	INLINE_VIEWS           (default true)
//	STREAM_VIEWS           chserver only: a stream worker applies the views (default true)
//	CONSISTENT_READS       (default true)
//	CATALOG_REFRESH        Go duration (default 2s)
//	TZ_NAME                (default "UTC")
//	SERVER_VERSION         (default "25.8.1.1")
//	LOG_LEVEL              (default info)
//	STREAM_POLL_INTERVAL   Go duration (default 250ms)
//	RUN_STREAM_WORKER      chserver only: run the stream worker in-process (default false)
//	DISPLAY_NAME           chserver only: displayName() (default: hostname, suffixed
//	                       with "-<REGION>" unless REGION is "local" or empty)
//	MAX_EXECUTION_TIME     chserver only: default max_execution_time, seconds or a Go
//	                       duration (default 0 = unlimited)
//
// Load only checks values in isolation; processes that talk to DynamoDB
// through REGION/DYNAMODB_ENDPOINT also call Validate.
func Load() (Config, error) {
	c := Config{
		Region:           defaultRegion(),
		DynamoDBEndpoint: env("DYNAMODB_ENDPOINT", ""),
		TablePrefix:      env("TABLE_PREFIX", "chddb_"),
		GlobalMode:       env("GLOBAL_MODE", GlobalNone),
		HomeRegion:       strings.TrimSpace(env("GLOBAL_HOME_REGION", "")),
		ListenAddr:       env("LISTEN_ADDR", ":8123"),
		Timezone:         env("TZ_NAME", "UTC"),
		ServerVersion:    env("SERVER_VERSION", "25.8.1.1"),
		LogLevel:         env("LOG_LEVEL", "info"),
		Users:            map[string]string{},
	}
	for _, r := range strings.Split(env("REPLICA_REGIONS", ""), ",") {
		if r = strings.TrimSpace(r); r != "" && r != c.Region {
			c.ReplicaRegions = append(c.ReplicaRegions, r)
		}
	}
	for _, up := range strings.Split(env("CH_USERS", "default:"), ",") {
		up = strings.TrimSpace(up)
		if up == "" {
			continue
		}
		u, p, _ := strings.Cut(up, ":")
		c.Users[u] = p
	}
	var err error
	if c.InlineViews, err = envBool("INLINE_VIEWS", true); err != nil {
		return c, err
	}
	if c.StreamViews, err = envBool("STREAM_VIEWS", true); err != nil {
		return c, err
	}
	if c.ConsistentReads, err = envBool("CONSISTENT_READS", true); err != nil {
		return c, err
	}
	if c.CatalogRefresh, err = envDuration("CATALOG_REFRESH", 2*time.Second); err != nil {
		return c, err
	}
	if c.StreamPollInterval, err = envDuration("STREAM_POLL_INTERVAL", 250*time.Millisecond); err != nil {
		return c, err
	}
	if c.RunStreamWorker, err = envBool("RUN_STREAM_WORKER", false); err != nil {
		return c, err
	}
	if c.MaxExecutionTime, err = envSeconds("MAX_EXECUTION_TIME"); err != nil {
		return c, err
	}
	if c.DisplayName = os.Getenv("DISPLAY_NAME"); c.DisplayName == "" {
		c.DisplayName = defaultDisplayName(c.Region)
	}
	switch c.GlobalMode {
	case GlobalNone, GlobalAWS, GlobalEmulated:
	default:
		return c, fmt.Errorf("config: GLOBAL_MODE must be none, aws or emulated, got %q", c.GlobalMode)
	}
	return c, nil
}

// Validate checks the settings a process that reaches DynamoDB through
// REGION and DYNAMODB_ENDPOINT needs (chserver, mvworker):
//
//   - a real AWS region when DYNAMODB_ENDPOINT is empty (the SDK resolves
//     region "local" to http://localhost:8000, so a forgotten REGION would
//     otherwise retry against a non-existent endpoint forever);
//   - GLOBAL_MODE=aws runs against real AWS (no DYNAMODB_ENDPOINT, whose
//     dummy credentials are for DynamoDB Local) and names at least one other
//     region in REPLICA_REGIONS (otherwise global tables silently stay
//     regional).
func (c Config) Validate() error {
	if c.DynamoDBEndpoint == "" && (c.Region == "" || c.Region == "local") {
		return fmt.Errorf("config: REGION (or AWS_REGION) must name an AWS region when DYNAMODB_ENDPOINT is empty, got %q", c.Region)
	}
	if c.GlobalMode == GlobalAWS {
		if c.DynamoDBEndpoint != "" {
			return fmt.Errorf("config: GLOBAL_MODE=aws uses real DynamoDB global tables; DYNAMODB_ENDPOINT (%s) is for DynamoDB Local (use GLOBAL_MODE=emulated)", c.DynamoDBEndpoint)
		}
		if len(c.ReplicaRegions) == 0 {
			return fmt.Errorf("config: GLOBAL_MODE=aws needs REPLICA_REGIONS with at least one region other than REGION (%s)", c.Region)
		}
	}
	if c.HomeRegion != "" && c.HomeRegion != c.Region && !slices.Contains(c.ReplicaRegions, c.HomeRegion) {
		return fmt.Errorf("config: GLOBAL_HOME_REGION (%s) must be REGION (%s) or one of REPLICA_REGIONS (%s)",
			c.HomeRegion, c.Region, strings.Join(c.ReplicaRegions, ","))
	}
	return nil
}

// defaultRegion is REGION, else the AWS_REGION / AWS_DEFAULT_REGION the AWS
// runtime (ECS, Lambda) injects, else "local".
func defaultRegion() string {
	for _, k := range []string{"REGION", "AWS_REGION", "AWS_DEFAULT_REGION"} {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return "local"
}

func env(k, def string) string {
	if v, ok := os.LookupEnv(k); ok {
		return v
	}
	return def
}

func envBool(k string, def bool) (bool, error) {
	v, ok := os.LookupEnv(k)
	if !ok || v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def, fmt.Errorf("config: %s: %w", k, err)
	}
	return b, nil
}

func envDuration(k string, def time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(k)
	if !ok || v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def, fmt.Errorf("config: %s: %w", k, err)
	}
	return d, nil
}

// envSeconds reads a duration given as seconds ("1.5") or a Go duration
// ("1500ms"); unset or empty is 0.
func envSeconds(k string) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return 0, nil
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		if f < 0 || f > float64(1<<62)/float64(time.Second) {
			return 0, fmt.Errorf("config: %s: out of range: %s", k, v)
		}
		return time.Duration(f * float64(time.Second)), nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("config: %s: want seconds or a Go duration, got %q", k, v)
	}
	return d, nil
}

// defaultDisplayName is the hostname, suffixed with "-<region>" unless the
// region is "local" (or empty).
func defaultDisplayName(region string) string {
	host, err := hostname()
	if err != nil || host == "" {
		host = "chserver"
	}
	if region != "" && region != "local" {
		host += "-" + region
	}
	return host
}

// hostname is os.Hostname; tests replace it.
var hostname = os.Hostname
