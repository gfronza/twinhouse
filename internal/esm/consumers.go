package esm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/config"
)

// ConsumersConfig configures Consumers.
type ConsumersConfig struct {
	Manager *Manager
	// MVWorkerARN is the stream-worker Lambda (MVWORKER_FUNCTION_ARN); it
	// consumes the stream of every view-source table (MergeTree,
	// ReplacingMergeTree, Memory, Log). Required.
	MVWorkerARN string
	// ReplicatorARN is the replicator Lambda (REPLICATOR_FUNCTION_ARN); with
	// GlobalMode aws it also consumes the stream of every global
	// ReplacingMergeTree table (catalog.CopiedByReplicator: those are not
	// AWS global tables). "" with such tables is logged once per table.
	ReplicatorARN string
	// GlobalMode is config.GlobalNone | GlobalAWS | GlobalEmulated.
	GlobalMode string
	// Options are the settings of every mapping.
	Options Options
	Logger  *slog.Logger
}

// Consumers implements the reconciler's StreamConsumers hook with Lambda
// event-source mappings: it decides from a table's engine, replication and
// the deployment's GLOBAL_MODE which functions consume its stream.
type Consumers struct {
	cfg ConsumersConfig
	log *slog.Logger

	mu     sync.Mutex
	warned map[string]bool
}

// NewConsumers creates Consumers.
func NewConsumers(cfg ConsumersConfig) (*Consumers, error) {
	if cfg.Manager == nil {
		return nil, errors.New("esm: Manager is required")
	}
	if cfg.MVWorkerARN == "" {
		return nil, errors.New("esm: MVWorkerARN is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Consumers{cfg: cfg, log: cfg.Logger.With("component", "esm"), warned: map[string]bool{}}, nil
}

// Targets returns the functions that consume t's stream: the stream
// worker for every table materialized views can read from (not rollups:
// Aggregating/Summing tables cannot be view sources) and, with GLOBAL_MODE
// aws, the replicator for every global ReplacingMergeTree table.
func (c *Consumers) Targets(t *catalog.Table) []string {
	if t == nil {
		return nil
	}
	var out []string
	switch t.Engine {
	case catalog.EngineAggregatingMergeTree, catalog.EngineSummingMergeTree:
	default:
		out = append(out, c.cfg.MVWorkerARN)
	}
	if c.replicated(t) {
		if c.cfg.ReplicatorARN != "" {
			out = append(out, c.cfg.ReplicatorARN)
		} else {
			c.warnOnce(t.PhysicalName, "GLOBAL_MODE=aws without REPLICATOR_FUNCTION_ARN: this ReplacingMergeTree table is not replicated to the other regions",
				"table", t.FullName(), "physical", t.PhysicalName)
		}
	}
	return out
}

// replicated reports a table cmd/replicator copies between regions with
// GLOBAL_MODE=aws (a global ReplacingMergeTree table).
func (c *Consumers) replicated(t *catalog.Table) bool {
	if c.cfg.GlobalMode != config.GlobalAWS || !catalog.CopiedByReplicator(t) {
		return false
	}
	repl := t.Replication
	if repl == "" {
		repl = catalog.DefaultReplication(t.Engine)
	}
	return repl == catalog.ReplicationGlobal
}

// EnsureConsumers wires streamARN (t's stream) to every target of t.
func (c *Consumers) EnsureConsumers(ctx context.Context, t *catalog.Table, streamARN string) error {
	if streamARN == "" {
		return nil
	}
	var errs []error
	for _, fn := range c.Targets(t) {
		if err := c.cfg.Manager.Ensure(ctx, streamARN, fn, c.cfg.Options); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", fn, err))
		}
	}
	return errors.Join(errs...)
}

// RemoveConsumers removes the mappings of the table physicalName from every
// function that may consume it (the stream worker and, when configured,
// the replicator, whatever the table's engine was): by stream ARN when it
// is known, else by table name (the table may already be gone).
func (c *Consumers) RemoveConsumers(ctx context.Context, physicalName, streamARN string) error {
	fns := []string{c.cfg.MVWorkerARN}
	if c.cfg.ReplicatorARN != "" {
		fns = append(fns, c.cfg.ReplicatorARN)
	}
	var errs []error
	for _, fn := range fns {
		var err error
		if streamARN != "" {
			err = c.cfg.Manager.Remove(ctx, streamARN, fn)
		} else {
			err = c.cfg.Manager.RemoveByTable(ctx, physicalName, fn)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", fn, err))
		}
	}
	return errors.Join(errs...)
}

// ConsumedTables lists the physical tables whose streams are consumed by
// the stream worker or the replicator (one ListEventSourceMappings per
// function), the reconciler's server.ConsumedTables capability: the
// consumers of a table that vanished from the catalog while no reconciler
// observed it are removed through RemoveConsumers like any other drop.
func (c *Consumers) ConsumedTables(ctx context.Context) ([]string, error) {
	fns := []string{c.cfg.MVWorkerARN}
	if c.cfg.ReplicatorARN != "" {
		fns = append(fns, c.cfg.ReplicatorARN)
	}
	seen := map[string]bool{}
	var out []string
	for _, fn := range fns {
		mappings, err := c.cfg.Manager.list(ctx, "", fn)
		if err != nil {
			return nil, err
		}
		for _, m := range mappings {
			name := TableOfStreamARN(aws.ToString(m.EventSourceArn))
			if name == "" || seen[name] {
				continue
			}
			seen[name] = true
			out = append(out, name)
		}
	}
	return out, nil
}

func (c *Consumers) warnOnce(key, msg string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.warned[key] {
		return
	}
	c.warned[key] = true
	c.log.Warn(msg, args...)
}
