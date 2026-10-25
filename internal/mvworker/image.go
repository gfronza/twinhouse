package mvworker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/exec"
	"dynamodb-eventstream/internal/storage/dynamo"
)

// ImageApplierConfig configures an ImageApplier (one per region).
type ImageApplierConfig struct {
	Store   *dynamo.Store
	Catalog catalog.Catalog
	Engine  *exec.Engine
	Logger  *slog.Logger
	// DeadLetters, PoisonPolicy, PoisonAfter and CatalogMinReload are as in
	// LambdaConfig.
	DeadLetters      DeadLetterAPI
	PoisonPolicy     string
	PoisonAfter      time.Duration
	CatalogMinReload time.Duration
}

// ImageApplier applies a source table's materialized views to row images
// that will never appear in the region's own stream: a row version that
// cmd/replicator did not write because the region already holds a newer
// version of the same key (a cross-region conflict of a
// ReplacingMergeTree source, DESIGN.md §1). The origin region applied its
// views to that version, so without this the regions' rollups diverge.
//
// It applies the views exactly like the stream worker would have (same row
// id, so exactly-once markers make repeated calls no-ops; same view
// eligibility by write time) and shares its poison handling.
type ImageApplier struct {
	p       *processor
	scratch scratchTracker
}

// NewImageApplier creates an ImageApplier.
func NewImageApplier(cfg ImageApplierConfig) (*ImageApplier, error) {
	if cfg.Store == nil || cfg.Catalog == nil || cfg.Engine == nil {
		return nil, errors.New("mvworker: Store, Catalog and Engine are required")
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	o := processorOptions{policy: cfg.PoisonPolicy, poisonAfter: cfg.PoisonAfter, minReload: cfg.CatalogMinReload, origin: "replicator"}
	if cfg.DeadLetters != nil {
		o.deadLetters, o.dlqTable = cfg.DeadLetters, DeadLetterTableName(cfg.Store.CheckpointsTableName())
	}
	p, err := newProcessor(cfg.Catalog, cfg.Store, cfg.Engine, log, o)
	if err != nil {
		return nil, err
	}
	return &ImageApplier{p: p}, nil
}

// Apply applies the views of physical's table to one row image (an
// INSERT/MODIFY new image, with its _rid and _ts). seq identifies the record
// for logs and dead letters. Tables without views, and MV output rows, are
// skipped. An error means the caller must retry.
func (a *ImageApplier) Apply(ctx context.Context, physical, seq string, image map[string]ddbtypes.AttributeValue) error {
	return a.apply(ctx, physical, seq, image)
}

func (a *ImageApplier) apply(ctx context.Context, physical, seq string, image map[string]ddbtypes.AttributeValue) error {
	// No shortcut for tables without views in the current snapshot: the
	// processor first reloads a catalog older than the row's write time, so
	// a view created just before the write is honoured (rejections are rare:
	// cross-region conflicts only, redeliveries are filtered by the caller).
	wt := writeTime(image, time.Time{})
	c := change{EventName: eventInsert, NewImage: image, Seq: seq, WriteTime: wt, Created: wt}
	key := physical + "|" + seq
	st := a.scratch.get(key)
	err := a.p.handle(ctx, physical, []change{c}, &st)
	if err != nil {
		a.scratch.set(key, st)
		return err
	}
	a.scratch.set(key, nil)
	return nil
}

// Stats returns the applier's counters (RecordsApplied, RecordsSkipped,
// Errors, DeadLettered, CatalogReloads).
func (a *ImageApplier) Stats() Stats {
	return Stats{
		RecordsApplied: a.p.applied.Load(),
		RecordsSkipped: a.p.skipped.Load(),
		Errors:         a.p.errs.Load(),
		DeadLettered:   a.p.deadLettered.Load(),
		CatalogReloads: a.p.reloads.Load(),
	}
}
