// Package dynamo is the DynamoDB implementation of storage.Store and of the
// catalog backing store. See DESIGN.md §4 for the physical layout.
//
// API CONTRACT (stubs until implemented by the storage owner).
package dynamo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"dynamodb-eventstream/internal/awsx"
	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/storage"
)

// Options configures the store.
type Options struct {
	// Prefix is prepended to every physical table name (config.TablePrefix).
	Prefix string
	// Region is the local region name, written to _origin.
	Region string
	// GlobalMode is config.GlobalNone | GlobalAWS | GlobalEmulated.
	GlobalMode string
	// ReplicaRegions are added as global-table replicas when GlobalMode == aws.
	ReplicaRegions []string
	// HomeRegion (GlobalMode == aws) is the region that creates __catalog
	// and the global tables whose catalog entry names no home region; the
	// other regions wait for their replicas. Default: see homeRegion.
	HomeRegion string
	// ScanSegments is the parallel Scan segment count (default 4).
	ScanSegments int
	// MarkerTTLDays is the exactly-once marker retention (default 7).
	MarkerTTLDays int
	// AsyncDDL makes EnsureTable and DropTable return without waiting for
	// the slow parts of a DDL, for a process whose requests have a short
	// budget (the API Lambda behind API Gateway, 29 s): EnsureTable creates
	// the table and waits for it to become ACTIVE but leaves adding the
	// global-table replicas (minutes) to the reconciler of the home region,
	// and DropTable issues the deletion of a table without replicas but
	// does not wait for it and leaves a table that still has replicas to
	// the reconciler (which drops physical tables whose catalog entry is
	// gone). CheckSystemTables is the matching replacement for
	// EnsureSystemTables.
	AsyncDDL bool
}

// Store implements storage.Store on DynamoDB.
type Store struct {
	impl *storeImpl
}

var _ storage.Store = (*Store)(nil)

// New creates a store using the given clients.
func New(c *awsx.Clients, opts Options) (*Store, error) { return newStore(c, opts) }

// EnsureSystemTables creates __catalog, __mv_markers and __checkpoints if missing.
func (s *Store) EnsureSystemTables(ctx context.Context) error { return s.ensureSystemTables(ctx) }

// CheckSystemTables reports whether the system tables exist and accept
// item operations (one DescribeTable each; nothing is created, updated or
// awaited). A missing or still CREATING table is cherr.Unavailable: the
// process that converges them (chserver, cmd/reconciler-lambda) has not
// run yet.
func (s *Store) CheckSystemTables(ctx context.Context) error { return s.checkSystemTables(ctx) }

// ListPhysicalTables lists this region's row tables with the deployment
// prefix (system tables {prefix}__* and ".members" side tables excluded),
// paginating ListTables. The reconciler uses it to find physical tables
// whose catalog entry is gone (server.PhysicalTables).
func (s *Store) ListPhysicalTables(ctx context.Context) ([]string, error) {
	return s.impl.listPhysicalTables(ctx)
}

// DropPhysicalTable drops the row table physical (its replicas first, in
// GLOBAL_MODE=aws) and its ".members" side table, like DropTable does for a
// catalog table (idempotent). The reconciler uses it for tables it found
// with ListPhysicalTables, whose catalog entry no longer exists.
func (s *Store) DropPhysicalTable(ctx context.Context, physical string) error {
	return s.dropPhysicalTable(ctx, physical)
}

// PhysicalName returns the DynamoDB table name for a ClickHouse table.
func (s *Store) PhysicalName(db, table string) string { return s.physicalName(db, table) }

// CatalogTableName / MarkersTableName / CheckpointsTableName return system table names.
func (s *Store) CatalogTableName() string     { return s.catalogTableName() }
func (s *Store) MarkersTableName() string     { return s.markersTableName() }
func (s *Store) CheckpointsTableName() string { return s.checkpointsTableName() }

// NewCatalog returns a catalog.Catalog persisted in the __catalog table.
func (s *Store) NewCatalog() catalog.Catalog { return s.newCatalog() }

// DecodeRecord decodes a stream record image (already converted to
// dynamodb types.AttributeValue) of table t. eventName is INSERT|MODIFY|REMOVE.
// Returns (nil, nil) for items that are not table rows (should not happen
// for row tables, but side items are skipped this way).
func (s *Store) DecodeRecord(t *catalog.Table, eventName string, image map[string]types.AttributeValue) (*storage.StreamRecord, error) {
	return s.decodeRecord(t, eventName, image)
}

// StreamARN returns the latest stream ARN of a physical table ("" if streams
// are disabled).
func (s *Store) StreamARN(ctx context.Context, physical string) (string, error) {
	return s.streamARN(ctx, physical)
}

// Interface methods (storage.Store).

func (s *Store) EnsureTable(ctx context.Context, t *catalog.Table) error {
	return s.ensureTable(ctx, t)
}
func (s *Store) DropTable(ctx context.Context, t *catalog.Table) error { return s.dropTable(ctx, t) }
func (s *Store) TruncateTable(ctx context.Context, t *catalog.Table) error {
	return s.truncateTable(ctx, t)
}
func (s *Store) Insert(ctx context.Context, t *catalog.Table, rows []storage.Row) (*storage.InsertResult, error) {
	return s.insert(ctx, t, rows)
}
func (s *Store) Merge(ctx context.Context, t *catalog.Table, rows []storage.MergeRow) error {
	return s.merge(ctx, t, rows)
}
func (s *Store) Scan(ctx context.Context, t *catalog.Table, req storage.ScanRequest) (storage.RowIterator, error) {
	return s.scan(ctx, t, req)
}
func (s *Store) DeleteRows(ctx context.Context, t *catalog.Table, rows []storage.Row) error {
	return s.deleteRows(ctx, t, rows)
}
