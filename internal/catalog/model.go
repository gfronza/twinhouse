// Package catalog holds schema metadata (databases, tables, views) and
// persists it in a DynamoDB catalog table that is replicated globally, so every
// region sees the same schema.
//
// MODEL CONTRACT: shared by the engine, storage, the stream worker and the
// replicator.
package catalog

import (
	"context"
	"time"

	"github.com/gfronza/twinhouse/internal/chtype"
)

// Engines supported by the storage layer.
const (
	EngineMergeTree            = "MergeTree"
	EngineReplacingMergeTree   = "ReplacingMergeTree"
	EngineAggregatingMergeTree = "AggregatingMergeTree"
	EngineSummingMergeTree     = "SummingMergeTree"
	// EngineMemory / EngineLog are accepted and mapped to MergeTree semantics
	// (rows are append-only, ORDER BY tuple()).
	EngineMemory = "Memory"
	EngineLog    = "Log"
)

// Replication modes.
const (
	// ReplicationGlobal: the DynamoDB table is a global table (active-active
	// replicated to every region). Default for MergeTree / ReplacingMergeTree.
	ReplicationGlobal = "global"
	// ReplicationRegional: the table exists independently in every region and
	// is never replicated. Default for AggregatingMergeTree / SummingMergeTree,
	// whose rows are merged in place: replicating them would let regions
	// overwrite each other's counters (last-writer-wins). Each region rebuilds
	// them from its own copy of the replicated source tables via materialized
	// views.
	ReplicationRegional = "regional"
)

// Column is a table column.
type Column struct {
	Name     string
	Type     chtype.Type
	TypeText string // canonical type text (Type.String())
	// DefaultKind is "", "DEFAULT", "MATERIALIZED", "ALIAS" or "EPHEMERAL".
	DefaultKind string
	// DefaultExpr is the default expression SQL text ("" if none). The engine
	// parses it with sqlparse.ParseExpr and evaluates it per inserted row.
	DefaultExpr string
	Comment     string
	// Codec is the column's compression codec list as ClickHouse prints it,
	// without the CODEC(...) wrapper (e.g. "Delta(8), ZSTD(1)"; "" if none).
	// It has no storage effect; DESCRIBE (codec_expression) and SHOW CREATE
	// TABLE report it. Fill it with NormalizeCodec.
	Codec string
	// PrevTypes lists the type texts the column had before ALTER TABLE
	// MODIFY COLUMN changed its type, oldest first (nil if it never
	// changed). MODIFY is catalog-only, so items written earlier still hold
	// values encoded for one of these types: the store decodes such a value
	// with the type it was written with and converts it like CAST (DESIGN.md
	// §4 "Reads"). UpdateTable maintains it; callers need not set it.
	PrevTypes []string
}

// Table is a ClickHouse table and its DynamoDB physical mapping.
type Table struct {
	Database string
	Name     string
	Columns  []Column

	Engine     string   // one of the Engine* constants
	EngineArgs []string // raw SQL text of engine args, e.g. ReplacingMergeTree(updated_at) -> ["updated_at"]

	// OrderBy are the sorting-key column names (plain column references only).
	OrderBy []string
	// PartitionKey is the leading part of OrderBy that becomes the DynamoDB
	// partition key (default: first OrderBy column; overridable with
	// SETTINGS dynamodb_partition_key = 'a, b'). Empty means a single constant
	// partition (ORDER BY tuple()).
	PartitionKey []string
	// SortKey is OrderBy minus PartitionKey, encoded into the DynamoDB sort key.
	SortKey []string
	// VersionColumn is the ReplacingMergeTree version column ("" if none).
	VersionColumn string
	// SumColumns are SummingMergeTree explicit columns (nil = all numeric non-key columns).
	SumColumns []string

	Replication string // ReplicationGlobal | ReplicationRegional

	// PhysicalName is the DynamoDB table name.
	PhysicalName string

	// PartitionBy is the PARTITION BY expression SQL text ("" if none). It
	// has no storage effect (accepted and reported by SHOW CREATE TABLE).
	PartitionBy string
	// PrimaryKey holds the PRIMARY KEY expression texts when the clause was
	// given explicitly (nil otherwise); it is always a prefix of OrderBy.
	PrimaryKey []string

	Settings map[string]string
	// SettingsSQL lists the CREATE TABLE SETTINGS in declaration order, each
	// rendered as "name = <literal SQL>" (numbers unquoted, strings quoted),
	// for SHOW CREATE TABLE. nil for tables created before this field existed
	// (fall back to Settings).
	SettingsSQL []string
	Comment     string
	DDL         string // original CREATE statement
	CreatedAt   time.Time
	// HomeRegion is the region that created the table (CreateTable stamps
	// Options.Region). With GLOBAL_MODE=aws only the home region creates a
	// global table and adds its replicas; other regions wait for their
	// replica (DESIGN.md §6). "" for entries written by older builds.
	HomeRegion string
	// ResetAt is when the table was last truncated (zero: never). TRUNCATE
	// sets it (MarkTruncated + UpdateTable) before any row is deleted, so
	// every region learns the reset through the replicated catalog: a
	// region's store resets its own copy of a regional table, and Merge
	// ignores view output of source rows written before the reset
	// (DESIGN.md §5 "Resets").
	ResetAt time.Time
	// Version increments on every catalog change (ALTER), used for cache invalidation.
	Version int64
}

// ColumnIndex returns the index of a column by name, or -1.
func (t *Table) ColumnIndex(name string) int {
	for i := range t.Columns {
		if t.Columns[i].Name == name {
			return i
		}
	}
	return -1
}

// FullName returns "db.table".
func (t *Table) FullName() string { return t.Database + "." + t.Name }

// View is a (materialized) view.
type View struct {
	Database     string
	Name         string
	Materialized bool
	// ToDatabase/ToTable is the MV target. For plain views these are empty.
	ToDatabase string
	ToTable    string
	// SourceDatabase/SourceTable is the table whose inserts trigger the MV
	// (the first table in the SELECT's FROM, resolved at CREATE time).
	SourceDatabase string
	SourceTable    string
	// SelectSQL is the view's SELECT text (re-parsed by the engine).
	SelectSQL string
	DDL       string
	CreatedAt time.Time
}

// FullName returns "db.view".
func (v *View) FullName() string { return v.Database + "." + v.Name }

// Database is a database entry.
type Database struct {
	Name      string
	Engine    string
	CreatedAt time.Time
}

// Catalog is the schema registry. Implementations must be safe for
// concurrent use. Lookups are served from an in-memory snapshot; mutations
// write through to the backing store and update the snapshot.
//
// Errors are *cherr.Error: UNKNOWN_DATABASE, UNKNOWN_TABLE,
// TABLE_ALREADY_EXISTS, DATABASE_ALREADY_EXISTS.
type Catalog interface {
	// Refresh reloads the snapshot from the backing store (picks up DDL
	// executed in other regions / processes).
	Refresh(ctx context.Context) error

	Databases() []*Database
	Database(name string) (*Database, error)
	CreateDatabase(ctx context.Context, db *Database, ifNotExists bool) error
	// DropDatabase removes the database entry and every table/view in it from
	// the catalog (physical tables are dropped by the caller).
	DropDatabase(ctx context.Context, name string, ifExists bool) error

	Tables(db string) []*Table
	Table(db, name string) (*Table, error)
	TableByPhysicalName(physical string) (*Table, bool)
	CreateTable(ctx context.Context, t *Table, ifNotExists bool) error
	// UpdateTable replaces the entry (ALTER), bumping Version.
	UpdateTable(ctx context.Context, t *Table) error
	DropTable(ctx context.Context, db, name string, ifExists bool) error

	Views(db string) []*View
	View(db, name string) (*View, error)
	CreateView(ctx context.Context, v *View, ifNotExists bool) error
	DropView(ctx context.Context, db, name string, ifExists bool) error
	// ViewsFrom returns materialized views whose source is db.table, sorted by name.
	ViewsFrom(db, table string) []*View
}

// VersionedReplacing reports a ReplacingMergeTree(ver) table: replicas of it
// must let the highest version win, which item-level last-writer-wins (AWS
// global tables) does not do. With GLOBAL_MODE=aws such a table is therefore
// physically regional and copied between regions by cmd/replicator's
// version-aware applier (DESIGN.md §1, §6).
func VersionedReplacing(t *Table) bool {
	return t != nil && t.Engine == EngineReplacingMergeTree && t.VersionColumn != ""
}

// CopiedByReplicator reports a global table that, with GLOBAL_MODE=aws, is
// physically regional and copied between regions by cmd/replicator instead
// of being an AWS global table: every ReplacingMergeTree table. A versioned
// one needs the version-aware applier (VersionedReplacing); for one without
// a version, AWS's internal last-writer-wins would silently drop the losing
// version of a concurrent write in one region, so that region's views
// (regional rollups built from its own stream) would never see it, while
// the replicator reports the loser to its conflict hook, which applies the
// views (DESIGN.md §1, §6). A table cannot change its physical replication
// later, so this holds whether or not it is a view source yet.
func CopiedByReplicator(t *Table) bool {
	return t != nil && t.Engine == EngineReplacingMergeTree
}

// MarkTruncated returns a copy of t whose ResetAt is now (strictly after the
// previous ResetAt). TRUNCATE persists it with UpdateTable and then passes
// the stored entry to Store.TruncateTable.
func MarkTruncated(t *Table, now time.Time) *Table {
	nt := cloneTable(t)
	now = now.UTC()
	if !now.After(t.ResetAt) {
		now = t.ResetAt.Add(time.Nanosecond)
	}
	nt.ResetAt = now
	return nt
}
