// Package storage defines how table rows are persisted. The DynamoDB
// implementation lives in storage/dynamo; tests may use an in-memory store.
//
// INTERFACE CONTRACT: shared by the engine, the stream worker and storage
// implementations.
package storage

import (
	"context"
	"errors"

	"dynamodb-eventstream/internal/catalog"
)

// Row holds one value per table column, in catalog.Table.Columns order, using
// the canonical chtype value representation. Columns not requested by a
// projection are nil.
type Row []any

// Bound is one end of a range on a sort-key column.
type Bound struct {
	Value     any
	Inclusive bool
}

// KeyRange selects rows by the table's sorting key. All values use the
// canonical representation of the corresponding column type.
//
//   - Partition has one value per Table.PartitionKey column (all required;
//     for tables with an empty PartitionKey it is empty).
//   - SortPrefix holds equality values for the leading SortKey columns
//     (len <= len(SortKey)).
//   - Lower/Upper optionally bound the SortKey column right after the prefix.
//
// The store returns every row in the range; it MAY return extra rows, since the
// engine re-applies every WHERE predicate.
type KeyRange struct {
	Partition  []any
	SortPrefix []any
	Lower      *Bound
	Upper      *Bound
}

// ScanRequest describes a read.
type ScanRequest struct {
	// Ranges restricts the read to key ranges (each executed as a DynamoDB
	// Query). nil means a full table scan. An empty non-nil slice means "no rows".
	Ranges []KeyRange
	// Columns lists column indexes the caller needs; nil means all. The store
	// may return more.
	Columns []int
	// Consistent requests strongly consistent reads (local region).
	Consistent bool
	// Limit is an optional upper bound on returned rows (0 = unlimited);
	// only a hint, used when the engine knows no filtering/aggregation follows.
	Limit int
	// Reverse asks for descending sort-key order within each range (hint).
	Reverse bool
	// MarkAbsent asks the store to return Absent, instead of the type's zero
	// value, for a requested column that has a stored default (see
	// MarksAbsent) but no value in the stored row, i.e. the column was added
	// by ALTER TABLE ADD COLUMN after the row was written. The engine then
	// computes the column's DEFAULT/MATERIALIZED expression over the row, as
	// ClickHouse does for parts that predate the column. Off by default so
	// callers that do not handle Absent keep receiving zero values.
	MarkAbsent bool
}

// absentValue is the dynamic type of Absent.
type absentValue struct{}

func (absentValue) String() string { return "<absent>" }

// Absent is the Row value a store returns, when ScanRequest.MarkAbsent is
// set, for a column whose value is missing from the stored row. It never
// reaches formats or Insert: the engine replaces it with the column default.
var Absent any = absentValue{}

// IsAbsent reports whether v is Absent.
func IsAbsent(v any) bool {
	_, ok := v.(absentValue)
	return ok
}

// MarksAbsent reports whether a store honouring ScanRequest.MarkAbsent
// returns Absent for column c when its value is missing: only DEFAULT and
// MATERIALIZED columns with a default expression (for any other column the
// value read is the type's zero value, which is what ClickHouse computes
// too). ALIAS and EPHEMERAL columns are never stored.
func MarksAbsent(c *catalog.Column) bool {
	return (c.DefaultKind == "DEFAULT" || c.DefaultKind == "MATERIALIZED") && c.DefaultExpr != ""
}

// RowIterator streams rows. Next returns (nil, io.EOF) at the end.
type RowIterator interface {
	Next(ctx context.Context) (Row, error)
	Close() error
}

// SourceRef identifies, for exactly-once materialized-view application, the
// pair (view, source row) that produced a merge. Applying the same SourceRef
// to the same target key twice must have no additional effect.
type SourceRef struct {
	View  string // fully qualified view name "db.view"
	RowID string // source row id (see InsertResult.RowIDs / StreamRecord.RowID)
}

// MergeRow is one target row to merge into an AggregatingMergeTree or
// SummingMergeTree table (or upsert into a MergeTree/Replacing target of an MV).
type MergeRow struct {
	Row    Row
	Source SourceRef
	// Index distinguishes several target rows produced from one source row by
	// one view (e.g. multiple GROUP BY keys or ARRAY JOIN); 0 otherwise.
	Index int
}

// InsertResult reports what an insert wrote.
type InsertResult struct {
	// RowIDs[i] is the unique id assigned to rows[i] (used as SourceRef.RowID
	// when applying materialized views inline). For engines that collapse rows
	// (Replacing), it is still a unique per-insert id.
	RowIDs []string
	// Skipped[i] is true when the row was not written (e.g. a
	// ReplacingMergeTree row with an older version than the stored one).
	Skipped []bool
}

// Store persists table rows.
//
// Engine semantics the store implements:
//
//   - MergeTree (and Memory/Log): every inserted row is stored as a distinct
//     item, never deduplicated.
//   - ReplacingMergeTree(ver): one item per sorting key; a row replaces the
//     stored one iff it has ver >= stored ver (or always, when there is no
//     version column). Reads therefore always behave like FINAL.
//   - SummingMergeTree: one item per sorting key; numeric non-key columns
//     (or SumColumns) are added.
//   - AggregatingMergeTree: one item per sorting key;
//     AggregateFunction columns hold *chtype.AggPartial and
//     SimpleAggregateFunction(f, T) columns are merged with f
//     (sum: add; min/max: keep extreme; any: keep first; anyLast: overwrite;
//     groupUniqArray: union). uniq/uniqExact members are stored as separate
//     items, so set sizes are unbounded and exact.
type Store interface {
	// EnsureTable creates the physical table(s) for t if missing (idempotent),
	// including any side tables (aggregate members, MV markers) and enables
	// DynamoDB Streams (NEW_AND_OLD_IMAGES) on every row table that can have
	// a stream consumer (all but regional Aggregating/Summing rollups).
	EnsureTable(ctx context.Context, t *catalog.Table) error
	// DropTable deletes the physical table(s) (idempotent).
	DropTable(ctx context.Context, t *catalog.Table) error
	// TruncateTable deletes all rows (and side items) keeping the table.
	TruncateTable(ctx context.Context, t *catalog.Table) error

	// Insert writes rows (full rows in column order, defaults already applied).
	// For AggregatingMergeTree/SummingMergeTree tables it merges each row with
	// a fresh random SourceRef (i.e. direct inserts are not idempotent, like
	// ClickHouse).
	Insert(ctx context.Context, t *catalog.Table, rows []Row) (*InsertResult, error)

	// Merge applies materialized-view output rows exactly once per
	// (Source, target sorting key, Index). Safe to call concurrently and to
	// retry. For MergeTree targets the item key is derived deterministically
	// from (Source, Index) so redelivery and multi-region application collapse
	// to one item.
	Merge(ctx context.Context, t *catalog.Table, rows []MergeRow) error

	// Scan reads rows.
	Scan(ctx context.Context, t *catalog.Table, req ScanRequest) (RowIterator, error)

	// DeleteRows deletes rows for lightweight DELETE / ALTER DELETE: the
	// engine supplies the matching rows it read; the store deletes their items.
	// (Optional: may return NOT_IMPLEMENTED.)
	DeleteRows(ctx context.Context, t *catalog.Table, rows []Row) error
}

// StreamRecord is a decoded change-stream record for a table row.
type StreamRecord struct {
	Table *catalog.Table
	// EventName is "INSERT", "MODIFY" or "REMOVE".
	EventName string
	Row       Row // NewImage decoded (OldImage for REMOVE)
	// RowID is the row's unique id (same value Insert returned).
	RowID string
	// Origin is the region where the row was originally written.
	Origin string
	// IsMVOutput is true for rows written by Merge (materialized view output);
	// these do not trigger further views (no MV chaining in v1).
	IsMVOutput bool
}

// ErrAwaitingReplica is in the cause chain of an EnsureTable error that only
// means this region's replica of a global table is still being added by its
// home region (GLOBAL_MODE=aws); callers retrying periodically may treat
// it as progress rather than a failure.
var ErrAwaitingReplica = errors.New("awaiting the global table replica")

// ErrDeleteDeferred is in the cause chain of a DropTable error that only
// means the physical table could not be deleted yet although its catalog
// entry is gone (DynamoDB refuses to delete the source table of a global
// table within 24 hours of adding a replica): the reconciler finishes the
// deletion once the service allows it, so the DDL may report success.
var ErrDeleteDeferred = errors.New("physical table deletion deferred")
