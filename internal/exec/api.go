// Package exec is the query engine: it binds parameters, plans reads against
// storage (turning sorting-key predicates into DynamoDB Query ranges),
// evaluates expressions and aggregates, executes DDL and INSERTs, and applies
// materialized views.
package exec

import (
	"context"
	"io"
	"log/slog"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/format"
	"github.com/gfronza/twinhouse/internal/storage"
)

// Config configures an Engine.
type Config struct {
	Store   storage.Store
	Catalog catalog.Catalog
	// Region is this deployment's region name (e.g. "us-east-1").
	Region string
	// InlineViews applies materialized views synchronously during INSERT (in
	// addition to the stream worker), giving read-your-writes for aggregate
	// tables in the writing region. Exactly-once markers make the double
	// application harmless.
	InlineViews bool
	// StreamViews is set when a stream worker applies this deployment's
	// materialized views (cmd/mvworker, RUN_STREAM_WORKER or a Lambda; a
	// DynamoDB deployment needs one per region). An inline view
	// application that fails after the source rows were stored is then
	// logged and the INSERT succeeds: the worker applies the views to those
	// rows exactly once, whereas an error would make clients resend rows
	// that are already written. Without it the INSERT fails (the source
	// rows stay written, as in ClickHouse when a view fails).
	StreamViews bool
	// Logger receives warnings (nil: slog.Default()).
	Logger *slog.Logger
	// MultiRegion is set when other regions replicate this deployment's
	// global tables (GLOBAL_MODE aws or emulated). POPULATE fills only the
	// executing region's copy of a REGIONAL view target, so it is then
	// rejected for such targets (NOT_IMPLEMENTED) instead of leaving the
	// other regions' rollups permanently without the backfilled rows.
	MultiRegion bool
	// ConsistentReads sets the default for strongly consistent DynamoDB reads
	// (setting select_sequential_consistency overrides per query).
	ConsistentReads bool
	// Version strings reported by version(), revision(), displayName(), timezone().
	ServerVersion  string // e.g. "25.8.1.1"
	ServerRevision uint64 // e.g. 54477
	DisplayName    string
	Timezone       string // e.g. "UTC"
	// MaxResultRows guards memory (0 = unlimited).
	MaxResultRows int
	// PhysicalName maps a ClickHouse table to its DynamoDB table name (e.g.
	// dynamo.Store.PhysicalName). nil uses "db.table".
	PhysicalName func(db, table string) string
}

// Request is one query execution request (one HTTP request).
type Request struct {
	// Query is the SQL text. For HTTP, ClickHouse uses the `query` URL
	// parameter if present (and then the body is INSERT data), otherwise the
	// body is the query. The server resolves that and fills Query/Data.
	Query string
	// Data is the INSERT payload (request body when the query came from the
	// URL). nil if none.
	Data io.Reader
	// Params are query parameters (param_<name> without the prefix).
	Params map[string]string
	// Database is the current database (URL `database` / X-ClickHouse-Database), default "default".
	Database string
	// Settings are request settings (URL params other than reserved ones).
	Settings map[string]string
	QueryID  string
	User     string
	// FormatOptions are passed to input decoders.
	FormatOptions format.Options
}

// Result is the outcome of a statement.
type Result struct {
	// Columns describe the output; nil/empty for statements without output
	// (INSERT, DDL), in which case the HTTP body is empty.
	Columns []format.Column
	// Rows are the result rows (canonical values, len(Columns) each).
	Rows [][]any
	// Format is the FORMAT clause of the query ("" if absent).
	Format string
	// Stats for X-ClickHouse-Summary / JSON statistics.
	ReadRows    uint64
	ReadBytes   uint64
	WrittenRows uint64
	// RowsBeforeLimit is set when the query had a LIMIT.
	RowsBeforeLimit    uint64
	HasRowsBeforeLimit bool
	// Database is set by USE (session-less HTTP ignores it, like ClickHouse
	// without session_id).
	Database string
}

// Engine executes statements.
type Engine struct {
	cfg  Config
	impl *engineImpl
}

// New creates an Engine.
func New(cfg Config) (*Engine, error) { return newEngine(cfg) }

// Execute parses and executes one statement.
func (e *Engine) Execute(ctx context.Context, req *Request) (*Result, error) {
	return e.execute(ctx, req)
}

// ApplyViews runs every materialized view whose source is t over the given
// source rows and merges the output into the view targets exactly once per
// (view, rowIDs[i]). Used by the inline INSERT path and by the stream worker
// (which supplies rows decoded from the source table's DynamoDB stream).
func (e *Engine) ApplyViews(ctx context.Context, t *catalog.Table, rows []storage.Row, rowIDs []string) error {
	return e.applyViews(ctx, t, rows, rowIDs)
}

// ApplyViewList is ApplyViews restricted to the given views (each must
// have t as its source). The stream worker uses it so that rows written
// before a view was created (catalog.View.CreatedAt) are not applied to it
// when it replays a source stream: ClickHouse MVs only see inserts made after
// CREATE (POPULATE backfills older rows). Views whose source is not t are
// rejected.
func (e *Engine) ApplyViewList(ctx context.Context, t *catalog.Table, views []*catalog.View, rows []storage.Row, rowIDs []string) error {
	return e.applyViewList(ctx, t, views, rows, rowIDs)
}

// RowIDIterator is optionally implemented by a storage.RowIterator.
// RowID returns the stored row id (the item's _rid, the same id Insert
// returned and StreamRecord.RowID carries) of the row last returned by
// Next. POPULATE uses it so its exactly-once markers coincide with the
// stream worker's and the inline INSERT path's for the same source rows.
type RowIDIterator interface {
	RowID() string
}

// Catalog returns the engine's catalog.
func (e *Engine) Catalog() catalog.Catalog { return e.cfg.Catalog }

func (e *Engine) logger() *slog.Logger {
	if e.cfg.Logger != nil {
		return e.cfg.Logger
	}
	return slog.Default()
}
