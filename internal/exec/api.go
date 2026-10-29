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
	"github.com/gfronza/twinhouse/internal/sqlparse"
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
	// MaxMemoryUsage is the default max_memory_usage of a statement, in
	// bytes (0 = unlimited, as in ClickHouse). When it is set, a client may
	// lower the limit but not raise it or set 0.
	MaxMemoryUsage int64
	// ViewMemoryUsage is the limit of ApplyViews and ApplyViewList (the
	// stream worker; 0 = MaxMemoryUsage). Views applied inline during an
	// INSERT share the INSERT's limit.
	ViewMemoryUsage int64
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
	// Stream asks for a top-level SELECT's rows as Result.Stream instead
	// of Result.Rows. Other statements return Rows either way.
	Stream bool
}

// Result is the outcome of a statement.
type Result struct {
	// Columns describe the output; nil/empty for statements without output
	// (INSERT, DDL), in which case the HTTP body is empty.
	Columns []format.Column
	// Rows are the result rows (canonical values, len(Columns) each).
	Rows [][]any
	// Stream yields the rows instead of Rows when the request asked for it
	// (Request.Stream). The query runs until the caller closes it.
	Stream ResultStream
	// Format is the FORMAT clause of the query ("" if absent).
	Format string
	// Stats are the counts of collected rows (Counts adds ResultRows); a
	// stream keeps its own.
	Stats
	// WrittenRows counts the rows an INSERT wrote.
	WrittenRows uint64
	// Database is set by USE (session-less HTTP ignores it, like ClickHouse
	// without session_id).
	Database string
}

// Counts returns the result's counts: a stream's so far (final after its
// EOF), or those of the collected rows.
func (r *Result) Counts() Stats {
	if r.Stream != nil {
		return r.Stream.Stats()
	}
	st := r.Stats
	st.ResultRows = uint64(len(r.Rows))
	return st
}

// ResultStream yields a SELECT's rows block by block. It is used by one
// goroutine.
type ResultStream interface {
	// Next returns the next block of at most maxRows rows, or io.EOF at
	// the end. A block also ends once its rows hold about 1 MB, or with
	// the source block of maxRows rows read that its rows come from. It
	// stays valid, and charged to the query, until the next call or Close.
	Next(maxRows int) ([][]any, error)
	// Stats returns the counts so far; they are final after io.EOF.
	Stats() Stats
	// Charge counts n more bytes the caller holds for the query (a
	// response buffered whole, rows an output format keeps) against its
	// memory limit, until Close; a negative n releases bytes it charged.
	Charge(n int64) error
	// Close ends the query and releases what it holds. It may be called
	// more than once.
	Close()
}

// Stats are a result's counts, for X-ClickHouse-Summary, JSON statistics
// and logs.
type Stats struct {
	ReadRows, ReadBytes uint64
	// ResultRows is how many rows were returned.
	ResultRows uint64
	// RowsBeforeLimit is set when the query had a LIMIT.
	RowsBeforeLimit    uint64
	HasRowsBeforeLimit bool
	// PeakMemory is the most memory the query had charged at once
	// (counted only when it had a max_memory_usage).
	PeakMemory int64
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

// SettingText is what a session keeps for `SET name = v` when that differs
// from v as written: a size setting literal (max_memory_usage and the like)
// is kept as its UInt64 value (1e9 as "1000000000"), as ClickHouse keeps
// it, and DEFAULT drops the setting (reset). ok is false for other
// settings and invalid values.
func SettingText(name string, v sqlparse.Expr) (text string, reset, ok bool) {
	st := sqlparse.Setting{Name: name, Value: v}
	if resetsSetting(st) {
		return "", true, true
	}
	if !sizeSettings[name] {
		return "", false, false
	}
	text, err := clauseValue(st)
	return text, false, err == nil
}

// ParseSizeSetting parses a byte-size setting's text as the engine reads
// it (ClickHouse's parseWithSizeSuffix: an optional '+', digits, a size
// suffix), so a caller that acts on such a setting before the query runs
// reads the value the engine checks.
func ParseSizeSetting(s string) (uint64, error) { return parseSizeSetting(s) }

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
