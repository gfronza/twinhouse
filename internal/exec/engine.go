package exec

// Engine internals, in pipeline order:
//
//   - engine.go    request handling: parse, settings, readonly, parameters.
//   - select.go    SELECT planning: FROM (source.go, join.go, arrayjoin.go,
//                  system.go), alias expansion, WHERE, GROUP BY/aggregation
//                  (aggregate.go), projection, DISTINCT, ORDER BY, LIMIT BY,
//                  LIMIT, UNION; plans are relations opened as row iterators.
//   - expr.go      expression compiler: AST -> statically typed closures with
//                  constant folding; functions*.go and in.go hold the
//                  function library; types.go the numeric/type rules.
//   - pushdown.go  WHERE -> storage.KeyRange derivation.
//   - insert.go    INSERT (VALUES, formats, SELECT) with defaults.
//   - ddl.go, show.go  DDL and introspection statements.
//   - views.go     materialized views (ApplyViews, POPULATE).

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/format"
	"github.com/gfronza/twinhouse/internal/sqlparse"
)

// engineImpl holds engine-wide state.
type engineImpl struct {
	started time.Time
	// viewStmts caches parsed materialized-view SELECTs by view name and
	// SELECT text.
	viewMu    sync.Mutex
	viewStmts map[string]*sqlparse.SelectStmt
	// fresh reloads the catalog snapshot on lookup misses.
	fresh *freshCatalog
}

func newEngine(cfg Config) (*Engine, error) {
	if cfg.Store == nil {
		return nil, cherr.New(cherr.BadArguments, "exec: Config.Store is required")
	}
	if cfg.Catalog == nil {
		return nil, cherr.New(cherr.BadArguments, "exec: Config.Catalog is required")
	}
	if cfg.Timezone == "" {
		cfg.Timezone = "UTC"
	}
	if cfg.ServerVersion == "" {
		cfg.ServerVersion = "25.8.1.1"
	}
	if cfg.ServerRevision == 0 {
		cfg.ServerRevision = 54477
	}
	if cfg.DisplayName == "" {
		cfg.DisplayName = "twinhouse"
	}
	if _, err := time.LoadLocation(cfg.Timezone); err != nil {
		return nil, cherr.Wrap(cherr.BadArguments, err, "exec: invalid timezone %q", cfg.Timezone)
	}
	return &Engine{cfg: cfg, impl: &engineImpl{started: time.Now(), viewStmts: map[string]*sqlparse.SelectStmt{}, fresh: newFreshCatalog(cfg.Catalog)}}, nil
}

// query is the per-statement execution context.
type query struct {
	e        *Engine
	ctx      context.Context
	cat      *queryCatalog
	db       string
	settings map[string]string
	params   map[string]string
	bound    map[string]*expr
	now      time.Time
	loc      *time.Location
	// tzName is the session_timezone request setting ("" if none); loc
	// is then that zone.
	tzName string
	user   string
	id     string
	req    *Request

	readRows  atomic.Uint64
	readBytes atomic.Uint64

	// plan is the state of the current plan (shared by copies).
	plan *planState

	// subqueries caches results of uncorrelated subqueries (IN / scalar),
	// which are evaluated at plan time and may be needed twice (filter and
	// key pushdown).
	subqueries map[*sqlparse.SelectStmt]*subqueryResult
}

type subqueryResult struct {
	cols []format.Column
	rows [][]any
}

// subqueryProbeRows is how many rows past max a bounded subquery still reads
// and drops: about one ClickHouse block (the max_block_size default, counted
// in result rows), so an error within it still wins over "more than one row".
// A scalar subquery (max 2) stops there, as ClickHouse does after one block:
// once it has two rows it fails with 125, an error in a later row is never
// raised, and read_rows counts only the rows read.
const subqueryProbeRows = 65409

// subquery runs an uncorrelated subquery once per statement. max > 0 keeps
// only the first max rows and stops reading subqueryProbeRows rows later.
// Only a complete result (fewer than max rows) is cached, so a later caller
// never sees a truncated one.
func (q *query) subquery(sel *sqlparse.SelectStmt, env *planEnv, max int) (*subqueryResult, error) {
	if r, ok := q.subqueries[sel]; ok {
		return r, nil
	}
	rel, err := q.planSelect(sel, env)
	if err != nil {
		return nil, err
	}
	read := 0
	if max > 0 {
		read = max + subqueryProbeRows
	}
	rows, err := rel.collectFirst(q, max, read)
	if err != nil {
		return nil, err
	}
	r := &subqueryResult{cols: rel.cols, rows: rows}
	if max == 0 || len(rows) < max {
		if q.subqueries == nil {
			q.subqueries = map[*sqlparse.SelectStmt]*subqueryResult{}
		}
		q.subqueries[sel] = r
	}
	return r, nil
}

func (e *Engine) newQuery(ctx context.Context, req *Request) *query {
	db := req.Database
	if db == "" {
		db = "default"
	}
	settings := make(map[string]string, len(req.Settings))
	for k, v := range req.Settings {
		settings[k] = v
	}
	loc, _ := time.LoadLocation(e.cfg.Timezone)
	tzName := ""
	if tz := settings["session_timezone"]; tz != "" {
		if l, err := time.LoadLocation(tz); err == nil {
			loc, tzName = l, tz
		}
	}
	user := req.User
	if user == "" {
		user = "default"
	}
	now := time.Now()
	return &query{
		e: e, ctx: ctx, db: db, settings: settings, params: req.Params,
		cat:   &queryCatalog{Catalog: e.cfg.Catalog, f: e.impl.fresh, ctx: ctx, since: now},
		bound: map[string]*expr{}, now: now.UTC(), loc: loc, tzName: tzName, user: user, id: req.QueryID, req: req,
		plan: &planState{},
	}
}

func (e *Engine) execute(ctx context.Context, req *Request) (res *Result, err error) {
	defer recoverInternal(&err)
	stmt, err := sqlparse.ParseRequest(req.Query, req.Params, req.Settings)
	if err != nil {
		return nil, err
	}
	q := e.newQuery(ctx, req)
	for k, v := range req.Settings {
		if err := q.checkSetting(k, v, false); err != nil {
			return nil, err
		}
	}
	return q.run(stmt)
}

// recoverInternal turns a panic into a LOGICAL_ERROR so that one bad query
// cannot take the server down.
func recoverInternal(err *error) {
	if r := recover(); r != nil {
		*err = &cherr.Error{Code: cherr.LogicalError, Msg: fmt.Sprintf("internal error: %v", r), Cause: errors.New(string(debug.Stack()))}
	}
}

// run dispatches one parsed statement.
func (q *query) run(stmt sqlparse.Statement) (*Result, error) {
	if err := q.checkReadonly(stmt); err != nil {
		return nil, err
	}
	switch s := stmt.(type) {
	case *sqlparse.SelectStmt:
		return q.runSelect(s)
	case *sqlparse.InsertStmt:
		return q.runInsert(s)
	case *sqlparse.CreateDatabaseStmt:
		return q.runCreateDatabase(s)
	case *sqlparse.CreateTableStmt:
		return q.runCreateTable(s)
	case *sqlparse.CreateViewStmt:
		return q.runCreateView(s)
	case *sqlparse.DropStmt:
		return q.runDrop(s)
	case *sqlparse.TruncateStmt:
		return q.runTruncate(s)
	case *sqlparse.ShowStmt:
		return q.runShow(s)
	case *sqlparse.DescribeStmt:
		return q.runDescribe(s)
	case *sqlparse.ExistsStmt:
		return q.runExists(s)
	case *sqlparse.UseStmt:
		if builtinDatabase(s.Database) {
			return &Result{Database: s.Database}, nil
		}
		if _, err := q.cat.Database(s.Database); err != nil {
			return nil, err
		}
		return &Result{Database: s.Database}, nil
	case *sqlparse.SetStmt:
		for _, st := range s.Settings {
			v, err := settingValue(st.Value)
			if err != nil {
				return nil, err
			}
			if err := q.checkSetting(st.Name, v, true); err != nil {
				return nil, err
			}
			q.settings[st.Name] = v
		}
		return &Result{}, nil
	case *sqlparse.OptimizeStmt:
		if _, err := q.lookupTable(s.Database, s.Name); err != nil {
			return nil, err
		}
		return &Result{}, nil
	case *sqlparse.SystemStmt:
		return &Result{}, nil
	case *sqlparse.AlterStmt:
		return q.runAlter(s)
	case *sqlparse.DeleteStmt:
		return q.runDelete(s.Database, s.Name, s.Where)
	case *sqlparse.ExplainStmt:
		return nil, cherr.New(cherr.NotImplemented, "EXPLAIN is not supported")
	}
	return nil, cherr.New(cherr.NotImplemented, "statement %T is not supported", stmt)
}

// checkReadonly enforces the readonly setting (1 and 2 forbid writes; 1 also
// forbids SET).
func (q *query) checkReadonly(stmt sqlparse.Statement) error {
	ro := q.settings["readonly"]
	if ro == "" || ro == "0" {
		return nil
	}
	switch stmt.(type) {
	case *sqlparse.SelectStmt, *sqlparse.ShowStmt, *sqlparse.DescribeStmt, *sqlparse.ExistsStmt,
		*sqlparse.UseStmt, *sqlparse.ExplainStmt:
		return nil
	case *sqlparse.SetStmt:
		if ro != "1" {
			return nil
		}
	}
	return cherr.New(cherr.ReadonlyQuery, "Cannot execute query in readonly mode")
}

// applySettings merges a statement's SETTINGS clause into the query settings.
func (q *query) applySettings(ss []sqlparse.Setting) error {
	for _, st := range ss {
		v, err := settingValue(st.Value)
		if err != nil {
			return err
		}
		if err := q.checkSetting(st.Name, v, true); err != nil {
			return err
		}
		if st.Name == "session_timezone" && v != "" && v != q.timezoneName() {
			// The server formats results in the request's time zone,
			// which a SETTINGS clause cannot change.
			return cherr.New(cherr.NotImplemented, "session_timezone in a SETTINGS clause is not supported: pass it as a request (URL) setting")
		}
		q.settings[st.Name] = v
	}
	return nil
}

// settingValue renders a setting value expression as text.
func settingValue(e sqlparse.Expr) (string, error) {
	switch x := e.(type) {
	case *sqlparse.Literal:
		switch v := x.Value.(type) {
		case string:
			return v, nil
		case bool:
			if v {
				return "1", nil
			}
			return "0", nil
		case nil:
			return "", nil
		case uint64:
			return strconv.FormatUint(v, 10), nil
		case int64:
			return strconv.FormatInt(v, 10), nil
		}
		if x.Raw != "" {
			return x.Raw, nil
		}
	case *sqlparse.UnaryExpr:
		if x.Op == "-" {
			s, err := settingValue(x.X)
			return "-" + s, err
		}
	case *sqlparse.Ident:
		return strings.Join(x.Parts, "."), nil
	case nil:
		return "1", nil
	}
	return sqlparse.String(e), nil
}

func (q *query) settingInt(name string, def int64) int64 {
	v, ok := q.settings[name]
	if !ok || v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "true":
		return 1
	case "false":
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

func (q *query) consistent() bool {
	if _, ok := q.settings["select_sequential_consistency"]; ok {
		return q.settingInt("select_sequential_consistency", 0) != 0
	}
	return q.e.cfg.ConsistentReads
}

func (q *query) maxResultRows() int {
	n := int(q.settingInt("max_result_rows", 0))
	if c := q.e.cfg.MaxResultRows; c > 0 && (n == 0 || c < n) {
		n = c
	}
	return n
}

// param returns the bound value of a {name:Type} query parameter as a
// constant expression.
func (q *query) param(p *sqlparse.Param) (*expr, error) {
	key := p.Name + "\x00" + p.Type
	if x, ok := q.bound[key]; ok {
		return x, nil
	}
	text, ok := q.params[p.Name]
	if !ok {
		return nil, cherr.New(cherr.UnknownQueryParameter, "Substitution `%s` is not set", p.Name)
	}
	t, err := chtype.Parse(p.Type)
	if err != nil {
		return nil, err
	}
	v, err := chtype.ParseText(t, text)
	if err != nil {
		return nil, paramParseError(err, text, p, t)
	}
	x := constant(t, v)
	q.bound[key] = x
	return x, nil
}

func (q *query) defaultDB(db string) string {
	if db == "" {
		return q.db
	}
	return db
}

func (q *query) lookupTable(db, name string) (*catalog.Table, error) {
	return q.cat.Table(q.defaultDB(db), name)
}

func (q *query) location(t chtype.Type) *time.Location {
	if u := t.Unwrap(); u.TZ != "" {
		if l, err := time.LoadLocation(u.TZ); err == nil {
			return l
		}
	}
	return q.loc
}

// paramParseError reports a query parameter whose value does not parse,
// like ClickHouse: when the type's text parser stops before the end of the
// value, BAD_QUERY_PARAMETER (457) "Value X cannot be parsed as T for query
// parameter 'p' because it isn't parsed completely: only N of M bytes was
// parsed: <prefix>"; when the parser itself fails (date and time,
// malformed arrays/tuples/maps, quoted strings, UUID, Bool, Enum, an empty
// number) the parser's code is kept, with ": value X cannot be parsed as T
// for query parameter 'p'" appended to its message.
func paramParseError(err error, text string, p *sqlparse.Param, t chtype.Type) error {
	suffix := fmt.Sprintf("value %s cannot be parsed as %s for query parameter '%s'", text, p.Type, p.Name)
	if t.IsNumeric() {
		if code, msg, n := numberParamFailure(t, text); code != 0 {
			return &cherr.Error{Code: code, Cause: err, Msg: msg + ": " + suffix}
		} else if n < len(text) {
			return &cherr.Error{Code: cherr.BadQueryParameter, Cause: err, Msg: fmt.Sprintf(
				"Value %s cannot be parsed as %s for query parameter '%s' because it isn't parsed completely: only %d of %d bytes was parsed: %s",
				text, p.Type, p.Name, n, len(text), text[:n])}
		}
	}
	switch ce := cherr.As(err); {
	case ce == nil:
	case ce.Code == cherr.CannotParseDateTime:
		return &cherr.Error{Code: ce.Code, Cause: err, Msg: "Cannot parse datetime: " + suffix}
	case ce.Code == cherr.CannotParseDate:
		return &cherr.Error{Code: ce.Code, Cause: err, Msg: "Cannot parse date: " + suffix}
	case ce.Code != cherr.CannotParseText && ce.Code != cherr.CannotParseNumber && ce.Code != cherr.BadQueryParameter:
		return &cherr.Error{Code: ce.Code, Cause: err, Msg: ce.Msg + ": " + suffix}
	}
	return &cherr.Error{Code: cherr.BadQueryParameter, Cause: err, Msg: fmt.Sprintf("Value %s cannot be parsed as %s for query parameter '%s'", text, p.Type, p.Name)}
}

// attemptToReadAfterEOF is ClickHouse's ATTEMPT_TO_READ_AFTER_EOF.
const attemptToReadAfterEOF = 32

// numberParamFailure mirrors ClickHouse's readIntText/readFloatText on a
// query parameter of numeric type t that did not parse: either the parser
// throws (code, message) or it stops after n bytes (code 0).
func numberParamFailure(t chtype.Type, s string) (code int, msg string, n int) {
	isDigit := func(i int) bool { return i < len(s) && s[i] >= '0' && s[i] <= '9' }
	if t.IsInteger() {
		if s == "" || (s == "-" && t.IsSigned()) {
			return attemptToReadAfterEOF, "Attempt to read after eof", 0
		}
		i := 0
		if t.IsSigned() && s[0] == '-' {
			i = 1
		}
		j := i
		for isDigit(j) {
			j++
		}
		if j == i {
			return 0, "", 0
		}
		return 0, "", j
	}
	const prefix = "Cannot read floating point value"
	if s == "" {
		return cherr.CannotParseNumber, prefix, 0
	}
	i := 0
	if s[0] == '-' || s[0] == '+' {
		i = 1
	}
	if rest := strings.ToLower(s[i:]); strings.HasPrefix(rest, "nan") {
		return 0, "", i + 3
	} else if strings.HasPrefix(rest, "infinity") {
		return 0, "", i + 8
	} else if rest == "inf" {
		return 0, "", i + 3
	} else if strings.HasPrefix(rest, "i") {
		return cherr.CannotParseInputAssertionFailed, "Cannot parse infinity", 0
	}
	j := i
	for isDigit(j) {
		j++
	}
	digits := j > i
	if j < len(s) && s[j] == '.' {
		j++
		for isDigit(j) {
			j++
			digits = true
		}
	}
	if exp := j < len(s) && (s[j] == 'e' || s[j] == 'E'); !digits && !exp {
		switch {
		case j == len(s) && j == i:
			return cherr.CannotParseNumber, prefix + ": no digits read", 0
		case j == len(s):
			return 0, "", j // "." reads as 0
		}
		return 0, "", 0
	}
	if j < len(s) && (s[j] == 'e' || s[j] == 'E') {
		k := j + 1
		if k < len(s) && (s[k] == '-' || s[k] == '+') {
			k++
		}
		if !isDigit(k) {
			if k == len(s) {
				return cherr.CannotParseNumber, prefix + ": nothing after exponent", 0
			}
			return 0, "", j
		}
		for isDigit(k) {
			k++
		}
		j = k
	}
	return 0, "", j
}

// builtinDatabase reports the databases that always exist (case-sensitive,
// like ClickHouse): USE on them never consults the catalog.
func builtinDatabase(db string) bool {
	switch db {
	case "default", "system", "information_schema", "INFORMATION_SCHEMA":
		return true
	}
	return false
}
