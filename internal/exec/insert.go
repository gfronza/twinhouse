package exec

import (
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/format"
	"github.com/gfronza/twinhouse/internal/sqlparse"
	"github.com/gfronza/twinhouse/internal/storage"
)

// insertBatch is the number of rows written (and views applied) per batch.
const insertBatch = 1000

func isEOF(err error) bool { return errors.Is(err, io.EOF) }

// insertableColumns are the columns an INSERT without a column list fills.
func insertableColumns(t *catalog.Table) []int {
	var out []int
	for i, c := range t.Columns {
		switch c.DefaultKind {
		case "MATERIALIZED", "ALIAS", "EPHEMERAL":
			continue
		}
		out = append(out, i)
	}
	return out
}

// rowBuilder completes inserted rows: converts values to column types and
// evaluates DEFAULT/MATERIALIZED expressions (which may reference other
// columns of the same row).
type rowBuilder struct {
	q        *query
	t        *catalog.Table
	defaults []*expr
	deps     [][]int
}

func (q *query) newRowBuilder(t *catalog.Table) (*rowBuilder, error) {
	b := &rowBuilder{q: q, t: t, defaults: make([]*expr, len(t.Columns)), deps: make([][]int, len(t.Columns))}
	sc := tableScope(t, "")
	for i, c := range t.Columns {
		if c.DefaultExpr == "" || c.DefaultKind == "ALIAS" {
			continue
		}
		e, err := sqlparse.ParseExpr(c.DefaultExpr)
		if err != nil {
			return nil, cherr.Wrap(cherr.LogicalError, err, "cannot parse default expression of column %s", c.Name)
		}
		used := map[int]bool{}
		x, err := (&compiler{q: q, env: &planEnv{}, sc: sc, used: used, clause: "default expression of column " + c.Name}).compile(e)
		if err != nil {
			return nil, err
		}
		if x, err = castExpr(x, c.Type, false); err != nil {
			return nil, err
		}
		b.defaults[i] = x
		for d := range used {
			b.deps[i] = append(b.deps[i], d)
		}
	}
	return b, nil
}

// complete fills missing columns of row in place. present[i] reports whether
// column i was supplied by the INSERT.
func (b *rowBuilder) complete(row []any, present []bool) error {
	state := make([]uint8, len(row)) // 0 todo, 1 in progress, 2 done
	var resolve func(i int) error
	resolve = func(i int) error {
		switch state[i] {
		case 2:
			return nil
		case 1:
			return cherr.New(cherr.IncorrectQuery, "Cyclic default expressions involving column %s", b.t.Columns[i].Name)
		}
		state[i] = 1
		c := &b.t.Columns[i]
		switch {
		case c.DefaultKind == "ALIAS":
			// Not stored; the type default keeps rows valid for every store.
			row[i] = zeroValue(c.Type)
		case present[i] && c.DefaultKind != "MATERIALIZED":
		case b.defaults[i] != nil:
			for _, d := range b.deps[i] {
				if err := resolve(d); err != nil {
					return err
				}
			}
			v, err := b.defaults[i].eval(row)
			if err != nil {
				return err
			}
			if v == nil && !c.Type.IsNullable() {
				v = zeroValue(c.Type)
			}
			row[i] = v
		default:
			row[i] = zeroValue(c.Type)
		}
		state[i] = 2
		return nil
	}
	for i := range row {
		if err := resolve(i); err != nil {
			return err
		}
	}
	for i, c := range b.t.Columns {
		if c.DefaultKind == "EPHEMERAL" {
			row[i] = zeroValue(c.Type)
		}
	}
	return nil
}

// convertTo converts a value of type from to column type to with CAST
// semantics; ok=false means "use the column default" (NULL into a
// non-Nullable column, ClickHouse's input_format_null_as_default).
func (q *query) convertTo(v any, from, to chtype.Type) (any, bool, error) {
	if v == nil {
		if to.IsNullable() {
			return nil, true, nil
		}
		return nil, false, nil
	}
	if p, ok := v.(*chtype.AggPartial); ok {
		if to.Kind != chtype.AggregateFunction || p.Func != stateBase(to.AggFunc) {
			return nil, false, cherr.New(cherr.TypeMismatch, "Cannot insert aggregate state of %s into column of type %s", p.Func, to.String())
		}
		return v, true, nil
	}
	if to.Kind == chtype.AggregateFunction {
		return nil, false, cherr.New(cherr.TypeMismatch, "Cannot insert a value of type %s into column of type %s (use -State aggregate functions)", from.String(), to.String())
	}
	fu, tu := from.Unwrap(), to.Unwrap()
	if fu.Equal(tu) || fu.Kind == chtype.Nothing {
		return v, true, nil
	}
	out, err := convertValue(q, v, fu, tu)
	if err != nil {
		return nil, false, err
	}
	return out, true, nil
}

func (q *query) runInsert(s *sqlparse.InsertStmt) (*Result, error) {
	if err := q.applySettings(s.Settings); err != nil {
		return nil, err
	}
	db := q.defaultDB(s.Database)
	t, err := q.cat.Table(db, s.Table)
	if err != nil {
		v, verr := q.cat.View(db, s.Table)
		if verr != nil {
			return nil, err
		}
		if !v.Materialized || v.ToTable == "" {
			return nil, cherr.New(cherr.NotImplemented, "cannot insert into view %s", v.FullName())
		}
		if t, err = q.cat.Table(v.ToDatabase, v.ToTable); err != nil {
			return nil, err
		}
	}
	cols, err := insertColumns(t, s.Columns)
	if err != nil && cherr.Is(err, cherr.NoSuchColumnInTable) {
		// The snapshot may predate an ALTER ... ADD COLUMN executed on
		// another server: reload once and retry.
		if fresh, ferr := q.cat.refreshTable(t.Database, t.Name); ferr == nil && fresh != t {
			t = fresh
			cols, err = insertColumns(t, s.Columns)
		}
	}
	if err != nil {
		return nil, err
	}
	var n uint64
	switch {
	case s.Select != nil:
		rel, err := q.planSelect(s.Select, &planEnv{})
		if err != nil {
			return nil, err
		}
		n, err = q.insertRelation(t, cols, rel)
		if err != nil {
			return nil, err
		}
	case len(s.Values) > 0:
		n, err = q.insertValues(t, cols, s.Values)
		if err != nil {
			return nil, err
		}
	default:
		n, err = q.insertData(t, cols, s)
		if err != nil {
			return nil, err
		}
	}
	return &Result{WrittenRows: n}, nil
}

func insertColumns(t *catalog.Table, names []string) ([]int, error) {
	if names == nil {
		return insertableColumns(t), nil
	}
	seen := map[int]bool{}
	out := make([]int, len(names))
	for i, n := range names {
		ci := t.ColumnIndex(n)
		if ci < 0 {
			return nil, cherr.New(cherr.NoSuchColumnInTable, "No such column %s in table %s", n, t.FullName())
		}
		switch t.Columns[ci].DefaultKind {
		case "MATERIALIZED", "ALIAS":
			return nil, cherr.New(cherr.IllegalColumn, "Cannot insert column %s, because it is %s column", n, t.Columns[ci].DefaultKind)
		}
		if seen[ci] {
			return nil, cherr.New(duplicateColumn, "Column %s in table %s specified more than once", n, t.FullName())
		}
		seen[ci] = true
		out[i] = ci
	}
	return out, nil
}

// Default block bounds of an INSERT (ClickHouse's min_insert_block_size_rows
// and min_insert_block_size_bytes).
const (
	defaultInsertBlockRows  = 1048449
	defaultInsertBlockBytes = 268402944
)

// inserter collects completed rows into blocks and writes each block to the
// store (in storage batches of insertBatch rows) and applies views to it.
//
// Like ClickHouse, an INSERT is atomic per block with respect to parse,
// conversion and DEFAULT errors: a block is written only after every row of
// it was parsed and completed, so a malformed row writes nothing of its
// block (a client may fix the data and resend it). A block ends after
// min_insert_block_size_rows rows (default 1048449) or about
// min_insert_block_size_bytes (default 256 MiB) of values (0 disables a
// bound; with both 0, max_insert_block_size rows); an INSERT larger than one
// block can leave its earlier blocks written, as in ClickHouse. A
// storage failure while a block is written can leave part of it written
// (DynamoDB has no multi-batch transactions).
type inserter struct {
	q        *query
	t        *catalog.Table
	b        *rowBuilder
	rows     []storage.Row
	bytes    int64
	maxRows  int64 // <= 0: unbounded
	maxBytes int64 // <= 0: unbounded
	total    uint64
}

func (q *query) newInserter(t *catalog.Table) (*inserter, error) {
	b, err := q.newRowBuilder(t)
	if err != nil {
		return nil, err
	}
	in := &inserter{q: q, t: t, b: b,
		maxRows:  q.settingInt("min_insert_block_size_rows", defaultInsertBlockRows),
		maxBytes: q.settingInt("min_insert_block_size_bytes", defaultInsertBlockBytes),
	}
	if in.maxRows <= 0 && in.maxBytes <= 0 {
		// No squashing: blocks are what the input format produces.
		in.maxRows = q.settingInt("max_insert_block_size", defaultInsertBlockRows)
	}
	return in, nil
}

// add completes a row given supplied values (nil present entries mean "use
// the default") and queues it; a full block is written.
func (in *inserter) add(row []any, present []bool) error {
	if err := in.b.complete(row, present); err != nil {
		return err
	}
	in.rows = append(in.rows, storage.Row(row))
	for _, v := range row {
		in.bytes += valueBytes(v)
	}
	if (in.maxRows > 0 && int64(len(in.rows)) >= in.maxRows) || (in.maxBytes > 0 && in.bytes >= in.maxBytes) {
		return in.flush()
	}
	return nil
}

// flush writes the queued block.
func (in *inserter) flush() error {
	rows := in.rows
	in.rows, in.bytes = nil, 0
	for len(rows) > 0 {
		n := min(len(rows), insertBatch)
		if err := in.write(rows[:n]); err != nil {
			return err
		}
		rows = rows[n:]
	}
	return nil
}

// write stores one storage batch of a complete block and applies the
// source table's views to it inline.
func (in *inserter) write(batch []storage.Row) error {
	q := in.q
	res, err := q.e.cfg.Store.Insert(q.ctx, in.t, batch)
	if err != nil {
		return q.staleTableError(in.t, err)
	}
	in.total += uint64(len(batch))
	if !q.e.cfg.InlineViews || len(q.cat.ViewsFrom(in.t.Database, in.t.Name)) == 0 {
		return nil
	}
	if res == nil || len(res.RowIDs) < len(batch) {
		return cherr.New(cherr.LogicalError, "storage did not return row ids for inserted rows")
	}
	rows, ids := batch, res.RowIDs[:len(batch)]
	if q.e.cfg.MultiRegion {
		// A row the store did not keep (res.Skipped: a
		// ReplacingMergeTree version older than the stored one) never
		// reaches any stream, so no other region's views can see it:
		// to keep the regional rollups convergent it feeds no view
		// (DESIGN.md §1, §5). In a single region, views see every
		// inserted row, like ClickHouse (row ids are unique, so the
		// exactly-once markers still hold).
		rows, ids = nil, nil
		for i, r := range batch {
			if i < len(res.Skipped) && res.Skipped[i] {
				continue
			}
			rows = append(rows, r)
			ids = append(ids, res.RowIDs[i])
		}
	}
	if err := q.e.applyViewsQuery(q, in.t, rows, ids); err != nil {
		if q.e.cfg.StreamViews && q.ctx.Err() == nil {
			// The source rows are stored and the stream worker
			// applies the views to them exactly once; failing the
			// INSERT would make a client resend rows that are already
			// written (new row ids: counted twice). Read-your-writes
			// of the rollups lags until the worker catches up.
			q.e.logger().Warn("inline materialized view application failed; the stream worker will apply it",
				"table", in.t.FullName(), "rows", len(rows), "error", err)
			return nil
		}
		return err
	}
	return nil
}

// valueBytes approximates the in-memory size of an inserted value (for the
// min_insert_block_size_bytes block bound).
func valueBytes(v any) int64 {
	switch x := v.(type) {
	case nil:
		return 1
	case string:
		return int64(len(x)) + 8
	case []byte:
		return int64(len(x)) + 8
	case []any:
		n := int64(8)
		for _, e := range x {
			n += valueBytes(e)
		}
		return n
	}
	return 16
}

// staleTableError explains a write that failed because the table's physical
// table is gone: when the catalog (reloaded) now holds another incarnation
// (DROP + CREATE or CREATE OR REPLACE elsewhere), the statement used a stale
// definition and the client can simply retry it; the reload makes the retry
// use the new one.
func (q *query) staleTableError(t *catalog.Table, err error) error {
	if !cherr.Is(err, cherr.UnknownTable) {
		return err
	}
	fresh, ferr := q.cat.refreshTable(t.Database, t.Name)
	if ferr != nil {
		if cherr.Is(ferr, cherr.UnknownTable) || cherr.Is(ferr, cherr.UnknownDatabase) {
			return ferr
		}
		return err
	}
	if fresh.PhysicalName != t.PhysicalName || !fresh.CreatedAt.Equal(t.CreatedAt) {
		return cherr.Wrap(cherr.UnknownTable, err,
			"table %s was dropped and created again while this statement ran; retry it", t.FullName())
	}
	return err
}

func (q *query) insertRelation(t *catalog.Table, cols []int, rel *relation) (uint64, error) {
	if len(rel.cols) != len(cols) {
		return 0, cherr.New(cherr.NumberOfArgsDoesntMatch, "Number of columns doesn't match: SELECT returns %d columns, INSERT expects %d", len(rel.cols), len(cols))
	}
	in, err := q.newInserter(t)
	if err != nil {
		return 0, err
	}
	it, err := rel.open()
	if err != nil {
		return 0, err
	}
	defer it.close()
	for {
		r, err := it.next()
		if isEOF(err) {
			break
		}
		if err != nil {
			return 0, err
		}
		row, present := make([]any, len(t.Columns)), make([]bool, len(t.Columns))
		for i, ci := range cols {
			v, ok, err := q.convertTo(r[i], rel.cols[i].Type, t.Columns[ci].Type)
			if err != nil {
				return 0, err
			}
			row[ci], present[ci] = v, ok
		}
		if err := in.add(row, present); err != nil {
			return 0, err
		}
	}
	if err := in.flush(); err != nil {
		return 0, err
	}
	return in.total, nil
}

func (q *query) insertValues(t *catalog.Table, cols []int, values [][]sqlparse.Expr) (uint64, error) {
	in, err := q.newInserter(t)
	if err != nil {
		return 0, err
	}
	c := &compiler{q: q, env: &planEnv{}, sc: &scope{}, clause: "VALUES"}
	for rn, vals := range values {
		if len(vals) != len(cols) {
			return 0, cherr.New(cherr.NumberOfArgsDoesntMatch, "Row %d has %d values, expected %d columns", rn+1, len(vals), len(cols))
		}
		row, present := make([]any, len(t.Columns)), make([]bool, len(t.Columns))
		for i, ci := range cols {
			if id, ok := vals[i].(*sqlparse.Ident); ok && len(id.Parts) == 1 && strings.EqualFold(id.Parts[0], "DEFAULT") {
				continue
			}
			if v, ok := q.parseValuesLiteral(vals[i], t.Columns[ci].Type); ok {
				row[ci], present[ci] = v, true
				continue
			}
			x, err := c.compile(vals[i])
			if err != nil {
				return 0, err
			}
			v, err := x.eval(nil)
			if err != nil {
				return 0, err
			}
			if row[ci], present[ci], err = q.convertTo(v, x.t, t.Columns[ci].Type); err != nil {
				code := cherr.CannotParseText
				if ce, ok := err.(*cherr.Error); ok {
					code = ce.Code
				}
				return 0, cherr.Wrap(code, err, "Cannot convert value of column %s in row %d", t.Columns[ci].Name, rn+1)
			}
		}
		if err := in.add(row, present); err != nil {
			return 0, err
		}
	}
	if err := in.flush(); err != nil {
		return 0, err
	}
	return in.total, nil
}

// parseValuesLiteral parses a plain VALUES literal (number, string, or an
// array/tuple of them) with the column type's quoted-text parser, as
// ClickHouse's Values format does before falling back to evaluating the
// value as an expression. This matters where parsing and CAST differ, e.g.
// an integer literal for a DateTime64(3) column is a tick count
// (1714989600250 -> 2024-05-06 10:00:00.250), not unix seconds. ok is false
// when e is not a plain literal or does not parse; the caller then evaluates
// it as an expression.
func (q *query) parseValuesLiteral(e sqlparse.Expr, t chtype.Type) (any, bool) {
	if lit, ok := e.(*sqlparse.Literal); ok && lit.Value == nil {
		return nil, false // NULL: the expression path applies defaults
	}
	text, ok := valuesLiteralText(e)
	if !ok {
		return nil, false
	}
	v, n, err := chtype.ParseQuoted(q.withServerTZ(t), text)
	if err != nil || n != len(text) {
		return nil, false
	}
	return v, true
}

// valuesLiteralText renders e back to literal text when it is a plain
// literal or an array/tuple literal of plain literals.
func valuesLiteralText(e sqlparse.Expr) (string, bool) {
	switch n := e.(type) {
	case *sqlparse.Literal:
		if n.Raw == "" {
			return "", false
		}
		return n.Raw, true
	case *sqlparse.ArrayExpr, *sqlparse.TupleExpr:
		var elems []sqlparse.Expr
		open, close := "[", "]"
		if a, ok := n.(*sqlparse.ArrayExpr); ok {
			if a.Func {
				return "", false
			}
			elems = a.Elems
		} else {
			tu := n.(*sqlparse.TupleExpr)
			if tu.Func || len(tu.Elems) == 0 {
				return "", false
			}
			elems, open, close = tu.Elems, "(", ")"
		}
		parts := make([]string, len(elems))
		for i, x := range elems {
			p, ok := valuesLiteralText(x)
			if !ok {
				return "", false
			}
			parts[i] = p
		}
		return open + strings.Join(parts, ", ") + close, true
	}
	return "", false
}

// withServerTZ gives a DateTime/DateTime64 column type without an explicit
// time zone the server time zone, so text parses as server-local time.
func (q *query) withServerTZ(t chtype.Type) chtype.Type {
	switch t.Kind {
	case chtype.Nullable, chtype.LowCardinality:
		e := q.withServerTZ(*t.Elem)
		t.Elem = &e
	case chtype.DateTime, chtype.DateTime64:
		if l := q.location(t); t.TZ == "" && l != time.UTC {
			t.TZ = l.String()
		}
	}
	return t
}

func (q *query) insertData(t *catalog.Table, cols []int, s *sqlparse.InsertStmt) (uint64, error) {
	var readers []io.Reader
	if s.Data != "" {
		readers = append(readers, strings.NewReader(s.Data))
	}
	if q.req != nil && q.req.Data != nil {
		readers = append(readers, q.req.Data)
	}
	name := s.Format
	if name == "" {
		name = "Values"
	}
	if _, ok := format.Canonical(name); !ok {
		if unsupportedInputFormats[strings.ToLower(name)] {
			return 0, cherr.New(cherr.NotImplemented, "Input format %s is not supported by this server", name)
		}
		return 0, cherr.New(cherr.UnknownFormat, "Unknown format %s", name)
	}
	if len(readers) == 0 {
		return 0, nil
	}
	fc := make([]format.Column, len(cols))
	for i, ci := range cols {
		fc[i] = format.Column{Name: t.Columns[ci].Name, Type: t.Columns[ci].Type}
	}
	opts := format.Options{Timezone: q.timezoneName(), Settings: q.settings}
	if q.req != nil {
		opts = q.req.FormatOptions
		if opts.Timezone == "" {
			opts.Timezone = q.timezoneName()
		}
		merged := map[string]string{}
		for k, v := range opts.Settings {
			merged[k] = v
		}
		for k, v := range q.settings {
			merged[k] = v
		}
		opts.Settings = merged
	}
	dec, err := format.NewDecoder(name, io.MultiReader(readers...), fc, opts)
	if err != nil {
		return 0, err
	}
	in, err := q.newInserter(t)
	if err != nil {
		return 0, err
	}
	for {
		vals, err := dec.Next()
		if isEOF(err) {
			break
		}
		if err != nil {
			return 0, err
		}
		if len(vals) != len(cols) {
			return 0, cherr.New(cherr.LogicalError, "decoder returned %d values for %d columns", len(vals), len(cols))
		}
		row, present := make([]any, len(t.Columns)), make([]bool, len(t.Columns))
		for i, ci := range cols {
			v := vals[i]
			if v == format.Missing {
				continue
			}
			if v == nil && !t.Columns[ci].Type.IsNullable() {
				continue
			}
			row[ci], present[ci] = v, true
		}
		if err := in.add(row, present); err != nil {
			return 0, err
		}
	}
	if err := in.flush(); err != nil {
		return 0, err
	}
	return in.total, nil
}

// paramLiteral renders a parameter value the way ClickHouse prints the
// parsed constant in column names: dates as day numbers, DateTime as unix
// seconds, DateTime64 as a quoted decimal, Bool as a quoted word.
func paramLiteral(v any, t chtype.Type) string {
	u := t.Unwrap()
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		return quoteString(x)
	case bool:
		if x {
			return "'true'"
		}
		return "'false'"
	case uint64:
		return strconv.FormatUint(x, 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return floatLiteral(x)
	case time.Time:
		switch u.Kind {
		case chtype.Date, chtype.Date32:
			return strconv.FormatInt(dayNum(x.UTC()), 10)
		case chtype.DateTime:
			return strconv.FormatInt(x.Unix(), 10)
		case chtype.DateTime64:
			s := strconv.FormatInt(x.Unix(), 10)
			if u.Precision > 0 {
				frac := fmt.Sprintf("%09d", x.Nanosecond())
				s += "." + frac[:u.Precision]
			}
			return quoteString(s)
		}
	case []any:
		if u.Kind == chtype.Map && u.Key != nil && u.Elem != nil {
			// Maps print as arrays of (key, value) tuples.
			parts := make([]string, 0, len(x))
			for _, e := range x {
				kv, ok := e.([]any)
				if !ok || len(kv) != 2 {
					continue
				}
				parts = append(parts, "("+paramLiteral(kv[0], *u.Key)+", "+paramLiteral(kv[1], *u.Elem)+")")
			}
			return "[" + strings.Join(parts, ", ") + "]"
		}
		parts := make([]string, len(x))
		for i, e := range x {
			et := chtype.TNothing
			switch {
			case u.Kind == chtype.Array && u.Elem != nil:
				et = *u.Elem
			case u.Kind == chtype.Tuple && i < len(u.Fields):
				et = u.Fields[i].Type
			}
			parts[i] = paramLiteral(e, et)
		}
		if u.Kind == chtype.Tuple {
			return "(" + strings.Join(parts, ", ") + ")"
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	return quoteString(chtype.FormatText(t, v))
}

// floatLiteral renders a float like ClickHouse literals ("1." for integral values).
func floatLiteral(f float64) string {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	s := strconv.FormatFloat(f, 'g', -1, 64)
	if !strings.ContainsAny(s, ".e") {
		s += "."
	}
	return s
}

// unsupportedInputFormats are ClickHouse input formats this server does not
// implement (DESIGN §3: NOT_IMPLEMENTED (48)); other unknown names are
// UNKNOWN_FORMAT (73) like ClickHouse. Keys are lower case.
var unsupportedInputFormats = func() map[string]bool {
	m := map[string]bool{}
	for _, n := range []string{
		"RowBinary", "RowBinaryWithNames", "RowBinaryWithNamesAndTypes", "RowBinaryWithDefaults",
		"Parquet", "ParquetMetadata", "Arrow", "ArrowStream", "ORC", "Avro", "AvroConfluent",
		"Protobuf", "ProtobufSingle", "ProtobufList", "CapnProto", "MsgPack", "BSONEachRow", "Npy",
		"Template", "TemplateIgnoreSpaces", "CustomSeparated", "CustomSeparatedWithNames",
		"CustomSeparatedWithNamesAndTypes", "CustomSeparatedIgnoreSpaces", "Regexp", "LineAsString",
		"LineAsStringWithNames", "LineAsStringWithNamesAndTypes", "RawBLOB", "MySQLDump", "JSONAsString",
		"JSONAsObject", "JSONColumns", "JSONColumnsWithMetadata", "JSONCompactColumns", "JSONObjectEachRow",
		"DWARF", "Form", "One", "TSKV",
	} {
		m[strings.ToLower(n)] = true
	}
	return m
}()
