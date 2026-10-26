package exec

import (
	"io"
	"math"
	"sort"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/sqlparse"
	"github.com/gfronza/twinhouse/internal/storage"
)

// source is a planned FROM item.
type source struct {
	sc *scope
	// table is set for storage-backed sources (enables key pushdown and
	// projection pushdown through ScanRequest).
	table *catalog.Table
	open  func(req *storage.ScanRequest) (rowIter, error)
	// tableCols is set for table-shaped sources (storage or MV rows) so ALIAS
	// columns can be computed.
	tableCols []catalog.Column
	aliasX    map[int]*expr
	aliasOrd  []int
	// defaultX holds compiled DEFAULT/MATERIALIZED expressions of used
	// columns, evaluated for items stored before the column existed.
	defaultX map[int]*expr
	// inner is the wrapped source (ARRAY JOIN) that owns ALIAS columns.
	inner *source
	// finalizeFn overrides finalize (JOIN sources).
	finalizeFn func(c *compiler) error
	// noLimit disables the storage Limit hint (row count changes downstream).
	noLimit bool
	// joined marks the result of a JOIN.
	joined bool
}

// finalize compiles ALIAS columns referenced by the query.
func (s *source) finalize(c *compiler) error {
	if s.finalizeFn != nil {
		return s.finalizeFn(c)
	}
	if s.inner != nil {
		return s.inner.finalize(c)
	}
	if s.tableCols == nil {
		return nil
	}
	s.aliasX = map[int]*expr{}
	if s.table != nil {
		s.defaultX = compileDefaults(c, s.sc, s.tableCols, func(i int) bool { return c.used[i] })
	}
	for {
		progress := false
		for i := range s.tableCols {
			col := &s.tableCols[i]
			if col.DefaultKind != "ALIAS" || !c.used[i] || s.aliasX[i] != nil {
				continue
			}
			e, err := sqlparse.ParseExpr(col.DefaultExpr)
			if err != nil {
				return cherr.Wrap(cherr.LogicalError, err, "cannot parse ALIAS expression of column %s", col.Name)
			}
			ac := &compiler{q: c.q, env: &planEnv{}, sc: s.sc, used: c.used, clause: "ALIAS column " + col.Name}
			x, err := ac.compile(e)
			if err != nil {
				return err
			}
			if x, err = castExpr(x, col.Type, false); err != nil {
				return err
			}
			s.aliasX[i] = x
			s.aliasOrd = append(s.aliasOrd, i)
			progress = true
		}
		if !progress {
			return nil
		}
	}
}

// scanColumns returns the stored column indexes needed by the query.
func (s *source) scanColumns(used map[int]bool) []int {
	cols := []int{}
	for i := range s.tableCols {
		if used[i] && s.tableCols[i].DefaultKind != "ALIAS" {
			cols = append(cols, i)
		}
	}
	sort.Ints(cols)
	if len(cols) == 0 && len(s.tableCols) > 0 {
		// Read a single (key) column when no column is needed (count()).
		i := 0
		if s.table != nil && len(s.table.OrderBy) > 0 {
			if k := s.table.ColumnIndex(s.table.OrderBy[0]); k >= 0 {
				i = k
			}
		}
		cols = append(cols, i)
	}
	return cols
}

// compileDefaults compiles the DEFAULT/MATERIALIZED expressions of the
// columns selected by want (in column order, so that a default may refer to
// columns compiled before it). Columns referenced by a default are marked
// used through c.used. A default that does not compile is skipped (the
// store's zero value is kept), so reads never fail because of it.
func compileDefaults(c *compiler, sc *scope, cols []catalog.Column, want func(int) bool) map[int]*expr {
	var out map[int]*expr
	for i := range cols {
		col := &cols[i]
		if col.DefaultExpr == "" || (col.DefaultKind != "DEFAULT" && col.DefaultKind != "MATERIALIZED") || !want(i) {
			continue
		}
		e, err := sqlparse.ParseExpr(col.DefaultExpr)
		if err != nil {
			continue
		}
		dc := &compiler{q: c.q, env: &planEnv{}, sc: sc, used: c.used, clause: "default expression of column " + col.Name}
		x, err := dc.compile(e)
		if err != nil {
			continue
		}
		if x, err = castExpr(x, col.Type, false); err != nil {
			continue
		}
		if out == nil {
			out = map[int]*expr{}
		}
		out[i] = x
	}
	return out
}

// absentColumns appends to dst the indexes of row values that are
// storage.Absent (columns added by ALTER after the row was written).
func absentColumns(row []any, dst []int) []int {
	for i, v := range row {
		if storage.IsAbsent(v) {
			dst = append(dst, i)
		}
	}
	return dst
}

// fillMissing replaces the values of columns absent from the stored item
// (missing, plus nil values of non-Nullable columns that have a default) by
// their default expression, or by the type's zero value when there is none.
func fillMissing(row []any, cols []catalog.Column, defaults map[int]*expr, missing []int) error {
	fill := func(i int) error {
		if i < 0 || i >= len(row) || i >= len(cols) {
			return nil
		}
		if x := defaults[i]; x != nil {
			v, err := x.eval(row)
			if err != nil {
				return err
			}
			row[i] = v
			return nil
		}
		if !cols[i].Type.IsNullable() {
			row[i] = zeroValue(cols[i].Type)
		} else {
			row[i] = nil
		}
		return nil
	}
	done := map[int]bool{}
	for _, i := range missing {
		if !done[i] {
			done[i] = true
			if err := fill(i); err != nil {
				return err
			}
		}
	}
	for i := range defaults {
		if !done[i] && row[i] == nil && !cols[i].Type.IsNullable() {
			if err := fill(i); err != nil {
				return err
			}
		}
	}
	return nil
}

// fillAliases computes ALIAS columns of a table row in place.
func (s *source) fillAliases(row []any) error {
	for k := len(s.aliasOrd) - 1; k >= 0; k-- {
		i := s.aliasOrd[k]
		v, err := s.aliasX[i].eval(row)
		if err != nil {
			return err
		}
		row[i] = v
	}
	return nil
}

func tableScope(t *catalog.Table, alias string) *scope {
	quals := []string{t.Name, t.Database + "." + t.Name}
	if alias != "" {
		quals = append([]string{alias}, quals...)
	}
	sc := &scope{cols: make([]scopeCol, len(t.Columns))}
	for i, c := range t.Columns {
		sc.cols[i] = scopeCol{name: c.Name, quals: quals, t: c.Type, hidden: c.DefaultKind == "MATERIALIZED" || c.DefaultKind == "ALIAS" || c.DefaultKind == "EPHEMERAL"}
	}
	return sc
}

// planFrom plans the FROM clause.
func (q *query) planFrom(te *sqlparse.TableExpr, env *planEnv) (*source, error) {
	if te == nil {
		return oneSource(), nil
	}
	switch {
	case te.Subquery != nil:
		rel, err := q.planSelect(te.Subquery, env)
		if err != nil {
			return nil, err
		}
		return relSource(rel, te.Alias), nil
	case te.Func != nil:
		return q.tableFunction(te.Func, te.Alias, env)
	}
	if te.Database == "" {
		if c := env.lookupCTE(te.Table); c != nil {
			c.active = true
			rel, err := q.planSelect(c.sel, c.env)
			c.active = false
			if err != nil {
				return nil, err
			}
			alias := te.Alias
			if alias == "" {
				alias = te.Table
			}
			return relSource(rel, alias), nil
		}
	}
	db := q.defaultDB(te.Database)
	if mv := env.mvFor(db, te.Table); mv != nil {
		return q.mvRowsSource(mv, te.Alias), nil
	}
	if db == "system" || db == "INFORMATION_SCHEMA" || db == "information_schema" {
		return q.systemTable(db, te.Table, te.Alias)
	}
	t, err := q.cat.Table(db, te.Table)
	if err == nil {
		return q.storageSource(t, te.Alias), nil
	}
	if v, verr := q.cat.View(db, te.Table); verr == nil {
		if v.Materialized {
			if v.ToTable == "" {
				return nil, cherr.New(cherr.NotImplemented, "materialized view %s has no TO table", v.FullName())
			}
			t, err := q.cat.Table(v.ToDatabase, v.ToTable)
			if err != nil {
				return nil, err
			}
			return q.storageSource(t, firstNonEmpty(te.Alias, te.Table)), nil
		}
		sel, err := parseSelect(v.SelectSQL)
		if err != nil {
			return nil, err
		}
		inner := q.withDB(v.Database)
		rel, err := inner.planSelect(sel, &planEnv{})
		if err != nil {
			return nil, err
		}
		return relSource(rel, firstNonEmpty(te.Alias, te.Table)), nil
	}
	if _, derr := q.cat.Database(db); derr != nil {
		return nil, derr
	}
	return nil, err
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// withDB returns a shallow copy of q with another current database.
func (q *query) withDB(db string) *query {
	if db == q.db {
		return q
	}
	return &query{e: q.e, ctx: q.ctx, cat: q.cat, db: db, settings: q.settings, params: q.params, bound: q.bound,
		now: q.now, loc: q.loc, user: q.user, id: q.id, req: q.req, depth: q.depth}
}

func parseSelect(text string) (*sqlparse.SelectStmt, error) {
	st, err := sqlparse.Parse(text)
	if err != nil {
		return nil, err
	}
	sel, ok := st.(*sqlparse.SelectStmt)
	if !ok {
		return nil, cherr.New(cherr.LogicalError, "view definition is not a SELECT")
	}
	return sel, nil
}

func relSource(rel *relation, alias string) *source {
	var quals []string
	if alias != "" {
		quals = []string{alias}
	}
	sc := &scope{cols: make([]scopeCol, len(rel.cols))}
	for i, c := range rel.cols {
		sc.cols[i] = scopeCol{name: c.Name, quals: quals, t: c.Type}
	}
	return &source{sc: sc, open: func(*storage.ScanRequest) (rowIter, error) { return rel.open() }}
}

func oneSource() *source {
	sc := &scope{cols: []scopeCol{{name: "dummy", quals: []string{"one", "system.one"}, t: chtype.TUInt8}}}
	return &source{sc: sc, open: func(*storage.ScanRequest) (rowIter, error) {
		return &sliceIter{rows: [][]any{{uint64(0)}}}, nil
	}}
}

func (q *query) storageSource(t *catalog.Table, alias string) *source {
	src := &source{sc: tableScope(t, alias), table: t, tableCols: t.Columns}
	src.open = func(req *storage.ScanRequest) (rowIter, error) {
		r := storage.ScanRequest{Consistent: q.consistent()}
		if req != nil {
			r = *req
		}
		// Columns added by ALTER after a row was written come back as
		// storage.Absent; scanIter evaluates their defaults.
		r.MarkAbsent = true
		it, err := q.e.cfg.Store.Scan(q.ctx, t, r)
		if err != nil {
			return nil, err
		}
		return &scanIter{q: q, it: it, src: src, width: len(t.Columns)}, nil
	}
	return src
}

func (q *query) mvRowsSource(mv *mvSource, alias string) *source {
	t := mv.table
	src := &source{sc: tableScope(t, alias), tableCols: t.Columns}
	src.open = func(*storage.ScanRequest) (rowIter, error) {
		rows := make([][]any, len(mv.rows))
		for i, r := range mv.rows {
			row := make([]any, len(t.Columns))
			copy(row, r)
			if err := src.fillAliases(row); err != nil {
				return nil, err
			}
			rows[i] = row
		}
		return &sliceIter{rows: rows}, nil
	}
	return src
}

// scanIter adapts a storage iterator.
type scanIter struct {
	q     *query
	it    storage.RowIterator
	src   *source
	width int
	// missing lists the columns absent from the last row's stored item.
	missing []int
}

func (s *scanIter) next() ([]any, error) {
	r, err := s.it.Next(s.q.ctx)
	if err != nil {
		return nil, err
	}
	s.q.readRows.Add(1)
	s.q.readBytes.Add(approxSize(r))
	row := []any(r)
	if len(row) < s.width {
		row = append(row, make([]any, s.width-len(row))...)
	}
	s.missing = absentColumns(row, s.missing[:0])
	if len(s.missing) > 0 || len(s.src.defaultX) > 0 {
		if err := fillMissing(row, s.src.tableCols, s.src.defaultX, s.missing); err != nil {
			return nil, err
		}
	}
	if err := s.src.fillAliases(row); err != nil {
		return nil, err
	}
	return row, nil
}

func (s *scanIter) close() { _ = s.it.Close() }

// approxSize estimates the in-memory size of a row for read_bytes statistics.
func approxSize(row []any) uint64 {
	var n uint64
	for _, v := range row {
		switch x := v.(type) {
		case nil:
		case string:
			n += uint64(len(x))
		case []any:
			n += approxSize(x)
		case bool:
			n++
		default:
			n += 8
		}
	}
	return n
}

// tableFunction plans numbers(N) / numbers(offset, N).
func (q *query) tableFunction(f *sqlparse.FuncCall, alias string, env *planEnv) (*source, error) {
	switch f.Name {
	case "numbers", "numbers_mt":
		c := &compiler{q: q, env: env, sc: &scope{}, clause: "table function " + f.Name}
		var vals []uint64
		for _, a := range f.Args {
			x, err := c.compile(a)
			if err != nil {
				return nil, err
			}
			if !x.konst || !isIntKind(base(x).Kind) || asInt(x.val) < 0 {
				return nil, cherr.New(cherr.BadArguments, "Arguments of table function %s must be constant non-negative integers", f.Name)
			}
			vals = append(vals, asUint(x.val))
		}
		var from, count uint64
		switch len(vals) {
		case 1:
			count = vals[0]
		case 2, 3:
			from, count = vals[0], vals[1]
		default:
			return nil, argCountError(f.Name, len(vals), "1 or 2")
		}
		return q.numbersSource(from, count, firstNonEmpty(alias, f.Name)), nil
	}
	return nil, cherr.New(cherr.UnknownFunction, "Unknown table function %s", f.Name)
}

func (q *query) numbersSource(from, count uint64, qual string) *source {
	sc := &scope{cols: []scopeCol{{name: "number", quals: []string{qual, "system.numbers"}, t: chtype.TUInt64}}}
	return &source{sc: sc, open: func(*storage.ScanRequest) (rowIter, error) {
		end := uint64(math.MaxUint64)
		if count != math.MaxUint64 && from+count >= from {
			end = from + count
		}
		return &numbersIter{q: q, cur: from, end: end}, nil
	}}
}

type numbersIter struct {
	q        *query
	cur, end uint64
}

func (n *numbersIter) next() ([]any, error) {
	if n.cur >= n.end {
		return nil, io.EOF
	}
	if n.cur&4095 == 0 {
		if err := n.q.ctx.Err(); err != nil {
			return nil, cherr.Wrap(cherr.Timeout, err, "query cancelled")
		}
	}
	v := n.cur
	n.cur++
	n.q.readRows.Add(1)
	n.q.readBytes.Add(8)
	return []any{v}, nil
}

func (n *numbersIter) close() {}
