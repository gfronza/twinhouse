package exec

import (
	"io"
	"sort"
	"strings"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec/aggstate"
	"github.com/gfronza/twinhouse/internal/format"
	"github.com/gfronza/twinhouse/internal/limits"
	"github.com/gfronza/twinhouse/internal/sqlparse"
	"github.com/gfronza/twinhouse/internal/storage"
)

// rowIter streams rows; next returns (nil, io.EOF) at the end.
type rowIter interface {
	next() ([]any, error)
	close()
}

// relation is a planned query: its output columns and a way to run it. A
// relation may be opened several times (materialized views run their plan
// once per source row).
type relation struct {
	cols []format.Column
	open func() (rowIter, error)
	// stats receives LIMIT bookkeeping of the outermost SELECT.
	stats *limitStats
}

type limitStats struct {
	hasLimit        bool
	rowsBeforeLimit uint64
}

// collect runs the relation and returns all rows; max > 0 stops after max rows.
func (r *relation) collect(q *query, max int) ([][]any, error) {
	it, err := r.open()
	if err != nil {
		return nil, err
	}
	defer it.close()
	var rows [][]any
	for {
		if err := q.ctx.Err(); err != nil {
			return nil, cherr.Wrap(cherr.Timeout, err, "query cancelled")
		}
		row, err := it.next()
		if err == io.EOF {
			return rows, nil
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		if max > 0 && len(rows) >= max {
			return rows, nil
		}
	}
}

// planEnv carries names visible while planning: CTEs, WITH aliases, and the
// in-memory source used for materialized-view evaluation.
type planEnv struct {
	parent  *planEnv
	ctes    map[string]*cte
	aliases map[string]sqlparse.Expr
	mv      *mvSource
	// copied is set inside a CTE or view planned for a reference to it.
	copied bool
}

type cte struct {
	sel *sqlparse.SelectStmt
	env *planEnv
	// active is set while the CTE body is planned: inside its own body the
	// name refers to a table (CTEs are not recursive).
	active bool
}

// mvSource replaces a table by rows supplied at run time.
type mvSource struct {
	table *catalog.Table
	rows  []storage.Row
}

func (e *planEnv) nested() *planEnv {
	return &planEnv{parent: e, copied: e.copied}
}

func (e *planEnv) lookupCTE(name string) *cte {
	for p := e; p != nil; p = p.parent {
		if c, ok := p.ctes[name]; ok && !c.active {
			return c
		}
	}
	return nil
}

func (e *planEnv) mvFor(db, table string) *mvSource {
	for p := e; p != nil; p = p.parent {
		if p.mv != nil && p.mv.table.Database == db && p.mv.table.Name == table {
			return p.mv
		}
	}
	return nil
}

// withAliases returns WITH aliases visible from e (inner ones win).
func (e *planEnv) withAliases() map[string]sqlparse.Expr {
	out := map[string]sqlparse.Expr{}
	var chain []*planEnv
	for p := e; p != nil; p = p.parent {
		chain = append(chain, p)
	}
	for i := len(chain) - 1; i >= 0; i-- {
		for k, v := range chain[i].aliases {
			out[k] = v
		}
	}
	return out
}

// runSelect executes a top-level SELECT.
func (q *query) runSelect(s *sqlparse.SelectStmt) (*Result, error) {
	for _, b := range append([]*sqlparse.SelectStmt{s}, s.Union...) {
		if err := q.applySettings(b.Settings); err != nil {
			return nil, err
		}
	}
	rel, err := q.planSelect(s, &planEnv{})
	if err != nil {
		return nil, err
	}
	limit := q.maxResultRows()
	collectMax := 0
	if limit > 0 {
		collectMax = limit + 1
	}
	rows, err := rel.collect(q, collectMax)
	if err != nil {
		return nil, err
	}
	if limit > 0 && len(rows) > limit {
		if q.settings["result_overflow_mode"] != "break" {
			return nil, cherr.New(tooManyRowsOrBytes, "Limit for result exceeded, max rows: %d", limit)
		}
		rows = rows[:limit]
	}
	res := &Result{Columns: rel.cols, Rows: rows, Format: selectFormat(s), ReadRows: q.readRows.Load(), ReadBytes: q.readBytes.Load()}
	if rel.stats != nil && rel.stats.hasLimit {
		res.HasRowsBeforeLimit = true
		res.RowsBeforeLimit = rel.stats.rowsBeforeLimit
	}
	return res, nil
}

func selectFormat(s *sqlparse.SelectStmt) string {
	if s.Format != "" {
		return s.Format
	}
	for i := len(s.Union) - 1; i >= 0; i-- {
		if s.Union[i].Format != "" {
			return s.Union[i].Format
		}
	}
	return ""
}

// maxPlanDepth bounds nesting of subqueries, CTEs and views.
const maxPlanDepth = 64

// planState is what planning one top-level SELECT shares across its
// subqueries, CTEs and views.
type planState struct {
	depth int
	// nodesLeft is how many more nodes the expanded query may have.
	nodesLeft int
}

// maxQueryExpandedNodes bounds the whole expanded query: the nodes of all
// its expressions and frames, plus what each frame planned from a CTE or
// view reference takes (copiedFrameNodes and its source's width).
// ClickHouse checks each expression against MaxExpandedASTElements and
// leaves the sum to max_memory_usage; until twinhouse has that limit, this
// stops many references to one big alias and CTE or view copies that
// double per level. A full budget of copied width plans to about 600 MiB.
const maxQueryExpandedNodes = 6 * limits.MaxExpandedASTElements

// copiedFrameNodes is what a frame copied from a CTE or view takes besides
// its source's width. Planning one costs about as much memory as a dozen
// source columns; rounded up, copies that double per level over a narrow
// source fail from 15 levels (ClickHouse fails them from 12 to 14).
const copiedFrameNodes = 32

// take uses n nodes of the query's budget.
func (p *planState) take(n int) error {
	if n > p.nodesLeft {
		p.nodesLeft = 0
		return cherr.New(cherr.BadArguments, "Query tree is too big. Maximum: %d", maxQueryExpandedNodes)
	}
	p.nodesLeft -= n
	return nil
}

// selectNodes approximates the nodes ClickHouse's AST has for the frame of
// s (the query, its select list and each FROM or JOIN table element), which
// alias expansion does not visit. ClickHouse copies a CTE or view into each
// reference, so one used twice per level runs out of budget too.
func selectNodes(s *sqlparse.SelectStmt) int {
	n := 2
	if s.From != nil {
		n += 4
	}
	return n + 4*len(s.Joins)
}

// planSelect plans a SELECT including UNION branches.
func (q *query) planSelect(s *sqlparse.SelectStmt, env *planEnv) (*relation, error) {
	p := q.plan
	if p.depth == 0 {
		p.nodesLeft = maxQueryExpandedNodes
	}
	if p.depth++; p.depth > maxPlanDepth {
		p.depth--
		return nil, cherr.New(tooDeepRecursion, "Maximum query nesting depth (%d) exceeded (recursive view or subquery?)", maxPlanDepth)
	}
	defer func() { p.depth-- }()
	if len(s.Union) == 0 {
		return q.planSingle(s, env)
	}
	head := *s
	head.Union, head.UnionOps = nil, nil
	branches := []*sqlparse.SelectStmt{&head}
	branches = append(branches, s.Union...)
	rels := make([]*relation, len(branches))
	for i, b := range branches {
		r, err := q.planSelect(b, env)
		if err != nil {
			return nil, err
		}
		rels[i] = r
	}
	return unionRelations(rels, s.UnionOps)
}

func unionRelations(rels []*relation, ops []string) (*relation, error) {
	n := len(rels[0].cols)
	cols := make([]format.Column, n)
	for i := 0; i < n; i++ {
		ts := make([]chtype.Type, len(rels))
		for j, r := range rels {
			if len(r.cols) != n {
				return nil, cherr.New(258, "Different number of columns in UNION elements: %d and %d", n, len(r.cols))
			}
			ts[j] = r.cols[i].Type
		}
		st, err := chtype.Supertype(ts...)
		if err != nil {
			return nil, supertypeError(err, "No common type for column %d of UNION", i+1)
		}
		cols[i] = format.Column{Name: rels[0].cols[i].Name, Type: st}
	}
	convs := make([][]func(any) (any, error), len(rels))
	for j, r := range rels {
		convs[j] = make([]func(any) (any, error), n)
		for i := 0; i < n; i++ {
			c, err := converter(r.cols[i].Type, cols[i].Type)
			if err != nil {
				return nil, err
			}
			convs[j][i] = c
		}
	}
	distinct := false
	for _, op := range ops {
		if strings.EqualFold(op, "DISTINCT") {
			distinct = true
		}
	}
	open := func() (rowIter, error) {
		var it rowIter = &unionIter{rels: rels, convs: convs, idx: -1}
		if distinct {
			it = &distinctIter{in: it, width: n, seen: map[string]struct{}{}}
		}
		return it, nil
	}
	return &relation{cols: cols, open: open}, nil
}

type unionIter struct {
	rels  []*relation
	convs [][]func(any) (any, error)
	idx   int
	cur   rowIter
}

func (u *unionIter) next() ([]any, error) {
	for {
		if u.cur == nil {
			u.idx++
			if u.idx >= len(u.rels) {
				return nil, io.EOF
			}
			it, err := u.rels[u.idx].open()
			if err != nil {
				return nil, err
			}
			u.cur = it
		}
		row, err := u.cur.next()
		if err == io.EOF {
			u.cur.close()
			u.cur = nil
			continue
		}
		if err != nil {
			return nil, err
		}
		out := make([]any, len(row))
		for i, v := range row {
			if v == nil {
				continue
			}
			if out[i], err = u.convs[u.idx][i](v); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
}

func (u *unionIter) close() {
	if u.cur != nil {
		u.cur.close()
	}
}

// selItem is one output column of a SELECT.
type selItem struct {
	expr sqlparse.Expr // original expression
	name string
}

// planSingle plans one SELECT without UNION.
func (q *query) planSingle(s *sqlparse.SelectStmt, parent *planEnv) (*relation, error) {
	if err := q.plan.take(selectNodes(s)); err != nil {
		return nil, err
	}
	env := parent.nested()
	env.ctes = map[string]*cte{}
	env.aliases = map[string]sqlparse.Expr{}
	for _, w := range s.With {
		switch {
		case w.Subquery != nil:
			env.ctes[w.Name] = &cte{sel: w.Subquery, env: env}
		case w.Expr != nil:
			env.aliases[w.Name] = w.Expr
		}
	}
	if s.WithTotals {
		return nil, cherr.New(cherr.NotImplemented, "WITH TOTALS is not supported")
	}
	src, err := q.planFrom(s.From, env)
	if err != nil {
		return nil, err
	}
	if src, err = q.planJoins(src, s.Joins, s.Where, env); err != nil {
		return nil, err
	}
	// A frame copied from a CTE or view holds its own scope of its source's
	// columns, so copies that double per level over a wide table are bounded
	// by width too.
	if env.copied {
		if err := q.plan.take(copiedFrameNodes + len(src.sc.cols)); err != nil {
			return nil, err
		}
	}

	// Select list with * expansion and output names.
	var items []selItem
	namer := &colNamer{q: q, sc: src.sc}
	for _, it := range s.Columns {
		if st, ok := it.Expr.(*sqlparse.Star); ok {
			n := 0
			for _, col := range src.sc.cols {
				if col.hidden || (st.Table != "" && !hasQual(col.quals, st.Table)) {
					continue
				}
				items = append(items, selItem{expr: &sqlparse.Ident{Parts: []string{col.name}}, name: col.name})
				n++
			}
			if n == 0 && st.Table != "" {
				return nil, cherr.New(cherr.UnknownIdentifier, "Unknown table expression identifier '%s'", st.Table)
			}
			continue
		}
		name := it.Alias
		if name == "" {
			name = sqlparse.ColumnNameFunc(it.Expr, namer.hook)
		}
		items = append(items, selItem{expr: it.Expr, name: name})
	}
	if len(items) == 0 {
		return nil, cherr.New(cherr.IncorrectQuery, "Empty list of columns in SELECT query")
	}

	// Alias expansion (ClickHouse resolves aliases in every clause).
	ax := newAliasExpander(q.plan)
	for k, v := range env.withAliases() {
		ax.defs[k] = v
	}
	if err := checkAliases(s); err != nil {
		return nil, err
	}
	for _, it := range s.Columns {
		if it.Alias != "" {
			ax.defs[it.Alias] = it.Expr
		}
	}
	for _, e := range clauseExprs(s) {
		ax.collect(e)
	}
	// selNodes is what each select expression took, taken again by each
	// clause that reuses it.
	selExprs := make([]sqlparse.Expr, len(items))
	selNodes := make([]int, len(items))
	for i, it := range items {
		before := q.plan.nodesLeft
		if alias := aliasOf(s, it); alias != "" {
			selExprs[i], err = ax.expandAs(alias, it.expr)
		} else {
			selExprs[i], err = ax.expand(it.expr)
		}
		if err != nil {
			return nil, err
		}
		selNodes[i] = before - q.plan.nodesLeft
	}
	positional := func(e sqlparse.Expr) (sqlparse.Expr, error) {
		if lit, ok := e.(*sqlparse.Literal); ok {
			if n, ok := lit.Value.(uint64); ok {
				if n < 1 || int(n) > len(selExprs) {
					return nil, cherr.New(cherr.BadArguments, "Positional argument out of bounds: %d (expected in range [1, %d])", n, len(selExprs))
				}
				return selExprs[n-1], q.plan.take(selNodes[n-1])
			}
		}
		return ax.expand(e)
	}
	var where sqlparse.Expr
	for _, w := range []sqlparse.Expr{s.Prewhere, s.Where} {
		if w == nil {
			continue
		}
		if w, err = ax.expand(w); err != nil {
			return nil, err
		}
		if where == nil {
			where = w
		} else {
			where = &sqlparse.BinaryExpr{Op: "AND", Left: where, Right: w}
		}
	}
	var groupBy []sqlparse.Expr
	for _, g := range s.GroupBy {
		e, err := positional(g)
		if err != nil {
			return nil, err
		}
		groupBy = append(groupBy, e)
	}
	having, err := ax.expand(s.Having)
	if err != nil {
		return nil, err
	}
	orderExprs := make([]sqlparse.Expr, len(s.OrderBy))
	for i, o := range s.OrderBy {
		if o.Collate != "" {
			return nil, cherr.New(cherr.NotImplemented, "ORDER BY ... COLLATE is not supported")
		}
		e, err := positional(o.Expr)
		if err != nil {
			return nil, err
		}
		orderExprs[i] = e
	}
	var limitByExprs []sqlparse.Expr
	if s.LimitBy != nil {
		for _, e := range s.LimitBy.By {
			x, err := positional(e)
			if err != nil {
				return nil, err
			}
			limitByExprs = append(limitByExprs, x)
		}
	}

	used := map[int]bool{}
	base := &compiler{q: q, env: env, sc: src.sc, used: used}

	// ARRAY JOIN clause and arrayJoin() calls extend the source.
	whereList := []sqlparse.Expr{where, having}
	if src, err = q.planArrayJoin(base, src, s.ArrayJoin, [][]sqlparse.Expr{selExprs, whereList, groupBy, orderExprs, limitByExprs}); err != nil {
		return nil, err
	}
	where, having = whereList[0], whereList[1]
	base.sc = src.sc

	isAgg := len(groupBy) > 0 || s.GroupByAll || having != nil && hasAggregate(having)
	for _, e := range append(append([]sqlparse.Expr{}, selExprs...), orderExprs...) {
		if hasAggregate(e) {
			isAgg = true
		}
	}
	if s.GroupByAll {
		for i, e := range selExprs {
			if !hasAggregate(e) {
				if err := q.plan.take(selNodes[i]); err != nil {
					return nil, err
				}
				groupBy = append(groupBy, e)
			}
		}
	}

	// WHERE.
	var whereX *expr
	if where != nil {
		wc := base.child()
		wc.clause = "WHERE"
		if whereX, err = wc.compile(where); err != nil {
			return nil, err
		}
		if err := checkCondType("WHERE", whereX, 1); err != nil {
			return nil, cherr.New(cherr.IllegalTypeOfArgument, "Illegal type %s of column for filter. Must be UInt8 or Nullable(UInt8)", whereX.t.String())
		}
	}

	// Projection (and GROUP BY).
	var ac *aggContext
	proj := base
	if isAgg {
		ac = newAggContext()
		gc := base.child()
		gc.clause = "GROUP BY"
		for _, g := range groupBy {
			fp := gc.fingerprint(g)
			if _, dup := ac.keyFP[fp]; dup {
				continue
			}
			x, err := gc.compile(g)
			if err != nil {
				return nil, err
			}
			ac.keyFP[fp] = len(ac.keys)
			ac.keys = append(ac.keys, x)
		}
		proj = &compiler{q: q, env: env, sc: src.sc, used: used, agg: ac}
	}
	selX := make([]*expr, len(selExprs))
	for i, e := range selExprs {
		if selX[i], err = proj.compile(e); err != nil {
			return nil, err
		}
	}
	var havingX *expr
	if having != nil {
		// Without aggregation ClickHouse applies HAVING as a filter over the
		// source rows (aliases are already expanded), like WHERE.
		hc := *proj
		hc.clause = "HAVING"
		if havingX, err = hc.compile(having); err != nil {
			return nil, err
		}
	}
	orderX := make([]*expr, len(orderExprs))
	for i, e := range orderExprs {
		if orderX[i], err = proj.compile(e); err != nil {
			return nil, err
		}
		orderX[i] = orderKeyExpr(orderX[i])
	}
	limitByX := make([]*expr, len(limitByExprs))
	for i, e := range limitByExprs {
		if limitByX[i], err = proj.compile(e); err != nil {
			return nil, err
		}
	}

	// LIMIT / OFFSET.
	cc := &compiler{q: q, env: env, sc: &scope{}, clause: "LIMIT"}
	limit, hasLimit, err := constCount(cc, s.Limit, "LIMIT")
	if err != nil {
		return nil, err
	}
	offset, _, err := constCount(cc, s.Offset, "OFFSET")
	if err != nil {
		return nil, err
	}
	var lbLimit, lbOffset uint64
	if s.LimitBy != nil {
		if lbLimit, _, err = constCount(cc, s.LimitBy.Limit, "LIMIT BY"); err != nil {
			return nil, err
		}
		if lbOffset, _, err = constCount(cc, s.LimitBy.Offset, "LIMIT BY OFFSET"); err != nil {
			return nil, err
		}
	}

	if err := src.finalize(base); err != nil {
		return nil, err
	}

	// Storage read request: key ranges, projection, consistency, limit hint.
	var req *storage.ScanRequest
	if src.table != nil {
		req = &storage.ScanRequest{Consistent: q.consistent()}
		if where != nil {
			if req.Ranges, err = q.pushdown(base, src, where); err != nil {
				return nil, err
			}
		}
		req.Columns = src.scanColumns(used)
		if where == nil && having == nil && !isAgg && len(orderX) == 0 && !s.Distinct && s.LimitBy == nil && hasLimit && !src.noLimit {
			req.Limit = int(limit + offset)
		}
	}

	cols := make([]format.Column, len(items))
	for i := range items {
		cols[i] = format.Column{Name: items[i].name, Type: selX[i].t}
	}
	stats := &limitStats{hasLimit: hasLimit}
	width := len(selX)
	orders := s.OrderBy
	distinct := s.Distinct
	hasLimitBy := s.LimitBy != nil
	open := func() (rowIter, error) {
		it, err := src.open(req)
		if err != nil {
			return nil, err
		}
		if whereX != nil {
			it = &filterIter{in: it, cond: whereX}
		}
		if ac == nil && havingX != nil {
			it = &filterIter{in: it, cond: havingX}
		}
		if ac != nil {
			rows, err := runAggregation(q, it, ac)
			it.close()
			if err != nil {
				return nil, err
			}
			it = &sliceIter{rows: rows}
			if havingX != nil {
				it = &filterIter{in: it, cond: havingX}
			}
		}
		extra := append(append([]*expr{}, orderX...), limitByX...)
		it = &projectIter{in: it, exprs: selX, extra: extra}
		if distinct {
			it = &distinctIter{in: it, width: width, seen: map[string]struct{}{}}
		}
		if len(orderX) > 0 {
			sorted, err := sortRows(q, it, width, orders)
			it.close()
			if err != nil {
				return nil, err
			}
			it = &sliceIter{rows: sorted}
		}
		if hasLimitBy {
			it = &limitByIter{in: it, keyFrom: width + len(orderX), keyTo: width + len(orderX) + len(limitByX), limit: lbLimit, offset: lbOffset, counts: map[string]uint64{}}
		}
		if hasLimit || offset > 0 {
			it = &limitIter{in: it, limit: limit, hasLimit: hasLimit, offset: offset, stats: stats, drain: ac != nil || len(orderX) > 0}
		}
		return &trimIter{in: it, width: width}, nil
	}
	return &relation{cols: cols, open: open, stats: stats}, nil
}

// aliasOf returns the alias of the select item corresponding to it.
func aliasOf(s *sqlparse.SelectStmt, it selItem) string {
	for _, c := range s.Columns {
		if c.Expr == it.expr {
			return c.Alias
		}
	}
	return ""
}

func hasQual(quals []string, q string) bool {
	for _, x := range quals {
		if x == q {
			return true
		}
	}
	return false
}

// checkAliases rejects, like ClickHouse, one alias naming two different
// expressions in the same SELECT (MULTIPLE_EXPRESSIONS_FOR_ALIAS).
func checkAliases(s *sqlparse.SelectStmt) error {
	defs := map[string]string{}
	var err error
	add := func(alias string, e sqlparse.Expr) {
		if err != nil || alias == "" {
			return
		}
		text := sqlparse.String(e)
		if prev, ok := defs[alias]; ok && prev != text {
			err = cherr.New(multipleExpressionsForAlias, "Multiple expressions %s AS %s and %s AS %s for alias %s", text, alias, prev, alias, alias)
			return
		}
		defs[alias] = text
	}
	nested := func(e sqlparse.Expr) {
		walk(e, func(n sqlparse.Expr) bool {
			if x, ok := n.(*sqlparse.AliasExpr); ok {
				add(x.Alias, x.X)
			}
			_, isSub := n.(*sqlparse.SubqueryExpr)
			return !isSub
		})
	}
	for _, c := range s.Columns {
		nested(c.Expr)
		add(c.Alias, c.Expr)
	}
	for _, e := range clauseExprs(s)[len(s.Columns):] {
		nested(e)
	}
	return err
}

// clauseExprs lists the expressions of s that may define nested aliases.
func clauseExprs(s *sqlparse.SelectStmt) []sqlparse.Expr {
	var out []sqlparse.Expr
	for _, c := range s.Columns {
		out = append(out, c.Expr)
	}
	out = append(out, s.Where, s.Prewhere, s.Having)
	out = append(out, s.GroupBy...)
	for _, o := range s.OrderBy {
		out = append(out, o.Expr)
	}
	return out
}

// constCount evaluates a LIMIT/OFFSET expression.
func constCount(c *compiler, e sqlparse.Expr, what string) (uint64, bool, error) {
	if e == nil {
		return 0, false, nil
	}
	x, err := c.compile(e)
	if err != nil {
		return 0, false, err
	}
	if !x.konst || !isIntKind(base(x).Kind) {
		return 0, false, cherr.New(cherr.IncorrectQuery, "The value of %s expression must be a constant non-negative integer", what)
	}
	switch v := x.val.(type) {
	case uint64:
		return v, true, nil
	case int64:
		if v < 0 {
			return 0, false, cherr.New(cherr.NotImplemented, "negative %s is not supported", what)
		}
		return uint64(v), true, nil
	}
	return 0, false, cherr.New(cherr.IncorrectQuery, "The value of %s expression must be a constant non-negative integer", what)
}

// colNamer names result columns: bound `{p:T}` parameters are named the
// way ClickHouse does (`'v'` for String, `_CAST(<literal>, 'T')` otherwise)
// and scalar subqueries are numbered across the select list.
type colNamer struct {
	q    *query
	sc   *scope
	subq int
}

func (n *colNamer) hook(e sqlparse.Expr) (string, bool) {
	switch x := e.(type) {
	case *sqlparse.SubqueryExpr:
		n.subq++
		return "_subquery_" + itoa(n.subq), true
	case *sqlparse.Ident:
		// A column is named by its resolved name: JOIN columns that clash
		// with the left side are shown qualified ("t2.k").
		if i, err := n.sc.lookup(x.Parts); err == nil && i >= 0 && !strings.HasPrefix(n.sc.cols[i].name, "\x00") {
			return n.sc.cols[i].name, true
		}
		if len(x.Parts) > 1 {
			// Possibly a named tuple element (tp.x): ClickHouse keeps the path.
			return strings.Join(x.Parts, "."), true
		}
		return "", false
	}
	p, ok := e.(*sqlparse.Param)
	if !ok {
		return "", false
	}
	x, err := n.q.param(p)
	if err != nil {
		return "", false
	}
	if x.t.Kind == chtype.String {
		if s, ok := x.val.(string); ok {
			return quoteString(s), true
		}
	}
	raw := p.RawType
	if raw == "" {
		raw = p.Type
	}
	return "_CAST(" + paramLiteral(x.val, x.t) + ", " + quoteString(raw) + ")", true
}

// ---------------------------------------------------------------------------
// Iterators

type sliceIter struct {
	rows [][]any
	pos  int
}

func (s *sliceIter) next() ([]any, error) {
	if s.pos >= len(s.rows) {
		return nil, io.EOF
	}
	r := s.rows[s.pos]
	s.pos++
	return r, nil
}

func (s *sliceIter) close() {}

type filterIter struct {
	in   rowIter
	cond *expr
}

func (f *filterIter) next() ([]any, error) {
	for {
		row, err := f.in.next()
		if err != nil {
			return nil, err
		}
		v, err := f.cond.eval(row)
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		ok, err := truth(v)
		if err != nil {
			return nil, err
		}
		if ok {
			return row, nil
		}
	}
}

func (f *filterIter) close() { f.in.close() }

type projectIter struct {
	in    rowIter
	exprs []*expr
	extra []*expr
}

func (p *projectIter) next() ([]any, error) {
	row, err := p.in.next()
	if err != nil {
		return nil, err
	}
	out := make([]any, len(p.exprs)+len(p.extra))
	for i, x := range p.exprs {
		if out[i], err = x.eval(row); err != nil {
			return nil, err
		}
	}
	for i, x := range p.extra {
		if out[len(p.exprs)+i], err = x.eval(row); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (p *projectIter) close() { p.in.close() }

type distinctIter struct {
	in    rowIter
	width int
	seen  map[string]struct{}
}

func (d *distinctIter) next() ([]any, error) {
	for {
		row, err := d.in.next()
		if err != nil {
			return nil, err
		}
		var b []byte
		for _, v := range row[:d.width] {
			b = aggstate.AppendKey(b, v)
		}
		k := string(b)
		if _, ok := d.seen[k]; ok {
			continue
		}
		d.seen[k] = struct{}{}
		return row, nil
	}
}

func (d *distinctIter) close() { d.in.close() }

type limitByIter struct {
	in             rowIter
	keyFrom, keyTo int
	limit, offset  uint64
	counts         map[string]uint64
}

func (l *limitByIter) next() ([]any, error) {
	for {
		row, err := l.in.next()
		if err != nil {
			return nil, err
		}
		var b []byte
		for _, v := range row[l.keyFrom:l.keyTo] {
			b = aggstate.AppendKey(b, v)
		}
		k := string(b)
		n := l.counts[k]
		l.counts[k] = n + 1
		if n >= l.offset && n < l.offset+l.limit {
			return row, nil
		}
	}
}

func (l *limitByIter) close() { l.in.close() }

type limitIter struct {
	in       rowIter
	limit    uint64
	hasLimit bool
	offset   uint64
	seen     uint64
	stats    *limitStats
	done     bool
	// drain counts the remaining input rows once the limit is reached (for
	// rows_before_limit); used when the input is already materialized.
	drain bool
}

func (l *limitIter) next() ([]any, error) {
	for {
		if l.done || (l.hasLimit && l.seen >= l.offset+l.limit) {
			if !l.done && l.drain {
				for {
					if _, err := l.in.next(); err != nil {
						break
					}
					l.seen++
				}
				l.stats.rowsBeforeLimit = l.seen
			}
			l.done = true
			return nil, io.EOF
		}
		row, err := l.in.next()
		if err != nil {
			return nil, err
		}
		l.seen++
		l.stats.rowsBeforeLimit = l.seen
		if l.seen > l.offset {
			return row, nil
		}
	}
}

func (l *limitIter) close() { l.in.close() }

type trimIter struct {
	in    rowIter
	width int
}

func (t *trimIter) next() ([]any, error) {
	row, err := t.in.next()
	if err != nil {
		return nil, err
	}
	if len(row) > t.width {
		row = row[:t.width]
	}
	return row, nil
}

func (t *trimIter) close() { t.in.close() }

// ---------------------------------------------------------------------------
// Aggregation and sorting

type group struct {
	keys []any
	accs []*acc
}

// runAggregation consumes it and returns post-aggregation rows
// (GROUP BY keys followed by aggregate results).
func runAggregation(q *query, it rowIter, ac *aggContext) ([][]any, error) {
	groups := map[string]*group{}
	var order []*group
	newGroup := func(keys []any) *group {
		g := &group{keys: keys, accs: make([]*acc, len(ac.calls))}
		for i, c := range ac.calls {
			g.accs[i] = c.newAcc()
		}
		order = append(order, g)
		return g
	}
	var kb []byte
	for n := 0; ; n++ {
		if n&1023 == 0 {
			if err := q.ctx.Err(); err != nil {
				return nil, cherr.Wrap(cherr.Timeout, err, "query cancelled")
			}
		}
		row, err := it.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		keys := make([]any, len(ac.keys))
		kb = kb[:0]
		for i, k := range ac.keys {
			if keys[i], err = k.eval(row); err != nil {
				return nil, err
			}
			kb = aggstate.AppendKey(kb, keys[i])
		}
		g, ok := groups[string(kb)]
		if !ok {
			g = newGroup(keys)
			groups[string(kb)] = g
		}
		for i, c := range ac.calls {
			if err := c.feed(g.accs[i], row); err != nil {
				return nil, err
			}
		}
	}
	if len(order) == 0 && len(ac.keys) == 0 {
		newGroup(nil)
	}
	rows := make([][]any, len(order))
	for i, g := range order {
		row := make([]any, len(ac.keys)+len(ac.calls))
		copy(row, g.keys)
		for j, c := range ac.calls {
			v, err := c.result(g.accs[j])
			if err != nil {
				return nil, err
			}
			row[len(ac.keys)+j] = v
		}
		rows[i] = row
	}
	return rows, nil
}

// sortRows materializes it and sorts by the ORDER BY values stored after the
// first width columns of each row.
func sortRows(q *query, it rowIter, width int, items []sqlparse.OrderItem) ([][]any, error) {
	var rows [][]any
	for {
		row, err := it.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		if len(rows)&1023 == 0 {
			if err := q.ctx.Err(); err != nil {
				return nil, cherr.Wrap(cherr.Timeout, err, "query cancelled")
			}
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		for k, o := range items {
			nullsFirst := o.NullsFirst != nil && *o.NullsFirst
			if c := compareOrder(rows[i][width+k], rows[j][width+k], o.Desc, nullsFirst); c != 0 {
				return c < 0
			}
		}
		return false
	})
	return rows, nil
}

// orderKeyExpr maps an ORDER BY expression to its sort key: enums sort by
// their numeric value, not by their element name (ClickHouse).
func orderKeyExpr(x *expr) *expr {
	k := aggstate.OrderKey(x.t)
	if k == nil {
		return x
	}
	return fold(&expr{t: x.t, fn: func(row []any) (any, error) {
		v, err := x.eval(row)
		if err != nil || v == nil {
			return v, err
		}
		return k(v), nil
	}}, []*expr{x})
}

// compareOrder orders two ORDER BY values like ClickHouse: at every depth
// (top level and inside arrays/tuples) NULL and NaN go last with NULLS LAST
// (the default) and first with NULLS FIRST, in both directions, NaN closer
// to the numbers than NULL (IColumn::compareAt with nan_direction_hint =
// direction, negated for NULLS FIRST).
func compareOrder(a, b any, desc, nullsFirst bool) int {
	hint := 1
	if desc {
		hint = -1
	}
	if nullsFirst {
		hint = -hint
	}
	c := chtype.CompareHint(a, b, hint)
	if desc {
		return -c
	}
	return c
}
