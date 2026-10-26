package exec

import (
	"context"
	"errors"
	"reflect"
	"strings"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/sqlparse"
	"github.com/gfronza/twinhouse/internal/storage"
)

// mergeBatch is the number of view output rows per Store.Merge call.
const mergeBatch = 500

// viewPlan is a materialized view compiled against an in-memory source.
type viewPlan struct {
	rel     *relation
	mv      *mvSource
	mapping []int // output column index -> target column index
	builder *rowBuilder
}

func (e *Engine) applyViews(ctx context.Context, t *catalog.Table, rows []storage.Row, rowIDs []string) (err error) {
	defer recoverInternal(&err)
	q := e.newQuery(ctx, &Request{Database: t.Database})
	return e.applyViewsQuery(q, t, rows, rowIDs)
}

func (e *Engine) applyViewList(ctx context.Context, t *catalog.Table, views []*catalog.View, rows []storage.Row, rowIDs []string) (err error) {
	defer recoverInternal(&err)
	if len(rows) != len(rowIDs) {
		return cherr.New(cherr.LogicalError, "ApplyViewList: %d rows but %d row ids", len(rows), len(rowIDs))
	}
	for _, v := range views {
		if v == nil || v.SourceDatabase != t.Database || v.SourceTable != t.Name {
			return cherr.New(cherr.LogicalError, "ApplyViewList: view is not a view of %s", t.FullName())
		}
	}
	if len(rows) == 0 || len(views) == 0 {
		return nil
	}
	q := e.newQuery(ctx, &Request{Database: t.Database})
	return q.applyViewList(views, t, rows, rowIDs)
}

func (e *Engine) applyViewsQuery(q *query, t *catalog.Table, rows []storage.Row, rowIDs []string) error {
	if len(rows) != len(rowIDs) {
		return cherr.New(cherr.LogicalError, "ApplyViews: %d rows but %d row ids", len(rows), len(rowIDs))
	}
	if len(rows) == 0 {
		return nil
	}
	views := e.cfg.Catalog.ViewsFrom(t.Database, t.Name)
	if len(views) == 0 {
		return nil
	}
	return q.applyViewList(views, t, rows, rowIDs)
}

// applyViewList applies every view in views. A failing view (e.g. a missing
// target) does not prevent the others from being applied; the errors are
// reported together.
func (q *query) applyViewList(views []*catalog.View, src *catalog.Table, rows []storage.Row, rowIDs []string) error {
	if mergingEngine(src) {
		// Views on merging sources are rejected at CREATE; one created
		// before that check is ignored (see mergingEngine).
		return nil
	}
	var msgs []string
	var errs []error
	code := 0
	for _, v := range views {
		if err := q.applyView(v, src, rows, rowIDs); err != nil {
			ce := cherr.As(err)
			if code == 0 {
				code = ce.Code
			}
			msgs = append(msgs, ce.Msg)
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	// Keep every view's cause chain (e.g. an AWS ResourceNotFound or
	// throttling error) for callers that classify errors with errors.As,
	// such as the stream worker's transient-error check.
	return &cherr.Error{Code: code, Msg: strings.Join(msgs, "; "), Cause: errors.Join(errs...)}
}

func (q *query) applyView(v *catalog.View, src *catalog.Table, rows []storage.Row, rowIDs []string) error {
	if v.ToTable == "" {
		return cherr.New(cherr.NotImplemented, "materialized view %s has no TO table", v.FullName())
	}
	target, err := q.cat.Table(v.ToDatabase, v.ToTable)
	if err != nil {
		return cherr.Wrap(cherr.UnknownTable, err, "materialized view %s: target table %s.%s is not available", v.FullName(), v.ToDatabase, v.ToTable)
	}
	sel, err := q.e.viewSelect(v)
	if err != nil {
		return err
	}
	vp, err := q.planView(v, sel, src, target)
	if err != nil {
		return cherr.Wrap(cherr.As(err).Code, err, "materialized view %s", v.FullName())
	}
	name := v.FullName()
	var batch []storage.MergeRow
	for i, r := range rows {
		vp.mv.rows = []storage.Row{r}
		out, err := vp.rel.collect(q, 0)
		if err != nil {
			return cherr.Wrap(cherr.As(err).Code, err, "materialized view %s", name)
		}
		for idx, o := range out {
			row, present := make([]any, len(target.Columns)), make([]bool, len(target.Columns))
			for oi, ti := range vp.mapping {
				val, ok, err := q.convertTo(o[oi], vp.rel.cols[oi].Type, target.Columns[ti].Type)
				if err != nil {
					return cherr.Wrap(cherr.As(err).Code, err, "materialized view %s: column %s", name, target.Columns[ti].Name)
				}
				row[ti], present[ti] = val, ok
			}
			if err := vp.builder.complete(row, present); err != nil {
				return err
			}
			batch = append(batch, storage.MergeRow{Row: row, Source: storage.SourceRef{View: name, RowID: rowIDs[i]}, Index: idx})
		}
		if len(batch) >= mergeBatch {
			if err := q.e.cfg.Store.Merge(q.ctx, target, batch); err != nil {
				return err
			}
			batch = nil
		}
	}
	if len(batch) > 0 {
		return q.e.cfg.Store.Merge(q.ctx, target, batch)
	}
	return nil
}

// viewSelect parses (and caches) a view's SELECT.
func (e *Engine) viewSelect(v *catalog.View) (*sqlparse.SelectStmt, error) {
	key := v.FullName() + "\x00" + v.SelectSQL
	e.impl.viewMu.Lock()
	sel, ok := e.impl.viewStmts[key]
	e.impl.viewMu.Unlock()
	if ok {
		return sel, nil
	}
	sel, err := parseSelect(v.SelectSQL)
	if err != nil {
		return nil, err
	}
	e.impl.viewMu.Lock()
	e.impl.viewStmts[key] = sel
	e.impl.viewMu.Unlock()
	return sel, nil
}

// planView compiles a view SELECT over an in-memory source and maps its
// output columns to target columns by name.
func (q *query) planView(v *catalog.View, sel *sqlparse.SelectStmt, src, target *catalog.Table) (*viewPlan, error) {
	mv := &mvSource{table: src}
	rel, err := q.withDB(v.Database).planSelect(sel, &planEnv{mv: mv})
	if err != nil {
		return nil, err
	}
	vp := &viewPlan{rel: rel, mv: mv, mapping: make([]int, len(rel.cols))}
	seen := map[int]bool{}
	for i, c := range rel.cols {
		ti := target.ColumnIndex(c.Name)
		if ti < 0 {
			return nil, cherr.New(thereIsNoColumn, "SELECT query outputs column with name '%s', which is not found in the target table %s. Use 'AS' to assign alias that matches a column name", c.Name, target.FullName())
		}
		if seen[ti] {
			return nil, cherr.New(duplicateColumn, "SELECT query outputs column %s more than once", c.Name)
		}
		seen[ti] = true
		tc := target.Columns[ti]
		if tc.DefaultKind == "ALIAS" || tc.DefaultKind == "MATERIALIZED" {
			return nil, cherr.New(cherr.IllegalColumn, "Cannot insert column %s, because it is %s column", tc.Name, tc.DefaultKind)
		}
		if err := checkViewColumnType(c.Name, c.Type, tc.Type); err != nil {
			return nil, err
		}
		vp.mapping[i] = ti
	}
	if vp.builder, err = q.newRowBuilder(target); err != nil {
		return nil, err
	}
	return vp, nil
}

// checkViewColumnType validates that a view output type can be stored in a
// target column.
func checkViewColumnType(name string, out, target chtype.Type) error {
	ou := out.Unwrap()
	switch {
	case target.Kind == chtype.AggregateFunction:
		if ou.Kind != chtype.AggregateFunction || stateBase(ou.AggFunc) != stateBase(target.AggFunc) {
			return cherr.New(cherr.TypeMismatch, "Column %s: cannot store %s in %s (use %sState)", name, out.String(), target.String(), target.AggFunc)
		}
	case ou.Kind == chtype.AggregateFunction:
		return cherr.New(cherr.TypeMismatch, "Column %s: cannot store %s in %s", name, out.String(), target.String())
	}
	return nil
}

// mergingEngine reports a table whose stored rows are merged in place
// (SummingMergeTree, AggregatingMergeTree). Such a table cannot be a view
// source: its stream images carry the accumulated state of a key under one
// per-key row id, not the inserted rows, so the stream worker would apply a
// view to merged state (double counting the first record of a key and
// skipping the later ones). CREATE MATERIALIZED VIEW rejects such sources;
// applyViewList ignores them (views created before that check) so that the
// inline INSERT path and the stream worker apply the same policy.
func mergingEngine(t *catalog.Table) bool {
	return t != nil && (t.Engine == catalog.EngineSummingMergeTree || t.Engine == catalog.EngineAggregatingMergeTree)
}

// volatileFuncs are functions whose value depends on when, where or by whom
// they are evaluated (lower-case names). A view's output is computed
// separately by the inline INSERT path, POPULATE and the stream worker of
// every region, so such a value differs between those evaluations.
var volatileFuncs = map[string]bool{
	"now": true, "current_timestamp": true, "now64": true, "today": true, "curdate": true,
	"current_date": true, "yesterday": true, "generateuuidv4": true, "queryid": true,
	"query_id": true, "uptime": true, "hostname": true, "currentuser": true, "user": true,
	"rand": true, "rand32": true, "rand64": true, "randcanonical": true, "randconstant": true,
	"generateuuidv7": true, "generateulid": true, "nowinblock": true, "initialqueryid": true,
	"currentqueryid": true, "rownumberinblock": true, "blocknumber": true,
}

// checkViewKeysDeterministic rejects a materialized view whose output
// column feeding a sorting-key column of the target depends on a volatile
// function (now(), today(), generateUUIDv4(), ...). The exactly-once marker
// of a view output row includes the target key, so the inline application
// and the stream worker's (or POPULATE's) would compute different keys for
// the same source row and both be applied: the row would be counted twice.
// Volatile values in non-key columns are allowed (the marker of the key
// keeps them applied once).
func checkViewKeysDeterministic(sel *sqlparse.SelectStmt, target *catalog.Table) error {
	keys := map[string]bool{}
	for _, k := range target.OrderBy {
		keys[k] = true
	}
	for _, k := range target.PartitionKey {
		keys[k] = true
	}
	if len(keys) == 0 {
		return nil
	}
	aliases := map[string]sqlparse.Expr{}
	for _, w := range sel.With {
		if w.Expr != nil && w.Name != "" {
			aliases[w.Name] = w.Expr
		}
	}
	collectInlineAliases(sel, aliases)
	for _, it := range sel.Columns {
		if it.Alias != "" {
			aliases[it.Alias] = it.Expr
		}
	}
	for _, it := range sel.Columns {
		name := it.Alias
		if name == "" {
			switch x := it.Expr.(type) {
			case *sqlparse.Star:
				continue
			case *sqlparse.Ident:
				name = x.Parts[len(x.Parts)-1]
			case *sqlparse.AliasExpr:
				name = x.Alias
			default:
				name = sqlparse.String(it.Expr)
			}
		}
		if !keys[name] {
			continue
		}
		if fn := findVolatile(it.Expr, aliases, map[string]bool{}); fn != "" {
			return cherr.New(cherr.NotImplemented, "materialized view column %s feeds the sorting key of %s but depends on %s(), which is not deterministic: the view's output must be a function of the source row", name, target.FullName(), fn)
		}
	}
	return nil
}

// collectInlineAliases records every `expr AS name` inside the statement
// (ClickHouse allows aliases in any clause, e.g. GROUP BY now() AS t).
func collectInlineAliases(e any, aliases map[string]sqlparse.Expr) {
	walkAST(reflect.ValueOf(e), func(v reflect.Value) {
		if a, ok := v.Interface().(*sqlparse.AliasExpr); ok && a.Alias != "" {
			aliases[a.Alias] = a.X
		}
	})
}

// findVolatile returns the name of a volatile function used by e (following
// aliases), or "".
func findVolatile(e sqlparse.Expr, aliases map[string]sqlparse.Expr, seen map[string]bool) string {
	found := ""
	walkAST(reflect.ValueOf(e), func(v reflect.Value) {
		if found != "" {
			return
		}
		switch x := v.Interface().(type) {
		case *sqlparse.FuncCall:
			if volatileFuncs[strings.ToLower(x.Name)] {
				found = x.Name
			}
		case *sqlparse.Ident:
			if len(x.Parts) != 1 || seen[x.Parts[0]] {
				return
			}
			if a, ok := aliases[x.Parts[0]]; ok {
				seen[x.Parts[0]] = true
				found = findVolatile(a, aliases, seen)
			}
		}
	})
	return found
}

// walkAST calls fn for every pointer reachable from v (AST nodes are
// pointers to structs; slices and interfaces are traversed).
func walkAST(v reflect.Value, fn func(reflect.Value)) {
	switch v.Kind() {
	case reflect.Interface:
		if !v.IsNil() {
			walkAST(v.Elem(), fn)
		}
	case reflect.Ptr:
		if v.IsNil() {
			return
		}
		if v.CanInterface() {
			fn(v)
		}
		walkAST(v.Elem(), fn)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				walkAST(v.Field(i), fn)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			walkAST(v.Index(i), fn)
		}
	}
}
