package exec

import (
	"io"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec/aggstate"
	"github.com/gfronza/twinhouse/internal/format"
	"github.com/gfronza/twinhouse/internal/memtrack"
	"github.com/gfronza/twinhouse/internal/sqlparse"
)

// compileIn compiles `left [NOT] IN right`. The right side must be constant
// (literal tuple/array, parameter, or uncorrelated subquery).
func (c *compiler) compileIn(left, right sqlparse.Expr, negate bool) (*expr, error) {
	l, err := c.compile(left)
	if err != nil {
		return nil, err
	}
	if isNothing(l.t) {
		return constant(nullNothing, nil), nil
	}
	set, err := c.inSet(l.t, right)
	if err != nil {
		return nil, err
	}
	keys := set.keys
	return strict([]*expr{l}, chtype.TUInt8, func(v []any) (any, error) {
		_, ok := keys[aggstate.Key(v[0])]
		return boolNum(ok != negate), nil
	}), nil
}

// valueSet is the right side of an IN: distinct values of one type, and
// their keys.
type valueSet struct {
	vals []any
	keys map[string]struct{}
	to   chtype.Type
	// acct is charged what the set holds (nil: unlimited).
	acct *memtrack.Account
	key  []byte // reused key buffer
}

func newValueSet(to chtype.Type, acct *memtrack.Account) *valueSet {
	return &valueSet{keys: map[string]struct{}{}, to: to, acct: acct}
}

// valueSlot is a value's slot in vals.
const valueSlot = 16

// add converts v from type from and keeps it, unless it is NULL, cannot be
// represented in the set's type, or is kept already.
func (s *valueSet) add(v any, from chtype.Type) error {
	v, ok := convertForSet(v, from, s.to)
	if !ok {
		return nil
	}
	s.key = aggstate.AppendKey(s.key[:0], v)
	if _, dup := s.keys[string(s.key)]; dup {
		return nil
	}
	s.keys[string(s.key)] = struct{}{}
	var err error
	if s.vals, err = appendCharged(s.acct, s.vals, v, valueSlot); err != nil || s.acct == nil {
		return err
	}
	return s.acct.Grow(memtrack.Value(v) + keyEntry(setEntry, len(s.key)))
}

// inSet evaluates the right side of IN into a set of the left side's type.
// Elements that cannot be represented in that type, or are NULL, are
// dropped (they can never match).
func (c *compiler) inSet(leftType chtype.Type, right sqlparse.Expr) (*valueSet, error) {
	lt := elemTypeForSet(leftType)
	switch r := right.(type) {
	case *sqlparse.SubqueryExpr:
		return c.subquerySet(r.Select, lt)
	case *sqlparse.Ident:
		return nil, cherr.New(cherr.NotImplemented, "IN with a table or identifier on the right side is not supported")
	}
	key := inSetKey{node: right, typ: lt.String()}
	return c.q.plan.consts.set(c.q, key, c.q.zoned(lt), func(s *valueSet) error { return c.readConstant(right, s) })
}

// readConstant adds the values of a literal tuple or array, or a
// parameter, to s.
func (c *compiler) readConstant(right sqlparse.Expr, s *valueSet) error {
	// The set, which is charged, keeps the values.
	defer c.q.plan.consts.release(c.q.plan.consts.mark())
	lt := s.to
	var items []sqlparse.Expr
	if tup, ok := right.(*sqlparse.TupleExpr); ok {
		items = tup.Elems
		if lt.Kind == chtype.Tuple && len(items) == len(lt.Fields) && !anyTupleElems(items) {
			items = []sqlparse.Expr{right}
		}
	} else {
		items = []sqlparse.Expr{right}
	}
	single := len(items) == 1
	for _, it := range items {
		x, err := c.child().compile(it)
		if err != nil {
			return err
		}
		if !x.konst {
			return cherr.New(cherr.NotImplemented, "IN with a non-constant right side is not supported")
		}
		if err := s.addConstant(x, single); err != nil {
			return err
		}
	}
	return nil
}

// addConstant adds the value of x to s, or its elements when x is the only
// item and an array or tuple the left side is not.
func (s *valueSet) addConstant(x *expr, single bool) error {
	xt := x.t.Unwrap()
	switch {
	case single && xt.Kind == chtype.Array && s.to.Kind != chtype.Array:
		arr, _ := x.val.([]any)
		for _, v := range arr {
			if err := s.add(v, *xt.Elem); err != nil {
				return err
			}
		}
		return nil
	case single && xt.Kind == chtype.Tuple && s.to.Kind != chtype.Tuple:
		tv, _ := x.val.([]any)
		for i, v := range tv {
			if err := s.add(v, xt.Fields[i].Type); err != nil {
				return err
			}
		}
		return nil
	}
	return s.add(x.val, x.t)
}

// inSetKey identifies an IN set: the subquery's SELECT or the constant
// right side, and the type its values are converted to.
type inSetKey struct {
	node any
	typ  string
}

// subquerySet builds the set of type lt of sel once per statement, as the
// filter and key pushdown both need it, and a view's plans for each block
// share its SELECT. The set counts against the query's memory until the
// query finishes.
func (c *compiler) subquerySet(sel *sqlparse.SelectStmt, lt chtype.Type) (*valueSet, error) {
	key := inSetKey{node: sel, typ: lt.String()}
	if s, ok := c.q.inSets[key]; ok {
		return s, nil
	}
	s := newValueSet(c.q.zoned(lt), c.q.lifetimeAccount())
	if err := c.readSubquery(sel, s); err != nil {
		s.acct.Close()
		return nil, err
	}
	if c.q.inSets == nil {
		c.q.inSets = map[inSetKey]*valueSet{}
	}
	c.q.inSets[key] = s
	return s, nil
}

// readSubquery adds each row of sel to s. A result of at most one row is
// shared with a scalar subquery over the same SELECT (a WITH alias used
// both ways), whichever runs first.
func (c *compiler) readSubquery(sel *sqlparse.SelectStmt, s *valueSet) error {
	if r, ok := c.q.subqueries[sel]; ok {
		from := rowType(r.cols)
		for _, row := range r.rows {
			if err := s.add(rowValue(row, r.cols), from); err != nil {
				return err
			}
		}
		return nil
	}
	// The plan is dropped once the set, which is charged, is read.
	defer c.q.plan.consts.release(c.q.plan.consts.mark())
	rel, err := c.q.planSelect(sel, c.env.nested())
	if err != nil {
		return err
	}
	it, err := rel.open()
	if err != nil {
		return err
	}
	defer it.close()
	from := rowType(rel.cols)
	var first []any
	n := 0
	for ; ; n++ {
		row, err := it.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := s.add(rowValue(row, rel.cols), from); err != nil {
			return err
		}
		if n == 0 {
			first = row
		}
		if n&1023 == 1023 {
			if err := c.q.ctx.Err(); err != nil {
				return cherr.Wrap(cherr.Timeout, err, "query cancelled")
			}
		}
	}
	if n > 1 {
		return nil
	}
	return c.q.shareWithScalar(sel, rel.cols, first, s.acct)
}

// shareWithScalar caches a subquery result of at most one row (first, nil
// when there is none) for a scalar subquery over the same SELECT. The row
// is charged to acct.
func (q *query) shareWithScalar(sel *sqlparse.SelectStmt, cols []format.Column, first []any, acct *memtrack.Account) error {
	r := &subqueryResult{cols: cols}
	if first != nil {
		if err := acct.Grow(memtrack.Row(first)); err != nil {
			return err
		}
		r.rows = [][]any{first}
	}
	if q.subqueries == nil {
		q.subqueries = map[*sqlparse.SelectStmt]*subqueryResult{}
	}
	q.subqueries[sel] = r
	return nil
}

// rowType is the type of a subquery's set elements: its column's, or the
// tuple of its columns' when it has several.
func rowType(cols []format.Column) chtype.Type {
	if len(cols) == 1 {
		return cols[0].Type
	}
	fields := make([]chtype.Field, len(cols))
	for i, col := range cols {
		fields[i] = chtype.Field{Type: col.Type}
	}
	return chtype.NewTuple(fields...)
}

// rowValue is the set element of a subquery row (see rowType).
func rowValue(row []any, cols []format.Column) any {
	if len(cols) == 1 {
		return row[0]
	}
	return row
}

func anyTupleElems(items []sqlparse.Expr) bool {
	for _, it := range items {
		if _, ok := it.(*sqlparse.TupleExpr); ok {
			return true
		}
	}
	return false
}

// elemTypeForSet strips Nullable/LowCardinality at every tuple level.
func elemTypeForSet(t chtype.Type) chtype.Type {
	u := t.Unwrap()
	if u.Kind == chtype.Tuple {
		fields := make([]chtype.Field, len(u.Fields))
		for i, f := range u.Fields {
			fields[i] = chtype.Field{Name: f.Name, Type: elemTypeForSet(f.Type)}
		}
		return chtype.NewTuple(fields...)
	}
	return u
}

// convertForSet converts a set element to the IN left-side type exactly.
func convertForSet(v any, from, to chtype.Type) (any, bool) {
	if v == nil {
		return nil, false
	}
	fu := from.Unwrap()
	if to.Kind == chtype.Tuple {
		tv, ok := v.([]any)
		if !ok || fu.Kind != chtype.Tuple || len(tv) != len(to.Fields) || len(fu.Fields) != len(tv) {
			return nil, false
		}
		out := make([]any, len(tv))
		for i := range tv {
			x, ok := convertForSet(tv[i], fu.Fields[i].Type, to.Fields[i].Type)
			if !ok {
				return nil, false
			}
			out[i] = x
		}
		return out, true
	}
	if fu.Kind == chtype.Nothing {
		return nil, false
	}
	return exactConvert(v, fu, to)
}
