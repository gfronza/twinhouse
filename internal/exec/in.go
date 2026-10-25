package exec

import (
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
	"dynamodb-eventstream/internal/exec/aggstate"
	"dynamodb-eventstream/internal/sqlparse"
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
	vals, err := c.inValues(l.t, right)
	if err != nil {
		return nil, err
	}
	set := make(map[string]struct{}, len(vals))
	for _, v := range vals {
		set[aggstate.Key(v)] = struct{}{}
	}
	return strict([]*expr{l}, chtype.TUInt8, func(v []any) (any, error) {
		_, ok := set[aggstate.Key(v[0])]
		return boolNum(ok != negate), nil
	}), nil
}

// inValues evaluates the right side of IN into distinct values converted to
// the left side's type. Elements that cannot be represented in that type, or
// are NULL, are dropped (they can never match).
func (c *compiler) inValues(leftType chtype.Type, right sqlparse.Expr) ([]any, error) {
	lt := elemTypeForSet(leftType)
	type elem struct {
		v any
		t chtype.Type
	}
	var elems []elem
	switch r := right.(type) {
	case *sqlparse.SubqueryExpr:
		res, err := c.q.subquery(r.Select, c.env.nested(), 0)
		if err != nil {
			return nil, err
		}
		rows := res.rows
		if len(res.cols) == 1 {
			for _, row := range rows {
				elems = append(elems, elem{row[0], res.cols[0].Type})
			}
		} else {
			fields := make([]chtype.Field, len(res.cols))
			for i, col := range res.cols {
				fields[i] = chtype.Field{Type: col.Type}
			}
			tt := chtype.NewTuple(fields...)
			for _, row := range rows {
				elems = append(elems, elem{append([]any(nil), row...), tt})
			}
		}
	case *sqlparse.Ident:
		return nil, cherr.New(cherr.NotImplemented, "IN with a table or identifier on the right side is not supported")
	default:
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
				return nil, err
			}
			if !x.konst {
				return nil, cherr.New(cherr.NotImplemented, "IN with a non-constant right side is not supported")
			}
			xt := x.t.Unwrap()
			switch {
			case single && xt.Kind == chtype.Array && lt.Kind != chtype.Array:
				arr, _ := x.val.([]any)
				for _, v := range arr {
					elems = append(elems, elem{v, *xt.Elem})
				}
			case single && xt.Kind == chtype.Tuple && lt.Kind != chtype.Tuple:
				tv, _ := x.val.([]any)
				for i, v := range tv {
					elems = append(elems, elem{v, xt.Fields[i].Type})
				}
			default:
				elems = append(elems, elem{x.val, x.t})
			}
		}
	}
	out := make([]any, 0, len(elems))
	seen := make(map[string]struct{}, len(elems))
	for _, e := range elems {
		v, ok := convertForSet(e.v, e.t, c.q.zoned(lt))
		if !ok {
			continue
		}
		k := aggstate.Key(v)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, v)
	}
	return out, nil
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
