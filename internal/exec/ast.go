package exec

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/gfronza/twinhouse/internal/sqlparse"
)

// rewrite returns a copy of e where every node for which f returns
// (replacement, true) is replaced (without descending into it). Other nodes
// are shallow-copied with rewritten children. Subqueries are not descended.
func rewrite(e sqlparse.Expr, f func(sqlparse.Expr) (sqlparse.Expr, bool)) sqlparse.Expr {
	if e == nil {
		return nil
	}
	if r, ok := f(e); ok {
		return r
	}
	rw := func(x sqlparse.Expr) sqlparse.Expr { return rewrite(x, f) }
	rws := func(xs []sqlparse.Expr) []sqlparse.Expr {
		if xs == nil {
			return nil
		}
		out := make([]sqlparse.Expr, len(xs))
		for i, x := range xs {
			out[i] = rw(x)
		}
		return out
	}
	switch n := e.(type) {
	case *sqlparse.FuncCall:
		c := *n
		c.Params = rws(n.Params)
		c.Args = rws(n.Args)
		return &c
	case *sqlparse.BinaryExpr:
		return &sqlparse.BinaryExpr{Op: n.Op, Left: rw(n.Left), Right: rw(n.Right)}
	case *sqlparse.UnaryExpr:
		return &sqlparse.UnaryExpr{Op: n.Op, X: rw(n.X)}
	case *sqlparse.BetweenExpr:
		return &sqlparse.BetweenExpr{X: rw(n.X), Low: rw(n.Low), High: rw(n.High), Not: n.Not}
	case *sqlparse.IsNullExpr:
		return &sqlparse.IsNullExpr{X: rw(n.X), Not: n.Not}
	case *sqlparse.TupleExpr:
		return &sqlparse.TupleExpr{Elems: rws(n.Elems)}
	case *sqlparse.ArrayExpr:
		return &sqlparse.ArrayExpr{Elems: rws(n.Elems)}
	case *sqlparse.IndexExpr:
		return &sqlparse.IndexExpr{X: rw(n.X), Index: rw(n.Index)}
	case *sqlparse.TupleAccessExpr:
		return &sqlparse.TupleAccessExpr{X: rw(n.X), Index: n.Index, Name: n.Name}
	case *sqlparse.CaseExpr:
		c := &sqlparse.CaseExpr{Operand: rw(n.Operand), Else: rw(n.Else)}
		for _, w := range n.Whens {
			c.Whens = append(c.Whens, sqlparse.WhenClause{Cond: rw(w.Cond), Result: rw(w.Result)})
		}
		return c
	case *sqlparse.CastExpr:
		return &sqlparse.CastExpr{X: rw(n.X), Type: n.Type}
	case *sqlparse.LambdaExpr:
		return &sqlparse.LambdaExpr{Params: n.Params, Body: rw(n.Body)}
	case *sqlparse.IntervalExpr:
		return &sqlparse.IntervalExpr{X: rw(n.X), Unit: n.Unit}
	case *sqlparse.AliasExpr:
		return &sqlparse.AliasExpr{X: rw(n.X), Alias: n.Alias}
	}
	return e
}

// walk calls f for every node of e in pre-order; f returns false to skip
// the node's children. Subqueries are not descended.
func walk(e sqlparse.Expr, f func(sqlparse.Expr) bool) {
	if e == nil || !f(e) {
		return
	}
	for _, c := range children(e) {
		walk(c, f)
	}
}

func children(e sqlparse.Expr) []sqlparse.Expr {
	switch n := e.(type) {
	case *sqlparse.FuncCall:
		return append(append([]sqlparse.Expr{}, n.Params...), n.Args...)
	case *sqlparse.BinaryExpr:
		return []sqlparse.Expr{n.Left, n.Right}
	case *sqlparse.UnaryExpr:
		return []sqlparse.Expr{n.X}
	case *sqlparse.BetweenExpr:
		return []sqlparse.Expr{n.X, n.Low, n.High}
	case *sqlparse.IsNullExpr:
		return []sqlparse.Expr{n.X}
	case *sqlparse.TupleExpr:
		return n.Elems
	case *sqlparse.ArrayExpr:
		return n.Elems
	case *sqlparse.IndexExpr:
		return []sqlparse.Expr{n.X, n.Index}
	case *sqlparse.TupleAccessExpr:
		return []sqlparse.Expr{n.X}
	case *sqlparse.CaseExpr:
		out := []sqlparse.Expr{}
		if n.Operand != nil {
			out = append(out, n.Operand)
		}
		for _, w := range n.Whens {
			out = append(out, w.Cond, w.Result)
		}
		if n.Else != nil {
			out = append(out, n.Else)
		}
		return out
	case *sqlparse.CastExpr:
		return []sqlparse.Expr{n.X}
	case *sqlparse.LambdaExpr:
		return []sqlparse.Expr{n.Body}
	case *sqlparse.IntervalExpr:
		return []sqlparse.Expr{n.X}
	case *sqlparse.AliasExpr:
		return []sqlparse.Expr{n.X}
	}
	return nil
}

// hasAggregate reports whether e contains an aggregate function call outside
// subqueries.
func hasAggregate(e sqlparse.Expr) bool {
	found := false
	walk(e, func(n sqlparse.Expr) bool {
		if found {
			return false
		}
		if fc, ok := n.(*sqlparse.FuncCall); ok && fc.Over == nil && isAggregateName(fc.Name) {
			found = true
			return false
		}
		_, isLambda := n.(*sqlparse.LambdaExpr)
		return !isLambda
	})
	return found
}

// splitAnd flattens a conjunction into its conjuncts.
func splitAnd(e sqlparse.Expr) []sqlparse.Expr {
	if e == nil {
		return nil
	}
	switch n := e.(type) {
	case *sqlparse.BinaryExpr:
		if n.Op == "AND" {
			return append(splitAnd(n.Left), splitAnd(n.Right)...)
		}
	case *sqlparse.FuncCall:
		if strings.EqualFold(n.Name, "and") && n.Params == nil {
			var out []sqlparse.Expr
			for _, a := range n.Args {
				out = append(out, splitAnd(a)...)
			}
			return out
		}
	case *sqlparse.AliasExpr:
		return splitAnd(n.X)
	}
	return []sqlparse.Expr{e}
}

// aliasExpander substitutes alias references (ClickHouse resolves an
// identifier matching an alias to the aliased expression, in every clause).
type aliasExpander struct {
	defs   map[string]sqlparse.Expr
	active map[string]bool
}

// aliasPathFunc is the internal function the alias expander produces for
// `alias.field`: Args are the original identifier, the alias expression and
// the field names (see compiler.aliasPath).
const aliasPathFunc = "\x00aliasPath"

func newAliasExpander() *aliasExpander {
	return &aliasExpander{defs: map[string]sqlparse.Expr{}, active: map[string]bool{}}
}

func (a *aliasExpander) expand(e sqlparse.Expr) sqlparse.Expr {
	return rewrite(e, func(n sqlparse.Expr) (sqlparse.Expr, bool) {
		switch x := n.(type) {
		case *sqlparse.Ident:
			if len(x.Parts) == 1 {
				if d, ok := a.defs[x.Parts[0]]; ok && !a.active[x.Parts[0]] {
					return a.expandAs(x.Parts[0], d), true
				}
			}
			if len(x.Parts) > 1 {
				// alias.field: a named-tuple element of an aliased expression,
				// unless the identifier resolves as a (qualified) column.
				if d, ok := a.defs[x.Parts[0]]; ok && !a.active[x.Parts[0]] {
					args := []sqlparse.Expr{x, a.expandAs(x.Parts[0], d)}
					for _, f := range x.Parts[1:] {
						args = append(args, &sqlparse.Literal{Value: f, Raw: quoteString(f)})
					}
					return &sqlparse.FuncCall{Name: aliasPathFunc, Args: args}, true
				}
			}
		case *sqlparse.AliasExpr:
			return a.expandAs(x.Alias, x.X), true
		case *sqlparse.SubqueryExpr, *sqlparse.LambdaExpr:
			return n, true
		}
		return nil, false
	})
}

func (a *aliasExpander) expandAs(name string, e sqlparse.Expr) sqlparse.Expr {
	prev := a.active[name]
	a.active[name] = true
	r := a.expand(e)
	a.active[name] = prev
	return r
}

// collect records `expr AS name` occurrences nested in e.
func (a *aliasExpander) collect(e sqlparse.Expr) {
	walk(e, func(n sqlparse.Expr) bool {
		if x, ok := n.(*sqlparse.AliasExpr); ok {
			if _, dup := a.defs[x.Alias]; !dup {
				a.defs[x.Alias] = x.X
			}
		}
		_, isSub := n.(*sqlparse.SubqueryExpr)
		return !isSub
	})
}

// fingerprint renders a canonical text of e used to match GROUP BY keys and
// deduplicate aggregates. resolve maps identifiers to a stable column id.
func fingerprint(e sqlparse.Expr, resolve func(*sqlparse.Ident) string) string {
	var b strings.Builder
	fpWrite(&b, e, resolve)
	return b.String()
}

func fpWrite(b *strings.Builder, e sqlparse.Expr, resolve func(*sqlparse.Ident) string) {
	list := func(xs []sqlparse.Expr) {
		for i, x := range xs {
			if i > 0 {
				b.WriteByte(',')
			}
			fpWrite(b, x, resolve)
		}
	}
	switch n := e.(type) {
	case nil:
		b.WriteString("<nil>")
	case *sqlparse.Literal:
		b.WriteString("L:")
		switch v := n.Value.(type) {
		case string:
			b.WriteString(strconv.Quote(v))
		case nil:
			b.WriteString("NULL")
		default:
			b.WriteString(n.Raw)
			if n.Raw == "" {
				b.WriteString(strings.TrimSpace(sqlparse.String(n)))
			}
		}
	case *sqlparse.Ident:
		b.WriteString(resolve(n))
	case *sqlparse.Param:
		b.WriteString("P:" + n.Name + ":" + n.Type)
	case *sqlparse.Star:
		b.WriteString("*" + n.Table)
	case *sqlparse.FuncCall:
		b.WriteString("F:" + canonicalFuncName(n.Name))
		if n.Distinct {
			b.WriteString(":D")
		}
		if n.Params != nil {
			b.WriteByte('[')
			list(n.Params)
			b.WriteByte(']')
		}
		b.WriteByte('(')
		list(n.Args)
		b.WriteByte(')')
	case *sqlparse.BinaryExpr:
		b.WriteString("B:" + n.Op + "(")
		fpWrite(b, n.Left, resolve)
		b.WriteByte(',')
		fpWrite(b, n.Right, resolve)
		b.WriteByte(')')
	case *sqlparse.UnaryExpr:
		b.WriteString("U:" + n.Op + "(")
		fpWrite(b, n.X, resolve)
		b.WriteByte(')')
	case *sqlparse.BetweenExpr:
		b.WriteString("BT:" + strconv.FormatBool(n.Not) + "(")
		list([]sqlparse.Expr{n.X, n.Low, n.High})
		b.WriteByte(')')
	case *sqlparse.IsNullExpr:
		b.WriteString("NULL:" + strconv.FormatBool(n.Not) + "(")
		fpWrite(b, n.X, resolve)
		b.WriteByte(')')
	case *sqlparse.TupleExpr:
		b.WriteString("T(")
		list(n.Elems)
		b.WriteByte(')')
	case *sqlparse.ArrayExpr:
		b.WriteString("A(")
		list(n.Elems)
		b.WriteByte(')')
	case *sqlparse.IndexExpr:
		b.WriteString("I(")
		list([]sqlparse.Expr{n.X, n.Index})
		b.WriteByte(')')
	case *sqlparse.TupleAccessExpr:
		b.WriteString("TA:" + strconv.Itoa(n.Index) + ":" + n.Name + "(")
		fpWrite(b, n.X, resolve)
		b.WriteByte(')')
	case *sqlparse.CaseExpr:
		b.WriteString("C(")
		list(children(n))
		b.WriteString(strconv.FormatBool(n.Operand != nil) + strconv.FormatBool(n.Else != nil) + ")")
	case *sqlparse.CastExpr:
		b.WriteString("CAST:" + n.Type + "(")
		fpWrite(b, n.X, resolve)
		b.WriteByte(')')
	case *sqlparse.IntervalExpr:
		b.WriteString("IV:" + n.Unit + "(")
		fpWrite(b, n.X, resolve)
		b.WriteByte(')')
	case *sqlparse.AliasExpr:
		fpWrite(b, n.X, resolve)
	case *sqlparse.SubqueryExpr:
		fmt.Fprintf(b, "SQ:%p", n)
	case *sqlparse.LambdaExpr:
		b.WriteString("LAMBDA:" + strings.Join(n.Params, ",") + "(")
		fpWrite(b, n.Body, resolve)
		b.WriteByte(')')
	default:
		b.WriteString("?")
	}
}
