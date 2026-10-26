package exec

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/limits"
	"github.com/gfronza/twinhouse/internal/sqlparse"
)

// rewrite returns a copy of e where every node for which f returns
// (replacement, true) is replaced (without descending into it). Other nodes
// are shallow-copied with rewritten children. Subqueries are not descended.
func rewrite(e sqlparse.Expr, f func(sqlparse.Expr) (sqlparse.Expr, bool)) sqlparse.Expr {
	return rewriteAt(e, treePos{}, func(n sqlparse.Expr, _ treePos) (sqlparse.Expr, bool) { return f(n) })
}

// treePos is where a node sits in an expression tree.
type treePos struct {
	depth int
	// chain is the parent's operator when that is AND, OR or ||. ClickHouse
	// parses such a chain into one call, so a node continuing it does not
	// add a level.
	chain string
}

// rewriteAt is rewrite that also passes f the position of each node, e
// being at p.
func rewriteAt(e sqlparse.Expr, p treePos, f func(sqlparse.Expr, treePos) (sqlparse.Expr, bool)) sqlparse.Expr {
	if e == nil {
		return nil
	}
	if b, ok := e.(*sqlparse.BinaryExpr); ok && b.Op == p.chain {
		p.depth--
	}
	if r, ok := f(e, p); ok {
		return r
	}
	rw := func(x sqlparse.Expr) sqlparse.Expr { return rewriteAt(x, treePos{depth: p.depth + 1}, f) }
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
		c := treePos{depth: p.depth + 1}
		if n.Op == "AND" || n.Op == "OR" || n.Op == "||" {
			c.chain = n.Op
		}
		return &sqlparse.BinaryExpr{Op: n.Op, Left: rewriteAt(n.Left, c, f), Right: rewriteAt(n.Right, c, f)}
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
// Each node it produces counts against MaxExpandedASTElements for its
// expression, as in ClickHouse, and against the query's node budget, so an
// alias used twice per level cannot double the tree without bound. Expansion
// fails once the produced tree nests deeper than MaxASTDepth or aliases nest
// deeper than maxAliasNesting.
type aliasExpander struct {
	defs   map[string]sqlparse.Expr
	active map[string]bool
	plan   *planState
	err    error
	// nesting is how many alias substitutions are being expanded.
	nesting int
	// nodes is how many nodes the current expression has so far.
	nodes int
}

// maxAliasNesting bounds alias references that resolve through other
// aliases (`c0 AS c1, c1 AS c2, ...`), which add no tree level. ClickHouse
// 25.8 accepts such a chain of 6000 aliases.
const maxAliasNesting = 10_000

// aliasPathFunc is the internal function the alias expander produces for
// `alias.field`: Args are the original identifier, the alias expression and
// the field names (see compiler.aliasPath).
const aliasPathFunc = "\x00aliasPath"

func newAliasExpander(plan *planState) *aliasExpander {
	return &aliasExpander{defs: map[string]sqlparse.Expr{}, active: map[string]bool{}, plan: plan}
}

func (a *aliasExpander) expand(e sqlparse.Expr) (sqlparse.Expr, error) {
	a.nodes = 0
	r := a.at(e, treePos{})
	return r, a.err
}

func (a *aliasExpander) expandAs(name string, e sqlparse.Expr) (sqlparse.Expr, error) {
	a.nodes = 0
	r := a.asAt(name, e, treePos{})
	return r, a.err
}

func (a *aliasExpander) at(e sqlparse.Expr, p treePos) sqlparse.Expr {
	return rewriteAt(e, p, func(n sqlparse.Expr, p treePos) (sqlparse.Expr, bool) {
		if a.err != nil {
			return n, true
		}
		if r, ok := a.substitute(n, p); ok {
			return r, true
		}
		if !a.charge(p.depth, 1) {
			return n, true
		}
		switch n.(type) {
		case *sqlparse.SubqueryExpr, *sqlparse.LambdaExpr:
			return n, true
		}
		// A constant array or tuple is one literal node in ClickHouse; it
		// has no aliases in it, so every reference shares it.
		return n, isConstCollection(n)
	})
}

// substitute expands an alias reference or an inline alias n at p, the
// aliased expression taking n's place in the tree.
func (a *aliasExpander) substitute(n sqlparse.Expr, p treePos) (sqlparse.Expr, bool) {
	switch x := n.(type) {
	case *sqlparse.Ident:
		if len(x.Parts) == 0 {
			return nil, false
		}
		d, ok := a.defs[x.Parts[0]]
		if !ok || a.active[x.Parts[0]] {
			return nil, false
		}
		if len(x.Parts) == 1 {
			return a.asAt(x.Parts[0], d, treePos{depth: p.depth}), true
		}
		// alias.field: a named-tuple element of an aliased expression,
		// unless the identifier resolves as a (qualified) column. The call,
		// the identifier and each field name are new nodes.
		if !a.charge(p.depth, len(x.Parts)+1) {
			return n, true
		}
		args := []sqlparse.Expr{x, a.asAt(x.Parts[0], d, treePos{depth: p.depth + 1})}
		for _, f := range x.Parts[1:] {
			args = append(args, &sqlparse.Literal{Value: f, Raw: quoteString(f)})
		}
		return &sqlparse.FuncCall{Name: aliasPathFunc, Args: args}, true
	case *sqlparse.AliasExpr:
		return a.asAt(x.Alias, x.X, treePos{depth: p.depth}), true
	}
	return nil, false
}

func (a *aliasExpander) asAt(name string, e sqlparse.Expr, p treePos) sqlparse.Expr {
	if a.nesting >= maxAliasNesting {
		a.fail(cherr.New(cherr.TooDeepRecursion, "Maximum alias nesting (%d) exceeded", maxAliasNesting))
		return e
	}
	prev := a.active[name]
	a.active[name] = true
	a.nesting++
	r := a.at(e, p)
	a.nesting--
	a.active[name] = prev
	return r
}

// charge adds n nodes at depth d to the expression and takes them from the
// query's budget; false once expansion has failed. ClickHouse has no fixed
// depth limit here: it fails when its stack runs out, with
// TOO_DEEP_RECURSION.
func (a *aliasExpander) charge(d, n int) bool {
	a.nodes += n
	switch {
	case d > limits.MaxASTDepth:
		a.fail(cherr.New(cherr.TooDeepRecursion, "Maximum expression depth after alias expansion (%d) exceeded", limits.MaxASTDepth))
	case a.nodes > limits.MaxExpandedASTElements:
		a.fail(cherr.New(cherr.BadArguments, "Query tree is too big. Maximum: %d", limits.MaxExpandedASTElements))
	default:
		a.fail(a.plan.take(n))
	}
	return a.err == nil
}

// fail records the first expansion error.
func (a *aliasExpander) fail(err error) {
	if a.err == nil {
		a.err = err
	}
}

// isConstCollection reports whether e is an array or tuple whose elements
// are all literals or such collections.
func isConstCollection(e sqlparse.Expr) bool {
	var elems []sqlparse.Expr
	switch x := e.(type) {
	case *sqlparse.ArrayExpr:
		elems = x.Elems
	case *sqlparse.TupleExpr:
		elems = x.Elems
	default:
		return false
	}
	for _, el := range elems {
		if _, ok := el.(*sqlparse.Literal); !ok && !isConstCollection(el) {
			return false
		}
	}
	return true
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
