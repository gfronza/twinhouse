package exec

import (
	"strings"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/sqlparse"
)

// expr is a compiled, statically typed expression evaluated over a row.
type expr struct {
	t  chtype.Type
	fn func(row []any) (any, error)
	// konst marks a constant; val holds its value.
	konst bool
	val   any
	// volatile marks non-deterministic expressions (never constant-folded).
	volatile bool
}

func constant(t chtype.Type, v any) *expr { return &expr{t: t, konst: true, val: v} }

func (x *expr) eval(row []any) (any, error) {
	if x.konst {
		return x.val, nil
	}
	return x.fn(row)
}

func slotExpr(i int, t chtype.Type) *expr {
	return &expr{t: t, fn: func(row []any) (any, error) { return row[i], nil }}
}

// fold turns x into a constant when every argument is constant and nothing
// is volatile. Evaluation errors are deferred to run time.
func fold(x *expr, args []*expr) *expr {
	for _, a := range args {
		if a.volatile {
			x.volatile = true
		}
		if !a.konst {
			return x
		}
	}
	if x.volatile || x.konst {
		return x
	}
	v, err := x.fn(nil)
	if err != nil {
		return x
	}
	return constant(x.t, v)
}

// scopeCol is one column visible to expressions.
type scopeCol struct {
	name string
	// quals are accepted qualifiers: table name, alias, "db.table".
	quals []string
	t     chtype.Type
	// hidden columns (MATERIALIZED/ALIAS) are not expanded by `*`.
	hidden bool
	// qname, when set, is the name matched by qualified references
	// (JOIN columns renamed to "t.col" keep "col" here).
	qname string
}

type scope struct {
	cols []scopeCol
}

// lookup resolves an identifier; -1 if not found.
func (s *scope) lookup(parts []string) (int, error) {
	if s == nil {
		return -1, nil
	}
	full := strings.Join(parts, ".")
	found := -1
	for i := range s.cols {
		if s.cols[i].name == full {
			if found >= 0 {
				return -1, cherr.New(cherr.UnknownIdentifier, "Identifier `%s` is ambiguous", full)
			}
			found = i
		}
	}
	if found >= 0 {
		return found, nil
	}
	if len(parts) < 2 {
		// A name only reachable qualified (multi-table JOIN): unique wins.
		for i := range s.cols {
			if s.cols[i].qname == full {
				if found >= 0 {
					return -1, cherr.New(ambiguousIdentifier, "Identifier '%s' is ambiguous: it exists in several joined tables", full)
				}
				found = i
			}
		}
		return found, nil
	}
	qual := strings.Join(parts[:len(parts)-1], ".")
	name := parts[len(parts)-1]
	for i := range s.cols {
		base := s.cols[i].name
		if s.cols[i].qname != "" {
			base = s.cols[i].qname
		}
		if base != name {
			continue
		}
		for _, q := range s.cols[i].quals {
			if q == qual {
				if found >= 0 && found != i {
					return -1, cherr.New(cherr.UnknownIdentifier, "Identifier `%s` is ambiguous", full)
				}
				found = i
			}
		}
	}
	return found, nil
}

// compiler compiles AST expressions against a scope.
type compiler struct {
	q   *query
	env *planEnv
	sc  *scope
	// used records referenced scope columns (projection pushdown).
	used map[int]bool
	// agg is set when compiling post-aggregation expressions.
	agg *aggContext
	// clause names the clause for "aggregate not allowed" errors.
	clause string
}

func (c *compiler) child() *compiler {
	return &compiler{q: c.q, env: c.env, sc: c.sc, used: c.used, clause: c.clause}
}

func (c *compiler) resolveFP(id *sqlparse.Ident) string {
	if i, err := c.sc.lookup(id.Parts); err == nil && i >= 0 {
		return "#" + itoa(i)
	}
	return "?" + strings.Join(id.Parts, ".")
}

func (c *compiler) fingerprint(e sqlparse.Expr) string { return fingerprint(e, c.resolveFP) }

func (c *compiler) compile(e sqlparse.Expr) (*expr, error) {
	if c.agg != nil {
		if x, ok, err := c.postAgg(e); ok || err != nil {
			return x, err
		}
	}
	if fc, ok := e.(*sqlparse.FuncCall); ok && fc.Name == aliasPathFunc {
		return c.aliasPath(fc)
	}
	switch n := e.(type) {
	case nil:
		return nil, cherr.New(cherr.LogicalError, "nil expression")
	case *sqlparse.Literal:
		return literalExpr(n.Value)
	case *sqlparse.Ident:
		return c.ident(n)
	case *sqlparse.Param:
		return c.q.param(n)
	case *sqlparse.Star:
		return nil, cherr.New(cherr.IncorrectQuery, "Asterisk is not allowed here")
	case *sqlparse.AliasExpr:
		return c.compile(n.X)
	case *sqlparse.UnaryExpr:
		switch n.Op {
		case "-":
			// The parser already folds a minus directly before a numeric
			// literal (`-1` is an Int8 literal); a UnaryExpr is a real
			// negate() call (`-(1)` is negate(UInt8) -> Int16), as in ClickHouse.
			return c.callAST("negate", n.X)
		case "NOT":
			return c.callAST("not", n.X)
		}
		return nil, cherr.New(cherr.NotImplemented, "unary operator %s is not supported", n.Op)
	case *sqlparse.BinaryExpr:
		return c.binary(n)
	case *sqlparse.BetweenExpr:
		x, err := c.compile(n.X)
		if err != nil {
			return nil, err
		}
		lo, err := c.compile(n.Low)
		if err != nil {
			return nil, err
		}
		hi, err := c.compile(n.High)
		if err != nil {
			return nil, err
		}
		if n.Not {
			a, err := c.call("less", x, lo)
			if err != nil {
				return nil, err
			}
			b, err := c.call("greater", x, hi)
			if err != nil {
				return nil, err
			}
			return c.call("or", a, b)
		}
		a, err := c.call("greaterOrEquals", x, lo)
		if err != nil {
			return nil, err
		}
		b, err := c.call("lessOrEquals", x, hi)
		if err != nil {
			return nil, err
		}
		return c.call("and", a, b)
	case *sqlparse.IsNullExpr:
		if n.Not {
			return c.callAST("isNotNull", n.X)
		}
		return c.callAST("isNull", n.X)
	case *sqlparse.TupleExpr:
		return c.callAST("tuple", n.Elems...)
	case *sqlparse.ArrayExpr:
		return c.callAST("array", n.Elems...)
	case *sqlparse.IndexExpr:
		return c.callAST("arrayElement", n.X, n.Index)
	case *sqlparse.TupleAccessExpr:
		x, err := c.compile(n.X)
		if err != nil {
			return nil, err
		}
		if n.Name != "" {
			return c.call("tupleElement", x, constant(chtype.TString, n.Name))
		}
		return c.call("tupleElement", x, constant(chtype.TUInt64, uint64(n.Index)))
	case *sqlparse.CaseExpr:
		return c.caseExpr(n)
	case *sqlparse.CastExpr:
		x, err := c.compile(n.X)
		if err != nil {
			return nil, err
		}
		return c.castTo(x, n.Type)
	case *sqlparse.SubqueryExpr:
		return c.scalarSubquery(n.Select)
	case *sqlparse.FuncCall:
		return c.funcCall(n)
	case *sqlparse.IntervalExpr:
		return nil, cherr.New(cherr.NotImplemented, "INTERVAL is supported only in date/time arithmetic (x + INTERVAL n UNIT)")
	case *sqlparse.LambdaExpr:
		return nil, cherr.New(cherr.NotImplemented, "lambda expressions are not supported")
	}
	return nil, cherr.New(cherr.NotImplemented, "expression %T is not supported", e)
}

func literalExpr(v any) (*expr, error) {
	switch x := v.(type) {
	case nil:
		return constant(nullNothing, nil), nil
	case bool:
		return constant(chtype.TBool, x), nil
	case uint64, int64, float64, string:
		return constant(chtype.InferLiteral(x, false), x), nil
	}
	return nil, cherr.New(cherr.LogicalError, "unexpected literal value %T", v)
}

func (c *compiler) ident(n *sqlparse.Ident) (*expr, error) {
	i, err := c.sc.lookup(n.Parts)
	if err != nil {
		return nil, err
	}
	if i < 0 {
		if x, ok, err := c.tuplePath(n.Parts); ok || err != nil {
			return x, err
		}
		return nil, cherr.New(cherr.UnknownIdentifier, "Unknown expression identifier `%s` in scope %s", strings.Join(n.Parts, "."), c.scopeDesc())
	}
	if c.used != nil {
		c.used[i] = true
	}
	return slotExpr(i, c.sc.cols[i].t), nil
}

// tuplePath resolves `col.field[.field...]` as named tuple element access.
func (c *compiler) tuplePath(parts []string) (*expr, bool, error) {
	for k := len(parts) - 1; k >= 1; k-- {
		i, err := c.sc.lookup(parts[:k])
		if err != nil || i < 0 || c.sc.cols[i].t.Unwrap().Kind != chtype.Tuple {
			continue
		}
		if c.used != nil {
			c.used[i] = true
		}
		x := slotExpr(i, c.sc.cols[i].t)
		for _, f := range parts[k:] {
			if x, err = c.call("tupleElement", x, constant(chtype.TString, f)); err != nil {
				return nil, true, err
			}
		}
		return x, true, nil
	}
	return nil, false, nil
}

// aliasPath compiles `alias.field[.field...]` produced by the alias
// expander: a column (or a column's tuple path) wins; otherwise the alias
// expression must be a named tuple and the fields are its elements.
func (c *compiler) aliasPath(fc *sqlparse.FuncCall) (*expr, error) {
	id := fc.Args[0].(*sqlparse.Ident)
	if i, err := c.sc.lookup(id.Parts); err == nil && i >= 0 {
		return c.ident(id)
	}
	if x, ok, err := c.tuplePath(id.Parts); ok || err != nil {
		return x, err
	}
	x, err := c.compile(fc.Args[1])
	if err != nil {
		return nil, err
	}
	if x.t.Unwrap().Kind != chtype.Tuple {
		return nil, cherr.New(cherr.UnknownIdentifier, "Unknown expression identifier `%s` in scope %s", strings.Join(id.Parts, "."), c.scopeDesc())
	}
	for _, f := range fc.Args[2:] {
		if x, err = c.call("tupleElement", x, constant(chtype.TString, f.(*sqlparse.Literal).Value)); err != nil {
			return nil, err
		}
	}
	return x, nil
}

func (c *compiler) scopeDesc() string {
	if c.clause != "" {
		return c.clause
	}
	return "query"
}

// postAgg resolves e against GROUP BY keys and aggregates.
func (c *compiler) postAgg(e sqlparse.Expr) (*expr, bool, error) {
	switch e.(type) {
	case *sqlparse.Literal, *sqlparse.Param, *sqlparse.SubqueryExpr:
		return nil, false, nil
	}
	fp := c.fingerprint(e)
	if i, ok := c.agg.keyFP[fp]; ok {
		return slotExpr(i, c.agg.keys[i].t), true, nil
	}
	switch n := e.(type) {
	case *sqlparse.FuncCall:
		if n.Over == nil && isAggregateName(n.Name) {
			i, err := c.agg.register(c, n, fp)
			if err != nil {
				return nil, true, err
			}
			return slotExpr(len(c.agg.keys)+i, c.agg.calls[i].t), true, nil
		}
	case *sqlparse.Ident:
		if _, err := c.ident(n); err != nil {
			return nil, true, err
		}
		return nil, true, cherr.New(cherr.NotAnAggregate, "Column `%s` is not under aggregate function and not in GROUP BY keys", strings.Join(n.Parts, "."))
	}
	return nil, false, nil
}

var binaryFuncs = map[string]string{
	"=": "equals", "!=": "notEquals", "<": "less", "<=": "lessOrEquals", ">": "greater", ">=": "greaterOrEquals",
	"+": "plus", "-": "minus", "*": "multiply", "/": "divide", "%": "modulo", "||": "concat",
	"LIKE": "like", "NOT LIKE": "notLike", "ILIKE": "ilike", "NOT ILIKE": "notILike",
}

func (c *compiler) binary(n *sqlparse.BinaryExpr) (*expr, error) {
	switch n.Op {
	case "AND", "OR":
		var ops []sqlparse.Expr
		var flat func(e sqlparse.Expr)
		flat = func(e sqlparse.Expr) {
			if b, ok := e.(*sqlparse.BinaryExpr); ok && b.Op == n.Op {
				flat(b.Left)
				flat(b.Right)
				return
			}
			ops = append(ops, e)
		}
		flat(n)
		return c.callAST(strings.ToLower(n.Op), ops...)
	case "IN", "GLOBAL IN":
		return c.compileIn(n.Left, n.Right, false)
	case "NOT IN", "GLOBAL NOT IN":
		return c.compileIn(n.Left, n.Right, true)
	case "+", "-":
		if iv, ok := n.Right.(*sqlparse.IntervalExpr); ok {
			return c.intervalArith(n.Left, iv, n.Op == "-")
		}
		if iv, ok := n.Left.(*sqlparse.IntervalExpr); ok && n.Op == "+" {
			return c.intervalArith(n.Right, iv, false)
		}
	}
	name, ok := binaryFuncs[n.Op]
	if !ok {
		return nil, cherr.New(cherr.NotImplemented, "operator %s is not supported", n.Op)
	}
	return c.callAST(name, n.Left, n.Right)
}

func (c *compiler) caseExpr(n *sqlparse.CaseExpr) (*expr, error) {
	var args []*expr
	var operand *expr
	if n.Operand != nil {
		var err error
		if operand, err = c.compile(n.Operand); err != nil {
			return nil, err
		}
	}
	for _, w := range n.Whens {
		cond, err := c.compile(w.Cond)
		if err != nil {
			return nil, err
		}
		if operand != nil {
			if cond, err = c.call("equals", operand, cond); err != nil {
				return nil, err
			}
		}
		res, err := c.compile(w.Result)
		if err != nil {
			return nil, err
		}
		args = append(args, cond, res)
	}
	if n.Else != nil {
		e, err := c.compile(n.Else)
		if err != nil {
			return nil, err
		}
		args = append(args, e)
	} else {
		args = append(args, constant(nullNothing, nil))
	}
	return c.call("multiIf", args...)
}

// callAST compiles args and calls a scalar function.
func (c *compiler) callAST(name string, args ...sqlparse.Expr) (*expr, error) {
	xs := make([]*expr, len(args))
	for i, a := range args {
		x, err := c.compile(a)
		if err != nil {
			return nil, err
		}
		xs[i] = x
	}
	return c.call(name, xs...)
}

// call invokes a registered scalar function on compiled args.
func (c *compiler) call(name string, args ...*expr) (*expr, error) {
	b, canon := lookupFunc(name)
	if b == nil {
		return nil, unknownFunction(name)
	}
	return b(c, canon, args)
}

func unknownFunction(name string) error {
	if isDecimalFunction(name) {
		return cherr.New(cherr.NotImplemented, "Decimal types are not supported (function %s)", name)
	}
	return cherr.New(cherr.UnknownFunction, "Function with name `%s` does not exist. In scope %s", name, name)
}

// isDecimalFunction reports ClickHouse's Decimal conversion functions
// (toDecimal32/64/128/256 and their OrZero/OrNull/OrDefault variants,
// toDecimalString): Decimal is a documented limitation (NOT_IMPLEMENTED),
// not an unknown function.
func isDecimalFunction(name string) bool {
	switch {
	case name == "toDecimalString":
		return true
	case strings.HasPrefix(name, "toDecimal"):
		rest := strings.TrimPrefix(name, "toDecimal")
		for _, w := range []string{"32", "64", "128", "256"} {
			if strings.HasPrefix(rest, w) {
				switch strings.TrimPrefix(rest, w) {
				case "", "OrZero", "OrNull", "OrDefault":
					return true
				}
			}
		}
	}
	return false
}

func (c *compiler) funcCall(n *sqlparse.FuncCall) (*expr, error) {
	if n.Over != nil {
		return nil, cherr.New(cherr.NotImplemented, "window functions are not supported")
	}
	if isAggregateName(n.Name) {
		where := c.clause
		if where == "" {
			where = "a non-aggregating context"
		}
		return nil, cherr.New(illegalAggregation, "Aggregate function %s is found in %s in query", sqlparse.ColumnName(n), where)
	}
	if n.Params != nil {
		return nil, cherr.New(cherr.NotImplemented, "parametric function %s is not supported", n.Name)
	}
	for _, a := range n.Args {
		if _, ok := a.(*sqlparse.LambdaExpr); ok {
			return nil, cherr.New(cherr.NotImplemented, "higher-order function %s is not supported", n.Name)
		}
	}
	_, canon := lookupFunc(n.Name)
	switch canon {
	case "in", "globalIn", "notIn", "globalNotIn":
		if len(n.Args) != 2 {
			return nil, argCountError(n.Name, len(n.Args), "2")
		}
		return c.compileIn(n.Args[0], n.Args[1], canon == "notIn" || canon == "globalNotIn")
	case "arrayJoin":
		return nil, cherr.New(cherr.NotImplemented, "arrayJoin is supported only in SELECT, WHERE, GROUP BY, HAVING and ORDER BY of a query with a FROM source")
	}
	if strings.HasPrefix(canon, "toInterval") {
		return nil, cherr.New(cherr.NotImplemented, "%s is supported only as INTERVAL n UNIT in date/time arithmetic", n.Name)
	}
	return c.callAST(n.Name, n.Args...)
}

func argCountError(name string, got int, want string) error {
	return cherr.New(cherr.NumberOfArgsDoesntMatch, "Number of arguments for function %s doesn't match: passed %d, should be %s", name, got, want)
}

// intervalArith compiles x ± INTERVAL n UNIT.
func (c *compiler) intervalArith(x sqlparse.Expr, iv *sqlparse.IntervalExpr, subtract bool) (*expr, error) {
	fn, ok := intervalFuncs[strings.ToUpper(iv.Unit)]
	if !ok {
		return nil, cherr.New(cherr.NotImplemented, "INTERVAL unit %s is not supported", iv.Unit)
	}
	if subtract {
		fn = "subtract" + strings.TrimPrefix(fn, "add")
	}
	return c.callAST(fn, x, iv.X)
}

var intervalFuncs = map[string]string{
	"NANOSECOND": "addNanoseconds", "MICROSECOND": "addMicroseconds", "MILLISECOND": "addMilliseconds",
	"SECOND": "addSeconds", "MINUTE": "addMinutes", "HOUR": "addHours", "DAY": "addDays",
	"WEEK": "addWeeks", "MONTH": "addMonths", "QUARTER": "addQuarters", "YEAR": "addYears",
}

// castTo compiles CAST(x AS typeText).
func (c *compiler) castTo(x *expr, typeText string) (*expr, error) {
	t, err := chtype.Parse(typeText)
	if err != nil {
		return nil, err
	}
	return castExpr(x, t, true)
}

// castExpr converts x to type t. With explicit=false it is an implicit
// conversion (supertype unification).
func castExpr(x *expr, t chtype.Type, explicit bool) (*expr, error) {
	if x.t.Equal(t) {
		return x, nil
	}
	conv, err := converter(x.t, t)
	if err != nil {
		return nil, err
	}
	targetNullable := t.IsNullable()
	out := &expr{t: t, fn: func(row []any) (any, error) {
		v, err := x.eval(row)
		if err != nil {
			return nil, err
		}
		if v == nil {
			if targetNullable {
				return nil, nil
			}
			if explicit {
				return nil, cherr.New(cherr.CannotConvertType, "Cannot convert NULL value to non-Nullable type %s", t.String())
			}
			return zeroValue(t), nil
		}
		return conv(v)
	}}
	return fold(out, []*expr{x}), nil
}

// converter returns a function converting non-NULL canonical values of type
// from to type to.
func converter(from, to chtype.Type) (func(any) (any, error), error) {
	fu, tu := from.Unwrap(), to.Unwrap()
	if to.Kind == chtype.SimpleAggregateFunction {
		tu = *to.Elem
	}
	if fu.Equal(tu) || fu.Kind == chtype.Nothing {
		return func(v any) (any, error) { return v, nil }, nil
	}
	if tu.Kind == chtype.AggregateFunction {
		return func(v any) (any, error) {
			if p, ok := v.(*chtype.AggPartial); ok && p.Func == stateBase(tu.AggFunc) {
				return v, nil
			}
			return nil, cherr.New(cherr.CannotConvertType, "Cannot convert %s to %s", from.String(), to.String())
		}, nil
	}
	return func(v any) (any, error) { return chtype.Convert(v, fu, tu) }, nil
}

// scalarSubquery evaluates an uncorrelated subquery once at plan time.
func (c *compiler) scalarSubquery(sel *sqlparse.SelectStmt) (*expr, error) {
	res, err := c.q.subquery(sel, c.env.nested(), 0)
	if err != nil {
		return nil, err
	}
	rows := res.rows
	if len(rows) > 1 {
		return nil, cherr.New(incorrectResultOfScalarSubquery, "Scalar subquery returned more than one row")
	}
	cols := res.cols
	if len(cols) == 1 {
		t := cols[0].Type
		nt := nullableIf(t, true)
		if len(rows) == 0 {
			if !nt.IsNullable() {
				return nil, cherr.New(incorrectResultOfScalarSubquery, "Scalar subquery returned empty result of type %s which cannot be Nullable", t.String())
			}
			return constant(nt, nil), nil
		}
		return constant(nt, rows[0][0]), nil
	}
	fields := make([]chtype.Field, len(cols))
	for i, col := range cols {
		fields[i] = chtype.Field{Name: col.Name, Type: col.Type}
	}
	t := chtype.NewTuple(fields...)
	if len(rows) == 0 {
		return nil, cherr.New(incorrectResultOfScalarSubquery, "Scalar subquery returned empty result of type %s which cannot be Nullable", t.String())
	}
	return constant(t, append([]any(nil), rows[0]...)), nil
}

// ClickHouse error codes not in cherr.
const (
	illegalAggregation              = 184
	incorrectResultOfScalarSubquery = 125
	illegalDivision                 = 153
	thereIsNoColumn                 = 8
	duplicateColumn                 = 15
	tooManyRowsOrBytes              = 396
	ambiguousIdentifier             = 207
	tooDeepRecursion                = 306
	multipleExpressionsForAlias     = 179
	decimalOverflow                 = 407
	badTypeOfField                  = 169
)
