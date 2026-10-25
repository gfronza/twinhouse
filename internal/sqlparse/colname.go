package sqlparse

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

func columnName(e Expr) string { return columnNameFunc(e, nil) }

func columnNameFunc(e Expr, hook func(Expr) (string, bool)) string {
	n := namer{hook: hook}
	n.expr(e)
	return n.b.String()
}

// namer prints expressions the way ClickHouse names result columns: the AST
// in function form, with identifiers unquoted.
type namer struct {
	b    strings.Builder
	hook func(Expr) (string, bool)
	subq int // _subquery_N counter
}

var binaryFuncs = map[string]string{
	"OR": "or", "AND": "and", "=": "equals", "!=": "notEquals", "<": "less", "<=": "lessOrEquals",
	">": "greater", ">=": "greaterOrEquals", "+": "plus", "-": "minus", "*": "multiply", "/": "divide",
	"%": "modulo", "||": "concat", "LIKE": "like", "NOT LIKE": "notLike", "ILIKE": "ilike",
	"NOT ILIKE": "notILike", "IN": "in", "NOT IN": "notIn", "GLOBAL IN": "globalIn",
	"GLOBAL NOT IN": "globalNotIn",
}

var intervalFuncs = map[string]string{
	"NANOSECOND": "toIntervalNanosecond", "MICROSECOND": "toIntervalMicrosecond",
	"MILLISECOND": "toIntervalMillisecond", "SECOND": "toIntervalSecond", "MINUTE": "toIntervalMinute",
	"HOUR": "toIntervalHour", "DAY": "toIntervalDay", "WEEK": "toIntervalWeek", "MONTH": "toIntervalMonth",
	"QUARTER": "toIntervalQuarter", "YEAR": "toIntervalYear",
}

func (n *namer) call(name string, args ...Expr) {
	n.b.WriteString(name)
	n.b.WriteByte('(')
	n.list(args)
	n.b.WriteByte(')')
}

func (n *namer) list(args []Expr) {
	for i, a := range args {
		if i > 0 {
			n.b.WriteString(", ")
		}
		n.expr(a)
	}
}

func (n *namer) expr(e Expr) {
	if n.hook != nil {
		if s, ok := n.hook(e); ok {
			n.b.WriteString(s)
			return
		}
	}
	b := &n.b
	switch v := e.(type) {
	case nil:
		b.WriteString("NULL")
	case *Literal:
		b.WriteString(literalName(v.Value))
	case *Ident:
		// The analyzer names a column reference by the column, without its
		// table/database qualifier (t.col -> col).
		if len(v.Parts) > 0 {
			b.WriteString(v.Parts[len(v.Parts)-1])
		}
	case *Param:
		fmt.Fprintf(b, "{%s:%s}", v.Name, v.Type)
	case *Star:
		if v.Table != "" {
			b.WriteString(v.Table + ".")
		}
		b.WriteByte('*')
	case *FuncCall:
		n.funcCall(v)
	case *BinaryExpr:
		fn := binaryFuncs[v.Op]
		if fn == "" {
			fn = strings.ToLower(v.Op)
		}
		if v.Op == "AND" || v.Op == "OR" || v.Op == "||" {
			n.call(fn, flatten(v)...)
			return
		}
		n.call(fn, v.Left, v.Right)
	case *UnaryExpr:
		if v.Op == "NOT" {
			n.call("not", v.X)
		} else {
			n.call("negate", v.X)
		}
	case *BetweenExpr:
		if v.Not {
			b.WriteString("or(")
			n.call("less", v.X, v.Low)
			b.WriteString(", ")
			n.call("greater", v.X, v.High)
		} else {
			b.WriteString("and(")
			n.call("greaterOrEquals", v.X, v.Low)
			b.WriteString(", ")
			n.call("lessOrEquals", v.X, v.High)
		}
		b.WriteByte(')')
	case *IsNullExpr:
		if v.Not {
			n.call("isNotNull", v.X)
		} else {
			n.call("isNull", v.X)
		}
	case *TupleExpr:
		if isTupleLiteral(v) {
			n.constant(v, false)
			return
		}
		b.WriteByte('(')
		n.list(v.Elems)
		b.WriteByte(')')
	case *ArrayExpr:
		if isArrayLiteral(v) {
			n.constant(v, hasFloatElem(v))
			return
		}
		b.WriteByte('[')
		n.list(v.Elems)
		b.WriteByte(']')
	case *IndexExpr:
		n.call("arrayElement", v.X, v.Index)
	case *TupleAccessExpr:
		b.WriteString("tupleElement(")
		n.expr(v.X)
		if v.Name != "" {
			b.WriteString(", " + quoteString(v.Name) + ")")
		} else {
			b.WriteString(", " + strconv.Itoa(v.Index) + ")")
		}
	case *CaseExpr:
		var args []Expr
		fn := "multiIf"
		if v.Operand != nil {
			fn = "caseWithExpression"
			args = append(args, v.Operand)
		}
		for _, w := range v.Whens {
			args = append(args, w.Cond, w.Result)
		}
		if v.Else != nil {
			args = append(args, v.Else)
		} else {
			args = append(args, nil)
		}
		n.call(fn, args...)
	case *CastExpr:
		b.WriteString("CAST(")
		n.expr(v.X)
		b.WriteString(", " + quoteString(v.Type) + ")")
	case *SubqueryExpr:
		n.subq++
		b.WriteString("_subquery_" + strconv.Itoa(n.subq))
	case *LambdaExpr:
		b.WriteString("lambda(tuple(")
		b.WriteString(strings.Join(v.Params, ", "))
		b.WriteString("), ")
		n.expr(v.Body)
		b.WriteByte(')')
	case *IntervalExpr:
		fn := intervalFuncs[v.Unit]
		if fn == "" {
			fn = "toInterval" + v.Unit
		}
		n.call(fn, v.X)
	case *AliasExpr:
		b.WriteString(v.Alias)
	default:
		fmt.Fprintf(b, "%T", e)
	}
}

// constant names a tuple or array literal the way ClickHouse prints the
// constant it folds it into: booleans as 1/0 and, when the numbers of an
// array (through nested arrays) include a float, every number as a float of
// the common type ([1, 2.5] -> "[1., 2.5]", [[1], [2.5]] -> "[[1.], [2.5]]").
// Tuple elements keep their own types: [(1, 2.5), (2, 3)] is unchanged.
func (n *namer) constant(e Expr, float bool) {
	b := &n.b
	switch v := e.(type) {
	case *Literal:
		switch x := v.Value.(type) {
		case bool:
			if x {
				b.WriteByte('1')
			} else {
				b.WriteByte('0')
			}
			return
		case uint64:
			if float {
				b.WriteString(formatFloat(float64(x)))
				return
			}
		case int64:
			if float {
				b.WriteString(formatFloat(float64(x)))
				return
			}
		case int:
			if float {
				b.WriteString(formatFloat(float64(x)))
				return
			}
		}
		n.expr(v)
	case *TupleExpr:
		b.WriteByte('(')
		for i, x := range v.Elems {
			if i > 0 {
				b.WriteString(", ")
			}
			n.constant(x, false)
		}
		b.WriteByte(')')
	case *ArrayExpr:
		b.WriteByte('[')
		for i, x := range v.Elems {
			if i > 0 {
				b.WriteString(", ")
			}
			n.constant(x, float)
		}
		b.WriteByte(']')
	default:
		n.expr(e)
	}
}

// hasFloatElem reports whether an array literal has a floating-point number
// among its elements or those of its nested arrays.
func hasFloatElem(a *ArrayExpr) bool {
	for _, x := range a.Elems {
		switch v := x.(type) {
		case *Literal:
			switch v.Value.(type) {
			case float64, float32:
				return true
			}
		case *ArrayExpr:
			if hasFloatElem(v) {
				return true
			}
		}
	}
	return false
}

// flatten returns the operands of a left-nested chain of the same mergeable
// operator, as ClickHouse merges `a AND b AND c` into and(a, b, c). A chain
// written in its own parentheses is not merged.
func flatten(b *BinaryExpr) []Expr {
	var out []Expr
	var walk func(e Expr)
	walk = func(e Expr) {
		if l, ok := e.(*BinaryExpr); ok && l.Op == b.Op && (l == b || !l.Parens) {
			walk(l.Left)
			out = append(out, l.Right)
			return
		}
		out = append(out, e)
	}
	walk(b)
	return out
}

func (n *namer) funcCall(f *FuncCall) {
	b := &n.b
	name := f.Name
	if f.Distinct {
		name += "Distinct"
	}
	switch {
	case f.Name == "tuple" && f.Params == nil && f.Over == nil:
		b.WriteByte('(')
		n.list(f.Args)
		b.WriteByte(')')
		return
	case f.Name == "array" && f.Params == nil && f.Over == nil:
		b.WriteByte('[')
		n.list(f.Args)
		b.WriteByte(']')
		return
	case strings.EqualFold(f.Name, "cast"):
		name = "CAST"
	}
	b.WriteString(name)
	if f.Params != nil {
		b.WriteByte('(')
		n.list(f.Params)
		b.WriteByte(')')
	}
	b.WriteByte('(')
	n.list(f.Args)
	b.WriteByte(')')
	if w := f.Over; w != nil {
		b.WriteString(" OVER (")
		sep := ""
		if len(w.PartitionBy) > 0 {
			b.WriteString("PARTITION BY ")
			n.list(w.PartitionBy)
			sep = " "
		}
		if len(w.OrderBy) > 0 {
			b.WriteString(sep + "ORDER BY ")
			for i, it := range w.OrderBy {
				if i > 0 {
					b.WriteString(", ")
				}
				n.expr(it.Expr)
				if it.Desc {
					b.WriteString(" DESC")
				} else {
					b.WriteString(" ASC")
				}
				if it.NullsFirst != nil {
					if *it.NullsFirst {
						b.WriteString(" NULLS FIRST")
					} else {
						b.WriteString(" NULLS LAST")
					}
				}
			}
			sep = " "
		}
		if w.Frame != "" {
			b.WriteString(sep + w.Frame)
		}
		b.WriteByte(')')
	}
}

// literalName renders a constant the way ClickHouse prints it in names.
func literalName(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		return quoteString(x)
	case bool:
		if x {
			return "true"
		}
		return "false"
	case uint64:
		return strconv.FormatUint(x, 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case int:
		return strconv.Itoa(x)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case float64:
		return formatFloat(x)
	case float32:
		return formatFloat(float64(x))
	}
	return quoteString(fmt.Sprint(v))
}

// formatFloat formats f like ClickHouse (double-conversion ToShortest with
// trailing decimal point): shortest round-trip digits, fixed notation for
// decimal exponents in [-6, 21), "e" notation otherwise.
func formatFloat(f float64) string {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	s := strconv.FormatFloat(f, 'e', -1, 64) // [-]d[.ddd]e±XX
	neg := s[0] == '-'
	if neg {
		s = s[1:]
	}
	mant, expStr, _ := strings.Cut(s, "e")
	exp, _ := strconv.Atoi(expStr)
	digits := strings.Replace(mant, ".", "", 1)
	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	switch {
	case exp >= 21 || exp < -6:
		b.WriteByte(digits[0])
		if len(digits) > 1 {
			b.WriteByte('.')
			b.WriteString(digits[1:])
		}
		b.WriteByte('e')
		b.WriteString(strconv.Itoa(exp))
	case exp >= 0:
		if len(digits) <= exp+1 {
			b.WriteString(digits)
			b.WriteString(strings.Repeat("0", exp+1-len(digits)))
			b.WriteByte('.')
		} else {
			b.WriteString(digits[:exp+1])
			b.WriteByte('.')
			b.WriteString(digits[exp+1:])
		}
	default:
		b.WriteString("0.")
		b.WriteString(strings.Repeat("0", -exp-1))
		b.WriteString(digits)
	}
	return b.String()
}
