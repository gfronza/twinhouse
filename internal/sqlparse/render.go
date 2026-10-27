package sqlparse

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// exprString renders e as SQL that parses back to an equivalent AST.
func exprString(e Expr) string {
	var w sqlWriter
	w.sub(e, false)
	return w.String()
}

// selectString renders a SELECT (including UNION branches, SETTINGS, FORMAT).
func selectString(s *SelectStmt) string {
	var w sqlWriter
	w.selectStmt(s)
	return w.String()
}

type sqlWriter struct{ strings.Builder }

// renderPrec is the binding strength of e when printed (see parser priorities).
func renderPrec(e Expr) int {
	switch v := e.(type) {
	case *LambdaExpr:
		return precLambda
	case *BinaryExpr:
		return binaryPrec(v.Op)
	case *UnaryExpr:
		if v.Op == "NOT" {
			return precNot
		}
		return precUnary
	case *IsNullExpr:
		return precIsNull
	case *BetweenExpr:
		return precBetween
	case *IntervalExpr:
		return precUnary
	case *Literal:
		if isNegativeNumber(v) {
			return precUnary
		}
	}
	return precAtom
}

func binaryPrec(op string) int {
	switch op {
	case "OR":
		return precOr
	case "AND":
		return precAnd
	case "||":
		return precConcat
	case "+", "-":
		return precAdd
	case "*", "/", "%":
		return precMul
	}
	return precCompare
}

func isNegativeNumber(l *Literal) bool {
	if l.Raw != "" {
		return strings.HasPrefix(l.Raw, "-")
	}
	switch v := l.Value.(type) {
	case int64:
		return v < 0
	case float64:
		return math.Signbit(v) && !math.IsNaN(v)
	}
	return false
}

func isNumericLiteral(e Expr) bool {
	l, ok := e.(*Literal)
	if !ok {
		return false
	}
	switch l.Value.(type) {
	case uint64, int64, float64, int, float32:
		return true
	}
	return false
}

func hasParens(e Expr) bool {
	b, ok := e.(*BinaryExpr)
	return ok && b.Parens
}

// sub renders e, parenthesized when need is set or the source had parens.
func (w *sqlWriter) sub(e Expr, need bool) {
	if need || hasParens(e) {
		w.WriteByte('(')
		w.expr(e)
		w.WriteByte(')')
		return
	}
	w.expr(e)
}

func (w *sqlWriter) list(es []Expr) {
	for i, e := range es {
		if i > 0 {
			w.WriteString(", ")
		}
		w.sub(e, false)
	}
}

func (w *sqlWriter) expr(e Expr) {
	switch v := e.(type) {
	case nil:
		w.WriteString("NULL")
	case *Literal:
		w.WriteString(literalSQL(v))
	case *Ident:
		for i, part := range v.Parts {
			if i > 0 {
				w.WriteByte('.')
			}
			w.WriteString(quoteIdent(part))
		}
	case *Param:
		typ := v.RawType
		if strings.TrimSpace(typ) == "" {
			typ = v.Type
		}
		fmt.Fprintf(w, "{%s:%s}", v.Name, typ)
	case *Star:
		if v.Table != "" {
			for _, part := range strings.Split(v.Table, ".") {
				w.WriteString(quoteIdent(part))
				w.WriteByte('.')
			}
		}
		w.WriteByte('*')
	case *FuncCall:
		w.funcCall(v)
	case *BinaryExpr:
		p := binaryPrec(v.Op)
		w.sub(v.Left, renderPrec(v.Left) < p)
		w.WriteString(" " + v.Op + " ")
		w.sub(v.Right, renderPrec(v.Right) <= p)
	case *UnaryExpr:
		if v.Op == "NOT" {
			// Always NOT(...): it parses as a call with exactly this operand.
			// A tuple operand keeps its own brackets: NOT ((1, 2)).
			w.WriteString("NOT (")
			w.sub(v.X, false)
			w.WriteByte(')')
			return
		}
		w.WriteString("-")
		w.sub(v.X, isNumericLiteral(v.X) || renderPrec(v.X) <= precUnary)
	case *IsNullExpr:
		w.sub(v.X, renderPrec(v.X) < precIsNull)
		if v.Not {
			w.WriteString(" IS NOT NULL")
		} else {
			w.WriteString(" IS NULL")
		}
	case *BetweenExpr:
		w.sub(v.X, renderPrec(v.X) <= precBetween)
		if v.Not {
			w.WriteString(" NOT")
		}
		w.WriteString(" BETWEEN ")
		w.sub(v.Low, renderPrec(v.Low) <= precBetween)
		w.WriteString(" AND ")
		w.sub(v.High, renderPrec(v.High) <= precBetween)
	case *TupleExpr:
		if v.Func {
			w.WriteString("tuple(")
			w.list(v.Elems)
			w.WriteByte(')')
			break
		}
		w.WriteByte('(')
		w.list(v.Elems)
		if len(v.Elems) == 1 {
			w.WriteByte(',')
		}
		w.WriteByte(')')
	case *ArrayExpr:
		if v.Func {
			w.WriteString("array(")
			w.list(v.Elems)
			w.WriteByte(')')
			break
		}
		w.WriteByte('[')
		w.list(v.Elems)
		w.WriteByte(']')
	case *IndexExpr:
		w.postfixOperand(v.X)
		w.WriteByte('[')
		w.sub(v.Index, false)
		w.WriteByte(']')
	case *TupleAccessExpr:
		_, isIdent := v.X.(*Ident)
		if v.Name != "" && isIdent {
			w.sub(v.X, true) // (t).name, since t.name would be a compound identifier
		} else {
			w.postfixOperand(v.X)
		}
		if v.Name != "" {
			w.WriteString("." + quoteIdent(v.Name))
		} else {
			w.WriteString("." + strconv.Itoa(v.Index))
		}
	case *CaseExpr:
		w.WriteString("CASE")
		if v.Operand != nil {
			w.WriteByte(' ')
			w.sub(v.Operand, false)
		}
		for _, wh := range v.Whens {
			w.WriteString(" WHEN ")
			w.sub(wh.Cond, false)
			w.WriteString(" THEN ")
			w.sub(wh.Result, false)
		}
		if v.Else != nil {
			w.WriteString(" ELSE ")
			w.sub(v.Else, false)
		}
		w.WriteString(" END")
	case *CastExpr:
		w.WriteString("CAST(")
		w.sub(v.X, false)
		w.WriteString(", " + quoteString(v.Type) + ")")
	case *SubqueryExpr:
		w.WriteByte('(')
		w.selectStmt(v.Select)
		w.WriteByte(')')
	case *LambdaExpr:
		if len(v.Params) == 1 {
			w.WriteString(quoteIdent(v.Params[0]))
		} else {
			w.WriteByte('(')
			for i, p := range v.Params {
				if i > 0 {
					w.WriteString(", ")
				}
				w.WriteString(quoteIdent(p))
			}
			w.WriteByte(')')
		}
		w.WriteString(" -> ")
		w.sub(v.Body, false)
	case *IntervalExpr:
		w.WriteString("INTERVAL ")
		w.sub(v.X, renderPrec(v.X) <= precConcat)
		w.WriteString(" " + v.Unit)
	case *AliasExpr:
		w.WriteByte('(')
		w.sub(v.X, false)
		w.WriteString(" AS " + quoteIdent(v.Alias) + ")")
	default:
		fmt.Fprintf(w, "/* unknown expression %T */", e)
	}
}

// postfixOperand renders the operand of [], .N; literals and operators need
// parentheses there.
func (w *sqlWriter) postfixOperand(x Expr) {
	_, star := x.(*Star)
	w.sub(x, renderPrec(x) < precPostfix || isNumericLiteral(x) || star)
}

func (w *sqlWriter) funcCall(f *FuncCall) {
	w.WriteString(quoteFuncName(f.Name))
	if f.Params != nil {
		w.WriteByte('(')
		w.list(f.Params)
		w.WriteByte(')')
	}
	w.WriteByte('(')
	if f.Distinct {
		w.WriteString("DISTINCT ")
	}
	w.list(f.Args)
	w.WriteByte(')')
	if f.Over != nil {
		w.WriteString(" OVER (")
		w.windowSpec(f.Over)
		w.WriteByte(')')
	}
}

func (w *sqlWriter) windowSpec(s *WindowSpec) {
	sep := ""
	if len(s.PartitionBy) > 0 {
		w.WriteString("PARTITION BY ")
		w.list(s.PartitionBy)
		sep = " "
	}
	if len(s.OrderBy) > 0 {
		w.WriteString(sep + "ORDER BY ")
		w.orderList(s.OrderBy)
		sep = " "
	}
	if s.Frame != "" {
		w.WriteString(sep + s.Frame)
	}
}

func (w *sqlWriter) orderList(items []OrderItem) {
	for i, it := range items {
		if i > 0 {
			w.WriteString(", ")
		}
		w.sub(it.Expr, false)
		if it.Desc {
			w.WriteString(" DESC")
		}
		if it.NullsFirst != nil {
			if *it.NullsFirst {
				w.WriteString(" NULLS FIRST")
			} else {
				w.WriteString(" NULLS LAST")
			}
		}
		if it.Collate != "" {
			w.WriteString(" COLLATE " + quoteString(it.Collate))
		}
	}
}

func literalSQL(l *Literal) string {
	if l.Raw != "" {
		return l.Raw
	}
	switch v := l.Value.(type) {
	case nil:
		return "NULL"
	case string:
		return quoteString(v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case uint64:
		return strconv.FormatUint(v, 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	case float64:
		return formatFloat(v)
	case float32:
		return formatFloat(float64(v))
	}
	return quoteString(fmt.Sprint(l.Value))
}

// quoteString renders s as a ClickHouse single-quoted literal.
func quoteString(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('\'')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case 0:
			b.WriteString(`\0`)
		case '\\':
			b.WriteString(`\\`)
		case '\'':
			b.WriteString(`\'`)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('\'')
	return b.String()
}

// Words that must be back-quoted when used as identifiers in rendered SQL.
var renderReserved = func() map[string]bool {
	m := map[string]bool{}
	for k := range aliasReserved {
		m[k] = true
	}
	for k := range nonExprKeywords {
		m[k] = true
	}
	for _, k := range []string{"NULL", "TRUE", "FALSE", "INF", "INFINITY", "NAN", "CASE", "CAST",
		"INTERVAL", "DATE", "TIMESTAMP", "EXISTS", "NOT", "COLUMNS", "TUPLE", "ARRAY"} {
		m[k] = true
	}
	return m
}()

// isPlainIdent reports whether s needs no quoting. Unlike the lexer it does
// not accept '$', so such names are always back-quoted.
func isPlainIdent(s string) bool {
	if s == "" || !isIdentStart(s[0]) {
		return false
	}
	for i := 1; i < len(s); i++ {
		if !isIdentStart(s[i]) && !isDigit(s[i]) {
			return false
		}
	}
	return true
}

// quoteIdent back-quotes an identifier when needed.
func quoteIdent(s string) string {
	if isPlainIdent(s) && !renderReserved[strings.ToUpper(s)] {
		return s
	}
	return backQuote(s)
}

func backQuote(s string) string {
	var b strings.Builder
	b.WriteByte('`')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '`':
			b.WriteString("\\`")
		case '\\':
			b.WriteString(`\\`)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('`')
	return b.String()
}

func quoteFuncName(s string) string {
	if isPlainIdent(s) {
		return s
	}
	return backQuote(s)
}

// ---------------------------------------------------------------------------
// SELECT

func (w *sqlWriter) selectStmt(s *SelectStmt) {
	w.selectCore(s, len(s.Union) == 0)
	if len(s.Union) > 0 && s.OwnSettings > 0 {
		w.settings(s.Settings[:s.OwnSettings])
	}
	for i, b := range s.Union {
		w.WriteString(" UNION " + s.UnionOps[i] + " ")
		if len(b.Union) > 0 || len(b.Settings) > 0 || b.Format != "" {
			w.WriteByte('(')
			w.selectStmt(b)
			w.WriteByte(')')
		} else {
			w.selectCore(b, false)
		}
	}
	if len(s.Union) > 0 && len(s.Settings) > s.OwnSettings {
		w.settings(s.Settings[s.OwnSettings:])
	}
	if s.Format != "" {
		w.WriteString(" FORMAT " + quoteIdent(s.Format))
	}
}

func (w *sqlWriter) selectCore(s *SelectStmt, withSettings bool) {
	if len(s.With) > 0 {
		w.WriteString("WITH ")
		for i, it := range s.With {
			if i > 0 {
				w.WriteString(", ")
			}
			if it.Subquery != nil {
				w.WriteString(quoteIdent(it.Name) + " AS (")
				w.selectStmt(it.Subquery)
				w.WriteByte(')')
			} else {
				w.sub(it.Expr, false)
				w.WriteString(" AS " + quoteIdent(it.Name))
			}
		}
		w.WriteByte(' ')
	}
	w.WriteString("SELECT ")
	if s.Distinct {
		w.WriteString("DISTINCT ")
	}
	w.selectItems(s.Columns)
	if s.From != nil {
		w.WriteString(" FROM ")
		w.tableExpr(s.From)
	}
	if aj := s.ArrayJoin; aj != nil {
		if aj.Left {
			w.WriteString(" LEFT")
		}
		w.WriteString(" ARRAY JOIN ")
		w.selectItems(aj.Items)
	}
	for _, j := range s.Joins {
		if j.Strictness != "" {
			w.WriteString(" " + j.Strictness)
		}
		w.WriteString(" " + j.Kind + " JOIN ")
		w.tableExpr(j.Table)
		if j.On != nil {
			w.WriteString(" ON ")
			w.sub(j.On, false)
		}
		if len(j.Using) > 0 {
			w.WriteString(" USING (")
			for i, u := range j.Using {
				if i > 0 {
					w.WriteString(", ")
				}
				w.WriteString(quoteIdent(u))
			}
			w.WriteByte(')')
		}
	}
	if s.Prewhere != nil {
		w.WriteString(" PREWHERE ")
		w.sub(s.Prewhere, false)
	}
	if s.Where != nil {
		w.WriteString(" WHERE ")
		w.sub(s.Where, false)
	}
	if s.GroupByAll {
		w.WriteString(" GROUP BY ALL")
	} else if len(s.GroupBy) > 0 {
		w.WriteString(" GROUP BY ")
		w.list(s.GroupBy)
	}
	if s.WithTotals {
		w.WriteString(" WITH TOTALS")
	}
	if s.Having != nil {
		w.WriteString(" HAVING ")
		w.sub(s.Having, false)
	}
	if len(s.OrderBy) > 0 {
		w.WriteString(" ORDER BY ")
		w.orderList(s.OrderBy)
	}
	if lb := s.LimitBy; lb != nil {
		w.WriteString(" LIMIT ")
		w.sub(lb.Limit, false)
		if lb.Offset != nil {
			w.WriteString(" OFFSET ")
			w.sub(lb.Offset, false)
		}
		w.WriteString(" BY ")
		w.list(lb.By)
	}
	if s.Limit != nil {
		w.WriteString(" LIMIT ")
		w.sub(s.Limit, false)
		if s.Offset != nil {
			w.WriteString(" OFFSET ")
			w.sub(s.Offset, false)
		}
	} else if s.Offset != nil {
		w.WriteString(" OFFSET ")
		w.sub(s.Offset, false)
	}
	if withSettings && len(s.Settings) > 0 {
		w.settings(s.Settings)
	}
}

func (w *sqlWriter) selectItems(items []SelectItem) {
	for i, it := range items {
		if i > 0 {
			w.WriteString(", ")
		}
		w.sub(it.Expr, false)
		if it.Alias != "" {
			w.WriteString(" AS " + quoteIdent(it.Alias))
		}
	}
}

func (w *sqlWriter) settings(ss []Setting) {
	w.WriteString(" SETTINGS ")
	for i, s := range ss {
		if i > 0 {
			w.WriteString(", ")
		}
		w.WriteString(quoteIdent(s.Name) + " = ")
		w.sub(s.Value, false)
	}
}

func (w *sqlWriter) tableExpr(t *TableExpr) {
	switch {
	case t.Subquery != nil:
		w.WriteByte('(')
		w.selectStmt(t.Subquery)
		w.WriteByte(')')
	case t.Func != nil:
		w.funcCall(t.Func)
	default:
		if t.Database != "" {
			w.WriteString(quoteIdent(t.Database) + ".")
		}
		w.WriteString(quoteIdent(t.Table))
	}
	if t.Alias != "" {
		w.WriteString(" AS " + quoteIdent(t.Alias))
	}
	if t.Final {
		w.WriteString(" FINAL")
	}
}
