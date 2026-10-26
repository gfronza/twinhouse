package sqlparse

import (
	"math"
	"math/big"
	"strconv"
	"strings"
)

// Operator priorities, lowest to highest, as in ClickHouse's
// ExpressionListParsers.cpp. Comparisons, LIKE and IN share one level.
const (
	precLambda  = 1
	precTernary = 2
	precOr      = 3
	precAnd     = 4
	precNot     = 5
	precIsNull  = 6
	precBetween = 7
	precCompare = 9
	precConcat  = 10
	precAdd     = 11
	precMul     = 12
	precUnary   = 13
	precPostfix = 14
	precAtom    = 15
)

// parseExpr parses a full expression (including lambdas), without alias.
func (p *parser) parseExpr() Expr {
	p.enter()
	defer p.leave()
	if params, ok := p.tryLambdaParams(); ok {
		return &LambdaExpr{Params: params, Body: p.parseExpr()}
	}
	return p.parseOp(0)
}

// parseExprAlias parses an expression optionally followed by `AS alias`; it
// is used for list elements, where ClickHouse allows aliases.
func (p *parser) parseExprAlias() Expr {
	e := p.parseExpr()
	if p.acceptKw("AS") {
		name, _ := p.parseIdentName()
		return &AliasExpr{X: e, Alias: name}
	}
	return e
}

// tryLambdaParams consumes `x ->` or `(x, y) ->` if present.
func (p *parser) tryLambdaParams() ([]string, bool) {
	t := p.peek()
	if (t.kind == tIdent || t.kind == tQIdent) && isOp(p.peekN(1), "->") {
		name, _ := p.parseIdentName()
		p.next()
		return []string{name}, true
	}
	if !isOp(t, "(") {
		return nil, false
	}
	i := 1
	for {
		k := p.peekN(i).kind
		if k != tIdent && k != tQIdent {
			return nil, false
		}
		i++
		if isOp(p.peekN(i), ",") {
			i++
			continue
		}
		if isOp(p.peekN(i), ")") && isOp(p.peekN(i+1), "->") {
			break
		}
		return nil, false
	}
	p.next()
	var params []string
	for {
		name, _ := p.parseIdentName()
		params = append(params, name)
		if !p.acceptOp(",") {
			break
		}
	}
	p.expectOp(")")
	p.expectOp("->")
	return params, true
}

// binaryOp identifies the infix/postfix operator at the current position.
// It returns the canonical operator, its priority and its token count.
func (p *parser) binaryOp() (string, int, int) {
	t := p.peek()
	if t.kind == tOp {
		switch t.text {
		case "=", "==":
			return "=", precCompare, 1
		case "!=", "<>":
			return "!=", precCompare, 1
		case "<", "<=", ">", ">=":
			return t.text, precCompare, 1
		case "||":
			return "||", precConcat, 1
		case "+", "-":
			return t.text, precAdd, 1
		case "*", "/", "%":
			return t.text, precMul, 1
		case "?":
			return "?", precTernary, 1
		case "<=>":
			return "<=>", precIsNull, 1
		}
		return "", 0, 0
	}
	if t.kind != tIdent {
		return "", 0, 0
	}
	switch strings.ToUpper(t.text) {
	case "OR":
		return "OR", precOr, 1
	case "AND":
		return "AND", precAnd, 1
	case "IS":
		if p.atKw("IS", "NULL") {
			return "IS NULL", precIsNull, 2
		}
		if p.atKw("IS", "NOT", "NULL") {
			return "IS NOT NULL", precIsNull, 3
		}
		if p.atKw("IS", "NOT", "DISTINCT") || p.atKw("IS", "DISTINCT") {
			return "<=>", precIsNull, 1
		}
		return "", 0, 0
	case "BETWEEN":
		return "BETWEEN", precBetween, 1
	case "LIKE", "ILIKE", "IN", "REGEXP":
		return strings.ToUpper(t.text), precCompare, 1
	case "NOT":
		n := p.peekN(1)
		if n.kind == tIdent {
			switch u := strings.ToUpper(n.text); u {
			case "LIKE", "ILIKE", "IN":
				return "NOT " + u, precCompare, 2
			case "BETWEEN":
				return "NOT BETWEEN", precBetween, 2
			}
		}
		return "", 0, 0
	case "GLOBAL":
		if p.atKw("GLOBAL", "IN") {
			return "GLOBAL IN", precCompare, 2
		}
		if p.atKw("GLOBAL", "NOT", "IN") {
			return "GLOBAL NOT IN", precCompare, 3
		}
		return "", 0, 0
	case "MOD":
		return "%", precMul, 1
	case "DIV":
		return "DIV", precMul, 1
	}
	return "", 0, 0
}

// parseOp parses an expression whose operators all bind tighter than min.
func (p *parser) parseOp(min int) Expr {
	p.enter()
	defer p.leave()
	return p.parseOpFrom(p.parseUnary(), min)
}

func (p *parser) parseOpFrom(left Expr, min int) Expr {
	for {
		op, prec, n := p.binaryOp()
		if op == "" || prec <= min {
			return left
		}
		if op == "<=>" {
			notImplemented("IS [NOT] DISTINCT FROM is not supported")
		}
		for i := 0; i < n; i++ {
			p.next()
		}
		switch op {
		case "?":
			then := p.parseOp(precTernary)
			p.expectOp(":")
			els := p.parseOp(precTernary)
			if p.atOp("?") {
				p.failExpected("end of expression (nested ternary operators must be parenthesized)")
			}
			left = &FuncCall{Name: "if", Args: []Expr{left, then, els}}
		case "IS NULL", "IS NOT NULL":
			// ClickHouse applies following postfix operators to the result:
			// a IS NULL::String is CAST(isNull(a), 'String').
			left = p.parsePostfix(&IsNullExpr{X: left, Not: op == "IS NOT NULL"}, p.last.end)
		case "BETWEEN", "NOT BETWEEN":
			lo := p.parseOp(precBetween)
			p.expectKw("AND")
			hi := p.parseOp(precBetween)
			left = &BetweenExpr{X: left, Low: lo, High: hi, Not: op == "NOT BETWEEN"}
		case "DIV":
			left = &FuncCall{Name: "intDiv", Args: []Expr{left, p.parseOp(prec)}}
		case "REGEXP":
			left = &FuncCall{Name: "match", Args: []Expr{left, p.parseOp(prec)}}
		default:
			left = &BinaryExpr{Op: op, Left: left, Right: p.parseOp(prec)}
		}
	}
}

// parseUnary parses prefix operators and a postfix-decorated primary.
func (p *parser) parseUnary() Expr {
	t := p.peek()
	if isKw(t, "NOT") {
		p.next()
		if p.atOp("(") {
			// NOT(...) is the function not(...) and binds like a primary.
			start := p.peek().pos
			p.next()
			args, distinct := p.parseCallArgs()
			if distinct {
				p.fail("DISTINCT is not allowed in NOT(...)")
			}
			var e Expr
			if len(args) == 1 {
				e = &UnaryExpr{Op: "NOT", X: args[0]}
			} else {
				e = &FuncCall{Name: t.text, Args: args}
			}
			return p.parsePostfix(e, start)
		}
		return &UnaryExpr{Op: "NOT", X: p.parseOp(precNot)}
	}
	if isOp(t, "+") {
		// ClickHouse drops a unary plus (+1 is the literal 1). It does not
		// fold the minus of `+ -1` into the literal: that is negate(1).
		p.next()
		if isOp(p.peek(), "-") {
			p.next()
			return &UnaryExpr{Op: "-", X: p.parseOp(precUnary)}
		}
		return p.parseOp(precUnary)
	}
	if isOp(t, "-") {
		n := p.peekN(1)
		if n.kind == tNumber || (n.kind == tIdent && isInfNan(n.text) && !isOp(p.peekN(2), "(")) {
			p.next()
			p.next()
			return p.parsePostfix(p.numberLiteral(n, true), t.pos)
		}
		p.next()
		return &UnaryExpr{Op: "-", X: p.parseOp(precUnary)}
	}
	start := t.pos
	return p.parsePostfix(p.parsePrimary(), start)
}

func isInfNan(s string) bool {
	switch strings.ToLower(s) {
	case "inf", "infinity", "nan":
		return true
	}
	return false
}

// parsePostfix applies [], .N and ::Type to x, which started at offset start.
func (p *parser) parsePostfix(x Expr, start int) Expr {
	for {
		t := p.peek()
		switch {
		case isOp(t, "["):
			p.next()
			idx := p.parseExprAlias()
			p.expectOp("]")
			x = &IndexExpr{X: x, Index: idx}
		case isOp(t, "."):
			n := p.peekN(1)
			switch n.kind {
			case tNumber:
				p.next()
				p.next()
				parts := strings.Split(n.text, ".")
				for _, part := range parts {
					i, err := strconv.Atoi(part)
					if err != nil || i < 1 {
						p.failAt(n, "tuple element index (a positive integer)")
					}
					x = &TupleAccessExpr{X: x, Index: i}
				}
			case tIdent, tQIdent:
				p.next()
				name, _ := p.parseIdentName()
				x = &TupleAccessExpr{X: x, Name: name}
			default:
				return x
			}
		case isOp(t, "::"):
			p.next()
			end := p.last.end
			typ := p.typeText(func(token) bool { return true })
			x = &CastExpr{X: castOperand(x, p.src[start:end-2]), Type: typ}
		default:
			return x
		}
	}
}

// castOperand mirrors ClickHouse: in `<literal>::T` the source text of a
// number or array/tuple literal is cast as a string (1::UInt8 is
// CAST('1', 'UInt8'), [1, 2]::Array(UInt8) is CAST('[1, 2]', ...), even
// (1)::UInt8 is CAST('(1)', ...)); strings, NULL, booleans and inf/nan are
// cast as values.
func castOperand(x Expr, src string) Expr {
	src = strings.TrimSpace(src)
	switch v := x.(type) {
	case *Literal:
		digits := strings.TrimLeft(v.Raw, "- ")
		if !isNumericLiteral(v) || digits == "" || !(isDigit(digits[0]) || digits[0] == '.') {
			return x
		}
	case *ArrayExpr, *TupleExpr:
		if !allLiterals(x) {
			return x
		}
	default:
		return x
	}
	return &Literal{Value: src, Raw: quoteString(src)}
}

func allLiterals(x Expr) bool {
	switch v := x.(type) {
	case *Literal:
		return true
	case *ArrayExpr:
		for _, e := range v.Elems {
			if !allLiterals(e) {
				return false
			}
		}
		return true
	case *TupleExpr:
		for _, e := range v.Elems {
			if !allLiterals(e) {
				return false
			}
		}
		return true
	}
	return false
}

// Clause keywords. ClickHouse accepts almost every keyword as a bare
// identifier in expression position (SELECT end, order FROM t), so these only
// stop optional constructs such as an INTERVAL operand; see primaryReserved.
var nonExprKeywords = map[string]bool{
	"SELECT": true, "FROM": true, "WHERE": true, "PREWHERE": true, "GROUP": true, "ORDER": true,
	"HAVING": true, "LIMIT": true, "OFFSET": true, "UNION": true, "SETTINGS": true, "FORMAT": true,
	"JOIN": true, "ON": true, "USING": true, "AS": true, "AND": true, "OR": true, "THEN": true,
	"ELSE": true, "END": true, "WHEN": true, "BETWEEN": true, "LIKE": true, "ILIKE": true, "IN": true,
	"IS": true, "WITH": true, "INTO": true, "VALUES": true, "FINAL": true, "DISTINCT": true,
}

func (p *parser) parsePrimary() Expr {
	t := p.peek()
	switch t.kind {
	case tNumber:
		p.next()
		return p.numberLiteral(t, false)
	case tString:
		p.next()
		return &Literal{Value: t.val, Raw: t.text}
	case tIdent:
		return p.parseIdentPrimary()
	case tQIdent:
		return p.parseIdentifierOrCall()
	case tOp:
		switch t.text {
		case "(":
			return p.parseParen()
		case "[":
			p.next()
			return &ArrayExpr{Elems: nonNil(p.parseExprList("]"))}
		case "{":
			if p.inValues {
				if n := p.peekN(1); n.kind == tString || n.kind == tNumber || isOp(n, "-") || isOp(n, "}") {
					return p.parseMapLiteral()
				}
			}
			return p.parseParam()
		case "*":
			p.next()
			p.checkStarModifiers()
			return &Star{}
		}
	}
	p.failAt(t, "expression")
	return nil
}

// parseExprList parses comma-separated aliased expressions up to the closing
// token, which it consumes.
func (p *parser) parseExprList(closing string) []Expr {
	var out []Expr
	if p.acceptOp(closing) {
		return out
	}
	for {
		out = append(out, p.parseExprAlias())
		if !p.acceptOp(",") {
			break
		}
	}
	p.expectOp(closing)
	return out
}

func (p *parser) checkStarModifiers() {
	for _, kw := range []string{"EXCEPT", "REPLACE", "APPLY"} {
		if p.atKw(kw) {
			notImplemented("Asterisk modifier %s is not supported", kw)
		}
	}
}

// looksLikeSelect reports whether the tokens at offset i start a SELECT
// (possibly wrapped in parentheses).
func (p *parser) looksLikeSelect(i int) bool {
	for isOp(p.peekN(i), "(") {
		i++
	}
	t := p.peekN(i)
	return isKw(t, "SELECT") || isKw(t, "WITH")
}

func (p *parser) parseParen() Expr {
	if p.looksLikeSelect(1) {
		p.next()
		sel := p.parseSelectUnion()
		p.expectOp(")")
		return &SubqueryExpr{Select: sel}
	}
	p.next()
	if p.acceptOp(")") {
		return &TupleExpr{Elems: []Expr{}}
	}
	first := p.parseExprAlias()
	if p.acceptOp(",") {
		// A trailing comma is allowed: (1,) and (1, 2,).
		elems := []Expr{first}
		for !p.acceptOp(")") {
			elems = append(elems, p.parseExprAlias())
			if !p.acceptOp(",") {
				p.expectOp(")")
				break
			}
		}
		return &TupleExpr{Elems: elems}
	}
	p.expectOp(")")
	if b, ok := first.(*BinaryExpr); ok && (b.Op == "AND" || b.Op == "OR" || b.Op == "||") {
		b.Parens = true
	}
	if isTupleLiteral(first) {
		// ClickHouse: a parenthesized tuple literal is a one-element tuple,
		// (('a', 'b')) = tuple(('a', 'b')).
		return &TupleExpr{Elems: []Expr{first}}
	}
	return first
}

// isTupleLiteral reports whether e is what ClickHouse parses as a tuple
// literal: (a, b, ...) with at least two elements, each a literal constant
// (number, string, NULL, boolean) or itself a tuple literal. Arrays,
// expressions, casts and tuple(...) calls make it an ordinary tuple().
func isTupleLiteral(e Expr) bool {
	t, ok := e.(*TupleExpr)
	if !ok || t.Func || len(t.Elems) < 2 {
		return false
	}
	for _, x := range t.Elems {
		if _, ok := x.(*Literal); !ok && !isTupleLiteral(x) {
			return false
		}
	}
	return true
}

// isArrayLiteral reports whether e is what ClickHouse parses as an array
// literal: a non-empty [...] whose elements are literal constants, tuple
// literals or non-empty array literals. array(...) calls are not literals.
func isArrayLiteral(e Expr) bool {
	a, ok := e.(*ArrayExpr)
	if !ok || a.Func || len(a.Elems) == 0 {
		return false
	}
	for _, x := range a.Elems {
		if _, ok := x.(*Literal); !ok && !isTupleLiteral(x) && !isArrayLiteral(x) {
			return false
		}
	}
	return true
}

// parseMapLiteral parses the Values-format map literal {k: v, ...} into
// map(k, v, ...).
func (p *parser) parseMapLiteral() Expr {
	p.next() // {
	fc := &FuncCall{Name: "map", Args: []Expr{}}
	for !p.acceptOp("}") {
		if len(fc.Args) > 0 {
			p.expectOp(",")
		}
		k := p.parseOp(precTernary)
		p.expectOp(":")
		fc.Args = append(fc.Args, k, p.parseExpr())
	}
	return fc
}

// parseParam parses {name:Type}.
func (p *parser) parseParam() Expr {
	p.next()
	name, _ := p.parseIdentName()
	p.expectOp(":")
	start := p.peek().pos
	typ := p.typeUntil("}", "closing curly bracket")
	return &Param{Name: name, Type: typ, RawType: p.src[start:p.last.pos]}
}

// typeUntil reads a type up to the closing token at bracket depth 0, which it
// consumes, and returns the normalized type text.
func (p *parser) typeUntil(closing, what string) string {
	if k := p.peek().kind; k != tIdent && k != tQIdent {
		p.failExpected("data type")
	}
	var toks []token
	depth := 0
	for {
		t := p.peek()
		if t.kind == tEOF {
			p.failExpected(what)
		}
		if depth == 0 && isOp(t, closing) {
			break
		}
		if isOp(t, "(") {
			depth++
		} else if isOp(t, ")") {
			depth--
		}
		toks = append(toks, p.next())
	}
	p.next()
	return joinTokens(toks)
}

// numberLiteral converts a number token, applying a folded leading minus as
// ClickHouse's parser does (-1 is the literal -1, not negate(1)).
func (p *parser) numberLiteral(t token, neg bool) *Literal {
	raw := t.text
	if neg {
		raw = "-" + raw
	}
	v, ok := parseNumber(t.text)
	if !ok {
		p.failAt(t, "number")
	}
	if !neg {
		return &Literal{Value: v, Raw: raw}
	}
	switch n := v.(type) {
	case uint64:
		switch {
		case n == 0:
			return &Literal{Value: uint64(0), Raw: raw}
		case n <= 1<<63:
			return &Literal{Value: int64(-n), Raw: raw}
		default:
			return &Literal{Value: -float64(n), Raw: raw}
		}
	case float64:
		return &Literal{Value: -n, Raw: raw}
	}
	return &Literal{Value: v, Raw: raw}
}

// parseNumber parses ClickHouse numeric literal text (also inf/nan).
func parseNumber(text string) (any, bool) {
	switch strings.ToLower(text) {
	case "inf", "infinity":
		return math.Inf(1), true
	case "nan":
		return math.NaN(), true
	}
	s := strings.ReplaceAll(text, "_", "")
	if len(s) > 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X' || s[1] == 'b' || s[1] == 'B') {
		base := 16
		if s[1] == 'b' || s[1] == 'B' {
			base = 2
		}
		if u, err := strconv.ParseUint(s[2:], base, 64); err == nil {
			return u, true
		}
		bi, ok := new(big.Int).SetString(s[2:], base)
		if !ok {
			return nil, false
		}
		f, _ := new(big.Float).SetInt(bi).Float64()
		return f, true
	}
	if strings.ContainsAny(s, ".eE") {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil && !isRangeErr(err) {
			return nil, false
		}
		return f, true
	}
	if u, err := strconv.ParseUint(s, 10, 64); err == nil {
		return u, true
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil && !isRangeErr(err) {
		return nil, false
	}
	return f, true
}

func isRangeErr(err error) bool {
	ne, ok := err.(*strconv.NumError)
	return ok && ne.Err == strconv.ErrRange
}

// ---------------------------------------------------------------------------
// Identifiers, keywords and special function syntaxes

func (p *parser) parseIdentPrimary() Expr {
	t := p.peek()
	n1 := p.peekN(1)
	call := isOp(n1, "(")
	switch u := strings.ToUpper(t.text); u {
	case "NULL":
		if !call {
			p.next()
			return &Literal{Value: nil, Raw: t.text}
		}
	case "TRUE", "FALSE":
		if !call {
			p.next()
			return &Literal{Value: u == "TRUE", Raw: t.text}
		}
	case "INF", "INFINITY", "NAN":
		if !call && !isOp(n1, ".") {
			p.next()
			return p.numberLiteral(t, false)
		}
	case "CASE":
		return p.parseCase()
	case "CAST":
		if call {
			return p.parseCast()
		}
	case "INTERVAL":
		if canStartExpr(n1) {
			return p.parseInterval()
		}
	case "DATE", "TIMESTAMP":
		if n1.kind == tString {
			p.next()
			p.next()
			fn := "toDate"
			if u == "TIMESTAMP" {
				fn = "toDateTime"
			}
			return &FuncCall{Name: fn, Args: []Expr{&Literal{Value: n1.val, Raw: n1.text}}}
		}
	case "EXISTS":
		if call {
			p.next()
			if !p.looksLikeSelect(1) {
				p.failAt(p.peekN(1), "SELECT subquery")
			}
			sub := p.parseParen()
			return &FuncCall{Name: "exists", Args: []Expr{sub}}
		}
	case "COLUMNS":
		if call {
			notImplemented("COLUMNS matcher is not supported")
		}
	default:
		if primaryReserved[u] && !call {
			p.failAt(t, "expression")
		}
	}
	return p.parseIdentifierOrCall()
}

// Keywords ClickHouse rejects as bare identifiers in expression position.
var primaryReserved = map[string]bool{"DISTINCT": true, "ALL": true}

// canStartExpr reports whether t can begin an INTERVAL operand.
func canStartExpr(t token) bool {
	switch t.kind {
	case tNumber, tString, tQIdent:
		return true
	case tIdent:
		return !nonExprKeywords[strings.ToUpper(t.text)]
	case tOp:
		return t.text == "(" || t.text == "-" || t.text == "{"
	}
	return false
}

// parseIdentifierOrCall parses a compound identifier, qualified asterisk or
// function call.
func (p *parser) parseIdentifierOrCall() Expr {
	name, _ := p.parseIdentName()
	if p.atOp("(") {
		return p.parseCall(name)
	}
	parts := []string{name}
	for p.atOp(".") {
		n := p.peekN(1)
		if n.kind == tIdent || n.kind == tQIdent {
			p.next()
			part, _ := p.parseIdentName()
			parts = append(parts, part)
			continue
		}
		if isOp(n, "*") {
			p.next()
			p.next()
			p.checkStarModifiers()
			return &Star{Table: strings.Join(parts, ".")}
		}
		break
	}
	if len(parts) > 1 && p.atOp("(") {
		p.failExpected("identifier (qualified function names are not supported)")
	}
	return &Ident{Parts: parts}
}

// parseCallArgs parses call arguments after '(' through ')'.
func (p *parser) parseCallArgs() (args []Expr, distinct bool) {
	if p.acceptOp(")") {
		return nil, false
	}
	if p.atKw("DISTINCT") && !isOp(p.peekN(1), ")") && !isOp(p.peekN(1), ",") {
		p.next()
		distinct = true
	} else if p.atKw("ALL") && !isOp(p.peekN(1), ")") && !isOp(p.peekN(1), ",") && !isOp(p.peekN(1), "(") {
		p.next()
	}
	for {
		args = append(args, p.parseExprAlias())
		if !p.acceptOp(",") {
			break
		}
	}
	p.expectOp(")")
	return args, distinct
}

func (p *parser) parseCall(name string) Expr {
	lower := strings.ToLower(name)
	switch lower {
	case "tuple", "array":
		if name == lower {
			p.next()
			if lower == "tuple" {
				return &TupleExpr{Elems: nonNil(p.parseExprList(")")), Func: true}
			}
			return &ArrayExpr{Elems: nonNil(p.parseExprList(")")), Func: true}
		}
	case "extract":
		if p.peekN(1).kind == tIdent && isKw(p.peekN(2), "FROM") {
			return p.parseExtract()
		}
	case "position":
		return p.parsePosition(name)
	case "substring":
		return p.parseSubstring(name)
	case "trim", "ltrim", "rtrim":
		return p.parseTrim(lower)
	case "dateadd", "date_add", "timestampadd", "timestamp_add":
		return p.parseDateAddSub(name, "+")
	case "datesub", "date_sub", "timestampsub", "timestamp_sub":
		return p.parseDateAddSub(name, "-")
	case "datediff", "date_diff", "timestampdiff", "timestamp_diff":
		return p.parseDateDiff()
	case "any", "all", "some":
		if p.looksLikeSelect(1) {
			notImplemented("%s (subquery) comparisons are not supported", strings.ToUpper(name))
		}
	}
	p.next() // (
	fc := &FuncCall{Name: name}
	args, distinct := p.parseCallArgs()
	if p.atOp("(") {
		if distinct {
			p.failExpected("closing round bracket (DISTINCT is not allowed in parameters)")
		}
		p.next()
		fc.Params = nonNil(args)
		args, distinct = p.parseCallArgs()
	}
	fc.Args, fc.Distinct = args, distinct
	if len(fc.Args) == 1 && lower == "count" && fc.Params == nil {
		if s, ok := fc.Args[0].(*Star); ok && s.Table == "" {
			fc.Args = nil
		}
	}
	if p.atKw("OVER") {
		p.next()
		fc.Over = p.parseWindowSpec()
	}
	for _, kw := range [][]string{{"FILTER"}, {"RESPECT", "NULLS"}, {"IGNORE", "NULLS"}} {
		if p.atKw(kw...) {
			notImplemented("%s is not supported", strings.Join(kw, " "))
		}
	}
	return fc
}

func nonNil(es []Expr) []Expr {
	if es == nil {
		return []Expr{}
	}
	return es
}

func (p *parser) parseWindowSpec() *WindowSpec {
	if !p.atOp("(") {
		notImplemented("Named windows are not supported")
	}
	p.next()
	w := &WindowSpec{}
	if p.acceptKw("PARTITION", "BY") {
		for {
			w.PartitionBy = append(w.PartitionBy, p.parseExprAlias())
			if !p.acceptOp(",") {
				break
			}
		}
	}
	if p.acceptKw("ORDER", "BY") {
		w.OrderBy = p.parseOrderList()
	}
	if p.atKw("ROWS") || p.atKw("RANGE") || p.atKw("GROUPS") {
		var toks []token
		for !p.atOp(")") {
			if p.peek().kind == tEOF {
				p.failExpected("closing round bracket")
			}
			toks = append(toks, p.next())
		}
		w.Frame = joinTokens(toks)
	}
	p.expectOp(")")
	return w
}

func (p *parser) parseCase() Expr {
	p.next()
	c := &CaseExpr{}
	if !p.atKw("WHEN") {
		c.Operand = p.parseExpr()
	}
	for p.acceptKw("WHEN") {
		cond := p.parseExpr()
		p.expectKw("THEN")
		c.Whens = append(c.Whens, WhenClause{Cond: cond, Result: p.parseExpr()})
	}
	if len(c.Whens) == 0 {
		p.failExpected("WHEN")
	}
	if p.acceptKw("ELSE") {
		c.Else = p.parseExpr()
	}
	p.expectKw("END")
	return c
}

func (p *parser) parseCast() Expr {
	name := p.next().text
	p.next() // (
	x := p.parseExpr()
	if p.acceptKw("AS") {
		return &CastExpr{X: x, Type: p.typeUntil(")", "closing round bracket")}
	}
	p.expectOp(",")
	t := p.parseExprAlias()
	p.expectOp(")")
	if l, ok := t.(*Literal); ok {
		if s, ok := l.Value.(string); ok {
			return &CastExpr{X: x, Type: s}
		}
	}
	return &FuncCall{Name: name, Args: []Expr{x, t}}
}

// intervalUnits maps ClickHouse interval unit spellings to canonical units.
var intervalUnits = func() map[string]string {
	m := map[string]string{}
	add := func(unit string, aliases ...string) {
		for _, a := range append([]string{unit, "SQL_TSI_" + unit}, aliases...) {
			m[a] = unit
		}
		m[unit+"S"] = unit
	}
	add("NANOSECOND", "NS")
	add("MICROSECOND", "MCS", "US")
	add("MILLISECOND", "MS")
	add("SECOND", "SS", "S")
	add("MINUTE", "MI", "N")
	add("HOUR", "HH", "H")
	add("DAY", "DD", "D")
	add("WEEK", "WK", "WW")
	add("MONTH", "MM", "M")
	add("QUARTER", "QQ", "Q")
	add("YEAR", "YYYY", "YY")
	return m
}()

func intervalUnit(word string) (string, bool) {
	u, ok := intervalUnits[strings.ToUpper(word)]
	return u, ok
}

func (p *parser) parseInterval() Expr {
	p.next()
	if t := p.peek(); t.kind == tString {
		if n := p.peekN(1); n.kind == tIdent {
			if u, ok := intervalUnit(n.text); ok {
				p.next()
				p.next()
				return &IntervalExpr{X: &Literal{Value: t.val, Raw: t.text}, Unit: u}
			}
		}
		// INTERVAL '1 day'
		fields := strings.Fields(t.val)
		if len(fields) == 2 {
			if u, ok := intervalUnit(fields[1]); ok {
				if v, ok := parseNumber(fields[0]); ok {
					p.next()
					return &IntervalExpr{X: &Literal{Value: v, Raw: fields[0]}, Unit: u}
				}
			}
		}
		p.failAt(t, "interval value with unit, e.g. '1 day'")
	}
	x := p.parseOp(precConcat)
	t := p.peek()
	if t.kind == tIdent {
		if u, ok := intervalUnit(t.text); ok {
			p.next()
			return &IntervalExpr{X: x, Unit: u}
		}
	}
	p.failAt(t, "interval unit (SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR, ...)")
	return nil
}

var extractFuncs = map[string]string{
	"SECOND": "toSecond", "MINUTE": "toMinute", "HOUR": "toHour", "DAY": "toDayOfMonth",
	"MONTH": "toMonth", "QUARTER": "toQuarter", "YEAR": "toYear",
}

func (p *parser) parseExtract() Expr {
	p.next() // (
	ut := p.next()
	u, _ := intervalUnit(ut.text)
	fn, ok := extractFuncs[u]
	if !ok {
		p.fail("The syntax 'EXTRACT(" + strings.ToUpper(ut.text) + " FROM date)' is not supported")
	}
	p.expectKw("FROM")
	x := p.parseExprAlias()
	p.expectOp(")")
	return &FuncCall{Name: fn, Args: []Expr{x}}
}

// parsePosition handles POSITION(needle IN haystack) and position(h, n).
func (p *parser) parsePosition(name string) Expr {
	p.next() // (
	if p.acceptOp(")") {
		return &FuncCall{Name: name}
	}
	first := p.parseOp(precCompare)
	if p.acceptKw("IN") {
		hay := p.parseExprAlias()
		p.expectOp(")")
		return &FuncCall{Name: "position", Args: []Expr{hay, first}}
	}
	first = p.parseOpFrom(first, 0)
	if p.acceptKw("AS") {
		alias, _ := p.parseIdentName()
		first = &AliasExpr{X: first, Alias: alias}
	}
	args := []Expr{first}
	for p.acceptOp(",") {
		args = append(args, p.parseExprAlias())
	}
	p.expectOp(")")
	return &FuncCall{Name: "position", Args: args}
}

// parseSubstring handles SUBSTRING(s FROM a [FOR b]) and substring(s, a, b).
func (p *parser) parseSubstring(name string) Expr {
	p.next() // (
	if p.acceptOp(")") {
		return &FuncCall{Name: name}
	}
	s := p.parseExprAlias()
	if p.acceptKw("FROM") {
		args := []Expr{s, p.parseExprAlias()}
		if p.acceptKw("FOR") {
			args = append(args, p.parseExprAlias())
		}
		p.expectOp(")")
		return &FuncCall{Name: "substring", Args: args}
	}
	args := []Expr{s}
	for p.acceptOp(",") {
		args = append(args, p.parseExprAlias())
	}
	p.expectOp(")")
	return &FuncCall{Name: "substring", Args: args}
}

// parseTrim handles TRIM([BOTH|LEADING|TRAILING] [chars FROM] s), LTRIM and
// RTRIM, producing trimBoth/trimLeft/trimRight like ClickHouse.
func (p *parser) parseTrim(lower string) Expr {
	p.next() // (
	fn := map[string]string{"trim": "trimBoth", "ltrim": "trimLeft", "rtrim": "trimRight"}[lower]
	mode := false
	if lower == "trim" {
		switch {
		case p.acceptKw("BOTH"):
			mode = true
		case p.acceptKw("LEADING"):
			fn, mode = "trimLeft", true
		case p.acceptKw("TRAILING"):
			fn, mode = "trimRight", true
		}
	}
	first := p.parseExprAlias()
	if mode {
		p.expectKw("FROM")
	}
	if lower == "trim" && (mode || p.acceptKw("FROM")) {
		s := p.parseExprAlias()
		p.expectOp(")")
		return &FuncCall{Name: fn, Args: []Expr{s, first}}
	}
	args := []Expr{first}
	for p.acceptOp(",") {
		args = append(args, p.parseExprAlias())
	}
	p.expectOp(")")
	return &FuncCall{Name: fn, Args: args}
}

// parseDateAddSub handles dateAdd(unit, n, d) / dateAdd(d, interval) and the
// sub variants, which ClickHouse rewrites to d +/- INTERVAL n unit.
func (p *parser) parseDateAddSub(name, op string) Expr {
	p.next() // (
	if u, ok := p.unitArg(); ok {
		n := p.parseExprAlias()
		p.expectOp(",")
		d := p.parseExprAlias()
		p.expectOp(")")
		return &BinaryExpr{Op: op, Left: d, Right: &IntervalExpr{X: n, Unit: u}}
	}
	args, _ := p.parseCallArgs()
	if len(args) == 2 {
		return &BinaryExpr{Op: op, Left: args[0], Right: args[1]}
	}
	return &FuncCall{Name: name, Args: args}
}

// parseDateDiff handles dateDiff(unit, a, b[, tz]) with a bare unit keyword.
func (p *parser) parseDateDiff() Expr {
	p.next() // (
	fc := &FuncCall{Name: "dateDiff"}
	if u, ok := p.unitArg(); ok {
		lu := strings.ToLower(u)
		fc.Args = []Expr{&Literal{Value: lu, Raw: "'" + lu + "'"}}
		rest, _ := p.parseCallArgs()
		fc.Args = append(fc.Args, rest...)
		return fc
	}
	fc.Args, _ = p.parseCallArgs()
	return fc
}

// unitArg consumes a bare interval-unit keyword followed by a comma.
func (p *parser) unitArg() (string, bool) {
	t := p.peek()
	if t.kind != tIdent || !isOp(p.peekN(1), ",") {
		return "", false
	}
	u, ok := intervalUnit(t.text)
	if !ok {
		return "", false
	}
	p.next()
	p.next()
	return u, true
}
