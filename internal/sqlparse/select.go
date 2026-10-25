package sqlparse

import (
	"strings"

	"dynamodb-eventstream/internal/cherr"
)

// Words that cannot be an implicit alias (`SELECT a b` makes b an alias of a),
// after ClickHouse's ParserAlias restricted keywords.
var aliasReserved = map[string]bool{
	"ALL": true, "AND": true, "ANTI": true, "ANY": true, "ARRAY": true, "AS": true, "ASC": true,
	"ASCENDING": true, "ASOF": true, "BETWEEN": true, "BY": true, "COLLATE": true, "CROSS": true,
	"DESC": true, "DESCENDING": true, "DIV": true, "ELSE": true, "END": true, "EXCEPT": true,
	"FETCH": true, "FINAL": true, "FORMAT": true, "FROM": true, "FULL": true, "GLOBAL": true,
	"GROUP": true, "HAVING": true, "ILIKE": true, "IN": true, "INNER": true, "INTERSECT": true,
	"INTO": true, "IS": true, "JOIN": true, "LEFT": true, "LIKE": true, "LIMIT": true, "LOCAL": true,
	"MOD": true, "NOT": true, "NULLS": true, "OFFSET": true, "ON": true, "ONLY": true, "OR": true,
	"ORDER": true, "OUTER": true, "PASTE": true, "PREWHERE": true, "QUALIFY": true, "REGEXP": true,
	"RIGHT": true, "SAMPLE": true, "SELECT": true, "SEMI": true, "SETTINGS": true, "THEN": true,
	"TOTALS": true, "UNION": true, "USING": true, "VALUES": true, "WHEN": true, "WHERE": true,
	"WINDOW": true, "WITH": true,
}

// implicitAlias consumes an alias written without AS, if present.
func (p *parser) implicitAlias() string {
	t := p.peek()
	if t.kind == tQIdent || (t.kind == tIdent && !aliasReserved[strings.ToUpper(t.text)]) {
		name, _ := p.parseIdentName()
		return name
	}
	return ""
}

// optAlias parses `AS name` or an implicit alias.
func (p *parser) optAlias() string {
	if p.acceptKw("AS") {
		name, _ := p.parseIdentName()
		return name
	}
	return p.implicitAlias()
}

// parseSelectStatement parses a top-level SELECT with FORMAT/SETTINGS tail.
func (p *parser) parseSelectStatement() Statement {
	sel := p.parseSelectUnion()
	if p.atKw("INTO", "OUTFILE") {
		notImplemented("INTO OUTFILE is not supported")
	}
	// SETTINGS may appear before and/or after FORMAT.
	if p.acceptKw("SETTINGS") {
		sel.Settings = append(sel.Settings, p.parseSettingList()...)
	}
	if p.acceptKw("FORMAT") {
		sel.Format = p.parseFormatName()
		if p.acceptKw("SETTINGS") {
			sel.Settings = append(sel.Settings, p.parseSettingList()...)
		}
	}
	return sel
}

// parseSelectUnion parses select elements joined by UNION ALL|DISTINCT.
// Query-level SETTINGS written after the last branch are moved to the head.
func (p *parser) parseSelectUnion() *SelectStmt {
	p.enter()
	defer p.leave()
	head, _ := p.parseSelectElement()
	lastParen := false
	for {
		if p.atKw("UNION") {
			p.next()
			var op string
			switch {
			case p.acceptKw("ALL"):
				op = "ALL"
			case p.acceptKw("DISTINCT"):
				op = "DISTINCT"
			default:
				// Resolved from union_default_mode once the SETTINGS are
				// known (resolveBareUnions).
				if len(p.bareUnions) == 0 || p.bareUnions[len(p.bareUnions)-1] != head {
					p.bareUnions = append(p.bareUnions, head)
				}
			}
			branch, paren := p.parseSelectElement()
			head.Union = append(head.Union, branch)
			head.UnionOps = append(head.UnionOps, op)
			lastParen = paren
			continue
		}
		if p.atKw("EXCEPT") || p.atKw("INTERSECT") {
			notImplemented("%s is not supported", strings.ToUpper(p.peek().text))
		}
		break
	}
	if n := len(head.Union); n > 0 && !lastParen {
		last := head.Union[n-1]
		head.Settings = append(head.Settings, last.Settings...)
		last.Settings = nil
	}
	return head
}

// resolveBareUnions gives every UNION written without ALL or DISTINCT the
// mode of the union_default_mode setting from the SETTINGS clause of its
// own SELECT or of the statement, falling back to the request/session
// settings given to ParseRequest. ClickHouse 25.8's default is the empty string, which
// rejects a bare UNION (EXPECTED_ALL_OR_DISTINCT, 558).
func (p *parser) resolveBareUnions(st Statement) {
	if len(p.bareUnions) == 0 {
		return
	}
	var top []Setting
	if s, ok := st.(*SelectStmt); ok {
		top = s.Settings
	}
	for _, head := range p.bareUnions {
		mode, ok := unionDefaultMode(head.Settings)
		if !ok {
			mode, ok = unionDefaultMode(top)
		}
		if !ok {
			mode, ok = p.settings["union_default_mode"]
		}
		switch {
		case !ok || mode == "":
			panic(bail{cherr.New(cherr.ExpectedAllOrDistinct, "Expected ALL or DISTINCT in SelectWithUnion query, because setting (union_default_mode) is empty")})
		case mode != "ALL" && mode != "DISTINCT":
			panic(bail{cherr.New(557 /* UNKNOWN_UNION */, "Unexpected value of SetOperationMode: '%s'. Must be one of ['DISTINCT', 'ALL', '']", mode)})
		}
		for i, op := range head.UnionOps {
			if op == "" {
				head.UnionOps[i] = mode
			}
		}
	}
}

// unionDefaultMode returns the string value of a union_default_mode setting.
func unionDefaultMode(settings []Setting) (string, bool) {
	for i := len(settings) - 1; i >= 0; i-- {
		if settings[i].Name != "union_default_mode" {
			continue
		}
		if l, ok := settings[i].Value.(*Literal); ok {
			if v, ok := l.Value.(string); ok {
				return v, true
			}
		}
		return "", false
	}
	return "", false
}

func (p *parser) parseSelectElement() (*SelectStmt, bool) {
	if p.atOp("(") {
		p.next()
		s := p.parseSelectUnion()
		p.expectOp(")")
		return s, true
	}
	return p.parseSelectCore(), false
}

func (p *parser) parseSelectCore() *SelectStmt {
	s := &SelectStmt{}
	if p.acceptKw("WITH") {
		if p.atKw("RECURSIVE") {
			notImplemented("WITH RECURSIVE is not supported")
		}
		s.With = p.parseWithList()
	}
	p.expectKw("SELECT")
	if p.atKw("DISTINCT") {
		p.next()
		if p.atKw("ON") {
			notImplemented("DISTINCT ON is not supported")
		}
		s.Distinct = true
	} else if p.atKw("ALL") && !isOp(p.peekN(1), ",") && !isOp(p.peekN(1), "(") && !p.atKw("ALL", "FROM") {
		p.next()
	}
	if p.atKw("TOP") && p.peekN(1).kind == tNumber {
		p.next()
		s.Limit = p.parsePrimary()
		if p.atKw("WITH", "TIES") {
			notImplemented("TOP ... WITH TIES is not supported")
		}
	}
	s.Columns = p.parseSelectItems()
	if p.acceptKw("FROM") {
		s.From = p.parseTableExpr()
		p.parseJoins(s)
	}
	if p.acceptKw("PREWHERE") {
		s.Prewhere = p.parseExpr()
	}
	if p.acceptKw("WHERE") {
		s.Where = p.parseExpr()
	}
	if p.acceptKw("GROUP", "BY") {
		p.parseGroupBy(s)
	}
	if p.atKw("WITH", "ROLLUP") || p.atKw("WITH", "CUBE") {
		notImplemented("WITH %s is not supported", strings.ToUpper(p.peekN(1).text))
	}
	if p.acceptKw("WITH", "TOTALS") {
		s.WithTotals = true
	}
	if p.acceptKw("HAVING") {
		s.Having = p.parseExpr()
	}
	if p.atKw("WINDOW") || p.atKw("QUALIFY") {
		notImplemented("%s clause is not supported", strings.ToUpper(p.peek().text))
	}
	if p.acceptKw("ORDER", "BY") {
		if p.atKw("ALL") && (p.atEndOfClause(1) || isKw(p.peekN(1), "ASC") || isKw(p.peekN(1), "DESC")) {
			notImplemented("ORDER BY ALL is not supported")
		}
		s.OrderBy = p.parseOrderList()
	}
	p.parseLimits(s)
	if p.acceptKw("SETTINGS") {
		s.Settings = p.parseSettingList()
	}
	return s
}

// atEndOfClause reports whether the token at offset i ends a clause.
func (p *parser) atEndOfClause(i int) bool {
	t := p.peekN(i)
	if t.kind == tEOF || isOp(t, ";") || isOp(t, ")") {
		return true
	}
	return t.kind == tIdent && aliasReserved[strings.ToUpper(t.text)]
}

func (p *parser) parseWithList() []WithItem {
	var out []WithItem
	for {
		t := p.peek()
		if (t.kind == tIdent || t.kind == tQIdent) && isKw(p.peekN(1), "AS") && isOp(p.peekN(2), "(") && p.looksLikeSelect(3) {
			name, _ := p.parseIdentName()
			p.next() // AS
			p.next() // (
			sub := p.parseSelectUnion()
			p.expectOp(")")
			out = append(out, WithItem{Name: name, Subquery: sub})
		} else {
			e := p.parseExpr()
			p.expectKw("AS")
			name, _ := p.parseIdentName()
			out = append(out, WithItem{Name: name, Expr: e})
		}
		if !p.acceptOp(",") {
			return out
		}
	}
}

func (p *parser) parseSelectItems() []SelectItem {
	var out []SelectItem
	for {
		item := SelectItem{Expr: p.parseExpr()}
		item.Alias = p.optAlias()
		out = append(out, item)
		if !p.acceptOp(",") {
			return out
		}
		// ClickHouse allows a trailing comma: SELECT a, b, FROM t.
		if p.atEnd() || p.atKw("FROM") || isOp(p.peek(), ")") {
			return out
		}
	}
}

func (p *parser) parseTableExpr() *TableExpr {
	te := &TableExpr{}
	switch {
	case p.atOp("("):
		if !p.looksLikeSelect(1) {
			p.failAt(p.peekN(1), "SELECT subquery")
		}
		p.next()
		te.Subquery = p.parseSelectUnion()
		p.expectOp(")")
	default:
		name, _ := p.parseIdentName()
		if p.atOp("(") {
			fc, ok := p.parseCall(name).(*FuncCall)
			if !ok {
				p.fail("Unsupported table function " + name)
			}
			te.Func = fc
			break
		}
		te.Table = name
		if p.acceptOp(".") {
			te.Database = name
			te.Table, _ = p.parseIdentName()
			if p.atOp("(") {
				notImplemented("Qualified table functions are not supported")
			}
		}
	}
	te.Alias = p.optAlias()
	if p.acceptKw("FINAL") {
		te.Final = true
	}
	if p.atKw("SAMPLE") {
		notImplemented("SAMPLE is not supported")
	}
	return te
}

var joinWords = map[string]bool{
	"GLOBAL": true, "LOCAL": true, "ANY": true, "ALL": true, "ASOF": true, "SEMI": true, "ANTI": true,
	"INNER": true, "LEFT": true, "RIGHT": true, "FULL": true, "CROSS": true, "OUTER": true, "PASTE": true,
}

func (p *parser) parseJoins(s *SelectStmt) {
	for {
		if p.acceptOp(",") {
			s.Joins = append(s.Joins, JoinClause{Kind: "CROSS", Table: p.parseTableExpr()})
			continue
		}
		if p.atKw("ARRAY", "JOIN") || p.atKw("LEFT", "ARRAY", "JOIN") || p.atKw("INNER", "ARRAY", "JOIN") {
			if s.ArrayJoin != nil {
				notImplemented("Multiple ARRAY JOIN clauses are not supported")
			}
			aj := &ArrayJoinClause{Left: p.atKw("LEFT")}
			for !p.acceptKw("JOIN") {
				p.next()
			}
			for {
				item := SelectItem{Expr: p.parseExpr()}
				item.Alias = p.optAlias()
				aj.Items = append(aj.Items, item)
				if !p.acceptOp(",") {
					break
				}
			}
			s.ArrayJoin = aj
			continue
		}
		i := 0
		for t := p.peekN(i); t.kind == tIdent && joinWords[strings.ToUpper(t.text)]; t = p.peekN(i) {
			i++
		}
		if !isKw(p.peekN(i), "JOIN") {
			return
		}
		j := JoinClause{}
		for k := 0; k <= i; k++ {
			switch w := strings.ToUpper(p.next().text); w {
			case "ANY", "ALL", "ASOF", "SEMI", "ANTI":
				j.Strictness = w
			case "INNER", "LEFT", "RIGHT", "FULL", "CROSS":
				j.Kind = w
			case "PASTE":
				notImplemented("PASTE JOIN is not supported")
			}
		}
		if j.Kind == "" {
			j.Kind = "INNER"
		}
		j.Table = p.parseTableExpr()
		switch {
		case p.acceptKw("ON"):
			j.On = p.parseExpr()
		case p.acceptKw("USING"):
			paren := p.acceptOp("(")
			for {
				name, _ := p.parseIdentName()
				j.Using = append(j.Using, name)
				if !p.acceptOp(",") {
					break
				}
			}
			if paren {
				p.expectOp(")")
			}
		case j.Kind != "CROSS":
			p.failExpected("ON or USING")
		}
		s.Joins = append(s.Joins, j)
	}
}

func (p *parser) parseGroupBy(s *SelectStmt) {
	if p.atKw("ALL") && p.atEndOfClause(1) {
		p.next()
		s.GroupByAll = true
		return
	}
	if p.atKw("GROUPING", "SETS") || ((p.atKw("ROLLUP") || p.atKw("CUBE")) && isOp(p.peekN(1), "(")) {
		notImplemented("GROUP BY %s is not supported", strings.ToUpper(p.peek().text))
	}
	for {
		s.GroupBy = append(s.GroupBy, p.parseExprAlias())
		if !p.acceptOp(",") {
			return
		}
	}
}

func (p *parser) parseOrderList() []OrderItem {
	var out []OrderItem
	for {
		it := OrderItem{Expr: p.parseExprAlias()}
		switch {
		case p.acceptKw("DESC"), p.acceptKw("DESCENDING"):
			it.Desc = true
		case p.acceptKw("ASC"), p.acceptKw("ASCENDING"):
		}
		if p.acceptKw("NULLS") {
			first := false
			switch {
			case p.acceptKw("FIRST"):
				first = true
			case p.acceptKw("LAST"):
			default:
				p.failExpected("FIRST or LAST")
			}
			it.NullsFirst = &first
		}
		if p.acceptKw("COLLATE") {
			t := p.peek()
			if t.kind != tString {
				p.failExpected("string literal")
			}
			p.next()
			it.Collate = t.val
		}
		if p.atKw("WITH", "FILL") {
			notImplemented("ORDER BY ... WITH FILL is not supported")
		}
		out = append(out, it)
		if !p.acceptOp(",") {
			return out
		}
	}
}

// parseLimits parses LIMIT n BY ..., LIMIT [m,] n [OFFSET m] and OFFSET m.
func (p *parser) parseLimits(s *SelectStmt) {
	for p.atKw("LIMIT") {
		kw := p.next()
		n := p.parseExpr()
		var off Expr
		if p.acceptOp(",") {
			off, n = n, p.parseExpr()
		} else if p.acceptKw("OFFSET") {
			off = p.parseExpr()
		}
		if p.acceptKw("BY") {
			if s.LimitBy != nil || s.Limit != nil {
				p.failAt(kw, "")
			}
			lb := &LimitByClause{Limit: n, Offset: off}
			for {
				lb.By = append(lb.By, p.parseExprAlias())
				if !p.acceptOp(",") {
					break
				}
			}
			s.LimitBy = lb
			continue
		}
		if p.atKw("WITH", "TIES") {
			notImplemented("LIMIT ... WITH TIES is not supported")
		}
		if s.Limit != nil {
			p.failAt(kw, "")
		}
		s.Limit, s.Offset = n, off
	}
	if s.Offset == nil && p.acceptKw("OFFSET") {
		s.Offset = p.parseExpr()
		if !p.acceptKw("ROWS") {
			p.acceptKw("ROW")
		}
		if p.atKw("FETCH") {
			notImplemented("FETCH is not supported")
		}
	}
}
