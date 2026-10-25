package sqlparse

import (
	"fmt"
	"strings"

	"dynamodb-eventstream/internal/cherr"
)

// maxDepth bounds recursion so hostile input cannot overflow the stack.
const maxDepth = 512

type parser struct {
	src   string
	lx    lexer
	ahead []token // lookahead buffer
	last  token   // most recently consumed token
	depth int
	// dataTaken is set once INSERT ... FORMAT has claimed the rest of the
	// input as raw data.
	dataTaken bool
	// inValues is set while parsing INSERT ... VALUES rows, where the Values
	// format also accepts map literals {'k': v}.
	inValues bool
	// bindIdents enables {name:Identifier} substitution from params
	// (ParseWithParams); plain Parse leaves such parameters unbound.
	bindIdents bool
	params     map[string]string
	// bareUnions are the SELECTs with a UNION written without ALL or
	// DISTINCT (UnionOps ""), resolved from union_default_mode at the end.
	bareUnions []*SelectStmt
	// settings are the request/session settings (ParseRequest); only
	// union_default_mode is consulted, as the fallback for bare UNIONs.
	settings map[string]string
}

func newParser(src string) *parser {
	return &parser{src: src, lx: lexer{src: src}}
}

// bail is the panic payload used to unwind on the first syntax error.
type bail struct{ err *cherr.Error }

func (p *parser) run(f func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			b, ok := r.(bail)
			if !ok {
				panic(r)
			}
			err = b.err
		}
	}()
	f()
	return nil
}

func (p *parser) peekN(n int) token {
	for len(p.ahead) <= n {
		p.ahead = append(p.ahead, p.lexNext())
	}
	return p.ahead[n]
}

func (p *parser) peek() token { return p.peekN(0) }

func (p *parser) next() token {
	t := p.peek()
	if t.kind == tError {
		p.failAt(t, "")
	}
	if t.unbound {
		if !p.bindIdents {
			notImplemented("Query parameter %s cannot be substituted here: Identifier parameters are not supported in this context", t.text)
		}
		panic(bail{cherr.New(cherr.UnknownQueryParameter, "Substitution `%s` is not set", t.identParam)})
	}
	// Shift in place: the buffer holds only a few tokens and reusing its
	// backing array avoids an allocation per token.
	n := copy(p.ahead, p.ahead[1:])
	p.ahead = p.ahead[:n]
	if t.kind != tEOF {
		p.last = t
	}
	return t
}

// lexNext returns the next token from the lexer, turning a query-parameter
// substitution `{name:Identifier}` into one quoted-identifier token holding
// the parameter value, as ClickHouse substitutes it in the parser. The value
// is always a single identifier (a dot in it does not qualify anything). The
// lexer only runs as far as the parser looks, so INSERT ... FORMAT data is
// never substituted.
func (p *parser) lexNext() token {
	t := p.lx.next()
	if !isOp(t, "{") {
		return t
	}
	save := p.lx
	name := p.lx.next()
	colon := p.lx.next()
	typ := p.lx.next()
	closing := p.lx.next()
	if (name.kind != tIdent && name.kind != tQIdent) || !isOp(colon, ":") ||
		typ.kind != tIdent || typ.text != "Identifier" || !isOp(closing, "}") {
		p.lx = save
		return t
	}
	n := name.text
	if name.kind == tQIdent {
		n = name.val
	}
	tok := token{kind: tQIdent, text: p.src[t.pos:closing.end], pos: t.pos, end: closing.end, identParam: n}
	if v, ok := p.params[n]; ok && p.bindIdents {
		tok.val = v
	} else {
		tok.unbound = true
	}
	return tok
}

func (p *parser) enter() {
	p.depth++
	if p.depth > maxDepth {
		p.fail("Maximum parse depth exceeded")
	}
}

func (p *parser) leave() { p.depth-- }

// ---------------------------------------------------------------------------
// Token predicates

func isKw(t token, kw string) bool { return t.kind == tIdent && strings.EqualFold(t.text, kw) }

func isOp(t token, op string) bool { return t.kind == tOp && t.text == op }

func (p *parser) atKw(kws ...string) bool {
	for i, kw := range kws {
		if !isKw(p.peekN(i), kw) {
			return false
		}
	}
	return true
}

func (p *parser) atOp(op string) bool { return isOp(p.peek(), op) }

// acceptKw consumes the keyword sequence if it is next.
func (p *parser) acceptKw(kws ...string) bool {
	if !p.atKw(kws...) {
		return false
	}
	for range kws {
		p.next()
	}
	return true
}

func (p *parser) acceptOp(op string) bool {
	if p.atOp(op) {
		p.next()
		return true
	}
	return false
}

func (p *parser) expectKw(kws ...string) {
	if !p.acceptKw(kws...) {
		p.failExpected(strings.Join(kws, " "))
	}
}

func (p *parser) expectOp(op string) {
	if !p.acceptOp(op) {
		p.failExpected(opName(op))
	}
}

func opName(op string) string {
	switch op {
	case "(":
		return "opening round bracket"
	case ")":
		return "closing round bracket"
	case "[":
		return "opening square bracket"
	case "]":
		return "closing square bracket"
	case ",":
		return "comma"
	case "=":
		return "equals sign"
	}
	return "'" + op + "'"
}

// atEnd reports whether the statement ends here (EOF or ';').
func (p *parser) atEnd() bool {
	t := p.peek()
	return t.kind == tEOF || isOp(t, ";")
}

// ---------------------------------------------------------------------------
// Errors

func (p *parser) fail(msg string) {
	panic(bail{cherr.New(cherr.SyntaxError, "%s", msg)})
}

func (p *parser) failExpected(expected string) { p.failAt(p.peek(), expected) }

// failAt reports a syntax error at token t in ClickHouse's format:
// "Syntax error: failed at position N (tok) (line L, col C): excerpt. Expected X".
func (p *parser) failAt(t token, expected string) {
	var b strings.Builder
	if t.kind == tError {
		b.WriteString(t.val)
		b.WriteString(": ")
		expected = ""
	}
	tok := t.text
	if t.kind == tEOF {
		tok = "end of query"
	}
	tok = truncate(tok, 60)
	fmt.Fprintf(&b, "Syntax error: failed at position %d (%s)", t.pos+1, tok)
	if strings.ContainsRune(p.src, '\n') {
		line := strings.Count(p.src[:t.pos], "\n") + 1
		col := t.pos - strings.LastIndexByte(p.src[:t.pos], '\n')
		fmt.Fprintf(&b, " (line %d, col %d)", line, col)
	}
	b.WriteString(": ")
	b.WriteString(truncate(p.src[t.pos:], 120))
	b.WriteString(".")
	if expected != "" {
		b.WriteString(" Expected ")
		b.WriteString(expected)
	}
	panic(bail{cherr.New(cherr.SyntaxError, "%s", b.String())})
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && cut < len(s) && s[cut]&0xC0 == 0x80 {
		cut--
	}
	return s[:cut] + "..."
}

func notImplemented(format string, args ...any) {
	panic(bail{cherr.New(cherr.NotImplemented, format, args...)})
}

// ---------------------------------------------------------------------------
// Entry points

func parse(sql string) (Statement, error) { return parseStatementWith(newParser(sql)) }

func parseWithParams(sql string, params map[string]string) (Statement, error) {
	p := newParser(sql)
	p.bindIdents, p.params = true, params
	return parseStatementWith(p)
}

func parseRequest(sql string, params, settings map[string]string) (Statement, error) {
	p := newParser(sql)
	p.bindIdents, p.params, p.settings = true, params, settings
	return parseStatementWith(p)
}

func parseStatementWith(p *parser) (Statement, error) {
	var st Statement
	err := p.run(func() {
		st = p.parseStatement()
		if !p.dataTaken {
			p.finish()
		}
		p.resolveBareUnions(st)
	})
	if err != nil {
		return nil, err
	}
	return st, nil
}

// finish accepts optional trailing ';' and rejects anything after it.
func (p *parser) finish() {
	if p.acceptOp(";") {
		for p.acceptOp(";") {
		}
		if t := p.peek(); t.kind != tEOF {
			p.failAt(t, "end of query (Multi-statements are not allowed)")
		}
		return
	}
	if t := p.peek(); t.kind != tEOF {
		p.failAt(t, "end of query")
	}
}

func parseExpr(s string) (Expr, error) {
	p := newParser(s)
	var e Expr
	err := p.run(func() {
		e = p.parseExprAlias()
		if t := p.peek(); t.kind != tEOF {
			p.failAt(t, "end of query")
		}
	})
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (p *parser) parseStatement() Statement {
	t := p.peek()
	switch {
	case t.kind == tEOF:
		p.failAt(t, "Query")
	case isOp(t, "("):
		return p.parseSelectStatement()
	case t.kind != tIdent:
		p.failAt(t, "Query")
	}
	switch strings.ToUpper(t.text) {
	case "SELECT", "WITH":
		return p.parseSelectStatement()
	case "INSERT":
		return p.parseInsert()
	case "CREATE":
		return p.parseCreate()
	case "DROP":
		return p.parseDrop()
	case "TRUNCATE":
		return p.parseTruncate()
	case "SHOW":
		return p.parseShow()
	case "DESCRIBE", "DESC":
		return p.parseDescribe()
	case "EXISTS":
		return p.parseExists()
	case "USE":
		p.next()
		db, _ := p.parseIdentName()
		return &UseStmt{Database: db}
	case "SET":
		p.next()
		if p.atKw("ROLE") || p.atKw("DEFAULT", "ROLE") || p.atKw("TRANSACTION") {
			notImplemented("SET %s is not supported", strings.ToUpper(p.peek().text))
		}
		return &SetStmt{Settings: p.parseSettingList()}
	case "OPTIMIZE":
		return p.parseOptimize()
	case "SYSTEM":
		p.next()
		return &SystemStmt{Command: p.restText()}
	case "ALTER":
		return p.parseAlter()
	case "DELETE":
		return p.parseDelete()
	case "EXPLAIN":
		return p.parseExplain()
	case "ATTACH", "DETACH", "RENAME", "EXCHANGE", "GRANT", "REVOKE", "KILL", "WATCH", "CHECK",
		"BACKUP", "RESTORE", "UNDROP", "UPDATE", "BEGIN", "COMMIT", "ROLLBACK", "MOVE", "REPLACE", "COPY":
		notImplemented("%s queries are not supported", strings.ToUpper(t.text))
	}
	p.failAt(t, "Query")
	return nil
}

// restText consumes all remaining tokens up to the end of the statement and
// returns their source text.
func (p *parser) restText() string {
	if p.atEnd() {
		return ""
	}
	start := p.peek().pos
	for !p.atEnd() {
		p.next()
	}
	return p.src[start:p.last.end]
}

// ---------------------------------------------------------------------------
// Names

// parseIdentName parses one identifier (bare or quoted) and returns its name.
func (p *parser) parseIdentName() (string, token) {
	t := p.peek()
	switch t.kind {
	case tIdent:
		p.next()
		return t.text, t
	case tQIdent:
		p.next()
		return t.val, t
	}
	p.failExpected("identifier")
	return "", t
}

// parseQualifiedName parses [db.]name.
func (p *parser) parseQualifiedName() (db, name string) {
	name, _ = p.parseIdentName()
	if p.atOp(".") {
		p.next()
		db = name
		name, _ = p.parseIdentName()
	}
	return db, name
}

// skipOnCluster skips an optional ON CLUSTER name (accepted and ignored).
func (p *parser) skipOnCluster() {
	if p.acceptKw("ON", "CLUSTER") {
		if p.peek().kind == tString {
			p.next()
			return
		}
		p.parseIdentName()
	}
}

// parseFormatName parses the identifier after FORMAT.
func (p *parser) parseFormatName() string {
	t := p.peek()
	if t.kind != tIdent && t.kind != tQIdent {
		p.failExpected("format name")
	}
	name, _ := p.parseIdentName()
	return name
}

// ---------------------------------------------------------------------------
// Settings

// parseSettingList parses `name = value, ...`.
func (p *parser) parseSettingList() []Setting {
	var out []Setting
	for {
		name, _ := p.parseIdentName()
		p.expectOp("=")
		out = append(out, Setting{Name: name, Value: p.parseExpr()})
		if !p.acceptOp(",") {
			return out
		}
	}
}

// typeText parses a data type at the current position and returns its
// whitespace-normalized text. A type is a name (possibly several words, e.g.
// "Double Precision") with an optional balanced argument list. stop reports
// words that end the type.
func (p *parser) typeText(stop func(token) bool) string {
	var toks []token
	t := p.peek()
	if t.kind != tIdent && t.kind != tQIdent {
		p.failExpected("data type")
	}
	toks = append(toks, p.next())
	for {
		t = p.peek()
		if isOp(t, "(") {
			toks = append(toks, p.balancedTokens()...)
			break
		}
		if t.kind == tIdent && !stop(t) {
			toks = append(toks, p.next())
			continue
		}
		break
	}
	return joinTokens(toks)
}

// balancedTokens consumes a parenthesized group, returning all its tokens
// including the outer brackets.
func (p *parser) balancedTokens() []token {
	var toks []token
	depth := 0
	for {
		t := p.peek()
		switch {
		case t.kind == tEOF:
			p.failExpected("closing round bracket")
		case isOp(t, "(") || isOp(t, "["):
			depth++
		case isOp(t, ")") || isOp(t, "]"):
			depth--
		}
		toks = append(toks, p.next())
		if depth == 0 {
			return toks
		}
	}
}

// joinTokens renders tokens with normalized spacing: one space between
// words, after commas and around '='; none inside brackets.
func joinTokens(toks []token) string {
	var b strings.Builder
	for i, t := range toks {
		if i > 0 {
			prev := toks[i-1]
			if (isWordTok(prev) && isWordTok(t)) || isOp(prev, ",") || isOp(prev, "=") || isOp(t, "=") {
				b.WriteByte(' ')
			}
		}
		b.WriteString(t.text)
	}
	return b.String()
}

func isWordTok(t token) bool {
	switch t.kind {
	case tIdent, tQIdent, tNumber, tString:
		return true
	}
	return false
}
