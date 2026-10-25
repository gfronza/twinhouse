package sqlparse

import "strings"

// ---------------------------------------------------------------------------
// INSERT

func (p *parser) parseInsert() Statement {
	p.next() // INSERT
	p.expectKw("INTO")
	if p.atKw("FUNCTION") {
		notImplemented("INSERT INTO FUNCTION is not supported")
	}
	p.acceptKw("TABLE")
	ins := &InsertStmt{}
	ins.Database, ins.Table = p.parseQualifiedName()
	if p.atOp("(") && !p.looksLikeSelect(1) {
		p.next()
		ins.Columns = []string{}
		if p.atOp("*") {
			notImplemented("INSERT column list with * is not supported")
		}
		for {
			parts := []string{}
			for {
				name, _ := p.parseIdentName()
				parts = append(parts, name)
				if !p.acceptOp(".") {
					break
				}
			}
			ins.Columns = append(ins.Columns, strings.Join(parts, "."))
			if !p.acceptOp(",") {
				break
			}
		}
		p.expectOp(")")
	}
	if p.acceptKw("SETTINGS") {
		ins.Settings = p.parseSettingList()
	}
	switch {
	case p.atKw("VALUES"):
		p.next()
		ins.Format = "Values"
		ins.Values = p.parseValuesRows()
	case p.atKw("FORMAT"):
		p.next()
		ins.Format = p.parseFormatName()
		ins.Data = p.takeData()
	case p.atKw("SELECT") || p.atKw("WITH") || (p.atOp("(") && p.looksLikeSelect(1)):
		ins.Select = p.parseSelectUnion()
	default:
		p.failExpected("one of: VALUES, FORMAT, SELECT, WITH")
	}
	return ins
}

func (p *parser) parseValuesRows() [][]Expr {
	p.inValues = true
	defer func() { p.inValues = false }()
	var rows [][]Expr
	for !p.atEnd() {
		p.expectOp("(")
		var row []Expr
		if !p.acceptOp(")") {
			for {
				row = append(row, p.parseExpr())
				if !p.acceptOp(",") {
					break
				}
			}
			p.expectOp(")")
		}
		rows = append(rows, row)
		p.acceptOp(",")
	}
	return rows
}

// takeData returns the raw text after the FORMAT name: leading spaces/tabs
// and at most one line break are dropped; the rest is untouched. The parser
// stops here, so the data is never tokenized.
func (p *parser) takeData() string {
	if len(p.ahead) != 0 {
		// Unreachable: the format name is the last token read.
		p.fail("internal error: data already tokenized")
	}
	d := p.src[p.last.end:]
	i := 0
	for i < len(d) && (d[i] == ' ' || d[i] == '\t' || d[i] == '\f') {
		i++
	}
	if i < len(d) && d[i] == '\r' {
		i++
	}
	if i < len(d) && d[i] == '\n' {
		i++
	}
	d = d[i:]
	if strings.HasPrefix(d, ";") {
		if strings.TrimSpace(d[1:]) != "" {
			p.failAt(token{kind: tOp, text: ";", pos: p.last.end + i, end: p.last.end + i + 1},
				"data (You have excessive ';' symbol before data for INSERT)")
		}
		d = ""
	}
	p.dataTaken = true
	return d
}

// ---------------------------------------------------------------------------
// CREATE

func (p *parser) parseCreate() Statement {
	start := p.next().pos // CREATE
	orReplace := p.acceptKw("OR", "REPLACE")
	switch {
	case p.acceptKw("DATABASE"):
		return p.parseCreateDatabase()
	case p.acceptKw("TABLE"):
		return p.parseCreateTable(start, orReplace)
	case p.acceptKw("MATERIALIZED", "VIEW"):
		return p.parseCreateView(start, orReplace, true)
	case p.acceptKw("VIEW"):
		return p.parseCreateView(start, orReplace, false)
	}
	t := p.peek()
	if t.kind == tIdent {
		notImplemented("CREATE %s is not supported", strings.ToUpper(t.text))
	}
	p.failExpected("one of: DATABASE, TABLE, VIEW, MATERIALIZED VIEW")
	return nil
}

func (p *parser) parseIfNotExists() bool { return p.acceptKw("IF", "NOT", "EXISTS") }

func (p *parser) parseCreateDatabase() Statement {
	st := &CreateDatabaseStmt{IfNotExists: p.parseIfNotExists()}
	st.Name, _ = p.parseIdentName()
	p.skipOnCluster()
	if p.acceptKw("ENGINE") {
		p.acceptOp("=")
		toks := []token{p.next()}
		if p.atOp("(") {
			toks = append(toks, p.balancedTokens()...)
		}
		st.Engine = joinTokens(toks)
	}
	if p.acceptKw("COMMENT") {
		p.expectString()
	}
	return st
}

func (p *parser) expectString() string {
	t := p.peek()
	if t.kind != tString {
		p.failExpected("string literal")
	}
	p.next()
	return t.val
}

func (p *parser) parseCreateTable(start int, orReplace bool) Statement {
	ct := &CreateTableStmt{OrReplace: orReplace, IfNotExists: p.parseIfNotExists()}
	ct.Database, ct.Name = p.parseQualifiedName()
	if p.acceptKw("UUID") {
		p.expectString()
	}
	p.skipOnCluster()
	if p.atOp("(") && !p.looksLikeSelect(1) {
		ct.Columns = p.parseColumnList(&ct.PrimaryKey)
	}
	p.parseCreateAs(ct)
	if ct.AsSelect == nil {
		p.parseTableClauses(ct)
		p.parseCreateAs(ct)
	}
	ct.Text = p.src[start:p.last.end]
	return ct
}

// parseCreateAs handles `AS [db.]table` and `AS SELECT ...`.
func (p *parser) parseCreateAs(ct *CreateTableStmt) {
	if p.atKw("EMPTY", "AS") {
		notImplemented("CREATE TABLE ... EMPTY AS is not supported")
	}
	if !p.atKw("AS") {
		return
	}
	p.next()
	if p.looksLikeSelect(0) {
		ct.AsSelect = p.parseSelectUnion()
		return
	}
	if ct.AsTable != "" {
		p.failExpected("SELECT")
	}
	name, _ := p.parseIdentName()
	if p.atOp("(") {
		notImplemented("CREATE TABLE ... AS table function is not supported")
	}
	ct.AsTable = name
	if p.acceptOp(".") {
		ct.AsDatabase = name
		ct.AsTable, _ = p.parseIdentName()
	}
}

// parseTableClauses parses ENGINE, ORDER BY, PARTITION BY, PRIMARY KEY,
// SAMPLE BY, TTL, SETTINGS and COMMENT in any order.
func (p *parser) parseTableClauses(ct *CreateTableStmt) {
	for {
		switch {
		case p.acceptKw("ENGINE"):
			p.acceptOp("=")
			name, _ := p.parseIdentName()
			ct.Engine = &EngineClause{Name: name}
			if p.acceptOp("(") {
				ct.Engine.Args = p.parseExprList(")")
				if len(ct.Engine.Args) == 0 {
					ct.Engine.Args = nil
				}
			}
		case p.acceptKw("ORDER", "BY"):
			ct.OrderBy = p.parseKeyExprs()
		case p.acceptKw("PARTITION", "BY"):
			ct.PartitionBy = p.parseExpr()
		case p.acceptKw("PRIMARY", "KEY"):
			ct.PrimaryKey = p.parseKeyExprs()
		case p.acceptKw("SAMPLE", "BY"):
			ct.SampleBy = p.parseExpr()
		case p.acceptKw("TTL"):
			ct.TTL = p.rawUntil(tableClauseStart)
		case p.acceptKw("SETTINGS"):
			ct.Settings = append(ct.Settings, p.parseSettingList()...)
		case p.acceptKw("COMMENT"):
			ct.Comment = p.expectString()
		default:
			return
		}
	}
}

func tableClauseStart(p *parser) bool {
	t := p.peek()
	if t.kind != tIdent {
		return false
	}
	switch strings.ToUpper(t.text) {
	case "ENGINE", "SETTINGS", "COMMENT", "AS", "POPULATE":
		return true
	case "ORDER", "PARTITION", "SAMPLE":
		return isKw(p.peekN(1), "BY")
	case "PRIMARY":
		return isKw(p.peekN(1), "KEY")
	}
	return false
}

// rawUntil consumes tokens (balancing brackets) until stop or the end of the
// statement and returns their source text.
func (p *parser) rawUntil(stop func(*parser) bool) string {
	if p.atEnd() || stop(p) {
		p.failExpected("expression")
	}
	start := p.peek().pos
	for !p.atEnd() && !stop(p) {
		if p.atOp("(") || p.atOp("[") {
			p.balancedTokens()
			continue
		}
		p.next()
	}
	return p.src[start:p.last.end]
}

// parseKeyExprs parses a sorting/primary key: `col`, `(a, b)` or `tuple()`.
func (p *parser) parseKeyExprs() []Expr {
	e := p.parseExpr()
	if t, ok := e.(*TupleExpr); ok {
		return append([]Expr{}, t.Elems...)
	}
	return []Expr{e}
}

// parseColumnList parses `(col defs, INDEX ..., PRIMARY KEY (...), ...)`.
func (p *parser) parseColumnList(pk *[]Expr) []ColumnDef {
	p.expectOp("(")
	var cols []ColumnDef
	for {
		switch {
		case p.atKw("INDEX") || p.atKw("PROJECTION"):
			// Data-skipping indexes and projections only affect performance.
			p.skipListEntry()
		case p.atKw("CONSTRAINT"):
			notImplemented("Table constraints are not supported")
		case p.atKw("PRIMARY", "KEY"):
			p.next()
			p.next()
			*pk = append(*pk, p.parseKeyExprs()...)
		default:
			cols = append(cols, p.parseColumnDef(pk))
		}
		if !p.acceptOp(",") {
			break
		}
	}
	p.expectOp(")")
	return cols
}

func (p *parser) skipListEntry() {
	for !p.atOp(",") && !p.atOp(")") {
		if p.peek().kind == tEOF {
			p.failExpected("closing round bracket")
		}
		if p.atOp("(") || p.atOp("[") {
			p.balancedTokens()
			continue
		}
		p.next()
	}
}

var columnModifiers = map[string]bool{
	"DEFAULT": true, "MATERIALIZED": true, "ALIAS": true, "EPHEMERAL": true, "COMMENT": true,
	"CODEC": true, "TTL": true, "NULL": true, "NOT": true, "PRIMARY": true, "STATISTICS": true,
	"SETTINGS": true, "AUTO_INCREMENT": true, "AFTER": true, "FIRST": true,
}

func isColumnModifier(t token) bool {
	return t.kind == tIdent && columnModifiers[strings.ToUpper(t.text)]
}

func (p *parser) parseColumnDef(pk *[]Expr) ColumnDef {
	name, _ := p.parseIdentName()
	col := ColumnDef{Name: name}
	if t := p.peek(); (t.kind == tIdent || t.kind == tQIdent) && !isColumnModifier(t) {
		col.Type = p.typeText(isColumnModifier)
	}
	if p.acceptKw("NULL") {
		if col.Type == "" {
			p.failExpected("data type")
		}
		col.Type = "Nullable(" + col.Type + ")"
	} else {
		p.acceptKw("NOT", "NULL")
	}
	for {
		t := p.peek()
		switch u := strings.ToUpper(t.text); {
		case t.kind != tIdent:
			return col
		case u == "DEFAULT" || u == "MATERIALIZED" || u == "ALIAS" || u == "EPHEMERAL":
			p.next()
			col.DefaultKind = u
			if u == "EPHEMERAL" && (p.atOp(",") || p.atOp(")") || isColumnModifier(p.peek()) || p.atEnd()) {
				continue
			}
			start := p.peek().pos
			col.Default = p.parseExpr()
			col.DefaultText = p.src[start:p.last.end]
		case u == "COMMENT":
			p.next()
			col.Comment = p.expectString()
		case u == "CODEC":
			p.next()
			if !p.atOp("(") {
				p.failExpected("opening round bracket")
			}
			toks := p.balancedTokens()
			col.Codec = joinTokens(toks[1 : len(toks)-1])
		case u == "STATISTICS" || u == "SETTINGS":
			p.next()
			if p.atOp("(") {
				p.balancedTokens()
			}
		case u == "TTL":
			p.next()
			start := p.peek().pos
			p.parseExpr()
			col.TTL = p.src[start:p.last.end]
		case u == "PRIMARY" && isKw(p.peekN(1), "KEY"):
			p.next()
			p.next()
			*pk = append(*pk, &Ident{Parts: []string{name}})
		case u == "AUTO_INCREMENT":
			notImplemented("AUTO_INCREMENT is not supported")
		default:
			return col
		}
	}
}

func (p *parser) parseCreateView(start int, orReplace, materialized bool) Statement {
	cv := &CreateViewStmt{OrReplace: orReplace, Materialized: materialized, IfNotExists: p.parseIfNotExists()}
	cv.Database, cv.Name = p.parseQualifiedName()
	p.skipOnCluster()
	if p.atKw("REFRESH") {
		notImplemented("Refreshable materialized views are not supported")
	}
	if materialized && p.acceptKw("TO") {
		cv.ToDatabase, cv.ToTable = p.parseQualifiedName()
	}
	var pk []Expr
	if p.atOp("(") && !p.looksLikeSelect(1) {
		cv.Columns = p.parseColumnList(&pk)
	}
	inner := &CreateTableStmt{}
	p.parseTableClauses(inner)
	if inner.Engine != nil || inner.OrderBy != nil {
		if cv.ToTable != "" {
			p.fail("Cannot use ENGINE or ORDER BY together with TO in a materialized view")
		}
		if !materialized {
			p.fail("A plain VIEW cannot have an ENGINE")
		}
	}
	if len(inner.Settings) > 0 {
		notImplemented("SETTINGS on a materialized view's inner table are not supported")
	}
	cv.Engine, cv.OrderBy = inner.Engine, inner.OrderBy
	for {
		switch {
		case p.acceptKw("POPULATE"):
			cv.Populate = true
			continue
		case p.acceptKw("DEFINER"):
			p.expectOp("=")
			p.parseIdentName()
			continue
		case p.acceptKw("SQL", "SECURITY"):
			p.parseIdentName()
			continue
		}
		break
	}
	p.expectKw("AS")
	selStart := p.peek().pos
	cv.Select = p.parseSelectUnion()
	cv.SelectText = p.src[selStart:p.last.end]
	cv.Text = p.src[start:p.last.end]
	return cv
}

// ---------------------------------------------------------------------------
// DROP / TRUNCATE / USE / OPTIMIZE / SHOW / DESCRIBE / EXISTS

func (p *parser) parseDrop() Statement {
	p.next() // DROP
	if p.atKw("TEMPORARY") {
		p.next()
	}
	st := &DropStmt{}
	switch {
	case p.acceptKw("TABLE"):
		st.Kind = "TABLE"
	case p.acceptKw("VIEW"):
		st.Kind = "VIEW"
	case p.acceptKw("DATABASE"):
		st.Kind = "DATABASE"
	case p.acceptKw("DICTIONARY"):
		st.Kind = "DICTIONARY"
	default:
		if t := p.peek(); t.kind == tIdent {
			notImplemented("DROP %s is not supported", strings.ToUpper(t.text))
		}
		p.failExpected("one of: TABLE, VIEW, DATABASE, DICTIONARY")
	}
	st.IfExists = p.acceptKw("IF", "EXISTS")
	if p.atKw("IF", "EMPTY") {
		notImplemented("DROP ... IF EMPTY is not supported")
	}
	if st.Kind == "DATABASE" {
		st.Name, _ = p.parseIdentName()
	} else {
		st.Database, st.Name = p.parseQualifiedName()
		if p.atOp(",") {
			notImplemented("Dropping several tables in one query is not supported")
		}
	}
	p.skipOnCluster()
	switch {
	case p.acceptKw("SYNC"):
		st.Sync = true
	case p.acceptKw("NO", "DELAY"):
		st.Sync = true
	case p.acceptKw("PERMANENTLY"):
	}
	return st
}

func (p *parser) parseTruncate() Statement {
	p.next() // TRUNCATE
	p.acceptKw("TEMPORARY")
	if p.atKw("DATABASE") || p.atKw("ALL") {
		notImplemented("TRUNCATE %s is not supported", strings.ToUpper(p.peek().text))
	}
	p.acceptKw("TABLE")
	st := &TruncateStmt{IfExists: p.acceptKw("IF", "EXISTS")}
	st.Database, st.Name = p.parseQualifiedName()
	p.skipOnCluster()
	p.acceptKw("SYNC")
	return st
}

func (p *parser) parseOptimize() Statement {
	p.next() // OPTIMIZE
	p.expectKw("TABLE")
	st := &OptimizeStmt{}
	st.Database, st.Name = p.parseQualifiedName()
	for !p.atEnd() {
		if p.atKw("FINAL") {
			st.Final = true
		}
		if p.atOp("(") || p.atOp("[") {
			p.balancedTokens()
			continue
		}
		p.next()
	}
	return st
}

// parseOutputTail parses an optional FORMAT clause (and ignores SETTINGS).
func (p *parser) parseOutputTail() string {
	if p.atKw("INTO", "OUTFILE") {
		notImplemented("INTO OUTFILE is not supported")
	}
	f := ""
	if p.acceptKw("FORMAT") {
		f = p.parseFormatName()
	}
	if p.acceptKw("SETTINGS") {
		p.parseSettingList()
	}
	return f
}

func (p *parser) parseShow() Statement {
	p.next() // SHOW
	st := &ShowStmt{}
	switch {
	case p.acceptKw("DATABASES"):
		st.Kind = "DATABASES"
		p.parseShowLike(st)
	case p.acceptKw("TABLES"):
		st.Kind = "TABLES"
		if p.acceptKw("FROM") || p.acceptKw("IN") {
			st.Database, _ = p.parseIdentName()
		}
		p.parseShowLike(st)
	case p.acceptKw("CREATE"):
		p.acceptKw("TEMPORARY")
		st.Kind = "CREATE TABLE"
		switch {
		case p.acceptKw("TABLE"):
		case p.acceptKw("VIEW"):
			st.Kind = "CREATE VIEW"
		case p.acceptKw("DATABASE"):
			st.Kind = "CREATE DATABASE"
			st.Name, _ = p.parseIdentName()
			st.Format = p.parseOutputTail()
			return st
		case p.atKw("DICTIONARY") || p.atKw("USER") || p.atKw("ROLE") || p.atKw("QUOTA") || p.atKw("POLICY") ||
			p.atKw("ROW") || p.atKw("PROFILE") || p.atKw("SETTINGS"):
			notImplemented("SHOW CREATE %s is not supported", strings.ToUpper(p.peek().text))
		}
		st.Database, st.Name = p.parseQualifiedName()
	case p.acceptKw("COLUMNS"), p.acceptKw("FIELDS"):
		st.Kind = "COLUMNS"
		if !p.acceptKw("FROM") && !p.acceptKw("IN") {
			p.failExpected("FROM or IN")
		}
		st.Database, st.Name = p.parseQualifiedName()
		if p.acceptKw("FROM") || p.acceptKw("IN") {
			st.Database, _ = p.parseIdentName()
		}
		p.parseShowLike(st)
	case p.acceptKw("PROCESSLIST"):
		st.Kind = "PROCESSLIST"
	default:
		if t := p.peek(); t.kind == tIdent {
			notImplemented("SHOW %s is not supported", strings.ToUpper(t.text))
		}
		p.failExpected("one of: DATABASES, TABLES, CREATE, COLUMNS, PROCESSLIST")
	}
	st.Format = p.parseOutputTail()
	return st
}

func (p *parser) parseShowLike(st *ShowStmt) {
	not := p.acceptKw("NOT")
	switch {
	case p.acceptKw("LIKE"):
		st.Like, st.NotLike = p.expectString(), not
	case p.atKw("ILIKE"):
		notImplemented("SHOW ... ILIKE is not supported")
	case not:
		p.failExpected("LIKE")
	}
	if p.atKw("LIMIT") {
		notImplemented("SHOW ... LIMIT is not supported")
	}
}

func (p *parser) parseDescribe() Statement {
	p.next() // DESCRIBE | DESC
	p.acceptKw("TABLE")
	if p.atOp("(") {
		notImplemented("DESCRIBE of a subquery is not supported")
	}
	st := &DescribeStmt{}
	st.Database, st.Name = p.parseQualifiedName()
	if p.atOp("(") {
		notImplemented("DESCRIBE of a table function is not supported")
	}
	st.Format = p.parseOutputTail()
	return st
}

func (p *parser) parseExists() Statement {
	p.next() // EXISTS
	p.acceptKw("TEMPORARY")
	st := &ExistsStmt{Kind: "TABLE"}
	switch {
	case p.acceptKw("TABLE"):
	case p.acceptKw("DATABASE"):
		st.Kind = "DATABASE"
	case p.acceptKw("VIEW"):
		st.Kind = "VIEW"
	case p.acceptKw("DICTIONARY"):
		st.Kind = "DICTIONARY"
	}
	if st.Kind == "DATABASE" {
		st.Name, _ = p.parseIdentName()
	} else {
		st.Database, st.Name = p.parseQualifiedName()
	}
	st.Format = p.parseOutputTail()
	return st
}

// ---------------------------------------------------------------------------
// ALTER / DELETE / EXPLAIN

func (p *parser) parseAlter() Statement {
	p.next() // ALTER
	if !p.acceptKw("TABLE") {
		if t := p.peek(); t.kind == tIdent {
			notImplemented("ALTER %s is not supported", strings.ToUpper(t.text))
		}
		p.failExpected("TABLE")
	}
	st := &AlterStmt{}
	st.Database, st.Name = p.parseQualifiedName()
	p.skipOnCluster()
	for {
		st.Commands = append(st.Commands, p.parseAlterCommand())
		if !p.acceptOp(",") {
			break
		}
	}
	if p.acceptKw("SETTINGS") {
		p.parseSettingList()
	}
	return st
}

func (p *parser) parseAlterCommand() AlterCommand {
	start := p.peek().pos
	var c AlterCommand
	var pk []Expr
	switch {
	case p.atKw("ADD", "COLUMN"):
		p.next()
		p.next()
		c.Kind = "ADD COLUMN"
		c.IfNotExists = p.parseIfNotExists()
		col := p.parseColumnDef(&pk)
		c.Column, c.ColumnName = &col, col.Name
		p.parseColumnPosition(&c)
	case p.atKw("DROP", "COLUMN"):
		p.next()
		p.next()
		c.Kind = "DROP COLUMN"
		c.IfExists = p.acceptKw("IF", "EXISTS")
		c.ColumnName, _ = p.parseIdentName()
	case p.atKw("MODIFY", "COLUMN") && !p.modifyColumnIsOther():
		p.next()
		p.next()
		c.Kind = "MODIFY COLUMN"
		c.IfExists = p.acceptKw("IF", "EXISTS")
		col := p.parseColumnDef(&pk)
		c.Column, c.ColumnName = &col, col.Name
		p.parseColumnPosition(&c)
	case p.atKw("COMMENT", "COLUMN"):
		p.next()
		p.next()
		c.Kind = "COMMENT COLUMN"
		c.IfExists = p.acceptKw("IF", "EXISTS")
		c.ColumnName, _ = p.parseIdentName()
		c.Column = &ColumnDef{Name: c.ColumnName, Comment: p.expectString()}
	case p.atKw("DELETE", "WHERE"):
		p.next()
		p.next()
		c.Kind = "DELETE"
		c.Where = p.parseExpr()
	case p.atKw("UPDATE"):
		p.next()
		c.Kind = "UPDATE"
		for {
			name, _ := p.parseIdentName()
			p.expectOp("=")
			c.Assignments = append(c.Assignments, Setting{Name: name, Value: p.parseExpr()})
			if !p.acceptOp(",") {
				break
			}
		}
		p.expectKw("WHERE")
		c.Where = p.parseExpr()
	case p.atKw("MODIFY", "SETTING"):
		p.next()
		p.next()
		c.Kind = "MODIFY SETTING"
		for {
			name, _ := p.parseIdentName()
			p.expectOp("=")
			c.Assignments = append(c.Assignments, Setting{Name: name, Value: p.parseExpr()})
			// A comma continues the setting list only if `name =` follows.
			if !p.atOp(",") || !isOp(p.peekN(2), "=") {
				break
			}
			p.next()
		}
	default:
		if p.atEnd() || p.atOp(",") {
			p.failExpected("ALTER command")
		}
		c.Kind = "OTHER"
		p.skipListEntryUntilEnd()
	}
	c.Raw = p.src[start:p.last.end]
	return c
}

// modifyColumnIsOther reports MODIFY COLUMN forms that are not a column
// definition (e.g. MODIFY COLUMN x REMOVE DEFAULT).
func (p *parser) modifyColumnIsOther() bool {
	i := 2
	if isKw(p.peekN(i), "IF") {
		i += 2
	}
	return isKw(p.peekN(i+1), "REMOVE") || isKw(p.peekN(i+1), "MODIFY")
}

func (p *parser) parseColumnPosition(c *AlterCommand) {
	if p.acceptKw("FIRST") {
		c.First = true
	} else if p.acceptKw("AFTER") {
		c.After, _ = p.parseIdentName()
	}
}

func (p *parser) skipListEntryUntilEnd() {
	for !p.atEnd() && !p.atOp(",") {
		if p.atOp("(") || p.atOp("[") {
			p.balancedTokens()
			continue
		}
		p.next()
	}
}

func (p *parser) parseDelete() Statement {
	p.next() // DELETE
	p.expectKw("FROM")
	st := &DeleteStmt{}
	st.Database, st.Name = p.parseQualifiedName()
	p.skipOnCluster()
	if p.atKw("IN", "PARTITION") {
		notImplemented("DELETE ... IN PARTITION is not supported")
	}
	p.expectKw("WHERE")
	st.Where = p.parseExpr()
	return st
}

func (p *parser) parseExplain() Statement {
	p.next() // EXPLAIN
	st := &ExplainStmt{}
	switch {
	case p.acceptKw("QUERY", "TREE"):
		st.Kind = "QUERY TREE"
	case p.atKw("TABLE", "OVERRIDE") || p.atKw("CURRENT", "TRANSACTION"):
		notImplemented("EXPLAIN %s %s is not supported", strings.ToUpper(p.peek().text), strings.ToUpper(p.peekN(1).text))
	default:
		for _, k := range []string{"AST", "SYNTAX", "PLAN", "PIPELINE", "ESTIMATE"} {
			if p.acceptKw(k) {
				st.Kind = k
				break
			}
		}
	}
	// EXPLAIN options (`header = 1, actions = 1`) only affect presentation.
	for (p.peek().kind == tIdent) && isOp(p.peekN(1), "=") {
		p.next()
		p.next()
		p.parseExpr()
		p.acceptOp(",")
	}
	st.Stmt = p.parseStatement()
	return st
}
