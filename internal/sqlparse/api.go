package sqlparse

// Parse parses exactly one statement. A trailing ';' and trailing
// whitespace/comments are allowed. `--`, `#!`/`# ` and `/* */` comments are
// supported, and are recognised only outside string literals. Errors are
// *cherr.Error with code SYNTAX_ERROR (62) and a message in ClickHouse style:
// "Syntax error: failed at position N ('tok'): <query excerpt>. Expected ...".
// Input past ClickHouse's default limits is TOO_DEEP_RECURSION (306,
// max_parser_depth), TOO_DEEP_AST (167, max_ast_depth) or TOO_BIG_AST (168,
// max_ast_elements).
//
// For INSERT ... FORMAT <name>, parsing stops right after the format name;
// the remaining text becomes InsertStmt.Data (data is never tokenized).
// For INSERT ... VALUES, the tuples present in the query text are parsed
// into InsertStmt.Values (they may contain {name:Type} parameters).
func Parse(sql string) (Statement, error) { return parse(sql) }

// ParseWithParams is Parse with ClickHouse's parse-time substitution of
// {name:Identifier} query parameters: wherever the query has
// `{name:Identifier}` (a database, table, column or alias name, or an
// identifier in an expression), the parser reads params[name] as one quoted
// identifier, e.g. `SELECT count() FROM {db:Identifier}.{t:Identifier}`. The
// value is never split at dots. A missing value is UNKNOWN_QUERY_PARAMETER
// (456) "Substitution `name` is not set". Parameters of any other type stay
// Param nodes, bound by the engine. INSERT ... FORMAT data is not touched.
//
// Plain Parse cannot bind them and answers NOT_IMPLEMENTED (48) when it
// reaches an Identifier parameter.
func ParseWithParams(sql string, params map[string]string) (Statement, error) {
	return parseWithParams(sql, params)
}

// ParseRequest is ParseWithParams for a whole request: params bind
// {name:Identifier} parameters as in ParseWithParams, and settings are the
// request/session settings (URL settings, SET values). The parser consults
// only union_default_mode from them: a UNION written without ALL or DISTINCT
// takes the mode from the statement's own SETTINGS clause first, then from
// settings; empty or absent is EXPECTED_ALL_OR_DISTINCT (558) as in
// ClickHouse 25.8, and a value other than ALL, DISTINCT or the empty string is 557.
func ParseRequest(sql string, params, settings map[string]string) (Statement, error) {
	return parseRequest(sql, params, settings)
}

// ParseExpr parses a standalone expression (used for column DEFAULT text).
func ParseExpr(s string) (Expr, error) { return parseExpr(s) }

// ColumnName returns the result-column name ClickHouse gives an unaliased
// select expression. ClickHouse prints the AST in its function form:
//
//	count()                      -> "count()"
//	sum(event_count)             -> "sum(event_count)"
//	a + 1                        -> "plus(a, 1)"
//	a = 'x'                      -> "equals(a, 'x')"
//	NOT a                        -> "not(a)"
//	-x                           -> "negate(x)"
//	x IN (1, 2)                  -> "in(x, (1, 2))"
//	a AND b AND c                -> "and(a, b, c)"
//	x BETWEEN 1 AND 2            -> "and(greaterOrEquals(x, 1), lessOrEquals(x, 2))"
//	x IS NULL                    -> "isNull(x)"
//	CAST(x AS String)            -> "CAST(x, 'String')"
//	[1, 2]                       -> "[1, 2]"
//	(1, 'a')                     -> "(1, 'a')"
//	'str'                        -> "'str'"
//	t.col                        -> "col"     (qualifier stripped, like the CH analyzer)
//	{p:String}                   -> ColumnName renders "{p:String}"; ClickHouse
//	                                names bound parameters by value: a String
//	                                parameter as its quoted value ('x'), any other
//	                                type T as _CAST(<value literal>, 'T'), e.g.
//	                                _CAST(5, 'UInt64') — use ColumnNameFunc
//	quantile(0.9)(x)             -> "quantile(0.9)(x)"
//	count(DISTINCT x)            -> "countDistinct(x)"
//	CASE WHEN c THEN a ELSE b END-> "multiIf(c, a, b)"; CASE x WHEN ... -> "caseWithExpression(x, ...)"
//	x[1]                         -> "arrayElement(x, 1)"
//	t.1                          -> "tupleElement(t, 1)"
//	a || b                       -> "concat(a, b)"
//	a LIKE b                     -> "like(a, b)"
//	a % b                        -> "modulo(a, b)"
//	a / b, a * b, a - b          -> "divide(a, b)", "multiply(a, b)", "minus(a, b)"
//	<, <=, >, >=, !=             -> less, lessOrEquals, greater, greaterOrEquals, notEquals
//	OR                           -> "or(a, b)"
//	INTERVAL 1 DAY               -> "toIntervalDay(1)"
//	a AND (b AND c)              -> "and(a, and(b, c))"
//	x -> x + 1                   -> "lambda(tuple(x), plus(x, 1))"
//	x IN (SELECT ...)            -> "in(x, _subquery_1)"
//	(expr AS a) nested alias     -> "a"
//
// Literal strings are rendered quoted with ClickHouse escaping; numbers are
// formatted from their value like ClickHouse (1.50 -> "1.5", 1e3 -> "1000.",
// 0x1F -> "31"). Function names keep their source spelling.
func ColumnName(e Expr) string { return columnName(e) }

// ColumnNameFunc is ColumnName with an override: hook is consulted for every
// node first and, when it returns ok, its string names that subtree. The
// engine uses it where naming depends on resolution, e.g. bound parameters
// (ClickHouse names {p:UInt8} bound to 3 as "_CAST(3, 'UInt8')" and a String
// parameter as its quoted value) or named-tuple subcolumns (`tup.a` is
// "tup.a", whereas the default rule for a qualified Ident strips the
// qualifier: t.col -> "col", as the ClickHouse analyzer does).
func ColumnNameFunc(e Expr, hook func(Expr) (string, bool)) string { return columnNameFunc(e, hook) }

// SelectString renders a SELECT statement (with UNION branches, SETTINGS and
// FORMAT) back to parseable SQL.
func SelectString(s *SelectStmt) string { return selectString(s) }

// String renders e back to parseable SQL (used for storing view/default text
// and in error messages).
func String(e Expr) string { return exprString(e) }
