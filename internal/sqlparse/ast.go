// Package sqlparse parses the ClickHouse SQL dialect subset supported by
// twinhouse into an AST.
//
// AST CONTRACT: the node types below are shared by the parser and the query
// engine. Extend additively; do not rename or change field meanings.
package sqlparse

// Node is any AST node.
type Node interface{ node() }

// Statement is a top-level statement.
type Statement interface {
	Node
	stmt()
}

// Expr is an expression.
type Expr interface {
	Node
	expr()
}

// Setting is `name = value` from a SETTINGS clause or SET statement.
type Setting struct {
	Name  string
	Value Expr
}

// ---------------------------------------------------------------------------
// Statements

// SelectStmt is a SELECT, optionally followed by UNION ALL / UNION DISTINCT
// branches. Clauses appear in ClickHouse order.
type SelectStmt struct {
	With     []WithItem
	Distinct bool
	Columns  []SelectItem
	From     *TableExpr // nil for SELECT without FROM
	// ArrayJoin is `[LEFT] ARRAY JOIN expr [AS alias], ...` (optional support).
	ArrayJoin *ArrayJoinClause
	Joins     []JoinClause // optional support; engine may return NOT_IMPLEMENTED
	Prewhere  Expr
	Where     Expr
	GroupBy   []Expr
	// GroupByAll is true for `GROUP BY ALL`.
	GroupByAll bool
	WithTotals bool
	Having     Expr
	OrderBy    []OrderItem
	// LimitBy is `LIMIT n BY expr, ...` (optional support).
	LimitBy  *LimitByClause
	Limit    Expr // nil if absent
	Offset   Expr // nil if absent
	Settings []Setting
	Format   string // FORMAT clause, "" if absent (only on the outermost statement)
	// Union holds subsequent branches; Union[i] is combined with the result
	// so far using UnionOps[i] ("ALL" or "DISTINCT").
	Union    []*SelectStmt
	UnionOps []string
}

// WithItem is either `WITH <expr> AS name` (Expr set) or
// `WITH name AS (<select>)` (Subquery set, a CTE).
type WithItem struct {
	Name     string
	Expr     Expr
	Subquery *SelectStmt
}

// SelectItem is one projection. Alias is "" when absent.
type SelectItem struct {
	Expr  Expr
	Alias string
}

// TableExpr is a FROM source: a named table, a subquery or a table function.
type TableExpr struct {
	Database string // "" means current database
	Table    string
	Subquery *SelectStmt
	Func     *FuncCall // table function, e.g. numbers(10), system tables are named tables
	Alias    string
	Final    bool
}

// JoinClause is `[GLOBAL] [ANY|ALL|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] JOIN t ON expr | USING (cols)`.
type JoinClause struct {
	Kind       string // "INNER", "LEFT", "RIGHT", "FULL", "CROSS"
	Strictness string // "", "ANY", "ALL", "ASOF", "SEMI", "ANTI"
	Table      *TableExpr
	On         Expr
	Using      []string
}

// ArrayJoinClause is `[LEFT] ARRAY JOIN items`.
type ArrayJoinClause struct {
	Left  bool
	Items []SelectItem
}

// LimitByClause is `LIMIT n [OFFSET m] BY exprs`.
type LimitByClause struct {
	Limit  Expr
	Offset Expr
	By     []Expr
}

// OrderItem is one ORDER BY element.
type OrderItem struct {
	Expr       Expr
	Desc       bool
	NullsFirst *bool // nil = default
	Collate    string
}

// InsertStmt is INSERT INTO [db.]t [(cols)] VALUES ... | FORMAT f [data] | SELECT ...
type InsertStmt struct {
	Database string
	Table    string
	Columns  []string // nil = all insertable columns in table order
	// Format is the input format name: "Values" for VALUES (the parser
	// normalizes VALUES to "Values"), or the name after FORMAT.
	Format string
	// Values holds rows parsed from inline VALUES text in the query; it may
	// contain Param nodes ({name:Type}). Empty when the rows are in the body.
	Values [][]Expr
	// Data is the raw inline data that followed `FORMAT <name>` in the query
	// text (leading whitespace and a single newline trimmed); "" if none. The
	// engine reads Data first, then the request body.
	Data     string
	Select   *SelectStmt
	Settings []Setting
}

// CreateDatabaseStmt is CREATE DATABASE.
type CreateDatabaseStmt struct {
	Name        string
	IfNotExists bool
	Engine      string
}

// ColumnDef is one column in CREATE TABLE.
type ColumnDef struct {
	Name string
	// Type is the column type text normalized to ClickHouse spelling
	// (whitespace-normalized original text; chtype.Parse is applied by the engine).
	Type string
	// DefaultKind is "DEFAULT", "MATERIALIZED", "ALIAS", "EPHEMERAL" or "".
	DefaultKind string
	Default     Expr   // nil if no default expression
	DefaultText string // original text of Default, for the catalog
	Codec       string // ignored by the engine
	Comment     string
	TTL         string // raw text; ignored
}

// EngineClause is `ENGINE = Name(args)`.
type EngineClause struct {
	Name string
	Args []Expr
}

// CreateTableStmt is CREATE TABLE.
type CreateTableStmt struct {
	Database    string
	Name        string
	IfNotExists bool
	OrReplace   bool
	Columns     []ColumnDef
	Engine      *EngineClause
	OrderBy     []Expr // ORDER BY tuple() yields an empty, non-nil slice
	PartitionBy Expr
	PrimaryKey  []Expr
	SampleBy    Expr
	TTL         string // raw text; ignored
	Settings    []Setting
	Comment     string
	// AsTable is set for CREATE TABLE t AS other_db.other_t (copy schema).
	AsDatabase string
	AsTable    string
	// AsSelect is set for CREATE TABLE t ENGINE=... AS SELECT ...
	AsSelect *SelectStmt
	// Text is the full original statement text.
	Text string
}

// CreateViewStmt is CREATE [MATERIALIZED] VIEW.
type CreateViewStmt struct {
	Database     string
	Name         string
	IfNotExists  bool
	OrReplace    bool
	Materialized bool
	ToDatabase   string // TO [db.]table target, "" if none
	ToTable      string
	Populate     bool
	// Engine/OrderBy/Columns describe an implicit inner table when no TO
	// clause is used (optional support).
	Engine  *EngineClause
	OrderBy []Expr
	Columns []ColumnDef
	Select  *SelectStmt
	// SelectText is the original text of the AS SELECT ... part.
	SelectText string
	Text       string
}

// DropStmt is DROP TABLE|VIEW|DATABASE|DICTIONARY.
type DropStmt struct {
	Kind     string // "TABLE", "VIEW", "DATABASE", "DICTIONARY"
	IfExists bool
	Database string
	Name     string // for DATABASE, the database name is in Name
	Sync     bool
}

// TruncateStmt is TRUNCATE [TABLE] [IF EXISTS] [db.]t.
type TruncateStmt struct {
	IfExists bool
	Database string
	Name     string
}

// ShowStmt covers SHOW DATABASES | SHOW TABLES [FROM db] [LIKE 'p'] |
// SHOW CREATE TABLE [db.]t | SHOW COLUMNS FROM t.
type ShowStmt struct {
	Kind     string // "DATABASES", "TABLES", "CREATE TABLE", "CREATE VIEW", "COLUMNS", "PROCESSLIST"
	Database string
	Name     string
	Like     string
	NotLike  bool
	Format   string
}

// DescribeStmt is DESCRIBE/DESC [TABLE] [db.]t.
type DescribeStmt struct {
	Database string
	Name     string
	Format   string
}

// ExistsStmt is EXISTS [TABLE|DATABASE|VIEW] name.
type ExistsStmt struct {
	Kind     string // "TABLE", "DATABASE", "VIEW"
	Database string
	Name     string
	Format   string
}

// UseStmt is USE db.
type UseStmt struct{ Database string }

// SetStmt is SET a = 1, b = 'x'.
type SetStmt struct{ Settings []Setting }

// OptimizeStmt is OPTIMIZE TABLE t [FINAL] [DEDUPLICATE] — a no-op.
type OptimizeStmt struct {
	Database string
	Name     string
	Final    bool
}

// SystemStmt is any SYSTEM ... command (raw text after SYSTEM), mostly no-ops.
type SystemStmt struct{ Command string }

// AlterStmt is ALTER TABLE t <commands>.
type AlterStmt struct {
	Database string
	Name     string
	Commands []AlterCommand
}

// AlterCommand is one ALTER action.
type AlterCommand struct {
	// Kind: "ADD COLUMN", "DROP COLUMN", "MODIFY COLUMN", "COMMENT COLUMN",
	// "DELETE", "UPDATE", "MODIFY SETTING", or "OTHER" (raw text in Raw).
	Kind        string
	Column      *ColumnDef
	ColumnName  string
	IfExists    bool
	IfNotExists bool
	After       string
	First       bool
	Where       Expr      // DELETE/UPDATE
	Assignments []Setting // UPDATE col = expr
	Raw         string
}

// DeleteStmt is lightweight DELETE FROM t WHERE cond.
type DeleteStmt struct {
	Database string
	Name     string
	Where    Expr
}

// ExplainStmt is EXPLAIN [kind] <stmt>.
type ExplainStmt struct {
	Kind string
	Stmt Statement
}

func (*SelectStmt) node()         {}
func (*InsertStmt) node()         {}
func (*CreateDatabaseStmt) node() {}
func (*CreateTableStmt) node()    {}
func (*CreateViewStmt) node()     {}
func (*DropStmt) node()           {}
func (*TruncateStmt) node()       {}
func (*ShowStmt) node()           {}
func (*DescribeStmt) node()       {}
func (*ExistsStmt) node()         {}
func (*UseStmt) node()            {}
func (*SetStmt) node()            {}
func (*OptimizeStmt) node()       {}
func (*SystemStmt) node()         {}
func (*AlterStmt) node()          {}
func (*DeleteStmt) node()         {}
func (*ExplainStmt) node()        {}

func (*SelectStmt) stmt()         {}
func (*InsertStmt) stmt()         {}
func (*CreateDatabaseStmt) stmt() {}
func (*CreateTableStmt) stmt()    {}
func (*CreateViewStmt) stmt()     {}
func (*DropStmt) stmt()           {}
func (*TruncateStmt) stmt()       {}
func (*ShowStmt) stmt()           {}
func (*DescribeStmt) stmt()       {}
func (*ExistsStmt) stmt()         {}
func (*UseStmt) stmt()            {}
func (*SetStmt) stmt()            {}
func (*OptimizeStmt) stmt()       {}
func (*SystemStmt) stmt()         {}
func (*AlterStmt) stmt()          {}
func (*DeleteStmt) stmt()         {}
func (*ExplainStmt) stmt()        {}

// ---------------------------------------------------------------------------
// Expressions

// Literal is a constant. Value is one of: string, uint64 (non-negative
// integer literal), int64 (negative integer), float64, bool, nil (NULL).
// Like ClickHouse's parser, a minus sign directly before a numeric literal is
// folded into it: `-1` is Literal{int64(-1), "-1"} (`-0` stays uint64(0)),
// while `-(1)` and `-x` are UnaryExpr{"-", ...}. Integers that do not fit
// uint64/int64 become float64 (ClickHouse behaviour).
// Raw is the literal's source text (`1.50`, `'a”b'`); String() reuses it.
// ColumnName formats from Value the way ClickHouse does (1.50 -> "1.5").
// `<literal>::T` is parsed, as in ClickHouse, as CAST('<literal source>', T).
type Literal struct {
	Value any
	Raw   string
}

// Ident is a possibly-qualified identifier: col, t.col, db.t.col.
// Backquoted/double-quoted identifiers are unquoted in Parts.
type Ident struct{ Parts []string }

// Param is a server-side query parameter `{name:Type}`. Type is the type
// text with normalized spacing, e.g. "Array(Tuple(String, String))";
// RawType is the type exactly as written (from its first token up to the
// closing '}', trailing blanks included), which is what ClickHouse prints
// in the column name of a bound parameter: _CAST(v, '<RawType>').
// {name:Identifier} parameters never appear as Param nodes (see
// ParseWithParams).
type Param struct {
	Name    string
	Type    string
	RawType string
}

// Star is `*` or `t.*`.
type Star struct{ Table string }

// FuncCall is name(args) or name(params)(args) for parametric aggregates.
// Operators are NOT represented as FuncCall (see BinaryExpr/UnaryExpr) but
// the engine may map them to functions. Name keeps the source spelling;
// function-name matching is case-insensitive for the functions ClickHouse
// treats case-insensitively (count, sum, min, max, avg, if, ...).
type FuncCall struct {
	Name     string
	Params   []Expr // parametric parameters, nil if not parametric
	Args     []Expr
	Distinct bool // count(DISTINCT x)
	// Over is a window specification (optional support; nil if absent).
	Over *WindowSpec
}

// WindowSpec is OVER (PARTITION BY ... ORDER BY ...).
type WindowSpec struct {
	PartitionBy []Expr
	OrderBy     []OrderItem
	Frame       string
}

// BinaryExpr is a binary operator. Op is upper-case canonical:
// "OR", "AND", "=", "!=", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "||",
// "LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE", "IN", "NOT IN", "GLOBAL IN",
// "GLOBAL NOT IN". `==` normalizes to "=", `<>` to "!=", `MOD` to "%".
// `a DIV b` is integer division and is parsed as FuncCall intDiv(a, b);
// `a REGEXP b` is FuncCall match(a, b); `c ? a : b` is FuncCall if(c, a, b).
// Chains of the same operator are left-nested: a AND b AND c is
// AND(AND(a, b), c).
type BinaryExpr struct {
	Op    string
	Left  Expr
	Right Expr
	// Parens is set by the parser when an OR/AND/|| expression was written
	// inside its own parentheses. It only affects ColumnName (ClickHouse
	// merges `a AND b AND c` into and(a, b, c) but names `(a AND b) AND c`
	// and(and(a, b), c)) and String; evaluation ignores it.
	Parens bool
}

// UnaryExpr is "NOT" or "-".
type UnaryExpr struct {
	Op string
	X  Expr
}

// BetweenExpr is X [NOT] BETWEEN Low AND High.
type BetweenExpr struct {
	X, Low, High Expr
	Not          bool
}

// IsNullExpr is X IS [NOT] NULL.
type IsNullExpr struct {
	X   Expr
	Not bool
}

// TupleExpr is (a, b, ...) or tuple(...) with 2+ elements, or (a,) — a single
// parenthesized expression is NOT a tuple. Empty () / tuple() is a TupleExpr
// with no elements.
//
// A parenthesized tuple literal is a one-element tuple, as in ClickHouse:
// ((1, 'a')) is TupleExpr{(1, 'a')}. Func is set when the tuple was written
// as a tuple(...) call, which ClickHouse does not treat as a literal.
type TupleExpr struct {
	Elems []Expr
	Func  bool
}

// ArrayExpr is [a, b, ...].
// Func is set when it was written as an array(...) call, which ClickHouse
// does not treat as a literal (see ColumnName).
type ArrayExpr struct {
	Elems []Expr
	Func  bool
}

// IndexExpr is X[Index] (array/map subscript, 1-based for arrays).
type IndexExpr struct{ X, Index Expr }

// TupleAccessExpr is X.N (1-based) or X.name on a tuple-typed expression.
// The parser emits it only for `expr.<integer>`; `a.b` is an Ident.
type TupleAccessExpr struct {
	X     Expr
	Index int
	Name  string
}

// CaseExpr is CASE [Operand] WHEN ... THEN ... [ELSE ...] END.
type CaseExpr struct {
	Operand Expr
	Whens   []WhenClause
	Else    Expr
}

// WhenClause is one WHEN cond THEN result.
type WhenClause struct{ Cond, Result Expr }

// CastExpr is CAST(x AS T), CAST(x, 'T') or x::T. Type is raw type text.
type CastExpr struct {
	X    Expr
	Type string
}

// SubqueryExpr is a parenthesized SELECT used as an expression (scalar
// subquery or IN (SELECT ...)).
type SubqueryExpr struct{ Select *SelectStmt }

// LambdaExpr is x -> body or (x, y) -> body.
type LambdaExpr struct {
	Params []string
	Body   Expr
}

// IntervalExpr is INTERVAL x UNIT (UNIT upper-case singular: SECOND, MINUTE,
// HOUR, DAY, WEEK, MONTH, QUARTER, YEAR, MILLISECOND, MICROSECOND, NANOSECOND).
type IntervalExpr struct {
	X    Expr
	Unit string
}

// AliasExpr is `expr AS name` appearing inside an expression (ClickHouse
// allows aliases anywhere, e.g. WHERE (a + 1 AS b) > 2). Top-level select
// aliases use SelectItem.Alias instead.
type AliasExpr struct {
	X     Expr
	Alias string
}

func (*Literal) node()         {}
func (*Ident) node()           {}
func (*Param) node()           {}
func (*Star) node()            {}
func (*FuncCall) node()        {}
func (*BinaryExpr) node()      {}
func (*UnaryExpr) node()       {}
func (*BetweenExpr) node()     {}
func (*IsNullExpr) node()      {}
func (*TupleExpr) node()       {}
func (*ArrayExpr) node()       {}
func (*IndexExpr) node()       {}
func (*TupleAccessExpr) node() {}
func (*CaseExpr) node()        {}
func (*CastExpr) node()        {}
func (*SubqueryExpr) node()    {}
func (*LambdaExpr) node()      {}
func (*IntervalExpr) node()    {}
func (*AliasExpr) node()       {}

func (*Literal) expr()         {}
func (*Ident) expr()           {}
func (*Param) expr()           {}
func (*Star) expr()            {}
func (*FuncCall) expr()        {}
func (*BinaryExpr) expr()      {}
func (*UnaryExpr) expr()       {}
func (*BetweenExpr) expr()     {}
func (*IsNullExpr) expr()      {}
func (*TupleExpr) expr()       {}
func (*ArrayExpr) expr()       {}
func (*IndexExpr) expr()       {}
func (*TupleAccessExpr) expr() {}
func (*CaseExpr) expr()        {}
func (*CastExpr) expr()        {}
func (*SubqueryExpr) expr()    {}
func (*LambdaExpr) expr()      {}
func (*IntervalExpr) expr()    {}
func (*AliasExpr) expr()       {}
