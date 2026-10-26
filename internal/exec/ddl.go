package exec

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/gfronza/twinhouse/internal/catalog"
	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec/aggstate"
	"github.com/gfronza/twinhouse/internal/format"
	"github.com/gfronza/twinhouse/internal/sqlparse"
	"github.com/gfronza/twinhouse/internal/storage"
)

// System attribute names of the DynamoDB item layout; columns may not use them.
var reservedColumns = map[string]bool{"_pk": true, "_sk": true, "_rid": true, "_ts": true, "_origin": true, "_mv": true, "_exp": true}

func (q *query) runCreateDatabase(s *sqlparse.CreateDatabaseStmt) (*Result, error) {
	if s.Name == "system" || strings.EqualFold(s.Name, "information_schema") {
		return nil, cherr.New(cherr.DatabaseAlreadyExists, "Database %s already exists", s.Name)
	}
	if err := catalog.ValidateName("database", s.Name); err != nil {
		return nil, err
	}
	engine := s.Engine
	if engine == "" {
		engine = "Atomic"
	}
	db := &catalog.Database{Name: s.Name, Engine: engine, CreatedAt: time.Now().UTC()}
	if err := q.cat.CreateDatabase(q.ctx, db, s.IfNotExists); err != nil {
		return nil, err
	}
	return &Result{}, nil
}

func (q *query) physicalName(db, table string) string {
	if f := q.e.cfg.PhysicalName; f != nil {
		return f(db, table)
	}
	return db + "." + table
}

func (q *query) runCreateTable(s *sqlparse.CreateTableStmt) (*Result, error) {
	db := q.defaultDB(s.Database)
	cat := q.cat
	if _, err := cat.Database(db); err != nil {
		return nil, err
	}
	if db == "system" {
		return nil, cherr.New(cherr.NotImplemented, "cannot create tables in database system")
	}
	if err := catalog.ValidateName("table", s.Name); err != nil {
		return nil, err
	}
	if _, err := cat.View(db, s.Name); err == nil {
		if s.IfNotExists {
			return &Result{}, nil
		}
		return nil, cherr.New(cherr.TableAlreadyExists, "Table %s.%s already exists", db, s.Name)
	}
	var replaced *catalog.Table // OR REPLACE: dropped once the new definition is valid
	if existing, err := cat.Table(db, s.Name); err == nil {
		switch {
		case s.OrReplace:
			replaced = existing
		case s.IfNotExists:
			return &Result{}, q.e.cfg.Store.EnsureTable(q.ctx, existing)
		default:
			return nil, cherr.New(cherr.TableAlreadyExists, "Table %s.%s already exists", db, s.Name)
		}
	}
	stmt := *s
	var asSelect *relation
	// Column types copied from a table or a SELECT were rendered by the
	// server, so they are parsed without the client length cap.
	parse := chtype.Parse
	if s.AsTable != "" {
		src, err := q.lookupTable(s.AsDatabase, s.AsTable)
		if err != nil {
			return nil, err
		}
		if len(stmt.Columns) == 0 {
			for _, c := range src.Columns {
				def := sqlparse.ColumnDef{Name: c.Name, Type: c.TypeText, DefaultKind: c.DefaultKind, DefaultText: c.DefaultExpr, Comment: c.Comment}
				stmt.Columns = append(stmt.Columns, def)
			}
			parse = chtype.ParseStored
		}
		if stmt.Engine == nil {
			if replaced != nil {
				if err := q.dropTable(replaced); err != nil {
					return nil, err
				}
			}
			return q.createLike(&stmt, db, src)
		}
	}
	if s.AsSelect != nil {
		rel, err := q.planSelect(s.AsSelect, &planEnv{})
		if err != nil {
			return nil, err
		}
		asSelect = rel
		if len(stmt.Columns) == 0 {
			for _, c := range rel.cols {
				stmt.Columns = append(stmt.Columns, sqlparse.ColumnDef{Name: c.Name, Type: c.Type.String()})
			}
			parse = chtype.ParseStored
		}
	}
	t, err := q.buildTable(db, &stmt, parse)
	if err != nil {
		return nil, err
	}
	if replaced != nil {
		if err := q.dropTable(replaced); err != nil {
			return nil, err
		}
	}
	return q.createTable(t, s.IfNotExists, asSelect)
}

// createLike handles CREATE TABLE t AS other (same engine and keys).
func (q *query) createLike(s *sqlparse.CreateTableStmt, db string, src *catalog.Table) (*Result, error) {
	t := *src
	t.Database, t.Name = db, s.Name
	t.Columns = append([]catalog.Column(nil), src.Columns...)
	t.PhysicalName = q.physicalName(db, s.Name)
	t.DDL = s.Text
	t.CreatedAt = time.Now().UTC()
	t.Version = 0
	return q.createTable(&t, s.IfNotExists, nil)
}

func (q *query) createTable(t *catalog.Table, ifNotExists bool, asSelect *relation) (*Result, error) {
	cat := q.cat
	if err := cat.CreateTable(q.ctx, t, ifNotExists); err != nil {
		return nil, err
	}
	stored, err := cat.Table(t.Database, t.Name)
	if err != nil {
		return nil, err
	}
	if err := q.e.cfg.Store.EnsureTable(q.ctx, stored); err != nil {
		return nil, err
	}
	if asSelect == nil {
		return &Result{}, nil
	}
	cols := insertableColumns(stored)
	n, err := q.insertRelation(stored, cols, asSelect)
	if err != nil {
		return nil, err
	}
	return &Result{WrittenRows: n}, nil
}

// buildTable converts CREATE TABLE into a catalog table definition, parsing
// column types with parse.
func (q *query) buildTable(db string, s *sqlparse.CreateTableStmt, parse func(string) (chtype.Type, error)) (*catalog.Table, error) {
	t := &catalog.Table{
		Database: db, Name: s.Name, Comment: s.Comment, DDL: s.Text,
		CreatedAt: time.Now().UTC(), Settings: map[string]string{},
		PhysicalName: q.physicalName(db, s.Name),
	}
	if len(s.Columns) == 0 {
		return nil, cherr.New(cherr.IncorrectQuery, "Table %s.%s must have at least one column", db, s.Name)
	}
	seen := map[string]bool{}
	for _, def := range s.Columns {
		col, err := q.buildColumn(def, parse)
		if err != nil {
			return nil, err
		}
		if seen[col.Name] {
			return nil, cherr.New(duplicateColumn, "Column %s specified more than once", col.Name)
		}
		seen[col.Name] = true
		t.Columns = append(t.Columns, col)
	}
	if err := q.checkDefaults(t); err != nil {
		return nil, err
	}
	for _, st := range s.Settings {
		v, err := settingValue(st.Value)
		if err != nil {
			return nil, err
		}
		t.Settings[st.Name] = v
		t.SettingsSQL = append(t.SettingsSQL, st.Name+" = "+sqlparse.String(st.Value))
	}
	if s.PartitionBy != nil {
		t.PartitionBy = sqlparse.String(s.PartitionBy)
	}
	for _, e := range s.PrimaryKey {
		t.PrimaryKey = append(t.PrimaryKey, sqlparse.String(e))
	}

	engine, args, err := normalizeEngine(s.Engine)
	if err != nil {
		return nil, err
	}
	t.Engine = engine
	for _, a := range args {
		t.EngineArgs = append(t.EngineArgs, sqlparse.String(a))
	}

	orderBy, err := keyColumnNames(s.OrderBy, "ORDER BY")
	if err != nil {
		return nil, err
	}
	primary, err := keyColumnNames(s.PrimaryKey, "PRIMARY KEY")
	if err != nil {
		return nil, err
	}
	switch engine {
	case catalog.EngineMemory, catalog.EngineLog:
		if len(orderBy) > 0 || len(primary) > 0 {
			return nil, cherr.New(cherr.BadArguments, "Engine %s doesn't support ORDER BY or PRIMARY KEY", engine)
		}
		orderBy = []string{}
	default:
		if s.OrderBy == nil && s.PrimaryKey == nil {
			return nil, cherr.New(cherr.NumberOfArgsDoesntMatch, "ORDER BY or PRIMARY KEY clause is missing. Consider using extended storage definition syntax with ORDER BY or PRIMARY KEY clause")
		}
		if s.OrderBy == nil {
			orderBy = primary
		}
		if len(primary) > len(orderBy) {
			return nil, cherr.New(cherr.BadArguments, "Primary key must be a prefix of the sorting key, but its length: %d is greater than the sorting key length: %d", len(primary), len(orderBy))
		}
		for i := range primary {
			if primary[i] != orderBy[i] {
				return nil, cherr.New(cherr.BadArguments, "Primary key must be a prefix of the sorting key, but the column in the position %d is %s, not %s", i, orderBy[i], primary[i])
			}
		}
	}
	t.OrderBy = orderBy
	if err := validateKeyColumns(t); err != nil {
		return nil, err
	}

	// DynamoDB partition key: leading ORDER BY columns.
	pk := orderBy
	if len(orderBy) > 1 {
		pk = orderBy[:1]
	}
	if v, ok := t.Settings["dynamodb_partition_key"]; ok {
		pk = splitList(v)
		for i, c := range pk {
			if i >= len(orderBy) || orderBy[i] != c {
				return nil, cherr.New(cherr.BadArguments, "dynamodb_partition_key (%s) must be a prefix of ORDER BY (%s)", v, strings.Join(orderBy, ", "))
			}
		}
	}
	t.PartitionKey = append([]string{}, pk...)
	t.SortKey = append([]string{}, orderBy[len(pk):]...)

	switch t.Replication = t.Settings["dynamodb_replication"]; t.Replication {
	case "":
		t.Replication = catalog.ReplicationGlobal
		if engine == catalog.EngineAggregatingMergeTree || engine == catalog.EngineSummingMergeTree {
			t.Replication = catalog.ReplicationRegional
		}
	case catalog.ReplicationGlobal, catalog.ReplicationRegional:
	default:
		return nil, cherr.New(cherr.BadArguments, "dynamodb_replication must be 'global' or 'regional', got '%s'", t.Replication)
	}

	if err := q.engineArgs(t, args); err != nil {
		return nil, err
	}
	return t, nil
}

func splitList(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (q *query) buildColumn(def sqlparse.ColumnDef, parse func(string) (chtype.Type, error)) (catalog.Column, error) {
	if err := checkColumnName(def.Name); err != nil {
		return catalog.Column{}, err
	}
	col := catalog.Column{Name: def.Name, DefaultKind: strings.ToUpper(def.DefaultKind), Comment: def.Comment}
	if def.Default != nil || def.DefaultText != "" {
		col.DefaultExpr = def.DefaultText
		if col.DefaultExpr == "" {
			col.DefaultExpr = sqlparse.String(def.Default)
		}
		if col.DefaultKind == "" {
			col.DefaultKind = "DEFAULT"
		}
	}
	if def.Type == "" {
		if col.DefaultExpr == "" {
			return catalog.Column{}, cherr.New(cherr.IncorrectQuery, "Column %s has neither type nor default expression", def.Name)
		}
		e, err := sqlparse.ParseExpr(col.DefaultExpr)
		if err != nil {
			return catalog.Column{}, err
		}
		x, err := (&compiler{q: q, env: &planEnv{}, sc: &scope{}}).compile(e)
		if err != nil {
			return catalog.Column{}, err
		}
		col.Type = x.t
	} else {
		t, err := parse(def.Type)
		if err != nil {
			return catalog.Column{}, err
		}
		col.Type = t
	}
	col.TypeText = col.Type.String()
	col.Codec = catalog.NormalizeCodec(def.Codec, col.Type)
	switch col.Type.Kind {
	case chtype.AggregateFunction:
		if !aggstate.Supported(col.Type.AggFunc) {
			return catalog.Column{}, cherr.New(cherr.NotImplemented, "AggregateFunction(%s, ...) is not supported (column %s)", col.Type.AggFunc, col.Name)
		}
	case chtype.SimpleAggregateFunction:
		if !aggstate.SimpleSupported(col.Type.AggFunc) {
			return catalog.Column{}, cherr.New(cherr.NotImplemented, "SimpleAggregateFunction(%s, ...) is not supported (column %s)", col.Type.AggFunc, col.Name)
		}
	}
	return col, nil
}

func checkColumnName(name string) error {
	if name == "" {
		return cherr.New(cherr.BadArguments, "Empty column name")
	}
	if reservedColumns[name] {
		return cherr.New(cherr.BadArguments, "Column name %s is reserved by the DynamoDB storage layout", name)
	}
	if strings.ContainsRune(name, '#') {
		return cherr.New(cherr.BadArguments, "Column name %s must not contain '#'", name)
	}
	return nil
}

// checkDefaults compiles DEFAULT/MATERIALIZED/ALIAS expressions against the
// table's columns so that broken defaults fail at CREATE time.
func (q *query) checkDefaults(t *catalog.Table) error {
	sc := tableScope(t, "")
	for _, c := range t.Columns {
		if c.DefaultExpr == "" {
			continue
		}
		e, err := sqlparse.ParseExpr(c.DefaultExpr)
		if err != nil {
			return err
		}
		if hasAggregate(e) {
			return cherr.New(illegalAggregation, "Aggregate function in default expression of column %s", c.Name)
		}
		x, err := (&compiler{q: q, env: &planEnv{}, sc: sc, used: map[int]bool{}, clause: "default expression of column " + c.Name}).compile(e)
		if err != nil {
			return err
		}
		if _, err := converter(x.t, c.Type); err != nil {
			return err
		}
	}
	return nil
}

// normalizeEngine maps the ENGINE clause to a supported engine.
func normalizeEngine(ec *sqlparse.EngineClause) (string, []sqlparse.Expr, error) {
	if ec == nil {
		return catalog.EngineMergeTree, nil, nil
	}
	name, args := ec.Name, ec.Args
	if strings.HasPrefix(name, "Replicated") && strings.HasSuffix(name, "MergeTree") {
		name = strings.TrimPrefix(name, "Replicated")
		if len(args) >= 2 {
			args = args[2:]
		}
	}
	if strings.HasPrefix(name, "Shared") && strings.HasSuffix(name, "MergeTree") {
		name = strings.TrimPrefix(name, "Shared")
	}
	switch name {
	case catalog.EngineMergeTree, catalog.EngineReplacingMergeTree, catalog.EngineAggregatingMergeTree,
		catalog.EngineSummingMergeTree, catalog.EngineMemory, catalog.EngineLog:
		return name, args, nil
	case "TinyLog", "StripeLog":
		return catalog.EngineLog, args, nil
	}
	if !knownEngines[name] {
		for _, k := range knownEngineList {
			if strings.EqualFold(k, ec.Name) {
				return "", nil, cherr.New(cherr.UnknownStorage, "Unknown table engine %s. Maybe you meant: ['%s']", ec.Name, k)
			}
		}
		return "", nil, cherr.New(cherr.UnknownStorage, "Unknown table engine %s", ec.Name)
	}
	return "", nil, cherr.New(cherr.NotImplemented, "table engine %s is not supported (supported: MergeTree, ReplacingMergeTree, AggregatingMergeTree, SummingMergeTree, Memory, Log)", ec.Name)
}

// knownEngineList lists the table engines of ClickHouse 25.8
// (system.table_engines, without the Replicated/Shared variants, which are
// mapped to their base engine). Engines outside this list are unknown
// (UNKNOWN_STORAGE); known engines without an implementation here are
// NOT_IMPLEMENTED.
var knownEngineList = []string{
	"AggregatingMergeTree", "ArrowFlight", "AzureBlobStorage", "AzureQueue", "Buffer", "COSN",
	"CoalescingMergeTree", "CollapsingMergeTree", "DeltaLake", "DeltaLakeAzure", "DeltaLakeLocal",
	"DeltaLakeS3", "Dictionary", "Distributed", "EmbeddedRocksDB", "Executable", "ExecutablePool",
	"File", "FileLog", "FuzzJSON", "FuzzQuery", "GCS", "GenerateRandom", "GraphiteMergeTree", "HDFS",
	"Hive", "Hudi", "Iceberg", "IcebergAzure", "IcebergHDFS", "IcebergLocal", "IcebergS3", "JDBC",
	"Join", "Kafka", "KeeperMap", "LiveView", "Log", "Loop", "MaterializedPostgreSQL",
	"MaterializedView", "Memory", "Merge", "MergeTree", "MongoDB", "MySQL", "NATS", "Null", "ODBC",
	"OSS", "PostgreSQL", "RabbitMQ", "Redis", "ReplacingMergeTree", "S3", "S3Queue", "SQLite", "Set",
	"StripeLog", "SummingMergeTree", "TimeSeries", "TinyLog", "URL", "VersionedCollapsingMergeTree",
	"View", "WindowView", "YTsaurus",
}

var knownEngines = func() map[string]bool {
	m := make(map[string]bool, len(knownEngineList))
	for _, n := range knownEngineList {
		m[n] = true
	}
	return m
}()

func keyColumnNames(exprs []sqlparse.Expr, clause string) ([]string, error) {
	var out []string
	for _, e := range exprs {
		switch x := e.(type) {
		case *sqlparse.Ident:
			if len(x.Parts) != 1 {
				return nil, cherr.New(cherr.NotImplemented, "%s supports only plain column names, got %s", clause, sqlparse.String(e))
			}
			out = append(out, x.Parts[0])
		case *sqlparse.TupleExpr:
			inner, err := keyColumnNames(x.Elems, clause)
			if err != nil {
				return nil, err
			}
			out = append(out, inner...)
		case *sqlparse.FuncCall:
			if strings.EqualFold(x.Name, "tuple") {
				inner, err := keyColumnNames(x.Args, clause)
				if err != nil {
					return nil, err
				}
				out = append(out, inner...)
				continue
			}
			return nil, cherr.New(cherr.NotImplemented, "%s supports only plain column names, got %s", clause, sqlparse.String(e))
		default:
			return nil, cherr.New(cherr.NotImplemented, "%s supports only plain column names, got %s", clause, sqlparse.String(e))
		}
	}
	if out == nil && exprs != nil {
		out = []string{}
	}
	return out, nil
}

func validateKeyColumns(t *catalog.Table) error {
	seen := map[string]bool{}
	for _, n := range t.OrderBy {
		if seen[n] {
			return cherr.New(cherr.BadArguments, "Sorting key contains duplicate column %s", n)
		}
		seen[n] = true
		i := t.ColumnIndex(n)
		if i < 0 {
			return cherr.New(cherr.UnknownIdentifier, "Missing column %s in sorting key of table %s", n, t.FullName())
		}
		c := t.Columns[i]
		switch {
		case c.Type.IsNullable():
			return cherr.New(cherr.IllegalColumn, "Sorting key contains nullable columns, but merge tree setting `allow_nullable_key` is disabled")
		case c.Type.Kind == chtype.AggregateFunction || c.Type.Kind == chtype.SimpleAggregateFunction:
			return cherr.New(cherr.IllegalColumn, "Column %s with type %s is not allowed in key expression", n, c.TypeText)
		case c.DefaultKind == "ALIAS" || c.DefaultKind == "EPHEMERAL":
			return cherr.New(cherr.IllegalColumn, "%s column %s is not allowed in the sorting key", c.DefaultKind, n)
		}
		if k := c.Type.Unwrap().Kind; k == chtype.Map {
			return cherr.New(cherr.IllegalColumn, "Column %s with type %s is not allowed in key expression", n, c.TypeText)
		}
	}
	return nil
}

// engineArgs applies engine parameters and engine-specific column rules.
func (q *query) engineArgs(t *catalog.Table, args []sqlparse.Expr) error {
	isKey := map[string]bool{}
	for _, n := range t.OrderBy {
		isKey[n] = true
	}
	switch t.Engine {
	case catalog.EngineMergeTree, catalog.EngineMemory, catalog.EngineLog, catalog.EngineAggregatingMergeTree:
		if len(args) > 0 {
			return cherr.New(cherr.NumberOfArgsDoesntMatch, "Engine %s doesn't accept arguments in this syntax", t.Engine)
		}
	case catalog.EngineReplacingMergeTree:
		if len(args) > 2 {
			return cherr.New(cherr.NumberOfArgsDoesntMatch, "With extended storage definition syntax storage ReplacingMergeTree requires 0 to 2 parameters: [is_deleted column], [version]")
		}
		if len(args) == 2 {
			// ReplacingMergeTree(ver, is_deleted): reads (always FINAL) hide
			// keys whose latest row has is_deleted = 1.
			id, ok := args[1].(*sqlparse.Ident)
			if !ok || len(id.Parts) != 1 {
				return cherr.New(cherr.BadArguments, "is_deleted column of ReplacingMergeTree must be a column name")
			}
			i := t.ColumnIndex(id.Parts[0])
			if i < 0 {
				return cherr.New(cherr.NoSuchColumnInTable, "is_deleted column %s does not exist in table declaration", id.Parts[0])
			}
			if t.Columns[i].Type.Kind != chtype.UInt8 {
				return cherr.New(badTypeOfField, "is_deleted column (%s) for storage ReplacingMergeTree must have type UInt8. Provided column of type %s", id.Parts[0], t.Columns[i].TypeText)
			}
		}
		switch len(args) {
		case 0:
		case 1, 2:
			id, ok := args[0].(*sqlparse.Ident)
			if !ok || len(id.Parts) != 1 {
				return cherr.New(cherr.BadArguments, "Version column of ReplacingMergeTree must be a column name")
			}
			i := t.ColumnIndex(id.Parts[0])
			if i < 0 {
				return cherr.New(cherr.UnknownIdentifier, "Version column %s does not exist in table declaration", id.Parts[0])
			}
			k := t.Columns[i].Type.Kind
			if !(isIntKind(k) && k != chtype.Bool) && k != chtype.Date && k != chtype.Date32 && k != chtype.DateTime && k != chtype.DateTime64 {
				return cherr.New(cherr.BadArguments, "The column %s cannot be used as a version column for storage ReplacingMergeTree because it is of type %s (must be of an integer type or of type Date/DateTime/DateTime64)", id.Parts[0], t.Columns[i].TypeText)
			}
			t.VersionColumn = id.Parts[0]
		}
	case catalog.EngineSummingMergeTree:
		if len(args) > 1 {
			return cherr.New(cherr.NumberOfArgsDoesntMatch, "SummingMergeTree accepts at most one argument (a tuple of columns)")
		}
		if len(args) == 1 {
			names, err := keyColumnNames([]sqlparse.Expr{args[0]}, "SummingMergeTree columns")
			if err != nil {
				return err
			}
			for _, n := range names {
				i := t.ColumnIndex(n)
				if i < 0 {
					return cherr.New(cherr.UnknownIdentifier, "Column %s listed in SummingMergeTree does not exist", n)
				}
				if isKey[n] || !t.Columns[i].Type.IsNumeric() {
					return cherr.New(cherr.BadArguments, "Column %s listed in SummingMergeTree must be a numeric non-key column", n)
				}
			}
			t.SumColumns = names
		}
	}
	if t.Engine == catalog.EngineAggregatingMergeTree {
		for _, c := range t.Columns {
			if isKey[c.Name] || c.DefaultKind == "ALIAS" || c.DefaultKind == "EPHEMERAL" {
				continue
			}
			if k := c.Type.Kind; k != chtype.AggregateFunction && k != chtype.SimpleAggregateFunction {
				return cherr.New(cherr.NotImplemented, "AggregatingMergeTree column %s of type %s: non-key columns must be AggregateFunction or SimpleAggregateFunction", c.Name, c.TypeText)
			}
		}
	}
	return nil
}

func (q *query) runCreateView(s *sqlparse.CreateViewStmt) (*Result, error) {
	db := q.defaultDB(s.Database)
	cat := q.cat
	if _, err := cat.Database(db); err != nil {
		return nil, err
	}
	if err := catalog.ValidateName("view", s.Name); err != nil {
		return nil, err
	}
	if s.Select == nil {
		return nil, cherr.New(cherr.IncorrectQuery, "CREATE VIEW requires AS SELECT")
	}
	if _, err := cat.Table(db, s.Name); err == nil {
		if s.IfNotExists {
			return &Result{}, nil
		}
		return nil, cherr.New(cherr.TableAlreadyExists, "Table %s.%s already exists", db, s.Name)
	}
	// OR REPLACE drops the old view only once the new one is validated.
	replace := false
	replaceExisting := func() error {
		if !replace {
			return nil
		}
		return cat.DropView(q.ctx, db, s.Name, true)
	}
	if _, err := cat.View(db, s.Name); err == nil {
		switch {
		case s.OrReplace:
			replace = true
		case s.IfNotExists:
			return &Result{}, nil
		default:
			return nil, cherr.New(cherr.TableAlreadyExists, "Table %s.%s already exists", db, s.Name)
		}
	}
	selText := s.SelectText
	if strings.TrimSpace(selText) == "" {
		return nil, cherr.New(cherr.LogicalError, "CREATE VIEW: missing SELECT text")
	}
	v := &catalog.View{Database: db, Name: s.Name, Materialized: s.Materialized, SelectSQL: selText, DDL: s.Text, CreatedAt: time.Now().UTC()}
	if !s.Materialized {
		if s.Populate || s.ToTable != "" {
			return nil, cherr.New(cherr.IncorrectQuery, "POPULATE and TO are allowed only for materialized views")
		}
		if _, err := q.planSelect(s.Select, &planEnv{}); err != nil {
			return nil, err
		}
		if err := replaceExisting(); err != nil {
			return nil, err
		}
		if err := cat.CreateView(q.ctx, v, s.IfNotExists); err != nil {
			return nil, err
		}
		return &Result{}, nil
	}

	if s.ToTable == "" {
		return nil, cherr.New(cherr.NotImplemented, "materialized views without TO are not supported: create the target table and use CREATE MATERIALIZED VIEW ... TO target AS SELECT")
	}
	if len(s.Select.Union) > 0 {
		return nil, cherr.New(cherr.NotImplemented, "UNION in a materialized view SELECT is not supported")
	}
	from := s.Select.From
	if from == nil || from.Table == "" || from.Subquery != nil || from.Func != nil {
		return nil, cherr.New(cherr.NotImplemented, "a materialized view SELECT must read FROM a table")
	}
	if len(s.Select.Joins) > 0 {
		return nil, cherr.New(cherr.NotImplemented, "JOIN in a materialized view SELECT is not supported")
	}
	v.SourceDatabase = q.defaultDB(from.Database)
	v.SourceTable = from.Table
	v.ToDatabase = q.defaultDB(s.ToDatabase)
	v.ToTable = s.ToTable
	src, err := cat.Table(v.SourceDatabase, v.SourceTable)
	if err != nil {
		return nil, err
	}
	if mergingEngine(src) {
		return nil, cherr.New(cherr.NotImplemented, "a materialized view cannot read FROM %s table %s: its stored rows are merged in place, so the stream does not carry the inserted rows", src.Engine, src.FullName())
	}
	target, err := cat.Table(v.ToDatabase, v.ToTable)
	if err != nil {
		return nil, err
	}
	if _, err := q.planView(v, s.Select, src, target); err != nil {
		return nil, err
	}
	if err := checkViewKeysDeterministic(s.Select, target); err != nil {
		return nil, err
	}
	if s.Populate && q.e.cfg.MultiRegion && target.Replication == catalog.ReplicationRegional {
		return nil, cherr.New(cherr.NotImplemented, "POPULATE into the regional table %s is not supported in a multi-region deployment: it would back-fill only this region's copy. Create the view without POPULATE and back-fill with INSERT INTO ... SELECT in every region", target.FullName())
	}
	if err := replaceExisting(); err != nil {
		return nil, err
	}
	if err := cat.CreateView(q.ctx, v, s.IfNotExists); err != nil {
		return nil, err
	}
	if !s.Populate {
		return &Result{}, nil
	}
	stored, err := cat.View(db, s.Name)
	if err != nil {
		return nil, err
	}
	n, err := q.populate(stored, src)
	if err != nil {
		return nil, err
	}
	return &Result{WrittenRows: n}, nil
}

// populate applies a new view to every existing row of its source table.
func (q *query) populate(v *catalog.View, src *catalog.Table) (uint64, error) {
	it, err := q.e.cfg.Store.Scan(q.ctx, src, storage.ScanRequest{Consistent: q.consistent(), MarkAbsent: true})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	// Use the stored row ids so the exactly-once markers of the backfill
	// coincide with those of the stream worker (which may replay the same
	// rows from the source stream) and of concurrent inline inserts.
	ridIt, _ := it.(RowIDIterator)
	sc := tableScope(src, "")
	defaults := compileDefaults(&compiler{q: q, env: &planEnv{}, sc: sc, used: map[int]bool{}}, sc, src.Columns, func(int) bool { return true })
	run := uuid.NewString()
	var rows []storage.Row
	var ids []string
	var total uint64
	flush := func() error {
		if len(rows) == 0 {
			return nil
		}
		if err := q.applyViewList([]*catalog.View{v}, src, rows, ids); err != nil {
			return err
		}
		total += uint64(len(rows))
		rows, ids = rows[:0], ids[:0]
		return nil
	}
	for n := 0; ; n++ {
		r, err := it.Next(q.ctx)
		if err != nil {
			if isEOF(err) {
				break
			}
			return 0, err
		}
		if err := fillMissing(r, src.Columns, defaults, absentColumns(r, nil)); err != nil {
			return 0, err
		}
		rows = append(rows, r)
		id := ""
		if ridIt != nil {
			id = ridIt.RowID()
		}
		if id == "" {
			id = fmt.Sprintf("populate:%s:%d", run, n)
		}
		ids = append(ids, id)
		if len(rows) >= insertBatch {
			if err := flush(); err != nil {
				return 0, err
			}
		}
	}
	return total, flush()
}

func (q *query) runDrop(s *sqlparse.DropStmt) (*Result, error) {
	cat := q.cat
	switch s.Kind {
	case "DATABASE":
		if s.Name == "system" {
			return nil, cherr.New(cherr.NotImplemented, "cannot drop database system")
		}
		if _, err := cat.Database(s.Name); err != nil {
			if s.IfExists && cherr.Is(err, cherr.UnknownDatabase) {
				return &Result{}, nil
			}
			return nil, err
		}
		for _, t := range cat.Tables(s.Name) {
			if err := q.dropPhysical(t); err != nil {
				return nil, err
			}
		}
		if err := cat.DropDatabase(q.ctx, s.Name, s.IfExists); err != nil {
			return nil, err
		}
		return &Result{}, nil
	case "TABLE", "VIEW":
		db := q.defaultDB(s.Database)
		if v, err := cat.View(db, s.Name); err == nil {
			return &Result{}, cat.DropView(q.ctx, v.Database, v.Name, s.IfExists)
		}
		t, err := cat.Table(db, s.Name)
		if err != nil {
			if s.IfExists && (cherr.Is(err, cherr.UnknownTable) || cherr.Is(err, cherr.UnknownDatabase)) {
				return &Result{}, nil
			}
			return nil, err
		}
		if s.Kind == "VIEW" {
			return nil, cherr.New(cherr.UnknownTable, "%s is not a view", t.FullName())
		}
		return &Result{}, q.dropTable(t)
	}
	return nil, cherr.New(cherr.NotImplemented, "DROP %s is not supported", s.Kind)
}

func (q *query) dropTable(t *catalog.Table) error {
	if err := q.cat.DropTable(q.ctx, t.Database, t.Name, false); err != nil {
		return err
	}
	return q.dropPhysical(t)
}

// dropPhysical drops t's physical table once its catalog entry is gone. A
// deletion the store defers (storage.ErrDeleteDeferred: DynamoDB's 24 hour
// restriction on the source of a global table, or a non-waiting store) is
// success for the client: the table no longer exists logically and the
// reconciler deletes the physical table once the service allows it.
func (q *query) dropPhysical(t *catalog.Table) error {
	err := q.e.cfg.Store.DropTable(q.ctx, t)
	if errors.Is(err, storage.ErrDeleteDeferred) {
		q.e.logger().Info("physical table deletion deferred; the reconciler completes it",
			"table", t.FullName(), "physical", t.PhysicalName, "reason", err)
		return nil
	}
	return err
}

func (q *query) runTruncate(s *sqlparse.TruncateStmt) (*Result, error) {
	t, err := q.lookupTable(s.Database, s.Name)
	if err != nil {
		if s.IfExists && cherr.Is(err, cherr.UnknownTable) {
			return &Result{}, nil
		}
		return nil, err
	}
	// Record the reset in the catalog before any row is deleted, so every
	// region applies the same reset epoch to its copy of a regional table
	// (DESIGN.md §5 "Resets"); the store then truncates the stored entry.
	if err := q.cat.UpdateTable(q.ctx, catalog.MarkTruncated(t, time.Now())); err != nil {
		return nil, err
	}
	stored, err := q.cat.Table(t.Database, t.Name)
	if err != nil {
		return nil, err
	}
	return &Result{}, q.e.cfg.Store.TruncateTable(q.ctx, stored)
}

func (q *query) runAlter(s *sqlparse.AlterStmt) (*Result, error) {
	orig, err := q.lookupTable(s.Database, s.Name)
	if err != nil {
		return nil, err
	}
	t := *orig
	t.Columns = append([]catalog.Column(nil), orig.Columns...)
	changed := false
	isKey := map[string]bool{}
	for _, n := range t.OrderBy {
		isKey[n] = true
	}
	for _, cmd := range s.Commands {
		switch cmd.Kind {
		case "ADD COLUMN":
			if cmd.Column == nil {
				return nil, cherr.New(cherr.LogicalError, "ADD COLUMN without a column definition")
			}
			if t.ColumnIndex(cmd.Column.Name) >= 0 {
				if cmd.IfNotExists {
					continue
				}
				return nil, cherr.New(duplicateColumn, "Cannot add column %s: column with this name already exists", cmd.Column.Name)
			}
			col, err := q.buildColumn(*cmd.Column, chtype.Parse)
			if err != nil {
				return nil, err
			}
			if t.Engine == catalog.EngineAggregatingMergeTree && col.Type.Kind != chtype.AggregateFunction && col.Type.Kind != chtype.SimpleAggregateFunction && col.DefaultKind != "ALIAS" {
				return nil, cherr.New(cherr.NotImplemented, "AggregatingMergeTree column %s must be AggregateFunction or SimpleAggregateFunction", col.Name)
			}
			pos := len(t.Columns)
			switch {
			case cmd.First:
				pos = 0
			case cmd.After != "":
				i := t.ColumnIndex(cmd.After)
				if i < 0 {
					return nil, cherr.New(cherr.NoSuchColumnInTable, "Wrong column name. Cannot find column %s to insert after", cmd.After)
				}
				pos = i + 1
			}
			t.Columns = append(t.Columns[:pos], append([]catalog.Column{col}, t.Columns[pos:]...)...)
			changed = true
		case "DROP COLUMN":
			i := t.ColumnIndex(cmd.ColumnName)
			if i < 0 {
				if cmd.IfExists {
					continue
				}
				return nil, cherr.New(cherr.NoSuchColumnInTable, "Wrong column name. Cannot find column %s to drop", cmd.ColumnName)
			}
			if isKey[cmd.ColumnName] || cmd.ColumnName == t.VersionColumn || containsStr(t.SumColumns, cmd.ColumnName) {
				return nil, cherr.New(cherr.BadArguments, "Trying to ALTER DROP key column %s from key expression", cmd.ColumnName)
			}
			t.Columns = append(t.Columns[:i], t.Columns[i+1:]...)
			changed = true
		case "MODIFY COLUMN", "COMMENT COLUMN":
			name := cmd.ColumnName
			if cmd.Column != nil {
				name = cmd.Column.Name
			}
			i := t.ColumnIndex(name)
			if i < 0 {
				if cmd.IfExists {
					continue
				}
				return nil, cherr.New(cherr.NoSuchColumnInTable, "Wrong column name. Cannot find column %s to modify", name)
			}
			if cmd.Kind == "COMMENT COLUMN" || (cmd.Column != nil && cmd.Column.Type == "" && cmd.Column.Default == nil && cmd.Column.DefaultText == "") {
				if cmd.Column != nil {
					t.Columns[i].Comment = cmd.Column.Comment
				}
				changed = true
				continue
			}
			def := *cmd.Column
			parse := chtype.Parse
			if def.Type == "" {
				def.Type = t.Columns[i].TypeText
				parse = chtype.ParseStored
			}
			if def.Default == nil && def.DefaultText == "" && def.DefaultKind == "" {
				def.DefaultKind, def.DefaultText = t.Columns[i].DefaultKind, t.Columns[i].DefaultExpr
			}
			if def.Comment == "" {
				def.Comment = t.Columns[i].Comment
			}
			col, err := q.buildColumn(def, parse)
			if err != nil {
				return nil, err
			}
			if (isKey[name] || name == t.VersionColumn) && !col.Type.Equal(t.Columns[i].Type) {
				return nil, cherr.New(cherr.BadArguments, "ALTER of key column %s is forbidden", name)
			}
			t.Columns[i] = col
			changed = true
		case "DELETE":
			if changed {
				return nil, cherr.New(cherr.NotImplemented, "mixing DELETE with other ALTER commands is not supported")
			}
			return q.runDelete(s.Database, s.Name, cmd.Where)
		case "UPDATE":
			return nil, cherr.New(cherr.NotImplemented, "ALTER TABLE ... UPDATE is not supported")
		case "MODIFY SETTING":
			settings := make(map[string]string, len(t.Settings))
			for k, v := range t.Settings {
				settings[k] = v
			}
			for _, a := range cmd.Assignments {
				if strings.HasPrefix(a.Name, "dynamodb_") {
					return nil, cherr.New(cherr.NotImplemented, "setting %s cannot be changed after CREATE TABLE (it defines the DynamoDB layout)", a.Name)
				}
				v, err := settingValue(a.Value)
				if err != nil {
					return nil, err
				}
				settings[a.Name] = v
			}
			t.Settings = settings
			changed = true
		default:
			return nil, cherr.New(cherr.NotImplemented, "ALTER command %s is not supported", firstNonEmpty(cmd.Raw, cmd.Kind))
		}
	}
	if !changed {
		return &Result{}, nil
	}
	if err := q.checkDefaults(&t); err != nil {
		return nil, err
	}
	if err := q.cat.UpdateTable(q.ctx, &t); err != nil {
		return nil, err
	}
	return &Result{}, nil
}

func containsStr(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// runDelete deletes rows matching where (lightweight DELETE / ALTER DELETE).
func (q *query) runDelete(db, name string, where sqlparse.Expr) (*Result, error) {
	t, err := q.lookupTable(db, name)
	if err != nil {
		return nil, err
	}
	if where == nil {
		return nil, cherr.New(cherr.IncorrectQuery, "DELETE requires a WHERE clause")
	}
	src := q.storageSource(t, "")
	c := &compiler{q: q, env: &planEnv{}, sc: src.sc, used: map[int]bool{}, clause: "WHERE"}
	cond, err := c.compile(where)
	if err != nil {
		return nil, err
	}
	if err := src.finalize(c); err != nil {
		return nil, err
	}
	req := &storage.ScanRequest{Consistent: true}
	if req.Ranges, err = q.pushdown(c, src, where); err != nil {
		return nil, err
	}
	it, err := src.open(req)
	if err != nil {
		return nil, err
	}
	sit, _ := it.(*scanIter)
	fit := &filterIter{in: it, cond: cond}
	defer fit.close()
	var batch []storage.Row
	for {
		row, err := fit.next()
		if err != nil {
			if isEOF(err) {
				break
			}
			return nil, err
		}
		// Stores locate items by complete rows; computed columns carry the
		// type default the engine wrote for them.
		// Columns absent from the stored item (added by ALTER later) are
		// passed as the type's zero value, not the default the engine
		// evaluated for the WHERE clause (stores match an absent attribute
		// against any value).
		r := make(storage.Row, len(t.Columns))
		for i := range t.Columns {
			if k := t.Columns[i].DefaultKind; k == "ALIAS" || k == "EPHEMERAL" {
				r[i] = zeroValue(t.Columns[i].Type)
			} else {
				r[i] = row[i]
			}
		}
		if sit != nil {
			for _, i := range sit.missing {
				if i >= 0 && i < len(r) {
					r[i] = zeroValue(t.Columns[i].Type)
				}
			}
		}
		batch = append(batch, r)
		if len(batch) >= insertBatch {
			if err := q.e.cfg.Store.DeleteRows(q.ctx, t, batch); err != nil {
				return nil, err
			}
			batch = nil
		}
	}
	if len(batch) > 0 {
		if err := q.e.cfg.Store.DeleteRows(q.ctx, t, batch); err != nil {
			return nil, err
		}
	}
	return &Result{}, nil
}

// describeColumns is the DESCRIBE result header.
var describeColumns = []format.Column{
	{Name: "name", Type: chtype.TString}, {Name: "type", Type: chtype.TString},
	{Name: "default_type", Type: chtype.TString}, {Name: "default_expression", Type: chtype.TString},
	{Name: "comment", Type: chtype.TString}, {Name: "codec_expression", Type: chtype.TString},
	{Name: "ttl_expression", Type: chtype.TString},
}
