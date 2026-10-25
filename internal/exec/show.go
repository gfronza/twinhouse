package exec

import (
	"regexp"
	"sort"
	"strings"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
	"dynamodb-eventstream/internal/format"
	"dynamodb-eventstream/internal/sqlparse"
)

func stringResult(name string, values []string, formatName string) *Result {
	rows := make([][]any, len(values))
	for i, v := range values {
		rows[i] = []any{v}
	}
	return &Result{Columns: []format.Column{{Name: name, Type: chtype.TString}}, Rows: rows, Format: formatName}
}

// likeFilter returns a predicate implementing [NOT] LIKE 'pattern'.
func likeFilter(pattern string, not bool) (func(string) bool, error) {
	if pattern == "" {
		return func(string) bool { return true }, nil
	}
	re, err := regexp.Compile(likePattern(pattern, false))
	if err != nil {
		return nil, cherr.Wrap(cherr.BadArguments, err, "bad LIKE pattern")
	}
	return func(s string) bool { return re.MatchString(s) != not }, nil
}

func (q *query) runShow(s *sqlparse.ShowStmt) (*Result, error) {
	switch s.Kind {
	case "DATABASES":
		match, err := likeFilter(s.Like, s.NotLike)
		if err != nil {
			return nil, err
		}
		var names []string
		for _, n := range q.databaseNames() {
			if match(n) {
				names = append(names, n)
			}
		}
		return stringResult("name", names, s.Format), nil
	case "TABLES":
		db := q.defaultDB(s.Database)
		if db != "system" {
			if _, err := q.cat.Database(db); err != nil {
				return nil, err
			}
		}
		match, err := likeFilter(s.Like, s.NotLike)
		if err != nil {
			return nil, err
		}
		var names []string
		if db == "system" {
			names = []string{"columns", "databases", "numbers", "numbers_mt", "one", "tables"}
		}
		for _, t := range q.cat.Tables(db) {
			names = append(names, t.Name)
		}
		for _, v := range q.cat.Views(db) {
			names = append(names, v.Name)
		}
		sort.Strings(names)
		var out []string
		for _, n := range names {
			if match(n) {
				out = append(out, n)
			}
		}
		return stringResult("name", out, s.Format), nil
	case "CREATE TABLE", "CREATE VIEW", "CREATE":
		db := q.defaultDB(s.Database)
		if v, err := q.cat.View(db, s.Name); err == nil {
			return stringResult("statement", []string{v.DDL}, s.Format), nil
		}
		t, err := q.cat.Table(db, s.Name)
		if err != nil {
			return nil, err
		}
		return stringResult("statement", []string{showCreateTable(t)}, s.Format), nil
	case "COLUMNS":
		t, err := q.lookupTable(s.Database, s.Name)
		if err != nil {
			return nil, err
		}
		match, err := likeFilter(s.Like, s.NotLike)
		if err != nil {
			return nil, err
		}
		cols := []format.Column{
			{Name: "field", Type: chtype.TString}, {Name: "type", Type: chtype.TString}, {Name: "null", Type: chtype.TString},
			{Name: "key", Type: chtype.TString}, {Name: "default", Type: chtype.NewNullable(chtype.TString)}, {Name: "extra", Type: chtype.TString},
		}
		var rows [][]any
		for _, c := range t.Columns {
			if !match(c.Name) {
				continue
			}
			null := "NO"
			if c.Type.IsNullable() {
				null = "YES"
			}
			key := ""
			if containsStr(t.OrderBy, c.Name) {
				key = "PRI SOR"
			}
			var def any
			if c.DefaultExpr != "" {
				def = c.DefaultExpr
			}
			rows = append(rows, []any{c.Name, c.TypeText, null, key, def, ""})
		}
		sort.SliceStable(rows, func(i, j int) bool { return rows[i][0].(string) < rows[j][0].(string) })
		return &Result{Columns: cols, Rows: rows, Format: s.Format}, nil
	}
	return nil, cherr.New(cherr.NotImplemented, "SHOW %s is not supported", s.Kind)
}

// showCreateTable renders a table definition the way ClickHouse prints it.
func showCreateTable(t *catalog.Table) string {
	var b strings.Builder
	b.WriteString("CREATE TABLE " + t.Database + "." + t.Name + "\n(\n")
	for i, c := range t.Columns {
		b.WriteString("    `" + c.Name + "` " + c.TypeText)
		if c.DefaultKind != "" && c.DefaultExpr != "" {
			b.WriteString(" " + c.DefaultKind + " " + c.DefaultExpr)
		}
		if c.Comment != "" {
			b.WriteString(" COMMENT " + quoteString(c.Comment))
		}
		if c.Codec != "" {
			b.WriteString(" CODEC(" + c.Codec + ")")
		}
		if i < len(t.Columns)-1 {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}
	b.WriteString(")\nENGINE = " + t.Engine)
	if len(t.EngineArgs) > 0 {
		b.WriteString("(" + strings.Join(t.EngineArgs, ", ") + ")")
	}
	mergeTree := isMergeTreeFamily(t.Engine)
	if mergeTree && t.PartitionBy != "" {
		b.WriteString("\nPARTITION BY " + t.PartitionBy)
	}
	if mergeTree && len(t.PrimaryKey) > 0 {
		b.WriteString("\nPRIMARY KEY " + orderByText(t.PrimaryKey))
	}
	if mergeTree {
		b.WriteString("\nORDER BY " + orderByText(t.OrderBy))
	}
	// Settings in declaration order; MergeTree tables also show the implicit
	// index_granularity = 8192 like ClickHouse.
	parts := append([]string(nil), t.SettingsSQL...)
	if parts == nil && len(t.Settings) > 0 {
		keys := make([]string, 0, len(t.Settings))
		for k := range t.Settings {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			parts = append(parts, k+" = "+quoteString(t.Settings[k]))
		}
	}
	if _, ok := t.Settings["index_granularity"]; mergeTree && !ok {
		parts = append(parts, "index_granularity = 8192")
	}
	if len(parts) > 0 {
		b.WriteString("\nSETTINGS " + strings.Join(parts, ", "))
	}
	if t.Comment != "" {
		b.WriteString("\nCOMMENT " + quoteString(t.Comment))
	}
	return b.String()
}

// quoteString renders s as a ClickHouse single-quoted literal.
func quoteString(s string) string {
	var b strings.Builder
	b.WriteByte('\'')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\'':
			b.WriteString(`\'`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\t':
			b.WriteString(`\t`)
		case '\r':
			b.WriteString(`\r`)
		case 0:
			b.WriteString(`\0`)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('\'')
	return b.String()
}

func (q *query) runDescribe(s *sqlparse.DescribeStmt) (*Result, error) {
	db := q.defaultDB(s.Database)
	var rows [][]any
	if t, err := q.cat.Table(db, s.Name); err == nil {
		for _, c := range t.Columns {
			rows = append(rows, []any{c.Name, c.TypeText, c.DefaultKind, c.DefaultExpr, c.Comment, c.Codec, ""})
		}
		return &Result{Columns: describeColumns, Rows: rows, Format: s.Format}, nil
	}
	if db == "system" {
		src, err := q.systemTable(db, s.Name, "")
		if err != nil {
			return nil, err
		}
		for _, c := range src.sc.cols {
			rows = append(rows, []any{c.name, c.t.String(), "", "", "", "", ""})
		}
		return &Result{Columns: describeColumns, Rows: rows, Format: s.Format}, nil
	}
	v, err := q.cat.View(db, s.Name)
	if err != nil {
		if _, derr := q.cat.Database(db); derr != nil {
			return nil, derr
		}
		return nil, cherr.New(cherr.UnknownTable, "Table %s.%s does not exist", db, s.Name)
	}
	var cols []format.Column
	if v.Materialized {
		t, err := q.cat.Table(v.ToDatabase, v.ToTable)
		if err != nil {
			return nil, err
		}
		for _, c := range t.Columns {
			cols = append(cols, format.Column{Name: c.Name, Type: c.Type})
		}
	} else {
		sel, err := parseSelect(v.SelectSQL)
		if err != nil {
			return nil, err
		}
		rel, err := q.withDB(v.Database).planSelect(sel, &planEnv{})
		if err != nil {
			return nil, err
		}
		cols = rel.cols
	}
	for _, c := range cols {
		rows = append(rows, []any{c.Name, c.Type.String(), "", "", "", "", ""})
	}
	return &Result{Columns: describeColumns, Rows: rows, Format: s.Format}, nil
}

func (q *query) runExists(s *sqlparse.ExistsStmt) (*Result, error) {
	cat := q.cat
	found := false
	switch s.Kind {
	case "DATABASE":
		_, err := cat.Database(s.Name)
		found = err == nil || s.Name == "system"
	default:
		db := q.defaultDB(s.Database)
		_, terr := cat.Table(db, s.Name)
		_, verr := cat.View(db, s.Name)
		switch s.Kind {
		case "VIEW":
			found = verr == nil
		default:
			found = terr == nil || verr == nil
		}
	}
	return &Result{
		Columns: []format.Column{{Name: "result", Type: chtype.TUInt8}},
		Rows:    [][]any{{boolNum(found)}},
		Format:  s.Format,
	}, nil
}
