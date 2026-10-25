package exec

import (
	"math"
	"sort"
	"strings"
	"time"

	"dynamodb-eventstream/internal/catalog"
	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
	"dynamodb-eventstream/internal/storage"
)

const zeroUUID = "00000000-0000-0000-0000-000000000000"

type sysCol struct {
	name string
	t    chtype.Type
}

var (
	tStrings     = chtype.NewArray(chtype.TString)
	tNullUInt64  = chtype.NewNullable(chtype.TUInt64)
	tNullString  = chtype.NewNullable(chtype.TString)
	sysTablesDef = []sysCol{
		{"database", chtype.TString}, {"name", chtype.TString}, {"uuid", chtype.TUUID}, {"engine", chtype.TString},
		{"is_temporary", chtype.TUInt8}, {"data_paths", tStrings}, {"metadata_path", chtype.TString},
		{"metadata_modification_time", chtype.TDateTime}, {"dependencies_database", tStrings},
		{"dependencies_table", tStrings}, {"create_table_query", chtype.TString}, {"engine_full", chtype.TString},
		{"as_select", chtype.TString}, {"partition_key", chtype.TString}, {"sorting_key", chtype.TString},
		{"primary_key", chtype.TString}, {"total_rows", tNullUInt64}, {"total_bytes", tNullUInt64},
		{"comment", chtype.TString}, {"has_own_data", chtype.TUInt8},
	}
	sysColumnsDef = []sysCol{
		{"database", chtype.TString}, {"table", chtype.TString}, {"name", chtype.TString}, {"type", chtype.TString},
		{"position", chtype.TUInt64}, {"default_kind", chtype.TString}, {"default_expression", chtype.TString},
		{"data_compressed_bytes", chtype.TUInt64}, {"data_uncompressed_bytes", chtype.TUInt64}, {"marks_bytes", chtype.TUInt64},
		{"comment", chtype.TString}, {"is_in_partition_key", chtype.TUInt8}, {"is_in_sorting_key", chtype.TUInt8},
		{"is_in_primary_key", chtype.TUInt8}, {"is_in_sampling_key", chtype.TUInt8}, {"compression_codec", chtype.TString},
		{"character_octet_length", tNullUInt64}, {"numeric_precision", tNullUInt64}, {"numeric_precision_radix", tNullUInt64},
		{"numeric_scale", tNullUInt64}, {"datetime_precision", tNullUInt64}, {"serialization_hint", tNullString},
	}
	sysDatabasesDef = []sysCol{
		{"name", chtype.TString}, {"engine", chtype.TString}, {"data_path", chtype.TString}, {"metadata_path", chtype.TString},
		{"uuid", chtype.TUUID}, {"engine_full", chtype.TString}, {"comment", chtype.TString},
	}
)

// systemTable plans system.* tables.
func (q *query) systemTable(db, name, alias string) (*source, error) {
	if db != "system" {
		return nil, cherr.New(cherr.UnknownTable, "Table %s.%s does not exist", db, name)
	}
	qual := firstNonEmpty(alias, name)
	switch name {
	case "one":
		s := oneSource()
		s.sc.cols[0].quals = []string{qual, "system.one"}
		return s, nil
	case "numbers", "numbers_mt":
		return q.numbersSource(0, math.MaxUint64, qual), nil
	case "tables":
		return staticSource(sysTablesDef, qual, q.systemTablesRows), nil
	case "columns":
		return staticSource(sysColumnsDef, qual, q.systemColumnsRows), nil
	case "databases":
		return staticSource(sysDatabasesDef, qual, q.systemDatabasesRows), nil
	}
	return nil, cherr.New(cherr.UnknownTable, "Table system.%s does not exist", name)
}

func staticSource(def []sysCol, qual string, rows func() [][]any) *source {
	sc := &scope{cols: make([]scopeCol, len(def))}
	for i, c := range def {
		sc.cols[i] = scopeCol{name: c.name, quals: []string{qual, "system." + qual}, t: c.t}
	}
	return &source{sc: sc, open: func(*storage.ScanRequest) (rowIter, error) {
		return &sliceIter{rows: rows()}, nil
	}}
}

func (q *query) databaseNames() []string {
	seen := map[string]bool{"system": true, "default": true}
	names := []string{"default", "system"}
	for _, d := range q.cat.Databases() {
		if !seen[d.Name] {
			seen[d.Name] = true
			names = append(names, d.Name)
		}
	}
	sort.Strings(names)
	return names
}

func (q *query) systemDatabasesRows() [][]any {
	var rows [][]any
	for _, n := range q.databaseNames() {
		engine := "Atomic"
		if n == "system" {
			engine = "Atomic"
		}
		rows = append(rows, []any{n, engine, "", "", zeroUUID, engine, ""})
	}
	return rows
}

// tableEngineFull renders the ENGINE clause as ClickHouse shows it.
func tableEngineFull(t *catalog.Table) string {
	var b strings.Builder
	b.WriteString(t.Engine)
	if len(t.EngineArgs) > 0 {
		b.WriteString("(" + strings.Join(t.EngineArgs, ", ") + ")")
	}
	if len(t.OrderBy) > 0 || isMergeTreeFamily(t.Engine) {
		b.WriteString(" ORDER BY " + orderByText(t.OrderBy))
	}
	return b.String()
}

func orderByText(cols []string) string {
	switch len(cols) {
	case 0:
		return "tuple()"
	case 1:
		return cols[0]
	}
	return "(" + strings.Join(cols, ", ") + ")"
}

func isMergeTreeFamily(engine string) bool {
	return strings.HasSuffix(engine, "MergeTree")
}

func (q *query) systemTablesRows() [][]any {
	var rows [][]any
	for _, db := range q.databaseNames() {
		if db == "system" {
			continue
		}
		for _, t := range q.cat.Tables(db) {
			key := strings.Join(t.OrderBy, ", ")
			rows = append(rows, []any{t.Database, t.Name, zeroUUID, t.Engine, uint64(0), []any{}, "",
				ts(t.CreatedAt), []any{}, []any{}, t.DDL, tableEngineFull(t), "", "", key, key, nil, nil, t.Comment, uint64(1)})
		}
		for _, v := range q.cat.Views(db) {
			engine := "View"
			if v.Materialized {
				engine = "MaterializedView"
			}
			rows = append(rows, []any{v.Database, v.Name, zeroUUID, engine, uint64(0), []any{}, "",
				ts(v.CreatedAt), []any{}, []any{}, v.DDL, engine, v.SelectSQL, "", "", "", nil, nil, "", uint64(0)})
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i][0] != rows[j][0] {
			return rows[i][0].(string) < rows[j][0].(string)
		}
		return rows[i][1].(string) < rows[j][1].(string)
	})
	return rows
}

func ts(t time.Time) time.Time {
	if t.IsZero() {
		return chtype.Epoch
	}
	return t.UTC().Truncate(time.Second)
}

func (q *query) systemColumnsRows() [][]any {
	var rows [][]any
	for _, db := range q.databaseNames() {
		if db == "system" {
			continue
		}
		for _, t := range q.cat.Tables(db) {
			in := func(list []string, n string) uint64 {
				for _, x := range list {
					if x == n {
						return 1
					}
				}
				return 0
			}
			for i, c := range t.Columns {
				var dtp any
				if u := c.Type.Unwrap(); u.Kind == chtype.DateTime64 {
					dtp = uint64(u.Precision)
				} else if u.Kind == chtype.DateTime {
					dtp = uint64(0)
				}
				rows = append(rows, []any{t.Database, t.Name, c.Name, c.TypeText, uint64(i + 1), c.DefaultKind, c.DefaultExpr,
					uint64(0), uint64(0), uint64(0), c.Comment, uint64(0), in(t.OrderBy, c.Name),
					in(t.OrderBy, c.Name), uint64(0), c.Codec, nil, nil, nil, nil, dtp, nil})
			}
		}
	}
	return rows
}
