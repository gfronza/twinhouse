package catalog

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gfronza/twinhouse/internal/chtype"
)

// JSON shapes of persisted entries. Column types are stored as their text and
// re-parsed on load, so the format is independent of chtype's Go structs.

type columnDef struct {
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	DefaultKind string   `json:"default_kind,omitempty"`
	DefaultExpr string   `json:"default_expr,omitempty"`
	Comment     string   `json:"comment,omitempty"`
	Codec       string   `json:"codec,omitempty"`
	PrevTypes   []string `json:"prev_types,omitempty"`
}

type tableDef struct {
	Database      string            `json:"database"`
	Name          string            `json:"name"`
	Columns       []columnDef       `json:"columns"`
	Engine        string            `json:"engine"`
	EngineArgs    []string          `json:"engine_args,omitempty"`
	OrderBy       []string          `json:"order_by"`
	PartitionKey  []string          `json:"partition_key"`
	SortKey       []string          `json:"sort_key"`
	VersionColumn string            `json:"version_column,omitempty"`
	SumColumns    []string          `json:"sum_columns,omitempty"`
	Replication   string            `json:"replication"`
	PhysicalName  string            `json:"physical_name"`
	PartitionBy   string            `json:"partition_by,omitempty"`
	PrimaryKey    []string          `json:"primary_key,omitempty"`
	Settings      map[string]string `json:"settings,omitempty"`
	SettingsSQL   []string          `json:"settings_sql,omitempty"`
	Comment       string            `json:"comment,omitempty"`
	DDL           string            `json:"ddl,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
	Version       int64             `json:"version"`
	HomeRegion    string            `json:"home_region,omitempty"`
	ResetAt       *time.Time        `json:"reset_at,omitempty"`
}

type viewDef struct {
	Database       string    `json:"database"`
	Name           string    `json:"name"`
	Materialized   bool      `json:"materialized"`
	ToDatabase     string    `json:"to_database,omitempty"`
	ToTable        string    `json:"to_table,omitempty"`
	SourceDatabase string    `json:"source_database,omitempty"`
	SourceTable    string    `json:"source_table,omitempty"`
	SelectSQL      string    `json:"select_sql"`
	DDL            string    `json:"ddl,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

type databaseDef struct {
	Name      string    `json:"name"`
	Engine    string    `json:"engine,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// EncodeTable renders t as its persisted JSON definition. Every column must
// carry TypeText (validated tables always do).
func EncodeTable(t *Table) ([]byte, error) {
	d := tableDef{
		Database: t.Database, Name: t.Name, Engine: t.Engine, EngineArgs: t.EngineArgs,
		OrderBy: nonNil(t.OrderBy), PartitionKey: nonNil(t.PartitionKey), SortKey: nonNil(t.SortKey),
		VersionColumn: t.VersionColumn, SumColumns: t.SumColumns, Replication: t.Replication,
		PhysicalName: t.PhysicalName, Settings: t.Settings, Comment: t.Comment, DDL: t.DDL,
		CreatedAt: t.CreatedAt.UTC(), Version: t.Version,
		PartitionBy: t.PartitionBy, PrimaryKey: t.PrimaryKey, SettingsSQL: t.SettingsSQL,
		HomeRegion: t.HomeRegion,
	}
	if !t.ResetAt.IsZero() {
		r := t.ResetAt.UTC()
		d.ResetAt = &r
	}
	for _, c := range t.Columns {
		if c.TypeText == "" {
			return nil, fmt.Errorf("catalog: column %s of %s has no type text", c.Name, t.FullName())
		}
		d.Columns = append(d.Columns, columnDef{
			Name: c.Name, Type: c.TypeText, DefaultKind: c.DefaultKind, DefaultExpr: c.DefaultExpr, Comment: c.Comment, Codec: c.Codec,
			PrevTypes: c.PrevTypes,
		})
	}
	return json.Marshal(d)
}

// DecodeTable parses a persisted table definition, re-parsing column types
// with parse (chtype.ParseStored when nil).
func DecodeTable(b []byte, parse func(string) (chtype.Type, error)) (*Table, error) {
	if parse == nil {
		parse = chtype.ParseStored
	}
	var d tableDef
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, fmt.Errorf("catalog: decode table: %w", err)
	}
	t := &Table{
		Database: d.Database, Name: d.Name, Engine: d.Engine, EngineArgs: d.EngineArgs,
		OrderBy: nonNil(d.OrderBy), PartitionKey: nonNil(d.PartitionKey), SortKey: nonNil(d.SortKey),
		VersionColumn: d.VersionColumn, SumColumns: d.SumColumns, Replication: d.Replication,
		PhysicalName: d.PhysicalName, Settings: d.Settings, Comment: d.Comment, DDL: d.DDL,
		CreatedAt: d.CreatedAt.UTC(), Version: d.Version,
		PartitionBy: d.PartitionBy, PrimaryKey: d.PrimaryKey, SettingsSQL: d.SettingsSQL,
		HomeRegion: d.HomeRegion,
	}
	if d.ResetAt != nil {
		t.ResetAt = d.ResetAt.UTC()
	}
	for _, c := range d.Columns {
		typ, err := parse(c.Type)
		if err != nil {
			return nil, fmt.Errorf("catalog: table %s.%s column %s: type %q: %w", d.Database, d.Name, c.Name, c.Type, err)
		}
		t.Columns = append(t.Columns, Column{
			Name: c.Name, Type: typ, TypeText: c.Type, DefaultKind: c.DefaultKind, DefaultExpr: c.DefaultExpr, Comment: c.Comment, Codec: c.Codec,
			PrevTypes: c.PrevTypes,
		})
	}
	return t, nil
}

// EncodeView / DecodeView convert a view to and from its persisted JSON.
func EncodeView(v *View) ([]byte, error) {
	return json.Marshal(viewDef{
		Database: v.Database, Name: v.Name, Materialized: v.Materialized,
		ToDatabase: v.ToDatabase, ToTable: v.ToTable, SourceDatabase: v.SourceDatabase, SourceTable: v.SourceTable,
		SelectSQL: v.SelectSQL, DDL: v.DDL, CreatedAt: v.CreatedAt.UTC(),
	})
}

func DecodeView(b []byte) (*View, error) {
	var d viewDef
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, fmt.Errorf("catalog: decode view: %w", err)
	}
	return &View{
		Database: d.Database, Name: d.Name, Materialized: d.Materialized,
		ToDatabase: d.ToDatabase, ToTable: d.ToTable, SourceDatabase: d.SourceDatabase, SourceTable: d.SourceTable,
		SelectSQL: d.SelectSQL, DDL: d.DDL, CreatedAt: d.CreatedAt.UTC(),
	}, nil
}

// EncodeDatabase / DecodeDatabase convert a database entry.
func EncodeDatabase(db *Database) ([]byte, error) {
	return json.Marshal(databaseDef{Name: db.Name, Engine: db.Engine, CreatedAt: db.CreatedAt.UTC()})
}

func DecodeDatabase(b []byte) (*Database, error) {
	var d databaseDef
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, fmt.Errorf("catalog: decode database: %w", err)
	}
	return &Database{Name: d.Name, Engine: d.Engine, CreatedAt: d.CreatedAt.UTC()}, nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
