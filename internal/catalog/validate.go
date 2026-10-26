package catalog

import (
	"strings"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/storage/keyenc"
)

// ReservedAttributes are the DynamoDB item attributes used by the storage
// layout; columns cannot have these names.
var ReservedAttributes = []string{"_pk", "_sk", "_rid", "_ts", "_origin", "_mv"}

// MaxPhysicalNameLen leaves room for the ".members" side-table suffix within
// DynamoDB's 255-character table-name limit.
const MaxPhysicalNameLen = 255 - len(".members")

// reservedDatabases always exist (served by the engine) and cannot be created.
var reservedDatabases = map[string]bool{"system": true, "information_schema": true, "INFORMATION_SCHEMA": true}

// ValidateName checks that a database or table name maps to a DynamoDB table
// name: only ASCII letters, digits, '_' and '-'.
func ValidateName(kind, name string) error {
	if name == "" {
		return cherr.New(cherr.BadArguments, "%s name is empty", kind)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return cherr.New(cherr.BadArguments,
				"%s name '%s' cannot be mapped to a DynamoDB table name (allowed characters: a-z, A-Z, 0-9, '_', '-')", kind, name)
		}
	}
	return nil
}

// validateTable checks t and fills derived fields: column TypeText/Type,
// PartitionKey/SortKey defaults, VersionColumn default, Replication default.
// parse is used for columns that only carry TypeText.
func validateTable(t *Table, parse func(string) (chtype.Type, error)) error {
	if err := ValidateName("Database", t.Database); err != nil {
		return err
	}
	if err := ValidateName("Table", t.Name); err != nil {
		return err
	}
	if len(t.Columns) == 0 {
		return cherr.New(cherr.EmptyDataPassed, "table %s must have at least one column", t.FullName())
	}
	cols := make(map[string]*Column, len(t.Columns))
	for i := range t.Columns {
		c := &t.Columns[i]
		if err := validateColumnName(c.Name); err != nil {
			return err
		}
		if cols[c.Name] != nil {
			return cherr.New(cherr.IllegalColumn, "column %s specified more than once", c.Name)
		}
		cols[c.Name] = c
		if c.Type.Kind == chtype.Invalid {
			if c.TypeText == "" {
				return cherr.New(cherr.UnknownType, "column %s has no type", c.Name)
			}
			typ, err := parse(c.TypeText)
			if err != nil {
				return cherr.Wrap(cherr.UnknownType, err, "column %s: cannot parse type %s", c.Name, c.TypeText)
			}
			c.Type = typ
		}
		if c.TypeText == "" {
			c.TypeText = c.Type.String()
		}
		if c.TypeText == "" || c.TypeText == "?" {
			return cherr.New(cherr.UnknownType, "column %s has no type text", c.Name)
		}
	}

	switch t.Engine {
	case EngineMergeTree, EngineReplacingMergeTree, EngineAggregatingMergeTree, EngineSummingMergeTree:
	case EngineMemory, EngineLog:
		if len(t.OrderBy) > 0 {
			return cherr.New(cherr.BadArguments, "engine %s does not support ORDER BY", t.Engine)
		}
	case "":
		return cherr.New(cherr.UnknownStorage, "table %s has no engine", t.FullName())
	default:
		return cherr.New(cherr.UnknownStorage, "unknown table engine %s", t.Engine)
	}

	if err := validateKeys(t, cols); err != nil {
		return err
	}

	// Engine-specific columns.
	for i := range t.Columns {
		c := &t.Columns[i]
		switch c.Type.Kind {
		case chtype.AggregateFunction:
			if t.Engine != EngineAggregatingMergeTree {
				return cherr.New(cherr.NotImplemented,
					"AggregateFunction column %s is only supported in AggregatingMergeTree tables", c.Name)
			}
			if isKey(t, c.Name) {
				return cherr.New(cherr.IllegalColumn, "AggregateFunction column %s cannot be part of the sorting key", c.Name)
			}
			spec, err := ResolveAggregate(c.Type)
			if err != nil {
				return err
			}
			if spec.IsSet() {
				if err := keyenc.CheckType(spec.MemberType()); err != nil {
					return cherr.Wrap(cherr.IllegalTypeOfArgument, err, "column %s: unsupported set member type", c.Name)
				}
			}
		case chtype.SimpleAggregateFunction:
			fn, err := ResolveSimpleAggregate(c.Type)
			if err != nil {
				return err
			}
			if fn == AggGroupUniqArray {
				if err := keyenc.CheckType(*base(*c.Type.Elem).Elem); err != nil {
					return cherr.Wrap(cherr.IllegalTypeOfArgument, err, "column %s: unsupported set member type", c.Name)
				}
			}
		}
	}

	switch t.Engine {
	case EngineReplacingMergeTree:
		if t.VersionColumn == "" && len(t.EngineArgs) > 0 {
			t.VersionColumn = unquoteIdent(t.EngineArgs[0])
		}
		if t.VersionColumn != "" {
			c := cols[t.VersionColumn]
			if c == nil {
				return cherr.New(cherr.NoSuchColumnInTable, "version column %s does not exist in table %s", t.VersionColumn, t.FullName())
			}
			if !versionKind(base(c.Type).Kind) {
				return cherr.New(cherr.BadArguments,
					"the version column %s of ReplacingMergeTree must be of type UInt*, Int*, Date, DateTime or DateTime64, got %s", c.Name, c.TypeText)
			}
		}
		if col := t.IsDeletedColumn(); col != "" {
			c := cols[col]
			if c == nil {
				return cherr.New(cherr.NoSuchColumnInTable, "is_deleted column %s does not exist in table %s", col, t.FullName())
			}
			if c.Type.Kind != chtype.UInt8 {
				return cherr.New(cherr.BadArguments, "the is_deleted column %s of ReplacingMergeTree must be UInt8, got %s", col, c.TypeText)
			}
			if t.VersionColumn == "" {
				return cherr.New(cherr.BadArguments, "ReplacingMergeTree is_deleted column requires a version column")
			}
		}
	case EngineSummingMergeTree:
		if t.SumColumns == nil && len(t.EngineArgs) > 0 {
			t.SumColumns = parseSumColumns(t.EngineArgs)
		}
		for _, name := range t.SumColumns {
			c := cols[name]
			if c == nil {
				return cherr.New(cherr.NoSuchColumnInTable, "column %s listed in SummingMergeTree does not exist in table %s", name, t.FullName())
			}
			if isKey(t, name) {
				return cherr.New(cherr.BadArguments, "column %s listed in SummingMergeTree is part of the sorting key", name)
			}
			if !numericKind(base(c.Type).Kind) {
				return cherr.New(cherr.BadArguments, "column %s listed in SummingMergeTree must be numeric, got %s", name, c.TypeText)
			}
		}
	}

	switch t.Replication {
	case "":
		t.Replication = DefaultReplication(t.Engine)
	case ReplicationGlobal, ReplicationRegional:
	default:
		return cherr.New(cherr.BadArguments, "dynamodb_replication must be 'global' or 'regional', got '%s'", t.Replication)
	}
	return nil
}

// validateKeys checks OrderBy/PartitionKey/SortKey and derives defaults:
// PartitionKey from SETTINGS dynamodb_partition_key, else the first ORDER BY
// column; SortKey the remaining ORDER BY columns.
func validateKeys(t *Table, cols map[string]*Column) error {
	seen := map[string]bool{}
	for _, name := range t.OrderBy {
		c := cols[name]
		if c == nil {
			return cherr.New(cherr.UnknownIdentifier, "sorting key column %s does not exist in table %s", name, t.FullName())
		}
		if seen[name] {
			return cherr.New(cherr.BadArguments, "sorting key contains column %s more than once", name)
		}
		seen[name] = true
		if err := keyenc.CheckType(c.Type); err != nil {
			return cherr.Wrap(cherr.IllegalColumn, err, "sorting key column %s", name)
		}
	}
	if len(t.PartitionKey) == 0 && len(t.SortKey) == 0 && len(t.OrderBy) > 0 {
		if s, ok := t.Settings["dynamodb_partition_key"]; ok {
			t.PartitionKey = splitIdents(s)
		} else {
			t.PartitionKey = []string{t.OrderBy[0]}
		}
		if len(t.PartitionKey) > len(t.OrderBy) {
			return cherr.New(cherr.BadArguments, "dynamodb_partition_key must be a prefix of the sorting key")
		}
		t.SortKey = append([]string{}, t.OrderBy[len(t.PartitionKey):]...)
	}
	if t.PartitionKey == nil {
		t.PartitionKey = []string{}
	}
	if t.SortKey == nil {
		t.SortKey = []string{}
	}
	if len(t.PartitionKey)+len(t.SortKey) != len(t.OrderBy) {
		return cherr.New(cherr.BadArguments, "partition key and sort key must split the sorting key of %s", t.FullName())
	}
	for i, name := range t.PartitionKey {
		if t.OrderBy[i] != name {
			return cherr.New(cherr.BadArguments, "dynamodb_partition_key must be a prefix of the sorting key")
		}
	}
	for i, name := range t.SortKey {
		if t.OrderBy[len(t.PartitionKey)+i] != name {
			return cherr.New(cherr.BadArguments, "sort key must be the sorting key after the partition key")
		}
	}
	return nil
}

func validateColumnName(name string) error {
	if name == "" {
		return cherr.New(cherr.IllegalColumn, "column name is empty")
	}
	for _, r := range ReservedAttributes {
		if name == r {
			return cherr.New(cherr.IllegalColumn, "column name %s is reserved by the DynamoDB storage layout (reserved: %s)",
				name, strings.Join(ReservedAttributes, ", "))
		}
	}
	if strings.Contains(name, "#") {
		return cherr.New(cherr.IllegalColumn, "column name %s cannot contain '#' (used for aggregate state attributes)", name)
	}
	return nil
}

// DefaultReplication returns the default replication mode of an engine.
func DefaultReplication(engine string) string {
	switch engine {
	case EngineAggregatingMergeTree, EngineSummingMergeTree:
		return ReplicationRegional
	}
	return ReplicationGlobal
}

// IsDeletedColumn returns the ReplacingMergeTree(ver, is_deleted) column name
// ("" if none).
func (t *Table) IsDeletedColumn() string {
	if t.Engine != EngineReplacingMergeTree || len(t.EngineArgs) < 2 {
		return ""
	}
	return unquoteIdent(t.EngineArgs[1])
}

func isKey(t *Table, name string) bool {
	for _, k := range t.OrderBy {
		if k == name {
			return true
		}
	}
	return false
}

func versionKind(k chtype.Kind) bool {
	switch k {
	case chtype.UInt8, chtype.UInt16, chtype.UInt32, chtype.UInt64,
		chtype.Int8, chtype.Int16, chtype.Int32, chtype.Int64,
		chtype.Date, chtype.Date32, chtype.DateTime, chtype.DateTime64:
		return true
	}
	return false
}

// parseSumColumns accepts SummingMergeTree((a, b)) / SummingMergeTree(a, b).
func parseSumColumns(args []string) []string {
	var out []string
	for _, a := range args {
		a = strings.TrimSpace(a)
		a = strings.TrimSuffix(strings.TrimPrefix(a, "("), ")")
		out = append(out, splitIdents(a)...)
	}
	return out
}

func splitIdents(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = unquoteIdent(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func unquoteIdent(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && (s[0] == '`' && s[len(s)-1] == '`' || s[0] == '"' && s[len(s)-1] == '"') {
		return s[1 : len(s)-1]
	}
	return s
}
