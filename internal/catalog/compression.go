package catalog

import (
	"sort"
	"strconv"
	"strings"

	"github.com/gfronza/twinhouse/internal/chtype"
)

// NormalizeCodec renders a column CODEC list the way ClickHouse reports it in
// system.columns.compression_codec / DESCRIBE codec_expression (without the
// CODEC(...) wrapper), filling in the implicit parameters ClickHouse adds:
//
//	Delta, Gorilla  -> Delta(w), Gorilla(w)  (w = value width in bytes of t)
//	ZSTD            -> ZSTD(1)
//	LZ4HC           -> LZ4HC(0)
//	FPC             -> FPC(12)
//
// Other codecs (LZ4, NONE, T64, DoubleDelta, ...) and codecs with explicit
// parameters are kept as written. codec may be the list itself
// ("Delta, ZSTD(1)") or wrapped ("CODEC(Delta, ZSTD(1))"); "" yields "".
// The codec has no storage effect; it is only reported back.
func NormalizeCodec(codec string, t chtype.Type) string {
	codec = strings.TrimSpace(codec)
	if len(codec) > 6 && strings.EqualFold(codec[:6], "CODEC(") && strings.HasSuffix(codec, ")") {
		codec = strings.TrimSpace(codec[6 : len(codec)-1])
	}
	if codec == "" {
		return ""
	}
	items := splitTopLevel(codec)
	out := make([]string, 0, len(items))
	for _, it := range items {
		it = strings.TrimSpace(it)
		if it == "" {
			continue
		}
		name, args := it, ""
		if i := strings.IndexByte(it, '('); i >= 0 && strings.HasSuffix(it, ")") {
			name = strings.TrimSpace(it[:i])
			parts := splitTopLevel(it[i+1 : len(it)-1])
			for k := range parts {
				parts[k] = strings.TrimSpace(parts[k])
			}
			args = strings.Join(parts, ", ")
		}
		if args == "" {
			switch name {
			case "Delta", "Gorilla":
				if w := codecWidth(t); w > 0 {
					args = strconv.Itoa(w)
				}
			case "ZSTD":
				args = "1"
			case "LZ4HC":
				args = "0"
			case "FPC":
				args = "12"
			}
		}
		if args != "" {
			out = append(out, name+"("+args+")")
		} else {
			out = append(out, name)
		}
	}
	return strings.Join(out, ", ")
}

// codecWidth is the fixed value width (bytes) ClickHouse uses as the default
// Delta/Gorilla parameter, or 0 when the type has none.
func codecWidth(t chtype.Type) int {
	for (t.Kind == chtype.Nullable || t.Kind == chtype.LowCardinality || t.Kind == chtype.Array ||
		t.Kind == chtype.SimpleAggregateFunction) && t.Elem != nil {
		t = *t.Elem
	}
	switch t.Kind {
	case chtype.Bool, chtype.UInt8, chtype.Int8, chtype.Enum8:
		return 1
	case chtype.UInt16, chtype.Int16, chtype.Enum16, chtype.Date:
		return 2
	case chtype.UInt32, chtype.Int32, chtype.Float32, chtype.Date32, chtype.DateTime:
		return 4
	case chtype.UInt64, chtype.Int64, chtype.Float64, chtype.DateTime64:
		return 8
	}
	return 0
}

// splitTopLevel splits s at commas outside parentheses and quotes.
func splitTopLevel(s string) []string {
	var parts []string
	depth, start := 0, 0
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			switch c {
			case '\\':
				i++
			case quote:
				quote = 0
			}
		case c == '\'' || c == '"' || c == '`':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == ',' && depth == 0:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return append(parts, s[start:])
}

// SettingSQL renders one CREATE TABLE setting as SHOW CREATE TABLE prints
// it: "name = 8192" for numeric literals, "name = 'text'" for strings.
func SettingSQL(name, value string, numeric bool) string {
	if numeric {
		return name + " = " + value
	}
	return name + " = " + quoteLiteral(value)
}

// ShowSettings returns the SETTINGS entries SHOW CREATE TABLE prints for t,
// in ClickHouse order: the declared settings (SettingsSQL, or for tables
// persisted without it the Settings map sorted by name, numeric-looking
// values unquoted), then, for MergeTree-family engines, the implicit
// "index_granularity = 8192" unless index_granularity was set explicitly.
func (t *Table) ShowSettings() []string {
	var out []string
	if t.SettingsSQL != nil {
		out = append(out, t.SettingsSQL...)
	} else if len(t.Settings) > 0 {
		keys := make([]string, 0, len(t.Settings))
		for k := range t.Settings {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v := t.Settings[k]
			_, err := strconv.ParseFloat(v, 64)
			out = append(out, SettingSQL(k, v, err == nil))
		}
	}
	switch t.Engine {
	case EngineMergeTree, EngineReplacingMergeTree, EngineAggregatingMergeTree, EngineSummingMergeTree:
		if _, ok := t.Settings["index_granularity"]; !ok {
			out = append(out, "index_granularity = 8192")
		}
	}
	return out
}

// quoteLiteral renders s as a ClickHouse single-quoted string literal.
func quoteLiteral(s string) string {
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

// SetSetting records a table setting (CREATE TABLE ... SETTINGS or ALTER
// TABLE ... MODIFY SETTING): Settings[name] = value, and the matching
// SettingsSQL entry is replaced in place, or appended when new. numeric
// selects how SHOW CREATE TABLE prints the literal (see SettingSQL). The map
// and slice are copied, never modified in place, so t may be a clone of a
// catalog snapshot entry.
func (t *Table) SetSetting(name, value string, numeric bool) {
	m := make(map[string]string, len(t.Settings)+1)
	for k, v := range t.Settings {
		m[k] = v
	}
	m[name] = value
	if t.SettingsSQL == nil && len(t.Settings) > 0 {
		// Legacy entry without SettingsSQL: rebuild it from the map first.
		t.SettingsSQL = []string{}
		keys := make([]string, 0, len(t.Settings))
		for k := range t.Settings {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			_, err := strconv.ParseFloat(t.Settings[k], 64)
			t.SettingsSQL = append(t.SettingsSQL, SettingSQL(k, t.Settings[k], err == nil))
		}
	}
	entry := SettingSQL(name, value, numeric)
	sqls := make([]string, 0, len(t.SettingsSQL)+1)
	replaced := false
	for _, s := range t.SettingsSQL {
		if strings.HasPrefix(s, name+" = ") {
			s, replaced = entry, true
		}
		sqls = append(sqls, s)
	}
	if !replaced {
		sqls = append(sqls, entry)
	}
	t.Settings, t.SettingsSQL = m, sqls
}
