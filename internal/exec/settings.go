package exec

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/sqlparse"
)

// unsupportedSettings are ClickHouse settings that change query results
// but that the engine does not implement. A non-default value is rejected
// with NOT_IMPLEMENTED rather than silently ignored (which would return
// different rows or values than ClickHouse). The function reports whether
// a value is equivalent to ClickHouse's default.
var unsupportedSettings = map[string]func(string) bool{
	"limit":                              isZeroSetting,
	"offset":                             isZeroSetting,
	"transform_null_in":                  isFalseSetting,
	"aggregate_functions_null_for_empty": isFalseSetting,
	"prefer_column_name_to_alias":        isFalseSetting,
	"cast_keep_nullable":                 isFalseSetting,
	"any_join_distinct_right_table_keys": isFalseSetting,
	"final":                              isFalseSetting,
	"data_type_default_nullable":         isFalseSetting,
	"group_by_use_nulls":                 isFalseSetting,
	"join_default_strictness": func(v string) bool {
		return v == "" || strings.EqualFold(v, "ALL")
	},
	"count_distinct_implementation": func(v string) bool {
		return v == "" || v == "uniqExact"
	},
}

func isFalseSetting(v string) bool {
	switch strings.ToLower(v) {
	case "", "0", "false":
		return true
	}
	return false
}

func isZeroSetting(v string) bool {
	if v == "" {
		return true
	}
	n, err := strconv.ParseFloat(v, 64)
	return err == nil && n == 0
}

// checkSetting validates one setting. clause is true for SETTINGS clauses
// and SET statements, where (like ClickHouse) an unknown name is an error;
// request (URL) settings may carry client-specific names.
func (q *query) checkSetting(name, value string, clause bool) error {
	if clause && !knownSettingNames[name] && !strings.HasPrefix(name, "SQL_") {
		return cherr.New(cherr.UnknownSetting, "Setting %s is neither a builtin setting nor started with the prefix 'SQL_' registered for user-defined settings", name)
	}
	if name == maxMemoryUsageSetting {
		return q.checkMemoryLimit(value)
	}
	if sizeSettings[name] {
		_, err := parseSizeSetting(value)
		return err
	}
	if isDefault, ok := unsupportedSettings[name]; ok && !isDefault(value) {
		return cherr.New(cherr.NotImplemented, "setting %s = %s is not supported", name, value)
	}
	if name == "session_timezone" && value != "" {
		if _, err := time.LoadLocation(value); err != nil {
			return cherr.New(cherr.BadArguments, "Invalid time zone: %s", value)
		}
	}
	return nil
}

// timezoneName is the query's time zone: session_timezone, else the
// server's.
func (q *query) timezoneName() string {
	if q.tzName != "" {
		return q.tzName
	}
	return q.e.cfg.Timezone
}

// maxMemoryUsageSetting is the per-query memory limit setting (bytes, 0 =
// unlimited).
const maxMemoryUsageSetting = "max_memory_usage"

// sizeSettings are the byte-size settings, read like ClickHouse reads a
// UInt64 setting. Only max_memory_usage has an effect.
var sizeSettings = map[string]bool{
	maxMemoryUsageSetting:                true,
	"max_memory_usage_for_user":          true,
	"max_memory_usage_for_all_queries":   true,
	"max_bytes_before_external_sort":     true,
	"max_bytes_before_external_group_by": true,
}

// resetsSetting reports whether st sets a size setting to DEFAULT (the
// keyword, in any case), which drops the setting so the server default
// applies.
func resetsSetting(st sqlparse.Setting) bool {
	id, ok := st.Value.(*sqlparse.Ident)
	return ok && sizeSettings[st.Name] && len(id.Parts) == 1 && strings.EqualFold(id.Parts[0], "DEFAULT")
}

// maxMemoryUsage returns the query's memory limit: the setting if present,
// else the server default.
func (q *query) maxMemoryUsage() int64 {
	v, ok := q.settings[maxMemoryUsageSetting]
	if !ok {
		return q.memDefault
	}
	n, err := parseSizeSetting(v)
	if err != nil {
		return q.memDefault
	}
	return int64(min(n, math.MaxInt64))
}

// checkMemoryLimit validates a max_memory_usage value. When the server sets
// a limit, a client may only lower it: a larger value, or 0 (unlimited),
// is rejected with ClickHouse's wording for a max constraint.
func (q *query) checkMemoryLimit(value string) error {
	n, err := parseSizeSetting(value)
	if err != nil {
		return err
	}
	if d := q.memDefault; d > 0 && (n == 0 || n > uint64(d)) {
		return cherr.New(cherr.SettingConstraintViolation, "Setting %s shouldn't be greater than %d", maxMemoryUsageSetting, d)
	}
	return nil
}

// clauseValue renders a SETTINGS clause or SET value as setting text. A
// size setting literal converts to UInt64 as ClickHouse converts it (1.5
// is 1, 1e9 is 1000000000, -1 fails); a string is parsed as text.
func clauseValue(st sqlparse.Setting) (string, error) {
	lit, ok := st.Value.(*sqlparse.Literal)
	if !sizeSettings[st.Name] || !ok {
		return settingValue(st.Value)
	}
	var n uint64
	var err error
	switch x := lit.Value.(type) {
	case uint64:
		n = x
	case int64:
		n, err = uint64Int(x)
	case bool:
		if x {
			n = 1
		}
	case float64:
		n, err = uint64Float(x)
	case nil:
		err = cherr.New(cherr.CannotConvertType, "Invalid value NULL of the setting, which needs unsigned long")
	default:
		return settingValue(st.Value)
	}
	return strconv.FormatUint(n, 10), err
}

func uint64Int(n int64) (uint64, error) {
	if n < 0 {
		return 0, cherr.New(cherr.CannotConvertType, "Field value %d is out of range of unsigned long type", n)
	}
	return uint64(n), nil
}

// uint64Float truncates a float setting value; 2^64 itself saturates, as
// in ClickHouse.
func uint64Float(f float64) (uint64, error) {
	switch {
	case math.IsNaN(f):
		return 0, cherr.New(cherr.CannotParseNumber, "Float setting value must be finite, got nan")
	case math.IsInf(f, 1):
		return 0, cherr.New(cherr.CannotParseNumber, "Float setting value must be finite, got inf")
	case math.IsInf(f, -1):
		return 0, cherr.New(cherr.CannotParseNumber, "Float setting value must be finite, got -inf")
	case f < 0 || f > 1<<64:
		return 0, cherr.New(cherr.CannotConvertType, "Cannot convert out of range floating point value to integer type")
	case f == 1<<64:
		return math.MaxUint64, nil
	}
	return uint64(f), nil
}

// sizeSuffixes are the multipliers parseSizeSetting accepts after the
// digits; an "i" after the letter makes it binary.
var sizeSuffixes = map[byte][2]uint64{
	'k': {1e3, 1 << 10}, 'K': {1e3, 1 << 10}, 'M': {1e6, 1 << 20}, 'G': {1e9, 1 << 30}, 'T': {1e12, 1 << 40},
}

// parseSizeSetting parses UInt64 setting text like ClickHouse's
// parseWithSizeSuffix: an optional '+', decimal digits (wrapping on
// overflow; none reads as 0), then an optional size suffix ("10G" is
// 10000000000, "10Gi" 10737418240), then the end.
func parseSizeSetting(s string) (uint64, error) {
	if s == "" {
		return 0, cherr.New(cherr.AttemptToReadAfterEOF, "Attempt to read after eof")
	}
	i, signed := 0, false
	switch s[0] {
	case '-':
		return 0, cherr.New(cherr.CannotParseNumber, "Unsigned type must not contain '-' symbol")
	case '+':
		i, signed = 1, true
	}
	start := i
	n := uint64(0)
	for ; i < len(s) && s[i] >= '0' && s[i] <= '9'; i++ {
		n = n*10 + uint64(s[i]-'0')
	}
	if signed && i == start {
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			return 0, cherr.New(cherr.CannotParseNumber, "Cannot parse number with multiple sign (+/-) characters")
		}
		return 0, cherr.New(cherr.CannotParseNumber, "Cannot parse number without any digits")
	}
	if i < len(s) {
		i, n = sizeSuffix(s, i, n)
	}
	if i < len(s) {
		return 0, cherr.New(cherr.CannotParseInputAssertionFailed, "Cannot parse input: expected 'eof' before: '%s'", s[i:])
	}
	return n, nil
}

// sizeSuffix applies the size suffix at s[i:], if any, to n.
func sizeSuffix(s string, i int, n uint64) (int, uint64) {
	m, ok := sizeSuffixes[s[i]]
	if !ok {
		return i, n
	}
	if i+1 < len(s) && s[i+1] == 'i' {
		return i + 2, n * m[1]
	}
	return i + 1, n * m[0]
}
