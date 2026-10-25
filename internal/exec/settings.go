package exec

import (
	"strconv"
	"strings"
	"time"

	"dynamodb-eventstream/internal/cherr"
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
