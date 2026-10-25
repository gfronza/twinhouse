package format

import (
	"strconv"

	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
)

// inputSettings are the input_format_* settings shared by text decoders.
type inputSettings struct {
	skipUnknown       bool     // input_format_skip_unknown_fields (ClickHouse 25.8 default: 1)
	nullAsDefault     bool     // input_format_null_as_default (default 1)
	csvEmptyAsDefault bool     // input_format_csv_empty_as_default (default 1)
	tsvEmptyAsDefault bool     // input_format_tsv_empty_as_default (default 0)
	useHeader         bool     // input_format_with_names_use_header (default 1)
	useTypes          bool     // input_format_with_types_use_header (default 1)
	numbersAsStrings  bool     // input_format_json_read_numbers_as_strings (default 1)
	boolsAsNumbers    bool     // input_format_json_read_bools_as_numbers (default 1)
	dtIn              dtInMode // date_time_input_format (validated by newDecoder)
	tz                string   // server timezone, for DateTime columns without one
	tsvNull           string   // format_tsv_null_representation (default \N)
	csvNull           string   // format_csv_null_representation (default \N)
}

func newInputSettings(opts Options) inputSettings {
	s := opts.Settings
	dtIn, _ := dateTimeInputMode(s)
	return inputSettings{
		skipUnknown:       settingBool(s, "input_format_skip_unknown_fields", true),
		nullAsDefault:     settingBool(s, "input_format_null_as_default", true),
		csvEmptyAsDefault: settingBool(s, "input_format_csv_empty_as_default", true),
		tsvEmptyAsDefault: settingBool(s, "input_format_tsv_empty_as_default", false),
		useHeader:         settingBool(s, "input_format_with_names_use_header", true),
		useTypes:          settingBool(s, "input_format_with_types_use_header", true),
		numbersAsStrings:  settingBool(s, "input_format_json_read_numbers_as_strings", true),
		boolsAsNumbers:    settingBool(s, "input_format_json_read_bools_as_numbers", true),
		dtIn:              dtIn,
		tz:                opts.Timezone,
		tsvNull:           tsvNullRepr(opts),
		csvNull:           csvNullRepr(opts),
	}
}

// null resolves a NULL input value for a column of type t.
func (s *inputSettings) null(t chtype.Type) (any, error) {
	if isNullableType(t) {
		return nil, nil
	}
	if s.nullAsDefault {
		return Missing, nil
	}
	return nil, cherr.New(cherr.TypeMismatch, "Cannot insert NULL value into a column of type '%s'", t.String())
}

// isNullableType reports whether t accepts NULL (Nullable, LowCardinality(Nullable), Nothing).
func isNullableType(t chtype.Type) bool {
	for {
		switch t.Kind {
		case chtype.Nullable, chtype.Nothing:
			return true
		case chtype.LowCardinality, chtype.SimpleAggregateFunction:
			if t.Elem == nil {
				return false
			}
			t = *t.Elem
		default:
			return false
		}
	}
}

func isNumericKind(k chtype.Kind) bool {
	switch k {
	case chtype.UInt8, chtype.UInt16, chtype.UInt32, chtype.UInt64,
		chtype.Int8, chtype.Int16, chtype.Int32, chtype.Int64,
		chtype.Float32, chtype.Float64:
		return true
	}
	return false
}

func isStringKind(k chtype.Kind) bool { return k == chtype.String || k == chtype.FixedString }

// rowErr annotates a value error with its position, like ClickHouse's
// "(at row N)" suffix, keeping the error code.
func rowErr(err error, row int, col string) error {
	ce := cherr.As(err)
	code := ce.Code
	if code == cherr.StdException || code == cherr.LogicalError {
		code = cherr.CannotParseText
	}
	msg := ce.Msg
	if col != "" {
		msg = "column " + col + ": " + msg
	}
	return &cherr.Error{Code: code, Msg: msg + ": (at row " + strconv.Itoa(row) + ")", Cause: ce.Cause}
}

// headerMap maps header column names to target column indexes (-1 = skip).
func headerMap(names []string, cols []Column, s *inputSettings, format string) ([]int, error) {
	byName := make(map[string]int, len(cols))
	for i, c := range cols {
		byName[c.Name] = i
	}
	out := make([]int, len(names))
	seen := make(map[int]bool, len(names))
	for i, n := range names {
		ti, ok := byName[n]
		switch {
		case !ok && !s.skipUnknown:
			return nil, cherr.New(cherr.IncorrectData, "Unknown field found while parsing %s format: %s", format, n)
		case !ok:
			out[i] = -1
		case seen[ti]:
			return nil, cherr.New(cherr.IncorrectData, "Duplicate field %s found while parsing %s format", n, format)
		default:
			seen[ti] = true
			out[i] = ti
		}
	}
	return out, nil
}
