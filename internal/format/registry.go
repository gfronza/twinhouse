package format

import (
	"io"
	"strconv"
	"strings"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// Canonical format names.
const (
	fNative                          = "Native"
	fTabSeparated                    = "TabSeparated"
	fTabSeparatedRaw                 = "TabSeparatedRaw"
	fTabSeparatedWithNames           = "TabSeparatedWithNames"
	fTabSeparatedWithNamesAndTypes   = "TabSeparatedWithNamesAndTypes"
	fTabSeparatedRawWithNames        = "TabSeparatedRawWithNames"
	fTabSeparatedRawWithNamesAndType = "TabSeparatedRawWithNamesAndTypes"
	fCSV                             = "CSV"
	fCSVWithNames                    = "CSVWithNames"
	fCSVWithNamesAndTypes            = "CSVWithNamesAndTypes"
	fJSON                            = "JSON"
	fJSONCompact                     = "JSONCompact"
	fJSONStrings                     = "JSONStrings"
	fJSONCompactStrings              = "JSONCompactStrings"
	fJSONEachRow                     = "JSONEachRow"
	fJSONCompactEachRow              = "JSONCompactEachRow"
	fJSONStringsEachRow              = "JSONStringsEachRow"
	fJSONCompactStringsEachRow       = "JSONCompactStringsEachRow"
	fValues                          = "Values"
	fPretty                          = "Pretty"
	fPrettyCompact                   = "PrettyCompact"
	fNull                            = "Null"
)

const (
	ctTSV     = "text/tab-separated-values; charset=UTF-8"
	ctJSON    = "application/json; charset=UTF-8"
	ctNDJSON  = "application/x-ndjson; charset=UTF-8"
	ctPlain   = "text/plain; charset=UTF-8"
	ctBinary  = "application/octet-stream"
	ctCSV     = "text/csv; charset=UTF-8; header=absent"
	ctCSVHead = "text/csv; charset=UTF-8; header=present"
)

type formatInfo struct {
	name        string
	contentType string
	output      bool
	input       bool
}

var formats = []formatInfo{
	{fNative, ctBinary, true, true},
	{fTabSeparated, ctTSV, true, true},
	{fTabSeparatedRaw, ctTSV, true, true},
	{fTabSeparatedWithNames, ctTSV, true, true},
	{fTabSeparatedWithNamesAndTypes, ctTSV, true, true},
	{fTabSeparatedRawWithNames, ctTSV, true, true},
	{fTabSeparatedRawWithNamesAndType, ctTSV, true, true},
	{fCSV, ctCSV, true, true},
	{fCSVWithNames, ctCSVHead, true, true},
	{fCSVWithNamesAndTypes, ctCSVHead, true, true},
	{fJSON, ctJSON, true, false},
	{fJSONCompact, ctJSON, true, false},
	{fJSONStrings, ctJSON, true, false},
	{fJSONCompactStrings, ctJSON, true, false},
	{fJSONEachRow, ctNDJSON, true, true},
	{fJSONCompactEachRow, ctPlain, true, true},
	{fJSONStringsEachRow, ctNDJSON, true, false},
	{fJSONCompactStringsEachRow, ctPlain, true, false},
	{fValues, ctPlain, true, true},
	{fPretty, ctPlain, true, false},
	{fPrettyCompact, ctPlain, true, false},
	{fNull, ctPlain, true, false},
}

var aliases = map[string]string{
	"tsv":                     fTabSeparated,
	"tsvraw":                  fTabSeparatedRaw,
	"raw":                     fTabSeparatedRaw,
	"tsvwithnames":            fTabSeparatedWithNames,
	"tsvwithnamesandtypes":    fTabSeparatedWithNamesAndTypes,
	"tsvrawwithnames":         fTabSeparatedRawWithNames,
	"tsvrawwithnamesandtypes": fTabSeparatedRawWithNamesAndType,
	"rawwithnames":            fTabSeparatedRawWithNames,
	"rawwithnamesandtypes":    fTabSeparatedRawWithNamesAndType,
	"jsonlines":               fJSONEachRow,
	"ndjson":                  fJSONEachRow,
}

var byLower = func() map[string]*formatInfo {
	m := make(map[string]*formatInfo, len(formats)+len(aliases))
	for i := range formats {
		m[strings.ToLower(formats[i].name)] = &formats[i]
	}
	for a, canon := range aliases {
		m[a] = m[strings.ToLower(canon)]
	}
	return m
}()

func lookup(name string) *formatInfo { return byLower[strings.ToLower(name)] }

func canonical(name string) (string, bool) {
	if f := lookup(name); f != nil {
		return f.name, true
	}
	return "", false
}

func contentType(name string) string {
	if f := lookup(name); f != nil {
		return f.contentType
	}
	return ctPlain
}

// unsupportedFormats are ClickHouse formats this server knows by name but
// does not implement: they answer NOT_IMPLEMENTED (48) instead of
// UNKNOWN_FORMAT (73), like the other documented limitations (DESIGN §3).
var unsupportedFormats = func() map[string]bool {
	names := []string{
		"RowBinary", "RowBinaryWithNames", "RowBinaryWithNamesAndTypes", "RowBinaryWithDefaults",
		"Parquet", "ParquetMetadata", "Arrow", "ArrowStream", "ORC", "Avro", "AvroConfluent",
		"Protobuf", "ProtobufSingle", "ProtobufList", "MsgPack", "CapnProto", "BSONEachRow", "Npy",
		"TSKV", "XML", "Markdown", "MySQLWire", "PostgreSQLWire", "MySQLDump", "SQLInsert",
		"Template", "TemplateIgnoreSpaces", "Regexp", "RawBLOB", "LineAsString",
		"LineAsStringWithNames", "LineAsStringWithNamesAndTypes", "One", "Form", "Prometheus",
		"CustomSeparated", "CustomSeparatedWithNames", "CustomSeparatedWithNamesAndTypes",
		"CustomSeparatedIgnoreSpaces", "CustomSeparatedIgnoreSpacesWithNames",
		"CustomSeparatedIgnoreSpacesWithNamesAndTypes", "Vertical", "PrettyNoEscapes",
		"PrettyMonoBlock", "PrettyNoEscapesMonoBlock", "PrettyCompactNoEscapes",
		"PrettyCompactMonoBlock", "PrettyCompactNoEscapesMonoBlock", "PrettySpace",
		"PrettySpaceNoEscapes", "PrettySpaceMonoBlock", "PrettySpaceNoEscapesMonoBlock",
		"JSONColumns", "JSONCompactColumns", "JSONColumnsWithMetadata", "JSONObjectEachRow",
		"JSONAsString", "JSONAsObject", "JSONEachRowWithProgress", "JSONStringsEachRowWithProgress",
		"JSONCompactWithNames", "JSONCompactWithNamesAndTypes", "JSONCompactEachRowWithNames",
		"JSONCompactEachRowWithNamesAndTypes", "JSONCompactStringsEachRowWithNames",
		"JSONCompactStringsEachRowWithNamesAndTypes", "JSONCompactEachRowWithProgress",
		"JSONCompactStringsEachRowWithProgress", "HiveText", "DWARF", "ODBCDriver2", "PrettyJSONEachRow", "PrettyJSONLines", "PrettyNDJSON",
		"Hash",
	}
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[strings.ToLower(n)] = true
	}
	return m
}()

// Unsupported reports whether name is a ClickHouse format that this server
// recognises but does not implement (NOT_IMPLEMENTED rather than
// UNKNOWN_FORMAT). Supported formats and unknown names return false.
func Unsupported(name string) bool {
	return lookup(name) == nil && unsupportedFormats[strings.ToLower(name)]
}

func unknownFormat(name string) error {
	if unsupportedFormats[strings.ToLower(name)] {
		return cherr.New(cherr.NotImplemented, "Format %s is not supported by this server", name)
	}
	return cherr.New(cherr.UnknownFormat, "Unknown format %s", name)
}

func newEncoder(name string, w io.Writer, cols []Column, opts Options) (Encoder, error) {
	f := lookup(name)
	if f == nil || !f.output {
		return nil, unknownFormat(name)
	}
	if f.name == fNull {
		return nullEncoder{}, nil
	}
	for _, c := range cols {
		if err := checkOutputType(c.Type); err != nil {
			return nil, columnErr(err, c.Name)
		}
	}
	if f.name == fNative {
		return newNativeEncoder(w, cols, opts)
	}
	dt, err := dateTimeOutputMode(opts.Settings)
	if err != nil {
		return nil, err
	}
	enc, err := newTextEncoder(f, w, cols, opts)
	if err != nil {
		return nil, err
	}
	return wrapDateTimeOutput(enc, cols, opts, dt), nil
}

func newTextEncoder(f *formatInfo, w io.Writer, cols []Column, opts Options) (Encoder, error) {
	switch f.name {
	case fTabSeparated, fTabSeparatedRaw, fTabSeparatedWithNames, fTabSeparatedWithNamesAndTypes,
		fTabSeparatedRawWithNames, fTabSeparatedRawWithNamesAndType:
		return newTSVEncoder(f.name, w, cols, opts), nil
	case fCSV, fCSVWithNames, fCSVWithNamesAndTypes:
		return newCSVEncoder(f.name, w, cols, opts)
	case fJSON, fJSONCompact, fJSONStrings, fJSONCompactStrings:
		return newJSONEncoder(f.name, w, cols, opts), nil
	case fJSONEachRow, fJSONCompactEachRow, fJSONStringsEachRow, fJSONCompactStringsEachRow:
		return newJSONEachRowEncoder(f.name, w, cols, opts), nil
	case fValues:
		return newValuesEncoder(w, cols, opts), nil
	case fPretty, fPrettyCompact:
		return newPrettyEncoder(f.name, w, cols, opts), nil
	}
	return nil, unknownFormat(f.name)
}

func newDecoder(name string, r io.Reader, cols []Column, opts Options) (Decoder, error) {
	f := lookup(name)
	if f == nil {
		return nil, unknownFormat(name)
	}
	if !f.input {
		return nil, cherr.New(cherr.NotImplemented, "Format %s is not supported as an input format", f.name)
	}
	for _, c := range cols {
		if err := checkInputType(c.Type); err != nil {
			return nil, columnErr(err, c.Name)
		}
	}
	if _, err := dateTimeInputMode(opts.Settings); err != nil {
		return nil, err
	}
	switch f.name {
	case fNative:
		return newNativeDecoder(r, cols, opts), nil
	case fTabSeparated, fTabSeparatedRaw, fTabSeparatedWithNames, fTabSeparatedWithNamesAndTypes,
		fTabSeparatedRawWithNames, fTabSeparatedRawWithNamesAndType:
		return newTSVDecoder(f.name, r, cols, opts), nil
	case fCSV, fCSVWithNames, fCSVWithNamesAndTypes:
		return newCSVDecoder(f.name, r, cols, opts)
	case fJSONEachRow:
		return newJSONEachRowDecoder(r, cols, opts), nil
	case fJSONCompactEachRow:
		return newJSONCompactEachRowDecoder(r, cols, opts), nil
	case fValues:
		return newValuesDecoder(r, cols, opts), nil
	}
	return nil, unknownFormat(name)
}

// settingBool reads a boolean setting ("1"/"0"/"true"/"false"), with a default.
func settingBool(s map[string]string, key string, def bool) bool {
	v, ok := s[key]
	if !ok {
		return def
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off", "":
		return false
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return n != 0
	}
	return def
}

// settingInt reads an integer setting with a default.
func settingInt(s map[string]string, key string, def int64) int64 {
	v, ok := s[key]
	if !ok {
		return def
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil {
		return def
	}
	return n
}

type nullEncoder struct{}

func (nullEncoder) WriteRows([][]any) error { return nil }
func (nullEncoder) Finish(Stats) error      { return nil }
