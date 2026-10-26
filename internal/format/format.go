// Package format implements ClickHouse input/output formats.
//
// API CONTRACT (constructors are stubs until implemented by the format owner).
//
// Output formats: Native, TabSeparated (TSV), TabSeparatedRaw (TSVRaw),
// TabSeparatedWithNames, TabSeparatedWithNamesAndTypes, CSV, CSVWithNames,
// CSVWithNamesAndTypes, JSON, JSONCompact, JSONEachRow, JSONCompactEachRow,
// JSONStringsEachRow, Values, Pretty/PrettyCompact (best-effort), Null.
//
// Input formats: Native, Values, TabSeparated (+WithNames[AndTypes]), CSV
// (+WithNames), JSONEachRow, JSONCompactEachRow.
package format

import (
	"io"
	"time"

	"github.com/gfronza/twinhouse/internal/chtype"
)

// Column is a result/input column.
type Column struct {
	Name string
	Type chtype.Type
}

// Options carries client/session settings that affect encoding.
type Options struct {
	// ClientProtocolVersion is the HTTP client_protocol_version parameter
	// (clickhouse-go sends its TCP protocol revision). It controls Native
	// encoding details, e.g. the per-column custom-serialization byte that
	// ClickHouse writes for revisions >= 54454.
	ClientProtocolVersion uint64
	// Settings are request settings (URL params / SETTINGS clause), e.g.
	// output_format_json_quote_64bit_integers, input_format_skip_unknown_fields,
	// format_csv_delimiter, date_time_input_format, input_format_null_as_default.
	Settings map[string]string
	// Timezone is the server timezone used for DateTime rendering without explicit TZ.
	Timezone string
}

// Stats is appended by formats that print statistics (JSON family).
type Stats struct {
	Rows               uint64
	RowsBeforeLimit    uint64
	HasRowsBeforeLimit bool
	Elapsed            time.Duration
	RowsRead           uint64
	BytesRead          uint64
}

// Encoder writes a result set. Usage: NewEncoder → WriteRows (0+ times) → Finish.
// Encoders write the header (names/types/JSON meta) lazily on first
// WriteRows or on Finish, so empty results are still well-formed.
type Encoder interface {
	// WriteRows writes rows; each row has len(cols) values in canonical form.
	// Native writes one block per call (callers batch ~65k rows per call).
	WriteRows(rows [][]any) error
	// Finish writes trailers (e.g. JSON "rows"/"statistics") and flushes.
	Finish(stats Stats) error
}

// Decoder reads rows for INSERT. Each row returned has len(cols) values in
// the order of cols passed to NewDecoder, canonicalized for each column type.
// Formats that carry their own header (Native, *WithNames, JSONEachRow) map
// fields to cols by name; fields missing from the input are returned as
// Missing (so the engine can apply DEFAULT expressions).
type Decoder interface {
	Next() ([]any, error) // io.EOF at end
}

// Missing marks a value absent from the input (engine applies default).
var Missing = missing{}

type missing struct{}

// Canonical returns the canonical format name for a case-insensitive name or
// alias (TSV→TabSeparated, TSVWithNames→TabSeparatedWithNames, ...), and
// whether it is known.
func Canonical(name string) (string, bool) { return canonical(name) }

// ContentType returns the HTTP Content-Type ClickHouse uses for a format
// (e.g. "text/tab-separated-values; charset=UTF-8", "application/json; charset=UTF-8",
// "application/octet-stream" for Native).
func ContentType(name string) string { return contentType(name) }

// NewEncoder returns an encoder for an output format (canonical or alias name).
// Unknown format -> *cherr.Error UNKNOWN_FORMAT.
func NewEncoder(name string, w io.Writer, cols []Column, opts Options) (Encoder, error) {
	return newEncoder(name, w, cols, opts)
}

// NewDecoder returns a decoder for an input format.
func NewDecoder(name string, r io.Reader, cols []Column, opts Options) (Decoder, error) {
	return newDecoder(name, r, cols, opts)
}
