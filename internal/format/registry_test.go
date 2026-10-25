package format

import (
	"bytes"
	"io"
	"testing"

	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
)

func TestCanonical(t *testing.T) {
	cases := map[string]string{
		"Native": "Native", "native": "Native", "TSV": "TabSeparated", "tsv": "TabSeparated",
		"TabSeparated": "TabSeparated", "TSVRaw": "TabSeparatedRaw", "Raw": "TabSeparatedRaw",
		"TSVWithNames": "TabSeparatedWithNames", "TSVWithNamesAndTypes": "TabSeparatedWithNamesAndTypes",
		"tabseparatedwithnamesandtypes": "TabSeparatedWithNamesAndTypes", "csv": "CSV",
		"CSVWithNames": "CSVWithNames", "JSON": "JSON", "json": "JSON", "JSONLines": "JSONEachRow",
		"NDJSON": "JSONEachRow", "JSONCompactEachRow": "JSONCompactEachRow", "Values": "Values",
		"PrettyCompact": "PrettyCompact", "Null": "Null", "JSONStringsEachRow": "JSONStringsEachRow",
	}
	for in, want := range cases {
		got, ok := Canonical(in)
		if !ok || got != want {
			t.Errorf("Canonical(%q) = %q, %v; want %q", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "Parquet", "RowBinary", "JSONX"} {
		if _, ok := Canonical(in); ok {
			t.Errorf("Canonical(%q) unexpectedly known", in)
		}
	}
}

func TestContentType(t *testing.T) {
	// Values observed from ClickHouse 25.8 responses.
	cases := map[string]string{
		"Native":                "application/octet-stream",
		"TSV":                   "text/tab-separated-values; charset=UTF-8",
		"TabSeparatedWithNames": "text/tab-separated-values; charset=UTF-8",
		"CSV":                   "text/csv; charset=UTF-8; header=absent",
		"CSVWithNames":          "text/csv; charset=UTF-8; header=present",
		"JSON":                  "application/json; charset=UTF-8",
		"JSONCompact":           "application/json; charset=UTF-8",
		"JSONEachRow":           "application/x-ndjson; charset=UTF-8",
		"JSONStringsEachRow":    "application/x-ndjson; charset=UTF-8",
		"JSONCompactEachRow":    "text/plain; charset=UTF-8",
		"Values":                "text/plain; charset=UTF-8",
		"PrettyCompact":         "text/plain; charset=UTF-8",
		"Null":                  "text/plain; charset=UTF-8",
		"Unknown":               "text/plain; charset=UTF-8",
	}
	for in, want := range cases {
		if got := ContentType(in); got != want {
			t.Errorf("ContentType(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNewEncoderUnknown(t *testing.T) {
	_, err := NewEncoder("NoSuchFormat", io.Discard, nil, Options{})
	if !cherr.Is(err, cherr.UnknownFormat) {
		t.Fatalf("got %v", err)
	}
	if ce := cherr.As(err); ce.HTTPStatus() != 404 {
		t.Fatalf("status %d", ce.HTTPStatus())
	}
}

// Formats ClickHouse has but this server does not implement are a
// documented limitation: NOT_IMPLEMENTED (48), not UNKNOWN_FORMAT (73).
func TestNewEncoderKnownUnsupported(t *testing.T) {
	for _, f := range []string{"RowBinary", "rowbinarywithnamesandtypes", "Parquet", "Arrow", "ORC", "Avro", "MsgPack", "XML", "Markdown", "TSKV", "Vertical"} {
		_, err := NewEncoder(f, io.Discard, nil, Options{})
		if !cherr.Is(err, cherr.NotImplemented) {
			t.Errorf("%s: got %v", f, err)
		}
		if !Unsupported(f) {
			t.Errorf("Unsupported(%q) = false", f)
		}
	}
	for _, f := range []string{"TSV", "JSONEachRow", "NoSuchFormat", ""} {
		if Unsupported(f) {
			t.Errorf("Unsupported(%q) = true", f)
		}
	}
}

func TestNullEncoder(t *testing.T) {
	var buf bytes.Buffer
	enc, err := NewEncoder("Null", &buf, []Column{col("a", "UInt8")}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.WriteRows([][]any{{uint64(1)}}); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finish(Stats{}); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatalf("Null wrote %q", buf.String())
	}
}

func TestSettingBool(t *testing.T) {
	s := map[string]string{"a": "1", "b": "0", "c": "true", "d": "False", "e": "junk", "f": "2"}
	for k, want := range map[string]bool{"a": true, "b": false, "c": true, "d": false, "e": true, "f": true, "missing": true} {
		if got := settingBool(s, k, true); got != want {
			t.Errorf("settingBool(%s) = %v, want %v", k, got, want)
		}
	}
}

func TestJSONStringEscaping(t *testing.T) {
	st := jsonStyle{escapeSlash: true}
	got := string(st.appendJSONString(nil, "a\"b\\c/\n\t\x01\x1f\x7f  é\xff"))
	want := `"a\"b\\c\/\n\t\u0001\u001F` + "\x7f" + `  é` + "\xff" + `"`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	st.escapeSlash = false
	if got := string(st.appendJSONString(nil, "a/b")); got != `"a/b"` {
		t.Fatalf("got %q", got)
	}
}

func TestDisplayTypeTimezone(t *testing.T) {
	typ := chtype.MustParse("Array(Nullable(DateTime64(3)))")
	dt := displayType(typ, "Europe/Rome")
	if dt.Elem.Elem.TZ != "Europe/Rome" || typ.Elem.Elem.TZ != "" {
		t.Fatalf("displayType must set TZ on a copy: %+v / %+v", dt, typ)
	}
	explicit := chtype.MustParse("DateTime('UTC')")
	if displayType(explicit, "Europe/Rome").TZ != "UTC" {
		t.Fatal("explicit timezone must win")
	}
}
