package format

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	"dynamodb-eventstream/internal/cherr"
)

func tsUTC(s string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05.999999999", s)
	if err != nil {
		panic(err)
	}
	return t
}

// Expected outputs verified against ClickHouse 25.8.
func TestDateTimeOutputFormat(t *testing.T) {
	cols := []Column{col("t", "DateTime('UTC')"), col("t3", "DateTime64(3, 'UTC')"), col("a", "Array(DateTime('UTC'))"),
		col("d", "Date"), col("tk", "DateTime('Asia/Tokyo')")}
	row := []any{tsUTC("2024-01-02 03:04:05"), tsUTC("2024-01-02 03:04:05.5"), []any{tsUTC("2024-01-02 03:04:05")}, tsUTC("2024-01-02 00:00:00"), tsUTC("2024-01-01 18:04:05")}
	cases := []struct{ mode, format, want string }{
		{"iso", "TSV", "2024-01-02T03:04:05Z\t2024-01-02T03:04:05.500Z\t['2024-01-02T03:04:05Z']\t2024-01-02\t2024-01-01T18:04:05Z\n"},
		{"unix_timestamp", "TSV", "1704164645\t1704164645.500\t['1704164645']\t2024-01-02\t1704132245\n"},
		{"simple", "TSV", "2024-01-02 03:04:05\t2024-01-02 03:04:05.500\t['2024-01-02 03:04:05']\t2024-01-02\t2024-01-02 03:04:05\n"},
		{"iso", "JSONEachRow", `{"t":"2024-01-02T03:04:05Z","t3":"2024-01-02T03:04:05.500Z","a":["2024-01-02T03:04:05Z"],"d":"2024-01-02","tk":"2024-01-01T18:04:05Z"}` + "\n"},
		{"unix_timestamp", "JSONEachRow", `{"t":"1704164645","t3":"1704164645.500","a":["1704164645"],"d":"2024-01-02","tk":"1704132245"}` + "\n"},
		{"iso", "CSV", `"2024-01-02T03:04:05Z","2024-01-02T03:04:05.500Z","['2024-01-02T03:04:05Z']","2024-01-02","2024-01-01T18:04:05Z"` + "\n"},
		{"unix_timestamp", "Values", `('1704164645','1704164645.500',['1704164645'],'2024-01-02','1704132245')`},
	}
	for _, c := range cases {
		var buf bytes.Buffer
		enc, err := NewEncoder(c.format, &buf, cols, Options{Settings: map[string]string{"date_time_output_format": c.mode}, Timezone: "UTC"})
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.WriteRows([][]any{row}); err != nil {
			t.Fatal(err)
		}
		if err := enc.Finish(Stats{}); err != nil {
			t.Fatal(err)
		}
		if buf.String() != c.want {
			t.Errorf("%s/%s:\n got %q\nwant %q", c.mode, c.format, buf.String(), c.want)
		}
	}
}

func TestDateTimeOutputFormatDetails(t *testing.T) {
	cases := []struct {
		typ  string
		v    time.Time
		mode string
		want string
	}{
		{"DateTime64(3, 'UTC')", tsUTC("1960-01-02 03:04:05.5"), "unix_timestamp", "-315521754.500\n"},
		{"DateTime64(3, 'UTC')", tsUTC("1960-01-02 03:04:05.5"), "iso", "1960-01-02T03:04:05.500Z\n"},
		{"DateTime64(0, 'UTC')", tsUTC("2024-01-02 03:04:05"), "iso", "2024-01-02T03:04:05Z\n"},
		{"DateTime64(6, 'UTC')", tsUTC("2024-01-02 03:04:05.123456"), "iso", "2024-01-02T03:04:05.123456Z\n"},
		{"DateTime64(2)", tsUTC("2024-01-02 03:04:05"), "iso", "2024-01-02T03:04:05.00Z\n"},
		{"Nullable(DateTime)", tsUTC("2024-01-02 03:04:05"), "unix_timestamp", "1704164645\n"},
	}
	for _, c := range cases {
		var buf bytes.Buffer
		enc, err := NewEncoder("TSV", &buf, []Column{col("t", c.typ)}, Options{Settings: map[string]string{"date_time_output_format": c.mode}})
		if err != nil {
			t.Fatal(err)
		}
		enc.WriteRows([][]any{{c.v}})
		enc.Finish(Stats{})
		if buf.String() != c.want {
			t.Errorf("%s %s: got %q want %q", c.typ, c.mode, buf.String(), c.want)
		}
	}
	// Types header and JSON meta keep the real type.
	var buf bytes.Buffer
	enc, _ := NewEncoder("TSVWithNamesAndTypes", &buf, []Column{col("t", "DateTime('UTC')")}, Options{Settings: map[string]string{"date_time_output_format": "iso"}})
	enc.WriteRows([][]any{{tsUTC("2024-01-02 03:04:05")}})
	enc.Finish(Stats{})
	if want := "t\nDateTime(\\'UTC\\')\n2024-01-02T03:04:05Z\n"; buf.String() != want {
		t.Errorf("types header: got %q want %q", buf.String(), want)
	}
	for _, bad := range []string{"bogus", "ISO", ""} {
		_, err := NewEncoder("TSV", &buf, []Column{col("t", "DateTime")}, Options{Settings: map[string]string{"date_time_output_format": bad}})
		if !cherr.Is(err, cherr.BadArguments) {
			t.Errorf("date_time_output_format=%q: got %v", bad, err)
		}
	}
	// Native is binary and ignores the setting.
	if _, err := NewEncoder("Native", &buf, []Column{col("t", "DateTime")}, Options{Settings: map[string]string{"date_time_output_format": "iso"}}); err != nil {
		t.Errorf("Native: %v", err)
	}
}

// Expected outputs verified against ClickHouse 25.8.
func TestNullRepresentation(t *testing.T) {
	cols := []Column{col("n", "Nullable(UInt8)"), col("a", "Array(Nullable(UInt8))"), col("s", "String")}
	row := []any{nil, []any{nil, uint64(1)}, "a"}
	cases := []struct{ format, key, val, want string }{
		{"TSV", "format_tsv_null_representation", "NULL", "NULL\t[NULL,1]\ta\n"},
		{"TSVRaw", "format_tsv_null_representation", "NULL", "NULL\t[NULL,1]\ta\n"},
		{"TSV", "format_tsv_null_representation", "", "\t[NULL,1]\ta\n"},
		{"CSV", "format_tsv_null_representation", "NULL", "\\N,\"[NULL,1]\",\"a\"\n"},
		{"CSV", "format_csv_null_representation", "NULL", "NULL,\"[NULL,1]\",\"a\"\n"},
	}
	for _, c := range cases {
		var buf bytes.Buffer
		enc, err := NewEncoder(c.format, &buf, cols, Options{Settings: map[string]string{c.key: c.val}})
		if err != nil {
			t.Fatal(err)
		}
		enc.WriteRows([][]any{row})
		enc.Finish(Stats{})
		if buf.String() != c.want {
			t.Errorf("%s %s=%q: got %q want %q", c.format, c.key, c.val, buf.String(), c.want)
		}
	}
	// A top-level CSV tuple is flattened; its NULL elements use the setting.
	var buf bytes.Buffer
	enc, _ := NewEncoder("CSV", &buf, []Column{col("t", "Tuple(Nullable(UInt8), UInt8)")}, Options{Settings: map[string]string{"format_csv_null_representation": "NULL"}})
	enc.WriteRows([][]any{{[]any{nil, uint64(2)}}})
	enc.Finish(Stats{})
	if buf.String() != "NULL,2\n" {
		t.Errorf("csv tuple: got %q", buf.String())
	}
}

func decodeAll(t *testing.T, format, input string, cols []Column, settings map[string]string) ([][]any, error) {
	t.Helper()
	d, err := NewDecoder(format, strings.NewReader(input), cols, Options{Settings: settings})
	if err != nil {
		return nil, err
	}
	var out [][]any
	for {
		r, err := d.Next()
		if err != nil {
			if err.Error() == "EOF" {
				return out, nil
			}
			return out, err
		}
		out = append(out, r)
	}
}

// Expected values verified against ClickHouse 25.8 (INSERT with
// date_time_input_format=best_effort, then SELECT).
func TestBestEffortInput(t *testing.T) {
	cols := []Column{col("t", "DateTime"), col("t3", "DateTime64(3)")}
	be := map[string]string{"date_time_input_format": "best_effort"}
	cases := []struct {
		t, t3   string
		wt, wt3 string
	}{
		{"2024-05-01T10:00:03Z", "2024-05-01T10:00:03.250+02:00", "2024-05-01 10:00:03", "2024-05-01 08:00:03.25"},
		{"1714557604", "1714557604.5", "2024-05-01 10:00:04", "2024-05-01 10:00:04.5"},
		{"2024-05-01 10:00:04.999", "2024-05-01T10:00:03.123456Z", "2024-05-01 10:00:04", "2024-05-01 10:00:03.123"},
		{"2024-05-01", "20240501T100003", "2024-05-01 00:00:00", "2024-05-01 10:00:03"},
		{"01/05/2024 10:00", "2024-05-01T10:00:03-0530", "2024-05-01 10:00:00", "2024-05-01 15:30:03"},
		{"May 1 2024 10:00:03", "Wed, 01 May 2024 10:00:03 GMT", "2024-05-01 10:00:03", "2024-05-01 10:00:03"},
		{"2024-05-01T10:00:03+02", "1714557604123", "2024-05-01 08:00:03", "2024-05-01 10:00:04.123"},
		{"2024-05-01 10:00:03 +0200", "171455760", "2024-05-01 08:00:03", "1975-06-08 10:36:00"},
		{"2024-05-01T10:00", "202405", "2024-05-01 10:00:00", "2024-05-01 00:00:00"},
		{"2024-05-01 10:00:03 UTC", "2024", "2024-05-01 10:00:03", "2024-01-01 00:00:00"},
		{"2024-5-1 1:2:3", "20240501", "2024-05-01 01:02:03", "2024-05-01 00:00:00"},
		{"20240501100003", "2024/05/01 10:00:03", "2024-05-01 10:00:03", "2024-05-01 10:00:03"},
	}
	for _, c := range cases {
		for _, f := range []string{"JSONEachRow", "CSV", "TSV", "Values"} {
			var in string
			switch f {
			case "JSONEachRow":
				in = `{"t":"` + c.t + `","t3":"` + c.t3 + `"}`
			case "CSV":
				in = `"` + c.t + `","` + c.t3 + `"` + "\n"
			case "TSV":
				in = c.t + "\t" + c.t3 + "\n"
			case "Values":
				in = "('" + c.t + "','" + c.t3 + "')"
			}
			rows, err := decodeAll(t, f, in, cols, be)
			if err != nil {
				t.Errorf("%s %q: %v", f, in, err)
				continue
			}
			if len(rows) != 1 {
				t.Errorf("%s %q: %d rows", f, in, len(rows))
				continue
			}
			if got := rows[0][0].(time.Time); !got.Equal(tsUTC(c.wt)) {
				t.Errorf("%s t=%q: got %v want %s", f, c.t, got, c.wt)
			}
			if got := rows[0][1].(time.Time); !got.Equal(tsUTC(c.wt3)) {
				t.Errorf("%s t3=%q: got %v want %s", f, c.t3, got, c.wt3)
			}
		}
	}
	// Basic mode still rejects ISO forms with a zone.
	if _, err := decodeAll(t, "JSONEachRow", `{"t":"2024-05-01T10:00:03Z"}`, cols, nil); err == nil {
		t.Errorf("basic mode accepted an ISO zone suffix")
	}
	// Garbage is still a DateTime parse error.
	if _, err := decodeAll(t, "JSONEachRow", `{"t":"garbage"}`, cols, be); !cherr.Is(err, cherr.CannotParseDateTime) {
		t.Errorf("garbage: got %v", err)
	}
	// Nullable(DateTime) and NULL.
	ncols := []Column{col("n", "Nullable(DateTime)")}
	rows, err := decodeAll(t, "JSONEachRow", `{"n":"2024-05-01T10:00:03Z"}`+"\n"+`{"n":null}`, ncols, be)
	if err != nil || len(rows) != 2 || !rows[0][0].(time.Time).Equal(tsUTC("2024-05-01 10:00:03")) || rows[1][0] != nil {
		t.Errorf("nullable: %v %v", rows, err)
	}
	// best_effort_us reads NN/NN/YYYY as month first.
	rows, err = decodeAll(t, "CSV", "05/01/2024 10:00:03,05/01/2024\n", cols, map[string]string{"date_time_input_format": "best_effort_us"})
	if err != nil || !rows[0][0].(time.Time).Equal(tsUTC("2024-05-01 10:00:03")) || !rows[0][1].(time.Time).Equal(tsUTC("2024-05-01 00:00:00")) {
		t.Errorf("best_effort_us: %v %v", rows, err)
	}
	// The column's timezone applies when the input has no zone.
	tk := []Column{col("t", "DateTime('Asia/Tokyo')")}
	rows, err = decodeAll(t, "JSONEachRow", `{"t":"2024-05-01T10:00:03"}`, tk, be)
	if err != nil || !rows[0][0].(time.Time).Equal(tsUTC("2024-05-01 01:00:03")) {
		t.Errorf("tokyo: %v %v", rows, err)
	}
}

// output_format_json_quote_denormals=1 keeps the sign of a negative NaN
// ("-nan"), as ClickHouse 25.8 does for a stored Float32 -nan; text
// formats print nan for both.
func TestJSONQuotedNegativeNaN(t *testing.T) {
	cols := []Column{col("f32", "Float32"), col("f64", "Float64"), col("i", "Float64")}
	negNaN := math.Copysign(math.NaN(), -1)
	row := []any{negNaN, math.NaN(), math.Inf(-1)}
	cases := []struct{ format, want string }{
		{"JSONEachRow", `{"f32":"-nan","f64":"nan","i":"-inf"}` + "\n"},
		{"TSV", "nan\tnan\t-inf\n"},
	}
	for _, c := range cases {
		var buf bytes.Buffer
		enc, _ := NewEncoder(c.format, &buf, cols, Options{Settings: map[string]string{"output_format_json_quote_denormals": "1"}})
		enc.WriteRows([][]any{row})
		enc.Finish(Stats{})
		if buf.String() != c.want {
			t.Errorf("%s: got %q want %q", c.format, buf.String(), c.want)
		}
	}
}
