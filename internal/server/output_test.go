package server

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"

	chcompress "github.com/ClickHouse/ch-go/compress"
	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec"
	"github.com/gfronza/twinhouse/internal/format"
)

func TestSelectOutput(t *testing.T) {
	requireEncoders(t)
	result := func(fmtClause string) *exec.Result {
		r := stringRows("s", "a", "b")
		r.Format = fmtClause
		r.ReadRows, r.ReadBytes = 10, 100
		return r
	}
	cases := []struct {
		name       string
		url        string
		hdr        []string
		clause     string
		wantFormat string
		wantType   string
		wantBody   string
	}{
		{name: "default TSV", url: "/", wantFormat: "TabSeparated", wantType: "text/tab-separated-values; charset=UTF-8", wantBody: "a\nb\n"},
		{name: "FORMAT clause as written", url: "/", clause: "tsv", wantFormat: "tsv", wantType: "text/tab-separated-values; charset=UTF-8", wantBody: "a\nb\n"},
		{name: "default_format", url: "/?default_format=CSV", wantFormat: "CSV", wantType: "text/csv; charset=UTF-8; header=absent", wantBody: "\"a\"\n\"b\"\n"},
		{name: "header beats default_format", url: "/?default_format=CSV", hdr: []string{"X-ClickHouse-Format", "JSONEachRow"},
			wantFormat: "JSONEachRow", wantType: "application/x-ndjson; charset=UTF-8", wantBody: "{\"s\":\"a\"}\n{\"s\":\"b\"}\n"},
		{name: "clause beats header", url: "/", hdr: []string{"X-ClickHouse-Format", "CSV"}, clause: "TSVWithNames",
			wantFormat: "TSVWithNames", wantType: "text/tab-separated-values; charset=UTF-8", wantBody: "s\na\nb\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) { return result(tc.clause), nil }}
			_, ts := newTestServer(t, fe, nil)
			r := post(t, ts.URL+tc.url, "SELECT s FROM t", tc.hdr...)
			if r.Status != 200 || r.Body != tc.wantBody {
				t.Fatalf("%d %q, want %q", r.Status, r.Body, tc.wantBody)
			}
			if got := r.Header.Get("X-ClickHouse-Format"); got != tc.wantFormat {
				t.Errorf("X-ClickHouse-Format %q, want %q", got, tc.wantFormat)
			}
			if got := r.Header.Get("Content-Type"); got != tc.wantType {
				t.Errorf("Content-Type %q, want %q", got, tc.wantType)
			}
			re := regexp.MustCompile(`^\{"read_rows":"10","read_bytes":"100","written_rows":"0","written_bytes":"0","total_rows_to_read":"10","result_rows":"2","result_bytes":"` +
				fmt.Sprint(len(tc.wantBody)) + `","elapsed_ns":"\d+"\}$`)
			if s := r.Header.Get("X-ClickHouse-Summary"); !re.MatchString(s) {
				t.Errorf("summary %s", s)
			}
		})
	}
}

func TestUnknownOutputFormat(t *testing.T) {
	fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) {
		r := oneRow()
		r.Format = "Nope"
		return r, nil
	}}
	_, ts := newTestServer(t, fe, nil)
	r := post(t, ts.URL+"/", "SELECT 1 FORMAT Nope")
	if r.Status != 404 || r.Body != "Code: 73. DB::Exception: Unknown format Nope. (UNKNOWN_FORMAT) (version 25.8.1.1)\n" {
		t.Errorf("%d %q", r.Status, r.Body)
	}
}

func compressWith(t *testing.T, enc string, data []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	var w io.WriteCloser
	switch enc {
	case "gzip", "GZIP":
		w = gzip.NewWriter(&b)
	case "deflate":
		w = zlib.NewWriter(&b)
	case "br":
		w = brotli.NewWriter(&b)
	case "zstd":
		zw, err := zstd.NewWriter(&b)
		if err != nil {
			t.Fatal(err)
		}
		w = zw
	case "lz4":
		w = lz4.NewWriter(&b)
	default:
		t.Fatalf("no compressor for %s", enc)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func decompressWith(t *testing.T, enc string, data []byte) string {
	t.Helper()
	var r io.Reader
	var err error
	switch enc {
	case "gzip":
		r, err = gzip.NewReader(bytes.NewReader(data))
	case "deflate":
		r, err = zlib.NewReader(bytes.NewReader(data))
	case "br":
		r = brotli.NewReader(bytes.NewReader(data))
	case "zstd":
		var d *zstd.Decoder
		d, err = zstd.NewReader(bytes.NewReader(data))
		r = d
	case "lz4":
		r = lz4.NewReader(bytes.NewReader(data))
	case "bz2":
		r = bzip2.NewReader(bytes.NewReader(data))
	}
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("decompress %s: %v", enc, err)
	}
	return string(out)
}

func TestRequestDecompression(t *testing.T) {
	for _, enc := range []string{"gzip", "GZIP", "deflate", "br", "zstd", "lz4"} {
		t.Run(enc, func(t *testing.T) {
			fe := &fakeExec{}
			_, ts := newTestServer(t, fe, nil)
			body := compressWith(t, enc, []byte("SELECT 42"))
			if r := post(t, ts.URL+"/", string(body), "Content-Encoding", enc); r.Status != 200 {
				t.Fatalf("%d %s", r.Status, r.Body)
			}
			if q := fe.last(t).Query; q != "SELECT 42" {
				t.Errorf("query %q", q)
			}
			// INSERT data in a compressed body.
			data := strings.Repeat("{\"a\":1}\n", 1000)
			u := ts.URL + "/?query=" + url.QueryEscape("INSERT INTO t FORMAT JSONEachRow")
			if r := post(t, u, string(compressWith(t, enc, []byte(data))), "Content-Encoding", enc); r.Status != 200 {
				t.Fatalf("%d %s", r.Status, r.Body)
			}
			if got := string(fe.last(t).DataBytes); got != data {
				t.Errorf("data len %d, want %d", len(got), len(data))
			}
		})
	}
	t.Run("bz2", func(t *testing.T) {
		// Go has no bzip2 writer; this is "SELECT 1" compressed with bzip2 -9.
		raw := []byte{0x42, 0x5a, 0x68, 0x39, 0x31, 0x41, 0x59, 0x26, 0x53, 0x59, 0xac, 0xc3, 0x9b, 0x2e, 0x00, 0x00,
			0x03, 0x1e, 0x00, 0x40, 0x00, 0x20, 0x00, 0x0a, 0x04, 0x0c, 0x00, 0x20, 0x00, 0x31, 0x0c, 0x01, 0x06, 0x99,
			0xa1, 0xce, 0x0a, 0x14, 0x5d, 0xc9, 0x14, 0xe1, 0x42, 0x42, 0xb3, 0x0e, 0x6c, 0xb8}
		if got := decompressWith(t, "bz2", raw); got != "SELECT 1" {
			t.Fatalf("fixture decodes to %q", got)
		}
		fe := &fakeExec{}
		_, ts := newTestServer(t, fe, nil)
		if r := post(t, ts.URL+"/", string(raw), "Content-Encoding", "bz2"); r.Status != 200 || fe.last(t).Query != "SELECT 1" {
			t.Fatalf("%d %s", r.Status, r.Body)
		}
	})

	errCases := []struct {
		enc, body string
		status    int
		code      string
	}{
		{"gzip", "SELECT 1", 500, "354"},
		{"deflate", "SELECT 1", 500, "354"},
		{"zstd", "SELECT 1 garbage", 500, "561"},
		{"lz4", "SELECT 1 garbage", 500, "618"},
		{"br", "SELECT 1 garbage", 500, "448"},
		{"foo", "SELECT 1", 501, "48"},
		{"x-gzip", "SELECT 1", 501, "48"},
		{"identity", "SELECT 1", 501, "48"},
		{"xz", "SELECT 1", 501, "48"},
	}
	for _, ec := range errCases {
		fe := &fakeExec{}
		_, ts := newTestServer(t, fe, nil)
		r := post(t, ts.URL+"/", ec.body, "Content-Encoding", ec.enc)
		if r.Status != ec.status || r.Header.Get("X-ClickHouse-Exception-Code") != ec.code {
			t.Errorf("%s: %d %s", ec.enc, r.Status, r.Body)
		}
	}
	// "none" and empty are identity.
	fe := &fakeExec{}
	_, ts := newTestServer(t, fe, nil)
	if r := post(t, ts.URL+"/", "SELECT 7", "Content-Encoding", "none"); r.Status != 200 || fe.last(t).Query != "SELECT 7" {
		t.Errorf("none: %d %s", r.Status, r.Body)
	}
}

func TestChooseResponseEncoding(t *testing.T) {
	cases := map[string]string{
		"gzip": "gzip", "deflate": "deflate", "br": "br", "zstd": "zstd", "lz4": "lz4",
		"br, gzip": "br", "zstd, gzip": "zstd", "deflate, gzip": "gzip", "lz4, zstd": "zstd",
		"br, zstd": "zstd", "lz4, br": "br", "lz4, deflate": "lz4", "lz4, gzip": "lz4",
		"x-gzip": "gzip", "gzip;q=0": "gzip", "GZIP": "", "identity": "", "": "", "xz": "", "xz, gzip": "gzip",
	}
	for in, want := range cases {
		if got := chooseResponseEncoding(in); got != want {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
	}
}

func TestResponseCompression(t *testing.T) {
	requireEncoders(t)
	for _, enc := range []string{"gzip", "deflate", "br", "zstd", "lz4"} {
		t.Run(enc, func(t *testing.T) {
			fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) {
				return stringRows("s", "hello", "world"), nil
			}}
			_, ts := newTestServer(t, fe, nil)
			r := post(t, ts.URL+"/?enable_http_compression=1", "SELECT s", "Accept-Encoding", enc)
			if r.Status != 200 || r.Header.Get("Content-Encoding") != enc {
				t.Fatalf("%d %v", r.Status, r.Header)
			}
			if got := decompressWith(t, enc, []byte(r.Body)); got != "hello\nworld\n" {
				t.Errorf("body %q", got)
			}
			// Errors are compressed too.
			fe.fn = func(context.Context, *exec.Request) (*exec.Result, error) { return nil, cherr404() }
			r = post(t, ts.URL+"/?enable_http_compression=1", "SELECT s", "Accept-Encoding", enc)
			if r.Status != 404 || r.Header.Get("Content-Encoding") != enc ||
				!strings.HasPrefix(decompressWith(t, enc, []byte(r.Body)), "Code: 60.") {
				t.Errorf("error response %d %v", r.Status, r.Header)
			}
		})
	}
	fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) { return stringRows("s", "x"), nil }}
	_, ts := newTestServer(t, fe, nil)
	// Without enable_http_compression the response is not compressed.
	if r := post(t, ts.URL+"/", "SELECT s", "Accept-Encoding", "gzip"); r.Header.Get("Content-Encoding") != "" || r.Body != "x\n" {
		t.Errorf("uncompressed: %v %q", r.Header, r.Body)
	}
	// Authentication failures are never compressed.
	r := post(t, ts.URL+"/?enable_http_compression=1&user=nobody", "SELECT s", "Accept-Encoding", "gzip")
	if r.Status != 403 || r.Header.Get("Content-Encoding") != "" || !strings.HasPrefix(r.Body, "Code: 516.") {
		t.Errorf("auth failure: %d %v %q", r.Status, r.Header, r.Body)
	}
}

func cherr404() error { return cherr.New(cherr.UnknownTable, "Table default.t does not exist") }

func TestNativeCompression(t *testing.T) {
	requireEncoders(t)
	fe := &fakeExec{fn: func(_ context.Context, req *exec.Request) (*exec.Result, error) {
		if req.Query == "FAIL" {
			return nil, cherr404()
		}
		return stringRows("s", "a", "b"), nil
	}}
	_, ts := newTestServer(t, fe, nil)

	r := post(t, ts.URL+"/?compress=1", "SELECT s")
	if r.Status != 200 {
		t.Fatalf("%d %s", r.Status, r.Body)
	}
	if got := readNative(t, []byte(r.Body)); got != "a\nb\n" {
		t.Errorf("decompressed %q", got)
	}
	if method := r.Body[16]; method != nativeMethodLZ4 {
		t.Errorf("method byte %#x", method)
	}
	// Error bodies are native-compressed as well (clickhouse-go relies on it).
	r = post(t, ts.URL+"/?compress=1", "FAIL")
	if r.Status != 404 || !strings.HasPrefix(readNative(t, []byte(r.Body)), "Code: 60.") {
		t.Errorf("compressed error %d", r.Status)
	}

	// decompress=1: the request body is native-compressed (query and data).
	for _, m := range []chcompress.Method{chcompress.LZ4, chcompress.ZSTD, chcompress.None} {
		w := chcompress.NewWriter(chcompress.LevelZero, m)
		frame := func(s string) []byte {
			if err := w.Compress([]byte(s)); err != nil {
				t.Fatal(err)
			}
			return append([]byte(nil), w.Data...)
		}
		body := append(frame("SELECT "), frame("1")...)
		if r := post(t, ts.URL+"/?decompress=1", string(body)); r.Status != 200 || fe.last(t).Query != "SELECT 1" {
			t.Errorf("%v: %d %s %q", m, r.Status, r.Body, fe.last(t).Query)
		}
		u := ts.URL + "/?decompress=1&query=" + url.QueryEscape("INSERT INTO t FORMAT TSV")
		if r := post(t, u, string(frame("1\tx\n"))); r.Status != 200 || string(fe.last(t).DataBytes) != "1\tx\n" {
			t.Errorf("%v insert: %d %s", m, r.Status, r.Body)
		}
	}
	w := chcompress.NewWriter(chcompress.LevelZero, chcompress.LZ4)
	if err := w.Compress([]byte("SELECT 1")); err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), w.Data...)
	corrupt[0] ^= 0xff
	if r := post(t, ts.URL+"/?decompress=1", string(corrupt)); r.Header.Get("X-ClickHouse-Exception-Code") != "40" {
		t.Errorf("checksum: %d %s", r.Status, r.Body)
	}
	if r := post(t, ts.URL+"/?decompress=1", string(w.Data[:len(w.Data)-2])); r.Header.Get("X-ClickHouse-Exception-Code") != "33" {
		t.Errorf("truncated: %d %s", r.Status, r.Body)
	}
	if r := post(t, ts.URL+"/?decompress=1", "SELECT 77"); r.Header.Get("X-ClickHouse-Exception-Code") != "33" {
		t.Errorf("plain body with decompress=1: %d %s", r.Status, r.Body)
	}
}

// readNative decodes native compressed frames.
func readNative(t *testing.T, b []byte) string {
	t.Helper()
	out, err := io.ReadAll(newNativeReader(bytes.NewReader(b)))
	if err != nil {
		t.Fatalf("native decode: %v", err)
	}
	return string(out)
}

func TestNativeWriterFraming(t *testing.T) {
	var buf bytes.Buffer
	nw := newNativeWriter(&buf)
	payload := bytes.Repeat([]byte("0123456789"), nativeBlockSize/10+5000) // > 1 frame
	if _, err := nw.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := nw.Close(); err != nil {
		t.Fatal(err)
	}
	// Two frames, each decodable by ch-go's reader (the clickhouse-go side).
	got, err := io.ReadAll(chcompress.NewReader(bytes.NewReader(buf.Bytes())))
	if err != nil && !strings.Contains(err.Error(), "EOF") {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("roundtrip mismatch: %d vs %d bytes", len(got), len(payload))
	}
	first := binary.LittleEndian.Uint32(buf.Bytes()[nativeChecksumSize+5:])
	if first != nativeBlockSize {
		t.Errorf("first frame size %d", first)
	}
	if again := readNative(t, buf.Bytes()); again != string(payload) {
		t.Error("own reader mismatch")
	}
}

func TestBufferingAndWaitEndOfQuery(t *testing.T) {
	requireEncoders(t)
	many := stringRows("s")
	for i := 0; i < 2000; i++ {
		many.Rows = append(many.Rows, []any{strings.Repeat("x", 50)})
	}
	fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) { return many, nil }}
	_, ts := newTestServer(t, fe, func(c *Config) { c.ResponseBufferSize = 4096 })

	r := post(t, ts.URL+"/", "SELECT s")
	if len(r.Body) != 2000*51 {
		t.Fatalf("body len %d", len(r.Body))
	}
	// Committed early: result_bytes unknown when headers were sent.
	if s := r.Header.Get("X-ClickHouse-Summary"); !strings.Contains(s, `"result_bytes":"0"`) {
		t.Errorf("early summary %s", s)
	}
	r = post(t, ts.URL+"/?wait_end_of_query=1", "SELECT s")
	if s := r.Header.Get("X-ClickHouse-Summary"); !strings.Contains(s, fmt.Sprintf(`"result_bytes":"%d"`, 2000*51)) || r.Header.Get("Content-Length") != fmt.Sprint(2000*51) {
		t.Errorf("wait_end_of_query summary %s, headers %v", s, r.Header)
	}
}

// badRows returns a Native-encodable result whose third row is invalid, so
// the encoder fails on the third block (max_block_size=1).
func badRows() *exec.Result {
	return &exec.Result{
		Columns: []format.Column{{Name: "n", Type: chtype.TUInt64}},
		Rows:    [][]any{{uint64(1)}, {uint64(2)}, {"not a number"}},
		Format:  "Native",
	}
}

func TestEncoderErrorBeforeCommit(t *testing.T) {
	requireEncoders(t)
	fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) { return badRows(), nil }}
	_, ts := newTestServer(t, fe, nil)
	r := post(t, ts.URL+"/?max_block_size=1", "SELECT n FORMAT Native")
	if r.Status != 500 || r.Header.Get("X-ClickHouse-Exception-Code") != "49" || !strings.HasPrefix(r.Body, "Code: 49. DB::Exception: ") {
		t.Errorf("%d %v %q", r.Status, r.Header, r.Body)
	}
	if r.Header.Get("X-ClickHouse-Format") != "" || r.Header.Get("Content-Type") != "text/plain; charset=UTF-8" {
		t.Errorf("headers %v", r.Header)
	}
}

func TestMidStreamException(t *testing.T) {
	requireEncoders(t)
	fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) { return badRows(), nil }}
	_, ts := newTestServer(t, fe, func(c *Config) { c.ResponseBufferSize = 1 })
	r := post(t, ts.URL+"/?max_block_size=1", "SELECT n FORMAT Native")
	if r.Status != 200 {
		t.Fatalf("status %d", r.Status)
	}
	i := strings.Index(r.Body, "__exception__\r\nCode: 49. DB::Exception: ")
	if i <= 0 || !strings.HasSuffix(r.Body, "(LOGICAL_ERROR) (version 25.8.1.1)\n") {
		t.Errorf("body %q", r.Body)
	}
	// The same framing inside a compress=1 stream (frames are emitted per
	// MiB, so the result must be larger than that to commit before failing).
	big := &exec.Result{Columns: badRows().Columns, Format: "Native"}
	for i := 0; i < 300000; i++ {
		big.Rows = append(big.Rows, []any{uint64(i)})
	}
	big.Rows = append(big.Rows, []any{"bad"})
	fe.fn = func(context.Context, *exec.Request) (*exec.Result, error) { return big, nil }
	r = post(t, ts.URL+"/?compress=1", "SELECT n FORMAT Native")
	if body := readNative(t, []byte(r.Body)); r.Status != 200 || !strings.HasSuffix(body, "(LOGICAL_ERROR) (version 25.8.1.1)\n") ||
		!strings.Contains(body, "__exception__\r\nCode: 49.") {
		t.Errorf("compressed body: status %d, tail %q", r.Status, body[max(0, len(body)-200):])
	}
}

func TestNativeBlocks(t *testing.T) {
	requireEncoders(t)
	res := &exec.Result{Columns: []format.Column{{Name: "x", Type: chtype.TUInt8}}, Format: "Native"}
	for i := 0; i < 5; i++ {
		res.Rows = append(res.Rows, []any{uint64(i)})
	}
	fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) { return res, nil }}
	_, ts := newTestServer(t, fe, nil)
	cases := map[string][]int{"/?max_block_size=2": {2, 2, 1}, "/": {5}, "/?max_block_size=junk": {5}}
	for u, want := range cases {
		r := post(t, ts.URL+u, "SELECT x FORMAT Native")
		if got := nativeBlockRows(t, []byte(r.Body)); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s: blocks %v, want %v", u, got, want)
		}
	}
}

// nativeBlockRows parses a Native stream of single-UInt8-column blocks
// (client_protocol_version 0) and returns each block's row count.
func nativeBlockRows(t *testing.T, b []byte) []int {
	t.Helper()
	var out []int
	r := bytes.NewReader(b)
	for r.Len() > 0 {
		cols, err := binary.ReadUvarint(r)
		if err != nil || cols != 1 {
			t.Fatalf("columns %d %v", cols, err)
		}
		rows, _ := binary.ReadUvarint(r)
		for i := 0; i < 2; i++ { // name, type
			n, _ := binary.ReadUvarint(r)
			if _, err := r.Seek(int64(n), io.SeekCurrent); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := r.Seek(int64(rows), io.SeekCurrent); err != nil {
			t.Fatal(err)
		}
		out = append(out, int(rows))
	}
	return out
}

func TestHeadQuery(t *testing.T) {
	requireEncoders(t)
	fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) { return oneRow(), nil }}
	_, ts := newTestServer(t, fe, nil)
	req, _ := http.NewRequest(http.MethodHead, ts.URL+"/?query=SELECT+1", nil)
	r := do(t, req)
	if r.Status != 200 || r.Body != "" || r.Header.Get("X-ClickHouse-Format") != "TabSeparated" {
		t.Errorf("%d %q %v", r.Status, r.Body, r.Header)
	}
}

// A SELECT's own SETTINGS clause applies to the output format, like URL or
// session settings (ClickHouse: SELECT inf AS a SETTINGS
// output_format_json_quote_denormals = 1 FORMAT JSONEachRow -> {"a":"inf"}).
func TestQuerySettingsApplyToOutput(t *testing.T) {
	requireEncoders(t)
	if !parserReady() {
		t.Skip("sqlparse not implemented yet")
	}
	res := func() *exec.Result {
		return &exec.Result{
			Columns: []format.Column{{Name: "a", Type: chtype.TFloat64}, {Name: "n", Type: chtype.TUInt64}},
			Rows:    [][]any{{math.Inf(1), uint64(7)}},
			Format:  "JSONEachRow",
		}
	}
	cases := []struct{ name, url, query, want string }{
		{"no settings", "/", "SELECT inf AS a, 7 AS n FORMAT JSONEachRow", `{"a":null,"n":7}` + "\n"},
		{"settings clause", "/", "SELECT inf AS a, 7 AS n SETTINGS output_format_json_quote_denormals = 1, output_format_json_quote_64bit_integers = 1 FORMAT JSONEachRow",
			`{"a":"inf","n":"7"}` + "\n"},
		{"settings clause lowercase", "/", "select inf as a, 7 as n settings output_format_json_quote_denormals=1 format JSONEachRow", `{"a":"inf","n":7}` + "\n"},
		{"clause overrides url", "/?output_format_json_quote_denormals=1&output_format_json_quote_64bit_integers=1",
			"SELECT inf AS a, 7 AS n SETTINGS output_format_json_quote_denormals = 0 FORMAT JSONEachRow", `{"a":null,"n":"7"}` + "\n"},
		{"union branch", "/", "SELECT inf AS a, 7 AS n UNION ALL SELECT 1, 2 SETTINGS output_format_json_quote_denormals = 1 FORMAT JSONEachRow", `{"a":"inf","n":7}` + "\n"},
		{"string literal mentioning settings", "/", "SELECT inf AS a, 7 AS n WHERE 'SETTINGS' != '' FORMAT JSONEachRow", `{"a":null,"n":7}` + "\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) { return res(), nil }}
			_, ts := newTestServer(t, fe, nil)
			r := post(t, ts.URL+tc.url, tc.query)
			if r.Status != 200 || r.Body != tc.want {
				t.Fatalf("%d %q, want %q", r.Status, r.Body, tc.want)
			}
		})
	}
}

// ClickHouse output formats this server does not implement answer
// NOT_IMPLEMENTED (48) per DESIGN §2; names ClickHouse does not know stay
// UNKNOWN_FORMAT (73).
func TestUnsupportedOutputFormat(t *testing.T) {
	cases := []struct {
		format string
		code   string
		status int
	}{
		{"RowBinary", "48", 501},
		{"rowbinarywithnamesandtypes", "48", 501},
		{"Parquet", "48", 501},
		{"Arrow", "48", 501},
		{"NoSuchFormat", "73", 404},
	}
	for _, tc := range cases {
		t.Run(tc.format, func(t *testing.T) {
			fe := &fakeExec{fn: func(context.Context, *exec.Request) (*exec.Result, error) {
				r := oneRow()
				r.Format = tc.format
				return r, nil
			}}
			_, ts := newTestServer(t, fe, nil)
			r := post(t, ts.URL+"/", "SELECT 1 FORMAT "+tc.format)
			if r.Status != tc.status || r.Header.Get("X-ClickHouse-Exception-Code") != tc.code {
				t.Fatalf("%d %q code=%q, want %d code %s", r.Status, r.Body, r.Header.Get("X-ClickHouse-Exception-Code"), tc.status, tc.code)
			}
		})
	}
}
