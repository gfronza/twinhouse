package server

import (
	"compress/bzip2"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// zstdMaxWindow bounds the window a zstd request body may declare, which
// sizes the decoder's buffer: libzstd's default limit, which ClickHouse
// keeps (the Go decoder's own default is 512 MiB).
const zstdMaxWindow = 128 << 20

// decodeRequestBody wraps body according to the request Content-Encoding,
// accepting the same names as ClickHouse (case-insensitive). Decoder failures
// surface on Read as *cherr.Error with ClickHouse's codes.
func decodeRequestBody(encoding string, body io.Reader) (io.ReadCloser, error) {
	enc := strings.ToLower(strings.TrimSpace(encoding))
	switch enc {
	case "", "none", "auto":
		return io.NopCloser(body), nil
	case "gzip":
		zr, err := gzip.NewReader(body)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// An empty gzip body is an empty request.
				return io.NopCloser(strings.NewReader("")), nil
			}
			return nil, cherr.New(cherr.ZlibInflateFailed, "inflate failed: data error")
		}
		return &codecReader{r: zr, closer: zr, code: cherr.ZlibInflateFailed, msg: "inflate failed: data error"}, nil
	case "deflate":
		zr, err := zlib.NewReader(body)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return io.NopCloser(strings.NewReader("")), nil
			}
			return nil, cherr.New(cherr.ZlibInflateFailed, "inflate failed: data error")
		}
		return &codecReader{r: zr, closer: zr, code: cherr.ZlibInflateFailed, msg: "inflate failed: data error"}, nil
	case "br":
		return &codecReader{r: brotli.NewReader(body), code: cherr.BrotliReadFailed, msg: "brotli decode error"}, nil
	case "zstd":
		zr, err := zstd.NewReader(body, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(zstdMaxWindow))
		if err != nil {
			return nil, cherr.New(cherr.ZstdDecoderFailed, "ZSTD stream decoding failed: %v", err)
		}
		return &codecReader{r: zr, closer: closerFunc(func() error { zr.Close(); return nil }),
			code: cherr.ZstdDecoderFailed, msg: "ZSTD stream decoding failed"}, nil
	case "lz4":
		return &codecReader{r: lz4.NewReader(body), code: cherr.LZ4DecoderFailed, msg: "LZ4 decompression failed"}, nil
	case "bz2":
		return &codecReader{r: bzip2.NewReader(body), code: cherr.Bzip2StreamDecoderFailed, msg: "bzip2 stream decoder failed"}, nil
	case "xz", "snappy":
		return nil, cherr.New(cherr.NotImplemented, "Compression method '%s' is not supported by this server", enc)
	}
	return nil, cherr.New(cherr.NotImplemented,
		"Unknown compression method '%s'. Only 'auto', 'none', 'gzip', 'deflate', 'br', 'xz', 'zstd', 'lz4', 'bz2', 'snappy' are supported as compression methods",
		encoding)
}

type closerFunc func() error

func (f closerFunc) Close() error { return f() }

// codecReader maps decompressor failures to a ClickHouse error code.
type codecReader struct {
	r      io.Reader
	closer io.Closer
	code   int
	msg    string
}

func (c *codecReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if err != nil && err != io.EOF {
		var ce *cherr.Error
		if !errors.As(err, &ce) {
			err = cherr.Wrap(c.code, err, "%s", c.msg)
		}
	}
	return n, err
}

func (c *codecReader) Close() error {
	if c.closer != nil {
		return c.closer.Close()
	}
	return nil
}

// chooseResponseEncoding picks the response Content-Encoding the way
// ClickHouse does: substring matches on Accept-Encoding with the precedence
// zstd > br > lz4 > gzip > deflate (verified against ClickHouse 25.8).
// Encodings ClickHouse supports but this server does not (xz, bz2, snappy)
// are skipped. "" means no compression.
func chooseResponseEncoding(accept string) string {
	for _, enc := range []string{"zstd", "br", "lz4", "gzip", "deflate"} {
		if strings.Contains(accept, enc) {
			return enc
		}
	}
	return ""
}

// newResponseEncoder returns a compressing writer for enc. level comes from
// http_zlib_compression_level / http_zstd_compression_level (default 3, as in
// ClickHouse); out-of-range values are clamped.
func newResponseEncoder(enc string, w io.Writer, settings map[string]string) (io.WriteCloser, error) {
	switch enc {
	case "gzip":
		return gzip.NewWriterLevel(w, clampLevel(settings["http_zlib_compression_level"], 3, 1, 9))
	case "deflate":
		return zlib.NewWriterLevel(w, clampLevel(settings["http_zlib_compression_level"], 3, 1, 9))
	case "br":
		return brotli.NewWriterLevel(w, clampLevel(settings["http_brotli_compression_level"], 3, 0, 11)), nil
	case "zstd":
		lvl := clampLevel(settings["http_zstd_compression_level"], 3, 1, 22)
		return zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(lvl)), zstd.WithEncoderConcurrency(1))
	case "lz4":
		return lz4.NewWriter(w), nil
	}
	return nil, cherr.New(cherr.LogicalError, "unsupported response encoding %q", enc)
}

func clampLevel(v string, def, lo, hi int) int {
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return def
	}
	return max(lo, min(hi, n))
}
