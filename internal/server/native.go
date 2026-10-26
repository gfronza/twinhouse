package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	chcompress "github.com/ClickHouse/ch-go/compress"
	"github.com/go-faster/city"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/format"
)

// ClickHouse "native" block compression, used over HTTP with compress=1
// (response) and decompress=1 (request body). Each frame is:
//
//	checksum  16 bytes  CityHash128 (v1.0.2) of everything after it
//	method     1 byte   0x02 none, 0x82 LZ4, 0x90 ZSTD
//	rawSize    4 bytes  LE, compressed size including this 9-byte header
//	dataSize   4 bytes  LE, uncompressed size
//	payload
const (
	nativeChecksumSize = 16
	nativeHeaderSize   = 9
	nativeFrameHeader  = nativeChecksumSize + nativeHeaderSize

	nativeMethodNone = 0x02
	nativeMethodLZ4  = 0x82
	nativeMethodZSTD = 0x90

	// nativeBlockSize is the uncompressed size of each emitted frame
	// (ClickHouse's DBMS_DEFAULT_BUFFER_SIZE).
	nativeBlockSize = 1 << 20
	// nativeMaxBlock bounds a single frame read from a client.
	nativeMaxBlock = 1 << 30
	// nativeZSTDMaxWindow bounds the window of a zstd frame, which the
	// decoder allocates before any output; a single-segment frame's window
	// is its declared content size. Go clients (ch-go) write frames with
	// windows of at most 8 MiB.
	nativeZSTDMaxWindow = 8 << 20
)

// nativeReader decodes a stream of native compressed frames.
type nativeReader struct {
	r      io.Reader
	header [nativeFrameHeader]byte
	raw    []byte
	data   []byte
	pos    int
	zstd   *zstd.Decoder
	zsrc   bytes.Reader // zstd input
	err    error
}

func newNativeReader(r io.Reader) *nativeReader { return &nativeReader{r: r} }

func (n *nativeReader) Read(p []byte) (int, error) {
	for n.pos >= len(n.data) {
		if n.err != nil {
			return 0, n.err
		}
		if err := n.readFrame(); err != nil {
			n.err = err
			n.data, n.pos = nil, 0
			return 0, err
		}
	}
	c := copy(p, n.data[n.pos:])
	n.pos += c
	return c, nil
}

// Close releases decoder resources.
func (n *nativeReader) Close() error {
	if n.zstd != nil {
		n.zstd.Close()
		n.zstd = nil
	}
	return nil
}

func (n *nativeReader) readFrame() error {
	got, err := io.ReadFull(n.r, n.header[:])
	if err != nil {
		if errors.Is(err, io.EOF) && got == 0 {
			return io.EOF
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return cherr.New(cherr.CannotReadAllData, "Cannot read all data. Bytes read: %d. Bytes expected: %d", got, nativeFrameHeader)
		}
		return err
	}
	h := n.header[nativeChecksumSize:]
	method := h[0]
	rawSize := int64(binary.LittleEndian.Uint32(h[1:5]))
	dataSize := int64(binary.LittleEndian.Uint32(h[5:9]))
	if rawSize < nativeHeaderSize || rawSize > nativeMaxBlock || dataSize > nativeMaxBlock {
		return cherr.New(cherr.CannotDecompress, "Too large size_compressed_without_checksum: %d. Most likely corrupted data", rawSize)
	}
	payload := int(rawSize - nativeHeaderSize)
	if err := checkDataSize(method, int64(payload), dataSize); err != nil {
		return err
	}
	// The frame buffer grows as the payload arrives, so a short body cannot
	// make it allocate rawSize up front.
	n.raw, err = format.AppendFull(n.r, append(n.raw[:0], h...), payload)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return cherr.New(cherr.CannotReadAllData, "Cannot read all data. Bytes read: %d. Bytes expected: %d", len(n.raw)-nativeHeaderSize, payload)
		}
		return err
	}
	sum := city.CH128(n.raw)
	if binary.LittleEndian.Uint64(n.header[0:8]) != sum.Low || binary.LittleEndian.Uint64(n.header[8:16]) != sum.High {
		return cherr.New(cherr.ChecksumDoesntMatch, "Checksum doesn't match: corrupted data. Reference: %s. Actual: %s. Size of compressed block: %d",
			chcompress.FormatU128(city.U128{Low: binary.LittleEndian.Uint64(n.header[0:8]), High: binary.LittleEndian.Uint64(n.header[8:16])}),
			chcompress.FormatU128(sum), rawSize)
	}
	body := n.raw[nativeHeaderSize:]
	n.pos = 0
	if method == nativeMethodZSTD {
		return n.decodeZSTD(body, int(dataSize))
	}
	if cap(n.data) < int(dataSize) {
		n.data = make([]byte, dataSize)
	}
	n.data = n.data[:dataSize]
	switch method {
	case nativeMethodNone:
		copy(n.data, body)
	case nativeMethodLZ4:
		w, err := lz4.UncompressBlock(body, n.data)
		if err != nil || w != int(dataSize) {
			return cherr.New(cherr.CannotDecompress, "Cannot decompress LZ4-encoded data")
		}
	default:
		return cherr.New(cherr.UnknownCompressionMethod, "Unknown compression method: %d", method)
	}
	return nil
}

// decodeZSTD decodes a zstd payload of dataSize bytes into n.data. It
// streams with a bounded window and n.data grows as output arrives, so a
// frame's declared sizes cannot make it allocate up front.
func (n *nativeReader) decodeZSTD(body []byte, dataSize int) error {
	n.zsrc.Reset(body)
	if n.zstd == nil {
		// WithDecodeBuffersBelow(0): small inputs would otherwise be
		// decoded at once into a buffer sized by the frame header.
		d, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderMaxWindow(nativeZSTDMaxWindow), zstd.WithDecodeBuffersBelow(0))
		if err != nil {
			return err
		}
		n.zstd = d
	}
	err := n.zstd.Reset(&n.zsrc)
	if err == nil {
		n.data, err = format.AppendFull(n.zstd, n.data[:0], dataSize)
	}
	if err == nil {
		// The payload must decode to exactly dataSize bytes.
		var extra [1]byte
		if _, err = io.ReadFull(n.zstd, extra[:]); err == io.EOF {
			return nil
		}
	}
	return cherr.New(cherr.CannotDecompress, "Cannot decompress ZSTD-encoded data")
}

// checkDataSize rejects a frame declaring more data than its payload can
// decompress to, before that much is allocated: an LZ4 byte yields at most
// 255 bytes, and a 4-byte zstd RLE block at most 128 KiB.
func checkDataSize(method byte, payload, dataSize int64) error {
	switch {
	case method == nativeMethodNone && dataSize != payload:
		return cherr.New(cherr.CannotDecompress, "Cannot decompress: size mismatch")
	case method == nativeMethodLZ4 && dataSize > 255*payload:
		return cherr.New(cherr.CannotDecompress, "Cannot decompress LZ4-encoded data")
	case method == nativeMethodZSTD && dataSize > 32<<10*payload:
		return cherr.New(cherr.CannotDecompress, "Cannot decompress ZSTD-encoded data")
	}
	return nil
}

// nativeWriter encodes written bytes as native LZ4 frames of up to
// nativeBlockSize bytes (ClickHouse always answers compress=1 with LZ4).
type nativeWriter struct {
	w   io.Writer
	buf []byte
	enc *chcompress.Writer
}

func newNativeWriter(w io.Writer) *nativeWriter {
	return &nativeWriter{w: w, enc: chcompress.NewWriter(chcompress.LevelZero, chcompress.LZ4)}
}

func (n *nativeWriter) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		room := nativeBlockSize - len(n.buf)
		c := min(room, len(p))
		n.buf = append(n.buf, p[:c]...)
		p = p[c:]
		written += c
		if len(n.buf) == nativeBlockSize {
			if err := n.Flush(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

// Flush emits the buffered bytes as one frame.
func (n *nativeWriter) Flush() error {
	if len(n.buf) == 0 {
		return nil
	}
	if err := n.enc.Compress(n.buf); err != nil {
		return err
	}
	n.buf = n.buf[:0]
	_, err := n.w.Write(n.enc.Data)
	return err
}

// Close flushes the last frame; it does not close the underlying writer.
func (n *nativeWriter) Close() error { return n.Flush() }
