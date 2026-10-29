package server

import (
	"bytes"
	"encoding/binary"
	"io"
	"runtime"
	"testing"

	chcompress "github.com/ClickHouse/ch-go/compress"
	"github.com/go-faster/city"
	"github.com/klauspost/compress/zstd"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// allocated returns the bytes f allocates on the heap.
func allocated(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// nativeFrame builds a frame header declaring rawSize and dataSize, then
// payload, with the checksum of what is present.
func nativeFrame(method byte, rawSize, dataSize uint32, payload []byte) []byte {
	h := []byte{method}
	h = binary.LittleEndian.AppendUint32(h, rawSize)
	h = binary.LittleEndian.AppendUint32(h, dataSize)
	h = append(h, payload...)
	sum := city.CH128(h)
	f := binary.LittleEndian.AppendUint64(nil, sum.Low)
	f = binary.LittleEndian.AppendUint64(f, sum.High)
	return append(f, h...)
}

// readNativeErr reads body through a nativeReader and returns the bytes
// allocated meanwhile and the error.
func readNativeErr(body []byte) (uint64, error) {
	var err error
	n := allocated(func() {
		nr := newNativeReader(bytes.NewReader(body))
		defer nr.Close()
		_, err = io.Copy(io.Discard, nr)
	})
	return n, err
}

func TestNativeReaderShortFrameAllocatesLittle(t *testing.T) {
	body := nativeFrame(nativeMethodNone, 64<<20+nativeHeaderSize, 64<<20, nil)
	n, err := readNativeErr(body)
	if n > 4<<20 {
		t.Errorf("allocated %d bytes for a %d-byte body", n, len(body))
	}
	if !cherr.Is(err, cherr.CannotReadAllData) {
		t.Errorf("got %v, want CANNOT_READ_ALL_DATA", err)
	}
}

func TestNativeReaderRejectsDataSizeBeyondPayload(t *testing.T) {
	payload := []byte{0x10, 'x', 0, 0}
	for _, method := range []byte{nativeMethodLZ4, nativeMethodZSTD} {
		body := nativeFrame(method, nativeHeaderSize+uint32(len(payload)), 512<<20, payload)
		n, err := readNativeErr(body)
		if n > 4<<20 {
			t.Errorf("method %#x: allocated %d bytes for a %d-byte body", method, n, len(body))
		}
		if !cherr.Is(err, cherr.CannotDecompress) {
			t.Errorf("method %#x: got %v, want CANNOT_DECOMPRESS", method, err)
		}
	}
}

func TestNativeReaderZSTDOutputStopsAtDeclaredSize(t *testing.T) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	payload := enc.EncodeAll(make([]byte, 64<<20), nil)
	enc.Close()
	body := nativeFrame(nativeMethodZSTD, uint32(nativeHeaderSize+len(payload)), 1024, payload)
	n, err := readNativeErr(body)
	// The decoder's 8 MiB window is allocated up front.
	if n > 12<<20 {
		t.Errorf("allocated %d bytes decoding a frame declared as 1024 bytes", n)
	}
	if !cherr.Is(err, cherr.CannotDecompress) {
		t.Errorf("got %v, want CANNOT_DECOMPRESS", err)
	}
}

// zstdJunk returns a zstd frame header declaring contentSize, with the
// given window descriptor (0: single segment, the window is the content
// size), then one raw block of 1000 bytes and junk up to size bytes.
func zstdJunk(window byte, contentSize uint32, size int) []byte {
	b := []byte{0x28, 0xb5, 0x2f, 0xfd, 0xa0}
	if window != 0 {
		b = append(b[:4], 0x80, window)
	}
	b = binary.LittleEndian.AppendUint32(b, contentSize)
	b = append(b, 1000<<3&0xff, 1000<<3>>8, 0)
	for len(b) < size {
		b = append(b, byte(len(b)*7))
	}
	return b
}

func TestNativeReaderZSTDJunkFrameAllocatesLittle(t *testing.T) {
	const declared = 512 << 20
	for name, window := range map[string]byte{"single segment": 0, "1 MiB window": 10 << 3} {
		payload := zstdJunk(window, declared, 16<<10)
		body := nativeFrame(nativeMethodZSTD, uint32(nativeHeaderSize+len(payload)), declared, payload)
		n, err := readNativeErr(body)
		if n > 4<<20 {
			t.Errorf("%s: allocated %d bytes for a %d-byte body", name, n, len(body))
		}
		if !cherr.Is(err, cherr.CannotDecompress) {
			t.Errorf("%s: got %v, want CANNOT_DECOMPRESS", name, err)
		}
	}
}

func TestNativeReaderZSTDOutputMustMatchDeclaredSize(t *testing.T) {
	data := bytes.Repeat([]byte("twinhouse "), 100_000)
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	payload := enc.EncodeAll(data, nil)
	enc.Close()
	for _, dataSize := range []int{len(data) - 1, len(data) + 1} {
		body := nativeFrame(nativeMethodZSTD, uint32(nativeHeaderSize+len(payload)), uint32(dataSize), payload)
		if _, err := readNativeErr(body); !cherr.Is(err, cherr.CannotDecompress) {
			t.Errorf("declared %d of %d bytes: got %v, want CANNOT_DECOMPRESS", dataSize, len(data), err)
		}
	}
	body := nativeFrame(nativeMethodZSTD, uint32(nativeHeaderSize+len(payload)), uint32(len(data)), payload)
	out, err := io.ReadAll(newNativeReader(bytes.NewReader(append(body, body...))))
	if err != nil || !bytes.Equal(out, append(data, data...)) {
		t.Errorf("got %d bytes, %v", len(out), err)
	}
}

func TestNativeReaderDecodesHighlyCompressedFrames(t *testing.T) {
	data := make([]byte, 4<<20)
	for _, m := range []chcompress.Method{chcompress.LZ4, chcompress.LZ4HC, chcompress.ZSTD} {
		w := chcompress.NewWriter(chcompress.LevelZero, m)
		if err := w.Compress(data); err != nil {
			t.Fatal(err)
		}
		out, err := io.ReadAll(newNativeReader(bytes.NewReader(w.Data)))
		if err != nil || !bytes.Equal(out, data) {
			t.Errorf("%v: %d bytes, %v", m, len(out), err)
		}
	}
}
