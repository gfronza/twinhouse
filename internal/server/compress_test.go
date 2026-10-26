package server

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/gfronza/twinhouse/internal/cherr"
)

func TestZstdRequestBodyWindowIsCapped(t *testing.T) {
	// A zstd frame declaring a 256 MiB window (Window_Descriptor 0x90),
	// holding one raw block with "x".
	frame := "\x28\xb5\x2f\xfd\x00\x90\x09\x00\x00x"
	var got []byte
	var err error
	n := allocated(func() {
		body, derr := decodeRequestBody("zstd", strings.NewReader(frame))
		if derr != nil {
			t.Fatal(derr)
		}
		defer body.Close()
		got, err = io.ReadAll(body)
	})
	if !cherr.Is(err, cherr.ZstdDecoderFailed) {
		t.Errorf("got %q, %v; want ZSTD_DECODER_FAILED", got, err)
	}
	if n > 16<<20 {
		t.Errorf("allocated %d bytes for a %d-byte body", n, len(frame))
	}
}

func TestZstdRequestBodyWithLargestWindowDecodes(t *testing.T) {
	data := bytes.Repeat([]byte("twinhouse "), 1000)
	enc, err := zstd.NewWriter(nil, zstd.WithWindowSize(zstdMaxWindow), zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	frame := enc.EncodeAll(data, nil)
	enc.Close()
	body, err := decodeRequestBody("zstd", bytes.NewReader(frame))
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	if got, err := io.ReadAll(body); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("got %d bytes, %v", len(got), err)
	}
}
