package server

import (
	"bytes"
	"io"
	"net/http"
)

// commitWriter buffers the response body until either the buffer limit is
// exceeded or the response is finished, and only then sends the status line
// and headers. While nothing has been committed an error can still be
// reported with a proper HTTP status; afterwards it has to be appended to
// the body (like ClickHouse does).
type commitWriter struct {
	w     http.ResponseWriter
	limit int // < 0: buffer everything (wait_end_of_query)
	// noBody drops the body (HEAD requests) while still producing headers.
	noBody bool
	buf    bytes.Buffer
	// onCommit is called once, right before the headers are written; final
	// is true when the whole body is in the buffer.
	onCommit  func(h http.Header, final bool)
	committed bool
	err       error
}

func (c *commitWriter) Write(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	if c.committed {
		if c.noBody {
			return len(p), nil
		}
		n, err := c.w.Write(p)
		c.err = err
		return n, err
	}
	c.buf.Write(p)
	if c.limit >= 0 && c.buf.Len() > c.limit {
		c.commit(false)
		if c.err != nil {
			return 0, c.err
		}
	}
	return len(p), nil
}

func (c *commitWriter) commit(final bool) {
	if c.committed {
		return
	}
	c.committed = true
	if c.onCommit != nil {
		c.onCommit(c.w.Header(), final)
	}
	c.w.WriteHeader(http.StatusOK)
	if c.buf.Len() > 0 && !c.noBody {
		_, c.err = c.w.Write(c.buf.Bytes())
	}
	c.buf = bytes.Buffer{}
}

// finish commits whatever is buffered.
func (c *commitWriter) finish() error {
	c.commit(true)
	return c.err
}

// discard drops buffered output (only meaningful before commit).
func (c *commitWriter) discard() { c.buf.Reset() }

// bodyPipeline is the chain of writers a response body goes through:
// payload -> [native compress=1 frames] -> [HTTP Content-Encoding] -> sink.
type bodyPipeline struct {
	top    io.Writer
	native *nativeWriter
	http   io.WriteCloser
}

func newBodyPipeline(sink io.Writer, nativeCompress bool, httpEncoding string, settings map[string]string) (*bodyPipeline, error) {
	p := &bodyPipeline{top: sink}
	if httpEncoding != "" {
		enc, err := newResponseEncoder(httpEncoding, sink, settings)
		if err != nil {
			return nil, err
		}
		p.http = enc
		p.top = enc
	}
	if nativeCompress {
		p.native = newNativeWriter(p.top)
		p.top = p.native
	}
	return p, nil
}

func (p *bodyPipeline) Write(b []byte) (int, error) { return p.top.Write(b) }

// close flushes every stage (innermost first).
func (p *bodyPipeline) close() error {
	var first error
	if p.native != nil {
		first = p.native.Close()
	}
	if p.http != nil {
		if err := p.http.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// countingWriter counts bytes written through it.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// countingReader counts bytes read through it.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
