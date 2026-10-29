package server

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"
)

// commitWriter buffers the response body until either the buffer limit is
// exceeded or the response is finished, and only then sends the status line
// and headers. While nothing has been committed an error can still be
// reported with a proper HTTP status; afterwards it has to be appended to
// the body (like ClickHouse does).
//
// After the commit the body is gathered and sent in large writes, and
// each block is flushed to the client (flush), at once or by a timer when
// blocks come faster than flushInterval; mu guards what the timer shares.
type commitWriter struct {
	w     http.ResponseWriter
	limit int // < 0: buffer everything (wait_end_of_query)
	// noBody drops the body (HEAD requests) while still producing headers.
	noBody bool
	buf    bytes.Buffer
	// onCommit is called once, right before the headers are written; final
	// is true when the whole body is in the buffer.
	onCommit func(h http.Header, final bool)
	// charge, when set, counts what the buffer grows by past its first
	// free bytes against the query's memory limit.
	charge func(n int64) error
	free   int
	// sendTimeout bounds each write to the client (http_send_timeout;
	// 0: none), set through rc, which controller creates when needed.
	sendTimeout time.Duration
	committed   bool

	mu          sync.Mutex
	rc          *http.ResponseController
	deadlineSet bool
	// hijacked is set once abort has taken the connection over; ended
	// once endSend has run. Nothing is sent after either.
	hijacked, ended bool
	// out gathers the body after the commit, so it is sent in writes of
	// up to sendBuffer bytes.
	out []byte
	// flushed is when the client was last flushed; timer flushes the
	// blocks ended since, when it is set (pending).
	flushed time.Time
	timer   *time.Timer
	pending bool
	err     error
}

// sendChunk is the most bytes sent under one write deadline, so that
// http_send_timeout bounds a stalled client rather than a long transfer.
const sendChunk = 256 << 10

// sendBuffer is how much of the body is gathered after the commit before
// it is sent: each write to the client costs system calls, and the
// encoders write 64 KiB at a time.
const sendBuffer = 256 << 10

// flushInterval is the shortest time between two flushes to the client:
// a block that ends sooner after the last one is flushed with the blocks
// that follow it within the interval, so small blocks do not cost a
// system call each, and none waits longer.
const flushInterval = 2 * time.Millisecond

func (c *commitWriter) Write(p []byte) (int, error) {
	if c.committed {
		if c.noBody {
			return len(p), nil
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.gather(p)
	}
	if c.err != nil {
		return 0, c.err
	}
	if err := c.reserve(len(p)); err != nil {
		return 0, err
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

// reserve makes room for n more bytes, charging what the buffer grows by
// past its free bytes before it grows (bytes.Buffer at least doubles).
func (c *commitWriter) reserve(n int) error {
	if c.charge == nil || c.buf.Available() >= n {
		return nil
	}
	if grown := max(2*c.buf.Cap(), c.buf.Len()+n); grown > c.free {
		if err := c.charge(int64(grown - max(c.buf.Cap(), c.free))); err != nil {
			return err
		}
	}
	c.buf.Grow(n)
	return nil
}

// gather adds p to the body gathered after the commit, sending what is
// gathered when p does not fit, and p itself when it is that large.
func (c *commitWriter) gather(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	if len(c.out)+len(p) > sendBuffer {
		if c.sendGathered() != nil {
			return 0, c.err
		}
		if len(p) >= sendBuffer {
			n, err := c.send(p)
			c.err = err
			return n, err
		}
	}
	if c.out == nil {
		c.out = make([]byte, 0, sendBuffer)
	}
	c.out = append(c.out, p...)
	return len(p), nil
}

// sendGathered sends the body gathered since the commit.
func (c *commitWriter) sendGathered() error {
	if len(c.out) > 0 && c.err == nil && !c.hijacked && !c.ended {
		_, c.err = c.send(c.out)
		c.out = c.out[:0]
	}
	return c.err
}

// send writes p to the client in chunks of at most sendChunk bytes; a
// chunk that blocks for sendTimeout fails.
func (c *commitWriter) send(p []byte) (int, error) {
	n := 0
	for len(p) > 0 && c.sendTimeout > 0 {
		k := min(len(p), sendChunk)
		if !c.setDeadline() {
			break
		}
		m, err := c.w.Write(p[:k])
		n += m
		if err != nil {
			return n, err
		}
		p = p[k:]
	}
	if len(p) == 0 {
		return n, nil
	}
	m, err := c.w.Write(p)
	return n + m, err
}

// setDeadline bounds the next write by sendTimeout. It reports false, and
// stops trying, for a writer without deadlines (the Lambda adapter, tests).
func (c *commitWriter) setDeadline() bool {
	if c.controller().SetWriteDeadline(time.Now().Add(c.sendTimeout)) != nil {
		c.sendTimeout = 0
		return false
	}
	c.deadlineSet = true
	return true
}

// controller returns the response's controller, creating it on first use
// (a response without a body or a send timeout never needs one).
func (c *commitWriter) controller() *http.ResponseController {
	if c.rc == nil {
		c.rc = http.NewResponseController(c.w)
	}
	return c.rc
}

// flush ends a block of a committed response: the client gets it, with
// the status and headers after the commit, as ClickHouse's auto-flush
// sends each block. A block that ends within flushInterval of the last
// flush is flushed by the timer when the interval is over (with the
// blocks that end before then). Before the commit it does nothing.
func (c *commitWriter) flush() error {
	if !c.committed || c.noBody {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil || c.hijacked || c.ended || c.pending {
		return c.err
	}
	now := time.Now()
	wait := flushInterval - now.Sub(c.flushed)
	if wait <= 0 {
		c.flushNow(now)
		return c.err
	}
	c.pending = true
	if c.timer == nil {
		c.timer = time.AfterFunc(wait, c.flushLater)
	} else {
		c.timer.Reset(wait)
	}
	return nil
}

// flushLater is the timer's flush of the blocks ended since the last one.
func (c *commitWriter) flushLater() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending && c.err == nil && !c.hijacked && !c.ended {
		c.flushNow(time.Now())
	}
	c.pending = false
}

// flushNow sends what is gathered and pushes it out of the
// ResponseWriter's buffers to the client.
func (c *commitWriter) flushNow(now time.Time) {
	c.pending, c.flushed = false, now
	if c.sendGathered() != nil {
		return
	}
	if c.sendTimeout > 0 && !c.setDeadline() {
		return
	}
	if err := c.controller().Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		c.err = err
	}
}

// failed returns the error that ended sending, if any.
func (c *commitWriter) failed() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// sendHeaders commits the response and sends its status and headers at
// once, ending a HEAD request's result (errHeadersSent).
func (c *commitWriter) sendHeaders() error {
	c.commit(false)
	if c.err != nil {
		return c.err
	}
	_ = c.controller().Flush()
	return errHeadersSent
}

// endSend sends what the response still gathers or buffers within the
// send timeout, then clears the deadline, which must not outlive the
// response on a kept-alive connection. Nothing is sent afterwards (the
// flush timer included).
func (c *commitWriter) endSend() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.timer != nil {
		c.timer.Stop()
	}
	if c.hijacked || c.ended {
		c.ended = true
		return
	}
	_ = c.sendGathered()
	c.ended = true
	if !c.deadlineSet {
		return
	}
	if c.controller().Flush() == nil {
		_ = c.controller().SetWriteDeadline(time.Time{})
	}
}

// abort sends what is buffered and closes the connection without ending
// the body (no final chunk), so the client sees the transfer fail. A
// writer that cannot be taken over (the Lambda adapter, which buffers the
// whole response) ends the body as it is.
func (c *commitWriter) abort() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sendGathered() != nil || c.controller().Flush() != nil {
		return
	}
	conn, _, err := c.controller().Hijack()
	if err != nil {
		return
	}
	c.hijacked = true
	_ = conn.Close()
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
		_, c.err = c.send(c.buf.Bytes())
	}
	c.buf = bytes.Buffer{}
}

// finish commits whatever is buffered, or sends what was gathered since
// the commit.
func (c *commitWriter) finish() error {
	if !c.committed {
		c.commit(true)
		return c.err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sendGathered()
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

// endFrame emits the compress=1 frame written so far, so what follows
// starts a new one.
func (p *bodyPipeline) endFrame() error {
	if p.native == nil {
		return nil
	}
	return p.native.Flush()
}

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
