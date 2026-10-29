package server

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/exec"
	"github.com/gfronza/twinhouse/internal/format"
	"github.com/gfronza/twinhouse/internal/sqlparse"
)

// summaryStats is the X-ClickHouse-Summary payload.
type summaryStats struct {
	ReadRows, ReadBytes       uint64
	WrittenRows, WrittenBytes uint64
	TotalRowsToRead           uint64
	ResultRows, ResultBytes   uint64
	Elapsed                   time.Duration
}

// summaryJSON renders the summary with ClickHouse 25.8's field order; every
// value is a quoted decimal.
func summaryJSON(s summaryStats) string {
	return fmt.Sprintf(`{"read_rows":"%d","read_bytes":"%d","written_rows":"%d","written_bytes":"%d","total_rows_to_read":"%d","result_rows":"%d","result_bytes":"%d","elapsed_ns":"%d"}`,
		s.ReadRows, s.ReadBytes, s.WrittenRows, s.WrittenBytes, s.TotalRowsToRead, s.ResultRows, s.ResultBytes, s.Elapsed.Nanoseconds())
}

// summary is the X-ClickHouse-Summary of the result when the headers are
// sent: the rows read so far, and the result's rows and bytes only with
// counts (see countsResult).
func (c *call) summary(resultBytes uint64, counts bool) summaryStats {
	st := summaryStats{Elapsed: c.s.now().Sub(c.start)}
	res := c.result
	if res == nil {
		return st
	}
	rs := res.Counts()
	st.ReadRows, st.ReadBytes, st.WrittenRows = rs.ReadRows, rs.ReadBytes, res.WrittenRows
	if len(res.Columns) > 0 {
		st.TotalRowsToRead = rs.ReadRows
		if counts {
			st.ResultRows, st.ResultBytes = rs.ResultRows, resultBytes
		}
		return st
	}
	if res.WrittenRows > 0 {
		if c.data != nil {
			st.WrittenBytes = uint64(c.data.n)
		}
		// ClickHouse reports an INSERT's input as read and as its result.
		if st.ReadRows == 0 {
			st.ReadRows, st.ReadBytes = st.WrittenRows, st.WrittenBytes
		}
		st.ResultRows, st.ResultBytes = st.WrittenRows, st.WrittenBytes
	}
	return st
}

// blockSize is max_block_size, the statement's SETTINGS clause included.
func (c *call) blockSize() int {
	if n, err := strconv.Atoi(c.outputSettings()["max_block_size"]); err == nil && n > 0 {
		return min(n, maxBlockSize)
	}
	return defaultBlockSize
}

// writeResult encodes res in the selected output format, block by block
// as a streamed result yields them. Errors returned before anything was
// sent are rendered by the caller as an HTTP error; errors after the
// response was committed are appended to the body.
func (c *call) writeResult(res *exec.Result) error {
	h := c.w.Header()
	if len(res.Columns) == 0 {
		h.Set("Content-Type", "text/plain; charset=UTF-8")
		h.Set("X-ClickHouse-Summary", summaryJSON(c.summary(0, true)))
		c.committed = true
		c.w.WriteHeader(http.StatusOK)
		return nil
	}

	fmtName := res.Format
	if fmtName == "" {
		fmtName = c.defaultFormat
	}
	if fmtName == "" {
		fmtName = "TabSeparated"
	}
	c.outFormat = fmtName
	canon, ok := format.Canonical(fmtName)
	if !ok {
		if clickhouseOutputFormats[strings.ToLower(fmtName)] {
			return cherr.New(cherr.NotImplemented, "Output format %s is not supported by this server", fmtName)
		}
		return cherr.New(cherr.UnknownFormat, "Unknown format %s", fmtName)
	}

	settings := c.outputSettings()
	payload := &countingWriter{}
	sink := c.newSink(res)
	defer sink.endSend()
	sink.onCommit = func(h http.Header, final bool) {
		c.committed = true
		h.Set("Content-Type", format.ContentType(canon))
		h.Set("X-ClickHouse-Format", fmtName)
		if c.httpEncoding != "" {
			h.Set("Content-Encoding", c.httpEncoding)
		}
		if final {
			h.Set("Content-Length", strconv.Itoa(sink.buf.Len()))
		}
		h.Set("X-ClickHouse-Summary", summaryJSON(c.summary(uint64(payload.n), final && c.countsResult(payload.n))))
	}
	pipe, err := newBodyPipeline(sink, c.nativeCompress, c.httpEncoding, settings)
	if err != nil {
		return err
	}
	payload.w = pipe

	opts := c.encoderOptions()
	if res.Stream != nil {
		opts.Charge = res.Stream.Charge
	}
	enc, err := format.NewEncoder(canon, payload, res.Columns, opts)
	if err != nil {
		return err
	}
	c.outCanon = canon
	out := &output{enc: enc, sink: sink, pipe: pipe}
	if err := c.encodeSafely(res, out); err != nil {
		if err == errHeadersSent {
			return nil
		}
		if gone := cmp.Or(sink.failed(), c.r.Context().Err()); gone != nil {
			return c.clientGone(gone)
		}
		return c.streamFailure(err, out)
	}
	if err := sink.finish(); err != nil {
		return c.clientGone(err)
	}
	return nil
}

// countsResult reports whether a body sent at once has the result's rows
// and bytes in its summary, as in ClickHouse 25.8: when it is empty, or
// the client set its buffering (wait_end_of_query, a buffer size) or
// compression; not with the default buffer.
func (c *call) countsResult(bodyBytes int64) bool {
	return bodyBytes == 0 || c.wholeBody || c.explicitBuffer || c.nativeCompress || c.httpEncoding != ""
}

// encoderOptions are the options of the result's encoder.
func (c *call) encoderOptions() format.Options {
	cpv, _ := strconv.ParseUint(c.params.Get("client_protocol_version"), 10, 64)
	return format.Options{ClientProtocolVersion: cpv, Settings: c.outputSettings(), Timezone: c.tz}
}

// newSink returns the response writer of a result. It buffers
// c.bufferSize bytes before committing (all of them when -1); a streamed
// result counts what is buffered past ResponseBufferSize against the
// query's memory. Each write to the client is bounded by
// http_send_timeout.
func (c *call) newSink(res *exec.Result) *commitWriter {
	free := c.s.cfg.ResponseBufferSize
	sink := &commitWriter{w: c.w, limit: c.bufferSize, free: free, noBody: c.r.Method == http.MethodHead,
		sendTimeout: c.sendTimeout}
	if res.Stream != nil && (sink.limit < 0 || sink.limit > free) {
		sink.charge = res.Stream.Charge
	}
	return sink
}

// output is where a result is encoded: the format encoder, the body
// pipeline under it and the response it ends in.
type output struct {
	enc  format.Encoder
	sink *commitWriter
	pipe *bodyPipeline
}

// endBlock sends an encoded block on, as ClickHouse 25.8 does after each
// block: with compress=1 it ends a frame, and once the response is
// committed the block (with the status and headers, after the commit)
// reaches the client. HTTP compression keeps its own buffering, as there.
func (o *output) endBlock() error {
	if err := o.pipe.endFrame(); err != nil {
		return err
	}
	return o.sink.flush()
}

// errHeadersSent ends a HEAD request's result once its headers are sent.
var errHeadersSent = errors.New("headers sent")

// encodeSafely is encode with a panic turned into a LOGICAL_ERROR, so a
// committed body still ends with the exception and is cut off.
func (c *call) encodeSafely(res *exec.Result, out *output) (err error) {
	defer func() {
		if p := recover(); p != nil {
			c.s.log.Error("panic encoding result", "query_id", c.queryID, "panic", p, "stack", string(debug.Stack()))
			err = cherr.New(cherr.LogicalError, "Internal error: %v", p)
		}
	}()
	return c.encode(res, out)
}

// encode writes res's rows and the format's trailer. It returns the first
// error: the query's, the encoder's or the client connection's. A HEAD
// request ends after the first block with errHeadersSent, its headers
// sent, as ClickHouse answers it then; a HEAD with the whole body
// buffered runs to the end.
func (c *call) encode(res *exec.Result, out *output) error {
	blocks := resultBlocks(res)
	bs := c.blockSize()
	for {
		rows, err := blocks.Next(bs)
		if err == io.EOF {
			break
		}
		if err != nil {
			return c.queryError(err)
		}
		if err := out.enc.WriteRows(rows); err != nil {
			return err
		}
		if err := format.Flush(out.enc); err != nil {
			return err
		}
		if err := out.endBlock(); err != nil {
			return err
		}
		if out.sink.noBody && out.sink.limit >= 0 {
			return out.sink.sendHeaders()
		}
	}
	st := res.Counts()
	err := out.enc.Finish(format.Stats{
		Rows:               st.ResultRows,
		RowsBeforeLimit:    st.RowsBeforeLimit,
		HasRowsBeforeLimit: st.HasRowsBeforeLimit,
		Elapsed:            c.s.now().Sub(c.start),
		RowsRead:           st.ReadRows,
		BytesRead:          st.ReadBytes,
	})
	if err != nil {
		return err
	}
	return out.pipe.close()
}

// blockSource yields a result's rows block by block, as
// exec.ResultStream does.
type blockSource interface {
	Next(maxRows int) ([][]any, error)
}

// resultBlocks returns res's stream, or its collected rows in blocks.
func resultBlocks(res *exec.Result) blockSource {
	if res.Stream != nil {
		return res.Stream
	}
	return &rowBlocks{rows: res.Rows}
}

// rowBlocks yields collected rows in blocks.
type rowBlocks struct{ rows [][]any }

func (b *rowBlocks) Next(maxRows int) ([][]any, error) {
	if len(b.rows) == 0 {
		return nil, io.EOF
	}
	n := min(maxRows, len(b.rows))
	block := b.rows[:n]
	b.rows = b.rows[n:]
	return block, nil
}

// outputSettings returns the settings the output format sees: URL and
// session settings overlaid with the statement's own SETTINGS clause (outermost
// SELECT and its UNION branches, in order), as ClickHouse applies query-level
// settings to the whole query context, output format included. The query is
// re-parsed only when it mentions SETTINGS.
func (c *call) outputSettings() map[string]string {
	if c.outSettings == nil {
		c.outSettings = c.statementSettings()
	}
	return c.outSettings
}

func (c *call) statementSettings() map[string]string {
	if !outputKeywords[c.keyword] || len(c.query) > bodySplitThreshold || !containsFold(c.query, "settings") {
		return c.settings
	}
	st, err := c.parseQuery()
	if err != nil {
		return c.settings
	}
	sel, ok := st.(*sqlparse.SelectStmt)
	if !ok {
		return c.settings
	}
	var out map[string]string
	for _, b := range append([]*sqlparse.SelectStmt{sel}, sel.Union...) {
		for _, kv := range b.Settings {
			if out == nil {
				out = make(map[string]string, len(c.settings)+len(b.Settings))
				for k, v := range c.settings {
					out[k] = v
				}
			}
			out[kv.Name] = settingValueText(kv.Value)
		}
	}
	if out == nil {
		return c.settings
	}
	return out
}

// containsFold reports whether s contains the lower-case ASCII word sub,
// ignoring ASCII case.
func containsFold(s, sub string) bool {
	n := len(sub)
	for i := 0; i+n <= len(s); i++ {
		j := 0
		for ; j < n; j++ {
			ch := s[i+j]
			if 'A' <= ch && ch <= 'Z' {
				ch += 'a' - 'A'
			}
			if ch != sub[j] {
				break
			}
		}
		if j == n {
			return true
		}
	}
	return false
}

// streamFailure handles an error raised while a result is read or
// encoded. Before the response is committed the buffered output is dropped
// and the error is returned (to be rendered as an HTTP error). Afterwards
// the exception ends the body, as endWithException writes it.
func (c *call) streamFailure(err error, out *output) error {
	if !out.sink.committed {
		out.sink.discard()
		return err
	}
	ce := toCHError(err)
	c.errCode, c.errMsg = ce.Code, ce.Msg
	c.endWithException(exceptionText(ce, c.s.cfg.ServerVersion), out)
	return nil
}

// endWithException ends a committed body after every row encoded so far,
// as ClickHouse 25.8 does. The JSON formats carry the exception and end
// normally, unless http_write_exception_in_output_format is 0. The others
// get "__exception__\r\n<text>\n" (with compress=1 in a frame of its own;
// with HTTP compression no text, as there), and then the transfer is cut
// off, so a client that does not look for the marker still sees it fail.
func (c *call) endWithException(text string, out *output) {
	if format.Flush(out.enc) != nil || out.pipe.endFrame() != nil {
		out.sink.abort()
		return
	}
	if c.exceptionInFormat() {
		if ok, err := format.WriteException(out.enc, text); ok {
			if err != nil || out.pipe.close() != nil {
				out.sink.abort()
			}
			return
		}
	}
	if c.httpEncoding == "" {
		if _, err := out.pipe.Write([]byte("__exception__\r\n" + text + "\n")); err == nil {
			_ = out.pipe.endFrame()
		}
	}
	out.sink.abort()
}

// exceptionInFormat is http_write_exception_in_output_format (default 1):
// whether the formats that can carry an exception do. With 0 an error is
// plain text before the commit and __exception__ after it in every
// format; clickhouse-go's QueryFormat sets 0.
func (c *call) exceptionInFormat() bool {
	v := c.outputSettings()["http_write_exception_in_output_format"]
	return v == "" || isTrue(v)
}

// clientGone records a failed write to the client, or a client that went
// away before anything was sent (logged with the error's status, not 200);
// nothing more can be sent.
func (c *call) clientGone(err error) error {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		c.errCode = cherr.SocketTimeout
		c.errMsg = fmt.Sprintf("Timeout exceeded while writing to socket (%s, %d ms)", c.r.RemoteAddr, c.sendTimeout.Milliseconds())
	} else {
		c.errCode, c.errMsg = cherr.QueryWasCancelled, "client connection: "+err.Error()
	}
	if !c.committed {
		c.status = cherr.HTTPStatus(c.errCode)
	}
	return nil
}

// errorFormat returns the output format an error should be rendered in.
func (c *call) errorFormat() string {
	if c.outFormat != "" {
		return c.outFormat
	}
	if outputKeywords[c.keyword] && len(c.query) <= bodySplitThreshold {
		if st, err := c.parseQuery(); err == nil {
			if f := statementFormat(st); f != "" {
				return f
			}
		}
	}
	return c.defaultFormat
}

func statementFormat(st sqlparse.Statement) string {
	switch s := st.(type) {
	case *sqlparse.SelectStmt:
		return s.Format
	case *sqlparse.ShowStmt:
		return s.Format
	case *sqlparse.DescribeStmt:
		return s.Format
	case *sqlparse.ExistsStmt:
		return s.Format
	}
	return ""
}

// fail writes err as a complete ClickHouse error response.
func (c *call) fail(err error) {
	ce := toCHError(err)
	c.errCode, c.errMsg = ce.Code, ce.Msg
	c.status = cherr.HTTPStatus(ce.Code)
	text := exceptionText(ce, c.s.cfg.ServerVersion)

	h := c.w.Header()
	body, ctype := text+"\n", "text/plain; charset=UTF-8"
	if c.compressReady {
		if b, ct, fmtName := c.errorBody(text); fmtName != "" {
			body, ctype = b, ct
			h.Set("X-ClickHouse-Format", fmtName)
		}
	}
	h.Set("Content-Type", ctype)
	h.Set("X-ClickHouse-Exception-Code", strconv.Itoa(ce.Code))
	if ce.Code == cherr.RequiredPassword {
		h.Set("WWW-Authenticate", `Basic realm="ClickHouse server HTTP API"`)
	}
	h.Set("X-ClickHouse-Summary", summaryJSON(c.summary(0, false)))

	payload := []byte(body)
	if c.compressReady && (c.nativeCompress || c.httpEncoding != "") {
		var buf bytes.Buffer
		if pipe, perr := newBodyPipeline(&buf, c.nativeCompress, c.httpEncoding, c.settings); perr == nil {
			_, _ = pipe.Write(payload)
			if pipe.close() == nil {
				payload = buf.Bytes()
				if c.httpEncoding != "" {
					h.Set("Content-Encoding", c.httpEncoding)
				}
			}
		}
	}
	c.committed = true
	c.w.WriteHeader(c.status)
	_, _ = c.w.Write(payload)
}

// errorBody renders an exception in the request's output format. Once the
// result's encoder is set up (an error while its rows are read), it is
// that format's response, as ClickHouse 25.8 sends it: the encoder's own
// exception with no rows (JSON meta included) or the text, with the
// format's Content-Type. Before, only the JSON formats render it.
func (c *call) errorBody(text string) (body, ctype, fmtName string) {
	if c.outCanon != "" {
		return c.resultException(text), format.ContentType(c.outCanon), c.outFormat
	}
	if fmtName = c.errorFormat(); fmtName != "" {
		if canon, ok := format.Canonical(fmtName); ok {
			if b, ok := formatException(canon, text); ok {
				if !c.exceptionInFormat() {
					b = text + "\n"
				}
				return b, format.ContentType(canon), fmtName
			}
		}
	}
	return "", "", ""
}

// resultException is the result's encoder ending with the exception
// before any row, or the plain text for the formats that cannot carry it
// (or with http_write_exception_in_output_format = 0).
func (c *call) resultException(text string) string {
	if !c.exceptionInFormat() {
		return text + "\n"
	}
	var b strings.Builder
	enc, err := format.NewEncoder(c.outCanon, &b, c.result.Columns, c.encoderOptions())
	if err == nil {
		if ok, err := format.WriteException(enc, text); ok && err == nil {
			return b.String()
		}
	}
	return text + "\n"
}

func (c *call) logRequest() {
	log := c.s.log
	attrs := []any{
		"query_id", c.queryID, "user", c.user, "method", c.r.Method,
		"status", c.status, "duration", c.s.now().Sub(c.start),
	}
	if res := c.result; res != nil {
		st := res.Counts()
		attrs = append(attrs, "read_rows", st.ReadRows, "written_rows", res.WrittenRows, "result_rows", st.ResultRows,
			"peak_memory", st.PeakMemory)
	}
	if c.errCode != 0 {
		attrs = append(attrs, "code", c.errCode, "error", c.errMsg, "query", truncate(c.query, 512))
		log.Info("query failed", attrs...)
		return
	}
	if log.Enabled(context.Background(), slog.LevelDebug) {
		attrs = append(attrs, "query", truncate(c.query, 512))
		log.Debug("query", attrs...)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// clickhouseOutputFormats holds (lower-cased) every output format ClickHouse
// 25.8 knows (SELECT name FROM system.formats WHERE is_output). A name in
// this set that the format package does not implement (RowBinary, Parquet,
// ...) answers NOT_IMPLEMENTED (48) per DESIGN §2; any other unknown name is
// UNKNOWN_FORMAT (73) as in ClickHouse, which matches names case-insensitively.
var clickhouseOutputFormats = func() map[string]bool {
	m := map[string]bool{}
	for _, n := range strings.Fields(`Arrow ArrowStream Avro BSONEachRow CSV CSVWithNames
		CSVWithNamesAndTypes CapnProto CustomSeparated CustomSeparatedWithNames
		CustomSeparatedWithNamesAndTypes Hash JSON JSONColumns JSONColumnsWithMetadata JSONCompact
		JSONCompactColumns JSONCompactEachRow JSONCompactEachRowWithNames
		JSONCompactEachRowWithNamesAndTypes JSONCompactEachRowWithProgress JSONCompactStrings
		JSONCompactStringsEachRow JSONCompactStringsEachRowWithNames
		JSONCompactStringsEachRowWithNamesAndTypes JSONCompactStringsEachRowWithProgress JSONEachRow
		JSONEachRowWithProgress JSONLines JSONObjectEachRow JSONStrings JSONStringsEachRow
		JSONStringsEachRowWithProgress LineAsString LineAsStringWithNames LineAsStringWithNamesAndTypes
		Markdown MsgPack MySQLWire NDJSON Native Npy Null ODBCDriver2 ORC Parquet PostgreSQLWire Pretty
		PrettyCompact PrettyCompactMonoBlock PrettyCompactNoEscapes PrettyCompactNoEscapesMonoBlock
		PrettyJSONEachRow PrettyJSONLines PrettyMonoBlock PrettyNDJSON PrettyNoEscapes
		PrettyNoEscapesMonoBlock PrettySpace PrettySpaceMonoBlock PrettySpaceNoEscapes
		PrettySpaceNoEscapesMonoBlock Prometheus Protobuf ProtobufList ProtobufSingle Raw RawBLOB
		RawWithNames RawWithNamesAndTypes RowBinary RowBinaryWithNames RowBinaryWithNamesAndTypes
		SQLInsert TSKV TSV TSVRaw TSVRawWithNames TSVRawWithNamesAndTypes TSVWithNames
		TSVWithNamesAndTypes TabSeparated TabSeparatedRaw TabSeparatedRawWithNames
		TabSeparatedRawWithNamesAndTypes TabSeparatedWithNames TabSeparatedWithNamesAndTypes Template
		Values Vertical XML`) {
		m[strings.ToLower(n)] = true
	}
	return m
}()
