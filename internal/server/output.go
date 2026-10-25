package server

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/exec"
	"dynamodb-eventstream/internal/format"
	"dynamodb-eventstream/internal/sqlparse"
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

func (c *call) summary(resultBytes uint64) summaryStats {
	st := summaryStats{Elapsed: c.s.now().Sub(c.start)}
	res := c.result
	if res == nil {
		return st
	}
	st.ReadRows, st.ReadBytes, st.WrittenRows = res.ReadRows, res.ReadBytes, res.WrittenRows
	if len(res.Columns) > 0 {
		st.TotalRowsToRead = res.ReadRows
		st.ResultRows, st.ResultBytes = uint64(len(res.Rows)), resultBytes
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

func (c *call) blockSize() int {
	if n, err := strconv.Atoi(c.setting("max_block_size")); err == nil && n > 0 {
		return min(n, maxBlockSize)
	}
	return defaultBlockSize
}

// writeResult encodes res in the selected output format. Errors returned
// before anything was sent are rendered by the caller as an HTTP error;
// errors after the response was committed are appended to the body.
func (c *call) writeResult(res *exec.Result) error {
	h := c.w.Header()
	if len(res.Columns) == 0 {
		h.Set("Content-Type", "text/plain; charset=UTF-8")
		h.Set("X-ClickHouse-Summary", summaryJSON(c.summary(0)))
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
	limit := c.s.cfg.ResponseBufferSize
	if isTrue(c.setting("wait_end_of_query")) {
		limit = -1
	}
	payload := &countingWriter{}
	sink := &commitWriter{w: c.w, limit: limit, noBody: c.r.Method == http.MethodHead}
	sink.onCommit = func(h http.Header, final bool) {
		c.committed = true
		h.Set("Content-Type", format.ContentType(canon))
		h.Set("X-ClickHouse-Format", fmtName)
		if c.httpEncoding != "" {
			h.Set("Content-Encoding", c.httpEncoding)
		}
		var rb uint64
		if final {
			rb = uint64(payload.n)
			h.Set("Content-Length", strconv.Itoa(sink.buf.Len()))
		}
		h.Set("X-ClickHouse-Summary", summaryJSON(c.summary(rb)))
	}
	pipe, err := newBodyPipeline(sink, c.nativeCompress, c.httpEncoding, settings)
	if err != nil {
		return err
	}
	payload.w = pipe

	cpv, _ := strconv.ParseUint(c.params.Get("client_protocol_version"), 10, 64)
	enc, err := format.NewEncoder(canon, payload, res.Columns, format.Options{
		ClientProtocolVersion: cpv,
		Settings:              settings,
		Timezone:              c.tz,
	})
	if err != nil {
		return err
	}
	bs := c.blockSize()
	for rows := res.Rows; len(rows) > 0; {
		n := min(bs, len(rows))
		if err := enc.WriteRows(rows[:n]); err != nil {
			return c.streamFailure(err, sink, pipe)
		}
		rows = rows[n:]
		if sink.err != nil {
			return c.clientGone(sink.err)
		}
	}
	err = enc.Finish(format.Stats{
		Rows:               uint64(len(res.Rows)),
		RowsBeforeLimit:    res.RowsBeforeLimit,
		HasRowsBeforeLimit: res.HasRowsBeforeLimit,
		Elapsed:            c.s.now().Sub(c.start),
		RowsRead:           res.ReadRows,
		BytesRead:          res.ReadBytes,
	})
	if err != nil {
		return c.streamFailure(err, sink, pipe)
	}
	if err := pipe.close(); err != nil {
		return c.streamFailure(err, sink, pipe)
	}
	if err := sink.finish(); err != nil {
		return c.clientGone(err)
	}
	return nil
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

// streamFailure handles an error raised while encoding. Before the
// response is committed the buffered output is dropped and the error is
// returned (to be rendered as an HTTP error). Afterwards the exception is
// appended to the body the way ClickHouse 25.8 does
// ("__exception__\r\n<text>\n"), which clickhouse-go recognises.
func (c *call) streamFailure(err error, sink *commitWriter, pipe *bodyPipeline) error {
	if !sink.committed {
		sink.discard()
		return err
	}
	ce := toCHError(err)
	c.errCode, c.errMsg = ce.Code, ce.Msg
	if sink.err == nil {
		_, _ = pipe.Write([]byte("__exception__\r\n" + exceptionText(ce, c.s.cfg.ServerVersion) + "\n"))
		_ = pipe.close()
	}
	return nil
}

// clientGone records a failed write to the client; nothing more can be sent.
func (c *call) clientGone(err error) error {
	c.errCode, c.errMsg = cherr.QueryWasCancelled, "client connection: "+err.Error()
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
		if fmtName := c.errorFormat(); fmtName != "" {
			if canon, ok := format.Canonical(fmtName); ok {
				if b, ok := formatException(canon, text); ok {
					body, ctype = b, format.ContentType(canon)
					h.Set("X-ClickHouse-Format", fmtName)
				}
			}
		}
	}
	h.Set("Content-Type", ctype)
	h.Set("X-ClickHouse-Exception-Code", strconv.Itoa(ce.Code))
	if ce.Code == cherr.RequiredPassword {
		h.Set("WWW-Authenticate", `Basic realm="ClickHouse server HTTP API"`)
	}
	h.Set("X-ClickHouse-Summary", summaryJSON(summaryStats{Elapsed: c.s.now().Sub(c.start)}))

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

func (c *call) logRequest() {
	log := c.s.log
	attrs := []any{
		"query_id", c.queryID, "user", c.user, "method", c.r.Method,
		"status", c.status, "duration", c.s.now().Sub(c.start),
	}
	if res := c.result; res != nil {
		attrs = append(attrs, "read_rows", res.ReadRows, "written_rows", res.WrittenRows, "result_rows", len(res.Rows))
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
