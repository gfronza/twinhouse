package exec

import (
	"context"
	"errors"
	"io"
	"math"
	"strconv"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/memtrack"
)

// resultBlockBytes ends a result block early once its rows hold about this
// many estimated bytes (ClickHouse's preferred_block_size_bytes).
const resultBlockBytes = 1_000_000

// resultStream is the ResultStream of a top-level SELECT: it reads the
// plan's iterator block by block, so only the current block is held.
type resultStream struct {
	q *query
	// rel is opened by the first Next, so an error while the plan reads
	// its input (a sort, GROUP BY or JOIN build) comes after the output
	// is set up, as an execution error in ClickHouse; it is nil until then.
	rel   *relation
	it    rowIter
	stats *limitStats
	// block is the block Next returned last. Its rows are charged to
	// blockMem until the next call.
	block    [][]any
	blockMem *memtrack.Account
	sz       *rowSizer
	// held is what the caller charged with Charge.
	held *memtrack.Account
	// maxRows and maxBytes are max_result_rows and max_result_bytes (0:
	// none); overflowBreak is result_overflow_mode = 'break'.
	maxRows, maxBytes uint64
	overflowBreak     bool
	// timeoutBreak is timeout_overflow_mode = 'break': the stream ends
	// when the query's deadline passes.
	timeoutBreak bool
	// colTypes size the rows for max_result_bytes.
	colTypes []chtype.Type
	// read and readBytes count the rows read into blocks and their
	// result bytes; returned counts the rows returned.
	read, readBytes, returned uint64
	// blockBytes are the current block's result bytes.
	blockBytes uint64
	// carry is a row read past the end of the last block's source block;
	// it starts the next block, whose source block ends when read_rows
	// reaches carryEnd.
	carry        []any
	carryEnd     uint64
	done, closed bool
	err          error
}

// streamSelect returns rel's rows as a stream. The query finishes when
// the stream is closed.
func (q *query) streamSelect(rel *relation, fmtName string) *Result {
	s := &resultStream{
		q: q, rel: rel, stats: rel.stats, sz: newRowSizer(rel.cols),
		blockMem: q.lifetimeAccount(), held: q.lifetimeAccount(),
		maxRows: uint64(q.maxResultRows()), maxBytes: q.sizeSetting("max_result_bytes"),
		overflowBreak: q.settings["result_overflow_mode"] == "break",
		timeoutBreak:  q.settings["timeout_overflow_mode"] == "break",
	}
	if s.maxBytes > 0 {
		s.colTypes = columnTypes(rel.cols)
	}
	return &Result{Columns: rel.cols, Format: fmtName, Stream: s}
}

func (s *resultStream) Next(maxRows int) (_ [][]any, err error) {
	defer s.keepError(&err)
	defer recoverInternal(&err)
	s.releaseBlock()
	if s.err != nil {
		return nil, s.err
	}
	if s.done || s.closed {
		return nil, io.EOF
	}
	if err := s.open(); err != nil {
		return nil, err
	}
	if err := s.readBlock(max(maxRows, 1)); err != nil {
		return nil, err
	}
	if len(s.block) == 0 {
		return nil, io.EOF
	}
	if err := s.applyResultLimits(); err != nil {
		return nil, err
	}
	s.returned += uint64(len(s.block))
	return s.block, nil
}

// keepError keeps the first error, a recovered panic's too, so the
// stream does not read on after one.
func (s *resultStream) keepError(err *error) {
	if *err != nil && *err != io.EOF && s.err == nil {
		s.err = *err
	}
}

// open opens the plan's iterator on the first call.
func (s *resultStream) open() error {
	if s.it != nil {
		return nil
	}
	it, err := s.rel.open()
	if err != nil {
		return err
	}
	s.it, s.rel = it, nil
	return nil
}

// readBlock reads up to maxRows rows, or about resultBlockBytes, into
// s.block and charges them. A block also ends with the source block its
// rows come from (maxRows input rows, as the query counts them in
// read_rows), as in ClickHouse, which filters each source block into one
// result block: a selective filter does not hold rows back until maxRows
// of them pass it. The end is seen when a row of a later source block
// passes; that row starts the next block (s.carry).
func (s *resultStream) readBlock(maxRows int) error {
	if err := s.q.ctx.Err(); err != nil {
		return s.endOnTimeout(cherr.Wrap(cherr.Timeout, err, "query cancelled"))
	}
	bytes := int64(0)
	sourceEnd := s.q.readRows.Load() + uint64(maxRows)
	if row := s.carry; row != nil {
		s.carry, sourceEnd = nil, s.carryEnd
		if err := s.add(row, &bytes); err != nil {
			return err
		}
	}
	for len(s.block) < maxRows && bytes < resultBlockBytes {
		row, err := s.it.next()
		if err == io.EOF {
			s.done = true
			return nil
		}
		if err != nil {
			return s.endOnTimeout(err)
		}
		if read := s.q.readRows.Load(); read > sourceEnd {
			// The row is from a later source block: the one that holds
			// it ends a whole number of source blocks further on.
			sourceEnd += (read - sourceEnd + uint64(maxRows) - 1) / uint64(maxRows) * uint64(maxRows)
			if len(s.block) > 0 {
				s.carry, s.carryEnd = row, sourceEnd
				return nil
			}
		}
		if err := s.add(row, &bytes); err != nil {
			return err
		}
	}
	return nil
}

// add appends row to the block and charges it; bytes counts the block's
// estimated size.
func (s *resultStream) add(row []any, bytes *int64) error {
	n := s.sz.size(row) + sliceHeader
	if err := s.blockMem.Grow(n); err != nil {
		return err
	}
	s.block = append(s.block, row)
	*bytes += n
	if s.colTypes != nil {
		s.blockBytes += resultRowBytes(s.colTypes, row)
	}
	return nil
}

// endOnTimeout ends the stream after the rows read so far instead of
// failing with err when the query's deadline (max_execution_time) has
// passed and timeout_overflow_mode is 'break', as in ClickHouse.
func (s *resultStream) endOnTimeout(err error) error {
	if s.timeoutBreak && errors.Is(s.q.ctx.Err(), context.DeadlineExceeded) {
		s.done = true
		return nil
	}
	return err
}

// applyResultLimits counts the block and applies max_result_rows and
// max_result_bytes per block, as ClickHouse does: with throw, the block
// that takes the totals past a limit fails with 396; with break, the block
// that reaches a limit is the last one.
func (s *resultStream) applyResultLimits() error {
	s.read += uint64(len(s.block))
	s.readBytes += s.blockBytes
	rowsOver, bytesOver := s.maxRows > 0 && s.read >= s.maxRows, s.maxBytes > 0 && s.readBytes >= s.maxBytes
	switch {
	case s.overflowBreak:
		s.done = rowsOver || bytesOver
	case rowsOver && s.read > s.maxRows:
		return cherr.New(cherr.TooManyRowsOrBytes, "Limit for result exceeded, max rows: %s, current rows: %s",
			readableQuantity(s.maxRows), readableQuantity(s.read))
	case bytesOver && s.readBytes > s.maxBytes:
		return cherr.New(cherr.TooManyRowsOrBytes, "Limit for result exceeded, max bytes: %s, current bytes: %s",
			memtrack.ReadableSize(int64(min(s.maxBytes, math.MaxInt64))), memtrack.ReadableSize(int64(min(s.readBytes, math.MaxInt64))))
	}
	return nil
}

// releaseBlock drops the last block and its charge.
func (s *resultStream) releaseBlock() {
	if len(s.block) == 0 {
		return
	}
	clear(s.block)
	s.block = s.block[:0]
	s.blockBytes = 0
	s.blockMem.Close()
	s.sz.forget()
}

func (s *resultStream) Stats() Stats {
	st := Stats{
		ReadRows: s.q.readRows.Load(), ReadBytes: s.q.readBytes.Load(),
		ResultRows: s.returned, PeakMemory: s.q.mem.Peak(),
	}
	if s.stats != nil && s.stats.hasLimit {
		st.HasRowsBeforeLimit, st.RowsBeforeLimit = true, s.stats.rowsBeforeLimit
	}
	return st
}

func (s *resultStream) Charge(n int64) error {
	if n < 0 {
		s.held.Shrink(-n)
		return nil
	}
	return s.held.Grow(n)
}

func (s *resultStream) Close() {
	if s.closed {
		return
	}
	s.closed = true
	defer s.q.finish()
	s.releaseBlock()
	s.block, s.carry = nil, nil
	if s.it != nil {
		s.it.close()
	}
}

// readableQuantity renders n like ClickHouse's formatReadableQuantity:
// two decimals and a thousand/million/... unit.
func readableQuantity(n uint64) string {
	units := []string{"", " thousand", " million", " billion", " trillion", " quadrillion", " quintillion"}
	x, i := float64(n), 0
	for ; i+1 < len(units) && x >= 1000; i++ {
		x /= 1000
	}
	return strconv.FormatFloat(x, 'f', 2, 64) + units[i]
}
