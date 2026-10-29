package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"slices"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/limits"
)

// Limits guarding against corrupt or hostile input.
const (
	maxNativeBlockRows = 1 << 30
	maxNativeColumns   = 1 << 16
	maxNativeString    = 1 << 30
	// nativeByteChunk is how many bytes of a string, FixedString or null
	// mask may be allocated ahead of the bytes actually read.
	nativeByteChunk = 1 << 20
	// maxNativeValuesWithoutData is how many slots for values of types
	// without bytes on the wire (Tuple()) one block may allocate on top of
	// one per byte it has read.
	maxNativeValuesWithoutData = 1 << 20
)

// binReader reads little-endian primitives, mapping truncation to a
// ClickHouse-style error.
type binReader struct {
	r    *bufio.Reader
	src  *byteCounter // under r
	tmp  [8]byte
	buf  []byte // reused by reads of up to nativeByteChunk bytes
	bulk []byte // reused by column reads of fixed-width values
	// noData counts the slots for values without bytes on the wire
	// allocated in the block that began at input offset blockStart (see
	// emptyValues).
	noData     int
	blockStart int64
}

// byteCounter counts the bytes read through it.
type byteCounter struct {
	r io.Reader
	n int64
}

func (c *byteCounter) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func newBinReader(r io.Reader) binReader {
	src := &byteCounter{r: r}
	return binReader{r: bufio.NewReaderSize(src, 64<<10), src: src}
}

// offset returns how many input bytes have been consumed.
func (r *binReader) offset() int64 { return r.src.n - int64(r.r.Buffered()) }

func errTruncated(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return cherr.New(cherr.CannotReadAllData, "Cannot read all data in Native format: unexpected end of input")
	}
	return err
}

func (r *binReader) uvarint() (uint64, error) {
	v, err := binary.ReadUvarint(r.r)
	if err != nil {
		return 0, errTruncated(err)
	}
	return v, nil
}

// full reads n bytes into a buffer that the next read may reuse.
func (r *binReader) full(n int) ([]byte, error) {
	var b []byte
	switch {
	case n <= len(r.tmp):
		b = r.tmp[:n]
	case n <= nativeByteChunk:
		if cap(r.buf) < n {
			r.buf = make([]byte, n)
		}
		b = r.buf[:n]
	default:
		return r.bytes(n)
	}
	if _, err := io.ReadFull(r.r, b); err != nil {
		return nil, errTruncated(err)
	}
	return b, nil
}

// bytes reads n bytes into a new buffer that grows as they arrive.
func (r *binReader) bytes(n int) ([]byte, error) {
	buf, err := AppendFull(r.r, nil, n)
	if err != nil {
		return nil, errTruncated(err)
	}
	return buf, nil
}

// column reads n bytes into a buffer that grows as they arrive and that the
// next column read reuses.
func (r *binReader) column(n int) ([]byte, error) {
	b, err := AppendFull(r.r, r.bulk[:0], n)
	r.bulk = b[:0]
	if err != nil {
		return nil, errTruncated(err)
	}
	return b, nil
}

// AppendFull appends n bytes read from r to buf. buf grows as the bytes
// arrive, so a short input cannot make it allocate n bytes up front. On a
// short read it returns what was read with io.ErrUnexpectedEOF or io.EOF.
func AppendFull(r io.Reader, buf []byte, n int) ([]byte, error) {
	want := len(buf) + n
	for len(buf) < want {
		if len(buf) == cap(buf) {
			buf = slices.Grow(buf, min(want-len(buf), max(len(buf), nativeByteChunk)))
		}
		got, err := io.ReadFull(r, buf[len(buf):min(want, cap(buf))])
		buf = buf[:len(buf)+got]
		if err != nil {
			return buf, err
		}
	}
	return buf, nil
}

func (r *binReader) u64() (uint64, error) {
	b, err := r.full(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func (r *binReader) str() (string, error) {
	n, err := r.uvarint()
	if err != nil {
		return "", err
	}
	if n > maxNativeString {
		return "", cherr.New(cherr.TooLargeStringSize, "Too large string size.")
	}
	if n == 0 {
		return "", nil
	}
	buf, err := r.full(int(n))
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

// nativeDecoder reads Native blocks (revision 0 layout, as ClickHouse
// expects for HTTP input and clickhouse-go produces) and returns rows mapped
// onto the target columns by name.
type nativeDecoder struct {
	r         binReader
	cols      []Column
	byName    map[string]int
	skipUnkn  bool
	block     [][]any // decoded block, column-major
	blockMap  []int   // block column -> target column (-1: skipped)
	blockConv []func(any) (any, error)
	// blockEmpty holds, per block column, the codec of a type without bytes
	// on the wire; every row of such a column is its shared empty value.
	blockEmpty []*nativeCodec
	rows, pos  int
}

func newNativeDecoder(r io.Reader, cols []Column, opts Options) *nativeDecoder {
	d := &nativeDecoder{
		r:        newBinReader(r),
		cols:     cols,
		byName:   make(map[string]int, len(cols)),
		skipUnkn: settingBool(opts.Settings, "input_format_skip_unknown_fields", true),
	}
	for i, c := range cols {
		d.byName[c.Name] = i
	}
	return d
}

func (d *nativeDecoder) Next() ([]any, error) {
	for d.pos >= d.rows {
		if err := d.readBlock(); err != nil {
			return nil, err
		}
	}
	row := make([]any, len(d.cols))
	for i := range row {
		row[i] = Missing
	}
	for bi, ti := range d.blockMap {
		if ti < 0 {
			continue
		}
		var v any
		if empty := d.blockEmpty[bi]; empty != nil {
			v = empty.empty
		} else {
			v = d.block[bi][d.pos]
		}
		if conv := d.blockConv[bi]; conv != nil {
			var err error
			if v, err = conv(v); err != nil {
				return nil, columnErr(err, d.cols[ti].Name)
			}
		}
		row[ti] = v
	}
	d.pos++
	return row, nil
}

func (d *nativeDecoder) readBlock() error {
	if _, err := d.r.r.Peek(1); err != nil {
		if errors.Is(err, io.EOF) {
			return io.EOF
		}
		return err
	}
	d.r.noData, d.r.blockStart = 0, d.r.offset()
	ncols, err := d.r.uvarint()
	if err != nil {
		return err
	}
	nrows, err := d.r.uvarint()
	if err != nil {
		return err
	}
	if ncols > maxNativeColumns || nrows > maxNativeBlockRows {
		return cherr.New(cherr.IncorrectData, "Native block of %d columns x %d rows is too large", ncols, nrows)
	}
	d.block = d.block[:0]
	d.blockMap = d.blockMap[:0]
	d.blockConv = d.blockConv[:0]
	d.blockEmpty = d.blockEmpty[:0]
	seen := make(map[int]bool, len(d.cols))
	for i := uint64(0); i < ncols; i++ {
		name, err := d.r.str()
		if err != nil {
			return err
		}
		typName, err := d.r.str()
		if err != nil {
			return err
		}
		typ, err := chtype.Parse(typName)
		if err != nil {
			return cherr.Wrap(cherr.As(err).Code, nil, "type of column %s in Native input: %s", name, cherr.As(err).Msg)
		}
		codec, err := compileNative(typ)
		if err != nil {
			return columnErr(err, name)
		}
		var vals []any
		var empty *nativeCodec
		if codec.noData {
			empty = codec
		} else if nrows > 0 {
			if err := codec.readPrefix(&d.r); err != nil {
				return columnErr(err, name)
			}
			if vals, err = codec.readData(&d.r, int(nrows)); err != nil {
				return columnErr(err, name)
			}
		}
		ti, ok := d.byName[name]
		switch {
		case !ok && !d.skipUnkn:
			return cherr.New(cherr.IncorrectData, "Unknown column with name %s found while reading data in Native format", name)
		case !ok:
			ti = -1
		case seen[ti]:
			return cherr.New(cherr.IncorrectData, "Duplicate column %s in Native input", name)
		}
		var conv func(any) (any, error)
		if ti >= 0 {
			seen[ti] = true
			if dst := d.cols[ti].Type; !dst.Equal(typ) {
				src := typ
				conv = func(v any) (any, error) { return chtype.Convert(v, src, dst) }
			}
		}
		d.block = append(d.block, vals)
		d.blockMap = append(d.blockMap, ti)
		d.blockConv = append(d.blockConv, conv)
		d.blockEmpty = append(d.blockEmpty, empty)
	}
	d.rows, d.pos = int(nrows), 0
	return nil
}

// readPrefix reads serialization state prefixes for the type tree.
func (c *nativeCodec) readPrefix(r *binReader) error {
	switch c.kind {
	case chtype.LowCardinality:
		v, err := r.u64()
		if err != nil {
			return err
		}
		if v != lcSharedDictionariesWithAdditionalKeys {
			return cherr.New(cherr.IncorrectData, "invalid LowCardinality key serialization version %d", v)
		}
		return nil
	case chtype.Nullable, chtype.Array, chtype.SimpleAggregateFunction:
		return c.elem.readPrefix(r)
	case chtype.Map:
		if err := c.key.readPrefix(r); err != nil {
			return err
		}
		return c.elem.readPrefix(r)
	case chtype.Tuple:
		for _, f := range c.fields {
			if err := f.readPrefix(r); err != nil {
				return err
			}
		}
	}
	return nil
}

// readData reads n values of the column in canonical representation.
func (c *nativeCodec) readData(r *binReader, n int) ([]any, error) {
	return c.readDataMasked(r, n, nil)
}

// readDataMasked is readData where nulls (if non-nil) marks rows that are
// NULL in an enclosing Nullable: their nested values are placeholders and
// are not validated (e.g. Enum slots holding 0). A column is allocated only
// as its bytes arrive, so a short body cannot make n allocate up front.
func (c *nativeCodec) readDataMasked(r *binReader, n int, nulls []byte) ([]any, error) {
	if c.noData {
		return r.emptyValues(n, c.empty)
	}
	switch c.kind {
	case chtype.Nullable:
		return c.readNullable(r, n)
	case chtype.SimpleAggregateFunction:
		return c.elem.readDataMasked(r, n, nulls)
	case chtype.Array:
		return c.readArray(r, n)
	case chtype.Map:
		return c.readMap(r, n)
	case chtype.Tuple:
		return c.readTuple(r, n)
	case chtype.LowCardinality:
		return c.readLowCardinality(r, n)
	}
	if w := c.fixedWidth(); w > 0 {
		return c.readFixed(r, n, w, nulls)
	}
	out := make([]any, 0, min(n, limits.NativeAllocChunkRows))
	for len(out) < n {
		if len(out) == cap(out) {
			out = slices.Grow(out, min(n-len(out), 3*len(out)))
		}
		v, err := c.readValue(r)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// emptyValues returns n copies of empty, the value of a type without bytes
// on the wire. Nothing in the body shows they are there, so their slots are
// counted against the block: at most maxNativeValuesWithoutData plus one
// per byte the block has read.
func (r *binReader) emptyValues(n int, empty any) ([]any, error) {
	r.noData += n
	if allowed := maxNativeValuesWithoutData + int(r.offset()-r.blockStart); r.noData > allowed {
		return nil, cherr.New(cherr.TooLargeArraySize,
			"Too large array size: %d nested values without data in a Native block. The maximum for this block is: %d",
			r.noData, allowed)
	}
	out := make([]any, n)
	for i := range out {
		out[i] = empty
	}
	return out, nil
}

// emptyRows returns the rows of an Array or Map column whose elements have
// no bytes on the wire. The rows share one run of empty as long as the
// longest row, so only that row counts against the block.
func (r *binReader) emptyRows(offs []uint64, empty any) ([]any, error) {
	var longest, start uint64
	for _, end := range offs {
		longest, start = max(longest, end-start), end
	}
	run, err := r.emptyValues(int(longest), empty)
	if err != nil {
		return nil, err
	}
	out := make([]any, len(offs))
	start = 0
	for i, end := range offs {
		n := end - start
		out[i] = run[:n:n]
		start = end
	}
	return out, nil
}

// fixedWidth returns the bytes one value of a scalar type takes on the
// wire, or 0 when that varies (String) or the type is not a scalar.
func (c *nativeCodec) fixedWidth() int {
	switch c.kind {
	case chtype.Nothing, chtype.Bool, chtype.UInt8, chtype.Int8, chtype.Enum8:
		return 1
	case chtype.UInt16, chtype.Int16, chtype.Date, chtype.Enum16:
		return 2
	case chtype.UInt32, chtype.Int32, chtype.Float32, chtype.Date32, chtype.DateTime:
		return 4
	case chtype.UInt64, chtype.Int64, chtype.Float64, chtype.DateTime64:
		return 8
	case chtype.UUID:
		return 16
	}
	return 0
}

// readFixed reads n values of w bytes each. The bytes are read first, so
// the values are allocated only once the body has shown they are there.
func (c *nativeCodec) readFixed(r *binReader, n, w int, nulls []byte) ([]any, error) {
	b, err := r.column(n * w)
	if err != nil {
		return nil, err
	}
	out := make([]any, n)
	for i := range out {
		if out[i], err = c.decodeFixed(b[i*w:], i, nulls); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// decodeFixed decodes row i of a fixed-width column from the start of b.
func (c *nativeCodec) decodeFixed(b []byte, i int, nulls []byte) (any, error) {
	le := binary.LittleEndian
	switch c.kind {
	case chtype.Nothing:
		return nil, nil
	case chtype.Bool:
		return b[0] != 0, nil
	case chtype.UInt8:
		return uint64(b[0]), nil
	case chtype.UInt16:
		return uint64(le.Uint16(b)), nil
	case chtype.UInt32:
		return uint64(le.Uint32(b)), nil
	case chtype.UInt64:
		return le.Uint64(b), nil
	case chtype.Int8:
		return int64(int8(b[0])), nil
	case chtype.Int16:
		return int64(int16(le.Uint16(b))), nil
	case chtype.Int32:
		return int64(int32(le.Uint32(b))), nil
	case chtype.Int64:
		return int64(le.Uint64(b)), nil
	case chtype.Float32:
		return float64(math.Float32frombits(le.Uint32(b))), nil
	case chtype.Float64:
		return math.Float64frombits(le.Uint64(b)), nil
	case chtype.UUID:
		return decodeUUID(b), nil
	case chtype.Date:
		return time.Unix(int64(le.Uint16(b))*86400, 0).UTC(), nil
	case chtype.Date32:
		return time.Unix(int64(int32(le.Uint32(b)))*86400, 0).UTC(), nil
	case chtype.DateTime:
		return time.Unix(int64(le.Uint32(b)), 0).UTC(), nil
	case chtype.DateTime64:
		p := pow10[c.t.Precision]
		ticks := int64(le.Uint64(b))
		sec := floorDiv(ticks, p)
		return time.Unix(sec, (ticks-sec*p)*pow10[9-c.t.Precision]).UTC(), nil
	case chtype.Enum8:
		return c.enumName(int64(int8(b[0])), i, nulls)
	}
	return c.enumName(int64(int16(le.Uint16(b))), i, nulls) // Enum16
}

// readValue reads one value of a variable-width scalar type.
func (c *nativeCodec) readValue(r *binReader) (any, error) {
	switch c.kind {
	case chtype.String:
		return r.str()
	case chtype.FixedString:
		b, err := r.full(c.t.N)
		return string(b), err
	}
	return nil, cherr.New(cherr.NotImplemented, "Native deserialization of %s is not supported", c.t.String())
}

func decodeUUID(b []byte) string {
	var u [16]byte
	for j := 0; j < 8; j++ {
		u[j] = b[7-j]
		u[8+j] = b[15-j]
	}
	return formatUUID(&u)
}

// enumName returns the member of value v, or nil for a placeholder in a
// NULL row i.
func (c *nativeCodec) enumName(v int64, i int, nulls []byte) (any, error) {
	name, ok := c.enumByValue[v]
	if !ok {
		if nulls != nil && nulls[i] != 0 {
			return nil, nil
		}
		return nil, cherr.New(cherr.IncorrectData, "unexpected value %d for %s", v, c.t.String())
	}
	return name, nil
}

func (c *nativeCodec) readNullable(r *binReader, n int) ([]any, error) {
	nulls, err := r.bytes(n)
	if err != nil {
		return nil, err
	}
	vals, err := c.elem.readDataMasked(r, n, nulls)
	if err != nil {
		return nil, err
	}
	for i, isNull := range nulls {
		if isNull != 0 {
			vals[i] = nil
		}
	}
	return vals, nil
}

// readOffsets reads the n end offsets of an Array or Map column.
func (c *nativeCodec) readOffsets(r *binReader, n int) ([]uint64, error) {
	b, err := r.column(n * 8)
	if err != nil {
		return nil, err
	}
	offs := make([]uint64, n)
	var prev uint64
	for i := range offs {
		x := binary.LittleEndian.Uint64(b[i*8:])
		if x < prev || x-prev > maxNativeBlockRows {
			return nil, cherr.New(cherr.IncorrectData, "invalid offsets in Native %s column", c.t.String())
		}
		offs[i], prev = x, x
	}
	if prev > maxNativeBlockRows {
		return nil, cherr.New(cherr.IncorrectData, "too many nested values in Native %s column", c.t.String())
	}
	return offs, nil
}

func (c *nativeCodec) readArray(r *binReader, n int) ([]any, error) {
	offs, err := c.readOffsets(r, n)
	if err != nil {
		return nil, err
	}
	if c.elem.noData {
		return r.emptyRows(offs, c.elem.empty)
	}
	flat, err := c.elem.readData(r, lastOffset(offs))
	if err != nil {
		return nil, err
	}
	out := make([]any, len(offs))
	var start uint64
	for i, end := range offs {
		out[i] = flat[start:end:end]
		start = end
	}
	return out, nil
}

func (c *nativeCodec) readMap(r *binReader, n int) ([]any, error) {
	offs, err := c.readOffsets(r, n)
	if err != nil {
		return nil, err
	}
	if c.key.noData && c.elem.noData {
		return r.emptyRows(offs, []any{c.key.empty, c.elem.empty})
	}
	cols, err := readColumns(r, lastOffset(offs), c.key, c.elem)
	if err != nil {
		return nil, err
	}
	keys, vals := cols[0], cols[1]
	out := make([]any, len(offs))
	var start uint64
	for i, end := range offs {
		pairs := make([]any, 0, end-start)
		for j := start; j < end; j++ {
			pairs = append(pairs, []any{keys[j], vals[j]})
		}
		out[i] = pairs
		start = end
	}
	return out, nil
}

func lastOffset(offs []uint64) int {
	if len(offs) == 0 {
		return 0
	}
	return int(offs[len(offs)-1])
}

func (c *nativeCodec) readTuple(r *binReader, n int) ([]any, error) {
	cols, err := readColumns(r, n, c.fields...)
	if err != nil {
		return nil, err
	}
	out := make([]any, n)
	for i := range out {
		t := make([]any, len(cols))
		for fi, col := range cols {
			t[fi] = col[i]
		}
		out[i] = t
	}
	return out, nil
}

// readColumns reads n values of each codec, whose columns follow each other
// on the wire. Columns of types without bytes on the wire are made last, so
// the bytes of the others count towards their allowance (see emptyValues).
func readColumns(r *binReader, n int, codecs ...*nativeCodec) ([][]any, error) {
	cols := make([][]any, len(codecs))
	for _, noData := range []bool{false, true} {
		for i, f := range codecs {
			if f.noData != noData {
				continue
			}
			col, err := f.readData(r, n)
			if err != nil {
				return nil, err
			}
			cols[i] = col
		}
	}
	return cols, nil
}

func (c *nativeCodec) readLowCardinality(r *binReader, n int) ([]any, error) {
	var out []any
	// A LowCardinality column may be split into several granules, each
	// with its own (or a shared, updated) dictionary.
	var dict []any
	for len(out) < n {
		flags, err := r.u64()
		if err != nil {
			return nil, err
		}
		keyType := flags & 0xff
		if keyType > 3 {
			return nil, cherr.New(cherr.IncorrectData, "invalid LowCardinality index type %d", keyType)
		}
		if flags&(1<<8) != 0 {
			return nil, cherr.New(cherr.NotImplemented, "LowCardinality global dictionaries are not supported in Native input")
		}
		if flags&lcHasAdditionalKeysBit == 0 {
			return nil, cherr.New(cherr.IncorrectData, "LowCardinality additional keys bit is missing")
		}
		dictSize, err := r.u64()
		if err != nil {
			return nil, err
		}
		if dictSize > maxNativeBlockRows {
			return nil, cherr.New(cherr.IncorrectData, "LowCardinality dictionary of %d entries is too large", dictSize)
		}
		if dict, err = c.elem.readData(r, int(dictSize)); err != nil {
			return nil, err
		}
		nkeys, err := r.u64()
		if err != nil {
			return nil, err
		}
		if nkeys > uint64(n-len(out)) {
			return nil, cherr.New(cherr.IncorrectData, "LowCardinality has %d keys, expected at most %d", nkeys, n-len(out))
		}
		if nkeys == 0 {
			return nil, cherr.New(cherr.IncorrectData, "empty LowCardinality granule")
		}
		if out, err = c.appendLowCardinalityKeys(r, out, dict, int(nkeys), keyType); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// appendLowCardinalityKeys reads nkeys dictionary indexes of the width
// keyType names (0: 1 byte, 1: 2 bytes, 2: 4 bytes, 3: 8 bytes) and appends
// their values to out.
func (c *nativeCodec) appendLowCardinalityKeys(r *binReader, out, dict []any, nkeys int, keyType uint64) ([]any, error) {
	w := 1 << keyType
	b, err := r.column(nkeys * w)
	if err != nil {
		return nil, err
	}
	out = slices.Grow(out, nkeys)
	for i := 0; i < len(b); i += w {
		k := lowCardinalityKey(b[i:], keyType)
		if k >= uint64(len(dict)) {
			return nil, cherr.New(cherr.IncorrectData, "LowCardinality index %d is out of dictionary range %d", k, len(dict))
		}
		if c.lcNullable && k == 0 {
			out = append(out, nil)
		} else {
			out = append(out, dict[k])
		}
	}
	return out, nil
}

func lowCardinalityKey(b []byte, keyType uint64) uint64 {
	switch keyType {
	case 0:
		return uint64(b[0])
	case 1:
		return uint64(binary.LittleEndian.Uint16(b))
	case 2:
		return uint64(binary.LittleEndian.Uint32(b))
	}
	return binary.LittleEndian.Uint64(b)
}
