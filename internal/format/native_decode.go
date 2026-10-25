package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"time"

	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
)

// Limits guarding against corrupt or hostile input.
const (
	maxNativeBlockRows = 1 << 30
	maxNativeColumns   = 1 << 16
	maxNativeString    = 1 << 30
)

// binReader reads little-endian primitives, mapping truncation to a
// ClickHouse-style error.
type binReader struct {
	r   *bufio.Reader
	tmp [8]byte
}

func errTruncated(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return cherr.New(cherr.IncorrectData, "Cannot read all data in Native format: unexpected end of input")
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

func (r *binReader) full(n int) ([]byte, error) {
	if n <= len(r.tmp) {
		if _, err := io.ReadFull(r.r, r.tmp[:n]); err != nil {
			return nil, errTruncated(err)
		}
		return r.tmp[:n], nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return nil, errTruncated(err)
	}
	return buf, nil
}

func (r *binReader) u8() (uint8, error) {
	b, err := r.r.ReadByte()
	if err != nil {
		return 0, errTruncated(err)
	}
	return b, nil
}

func (r *binReader) u16() (uint16, error) {
	b, err := r.full(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b), nil
}

func (r *binReader) u32() (uint32, error) {
	b, err := r.full(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
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
		return "", cherr.New(cherr.IncorrectData, "string of %d bytes in Native input is too large", n)
	}
	if n == 0 {
		return "", nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return "", errTruncated(err)
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
	rows, pos int
}

func newNativeDecoder(r io.Reader, cols []Column, opts Options) *nativeDecoder {
	d := &nativeDecoder{
		r:        binReader{r: bufio.NewReaderSize(r, 64<<10)},
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
		v := d.block[bi][d.pos]
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
	seen := make(map[int]bool, ncols)
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
			return cherr.Wrap(cherr.UnknownType, nil, "column %s in Native input has unknown type %s: %s", name, typName, cherr.As(err).Msg)
		}
		codec, err := compileNative(typ)
		if err != nil {
			return columnErr(err, name)
		}
		var vals []any
		if nrows > 0 {
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
// are not validated (e.g. Enum slots holding 0).
func (c *nativeCodec) readDataMasked(r *binReader, n int, nulls []byte) ([]any, error) {
	out := make([]any, n)
	switch c.kind {
	case chtype.Nothing:
		for range out {
			if _, err := r.u8(); err != nil {
				return nil, err
			}
		}
	case chtype.Bool:
		for i := range out {
			b, err := r.u8()
			if err != nil {
				return nil, err
			}
			out[i] = b != 0
		}
	case chtype.UInt8, chtype.UInt16, chtype.UInt32, chtype.UInt64:
		for i := range out {
			var v uint64
			var err error
			switch c.kind {
			case chtype.UInt8:
				var x uint8
				x, err = r.u8()
				v = uint64(x)
			case chtype.UInt16:
				var x uint16
				x, err = r.u16()
				v = uint64(x)
			case chtype.UInt32:
				var x uint32
				x, err = r.u32()
				v = uint64(x)
			default:
				v, err = r.u64()
			}
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
	case chtype.Int8, chtype.Int16, chtype.Int32, chtype.Int64:
		for i := range out {
			var v int64
			switch c.kind {
			case chtype.Int8:
				x, err := r.u8()
				if err != nil {
					return nil, err
				}
				v = int64(int8(x))
			case chtype.Int16:
				x, err := r.u16()
				if err != nil {
					return nil, err
				}
				v = int64(int16(x))
			case chtype.Int32:
				x, err := r.u32()
				if err != nil {
					return nil, err
				}
				v = int64(int32(x))
			default:
				x, err := r.u64()
				if err != nil {
					return nil, err
				}
				v = int64(x)
			}
			out[i] = v
		}
	case chtype.Float32:
		for i := range out {
			x, err := r.u32()
			if err != nil {
				return nil, err
			}
			out[i] = float64(math.Float32frombits(x))
		}
	case chtype.Float64:
		for i := range out {
			x, err := r.u64()
			if err != nil {
				return nil, err
			}
			out[i] = math.Float64frombits(x)
		}
	case chtype.String:
		for i := range out {
			s, err := r.str()
			if err != nil {
				return nil, err
			}
			out[i] = s
		}
	case chtype.FixedString:
		for i := range out {
			buf := make([]byte, c.t.N)
			if _, err := io.ReadFull(r.r, buf); err != nil {
				return nil, errTruncated(err)
			}
			out[i] = string(buf)
		}
	case chtype.UUID:
		for i := range out {
			b, err := r.full(16)
			if err != nil {
				return nil, err
			}
			var u [16]byte
			for j := 0; j < 8; j++ {
				u[j] = b[7-j]
				u[8+j] = b[15-j]
			}
			out[i] = formatUUID(&u)
		}
	case chtype.Date:
		for i := range out {
			x, err := r.u16()
			if err != nil {
				return nil, err
			}
			out[i] = time.Unix(int64(x)*86400, 0).UTC()
		}
	case chtype.Date32:
		for i := range out {
			x, err := r.u32()
			if err != nil {
				return nil, err
			}
			out[i] = time.Unix(int64(int32(x))*86400, 0).UTC()
		}
	case chtype.DateTime:
		for i := range out {
			x, err := r.u32()
			if err != nil {
				return nil, err
			}
			out[i] = time.Unix(int64(x), 0).UTC()
		}
	case chtype.DateTime64:
		p := pow10[c.t.Precision]
		scale := pow10[9-c.t.Precision]
		for i := range out {
			x, err := r.u64()
			if err != nil {
				return nil, err
			}
			ticks := int64(x)
			sec := floorDiv(ticks, p)
			frac := ticks - sec*p
			out[i] = time.Unix(sec, frac*scale).UTC()
		}
	case chtype.Enum8, chtype.Enum16:
		for i := range out {
			var v int64
			if c.kind == chtype.Enum8 {
				x, err := r.u8()
				if err != nil {
					return nil, err
				}
				v = int64(int8(x))
			} else {
				x, err := r.u16()
				if err != nil {
					return nil, err
				}
				v = int64(int16(x))
			}
			name, ok := c.enumByValue[v]
			if !ok {
				if nulls != nil && nulls[i] != 0 {
					continue
				}
				return nil, cherr.New(cherr.IncorrectData, "unexpected value %d for %s", v, c.t.String())
			}
			out[i] = name
		}
	case chtype.Nullable:
		nm := make([]byte, n)
		if _, err := io.ReadFull(r.r, nm); err != nil {
			return nil, errTruncated(err)
		}
		inner, err := c.elem.readDataMasked(r, n, nm)
		if err != nil {
			return nil, err
		}
		for i := range out {
			if nm[i] == 0 {
				out[i] = inner[i]
			}
		}
	case chtype.SimpleAggregateFunction:
		return c.elem.readDataMasked(r, n, nulls)
	case chtype.Array, chtype.Map:
		offs := make([]uint64, n)
		var prev uint64
		for i := range offs {
			x, err := r.u64()
			if err != nil {
				return nil, err
			}
			if x < prev || x-prev > maxNativeBlockRows {
				return nil, cherr.New(cherr.IncorrectData, "invalid offsets in Native %s column", c.t.String())
			}
			offs[i], prev = x, x
		}
		if prev > maxNativeBlockRows {
			return nil, cherr.New(cherr.IncorrectData, "too many nested values in Native %s column", c.t.String())
		}
		total := int(prev)
		if c.kind == chtype.Array {
			flat, err := c.elem.readData(r, total)
			if err != nil {
				return nil, err
			}
			var start uint64
			for i, end := range offs {
				out[i] = flat[start:end:end]
				start = end
			}
		} else {
			keys, err := c.key.readData(r, total)
			if err != nil {
				return nil, err
			}
			vals, err := c.elem.readData(r, total)
			if err != nil {
				return nil, err
			}
			var start uint64
			for i, end := range offs {
				pairs := make([]any, 0, end-start)
				for j := start; j < end; j++ {
					pairs = append(pairs, []any{keys[j], vals[j]})
				}
				out[i] = pairs
				start = end
			}
		}
	case chtype.Tuple:
		cols := make([][]any, len(c.fields))
		for fi, f := range c.fields {
			col, err := f.readData(r, n)
			if err != nil {
				return nil, err
			}
			cols[fi] = col
		}
		for i := range out {
			t := make([]any, len(c.fields))
			for fi := range c.fields {
				t[fi] = cols[fi][i]
			}
			out[i] = t
		}
	case chtype.LowCardinality:
		return c.readLowCardinality(r, n)
	default:
		return nil, cherr.New(cherr.NotImplemented, "Native deserialization of %s is not supported", c.t.String())
	}
	return out, nil
}

func (c *nativeCodec) readLowCardinality(r *binReader, n int) ([]any, error) {
	out := make([]any, 0, n)
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
		for i := uint64(0); i < nkeys; i++ {
			var k uint64
			switch keyType {
			case 0:
				x, err := r.u8()
				if err != nil {
					return nil, err
				}
				k = uint64(x)
			case 1:
				x, err := r.u16()
				if err != nil {
					return nil, err
				}
				k = uint64(x)
			case 2:
				x, err := r.u32()
				if err != nil {
					return nil, err
				}
				k = uint64(x)
			default:
				if k, err = r.u64(); err != nil {
					return nil, err
				}
			}
			if k >= uint64(len(dict)) {
				return nil, cherr.New(cherr.IncorrectData, "LowCardinality index %d is out of dictionary range %d", k, len(dict))
			}
			if c.lcNullable && k == 0 {
				out = append(out, nil)
			} else {
				out = append(out, dict[k])
			}
		}
		if nkeys == 0 {
			return nil, cherr.New(cherr.IncorrectData, "empty LowCardinality granule")
		}
	}
	return out, nil
}
