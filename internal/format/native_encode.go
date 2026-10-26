package format

import (
	"encoding/binary"
	"encoding/hex"
	"io"
	"math"
	"strings"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
)

// Protocol revisions that change the Native layout (ClickHouse Protocol.h).
const (
	// Below this, DateTime('tz') is sent as plain "DateTime".
	minRevisionWithTimeZoneInDateTime = 54337
	// Below this (but > 0), LowCardinality is sent as its full inner type.
	minRevisionWithLowCardinality = 54405
	// From this on, a per-column "has custom serialization" byte is written.
	minRevisionWithCustomSerialization = 54454
)

// lowCardinality serialization constants (ClickHouse SerializationLowCardinality).
const (
	lcSharedDictionariesWithAdditionalKeys = 1
	lcHasAdditionalKeysBit                 = 1 << 9
	lcNeedUpdateDictionaryBit              = 1 << 10
)

// nativeEncoder writes the ClickHouse Native format: one block per WriteRows.
type nativeEncoder struct {
	w        io.Writer
	cols     []Column
	codecs   []*nativeCodec
	revision uint64
	types    []string // cached Type.String() per column
	buf      []byte
	scratch  []any
}

func newNativeEncoder(w io.Writer, cols []Column, opts Options) (*nativeEncoder, error) {
	e := &nativeEncoder{w: w, cols: cols, revision: opts.ClientProtocolVersion}
	e.codecs = make([]*nativeCodec, len(cols))
	e.types = make([]string, len(cols))
	rev := opts.ClientProtocolVersion
	for i, c := range cols {
		typ := c.Type
		if rev > 0 && rev < minRevisionWithLowCardinality {
			typ = removeLowCardinality(typ)
		}
		e.types[i] = typ.String()
		if rev < minRevisionWithTimeZoneInDateTime && strings.HasPrefix(e.types[i], "DateTime(") {
			e.types[i] = "DateTime"
		}
		codec, err := compileNative(typ)
		if err != nil {
			return nil, err
		}
		e.codecs[i] = codec
	}
	return e, nil
}

// WriteRows encodes rows as one Native block. Like ClickHouse, empty blocks
// are not written at all (an empty result is an empty body).
func (e *nativeEncoder) WriteRows(rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	for i, r := range rows {
		if len(r) != len(e.cols) {
			return cherr.New(cherr.LogicalError, "row %d has %d values, expected %d", i, len(r), len(e.cols))
		}
	}
	b := e.buf[:0]
	if e.revision > 0 {
		b = appendBlockInfo(b)
	}
	b = binary.AppendUvarint(b, uint64(len(e.cols)))
	b = binary.AppendUvarint(b, uint64(len(rows)))
	if cap(e.scratch) < len(rows) {
		e.scratch = make([]any, len(rows))
	}
	vals := e.scratch[:len(rows)]
	var err error
	for ci, col := range e.cols {
		b = appendNativeString(b, col.Name)
		b = appendNativeString(b, e.types[ci])
		if e.revision >= minRevisionWithCustomSerialization {
			b = append(b, 0)
		}
		for ri, r := range rows {
			vals[ri] = r[ci]
		}
		codec := e.codecs[ci]
		b = codec.appendPrefix(b)
		if b, err = codec.appendData(b, vals); err != nil {
			e.buf = b[:0]
			return columnErr(err, col.Name)
		}
	}
	clear(vals)
	e.buf = b[:0]
	if _, err := e.w.Write(b); err != nil {
		return err
	}
	return nil
}

func (e *nativeEncoder) Finish(Stats) error { return nil }

// removeLowCardinality strips LowCardinality wrappers anywhere in t.
func removeLowCardinality(t chtype.Type) chtype.Type {
	if t.Kind == chtype.LowCardinality && t.Elem != nil {
		return removeLowCardinality(*t.Elem)
	}
	if t.Elem != nil {
		e := removeLowCardinality(*t.Elem)
		t.Elem = &e
	}
	if t.Key != nil {
		k := removeLowCardinality(*t.Key)
		t.Key = &k
	}
	if len(t.Fields) > 0 {
		fs := make([]chtype.Field, len(t.Fields))
		for i, f := range t.Fields {
			fs[i] = chtype.Field{Name: f.Name, Type: removeLowCardinality(f.Type)}
		}
		t.Fields = fs
	}
	return t
}

// appendBlockInfo writes BlockInfo as ClickHouse does for revision > 0:
// field 1 is_overflows=false, field 2 bucket_num=-1, field 0 end marker.
func appendBlockInfo(b []byte) []byte {
	return append(b, 1, 0, 2, 0xff, 0xff, 0xff, 0xff, 0)
}

func appendNativeString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

// appendPrefix writes serialization state prefixes (LowCardinality key
// version) for the whole type tree, as ClickHouse does before column data.
func (c *nativeCodec) appendPrefix(b []byte) []byte {
	switch c.kind {
	case chtype.LowCardinality:
		return binary.LittleEndian.AppendUint64(b, lcSharedDictionariesWithAdditionalKeys)
	case chtype.Nullable, chtype.Array, chtype.SimpleAggregateFunction:
		return c.elem.appendPrefix(b)
	case chtype.Map:
		return c.elem.appendPrefix(c.key.appendPrefix(b))
	case chtype.Tuple:
		for _, f := range c.fields {
			b = f.appendPrefix(b)
		}
	}
	return b
}

func typeErr(c *nativeCodec, v any) error {
	return cherr.New(cherr.LogicalError, "value of Go type %T is not a canonical %s value", v, c.t.String())
}

func rangeErr(c *nativeCodec, v any) error {
	return cherr.New(cherr.CannotConvertType, "value %v is out of range for %s", v, c.t.String())
}

// appendData writes the bulk data of vals (all values of one column).
func (c *nativeCodec) appendData(b []byte, vals []any) ([]byte, error) {
	switch c.kind {
	case chtype.Nothing:
		// SerializationNothing writes one '0' byte per row.
		for range vals {
			b = append(b, '0')
		}
		return b, nil
	case chtype.Bool:
		for _, v := range vals {
			x, ok := v.(bool)
			if !ok {
				return b, typeErr(c, v)
			}
			if x {
				b = append(b, 1)
			} else {
				b = append(b, 0)
			}
		}
		return b, nil
	case chtype.UInt8, chtype.UInt16, chtype.UInt32, chtype.UInt64:
		return c.appendUints(b, vals)
	case chtype.Int8, chtype.Int16, chtype.Int32, chtype.Int64:
		return c.appendInts(b, vals)
	case chtype.Float32:
		for _, v := range vals {
			x, ok := v.(float64)
			if !ok {
				return b, typeErr(c, v)
			}
			b = binary.LittleEndian.AppendUint32(b, math.Float32bits(float32(x)))
		}
		return b, nil
	case chtype.Float64:
		for _, v := range vals {
			x, ok := v.(float64)
			if !ok {
				return b, typeErr(c, v)
			}
			b = binary.LittleEndian.AppendUint64(b, math.Float64bits(x))
		}
		return b, nil
	case chtype.String:
		for _, v := range vals {
			s, ok := v.(string)
			if !ok {
				return b, typeErr(c, v)
			}
			b = appendNativeString(b, s)
		}
		return b, nil
	case chtype.FixedString:
		n := c.t.N
		for _, v := range vals {
			s, ok := v.(string)
			if !ok {
				return b, typeErr(c, v)
			}
			if len(s) > n {
				return b, cherr.New(cherr.CannotConvertType, "string of %d bytes is too large for %s", len(s), c.t.String())
			}
			b = append(b, s...)
			for i := len(s); i < n; i++ {
				b = append(b, 0)
			}
		}
		return b, nil
	case chtype.UUID:
		for _, v := range vals {
			s, ok := v.(string)
			if !ok {
				return b, typeErr(c, v)
			}
			var err error
			if b, err = appendUUID(b, s); err != nil {
				return b, err
			}
		}
		return b, nil
	case chtype.Date, chtype.Date32, chtype.DateTime, chtype.DateTime64:
		return c.appendTimes(b, vals)
	case chtype.Enum8, chtype.Enum16:
		for _, v := range vals {
			var n int64
			if v != nil {
				s, ok := v.(string)
				if !ok {
					return b, typeErr(c, v)
				}
				if n, ok = c.enumByName[s]; !ok {
					return b, cherr.New(cherr.CannotConvertType, "unknown element '%s' for %s", s, c.t.String())
				}
			}
			if c.kind == chtype.Enum8 {
				b = append(b, byte(int8(n)))
			} else {
				b = binary.LittleEndian.AppendUint16(b, uint16(int16(n)))
			}
		}
		return b, nil
	case chtype.Nullable:
		inner := make([]any, len(vals))
		for i, v := range vals {
			if v == nil {
				b = append(b, 1)
				inner[i] = c.elem.zero()
			} else {
				b = append(b, 0)
				inner[i] = v
			}
		}
		return c.elem.appendData(b, inner)
	case chtype.SimpleAggregateFunction:
		return c.elem.appendData(b, vals)
	case chtype.Array:
		var off uint64
		total := 0
		for _, v := range vals {
			arr, ok := v.([]any)
			if !ok {
				return b, typeErr(c, v)
			}
			total += len(arr)
		}
		flat := make([]any, 0, total)
		for _, v := range vals {
			arr := v.([]any)
			off += uint64(len(arr))
			b = binary.LittleEndian.AppendUint64(b, off)
			flat = append(flat, arr...)
		}
		return c.elem.appendData(b, flat)
	case chtype.Map:
		var off uint64
		var keys, values []any
		for _, v := range vals {
			pairs, ok := v.([]any)
			if !ok {
				return b, typeErr(c, v)
			}
			for _, p := range pairs {
				kv, ok := p.([]any)
				if !ok || len(kv) != 2 {
					return b, cherr.New(cherr.LogicalError, "Map entry %T is not a [key, value] pair", p)
				}
				keys = append(keys, kv[0])
				values = append(values, kv[1])
			}
			off += uint64(len(pairs))
			b = binary.LittleEndian.AppendUint64(b, off)
		}
		b, err := c.key.appendData(b, keys)
		if err != nil {
			return b, err
		}
		return c.elem.appendData(b, values)
	case chtype.Tuple:
		for i, v := range vals {
			t, ok := v.([]any)
			if !ok || len(t) != len(c.fields) {
				return b, cherr.New(cherr.LogicalError, "row %d: value %T is not a %d-element tuple", i, v, len(c.fields))
			}
		}
		field := make([]any, len(vals))
		for fi, f := range c.fields {
			for i, v := range vals {
				field[i] = v.([]any)[fi]
			}
			var err error
			if b, err = f.appendData(b, field); err != nil {
				return b, err
			}
		}
		return b, nil
	case chtype.LowCardinality:
		return c.appendLowCardinality(b, vals)
	}
	return b, cherr.New(cherr.NotImplemented, "Native serialization of %s is not supported", c.t.String())
}

func (c *nativeCodec) appendUints(b []byte, vals []any) ([]byte, error) {
	var limit uint64
	switch c.kind {
	case chtype.UInt8:
		limit = math.MaxUint8
	case chtype.UInt16:
		limit = math.MaxUint16
	case chtype.UInt32:
		limit = math.MaxUint32
	default:
		limit = math.MaxUint64
	}
	for _, v := range vals {
		x, ok := v.(uint64)
		if !ok {
			return b, typeErr(c, v)
		}
		if x > limit {
			return b, rangeErr(c, x)
		}
		switch c.kind {
		case chtype.UInt8:
			b = append(b, byte(x))
		case chtype.UInt16:
			b = binary.LittleEndian.AppendUint16(b, uint16(x))
		case chtype.UInt32:
			b = binary.LittleEndian.AppendUint32(b, uint32(x))
		default:
			b = binary.LittleEndian.AppendUint64(b, x)
		}
	}
	return b, nil
}

func (c *nativeCodec) appendInts(b []byte, vals []any) ([]byte, error) {
	var lo, hi int64
	switch c.kind {
	case chtype.Int8:
		lo, hi = math.MinInt8, math.MaxInt8
	case chtype.Int16:
		lo, hi = math.MinInt16, math.MaxInt16
	case chtype.Int32:
		lo, hi = math.MinInt32, math.MaxInt32
	default:
		lo, hi = math.MinInt64, math.MaxInt64
	}
	for _, v := range vals {
		x, ok := v.(int64)
		if !ok {
			return b, typeErr(c, v)
		}
		if x < lo || x > hi {
			return b, rangeErr(c, x)
		}
		switch c.kind {
		case chtype.Int8:
			b = append(b, byte(int8(x)))
		case chtype.Int16:
			b = binary.LittleEndian.AppendUint16(b, uint16(int16(x)))
		case chtype.Int32:
			b = binary.LittleEndian.AppendUint32(b, uint32(int32(x)))
		default:
			b = binary.LittleEndian.AppendUint64(b, uint64(x))
		}
	}
	return b, nil
}

var pow10 = [10]int64{1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000}

func (c *nativeCodec) appendTimes(b []byte, vals []any) ([]byte, error) {
	for _, v := range vals {
		t, ok := v.(time.Time)
		if !ok {
			return b, typeErr(c, v)
		}
		sec := t.Unix()
		switch c.kind {
		case chtype.Date:
			days := floorDiv(sec, 86400)
			if days < 0 || days > math.MaxUint16 {
				return b, rangeErr(c, t)
			}
			b = binary.LittleEndian.AppendUint16(b, uint16(days))
		case chtype.Date32:
			days := floorDiv(sec, 86400)
			if days < math.MinInt32 || days > math.MaxInt32 {
				return b, rangeErr(c, t)
			}
			b = binary.LittleEndian.AppendUint32(b, uint32(int32(days)))
		case chtype.DateTime:
			if sec < 0 || sec > math.MaxUint32 {
				return b, rangeErr(c, t)
			}
			b = binary.LittleEndian.AppendUint32(b, uint32(sec))
		case chtype.DateTime64:
			p := c.t.Precision
			ticks := sec*pow10[p] + int64(t.Nanosecond())/pow10[9-p]
			b = binary.LittleEndian.AppendUint64(b, uint64(ticks))
		}
	}
	return b, nil
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// appendUUID writes a canonical UUID string in ClickHouse's binary layout:
// two little-endian UInt64 halves (high half first).
func appendUUID(b []byte, s string) ([]byte, error) {
	var u [16]byte
	if err := parseUUID(s, &u); err != nil {
		return b, err
	}
	for i := 7; i >= 0; i-- {
		b = append(b, u[i])
	}
	for i := 15; i >= 8; i-- {
		b = append(b, u[i])
	}
	return b, nil
}

func parseUUID(s string, u *[16]byte) error {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return cherr.New(cherr.CannotParseText, "cannot parse UUID %q", s)
	}
	var compact [32]byte
	copy(compact[0:8], s[0:8])
	copy(compact[8:12], s[9:13])
	copy(compact[12:16], s[14:18])
	copy(compact[16:20], s[19:23])
	copy(compact[20:32], s[24:36])
	if _, err := hex.Decode(u[:], compact[:]); err != nil {
		return cherr.New(cherr.CannotParseText, "cannot parse UUID %q", s)
	}
	return nil
}

func formatUUID(u *[16]byte) string {
	var out [36]byte
	hex.Encode(out[0:8], u[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], u[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], u[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], u[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], u[10:16])
	return string(out[:])
}

// lcKey normalizes a canonical value into a comparable map key.
type timeKey struct {
	sec  int64
	nsec int
}

func lcKey(v any) any {
	switch x := v.(type) {
	case time.Time:
		return timeKey{x.Unix(), x.Nanosecond()}
	case float64:
		return math.Float64bits(x)
	}
	return v
}

// appendLowCardinality writes a LowCardinality column the way ClickHouse
// does for a freshly built column: a per-block dictionary whose first entry
// is the default value (preceded by a NULL placeholder for
// LowCardinality(Nullable)), the remaining distinct values in order of first
// appearance, and the narrowest index type that fits the dictionary size.
func (c *nativeCodec) appendLowCardinality(b []byte, vals []any) ([]byte, error) {
	def := c.elem.zero()
	dict := make([]any, 0, 16)
	index := make(map[any]int, 16)
	if c.lcNullable {
		dict = append(dict, def) // NULL placeholder, serialized as the default
	}
	defIdx := len(dict)
	dict = append(dict, def)
	index[lcKey(def)] = defIdx

	keys := make([]uint64, len(vals))
	for i, v := range vals {
		if v == nil {
			if !c.lcNullable {
				return b, cherr.New(cherr.LogicalError, "NULL value in non-nullable %s", c.t.String())
			}
			keys[i] = 0
			continue
		}
		k := lcKey(v)
		idx, ok := index[k]
		if !ok {
			idx = len(dict)
			dict = append(dict, v)
			index[k] = idx
		}
		keys[i] = uint64(idx)
	}

	var keyType uint64
	switch n := len(dict); {
	case n <= math.MaxUint8:
		keyType = 0
	case n <= math.MaxUint16:
		keyType = 1
	case n <= math.MaxUint32:
		keyType = 2
	default:
		keyType = 3
	}
	b = binary.LittleEndian.AppendUint64(b, lcHasAdditionalKeysBit|lcNeedUpdateDictionaryBit|keyType)
	b = binary.LittleEndian.AppendUint64(b, uint64(len(dict)))
	b, err := c.elem.appendData(b, dict)
	if err != nil {
		return b, err
	}
	b = binary.LittleEndian.AppendUint64(b, uint64(len(keys)))
	for _, k := range keys {
		switch keyType {
		case 0:
			b = append(b, byte(k))
		case 1:
			b = binary.LittleEndian.AppendUint16(b, uint16(k))
		case 2:
			b = binary.LittleEndian.AppendUint32(b, uint32(k))
		default:
			b = binary.LittleEndian.AppendUint64(b, k)
		}
	}
	return b, nil
}
