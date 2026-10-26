package chtype

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// parseText implements ParseText (escaped / TabSeparated form).
func parseText(t Type, s string) (any, error) {
	switch t.Kind {
	case Nullable:
		if s == `\N` {
			return nil, nil
		}
		return parseText(*t.Elem, s)
	case LowCardinality, SimpleAggregateFunction:
		return parseText(*t.Elem, s)
	case Nothing:
		return nil, errf(cherr.CannotParseText, "Cannot parse value of type Nothing")
	case String:
		return unescapeText(s)
	case FixedString:
		v, err := unescapeText(s)
		if err != nil {
			return nil, err
		}
		return padFixedString(v, t.N)
	case Enum8, Enum16:
		v, err := unescapeText(s)
		if err != nil {
			return nil, err
		}
		return enumFromText(t, v)
	case UUID:
		return parseUUID(s)
	case Bool:
		return parseBoolText(s)
	case UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64:
		return parseIntText(s, t.Kind, true)
	case Float32, Float64:
		return parseFloatText(s, t.Kind)
	case Date, Date32:
		return parseDateText(s, t.Kind, false)
	case DateTime:
		return parseDateTimeText(s, locationOf(t))
	case DateTime64:
		return parseDateTime64Text(s, t.Precision, locationOf(t))
	case Array, Tuple, Map:
		p := &quotedParser{s: s}
		if p.pos < len(s) && isSpace(s[0]) {
			return nil, compositeStartErr(t)
		}
		v, err := p.parseValue(t)
		if err != nil {
			return nil, err
		}
		if p.pos != len(s) {
			return nil, errf(cherr.CannotParseText, "Cannot parse %s: unexpected data after value at position %d", t.String(), p.pos)
		}
		return v, nil
	case AggregateFunction:
		return nil, errf(cherr.NotImplemented, "Text parsing of %s is not supported", t.String())
	}
	return nil, errf(cherr.NotImplemented, "Text parsing of type %s is not supported", t.String())
}

func compositeStartErr(t Type) error {
	switch t.Kind {
	case Array:
		return errf(codeCannotReadArrayFromText, "Array does not start with '[' character")
	case Map:
		return errf(codeCannotParseInputAssertion, "Cannot parse input: expected '{'")
	}
	return errf(codeCannotParseInputAssertion, "Cannot parse input: expected '('")
}

// unescapeText processes ClickHouse escape sequences in an unquoted field.
// Raw tabs and newlines terminate a TSV field and are therefore rejected.
func unescapeText(s string) (string, error) {
	i := strings.IndexAny(s, "\\\t\n")
	if i < 0 {
		return s, nil
	}
	var b strings.Builder
	b.Grow(len(s))
	b.WriteString(s[:i])
	for i < len(s) {
		c := s[i]
		switch c {
		case '\\':
			n, err := unescapeSeq(s[i:], &b)
			if err != nil {
				return "", err
			}
			i += n
		case '\t', '\n':
			return "", errf(cherr.CannotParseText, "Cannot parse string: unescaped %s at position %d", map[byte]string{'\t': "tab", '\n': "line feed"}[c], i)
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String(), nil
}

// unescapeSeq decodes one backslash escape at s[0] into b and returns the
// number of bytes consumed (ClickHouse parseComplexEscapeSequence rules).
func unescapeSeq(s string, b *strings.Builder) (int, error) {
	if len(s) < 2 {
		return 0, errf(codeCannotParseEscapeSequence, "Cannot parse escape sequence")
	}
	c := s[1]
	switch c {
	case 'x':
		if len(s) < 4 {
			return 0, errf(codeCannotParseEscapeSequence, "Cannot parse escape sequence")
		}
		hi, ok1 := hexVal(s[2])
		lo, ok2 := hexVal(s[3])
		if !ok1 || !ok2 {
			return 0, errf(codeCannotParseEscapeSequence, "Cannot parse escape sequence \\x%s", s[2:4])
		}
		b.WriteByte(hi<<4 | lo)
		return 4, nil
	case 'N':
		// \N inside a string value decodes to nothing (ClickHouse behaviour).
		return 2, nil
	}
	d := unescapeChar(c)
	if d != '\\' && d != '\'' && d != '"' && d != '`' && d != '/' && d != '=' && d >= 0x20 {
		b.WriteByte('\\')
	}
	b.WriteByte(d)
	return 2, nil
}

func unescapeChar(c byte) byte {
	switch c {
	case 'a':
		return '\a'
	case 'b':
		return '\b'
	case 'e':
		return 0x1b
	case 'f':
		return '\f'
	case 'n':
		return '\n'
	case 'r':
		return '\r'
	case 't':
		return '\t'
	case 'v':
		return '\v'
	case '0':
		return 0
	}
	return c
}

func hexVal(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

func padFixedString(s string, n int) (string, error) {
	if len(s) > n {
		return "", errf(codeTooLargeStringSize, "Too large value for FixedString(%d)", n)
	}
	if len(s) == n {
		return s, nil
	}
	return s + strings.Repeat("\x00", n-len(s)), nil
}

// enumFromText resolves an enum element by name, falling back to its
// numeric value (ClickHouse accepts both in text formats).
func enumFromText(t Type, s string) (string, error) {
	for _, ev := range t.EnumValues {
		if ev.Name == s {
			return ev.Name, nil
		}
	}
	if iv, err := strconv.ParseInt(s, 10, 64); err == nil {
		for _, ev := range t.EnumValues {
			if ev.Value == iv {
				return ev.Name, nil
			}
		}
	}
	return "", errf(codeUnknownElementOfEnum, "Unknown element '%s' for enum", s)
}

func enumByValue(t Type, v int64) (string, error) {
	for _, ev := range t.EnumValues {
		if ev.Value == v {
			return ev.Name, nil
		}
	}
	return "", errf(codeUnknownElementOfEnum, "Unexpected value %d in enum", v)
}

func enumValueOf(t Type, name string) (int64, bool) {
	for _, ev := range t.EnumValues {
		if ev.Name == name {
			return ev.Value, true
		}
	}
	return 0, false
}

// parseUUID accepts the 36-character dashed form or 32 hex digits, any case.
func parseUUID(s string) (string, error) {
	var hex [32]byte
	switch len(s) {
	case 36:
		if s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
			return "", errf(codeCannotParseUUID, "Cannot parse uuid %s", s)
		}
		j := 0
		for i := 0; i < 36; i++ {
			if i == 8 || i == 13 || i == 18 || i == 23 {
				continue
			}
			hex[j] = s[i]
			j++
		}
	case 32:
		copy(hex[:], s)
	default:
		return "", errf(codeCannotParseUUID, "Cannot parse uuid %s", s)
	}
	out := make([]byte, 0, 36)
	for i, c := range hex {
		if _, ok := hexVal(c); !ok {
			return "", errf(codeCannotParseUUID, "Cannot parse uuid %s", s)
		}
		if i == 8 || i == 12 || i == 16 || i == 20 {
			out = append(out, '-')
		}
		if c >= 'A' && c <= 'F' {
			c += 'a' - 'A'
		}
		out = append(out, c)
	}
	return string(out), nil
}

var boolWords = map[string]bool{
	"true": true, "false": false, "1": true, "0": false,
	"t": true, "f": false, "y": true, "n": false,
	"yes": true, "no": false, "on": true, "off": false,
	"enable": true, "disable": false, "enabled": true, "disabled": false,
}

func parseBoolText(s string) (bool, error) {
	if v, ok := boolWords[strings.ToLower(s)]; ok {
		return v, nil
	}
	return false, errf(codeCannotParseBool, "Cannot parse boolean value here: '%s', should be 'true' or 'false' controlled by setting bool_true_representation and bool_false_representation or one of True/False/T/F/Y/N/Yes/No/On/Off/Enable/Disable/Enabled/Disabled/1/0", s)
}

// parseIntText parses an integer for kind k. In strict mode (TSV fields,
// query parameters) a leading '+' and leading zeros are rejected, as in
// ClickHouse; CAST from String (strict=false) accepts both. Values that do
// not fit wrap modulo 2^width exactly like ClickHouse ('300' as UInt8 is 44).
func parseIntText(s string, k Kind, strict bool) (any, error) {
	i := 0
	neg := false
	if s == "" {
		return nil, errf(cherr.CannotParseText, "Cannot parse %s from empty string", kindName(k))
	}
	switch s[0] {
	case '-':
		if !isSignedInt(k) {
			return nil, errf(codeCannotParseNumber, "Unsigned type must not contain '-' symbol")
		}
		neg = true
		i++
	case '+':
		if strict {
			return nil, errf(cherr.CannotParseText, "Cannot parse %s from '%s'", kindName(k), s)
		}
		i++
	}
	if i >= len(s) {
		return nil, errf(cherr.CannotParseText, "Cannot parse %s from '%s'", kindName(k), s)
	}
	if strict && s[i] == '0' && i+1 < len(s) {
		return nil, errf(cherr.CannotParseText, "Cannot parse %s from '%s': leading zeros are not allowed", kindName(k), s)
	}
	var u uint64
	for ; i < len(s); i++ {
		c := s[i]
		if !isDigit(c) {
			return nil, errf(cherr.CannotParseText, "Cannot parse %s from '%s'", kindName(k), s)
		}
		u = u*10 + uint64(c-'0')
	}
	if neg {
		u = -u
	}
	return wrapInt(u, k), nil
}

// asInputParseErr re-codes a malformed number in JSON input data as
// CANNOT_PARSE_INPUT_ASSERTION_FAILED (27), the code ClickHouse's input
// formats report; CAST keeps CANNOT_PARSE_TEXT (6).
// Usage: asInputParseErr(parse(...))(t, s).
func asInputParseErr(v any, err error) func(Type, string) (any, error) {
	return func(t Type, s string) (any, error) {
		if err != nil && errCodeOf(err) == cherr.CannotParseText {
			return nil, errf(codeCannotParseInputAssertion, "Cannot parse input: expected a %s value, got '%s'", t.String(), s)
		}
		return v, err
	}
}

func isSignedInt(k Kind) bool { return k == Int8 || k == Int16 || k == Int32 || k == Int64 }

// wrapInt truncates the two's-complement bit pattern u to kind k.
func wrapInt(u uint64, k Kind) any {
	switch k {
	case UInt8:
		return uint64(uint8(u))
	case UInt16:
		return uint64(uint16(u))
	case UInt32:
		return uint64(uint32(u))
	case UInt64:
		return u
	case Int8:
		return int64(int8(u))
	case Int16:
		return int64(int16(u))
	case Int32:
		return int64(int32(u))
	case Int64:
		return int64(u)
	}
	return u
}

// parseFloatText parses [+-](digits[.digits]|.digits)[e[+-]digits], inf,
// infinity or nan (case-insensitive). Out-of-range values become ±inf.
func parseFloatText(s string, k Kind) (any, error) {
	if !validFloatSyntax(s) {
		return nil, errf(codeCannotParseNumber, "Cannot read floating point value from '%s'", s)
	}
	if l := strings.ToLower(strings.TrimLeft(s, "+-")); l == "nan" {
		// The sign bit survives (ClickHouse stores -nan with it set); text
		// output prints both as nan, JSON quote_denormals prints "-nan".
		if strings.HasPrefix(s, "-") {
			return math.Copysign(math.NaN(), -1), nil
		}
		return math.NaN(), nil
	}
	bits := 64
	if k == Float32 {
		bits = 32
	}
	f, err := strconv.ParseFloat(s, bits)
	if err != nil {
		var ne *strconv.NumError
		if !errors.As(err, &ne) || ne.Err != strconv.ErrRange {
			return nil, errf(codeCannotParseNumber, "Cannot read floating point value from '%s'", s)
		}
	}
	return f, nil
}

func validFloatSyntax(s string) bool {
	i := 0
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		i++
	}
	rest := strings.ToLower(s[i:])
	if rest == "inf" || rest == "infinity" || rest == "nan" {
		return true
	}
	digits := 0
	for i < len(s) && isDigit(s[i]) {
		i++
		digits++
	}
	if i < len(s) && s[i] == '.' {
		i++
		for i < len(s) && isDigit(s[i]) {
			i++
			digits++
		}
	}
	if digits == 0 {
		return false
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			i++
		}
		ed := 0
		for i < len(s) && isDigit(s[i]) {
			i++
			ed++
		}
		if ed == 0 {
			return false
		}
	}
	return i == len(s)
}

// trailingDataErr marks a date/time text whose leading part parsed but which
// has unexpected trailing data at pos. Input formats report the wrapped
// CANNOT_PARSE_DATE/DATETIME error; CAST re-codes it as ClickHouse does
// (CANNOT_PARSE_TEXT "syntax error at position N (parsed just '...')").
type trailingDataErr struct {
	base *cherr.Error
	s    string
	pos  int
}

func (e *trailingDataErr) Error() string { return e.base.Error() }
func (e *trailingDataErr) Unwrap() error { return e.base }

func trailingData(s string, pos int, err error) error {
	var ce *cherr.Error
	if !errors.As(err, &ce) {
		return err
	}
	return &trailingDataErr{base: ce, s: s, pos: pos}
}

// plainErr unwraps package-internal error markers so callers always get a
// plain *cherr.Error.
func plainErr(err error) error {
	if td, ok := err.(*trailingDataErr); ok {
		return td.base
	}
	return err
}

// castParseErr re-codes a trailing-data error for CAST(String AS t).
func castParseErr(err error, t Type) error {
	var td *trailingDataErr
	if errors.As(err, &td) && td.pos > 0 && td.pos <= len(td.s) {
		return errf(cherr.CannotParseText, "Cannot parse string '%s' as %s: syntax error at position %d (parsed just '%s')", td.s, t.String(), td.pos, td.s[:td.pos])
	}
	return plainErr(err)
}

// parseDateText parses YYYY-MM-DD, YYYY-M-D (any non-digit separators) or
// YYYYMMDD. Out-of-range dates clamp to the type's range like ClickHouse.
// With allowTime (CAST from String), a trailing " hh:mm:ss[.frac]" is
// accepted and ignored.
func parseDateText(s string, k Kind, allowTime bool) (any, error) {
	y, m, d, n, err := scanDate(s)
	if err != nil {
		return nil, err
	}
	if n != len(s) {
		if !allowTime {
			return nil, errf(codeCannotParseDate, "Cannot parse date: unexpected data after date in '%s'", s)
		}
		if _, err := parseDateTime64Text(s, 9, time.UTC); err != nil {
			return nil, trailingData(s, n, errf(codeCannotParseDate, "Cannot parse date from '%s'", s))
		}
	}
	t, zero, err := civil(y, m, d, 0, 0, 0, time.UTC)
	if err != nil {
		return nil, errf(codeCannotParseDate, "Cannot parse date from '%s': invalid month or day", s)
	}
	if zero && k == Date32 {
		return minDate32, nil
	}
	return clampDate(t, k)
}

// scanDate reads the date prefix of s and returns the number of bytes used.
func scanDate(s string) (y, m, d, n int, err error) {
	bad := func() (int, int, int, int, error) {
		return 0, 0, 0, 0, errf(codeCannotParseDate, "Cannot parse date: value is too short or malformed: '%s'", s)
	}
	if len(s) < 8 {
		return bad()
	}
	for i := 0; i < 4; i++ {
		if !isDigit(s[i]) {
			return bad()
		}
		y = y*10 + int(s[i]-'0')
	}
	if isDigit(s[4]) {
		// YYYYMMDD
		for i := 4; i < 8; i++ {
			if !isDigit(s[i]) {
				return bad()
			}
		}
		m = int(s[4]-'0')*10 + int(s[5]-'0')
		d = int(s[6]-'0')*10 + int(s[7]-'0')
		return y, m, d, 8, nil
	}
	i := 5
	readPart := func() (int, bool) {
		if i >= len(s) || !isDigit(s[i]) {
			return 0, false
		}
		v := int(s[i] - '0')
		i++
		if i < len(s) && isDigit(s[i]) {
			v = v*10 + int(s[i]-'0')
			i++
		}
		return v, true
	}
	var ok bool
	if m, ok = readPart(); !ok {
		return bad()
	}
	if i >= len(s) || isDigit(s[i]) {
		return bad()
	}
	i++
	if d, ok = readPart(); !ok {
		return bad()
	}
	return y, m, d, i, nil
}

// scanDateTimeParts parses the fixed-layout "YYYY-MM-DD[( |T)hh:mm:ss]"
// prefix (separators may be any non-digit) and returns the consumed length.
func scanDateTimeParts(s string) (p [6]int, n int, err error) {
	bad := func(what string) ([6]int, int, error) {
		return p, 0, errf(cherr.CannotParseDateTime, "Cannot parse %s of DateTime from '%s'", what, s)
	}
	if len(s) < 10 {
		return bad("date component")
	}
	digitsAt := func(idx ...int) bool {
		for _, i := range idx {
			if !isDigit(s[i]) {
				return false
			}
		}
		return true
	}
	if !digitsAt(0, 1, 2, 3, 5, 6, 8, 9) || isDigit(s[4]) || isDigit(s[7]) {
		return bad("date component")
	}
	two := func(i int) int { return int(s[i]-'0')*10 + int(s[i+1]-'0') }
	p[0] = int(s[0]-'0')*1000 + int(s[1]-'0')*100 + two(2)
	p[1], p[2] = two(5), two(8)
	n = 10
	if len(s) > 10 && (s[10] == ' ' || s[10] == 'T') {
		if len(s) < 19 || !digitsAt(11, 12, 14, 15, 17, 18) || isDigit(s[13]) || isDigit(s[16]) {
			return bad("time component")
		}
		p[3], p[4], p[5] = two(11), two(14), two(17)
		n = 19
	}
	return p, n, nil
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isDigit(s[i]) {
			return false
		}
	}
	return true
}

// parseDateTimeText parses "YYYY-MM-DD hh:mm:ss", "YYYY-MM-DDThh:mm:ss",
// "YYYY-MM-DD" (wall clock in loc) or a unix timestamp of at least five
// digits. Times before the epoch clamp to it (ClickHouse behaviour).
func parseDateTimeText(s string, loc *time.Location) (any, error) {
	return parseDateTimeTextSat(s, loc, false)
}

// parseDateTimeTextSat is parseDateTimeText that, with saturate (CAST from
// String, as ClickHouse's toDateTime), saturates times after
// 2106-02-07 06:28:15 to it instead of rejecting them. Input formats and
// query parameters reject them (ClickHouse wraps them modulo 2^32).
func parseDateTimeTextSat(s string, loc *time.Location, saturate bool) (any, error) {
	if len(s) >= 5 && isDigit(s[4]) {
		if !allDigits(s) {
			return nil, errf(cherr.CannotParseDateTime, "Cannot parse DateTime from '%s'", s)
		}
		u, err := strconv.ParseUint(s, 10, 64)
		if saturate {
			if err != nil {
				return nil, errf(codeCannotParseNumber, "Overflow while parsing a number: Cannot parse DateTime from String")
			}
			if u > math.MaxUint32 {
				return maxDateTime, nil
			}
		}
		if err != nil || u > math.MaxUint32 {
			return nil, errf(codeArgumentOutOfBound, "Unix timestamp %s is out of range of DateTime", s)
		}
		return time.Unix(int64(u), 0).UTC(), nil
	}
	p, n, err := scanDateTimeParts(s)
	if err != nil {
		return nil, err
	}
	if n != len(s) {
		return nil, trailingData(s, n, errf(cherr.CannotParseDateTime, "Cannot parse DateTime from '%s': unexpected data at position %d", s, n))
	}
	t, _, err := civil(p[0], p[1], p[2], p[3], p[4], p[5], loc)
	if err != nil {
		return nil, err
	}
	if t.Before(Epoch) {
		return Epoch, nil
	}
	if saturate && t.After(maxDateTime) {
		return maxDateTime, nil
	}
	if err := checkDateTimeRange(t); err != nil {
		return nil, err
	}
	return t, nil
}

// dt64ScaledThreshold is 2300-01-01 UTC in seconds: larger integer inputs
// are unix timestamps already scaled by 10^precision (ClickHouse rule).
const dt64ScaledThreshold = 10413792000

// maxUnixSec64 bounds unix seconds so that seconds*1e9 fits in an int64.
const maxUnixSec64 = math.MaxInt64 / 1000000000

// parseDateTime64Text parses DateTime64 text: the DateTime layouts with an
// optional ".fraction" (extra digits beyond the precision are truncated),
// or a unix timestamp "[-]secs[.frac]" (an integer of at least 5 digits, or
// any negative/fractional number); integers >= 10413792000 are taken as
// already scaled by 10^precision.
func parseDateTime64Text(s string, prec int, loc *time.Location) (any, error) {
	i := 0
	neg := false
	if i < len(s) && s[i] == '-' {
		neg = true
		i++
	}
	j := i
	for j < len(s) && isDigit(s[j]) {
		j++
	}
	nd := j - i
	dateLayout := !neg && nd == 4 && j < len(s)
	var t time.Time
	var fracStart int
	zeroDate := false
	if dateLayout {
		p, n, err := scanDateTimeParts(s)
		if err != nil {
			return nil, err
		}
		tt, zero, err := civil(p[0], p[1], p[2], p[3], p[4], p[5], loc)
		if err != nil {
			return nil, err
		}
		t, zeroDate = tt, zero
		fracStart = n
	} else {
		if nd == 0 || (nd < 5 && !neg && (j >= len(s) || s[j] != '.')) {
			return nil, errf(cherr.CannotParseDateTime, "Cannot parse DateTime from '%s'", s)
		}
		whole, err := strconv.ParseInt(s[i:j], 10, 64)
		if err != nil && neg {
			whole = maxUnixSec64 + 1 // far before 1900: clamped below
		} else if err != nil {
			return nil, errf(codeDecimalOverflow, "Decimal math overflow parsing DateTime64 from '%s'", s)
		}
		var nanos int64
		if j < len(s) && s[j] == '.' {
			fracStart = j
		} else {
			fracStart = j
			if whole >= dt64ScaledThreshold && prec > 0 {
				scale := pow10[prec]
				nanos = (whole % scale) * pow10[9-prec]
				whole /= scale
			}
		}
		if whole > maxUnixSec64 && !neg {
			return nil, errf(codeDecimalOverflow, "Decimal math overflow parsing DateTime64 from '%s'", s)
		}
		frac, n := scanFraction(s[fracStart:], prec)
		if fracStart+n != len(s) {
			return nil, trailingData(s, fracStart+n, errf(cherr.CannotParseDateTime, "Cannot parse DateTime64 from '%s': unexpected data at position %d", s, fracStart+n))
		}
		if neg && whole > maxUnixSec64 {
			whole, nanos, frac = maxUnixSec64, 0, 0
		}
		total := whole*1e9 + nanos + frac
		if neg {
			total = -total
		}
		t = time.Unix(0, total).UTC()
		if t.Before(minDateTime64) {
			// ClickHouse clamps to 1900-01-01 00:00:00 in the column's zone.
			t = time.Date(1900, 1, 1, 0, 0, 0, 0, loc).UTC()
		}
		if err := checkDateTime64Range(t); err != nil {
			return nil, err
		}
		return t, nil
	}
	frac, n := scanFraction(s[fracStart:], prec)
	if fracStart+n != len(s) {
		return nil, trailingData(s, fracStart+n, errf(cherr.CannotParseDateTime, "Cannot parse DateTime64 from '%s': unexpected data at position %d", s, fracStart+n))
	}
	if zeroDate {
		return Epoch, nil
	}
	t = clampDateTime64Date(t, loc).Add(time.Duration(frac))
	if err := checkDateTime64Range(t); err != nil {
		return nil, err
	}
	return t, nil
}

// scanFraction reads ".digits" (if present) and returns the nanoseconds it
// represents truncated to prec digits, plus the bytes consumed.
func scanFraction(s string, prec int) (int64, int) {
	if s == "" || s[0] != '.' {
		return 0, 0
	}
	i := 1
	var v int64
	nd := 0
	for i < len(s) && isDigit(s[i]) {
		if nd < prec {
			v = v*10 + int64(s[i]-'0')
			nd++
		}
		i++
	}
	for ; nd < 9; nd++ {
		v *= 10
	}
	// v now has 9 digits of which only the first prec are significant.
	return v, i
}
