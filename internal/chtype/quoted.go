package chtype

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
)

// quotedParser reads values in ClickHouse's quoted form. In strict mode it
// follows the element grammar ClickHouse uses inside Array/Tuple/Map text
// (query parameters, TSV, CAST from String): strings, dates and UUIDs must
// be quoted, unsigned integers take no sign, Bool is true/false. Lenient
// mode (ParseQuoted, i.e. VALUES) additionally accepts what ClickHouse's
// VALUES literal evaluation accepts for plain literals: any numeric literal
// converted with CAST semantics, numbers for Bool/Enum/Date/DateTime, and
// quoted strings for numbers.
type quotedParser struct {
	s       string
	pos     int
	lenient bool
	// saturate selects CAST(String AS composite) semantics: DateTime
	// strings past 2106-02-07 06:28:15 saturate instead of erroring.
	saturate bool
}

func parseQuoted(t Type, s string) (any, int, error) {
	p := &quotedParser{s: s, lenient: true}
	p.skipSpace()
	v, err := p.parseValue(t)
	if err != nil {
		return nil, 0, err
	}
	return v, p.pos, nil
}

func (p *quotedParser) skipSpace() {
	for p.pos < len(p.s) && isSpace(p.s[p.pos]) {
		p.pos++
	}
}

func (p *quotedParser) peekByte() byte {
	if p.pos < len(p.s) {
		return p.s[p.pos]
	}
	return 0
}

func (p *quotedParser) eof() bool { return p.pos >= len(p.s) }

func (p *quotedParser) found() string {
	if p.eof() {
		return "end of stream"
	}
	return "'" + string(p.s[p.pos]) + "'"
}

// peekWord reports whether the case-insensitive keyword w starts at pos and
// is not followed by an identifier character.
func (p *quotedParser) peekWord(w string) bool {
	end := p.pos + len(w)
	if end > len(p.s) || !strings.EqualFold(p.s[p.pos:end], w) {
		return false
	}
	return end == len(p.s) || !isIdentChar(p.s[end])
}

func (p *quotedParser) parseValue(t Type) (any, error) {
	switch t.Kind {
	case Nullable:
		if p.peekWord("NULL") {
			p.pos += 4
			return nil, nil
		}
		return p.parseValue(*t.Elem)
	case LowCardinality, SimpleAggregateFunction:
		return p.parseValue(*t.Elem)
	case Nothing:
		if p.peekWord("NULL") {
			p.pos += 4
			return nil, nil
		}
		return nil, errf(cherr.CannotParseText, "Cannot parse value of type Nothing")
	case AggregateFunction:
		return nil, errf(cherr.NotImplemented, "Text parsing of %s is not supported", t.String())
	}
	if p.peekWord("NULL") {
		// input_format_null_as_default: NULL in a non-Nullable slot is the default.
		p.pos += 4
		return zeroValue(t), nil
	}
	switch t.Kind {
	case String, FixedString:
		var s string
		if p.peekByte() == '\'' {
			v, err := p.readString()
			if err != nil {
				return nil, err
			}
			s = v
		} else if p.lenient {
			num, err := p.readNumber()
			if err != nil {
				return nil, p.quoteErr()
			}
			s = formatNumberLiteral(num)
		} else {
			return nil, p.quoteErr()
		}
		if t.Kind == FixedString {
			return padFixedString(s, t.N)
		}
		return s, nil
	case Enum8, Enum16:
		if p.peekByte() == '\'' {
			s, err := p.readString()
			if err != nil {
				return nil, err
			}
			return enumFromText(t, s)
		}
		if p.lenient {
			num, err := p.readNumber()
			if err != nil {
				return nil, p.quoteErr()
			}
			iv, ok := numberAsInt64(num)
			if !ok {
				return nil, errf(codeUnknownElementOfEnum, "Unexpected value %v in enum", num)
			}
			return enumByValue(t, iv)
		}
		return nil, p.quoteErr()
	case UUID:
		if p.peekByte() != '\'' {
			return nil, p.quoteErr()
		}
		s, err := p.readString()
		if err != nil {
			return nil, err
		}
		return parseUUID(s)
	case Bool:
		return p.parseBool()
	case UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64:
		return p.parseNumber(t.Kind)
	case Date, Date32:
		if p.peekByte() == '\'' {
			s, err := p.readString()
			if err != nil {
				return nil, err
			}
			return parseDateText(s, t.Kind, false)
		}
		if p.lenient {
			num, err := p.readNumber()
			if err != nil {
				return nil, p.quoteErr()
			}
			return numberToDate(num, t.Kind)
		}
		return nil, errf(codeCannotParseInputAssertion, "Cannot parse input: expected '\\'' before: '%s'", p.rest())
	case DateTime:
		if p.peekByte() == '\'' {
			s, err := p.readString()
			if err != nil {
				return nil, err
			}
			return parseDateTimeTextSat(s, locationOf(t), p.saturate)
		}
		if p.lenient {
			num, err := p.readNumber()
			if err != nil {
				return nil, p.quoteErr()
			}
			return numberToDateTime(num)
		}
		// Unquoted: the raw value, i.e. unix seconds.
		digits := p.readDigits()
		if digits == "" {
			return nil, p.quoteErr()
		}
		u, err := strconv.ParseUint(digits, 10, 64)
		if err != nil || u > math.MaxUint32 {
			return nil, errf(codeArgumentOutOfBound, "Unix timestamp %s is out of range of DateTime", digits)
		}
		return time.Unix(int64(u), 0).UTC(), nil
	case DateTime64:
		if p.peekByte() == '\'' {
			s, err := p.readString()
			if err != nil {
				return nil, err
			}
			return parseDateTime64Text(s, t.Precision, locationOf(t))
		}
		// Unquoted integers are the raw value: ticks of 10^-precision seconds.
		// Other numeric literals (VALUES only) are seconds with a fraction.
		start := p.pos
		if p.peekByte() == '-' {
			p.pos++
		}
		digits := p.readDigits()
		if digits != "" && (p.eof() || !(isIdentChar(p.peekByte()) || p.peekByte() == '.')) {
			return ticksToDateTime64(p.s[start:p.pos], t.Precision)
		}
		p.pos = start
		if !p.lenient {
			return nil, p.quoteErr()
		}
		if _, err := p.readNumber(); err != nil {
			return nil, p.quoteErr()
		}
		return parseDateTime64Text(strings.TrimPrefix(p.s[start:p.pos], "+"), t.Precision, locationOf(t))
	case Array:
		return p.parseArray(t)
	case Tuple:
		return p.parseTuple(t)
	case Map:
		return p.parseMap(t)
	}
	return nil, errf(cherr.NotImplemented, "Parsing of type %s is not supported", t.String())
}

func (p *quotedParser) rest() string {
	r := p.s[p.pos:]
	if len(r) > 32 {
		r = r[:32]
	}
	return r
}

func (p *quotedParser) quoteErr() error {
	return errf(codeCannotParseQuotedString, "Cannot parse quoted string: expected opening quote ''', got %s", p.found())
}

func (p *quotedParser) readString() (string, error) {
	v, n, err := readQuotedStringLit(p.s[p.pos:], '\'')
	if err != nil {
		return "", err
	}
	p.pos += n
	return v, nil
}

func (p *quotedParser) readDigits() string {
	start := p.pos
	for p.pos < len(p.s) && isDigit(p.s[p.pos]) {
		p.pos++
	}
	return p.s[start:p.pos]
}

// scanNumberToken returns the extent of a numeric-literal-looking token.
func (p *quotedParser) scanNumberToken() string {
	start := p.pos
	i := p.pos
	if i < len(p.s) && (p.s[i] == '+' || p.s[i] == '-') {
		i++
	}
	for i < len(p.s) {
		c := p.s[i]
		if isIdentChar(c) || c == '.' {
			i++
			continue
		}
		if (c == '+' || c == '-') && i > start && (p.s[i-1] == 'e' || p.s[i-1] == 'E') && !strings.HasPrefix(strings.ToLower(p.s[start:i]), "0x") {
			i++
			continue
		}
		break
	}
	return p.s[start:i]
}

// readNumber reads a SQL numeric literal (lenient mode).
func (p *quotedParser) readNumber() (any, error) {
	tok := p.scanNumberToken()
	if tok == "" {
		return nil, errf(cherr.CannotParseText, "Cannot parse number: found %s", p.found())
	}
	v, err := parseNumberLiteral(tok)
	if err != nil {
		return nil, err
	}
	p.pos += len(tok)
	return v, nil
}

func (p *quotedParser) parseBool() (any, error) {
	switch {
	case p.peekWord("true"):
		p.pos += 4
		return true, nil
	case p.peekWord("false"):
		p.pos += 5
		return false, nil
	case p.peekByte() == '\'':
		s, err := p.readString()
		if err != nil {
			return nil, err
		}
		return parseBoolText(s)
	}
	if p.lenient {
		if num, err := p.readNumber(); err == nil {
			return convertNumber(num, Bool)
		}
	}
	return nil, errf(codeCannotParseBool, "Cannot parse boolean value here: %s", p.found())
}

// parseNumber reads an unquoted number for a numeric kind.
func (p *quotedParser) parseNumber(k Kind) (any, error) {
	if !p.lenient {
		tok := p.scanNumberToken()
		if tok == "" {
			return nil, p.arrayElemErr()
		}
		if k == Float32 || k == Float64 {
			v, err := parseFloatText(tok, k)
			if err != nil {
				return nil, err
			}
			p.pos += len(tok)
			return v, nil
		}
		// Strict integers: [-]digits; stop at the first non-digit so that the
		// enclosing composite reports the offending character.
		start := p.pos
		if p.peekByte() == '-' && isSignedInt(k) {
			p.pos++
		}
		if p.readDigits() == "" {
			p.pos = start
			return nil, p.arrayElemErr()
		}
		return parseIntText(p.s[start:p.pos], k, true)
	}
	switch {
	case p.peekByte() == '\'':
		s, err := p.readString()
		if err != nil {
			return nil, err
		}
		return convertFromString(s, Type{Kind: k})
	case p.peekWord("true"):
		p.pos += 4
		return convertNumber(true, k)
	case p.peekWord("false"):
		p.pos += 5
		return convertNumber(false, k)
	}
	if k == Float32 || k == Float64 {
		// Parse float literals directly so that -0 keeps its sign.
		if tok := p.scanNumberToken(); validFloatSyntax(tok) {
			v, err := parseFloatText(tok, k)
			if err == nil {
				p.pos += len(tok)
			}
			return v, err
		}
	}
	num, err := p.readNumber()
	if err != nil {
		return nil, err
	}
	return convertNumber(num, k)
}

func (p *quotedParser) arrayElemErr() error {
	return errf(codeCannotReadArrayFromText, "Cannot read array from text, expected comma or end of array, found %s", p.found())
}

func (p *quotedParser) parseArray(t Type) (any, error) {
	if p.peekByte() != '[' {
		return nil, errf(codeCannotReadArrayFromText, "Array does not start with '[' character")
	}
	p.pos++
	out := []any{}
	p.skipSpace()
	for {
		if p.peekByte() == ']' {
			p.pos++
			return out, nil
		}
		if p.eof() {
			return nil, errf(codeCannotParseInputAssertion, "Cannot parse input: expected ']' at end of stream")
		}
		v, err := p.parseValue(*t.Elem)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
		p.skipSpace()
		switch p.peekByte() {
		case ',':
			p.pos++
			p.skipSpace()
		case ']':
		default:
			if p.eof() {
				return nil, errf(codeCannotParseInputAssertion, "Cannot parse input: expected ']' at end of stream")
			}
			return nil, p.arrayElemErr()
		}
	}
}

func (p *quotedParser) parseTuple(t Type) (any, error) {
	if p.peekByte() != '(' {
		return nil, errf(codeCannotParseInputAssertion, "Cannot parse input: expected '(' before: '%s'", p.rest())
	}
	p.pos++
	p.skipSpace()
	out := make([]any, 0, len(t.Fields))
	for i, f := range t.Fields {
		if i > 0 {
			if p.peekByte() != ',' {
				return nil, errf(codeCannotParseInputAssertion, "Cannot parse input: expected ',' before: '%s'", p.rest())
			}
			p.pos++
			p.skipSpace()
		}
		v, err := p.parseValue(f.Type)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
		p.skipSpace()
	}
	if p.peekByte() == ',' && len(t.Fields) > 0 {
		p.pos++
		p.skipSpace()
	}
	if p.peekByte() != ')' {
		return nil, errf(codeCannotParseInputAssertion, "Cannot parse input: expected ')' before: '%s'", p.rest())
	}
	p.pos++
	return out, nil
}

func (p *quotedParser) parseMap(t Type) (any, error) {
	if p.peekByte() != '{' {
		return nil, errf(codeCannotParseInputAssertion, "Cannot parse input: expected '{' before: '%s'", p.rest())
	}
	p.pos++
	out := []any{}
	p.skipSpace()
	for {
		if p.peekByte() == '}' {
			p.pos++
			return out, nil
		}
		if p.eof() {
			return nil, errf(codeCannotParseInputAssertion, "Cannot parse input: expected '}' at end of stream")
		}
		k, err := p.parseValue(*t.Key)
		if err != nil {
			return nil, err
		}
		p.skipSpace()
		if p.peekByte() != ':' {
			return nil, errf(codeCannotParseInputAssertion, "Cannot parse input: expected ':' before: '%s'", p.rest())
		}
		p.pos++
		p.skipSpace()
		v, err := p.parseValue(*t.Elem)
		if err != nil {
			return nil, err
		}
		out = append(out, []any{k, v})
		p.skipSpace()
		switch p.peekByte() {
		case ',':
			p.pos++
			p.skipSpace()
		case '}':
		default:
			return nil, errf(codeCannotParseInputAssertion, "Cannot parse input: expected '}' before: '%s'", p.rest())
		}
	}
}

// parseNumberLiteral parses a SQL numeric literal with ClickHouse typing:
// non-negative integers -> uint64, negative integers -> int64, anything with
// a fraction/exponent, inf/nan, or an integer beyond 64 bits -> float64.
// Hex (0x..) and binary (0b..) integers are accepted.
func parseNumberLiteral(s string) (any, error) {
	bad := func() (any, error) {
		return nil, errf(cherr.SyntaxError, "Cannot parse numeric literal '%s'", s)
	}
	neg := false
	body := s
	if body != "" && (body[0] == '-' || body[0] == '+') {
		neg = body[0] == '-'
		body = body[1:]
	}
	if body == "" {
		return bad()
	}
	signed := func(u uint64) any {
		switch {
		case !neg:
			return u
		case u <= 1<<63:
			return -int64(u) // u == 1<<63 wraps to MinInt64, which is exact
		default:
			return -float64(u)
		}
	}
	lb := strings.ToLower(body)
	switch {
	case lb == "inf" || lb == "infinity":
		if neg {
			return math.Inf(-1), nil
		}
		return math.Inf(1), nil
	case lb == "nan":
		if neg {
			return math.Copysign(math.NaN(), -1), nil // sign bit kept, as in ClickHouse
		}
		return math.NaN(), nil
	case strings.HasPrefix(lb, "0x") || strings.HasPrefix(lb, "0b"):
		base := 16
		if lb[1] == 'b' {
			base = 2
		}
		u, err := strconv.ParseUint(body[2:], base, 64)
		if err != nil {
			return bad()
		}
		return signed(u), nil
	case allDigits(body):
		if u, err := strconv.ParseUint(body, 10, 64); err == nil {
			return signed(u), nil
		}
	}
	if !validFloatSyntax(body) {
		return bad()
	}
	f, err := strconv.ParseFloat(body, 64)
	if err != nil {
		if ne, ok := err.(*strconv.NumError); !ok || ne.Err != strconv.ErrRange {
			return bad()
		}
	}
	if neg {
		f = -f
	}
	return f, nil
}

func numberAsInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case uint64:
		if x <= math.MaxInt64 {
			return int64(x), true
		}
	case int64:
		return x, true
	case float64:
		if x == math.Trunc(x) && x >= math.MinInt64 && x < math.MaxInt64 {
			return int64(x), true
		}
	}
	return 0, false
}

// formatNumberLiteral renders a parsed numeric literal as CAST(x AS String) would.
func formatNumberLiteral(v any) string {
	switch x := v.(type) {
	case uint64:
		return strconv.FormatUint(x, 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return formatFloat(x, 64)
	}
	return ""
}

// numberToDate follows ClickHouse toDate(number): values up to 65535 are day
// numbers, larger ones unix timestamps; the result clamps to the type range.
func numberToDate(v any, k Kind) (any, error) {
	var f float64
	switch x := v.(type) {
	case uint64:
		f = float64(x)
	case int64:
		f = float64(x)
	case float64:
		f = x
	case bool:
		if x {
			f = 1
		}
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, errf(cherr.CannotConvertType, "Unexpected inf or nan to Date conversion")
	}
	f = math.Trunc(f)
	maxDays := 65535.0
	if k == Date32 {
		maxDays = 120529
	}
	var t time.Time
	if f > maxDays {
		if f > float64(math.MaxUint32) {
			f = math.MaxUint32
		}
		t = dateOf(time.Unix(int64(f), 0), time.UTC)
	} else {
		if f < -1e6 {
			f = -1e6
		}
		t = Epoch.AddDate(0, 0, int(f))
	}
	return clampDate(t, k)
}

// numberToDateTime follows CAST(number AS DateTime): unix seconds,
// truncated, saturating to the DateTime range.
func numberToDateTime(v any) (any, error) {
	switch x := v.(type) {
	case uint64:
		if x > math.MaxUint32 {
			return maxDateTime, nil
		}
		return time.Unix(int64(x), 0).UTC(), nil
	case int64:
		if x < 0 {
			return Epoch, nil
		}
		if x > math.MaxUint32 {
			return maxDateTime, nil
		}
		return time.Unix(x, 0).UTC(), nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil, errf(cherr.CannotConvertType, "Unexpected inf or nan to DateTime conversion")
		}
		x = math.Trunc(x)
		if x < 0 {
			return Epoch, nil
		}
		if x > math.MaxUint32 {
			return maxDateTime, nil
		}
		return time.Unix(int64(x), 0).UTC(), nil
	case bool:
		if x {
			return time.Unix(1, 0).UTC(), nil
		}
		return Epoch, nil
	}
	return nil, errf(cherr.CannotConvertType, "Cannot convert %T to DateTime", v)
}

// numberToDateTime64 converts unix seconds (with fraction) to DateTime64.
func numberToDateTime64(v any, prec int) (any, error) {
	var t time.Time
	switch x := v.(type) {
	case uint64:
		if x > maxUnixSec64 {
			return nil, errf(codeDecimalOverflow, "Value %d is out of range of DateTime64", x)
		}
		t = time.Unix(int64(x), 0).UTC()
	case int64:
		if x > maxUnixSec64 {
			return nil, errf(codeDecimalOverflow, "Value %d is out of range of DateTime64", x)
		}
		t = time.Unix(max(x, -maxUnixSec64), 0).UTC()
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 1) || x > float64(maxUnixSec64) {
			return nil, errf(codeDecimalOverflow, "Value %v is out of range of DateTime64", x)
		}
		x = max(x, -float64(maxUnixSec64))
		sec := math.Floor(x)
		ns := math.Round((x - sec) * 1e9)
		t = time.Unix(int64(sec), int64(ns)).UTC()
	case bool:
		if x {
			t = time.Unix(1, 0).UTC()
		} else {
			t = Epoch
		}
	default:
		return nil, errf(cherr.CannotConvertType, "Cannot convert %T to DateTime64", v)
	}
	if t.Before(minDateTime64) {
		t = minDateTime64 // ClickHouse saturates at 1900-01-01 00:00:00 UTC
	}
	t = truncPrecision(t, prec)
	if err := checkDateTime64Range(t); err != nil {
		return nil, err
	}
	return t, nil
}

// ticksToDateTime64 converts a raw DateTime64 integer (units of
// 10^-prec seconds) to a time.
func ticksToDateTime64(text string, prec int) (any, error) {
	ticks, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return nil, errf(codeDecimalOverflow, "Decimal math overflow parsing DateTime64 from '%s'", text)
	}
	scale := pow10[prec]
	sec, rem := ticks/scale, ticks%scale
	if rem < 0 {
		sec--
		rem += scale
	}
	if sec > maxUnixSec64 || sec < -maxUnixSec64 {
		return nil, errf(codeDecimalOverflow, "Value %s is out of range of DateTime64", text)
	}
	tm := time.Unix(sec, rem*pow10[9-prec]).UTC()
	if err := checkDateTime64Range(tm); err != nil {
		return nil, err
	}
	return tm, nil
}
