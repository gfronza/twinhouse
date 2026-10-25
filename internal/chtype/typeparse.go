package chtype

import (
	"math"
	"slices"
	"strconv"
	"strings"

	"dynamodb-eventstream/internal/cherr"
)

// codeDataTypeCannotHaveArguments is DATA_TYPE_CANNOT_HAVE_ARGUMENTS.
const codeDataTypeCannotHaveArguments = 378

// codeUnexpectedASTStructure is UNEXPECTED_AST_STRUCTURE.
const codeUnexpectedASTStructure = 223

// ---------------------------------------------------------------------------
// Lexer

type tokKind uint8

const (
	tkEOF    tokKind = iota
	tkIdent          // bare identifier
	tkQIdent         // `quoted` or "quoted" identifier (text is unquoted)
	tkNumber         // numeric literal (text is the literal, sign included)
	tkString         // 'string' literal (text is the unescaped value)
	tkPunct          // ( ) , =
)

type token struct {
	kind tokKind
	text string
	pos  int
}

func lexType(s string) ([]token, error) {
	var toks []token
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case isSpace(c):
			i++
		case c == '(' || c == ')' || c == ',' || c == '=':
			toks = append(toks, token{tkPunct, string(c), i})
			i++
		case c == '\'':
			v, n, err := readQuotedStringLit(s[i:], '\'')
			if err != nil {
				return nil, errf(cherr.SyntaxError, "Syntax error (data type): failed at position %d: %s", i+1, s)
			}
			toks = append(toks, token{tkString, v, i})
			i += n
		case c == '`' || c == '"':
			v, n, err := readQuotedStringLit(s[i:], c)
			if err != nil {
				return nil, errf(cherr.SyntaxError, "Syntax error (data type): failed at position %d: %s", i+1, s)
			}
			toks = append(toks, token{tkQIdent, v, i})
			i += n
		case isIdentStart(c):
			j := i + 1
			for j < len(s) && isIdentChar(s[j]) {
				j++
			}
			toks = append(toks, token{tkIdent, s[i:j], i})
			i = j
		case isDigit(c) || ((c == '-' || c == '+' || c == '.') && i+1 < len(s) && (isDigit(s[i+1]) || s[i+1] == '.')):
			j := i
			if s[j] == '-' || s[j] == '+' {
				j++
			}
			for j < len(s) && (isDigit(s[j]) || s[j] == '.' || s[j] == '_' || isIdentChar(s[j]) ||
				((s[j] == '-' || s[j] == '+') && (s[j-1] == 'e' || s[j-1] == 'E'))) {
				j++
			}
			toks = append(toks, token{tkNumber, s[i:j], i})
			i = j
		default:
			return nil, errf(cherr.SyntaxError, "Syntax error (data type): failed at position %d (%q): %s", i+1, string(c), s)
		}
	}
	toks = append(toks, token{tkEOF, "", len(s)})
	return toks, nil
}

// readQuotedStringLit reads a quoted literal starting at s[0] (the quote),
// processing backslash escapes and doubled quotes. It returns the value and
// the number of bytes consumed.
func readQuotedStringLit(s string, q byte) (string, int, error) {
	var b strings.Builder
	i := 1
	for i < len(s) {
		c := s[i]
		switch {
		case c == q:
			if i+1 < len(s) && s[i+1] == q {
				b.WriteByte(q)
				i += 2
				continue
			}
			return b.String(), i + 1, nil
		case c == '\\':
			n, err := unescapeSeq(s[i:], &b)
			if err != nil {
				return "", 0, err
			}
			i += n
		default:
			b.WriteByte(c)
			i++
		}
	}
	return "", 0, errf(codeCannotParseQuotedString, "Cannot parse quoted string: expected closing quote")
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}
func isDigit(c byte) bool      { return c >= '0' && c <= '9' }
func isIdentStart(c byte) bool { return c == '_' || (c|0x20 >= 'a' && c|0x20 <= 'z') }
func isIdentChar(c byte) bool  { return isIdentStart(c) || isDigit(c) }

// ---------------------------------------------------------------------------
// Parser

type typeParser struct {
	src  string
	toks []token
	pos  int
}

func parseType(s string) (Type, error) {
	toks, err := lexType(s)
	if err != nil {
		return Type{}, err
	}
	p := &typeParser{src: s, toks: toks}
	if p.peek().kind == tkEOF {
		return Type{}, errf(cherr.SyntaxError, "Syntax error (data type): empty type name")
	}
	t, err := p.parseType()
	if err != nil {
		return Type{}, err
	}
	if tk := p.peek(); tk.kind != tkEOF {
		return Type{}, p.syntaxErr(tk, "end of type name")
	}
	return t, nil
}

func (p *typeParser) peek() token { return p.toks[p.pos] }
func (p *typeParser) peekN(n int) token {
	if p.pos+n < len(p.toks) {
		return p.toks[p.pos+n]
	}
	return p.toks[len(p.toks)-1]
}
func (p *typeParser) next() token {
	t := p.toks[p.pos]
	if t.kind != tkEOF {
		p.pos++
	}
	return t
}

func (p *typeParser) syntaxErr(tk token, expected string) error {
	at := tk.text
	if tk.kind == tkEOF {
		at = "end of query"
	}
	return errf(cherr.SyntaxError, "Syntax error (data type): failed at position %d (%s): %s. Expected %s", tk.pos+1, at, p.src, expected)
}

func (p *typeParser) isPunct(s string) bool {
	tk := p.peek()
	return tk.kind == tkPunct && tk.text == s
}

func (p *typeParser) expectPunct(s string) error {
	tk := p.next()
	if tk.kind != tkPunct || tk.text != s {
		return p.syntaxErr(tk, "'"+s+"'")
	}
	return nil
}

// compound multi-word type names (upper-case, single-spaced).
var compoundTypes = map[string]string{
	"DOUBLE PRECISION":                "Float64",
	"CHARACTER LARGE OBJECT":          "String",
	"CHARACTER VARYING":               "String",
	"CHAR LARGE OBJECT":               "String",
	"CHAR VARYING":                    "String",
	"NATIONAL CHAR":                   "String",
	"NATIONAL CHARACTER":              "String",
	"NATIONAL CHARACTER LARGE OBJECT": "String",
	"NATIONAL CHARACTER VARYING":      "String",
	"NATIONAL CHAR VARYING":           "String",
	"NCHAR VARYING":                   "String",
	"NCHAR LARGE OBJECT":              "String",
	"BINARY LARGE OBJECT":             "String",
	"BINARY VARYING":                  "String",
	"TINYINT SIGNED":                  "Int8",
	"INT1 SIGNED":                     "Int8",
	"SMALLINT SIGNED":                 "Int16",
	"MEDIUMINT SIGNED":                "Int32",
	"INT SIGNED":                      "Int32",
	"INTEGER SIGNED":                  "Int32",
	"BIGINT SIGNED":                   "Int64",
	"TINYINT UNSIGNED":                "UInt8",
	"INT1 UNSIGNED":                   "UInt8",
	"SMALLINT UNSIGNED":               "UInt16",
	"MEDIUMINT UNSIGNED":              "UInt32",
	"INT UNSIGNED":                    "UInt32",
	"INTEGER UNSIGNED":                "UInt32",
	"BIGINT UNSIGNED":                 "UInt64",
}

// caseInsensitiveTypes maps upper-cased aliases (and the case-insensitive
// canonical families) to their canonical family.
var caseInsensitiveTypes = map[string]string{
	"BOOL": "Bool", "BOOLEAN": "Bool",
	"DATE": "Date", "DATE32": "Date32",
	"DATETIME": "DateTime", "TIMESTAMP": "DateTime", "DATETIME64": "DateTime64",
	"TINYINT": "Int8", "INT1": "Int8", "BYTE": "Int8",
	"SMALLINT": "Int16",
	"INT":      "Int32", "INTEGER": "Int32", "MEDIUMINT": "Int32",
	"BIGINT": "Int64", "SIGNED": "Int64",
	"YEAR":     "UInt16",
	"UNSIGNED": "UInt64", "BIT": "UInt64", "SET": "UInt64",
	"FLOAT": "Float32", "REAL": "Float32", "SINGLE": "Float32",
	"DOUBLE": "Float64",
	"CHAR":   "String", "NCHAR": "String", "CHARACTER": "String", "VARCHAR": "String",
	"NVARCHAR": "String", "VARCHAR2": "String", "TEXT": "String", "TINYTEXT": "String",
	"MEDIUMTEXT": "String", "LONGTEXT": "String", "BLOB": "String", "CLOB": "String",
	"TINYBLOB": "String", "MEDIUMBLOB": "String", "LONGBLOB": "String", "BYTEA": "String",
	"VARBINARY": "String",
	"BINARY":    "FixedString",
	"INET4":     "IPv4", "INET6": "IPv6",
	"DEC": "Decimal", "NUMERIC": "Decimal", "FIXED": "Decimal", "DECIMAL": "Decimal",
	"JSON": "JSON",
}

var canonicalFamilies = map[string]Kind{
	"Nothing": Nothing, "Bool": Bool,
	"UInt8": UInt8, "UInt16": UInt16, "UInt32": UInt32, "UInt64": UInt64,
	"Int8": Int8, "Int16": Int16, "Int32": Int32, "Int64": Int64,
	"Float32": Float32, "Float64": Float64,
	"String": String, "FixedString": FixedString, "UUID": UUID,
	"Date": Date, "Date32": Date32, "DateTime": DateTime, "DateTime64": DateTime64,
	"Enum8": Enum8, "Enum16": Enum16, "Enum": Enum8, // Enum: width picked from values
	"Nullable": Nullable, "LowCardinality": LowCardinality,
	"Array": Array, "Tuple": Tuple, "Map": Map,
	"AggregateFunction": AggregateFunction, "SimpleAggregateFunction": SimpleAggregateFunction,
}

// unsupportedFamilies are real ClickHouse types this server does not implement.
var unsupportedFamilies = map[string]bool{
	"Decimal": true, "Decimal32": true, "Decimal64": true, "Decimal128": true, "Decimal256": true,
	"Int128": true, "Int256": true, "UInt128": true, "UInt256": true, "BFloat16": true,
	"IPv4": true, "IPv6": true, "JSON": true, "Object": true, "Nested": true,
	"Variant": true, "Dynamic": true, "Time": true, "Time64": true,
	"Point": true, "Ring": true, "LineString": true, "MultiLineString": true,
	"Polygon": true, "MultiPolygon": true, "Geometry": true, "QBit": true,
	"IntervalNanosecond": true, "IntervalMicrosecond": true, "IntervalMillisecond": true,
	"IntervalSecond": true, "IntervalMinute": true, "IntervalHour": true, "IntervalDay": true,
	"IntervalWeek": true, "IntervalMonth": true, "IntervalQuarter": true, "IntervalYear": true,
}

// compoundAt reports the canonical family and token count of a multi-word
// type name starting at the current position, if any.
func (p *typeParser) compoundAt() (string, int) {
	for n := 4; n >= 2; n-- {
		words := make([]string, 0, n)
		ok := true
		for i := 0; i < n; i++ {
			tk := p.peekN(i)
			if tk.kind != tkIdent {
				ok = false
				break
			}
			words = append(words, strings.ToUpper(tk.text))
		}
		if !ok {
			continue
		}
		if fam, found := compoundTypes[strings.Join(words, " ")]; found {
			return fam, n
		}
	}
	return "", 0
}

func (p *typeParser) parseType() (Type, error) {
	tk := p.peek()
	if tk.kind != tkIdent && tk.kind != tkQIdent {
		return Type{}, p.syntaxErr(tk, "data type")
	}
	family := ""
	if fam, n := p.compoundAt(); n > 0 {
		family = fam
		p.pos += n
	} else {
		p.next()
		if _, ok := canonicalFamilies[tk.text]; ok {
			family = tk.text
		} else if fam, ok := caseInsensitiveTypes[strings.ToUpper(tk.text)]; ok {
			family = fam
		} else if unsupportedFamilies[tk.text] {
			family = tk.text
		} else {
			return Type{}, errf(cherr.UnknownType, "Unknown data type family: %s", tk.text)
		}
	}
	if unsupportedFamilies[family] {
		return Type{}, errf(cherr.NotImplemented, "Data type %s is not supported", family)
	}
	kind := canonicalFamilies[family]
	hasArgs := p.isPunct("(")
	var t Type
	var err error
	switch kind {
	case Nothing, Bool, UUID, Date, Date32:
		if hasArgs {
			return Type{}, errf(codeDataTypeCannotHaveArguments, "Data type %s cannot have arguments", family)
		}
		t = Type{Kind: kind}
	case UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64, String:
		// MySQL-style display widths (INT(11), VARCHAR(255), FLOAT(10)) are ignored.
		if hasArgs {
			if err := p.skipLiteralArgs(); err != nil {
				return Type{}, err
			}
		}
		t = Type{Kind: kind}
		if kind >= UInt8 && kind <= Int64 {
			t = p.applySignedness(t)
		}
	case FixedString:
		t, err = p.parseFixedString(hasArgs)
	case DateTime:
		t, err = p.parseDateTime(hasArgs)
	case DateTime64:
		t, err = p.parseDateTime64(hasArgs)
	case Enum8, Enum16:
		t, err = p.parseEnum(family, hasArgs)
	case Nullable, LowCardinality, Array:
		t, err = p.parseWrapper(kind, family, hasArgs)
	case Tuple:
		t, err = p.parseTuple(hasArgs)
	case Map:
		t, err = p.parseMap(hasArgs)
	case AggregateFunction:
		t, err = p.parseAggregateFunction(hasArgs)
	case SimpleAggregateFunction:
		t, err = p.parseSimpleAggregateFunction(hasArgs)
	default:
		return Type{}, errf(cherr.UnknownType, "Unknown data type family: %s", tk.text)
	}
	if err != nil {
		return Type{}, err
	}
	return t, nil
}

// applySignedness handles a trailing SIGNED/UNSIGNED after an integer alias
// with arguments, e.g. "BIGINT(20) UNSIGNED".
func (p *typeParser) applySignedness(t Type) Type {
	tk := p.peek()
	if tk.kind != tkIdent {
		return t
	}
	switch strings.ToUpper(tk.text) {
	case "UNSIGNED":
		p.next()
		switch t.Kind {
		case Int8:
			t.Kind = UInt8
		case Int16:
			t.Kind = UInt16
		case Int32:
			t.Kind = UInt32
		case Int64:
			t.Kind = UInt64
		}
	case "SIGNED":
		p.next()
		switch t.Kind {
		case UInt8:
			t.Kind = Int8
		case UInt16:
			t.Kind = Int16
		case UInt32:
			t.Kind = Int32
		case UInt64:
			t.Kind = Int64
		}
	}
	return t
}

func (p *typeParser) skipLiteralArgs() error {
	if err := p.expectPunct("("); err != nil {
		return err
	}
	for !p.isPunct(")") {
		tk := p.next()
		if tk.kind != tkNumber && tk.kind != tkString {
			return p.syntaxErr(tk, "literal")
		}
		if p.isPunct(",") {
			p.next()
		} else if !p.isPunct(")") {
			return p.syntaxErr(p.peek(), "',' or ')'")
		}
	}
	p.next()
	return nil
}

func parseUintLiteral(tk token) (uint64, bool) {
	if tk.kind != tkNumber {
		return 0, false
	}
	v, err := strconv.ParseUint(tk.text, 10, 64)
	return v, err == nil
}

func (p *typeParser) parseFixedString(hasArgs bool) (Type, error) {
	if !hasArgs {
		return Type{}, errf(cherr.NumberOfArgsDoesntMatch, "FixedString data type family must have exactly one argument - size in bytes")
	}
	p.next()
	tk := p.next()
	n, ok := parseUintLiteral(tk)
	if !ok || n == 0 || n > math.MaxInt32 {
		return Type{}, errf(cherr.BadArguments, "FixedString data type family must have a number (positive integer) as its argument")
	}
	if err := p.expectPunct(")"); err != nil {
		return Type{}, err
	}
	return Type{Kind: FixedString, N: int(n)}, nil
}

// parseDateTime handles DateTime, DateTime('tz') and DateTime(p[, 'tz'])
// (the latter is DateTime64, as in ClickHouse).
func (p *typeParser) parseDateTime(hasArgs bool) (Type, error) {
	if !hasArgs {
		return Type{Kind: DateTime}, nil
	}
	p.next()
	if p.peek().kind == tkNumber {
		return p.parseDateTime64Args()
	}
	t := Type{Kind: DateTime}
	if !p.isPunct(")") {
		tk := p.next()
		if tk.kind != tkString {
			return Type{}, errf(cherr.IllegalTypeOfArgument, "Parameter #0 'timezone' for DateTime has wrong type, expected String literal")
		}
		if err := checkTimezone(tk.text); err != nil {
			return Type{}, err
		}
		t.TZ = tk.text
	}
	if !p.isPunct(")") {
		return Type{}, errf(cherr.NumberOfArgsDoesntMatch, "DateTime data type can optionally have only one argument - time zone name")
	}
	p.next()
	return t, nil
}

func (p *typeParser) parseDateTime64(hasArgs bool) (Type, error) {
	if !hasArgs {
		return Type{Kind: DateTime64, Precision: 3}, nil
	}
	p.next()
	if p.isPunct(")") {
		p.next()
		return Type{Kind: DateTime64, Precision: 3}, nil
	}
	return p.parseDateTime64Args()
}

// parseDateTime64Args parses "p[, 'tz'])" after the opening parenthesis.
func (p *typeParser) parseDateTime64Args() (Type, error) {
	tk := p.next()
	prec, ok := parseUintLiteral(tk)
	if !ok {
		return Type{}, errf(cherr.IllegalTypeOfArgument, "Parameter #0 'scale' for DateTime64 has wrong type: expected UInt64 literal")
	}
	if prec > 9 {
		return Type{}, errf(codeArgumentOutOfBound, "Scale %d is too large for DateTime64. Maximum is up to nanoseconds (9)", prec)
	}
	t := Type{Kind: DateTime64, Precision: int(prec)}
	if p.isPunct(",") {
		p.next()
		tz := p.next()
		if tz.kind != tkString {
			return Type{}, errf(cherr.IllegalTypeOfArgument, "Parameter #1 'timezone' for DateTime64 has wrong type, expected String literal")
		}
		if err := checkTimezone(tz.text); err != nil {
			return Type{}, err
		}
		t.TZ = tz.text
	}
	if !p.isPunct(")") {
		return Type{}, errf(cherr.NumberOfArgsDoesntMatch, "DateTime64 data type can optionally have two argument - scale and time zone name")
	}
	p.next()
	return t, nil
}

func (p *typeParser) parseEnum(family string, hasArgs bool) (Type, error) {
	if !hasArgs {
		return Type{}, errf(cherr.EmptyDataPassed, "Enum data type cannot be empty")
	}
	p.next()
	var vals []EnumValue
	var explicit []bool
	var next int64 = 1
	seenName := map[string]int64{}
	seenVal := map[int64]string{}
	for !p.isPunct(")") {
		tk := p.next()
		if tk.kind != tkString {
			return Type{}, errf(cherr.BadArguments, "Elements of Enum data type must be of form: 'name' = number or 'name', where name is string literal and number is an integer")
		}
		v := next
		explicit = append(explicit, p.isPunct("="))
		if p.isPunct("=") {
			p.next()
			nt := p.next()
			if nt.kind != tkNumber {
				return Type{}, errf(cherr.BadArguments, "Elements of Enum data type must be of form: 'name' = number or 'name', where name is string literal and number is an integer")
			}
			text := strings.TrimPrefix(nt.text, "+")
			iv, err := strconv.ParseInt(text, 10, 64)
			if err != nil {
				return Type{}, errf(cherr.BadArguments, "Elements of Enum data type must be of form: 'name' = number or 'name', where name is string literal and number is an integer")
			}
			v = iv
		}
		if prev, dup := seenName[tk.text]; dup {
			return Type{}, errf(cherr.SyntaxError, "Duplicate names in enum: '%s' = %d and %d", tk.text, v, prev)
		}
		if prev, dup := seenVal[v]; dup {
			return Type{}, errf(cherr.SyntaxError, "Duplicate values in enum: '%s' = %d and '%s'", tk.text, v, prev)
		}
		seenName[tk.text] = v
		seenVal[v] = tk.text
		vals = append(vals, EnumValue{Name: tk.text, Value: v})
		next = v + 1
		if p.isPunct(",") {
			p.next()
		} else if !p.isPunct(")") {
			return Type{}, p.syntaxErr(p.peek(), "',' or ')'")
		}
	}
	p.next()
	if len(vals) == 0 {
		return Type{}, errf(cherr.EmptyDataPassed, "Enum data type cannot be empty")
	}
	// Implicit numbering is allowed only when no element has a value, or
	// when just the first one does (it sets the starting number).
	for i, e := range explicit {
		if e && i > 0 && slices.Contains(explicit, false) {
			return Type{}, errf(codeUnexpectedASTStructure, "All elements of Enum data type must be of form: 'name' = number or 'name', where name is string literal and number is an integer")
		}
	}
	kind := Enum8
	switch family {
	case "Enum16":
		kind = Enum16
	case "Enum":
		for _, ev := range vals {
			if ev.Value < math.MinInt8 || ev.Value > math.MaxInt8 {
				kind = Enum16
			}
		}
	}
	lo, hi := int64(math.MinInt8), int64(math.MaxInt8)
	if kind == Enum16 {
		lo, hi = math.MinInt16, math.MaxInt16
	}
	for _, ev := range vals {
		if ev.Value < lo || ev.Value > hi {
			return Type{}, errf(codeArgumentOutOfBound, "Value %d for element '%s' exceeds range of %s", ev.Value, ev.Name, kindName(kind))
		}
	}
	return Type{Kind: kind, EnumValues: vals}, nil
}

func (p *typeParser) parseWrapper(kind Kind, family string, hasArgs bool) (Type, error) {
	if !hasArgs {
		return Type{}, errf(cherr.NumberOfArgsDoesntMatch, "%s data type family must have exactly one argument", family)
	}
	p.next()
	if p.isPunct(")") {
		return Type{}, errf(cherr.NumberOfArgsDoesntMatch, "%s data type family must have exactly one argument", family)
	}
	inner, err := p.parseType()
	if err != nil {
		return Type{}, err
	}
	if !p.isPunct(")") {
		if p.isPunct(",") {
			return Type{}, errf(cherr.NumberOfArgsDoesntMatch, "%s data type family must have exactly one argument", family)
		}
		return Type{}, p.syntaxErr(p.peek(), "')'")
	}
	p.next()
	switch kind {
	case Nullable:
		if !canBeInsideNullable(inner) {
			return Type{}, errf(cherr.IllegalTypeOfArgument, "Nested type %s cannot be inside Nullable type", inner.String())
		}
		return NewNullable(inner), nil
	case LowCardinality:
		base := inner
		if base.Kind == Nullable {
			base = *base.Elem
		}
		if !canBeInsideLowCardinality(base) {
			return Type{}, errf(cherr.IllegalTypeOfArgument, "DataTypeLowCardinality is supported only for numbers, strings, Date or DateTime, but got %s", inner.String())
		}
		return NewLowCardinality(inner), nil
	default:
		return NewArray(inner), nil
	}
}

func canBeInsideNullable(t Type) bool {
	switch t.Kind {
	case Nullable, LowCardinality, Array, Tuple, Map, AggregateFunction, SimpleAggregateFunction:
		return false
	}
	return true
}

func canBeInsideLowCardinality(t Type) bool {
	switch t.Kind {
	case String, FixedString, UUID, Date, Date32, DateTime, DateTime64, Bool,
		UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64:
		return true
	}
	return false
}

func (p *typeParser) parseTuple(hasArgs bool) (Type, error) {
	if !hasArgs {
		return Type{Kind: Tuple}, nil
	}
	p.next()
	t := Type{Kind: Tuple}
	named, unnamed := 0, 0
	seen := map[string]bool{}
	for !p.isPunct(")") {
		var f Field
		tk := p.peek()
		switch {
		case tk.kind == tkQIdent:
			p.next()
			f.Name = tk.text
		case tk.kind == tkIdent && (p.peekN(1).kind == tkIdent || p.peekN(1).kind == tkQIdent):
			if _, n := p.compoundAt(); n == 0 {
				p.next()
				f.Name = tk.text
			}
		}
		ft, err := p.parseType()
		if err != nil {
			return Type{}, err
		}
		f.Type = ft
		if f.Name != "" {
			if seen[f.Name] {
				return Type{}, errf(codeDuplicateColumn, "Names of tuple elements must be unique. Duplicate name: %s", f.Name)
			}
			seen[f.Name] = true
			named++
		} else {
			unnamed++
		}
		t.Fields = append(t.Fields, f)
		if p.isPunct(",") {
			p.next()
			if p.isPunct(")") {
				return Type{}, p.syntaxErr(p.peek(), "data type")
			}
		} else if !p.isPunct(")") {
			return Type{}, p.syntaxErr(p.peek(), "',' or ')'")
		}
	}
	p.next()
	if named > 0 && unnamed > 0 {
		return Type{}, errf(cherr.BadArguments, "Names are specified not for all elements of Tuple type")
	}
	return t, nil
}

func (p *typeParser) parseMap(hasArgs bool) (Type, error) {
	errArgs := errf(cherr.NumberOfArgsDoesntMatch, "Map data type family must have two arguments: key and value types")
	if !hasArgs {
		return Type{}, errArgs
	}
	p.next()
	if p.isPunct(")") {
		return Type{}, errArgs
	}
	k, err := p.parseType()
	if err != nil {
		return Type{}, err
	}
	if !p.isPunct(",") {
		return Type{}, errArgs
	}
	p.next()
	v, err := p.parseType()
	if err != nil {
		return Type{}, err
	}
	if !p.isPunct(")") {
		return Type{}, errArgs
	}
	p.next()
	switch kb := k; {
	case kb.Kind == Nullable, kb.Kind == LowCardinality && kb.Elem.Kind == Nullable,
		kb.Kind == AggregateFunction, kb.Kind == SimpleAggregateFunction:
		return Type{}, errf(cherr.BadArguments, "Map cannot have a key of type %s", k.String())
	}
	return Type{Kind: Map, Key: &k, Elem: &v}, nil
}

// parseAggFunc parses "name" or "name(params...)".
func (p *typeParser) parseAggFunc(simple bool) (string, []any, error) {
	tk := p.next()
	if tk.kind != tkIdent && tk.kind != tkQIdent {
		if tk.kind == tkNumber {
			return "", nil, errf(cherr.UnknownAggregateFunction, "Unknown aggregate function %s", tk.text)
		}
		return "", nil, p.syntaxErr(tk, "aggregate function name")
	}
	name := canonicalAggName(tk.text)
	var params []any
	if p.isPunct("(") {
		if simple {
			return "", nil, errf(cherr.BadArguments, "Parametric aggregate functions are not supported in SimpleAggregateFunction")
		}
		p.next()
		for !p.isPunct(")") {
			pt := p.next()
			switch pt.kind {
			case tkNumber:
				v, err := parseNumberLiteral(pt.text)
				if err != nil {
					return "", nil, errf(cherr.SyntaxError, "Syntax error (data type): bad aggregate function parameter %s", pt.text)
				}
				params = append(params, v)
			case tkString:
				params = append(params, pt.text)
			default:
				return "", nil, p.syntaxErr(pt, "literal")
			}
			if p.isPunct(",") {
				p.next()
			} else if !p.isPunct(")") {
				return "", nil, p.syntaxErr(p.peek(), "',' or ')'")
			}
		}
		p.next()
	}
	return name, params, nil
}

// canonicalAggName lower-cases the aggregate functions ClickHouse registers
// case-insensitively.
func canonicalAggName(name string) string {
	switch l := strings.ToLower(name); l {
	case "count", "sum", "avg", "min", "max":
		return l
	}
	return name
}

func (p *typeParser) parseAggregateFunction(hasArgs bool) (Type, error) {
	if !hasArgs {
		return Type{}, errf(cherr.NumberOfArgsDoesntMatch, "Data type AggregateFunction requires parameters: name of aggregate function and list of data types for arguments")
	}
	p.next()
	name, params, err := p.parseAggFunc(false)
	if err != nil {
		return Type{}, err
	}
	t := Type{Kind: AggregateFunction, AggFunc: name, AggParams: params}
	for p.isPunct(",") {
		p.next()
		at, err := p.parseType()
		if err != nil {
			return Type{}, err
		}
		t.AggArgs = append(t.AggArgs, at)
	}
	if err := p.expectPunct(")"); err != nil {
		return Type{}, err
	}
	if err := validateAggSignature(t); err != nil {
		return Type{}, err
	}
	return t, nil
}

// validateAggSignature checks argument counts and parameter shapes for the
// well-known aggregate functions, as ClickHouse does when building the
// type. Other functions are accepted here and validated by the engine.
func validateAggSignature(t Type) error {
	name := t.AggFunc
	if strings.Contains(name, "Merge") {
		return nil // arguments are AggregateFunction states
	}
	base := name
	conds := 0
	for strings.HasSuffix(base, "If") && len(base) > 2 {
		base, conds = base[:len(base)-2], conds+1
	}
	base = strings.TrimSuffix(base, "State")
	n := len(t.AggArgs) - conds
	switch base {
	case "sum", "avg", "min", "max", "any", "anyLast", "groupArray", "groupUniqArray", "sumWithOverflow":
		if n != 1 {
			return errf(cherr.NumberOfArgsDoesntMatch, "Aggregate function %s requires single argument", base)
		}
		if base == "sum" || base == "avg" || base == "sumWithOverflow" {
			if u := t.AggArgs[0].Unwrap(); !isNumeric(u.Kind) && u.Kind != Bool {
				return errf(cherr.IllegalTypeOfArgument, "Illegal type %s of argument for aggregate function %s", t.AggArgs[0].String(), base)
			}
		}
	case "argMin", "argMax":
		if n != 2 {
			return errf(cherr.NumberOfArgsDoesntMatch, "Aggregate function %s requires two arguments", base)
		}
	case "uniq", "uniqExact":
		if n < 1 {
			return errf(cherr.NumberOfArgsDoesntMatch, "Aggregate function %s requires at least one argument", base)
		}
	case "quantile", "median":
		if len(t.AggParams) > 1 {
			return errf(cherr.NumberOfArgsDoesntMatch, "Aggregate function %s requires one level parameter or less", base)
		}
	}
	if n < 0 {
		return errf(cherr.NumberOfArgsDoesntMatch, "Aggregate function %s requires a condition argument", name)
	}
	if base == "quantile" || base == "quantiles" {
		for _, prm := range t.AggParams {
			if _, isStr := prm.(string); isStr {
				return errf(cherr.CannotConvertType, "Cannot convert String to double")
			}
		}
	}
	return nil
}

var simpleAggFuncs = map[string]bool{
	"any": true, "any_respect_nulls": true, "anyLast": true, "anyLast_respect_nulls": true,
	"min": true, "max": true, "sum": true, "sumWithOverflow": true,
	"groupBitAnd": true, "groupBitOr": true, "groupBitXor": true,
	"sumMap": true, "minMap": true, "maxMap": true,
	"groupArrayArray": true, "groupArrayLastArray": true, "groupUniqArrayArray": true,
	"sumMappedArrays": true, "minMappedArrays": true, "maxMappedArrays": true,
}

func (p *typeParser) parseSimpleAggregateFunction(hasArgs bool) (Type, error) {
	if !hasArgs {
		return Type{}, errf(cherr.NumberOfArgsDoesntMatch, "Data type SimpleAggregateFunction requires parameters: name of aggregate function and list of data types for arguments")
	}
	p.next()
	name, _, err := p.parseAggFunc(true)
	if err != nil {
		return Type{}, err
	}
	if !simpleAggFuncs[name] {
		return Type{}, errf(cherr.BadArguments, "Unsupported aggregate function %s, supported functions are any,anyLast,min,max,sum,sumWithOverflow,groupBitAnd,groupBitOr,groupBitXor,sumMap,minMap,maxMap,groupArrayArray,groupUniqArrayArray", name)
	}
	if !p.isPunct(",") {
		return Type{}, errf(cherr.NumberOfArgsDoesntMatch, "Aggregate function %s requires single argument", name)
	}
	p.next()
	vt, err := p.parseType()
	if err != nil {
		return Type{}, err
	}
	if !p.isPunct(")") {
		return Type{}, errf(cherr.NumberOfArgsDoesntMatch, "Aggregate function %s requires single argument", name)
	}
	p.next()
	if name == "sum" || name == "sumWithOverflow" {
		if !vt.IsNumeric() {
			return Type{}, errf(cherr.IllegalTypeOfArgument, "Illegal type %s of argument for aggregate function %s", vt.String(), name)
		}
	}
	return Type{Kind: SimpleAggregateFunction, AggFunc: name, Elem: &vt}, nil
}
