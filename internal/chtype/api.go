package chtype

// API CONTRACT. Signatures and documented semantics must not change without
// updating DESIGN.md and all callers.

// Parse parses a ClickHouse type name such as
// "Array(Tuple(qualification_id UInt32, precodes Array(String)))",
// "LowCardinality(String)", "DateTime64(9, 'UTC')",
// "AggregateFunction(uniqExact, String)", "SimpleAggregateFunction(sum, UInt64)",
// "Nullable(String)", "Enum8('a' = 1, 'b' = 2)", "Decimal" is not supported.
// Type names are case-sensitive except the aliases ClickHouse treats
// case-insensitively (e.g. "BOOLEAN", "TEXT", "VARCHAR(255)", "INT", "BIGINT",
// "DOUBLE", "FLOAT", "TIMESTAMP"); aliases resolve to canonical types.
// A name longer than 262144 bytes fails with SYNTAX_ERROR (62), as in
// ClickHouse; use ParseStored for names the server wrote itself.
func Parse(s string) (Type, error) { return parseTypeCapped(s) }

// ParseStored is Parse without the length cap, for type names the server
// wrote itself (catalog entries, CTAS columns): a big tuple built by a query
// can render longer than any type name a client may send.
func ParseStored(s string) (Type, error) { return parseType(s) }

// MustParse is Parse that panics; for tests and static tables.
func MustParse(s string) Type {
	t, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return t
}

// String renders the canonical ClickHouse name, exactly as ClickHouse prints
// it in DESCRIBE / Native headers, e.g. "DateTime64(9)", "Array(String)",
// "Tuple(a UInt32, b String)" (named) / "Tuple(UInt32, String)" (unnamed),
// "AggregateFunction(uniqExact, String)", "DateTime64(9, 'UTC')".
func (t Type) String() string { return typeString(t) }

// Unwrap strips Nullable and LowCardinality (any nesting) and returns the
// SimpleAggregateFunction value type for SimpleAggregateFunction.
func (t Type) Unwrap() Type { return unwrapType(t) }

// IsNullable reports whether t (possibly under LowCardinality) is Nullable.
func (t Type) IsNullable() bool { return isNullable(t) }

// Equal reports structural type equality.
func (t Type) Equal(o Type) bool { return typeEqual(t, o) }

// IsNumeric / IsInteger / IsSigned / IsString / IsTime classify Unwrap()'d kinds.
func (t Type) IsNumeric() bool { return isNumeric(t.Unwrap().Kind) }
func (t Type) IsInteger() bool { return isInteger(t.Unwrap().Kind) }
func (t Type) IsSigned() bool  { return isSigned(t.Unwrap().Kind) }
func (t Type) IsString() bool  { k := t.Unwrap().Kind; return k == String || k == FixedString }
func (t Type) IsTime() bool {
	k := t.Unwrap().Kind
	return k == Date || k == Date32 || k == DateTime || k == DateTime64
}

// Zero returns ClickHouse's default value for t (used for omitted INSERT
// columns without DEFAULT): "" , 0, false, epoch, [] , tuple of zeros,
// nil for Nullable, empty *AggPartial for AggregateFunction.
func Zero(t Type) any { return zeroValue(t) }

// Normalize coerces a Go value of any reasonable type (int, int32, float32,
// json.Number, string, []string, []any, time.Time, bool, nil, map[string]any
// for named tuples, ...) into the canonical representation for t, applying
// ClickHouse's implicit conversion rules for INSERT (e.g. numbers into String
// are rejected, strings into DateTime are parsed, out-of-range integers error).
func Normalize(t Type, v any) (any, error) {
	v, err := normalize(t, v)
	return v, plainErr(err)
}

// Convert implements CAST(v AS to) where v currently has type from.
// Supports numeric<->numeric (with ClickHouse overflow wrap semantics for
// toUIntN/toIntN), numeric<->string, string->date/time (best-effort ISO and
// "YYYY-MM-DD hh:mm:ss[.fffffffff]"), time->string, Nullable wrapping, arrays
// element-wise, tuples field-wise.
func Convert(v any, from, to Type) (any, error) {
	v, err := convert(v, from, to)
	return v, plainErr(err)
}

// ParseText parses s in ClickHouse's *escaped* text form for type t — the
// form used by query parameters (param_<name>=...) and TabSeparated fields:
//   - String: backslash escapes processed (\t \n \\ \' \0 \b \f \r \xHH).
//     NOTE: for query parameters ClickHouse applies the escaped rules too.
//   - Nullable: \N means NULL.
//   - DateTime/DateTime64: "YYYY-MM-DD hh:mm:ss[.frac]" (UTC unless TZ),
//     also unix timestamps for DateTime; Date: "YYYY-MM-DD".
//   - Array / Tuple / Map: the *quoted* literal form, e.g. ['a','b'],
//     [(12,['1','3'])], ('x',1), {'k':1}.
//   - Bool: true/false/1/0.
func ParseText(t Type, s string) (any, error) {
	v, err := parseText(t, s)
	return v, plainErr(err)
}

// ParseQuoted parses s in ClickHouse's *quoted* (Values / SQL literal) form:
// strings in single quotes, NULL, numbers, arrays [..], tuples (..).
// It returns the number of bytes consumed so callers can parse a stream.
func ParseQuoted(t Type, s string) (v any, n int, err error) {
	v, n, err = parseQuoted(t, s)
	return v, n, plainErr(err)
}

// FormatText renders v in TabSeparated escaped form (String: \t \n \\ escaped;
// NULL: \N; arrays/tuples in quoted form; DateTime64 with exactly Precision
// fractional digits; floats like ClickHouse: inf, -inf, nan, shortest repr).
func FormatText(t Type, v any) string { return formatText(t, v) }

// FormatQuoted renders v as a SQL literal in quoted form ('a\'b', [1,2], NULL).
func FormatQuoted(t Type, v any) string { return formatQuoted(t, v) }

// FormatCSV renders v as a CSV field following ClickHouse CSV rules.
func FormatCSV(t Type, v any) string { return formatCSV(t, v) }

// AppendJSON appends v rendered as JSON following ClickHouse's JSON output
// defaults: (U)Int64 and wider quoted as strings when quote64 is true
// (output_format_json_quote_64bit_integers=1 is ClickHouse's default),
// Date/DateTime as strings, NULL as null, arrays as arrays, tuples as arrays
// (named tuples as objects when namedTuplesAsObjects), floats: nan/inf as null.
func AppendJSON(dst []byte, t Type, v any, quote64, namedTuplesAsObjects bool) []byte {
	return appendJSON(dst, t, v, quote64, namedTuplesAsObjects)
}

// Compare orders two canonical values: a top-level nil sorts first; numbers
// compare by numeric value across uint64/int64/float64 (NaN after every
// number, NaN = NaN); strings bytewise; time.Time by instant; bool
// false<true; []any (arrays, tuples, map entries) lexicographically, a proper
// prefix first, with a nested NULL greater than every value (and than NaN)
// and equal to NULL — the order ClickHouse's comparison operators use for
// arrays and maps ([1] < [1,2] < [1,NULL] < [NULL]). *AggPartial is not
// comparable (returns 0). Mixed incomparable kinds order by a fixed kind
// rank. Compare(a, b) == CompareHint(a, b, 1) whenever neither a nor b is nil.
// Compare is untyped: UUID text compares bytewise and enum names
// alphabetically, which is NOT ClickHouse's order for those types — order
// typed values with CompareTyped (or OrderKey) instead. Equality is
// unaffected (equal UUIDs/enums are equal under both).
func Compare(a, b any) int { return compareTop(a, b) }

// CompareHint orders two canonical values like ClickHouse's
// IColumn::compareAt(a, b, nan_direction_hint): at every depth, including the
// top level, NULL and NaN compare greater than every other value when hint > 0
// (numbers < NaN < NULL) and less when hint <= 0 (NULL < NaN < numbers);
// NULL equals NULL and NaN equals NaN. ORDER BY uses hint = direction for
// NULLS LAST and -direction for NULLS FIRST (then multiplies by direction);
// max/argMax/greatest use hint -1 and min/argMin/least hint +1, so that a
// NULL or NaN element never wins an extremum.
func CompareHint(a, b any, hint int) int { return compareValues(a, b, hint) }

// CompareTyped orders two canonical values of type t like ClickHouse orders
// t. It is Compare (a top-level nil first) except for types whose canonical
// representation does not carry their ClickHouse order, at any depth (inside
// Array, Tuple, Map, Nullable, LowCardinality, SimpleAggregateFunction):
//   - UUID compares as ClickHouse's UInt128: the last 8 bytes (hex digits
//     19..35 of the dashed text) first, then the first 8 bytes, so
//     00000000-0000-0001-0000-000000000000 < 00000000-0000-0000-0000-000000000001
//     although the text sorts the other way;
//   - Enum8/Enum16 compare by numeric value, not by element name.
//
// Every ordering use of a typed value — comparison operators, ORDER BY,
// min/max/argMin/argMax and their CAS keys, greatest/least, key ranges —
// must go through CompareTyped / CompareTypedHint / OrderKey; the untyped
// Compare orders UUIDs as text and enums by name.
func CompareTyped(a, b any, t Type) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	}
	return compareTyped(t, a, b, 1)
}

// CompareTypedHint is CompareHint (ClickHouse's nan_direction_hint for NULL
// and NaN at every depth, including the top level) with CompareTyped's
// per-type order.
func CompareTypedHint(a, b any, t Type, hint int) int { return compareTyped(t, a, b, hint) }

// OrderKey returns a function mapping a canonical value of type t to a value
// that Compare/CompareHint order exactly as CompareTyped/CompareTypedHint
// order the original (UUIDs to UUIDOrderKey text, enum names to their
// int64 values, element-wise inside Array/Tuple/Map; nil stays nil), or nil
// when values of t already compare correctly with Compare. Use it to
// precompute sort keys; never store or output the mapped value.
func OrderKey(t Type) func(any) any { return orderKey(t) }

// UUIDOrderKey maps a UUID (any form parseUUID accepts) to a fixed-width
// 32-character lower-case hex string whose bytewise order is ClickHouse's
// UUID order: the hex digits of the last 8 bytes followed by those of the
// first 8 bytes. Suitable for order-preserving key encodings. Text that is
// not a UUID is returned unchanged.
func UUIDOrderKey(s string) string { return uuidOrderKey(s) }

// Equal reports Compare(a,b)==0 with ClickHouse equality semantics
// (NULL = anything is not true; callers handle NULL before calling).
func Equal(a, b any) bool { return compareTop(a, b) == 0 }

// Supertype returns the least common supertype ClickHouse would pick for
// if()/multiIf()/array literals/UNION, following ClickHouse's
// getLeastSupertype rules (UInt8+Int16 -> Int16, UInt32+Int32 -> Int64,
// UInt64+signed -> error, ints+floats -> Float64, String+number -> error),
// wrapping Nullable if any input is Nullable. Nothing is the identity element.
func Supertype(ts ...Type) (Type, error) { return supertype(ts...) }

// InferLiteral returns the type ClickHouse assigns to a literal value:
// non-negative integers -> smallest of UInt8/16/32/64 that fits, negative ->
// smallest IntN, float64 -> Float64, string -> String, bool -> Bool,
// nil -> Nullable(Nothing), []any -> Array(Supertype(elems)), tuple -> Tuple.
// isTuple distinguishes tuple literals from arrays when v is []any.
func InferLiteral(v any, isTuple bool) Type { return inferLiteral(v, isTuple) }
