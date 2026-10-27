package exec

import (
	"errors"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/exec/aggstate"
)

// builder compiles a call of a scalar function over compiled arguments.
type builder func(c *compiler, name string, args []*expr) (*expr, error)

var (
	funcs   = map[string]builder{}
	ciFuncs = map[string]string{} // lower-case alias -> canonical name
)

// reg registers a case-sensitive function.
func reg(b builder, names ...string) {
	for _, n := range names {
		funcs[n] = b
	}
}

// regCI registers a function whose name ClickHouse matches case-insensitively.
func regCI(b builder, names ...string) {
	for _, n := range names {
		funcs[n] = b
		ciFuncs[strings.ToLower(n)] = n
	}
}

// lookupFunc returns the builder and canonical name of a scalar function.
func lookupFunc(name string) (builder, string) {
	if b, ok := funcs[name]; ok {
		return b, name
	}
	if canon, ok := ciFuncs[strings.ToLower(name)]; ok {
		return funcs[canon], canon
	}
	return nil, name
}

// canonicalFuncName normalizes case-insensitive function names (for
// fingerprints).
func canonicalFuncName(name string) string {
	if _, ok := funcs[name]; ok {
		return name
	}
	if canon, ok := ciFuncs[strings.ToLower(name)]; ok {
		return canon
	}
	if spec, ok := parseAggName(name); ok {
		return spec.canonical()
	}
	return name
}

func itoa(i int) string { return strconv.Itoa(i) }

func wantArgs(name string, args []*expr, lo, hi int) error {
	if len(args) < lo || (hi >= 0 && len(args) > hi) {
		want := strconv.Itoa(lo)
		switch {
		case hi < 0:
			want = "at least " + want
		case hi != lo:
			want += " to " + strconv.Itoa(hi)
		}
		return argCountError(name, len(args), want)
	}
	return nil
}

func illegalType(name string, t chtype.Type, pos int) error {
	return cherr.New(cherr.IllegalTypeOfArgument, "Illegal type %s of argument %d of function %s", t.String(), pos, name)
}

// strict builds a function with ClickHouse's default NULL and LowCardinality
// handling: a NULL argument yields NULL, the result type is Nullable if any
// argument is, and LowCardinality if any argument is.
func strict(args []*expr, rt chtype.Type, f func(v []any) (any, error)) *expr {
	nullable, lc := false, false
	for _, a := range args {
		if isNothing(a.t) {
			return constant(nullNothing, nil)
		}
		if a.t.IsNullable() {
			nullable = true
		}
		if a.t.Kind == chtype.LowCardinality {
			lc = true
		}
	}
	t := rt
	if lc && t.Kind != chtype.LowCardinality && canBeInLowCardinality(t) {
		t = chtype.NewLowCardinality(t)
	}
	t = nullableIf(t, nullable)
	x := &expr{t: t, fn: func(row []any) (any, error) {
		vals := make([]any, len(args))
		for i, a := range args {
			v, err := a.eval(row)
			if err != nil {
				return nil, err
			}
			if v == nil {
				return nil, nil
			}
			vals[i] = v
		}
		return f(vals)
	}}
	return fold(x, args)
}

// base returns the argument type stripped of Nullable/LowCardinality.
func base(x *expr) chtype.Type { return x.t.Unwrap() }

func init() {
	// Comparison.
	for _, n := range []string{"equals", "notEquals", "less", "lessOrEquals", "greater", "greaterOrEquals"} {
		reg(compareBuilder(n), n)
	}
	// Logic.
	reg(logicBuilder(true), "and")
	reg(logicBuilder(false), "or")
	reg(buildNot, "not")
	reg(buildXor, "xor")
	// Arithmetic.
	for _, n := range []string{"plus", "minus", "multiply", "divide", "intDiv", "modulo", "intDivOrZero", "moduloOrZero"} {
		reg(arithBuilder(n), n)
	}
	reg(buildNegate, "negate")
	regCI(buildAbs, "abs")
	regCI(roundBuilder("round"), "round")
	regCI(roundBuilder("floor"), "floor")
	regCI(roundBuilder("ceil"), "ceil", "ceiling")
	regCI(roundBuilder("trunc"), "trunc", "truncate")
	regCI(extremeBuilder(true), "greatest")
	regCI(extremeBuilder(false), "least")
	// Conditional and NULL handling.
	regCI(buildIf, "if")
	reg(buildMultiIf, "multiIf")
	regCI(buildCoalesce, "coalesce", "ifNull")
	regCI(buildNullIf, "nullIf")
	regCI(nullCheckBuilder(true), "isNull")
	regCI(nullCheckBuilder(false), "isNotNull")
	reg(buildAssumeNotNull, "assumeNotNull")
	reg(buildToNullable, "toNullable")
	reg(buildToTypeName, "toTypeName")
	reg(buildIsFinite, "isFinite")
	reg(buildIsNaN, "isNaN")
	// Conversion.
	for _, n := range []string{"UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64", "Float32", "Float64"} {
		t := numericTypes[n]
		reg(convBuilder(t, convPlain), "to"+n)
		reg(convBuilder(t, convOrZero), "to"+n+"OrZero")
		reg(convBuilder(t, convOrNull), "to"+n+"OrNull")
	}
	reg(convBuilder(chtype.TBool, convPlain), "toBool")
	reg(convBuilder(chtype.TUUID, convPlain), "toUUID")
	reg(convBuilder(chtype.TUUID, convOrNull), "toUUIDOrNull")
	reg(buildToString, "toString")
	reg(buildToLowCardinality, "toLowCardinality")
	regCI(buildCastFunc, "CAST", "_CAST")
	// Tuples and arrays.
	reg(buildTuple, "tuple")
	reg(buildArray, "array")
	reg(buildArrayElement, "arrayElement")
	reg(buildTupleElement, "tupleElement")
	reg(buildHas, "has")
	reg(buildIndexOf, "indexOf")
	// Aggregate state finalization.
	reg(buildFinalizeAggregation, "finalizeAggregation")
	// Server/introspection.
	reg(constStringBuilder(func(c *compiler) string { return c.q.e.cfg.ServerVersion }), "version")
	reg(func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 0, 0); err != nil {
			return nil, err
		}
		return constant(chtype.TUInt32, c.q.e.cfg.ServerRevision), nil
	}, "revision")
	regCI(constStringBuilder(func(c *compiler) string { return c.q.timezoneName() }), "timezone", "timeZone")
	regCI(constStringBuilder(func(c *compiler) string { return c.q.e.cfg.Timezone }), "serverTimezone", "serverTimeZone")
	reg(constStringBuilder(func(c *compiler) string { return c.q.e.cfg.DisplayName }), "displayName")
	reg(constStringBuilder(func(c *compiler) string { h, _ := os.Hostname(); return h }), "hostName", "hostname")
	regCI(constStringBuilder(func(c *compiler) string { return c.q.db }), "currentDatabase", "database")
	regCI(constStringBuilder(func(c *compiler) string { return c.q.user }), "currentUser", "user")
	reg(constStringBuilder(func(c *compiler) string { return c.q.id }), "queryID", "query_id")
	reg(func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 0, 0); err != nil {
			return nil, err
		}
		return constant(chtype.TUInt32, uint64(time.Since(c.q.e.impl.started)/time.Second)), nil
	}, "uptime")
	reg(func(c *compiler, name string, args []*expr) (*expr, error) {
		return &expr{t: chtype.TUUID, volatile: true, fn: func([]any) (any, error) { return uuid.NewString(), nil }}, nil
	}, "generateUUIDv4")
	reg(func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 1, 1); err != nil {
			return nil, err
		}
		return args[0], nil
	}, "identity", "materialize")
}

var numericTypes = map[string]chtype.Type{
	"UInt8": chtype.TUInt8, "UInt16": chtype.TUInt16, "UInt32": chtype.TUInt32, "UInt64": chtype.TUInt64,
	"Int8": chtype.TInt8, "Int16": chtype.TInt16, "Int32": chtype.TInt32, "Int64": chtype.TInt64,
	"Float32": chtype.TFloat32, "Float64": chtype.TFloat64,
}

func constStringBuilder(f func(c *compiler) string) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 0, 0); err != nil {
			return nil, err
		}
		return constant(chtype.TString, f(c)), nil
	}
}

// ---------------------------------------------------------------------------
// Comparison

func compareBuilder(name string) builder {
	return func(c *compiler, _ string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 2, 2); err != nil {
			return nil, err
		}
		l, r, err := coerceCompare(c.q, name, args[0], args[1])
		if err != nil {
			return nil, err
		}
		if base(l).Kind == chtype.Tuple && base(r).Kind == chtype.Tuple {
			return tupleCompare(name, l, r), nil
		}
		test := compareTest(name)
		// UUIDs (also inside arrays and maps) compare as ClickHouse's
		// UInt128, not as text; a String operand compared with a UUID was
		// cast to UUID by coerceCompare. Other types keep the untyped
		// order: coerceCompare already mapped enums compared with numbers
		// or constants to their values, and an enum compared with a
		// non-constant String compares by name, like ClickHouse.
		cmp := chtype.Compare
		if ct := base(l); hasUUID(ct) {
			cmp = func(a, b any) int { return chtype.CompareTyped(a, b, ct) }
		} else if ct := base(r); hasUUID(ct) {
			cmp = func(a, b any) int { return chtype.CompareTyped(a, b, ct) }
		}
		return strict([]*expr{l, r}, chtype.TUInt8, func(v []any) (any, error) {
			if isNaN(v[0]) || isNaN(v[1]) {
				return boolNum(name == "notEquals"), nil
			}
			return boolNum(test(cmp(v[0], v[1]))), nil
		}), nil
	}
}

// tupleCompare compares two tuples like ClickHouse, which decomposes the
// comparison into per-element operators combined with three-valued and/or:
// less = less(a0,b0) OR (equals(a0,b0) AND (less(a1,b1) OR ...)) (the last
// element uses lessOrEquals for <=), equals = AND of equals, notEquals = OR
// of notEquals. So a NULL element yields NULL only when it decides the
// result ((1,NULL) < (2,1) = 1, (1,NULL) < (1,2) = NULL), and a NaN element
// is neither less, equal nor greater ((1,nan) < (1,2) = 0, (1,nan) =
// (1,nan) = 0). Non-tuple elements (arrays, ...) keep chtype.Compare:
// ([nan],1) = ([nan],1) = 1. A bare NULL literal element (type Nothing)
// makes the whole result NULL of type Nullable(Nothing), like any function
// of a Nothing argument; otherwise the result is Nullable(UInt8) when an
// element type is Nullable.
func tupleCompare(name string, l, r *expr) *expr {
	lt, rt := base(l), base(r)
	if tupleHasNothing(lt) || tupleHasNothing(rt) {
		return constant(nullNothing, nil)
	}
	resT := chtype.TUInt8
	if tupleMayHoldNull(lt) || tupleMayHoldNull(rt) {
		resT = nullableIf(resT, true)
	}
	// Both sides have the same structure (coerceCompare checked for a
	// supertype); l's type tells nested tuples from arrays.
	return strict([]*expr{l, r}, resT, func(v []any) (any, error) {
		a, _ := v[0].([]any)
		b, _ := v[1].([]any)
		var res tri
		switch name {
		case "equals":
			res = tupleEq(lt, a, b)
		case "notEquals":
			res = tupleEq(lt, a, b).not()
		case "less":
			res = tupleLess(lt, a, b, false)
		case "lessOrEquals":
			res = tupleLess(lt, a, b, true)
		case "greater":
			res = tupleLess(lt, b, a, false)
		default:
			res = tupleLess(lt, b, a, true)
		}
		if res == triNull {
			return nil, nil
		}
		return boolNum(res == triTrue), nil
	})
}

// tri is a three-valued (Kleene) boolean.
type tri uint8

const (
	triFalse tri = iota
	triTrue
	triNull
)

func triOf(b bool) tri {
	if b {
		return triTrue
	}
	return triFalse
}

func (x tri) not() tri {
	switch x {
	case triTrue:
		return triFalse
	case triFalse:
		return triTrue
	}
	return triNull
}

func (x tri) and(y tri) tri {
	switch {
	case x == triFalse || y == triFalse:
		return triFalse
	case x == triNull || y == triNull:
		return triNull
	}
	return triTrue
}

func (x tri) or(y tri) tri {
	switch {
	case x == triTrue || y == triTrue:
		return triTrue
	case x == triNull || y == triNull:
		return triNull
	}
	return triFalse
}

// tupleHasNothing reports whether a tuple type has an element of type
// Nothing (a bare NULL literal), nested tuples included.
func tupleHasNothing(t chtype.Type) bool {
	for _, f := range t.Fields {
		u := f.Type.Unwrap()
		if u.Kind == chtype.Nothing || u.Kind == chtype.Tuple && tupleHasNothing(u) {
			return true
		}
	}
	return false
}

// tupleMayHoldNull reports whether a tuple type has a Nullable element,
// nested tuples included.
func tupleMayHoldNull(t chtype.Type) bool {
	for _, f := range t.Fields {
		u := f.Type.Unwrap()
		if f.Type.IsNullable() || u.Kind == chtype.Tuple && tupleMayHoldNull(u) {
			return true
		}
	}
	return false
}

// nestedTuple returns the i-th element type of tuple type t when it is a
// tuple itself.
func nestedTuple(t chtype.Type, i int) (chtype.Type, bool) {
	if i >= len(t.Fields) {
		return chtype.Type{}, false
	}
	u := t.Fields[i].Type.Unwrap()
	return u, u.Kind == chtype.Tuple
}

// hasUUID reports whether t holds a UUID at any depth (whose canonical text
// does not carry ClickHouse's order, see chtype.CompareTyped).
func hasUUID(t chtype.Type) bool {
	switch t.Kind {
	case chtype.UUID:
		return true
	case chtype.Nullable, chtype.LowCardinality, chtype.Array, chtype.SimpleAggregateFunction:
		return t.Elem != nil && hasUUID(*t.Elem)
	case chtype.Map:
		return t.Key != nil && hasUUID(*t.Key) || t.Elem != nil && hasUUID(*t.Elem)
	case chtype.Tuple:
		for _, f := range t.Fields {
			if hasUUID(f.Type) {
				return true
			}
		}
	}
	return false
}

// tupleElemCmp compares the i-th elements of two tuples of type t with the
// scalar operators: less(a,b) (lessOrEquals when orEqual) and equals(a,b).
// NULL gives NULL, NaN false; nested tuples recurse.
func tupleElemCmp(t chtype.Type, i int, a, b any, orEqual bool) (less, eq tri) {
	if a == nil || b == nil {
		return triNull, triNull
	}
	if nt, ok := nestedTuple(t, i); ok {
		x, xok := a.([]any)
		y, yok := b.([]any)
		if xok && yok {
			return tupleLess(nt, x, y, orEqual), tupleEq(nt, x, y)
		}
	}
	if isNaN(a) || isNaN(b) {
		return triFalse, triFalse
	}
	var c int
	if i < len(t.Fields) && hasUUID(t.Fields[i].Type) {
		c = chtype.CompareTyped(a, b, t.Fields[i].Type)
	} else {
		c = chtype.Compare(a, b)
	}
	return triOf(c < 0 || orEqual && c == 0), triOf(c == 0)
}

func tupleEq(t chtype.Type, a, b []any) tri {
	res := triOf(len(a) == len(b))
	for i := 0; i < len(a) && i < len(b); i++ {
		_, eq := tupleElemCmp(t, i, a[i], b[i], false)
		res = res.and(eq)
	}
	return res
}

// tupleLess folds less(a0,b0) OR (equals(a0,b0) AND (... less(an,bn))) from
// the last element, which uses lessOrEquals when orEqual.
func tupleLess(t chtype.Type, a, b []any, orEqual bool) tri {
	n := min(len(a), len(b))
	if n == 0 {
		return triOf(orEqual)
	}
	res, _ := tupleElemCmp(t, n-1, a[n-1], b[n-1], orEqual)
	for i := n - 2; i >= 0; i-- {
		lt, eq := tupleElemCmp(t, i, a[i], b[i], false)
		res = lt.or(eq.and(res))
	}
	return res
}

func compareTest(name string) func(int) bool {
	switch name {
	case "equals":
		return func(c int) bool { return c == 0 }
	case "notEquals":
		return func(c int) bool { return c != 0 }
	case "less":
		return func(c int) bool { return c < 0 }
	case "lessOrEquals":
		return func(c int) bool { return c <= 0 }
	case "greater":
		return func(c int) bool { return c > 0 }
	}
	return func(c int) bool { return c >= 0 }
}

// typeClass groups types that compare directly with each other.
func typeClass(t chtype.Type) string {
	k := t.Kind
	switch {
	case isNumKind(k):
		return "num"
	case isStringKind(k):
		return "str"
	case isTimeKind(k):
		return "time"
	case k == chtype.Enum8 || k == chtype.Enum16:
		return "enum"
	case k == chtype.UUID:
		return "uuid"
	case k == chtype.Array:
		return "array"
	case k == chtype.Tuple:
		return "tuple"
	case k == chtype.Map:
		return "map"
	case k == chtype.Nothing:
		return "nothing"
	}
	return t.String()
}

// coerceCompare makes two expressions comparable, converting a String side
// to the other side's type like ClickHouse does.
func coerceCompare(q *query, name string, l, r *expr) (*expr, *expr, error) {
	lt, rt := base(l), base(r)
	lc, rc := typeClass(lt), typeClass(rt)
	if lc == "time" && rc == "time" {
		// A Date compares with a DateTime as midnight in the DateTime's
		// time zone (its explicit one, else the server's), not in UTC.
		switch {
		case isDateKind(lt.Kind) && !isDateKind(rt.Kind):
			return dateAsTime(q, l, lt, rt), r, nil
		case isDateKind(rt.Kind) && !isDateKind(lt.Kind):
			return l, dateAsTime(q, r, rt, lt), nil
		}
	}
	if lc == "str" && rc == "str" && (lt.Kind == chtype.FixedString || rt.Kind == chtype.FixedString) &&
		!(lt.Kind == chtype.FixedString && rt.Kind == chtype.FixedString && lt.N == rt.N) {
		// FixedString compares with String, or with a FixedString of
		// another length, as if both were zero padded to the same length:
		// trailing zero bytes of either side are ignored.
		return trimNul(l), trimNul(r), nil
	}
	if lc == "enum" || rc == "enum" {
		// ClickHouse compares enums by their numeric value.
		switch {
		case lc == "enum" && (rc == "enum" || rc == "num"):
			return enumOrdinal(l), enumOrdinal(r), nil
		case rc == "enum" && lc == "num":
			return l, enumOrdinal(r), nil
		case lc == "enum" && rc == "str":
			return enumVsString(name, l, r)
		case rc == "enum" && lc == "str":
			x, y, err := enumVsString(name, r, l)
			return y, x, err
		}
	}
	if lc == rc || lc == "nothing" || rc == "nothing" {
		if lc == "array" || lc == "tuple" || lc == "map" {
			if _, err := chtype.Supertype(lt, rt); err != nil {
				return nil, nil, cherr.New(cherr.IllegalTypeOfArgument, "Illegal types of arguments (%s, %s) of function %s", l.t.String(), r.t.String(), name)
			}
		}
		return l, r, nil
	}
	switch {
	case lc == "str" && (rc == "num" || rc == "time" || rc == "uuid"):
		x, err := castCompare(l, q.zoned(rt))
		return x, r, err
	case rc == "str" && (lc == "num" || lc == "time" || lc == "uuid"):
		x, err := castCompare(r, q.zoned(lt))
		return l, x, err
	}
	return nil, nil, cherr.New(cherr.IllegalTypeOfArgument, "Illegal types of arguments (%s, %s) of function %s", l.t.String(), r.t.String(), name)
}

func isDateKind(k chtype.Kind) bool { return k == chtype.Date || k == chtype.Date32 }

// dateAsTime converts a Date/Date32 expression (values are UTC midnights)
// to the instant of that midnight in the time zone ClickHouse uses when
// comparing it with a value of type other: other's explicit time zone, else
// the server's; a Date32 compared with a DateTime always uses the server's.
func dateAsTime(q *query, x *expr, dt, other chtype.Type) *expr {
	var loc *time.Location
	switch {
	case q == nil:
	case dt.Kind == chtype.Date32 && other.Kind == chtype.DateTime:
		loc = q.loc
	default:
		loc = q.location(other)
	}
	if loc == nil {
		loc = time.UTC
	}
	return fold(&expr{t: nullableIf(other, x.t.IsNullable()), fn: func(row []any) (any, error) {
		v, err := x.eval(row)
		if err != nil {
			return v, err
		}
		t, ok := v.(time.Time)
		if !ok {
			return v, nil
		}
		y, m, d := t.UTC().Date()
		return time.Date(y, m, d, 0, 0, 0, 0, loc), nil
	}}, []*expr{x})
}

// zoned resolves the implicit (server) time zone of DateTime/DateTime64
// types without an explicit one, so that converting a string or a Date to
// such a type uses the server time zone rather than UTC.
func (q *query) zoned(t chtype.Type) chtype.Type {
	if q == nil || q.loc == nil || q.loc == time.UTC || q.loc.String() == "UTC" {
		return t
	}
	u := t.Unwrap()
	switch u.Kind {
	case chtype.DateTime, chtype.DateTime64:
		if u.TZ == "" {
			u.TZ = q.loc.String()
		}
		return u
	case chtype.Tuple:
		fs := make([]chtype.Field, len(u.Fields))
		for i, f := range u.Fields {
			fs[i] = chtype.Field{Name: f.Name, Type: q.zoned(f.Type)}
		}
		u.Fields = fs
		return u
	}
	return t
}

// enumOrdinal maps an enum expression to its numeric value (other
// expressions are returned unchanged).
func enumOrdinal(x *expr) *expr {
	u := base(x)
	if u.Kind != chtype.Enum8 && u.Kind != chtype.Enum16 {
		return x
	}
	vals := make(map[string]int64, len(u.EnumValues))
	for _, ev := range u.EnumValues {
		vals[ev.Name] = ev.Value
	}
	return fold(&expr{t: nullableIf(chtype.TInt64, x.t.IsNullable()), fn: func(row []any) (any, error) {
		v, err := x.eval(row)
		if err != nil || v == nil {
			return v, err
		}
		s, _ := v.(string)
		n, ok := vals[s]
		if !ok {
			return nil, cherr.New(cherr.UnknownElementOfEnum, "Unknown element '%s' for enum", s)
		}
		return n, nil
	}}, []*expr{x})
}

// enumVsString makes an enum expression e comparable with a string
// expression s like ClickHouse: a constant string is converted to the
// enum's value (an unknown element makes every comparison false, except
// notEquals, which fails with UNKNOWN_ELEMENT_OF_ENUM); a non-constant
// string compares with the element name as a string.
func enumVsString(name string, e, s *expr) (*expr, *expr, error) {
	if !s.konst {
		return e, s, nil
	}
	str, ok := s.val.(string)
	if !ok {
		return e, s, nil // NULL
	}
	u := base(e)
	for _, ev := range u.EnumValues {
		if ev.Name == str {
			return enumOrdinal(e), constant(chtype.TInt64, ev.Value), nil
		}
	}
	if n, err := strconv.ParseInt(str, 10, 64); err == nil {
		for _, ev := range u.EnumValues {
			if ev.Value == n {
				return enumOrdinal(e), constant(chtype.TInt64, n), nil
			}
		}
	}
	if name == "notEquals" {
		return nil, nil, cherr.New(cherr.UnknownElementOfEnum, "Unknown element '%s' for enum: while converting '%s' to %s", str, str, u.String())
	}
	// NaN compares false with everything (see compareBuilder).
	return enumOrdinal(e), constant(chtype.TFloat64, math.NaN()), nil
}

// trimNul strips the trailing zero bytes of string values.
func trimNul(x *expr) *expr {
	if !isStringKind(base(x).Kind) {
		return x
	}
	return fold(&expr{t: nullableIf(chtype.TString, x.t.IsNullable()), fn: func(row []any) (any, error) {
		v, err := x.eval(row)
		if s, ok := v.(string); ok {
			return strings.TrimRight(s, "\x00"), err
		}
		return v, err
	}}, []*expr{x})
}

func castCompare(x *expr, to chtype.Type) (*expr, error) {
	t := nullableIf(to, x.t.IsNullable())
	out := &expr{t: t, fn: func(row []any) (any, error) {
		v, err := x.eval(row)
		if err != nil || v == nil {
			return nil, err
		}
		s, _ := v.(string)
		var res any
		if isNumKind(to.Kind) {
			res, err = numericFromString(s, to)
		} else {
			res, err = chtype.Convert(s, chtype.TString, to)
		}
		if err != nil {
			return nil, cherr.New(cherr.TypeMismatch, "Cannot convert string '%s' to type %s", s, to.String())
		}
		return res, nil
	}}
	folded := fold(out, []*expr{x})
	if x.konst && !folded.konst {
		// Constant conversion failed: report it now, like ClickHouse.
		if _, err := out.fn(nil); err != nil {
			return nil, err
		}
	}
	return folded, nil
}

// ---------------------------------------------------------------------------
// Logic

// truth interprets a canonical value as a condition.
func truth(v any) (bool, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case uint64:
		return x != 0, nil
	case int64:
		return x != 0, nil
	case float64:
		return x != 0 && !math.IsNaN(x), nil
	}
	return false, cherr.New(cherr.IllegalTypeOfArgument, "Illegal value of type %T used as a condition", v)
}

func checkCondType(name string, a *expr, pos int) error {
	k := base(a).Kind
	if isNumKind(k) || k == chtype.Nothing {
		return nil
	}
	return illegalType(name, a.t, pos)
}

// logicResult is the result type of logical functions: Bool when any
// argument is Bool, UInt8 otherwise (ClickHouse), with the matching value.
func logicResult(args []*expr) (chtype.Type, func(bool) any) {
	for _, a := range args {
		if base(a).Kind == chtype.Bool {
			return chtype.TBool, func(b bool) any { return b }
		}
	}
	return chtype.TUInt8, func(b bool) any { return boolNum(b) }
}

func logicBuilder(isAnd bool) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 2, -1); err != nil {
			return nil, err
		}
		nullable := false
		for i, a := range args {
			if err := checkCondType(name, a, i+1); err != nil {
				return nil, err
			}
			nullable = nullable || a.t.IsNullable()
		}
		rt, val := logicResult(args)
		x := &expr{t: nullableIf(rt, nullable), fn: func(row []any) (any, error) {
			sawNull := false
			for _, a := range args {
				v, err := a.eval(row)
				if err != nil {
					return nil, err
				}
				if v == nil {
					sawNull = true
					continue
				}
				b, err := truth(v)
				if err != nil {
					return nil, err
				}
				if b != isAnd {
					return val(b), nil
				}
			}
			if sawNull {
				return nil, nil
			}
			return val(isAnd), nil
		}}
		return fold(x, args), nil
	}
}

func buildNot(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 1); err != nil {
		return nil, err
	}
	if err := checkCondType(name, args[0], 1); err != nil {
		return nil, err
	}
	rt, val := logicResult(args)
	return strict(args, rt, func(v []any) (any, error) {
		b, err := truth(v[0])
		return val(!b), err
	}), nil
}

func buildXor(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 2, -1); err != nil {
		return nil, err
	}
	rt, val := logicResult(args)
	return strict(args, rt, func(v []any) (any, error) {
		res := false
		for _, x := range v {
			b, err := truth(x)
			if err != nil {
				return nil, err
			}
			res = res != b
		}
		return val(res), nil
	}), nil
}

// ---------------------------------------------------------------------------
// Arithmetic

func arithBuilder(op string) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 2, 2); err != nil {
			return nil, err
		}
		a, b := base(args[0]), base(args[1])
		if (op == "plus" || op == "minus") && (isTimeKind(a.Kind) || isTimeKind(b.Kind)) {
			return timeArith(c, op, args)
		}
		if isNothing(args[0].t) || isNothing(args[1].t) {
			return constant(nullNothing, nil), nil
		}
		rt, err := arithType(op, a, b)
		if err != nil {
			return nil, err
		}
		f := arithFunc(op, rt)
		return strict(args, rt, f), nil
	}
}

func arithFunc(op string, rt chtype.Type) func(v []any) (any, error) {
	k := rt.Kind
	switch {
	case isFloatKind(k):
		return func(v []any) (any, error) {
			x, y := asFloat(v[0]), asFloat(v[1])
			switch op {
			case "plus":
				return x + y, nil
			case "minus":
				return x - y, nil
			case "multiply":
				return x * y, nil
			case "divide":
				return x / y, nil
			case "modulo", "moduloOrZero":
				if y == 0 && op == "moduloOrZero" {
					return 0.0, nil
				}
				// ClickHouse computes x - trunc(x/y)*y (not fmod). The
				// conversion rounds the product before the subtraction
				// (no fused multiply-add); -0 prints as 0.
				r := x - float64(math.Trunc(x/y)*y)
				if r == 0 {
					r = 0
				}
				return r, nil
			}
			return nil, cherr.New(cherr.LogicalError, "bad float op %s", op)
		}
	case op == "intDiv" || op == "intDivOrZero":
		return func(v []any) (any, error) {
			y := asFloat(v[1])
			if y == 0 {
				if op == "intDivOrZero" {
					return aggstate.Wrap(rt, uint64(0)), nil
				}
				return nil, cherr.New(illegalDivision, "Division by zero")
			}
			if isFloatKind(kindOf(v[0])) || isFloatKind(kindOf(v[1])) {
				return aggstate.Wrap(rt, floatToInt64(math.Trunc(asFloat(v[0])/y))), nil
			}
			return intDivide(v[0], v[1], rt), nil
		}
	case op == "modulo" || op == "moduloOrZero":
		return func(v []any) (any, error) {
			if asFloat(v[1]) == 0 {
				if op == "moduloOrZero" {
					return aggstate.Wrap(rt, uint64(0)), nil
				}
				return nil, cherr.New(illegalDivision, "Division by zero")
			}
			return intModulo(v[0], v[1], rt), nil
		}
	case isSignedKind(k):
		return func(v []any) (any, error) {
			x, y := asInt(v[0]), asInt(v[1])
			var r int64
			switch op {
			case "plus":
				r = x + y
			case "minus":
				r = x - y
			case "multiply":
				r = x * y
			}
			return aggstate.Wrap(rt, r), nil
		}
	}
	return func(v []any) (any, error) {
		x, y := asUint(v[0]), asUint(v[1])
		var r uint64
		switch op {
		case "plus":
			r = x + y
		case "minus":
			r = x - y
		case "multiply":
			r = x * y
		}
		return aggstate.Wrap(rt, r), nil
	}
}

// kindOf reports a representative kind for a canonical numeric value.
func kindOf(v any) chtype.Kind {
	switch v.(type) {
	case float64:
		return chtype.Float64
	case int64:
		return chtype.Int64
	}
	return chtype.UInt64
}

func intDivide(a, b any, rt chtype.Type) any {
	_, au := a.(uint64)
	_, bu := b.(uint64)
	if au && bu {
		return aggstate.Wrap(rt, a.(uint64)/b.(uint64))
	}
	x, y := asInt(a), asInt(b)
	if y == -1 {
		return aggstate.Wrap(rt, -x)
	}
	return aggstate.Wrap(rt, x/y)
}

func intModulo(a, b any, rt chtype.Type) any {
	_, au := a.(uint64)
	_, bu := b.(uint64)
	if au && bu {
		return aggstate.Wrap(rt, a.(uint64)%b.(uint64))
	}
	if au {
		// Unsigned dividend, signed divisor: sign follows the dividend.
		y := asInt(b)
		if y < 0 {
			y = -y
		}
		return aggstate.Wrap(rt, a.(uint64)%uint64(y))
	}
	x, y := asInt(a), asInt(b)
	if y == -1 {
		return aggstate.Wrap(rt, int64(0))
	}
	if bu && b.(uint64) > math.MaxInt64 {
		return aggstate.Wrap(rt, x)
	}
	return aggstate.Wrap(rt, x%y)
}

func buildNegate(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 1); err != nil {
		return nil, err
	}
	t := base(args[0])
	var rt chtype.Type
	switch {
	case isFloatKind(t.Kind):
		rt = t
	case isSignedKind(t.Kind):
		rt = t
	case isIntKind(t.Kind):
		rt = intType(true, nextSize(intBytes(t.Kind)))
	case t.Kind == chtype.Nothing:
		return constant(nullNothing, nil), nil
	default:
		return nil, illegalType(name, args[0].t, 1)
	}
	return strict(args, rt, func(v []any) (any, error) {
		if isFloatKind(rt.Kind) {
			return -asFloat(v[0]), nil
		}
		return aggstate.Wrap(rt, -asInt(v[0])), nil
	}), nil
}

func buildAbs(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 1); err != nil {
		return nil, err
	}
	t := base(args[0])
	var rt chtype.Type
	switch {
	case isFloatKind(t.Kind):
		rt = t
	case isSignedKind(t.Kind):
		rt = intType(false, intBytes(t.Kind))
	case isIntKind(t.Kind):
		rt = t
	default:
		return nil, illegalType(name, args[0].t, 1)
	}
	return strict(args, rt, func(v []any) (any, error) {
		switch x := v[0].(type) {
		case float64:
			return math.Abs(x), nil
		case int64:
			if x < 0 {
				return uint64(-x), nil
			}
			return uint64(x), nil
		}
		return v[0], nil
	}), nil
}

func roundBuilder(mode string) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 1, 2); err != nil {
			return nil, err
		}
		t := base(args[0])
		if !isNumKind(t.Kind) {
			return nil, illegalType(name, args[0].t, 1)
		}
		if t.Kind == chtype.Bool {
			t = chtype.TUInt8
		}
		var scale int64
		if len(args) == 2 {
			if !args[1].konst || !isIntKind(base(args[1]).Kind) {
				return nil, cherr.New(cherr.IllegalColumn, "Scale argument of function %s must be a constant integer", name)
			}
			scale = asInt(args[1].val)
		}
		return strict(args[:1], t, func(v []any) (any, error) {
			return roundValue(mode, v[0], scale, t), nil
		}), nil
	}
}

func roundValue(mode string, v any, scale int64, t chtype.Type) any {
	apply := func(f float64) float64 {
		switch mode {
		case "floor":
			return math.Floor(f)
		case "ceil":
			return math.Ceil(f)
		case "trunc":
			return math.Trunc(f)
		}
		return math.RoundToEven(f)
	}
	if isFloatKind(t.Kind) {
		f := asFloat(v)
		p := math.Pow(10, float64(scale))
		r := apply(f*p) / p
		if t.Kind == chtype.Float32 {
			r = float64(float32(r))
		}
		return r
	}
	if scale >= 0 {
		return v
	}
	p := math.Pow(10, float64(-scale))
	return aggstate.Wrap(t, apply(asFloat(v)/p)*p)
}

func extremeBuilder(greatest bool) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 1, -1); err != nil {
			return nil, err
		}
		// NULL arguments are ignored (ClickHouse >= 24.12,
		// least_greatest_legacy_null_behavior = 0): the result is NULL only
		// when every argument is NULL.
		var ts []chtype.Type
		nullable, lc := false, false
		for _, a := range args {
			if a.t.IsNullable() || isNothing(a.t) {
				nullable = true
			}
			if a.t.Kind == chtype.LowCardinality {
				lc = true
			}
			if !isNothing(a.t) {
				ts = append(ts, base(a))
			}
		}
		if len(ts) == 0 {
			return constant(nullNothing, nil), nil
		}
		st, err := chtype.Supertype(ts...)
		if err != nil {
			return nil, err
		}
		conv := make([]*expr, len(args))
		for i, a := range args {
			if isNothing(a.t) {
				conv[i] = constant(nullableIf(st, true), nil)
				continue
			}
			if conv[i], err = castExpr(a, nullableIf(st, a.t.IsNullable()), false); err != nil {
				return nil, err
			}
		}
		rt := st
		if lc && canBeInLowCardinality(rt) {
			rt = chtype.NewLowCardinality(rt)
		}
		rt = nullableIf(rt, nullable)
		hint := 1
		if greatest {
			hint = -1
		}
		k := st.Unwrap().Kind
		composite := k == chtype.Array || k == chtype.Tuple || k == chtype.Map
		x := &expr{t: rt, fn: func(row []any) (any, error) {
			var best any
			for _, a := range conv {
				v, err := a.eval(row)
				if err != nil {
					return nil, err
				}
				if v == nil {
					continue
				}
				if best == nil {
					best = v
					continue
				}
				var cmp int
				if composite {
					// Nested NULL/NaN elements never win (compareAt with
					// nan_direction_hint -1 for greatest, +1 for least):
					// greatest([1,NULL],[1,2]) = [1,2], least([NULL],[1]) = [1].
					cmp = aggstate.CompareTypedHint(v, best, st, hint)
				} else {
					cmp = aggstate.CompareTyped(v, best, st)
				}
				if (greatest && cmp > 0) || (!greatest && cmp < 0) {
					best = v
				}
			}
			return best, nil
		}}
		return fold(x, conv), nil
	}
}

// ---------------------------------------------------------------------------
// Conditionals

// unify converts branch expressions to their common supertype.
func unify(name string, xs []*expr) ([]*expr, chtype.Type, error) {
	ts := make([]chtype.Type, len(xs))
	for i, x := range xs {
		ts[i] = x.t
	}
	st, err := chtype.Supertype(ts...)
	if err != nil {
		return nil, chtype.Type{}, supertypeError(err, "No common type for branches of function %s", name)
	}
	out := make([]*expr, len(xs))
	for i, x := range xs {
		if out[i], err = castExpr(x, st, false); err != nil {
			return nil, chtype.Type{}, err
		}
	}
	return out, st, nil
}

func buildIf(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 3, 3); err != nil {
		return nil, err
	}
	return buildMultiIf(c, name, args)
}

func buildMultiIf(c *compiler, name string, args []*expr) (*expr, error) {
	if len(args) < 3 || len(args)%2 == 0 {
		return nil, argCountError(name, len(args), "an odd number >= 3")
	}
	var conds, branches []*expr
	for i := 0; i+1 < len(args); i += 2 {
		if err := checkCondType(name, args[i], i+1); err != nil {
			return nil, err
		}
		conds = append(conds, args[i])
		branches = append(branches, args[i+1])
	}
	branches = append(branches, args[len(args)-1])
	branches, st, err := unify(name, branches)
	if err != nil {
		return nil, err
	}
	x := &expr{t: st, fn: func(row []any) (any, error) {
		for i, cnd := range conds {
			v, err := cnd.eval(row)
			if err != nil {
				return nil, err
			}
			if v == nil {
				continue
			}
			b, err := truth(v)
			if err != nil {
				return nil, err
			}
			if b {
				return branches[i].eval(row)
			}
		}
		return branches[len(branches)-1].eval(row)
	}}
	return fold(x, args), nil
}

func buildCoalesce(c *compiler, name string, args []*expr) (*expr, error) {
	if name == "ifNull" {
		if err := wantArgs(name, args, 2, 2); err != nil {
			return nil, err
		}
	} else if err := wantArgs(name, args, 1, -1); err != nil {
		return nil, err
	}
	var live []*expr
	for _, a := range args {
		if !isNothing(a.t) {
			live = append(live, a)
		}
	}
	if len(live) == 0 {
		return constant(nullNothing, nil), nil
	}
	nullable := true
	ts := make([]chtype.Type, len(live))
	for i, a := range live {
		ts[i] = removeNullable(a.t)
		if !a.t.IsNullable() {
			nullable = false
		}
	}
	st, err := chtype.Supertype(ts...)
	if err != nil {
		return nil, supertypeError(err, "No common type for arguments of function %s", name)
	}
	rt := nullableIf(st, nullable)
	convs := make([]func(any) (any, error), len(live))
	for i, a := range live {
		if convs[i], err = converter(a.t, st); err != nil {
			return nil, err
		}
	}
	x := &expr{t: rt, fn: func(row []any) (any, error) {
		for i, a := range live {
			v, err := a.eval(row)
			if err != nil {
				return nil, err
			}
			if v != nil {
				return convs[i](v)
			}
		}
		return nil, nil
	}}
	return fold(x, live), nil
}

// removeNullable strips a top-level (or LowCardinality-nested) Nullable.
func removeNullable(t chtype.Type) chtype.Type {
	switch t.Kind {
	case chtype.Nullable:
		return *t.Elem
	case chtype.LowCardinality:
		if t.Elem.Kind == chtype.Nullable {
			return chtype.NewLowCardinality(*t.Elem.Elem)
		}
	}
	return t
}

func buildNullIf(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 2, 2); err != nil {
		return nil, err
	}
	eq, err := c.call("equals", args[0], args[1])
	if err != nil {
		return nil, err
	}
	a := args[0]
	rt := nullableIf(a.t, true)
	x := &expr{t: rt, fn: func(row []any) (any, error) {
		v, err := a.eval(row)
		if err != nil || v == nil {
			return nil, err
		}
		e, err := eq.eval(row)
		if err != nil {
			return nil, err
		}
		if e != nil && asUint(e) == 1 {
			return nil, nil
		}
		return v, nil
	}}
	return fold(x, args), nil
}

func nullCheckBuilder(isNull bool) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 1, 1); err != nil {
			return nil, err
		}
		a := args[0]
		x := &expr{t: chtype.TUInt8, fn: func(row []any) (any, error) {
			v, err := a.eval(row)
			if err != nil {
				return nil, err
			}
			return boolNum((v == nil) == isNull), nil
		}}
		return fold(x, args), nil
	}
}

func buildAssumeNotNull(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 1); err != nil {
		return nil, err
	}
	a := args[0]
	rt := removeNullable(a.t)
	if isNothing(a.t) {
		rt = chtype.TNothing
	}
	x := &expr{t: rt, fn: func(row []any) (any, error) {
		v, err := a.eval(row)
		if err != nil {
			return nil, err
		}
		if v == nil {
			return zeroValue(rt), nil
		}
		return v, nil
	}}
	return fold(x, args), nil
}

func buildToNullable(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 1); err != nil {
		return nil, err
	}
	a := args[0]
	return fold(&expr{t: nullableIf(a.t, true), fn: a.eval}, args), nil
}

func buildToTypeName(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 1); err != nil {
		return nil, err
	}
	return constant(chtype.TString, args[0].t.String()), nil
}

func buildIsFinite(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 1); err != nil {
		return nil, err
	}
	return strict(args, chtype.TUInt8, func(v []any) (any, error) {
		f := asFloat(v[0])
		return boolNum(!math.IsInf(f, 0) && !math.IsNaN(f)), nil
	}), nil
}

func buildIsNaN(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 1); err != nil {
		return nil, err
	}
	return strict(args, chtype.TUInt8, func(v []any) (any, error) { return boolNum(isNaN(v[0])), nil }), nil
}

// ---------------------------------------------------------------------------
// Conversion

type convMode int

const (
	convPlain convMode = iota
	convOrZero
	convOrNull
)

func convBuilder(to chtype.Type, mode convMode) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 1, 1); err != nil {
			return nil, err
		}
		a := args[0]
		from := base(a)
		if mode != convPlain && !isStringKind(from.Kind) {
			return nil, illegalType(name, a.t, 1)
		}
		f := func(v []any) (any, error) {
			if isStringKind(from.Kind) && isNumKind(to.Kind) {
				s := strings.TrimRight(v[0].(string), "\x00")
				out, err := parseNumberText(s, to, mode != convPlain)
				if err == nil || mode == convPlain {
					return out, err
				}
			} else {
				out, err := convertValue(c.q, v[0], from, to)
				if err == nil || mode == convPlain {
					return out, err
				}
			}
			if mode == convOrNull {
				return nil, nil
			}
			return zeroValue(to), nil
		}
		rt := to
		if mode == convOrNull {
			rt = chtype.NewNullable(to)
		}
		return strict(args, rt, f), nil
	}
}

// parseNumberText parses a string as ClickHouse's toIntN/toFloatN do: an
// out-of-range integer wraps around to the type's width (toUInt8('300') =
// 44), or, when strict (the OrNull/OrZero variants), is an error. A '-'
// sign is never accepted for an unsigned type.
func parseNumberText(s string, to chtype.Type, strict bool) (any, error) {
	if isFloatKind(to.Kind) {
		switch strings.ToLower(s) {
		case "inf", "+inf", "infinity":
			return math.Inf(1), nil
		case "-inf", "-infinity":
			return math.Inf(-1), nil
		case "nan", "+nan", "-nan":
			return math.NaN(), nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil && !errors.Is(err, strconv.ErrRange) {
			return nil, numberTextError(s, to)
		}
		if to.Kind == chtype.Float32 {
			f = float64(float32(f))
		}
		return f, nil
	}
	if isSignedKind(to.Kind) {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, numberTextError(s, to)
		}
		w := aggstate.Wrap(to, n)
		if strict && w != any(n) {
			return nil, cherr.New(cherr.CannotParseNumber, "Value %s is out of range for %s", s, to.String())
		}
		return w, nil
	}
	if to.Kind == chtype.Bool {
		return chtype.Convert(s, chtype.TString, to)
	}
	if strings.HasPrefix(s, "-") {
		if _, err := strconv.ParseInt(s, 10, 64); err != nil && !errors.Is(err, strconv.ErrRange) {
			return nil, numberTextError(s, to)
		}
		return nil, cherr.New(cherr.CannotParseNumber, "Unsigned type must not contain '-' symbol: Cannot parse %s from String", to.String())
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(s, "+"), 10, 64)
	if err != nil {
		return nil, numberTextError(s, to)
	}
	w := aggstate.Wrap(to, n)
	if strict && w != any(n) {
		return nil, cherr.New(cherr.CannotParseNumber, "Value %s is out of range for %s", s, to.String())
	}
	return w, nil
}

// numberTextError is the error ClickHouse's toIntN/toFloatN/CAST give for a
// string that is not a number (verified against ClickHouse 25.8): an empty
// integer is ATTEMPT_TO_READ_AFTER_EOF (32), a lone sign is
// CANNOT_PARSE_NUMBER (72); a float with no digits or a dangling exponent is
// 72; anything else is CANNOT_PARSE_TEXT (6).
func numberTextError(s string, to chtype.Type) error {
	name := to.String()
	if isFloatKind(to.Kind) {
		l := strings.ToLower(s)
		switch {
		case l == "":
			return cherr.New(cherr.CannotParseNumber, "Cannot read floating point value: Cannot parse %s from String", name)
		case l == "-" || l == "+":
			return cherr.New(cherr.CannotParseNumber, "Cannot read floating point value: no digits read: Cannot parse %s from String", name)
		case strings.HasSuffix(l, "e") || strings.HasSuffix(l, "e+") || strings.HasSuffix(l, "e-"):
			if m := strings.TrimRight(strings.TrimRight(l, "+-"), "e"); m != "" && strings.Trim(m, "+-0123456789.") == "" && strings.ContainsAny(m, "0123456789") {
				return cherr.New(cherr.CannotParseNumber, "Cannot read floating point value: nothing after exponent: Cannot parse %s from String", name)
			}
		}
		return cherr.New(cherr.CannotParseText, "Cannot parse string '%s' as %s", s, name)
	}
	switch s {
	case "":
		return cherr.New(32, "Attempt to read after eof: Cannot parse %s from String, because value is too short", name)
	case "-", "+":
		return cherr.New(cherr.CannotParseNumber, "Cannot parse number without any digits: Cannot parse %s from String", name)
	}
	return cherr.New(cherr.CannotParseText, "Cannot parse string '%s' as %s", s, name)
}

// convertValue converts a non-NULL value with time-zone awareness for
// date/time targets.
func convertValue(q *query, v any, from, to chtype.Type) (any, error) {
	if isTimeKind(to.Kind) {
		return convertToTime(q, v, from, to)
	}
	if isNumKind(to.Kind) && isNumKind(from.Kind) {
		if to.Kind == chtype.Bool {
			return asFloat(v) != 0, nil
		}
		if isFloatKind(to.Kind) {
			f := asFloat(v)
			if to.Kind == chtype.Float32 {
				f = float64(float32(f))
			}
			return f, nil
		}
		if f, ok := v.(float64); ok {
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return chtype.Convert(v, from, to)
			}
			if isSignedKind(to.Kind) {
				return aggstate.Wrap(to, floatToInt64(f)), nil
			}
			return aggstate.Wrap(to, asUint(f)), nil
		}
		return aggstate.Wrap(to, v), nil
	}
	if isNumKind(to.Kind) && isTimeKind(from.Kind) {
		t := v.(time.Time)
		var n int64
		switch from.Kind {
		case chtype.Date, chtype.Date32:
			n = t.Unix() / 86400
		default:
			n = t.Unix()
		}
		if isFloatKind(to.Kind) {
			return float64(n), nil
		}
		return aggstate.Wrap(to, n), nil
	}
	return chtype.Convert(v, from, to)
}

func buildToString(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 2); err != nil {
		return nil, err
	}
	a := args[0]
	from := base(a)
	if len(args) == 2 {
		if !isTimeKind(from.Kind) || !args[1].konst {
			return nil, illegalType(name, args[1].t, 2)
		}
		tz, _ := args[1].val.(string)
		from = withTZ(from, tz)
	}
	return strict(args[:1], chtype.TString, func(v []any) (any, error) {
		return valueToString(c.q, v[0], from), nil
	}), nil
}

func withTZ(t chtype.Type, tz string) chtype.Type {
	switch t.Kind {
	case chtype.DateTime, chtype.DateTime64:
		t.TZ = tz
	}
	return t
}

// valueToString renders v like toString does (unescaped text form, times in
// the column/server time zone).
func valueToString(q *query, v any, t chtype.Type) string {
	switch t.Kind {
	case chtype.FixedString:
		if s, ok := v.(string); ok {
			return strings.TrimRight(s, "\x00")
		}
	case chtype.String, chtype.Enum8, chtype.Enum16, chtype.UUID:
		if s, ok := v.(string); ok {
			return s
		}
	case chtype.DateTime, chtype.DateTime64:
		if tm, ok := v.(time.Time); ok {
			return formatDateTimeText(tm.In(q.location(t)), t)
		}
	case chtype.Array, chtype.Tuple, chtype.Map:
		// Rendered piece by piece into a grown buffer: copy the bytes
		// out of its slack, as the value may be kept.
		return strings.Clone(chtype.FormatText(t, v))
	}
	return chtype.FormatText(t, v)
}

func formatDateTimeText(tm time.Time, t chtype.Type) string {
	s := tm.Format("2006-01-02 15:04:05")
	if t.Kind == chtype.DateTime64 && t.Precision > 0 {
		frac := strconv.FormatInt(int64(tm.Nanosecond()), 10)
		frac = strings.Repeat("0", 9-len(frac)) + frac
		s += "." + frac[:t.Precision]
	}
	return s
}

func buildToLowCardinality(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 1); err != nil {
		return nil, err
	}
	a := args[0]
	if a.t.Kind == chtype.LowCardinality {
		return a, nil
	}
	return fold(&expr{t: chtype.NewLowCardinality(a.t), fn: a.eval}, args), nil
}

func buildCastFunc(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 2, 2); err != nil {
		return nil, err
	}
	s, ok := args[1].val.(string)
	if !args[1].konst || !ok {
		return nil, cherr.New(cherr.IllegalTypeOfArgument, "Second argument to %s must be a constant string describing type", name)
	}
	return c.castTo(args[0], s)
}

// ---------------------------------------------------------------------------
// Tuples and arrays

func buildTuple(c *compiler, name string, args []*expr) (*expr, error) {
	fields := make([]chtype.Field, len(args))
	for i, a := range args {
		fields[i] = chtype.Field{Type: a.t}
	}
	x := &expr{t: chtype.NewTuple(fields...), fn: func(row []any) (any, error) {
		out := make([]any, len(args))
		for i, a := range args {
			v, err := a.eval(row)
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	}}
	return fold(x, args), nil
}

func buildArray(c *compiler, name string, args []*expr) (*expr, error) {
	if len(args) == 0 {
		return constant(chtype.NewArray(chtype.TNothing), []any{}), nil
	}
	elems, st, err := unify(name, args)
	if err != nil {
		return nil, err
	}
	x := &expr{t: chtype.NewArray(st), fn: func(row []any) (any, error) {
		out := make([]any, len(elems))
		for i, a := range elems {
			v, err := a.eval(row)
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	}}
	return fold(x, args), nil
}

func buildArrayElement(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 2, 2); err != nil {
		return nil, err
	}
	at := base(args[0])
	switch at.Kind {
	case chtype.Array:
		et := *at.Elem
		if !isIntKind(base(args[1]).Kind) {
			return nil, illegalType(name, args[1].t, 2)
		}
		return strict(args, et, func(v []any) (any, error) {
			arr := v[0].([]any)
			i := asInt(v[1])
			switch {
			case i > 0 && i <= int64(len(arr)):
				return arr[i-1], nil
			case i < 0 && -i <= int64(len(arr)):
				return arr[int64(len(arr))+i], nil
			}
			return zeroValue(et), nil
		}), nil
	case chtype.Map:
		vt := *at.Elem
		key, err := castExpr(args[1], *at.Key, false)
		if err != nil {
			return nil, err
		}
		return strict([]*expr{args[0], key}, vt, func(v []any) (any, error) {
			for _, kv := range v[0].([]any) {
				p := kv.([]any)
				if chtype.Compare(p[0], v[1]) == 0 {
					return p[1], nil
				}
			}
			return zeroValue(vt), nil
		}), nil
	}
	return nil, illegalType(name, args[0].t, 1)
}

func buildTupleElement(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 2, 2); err != nil {
		return nil, err
	}
	tt := base(args[0])
	if tt.Kind != chtype.Tuple {
		return nil, illegalType(name, args[0].t, 1)
	}
	if !args[1].konst {
		return nil, cherr.New(cherr.IllegalColumn, "Second argument of %s must be a constant", name)
	}
	idx := -1
	switch k := args[1].val.(type) {
	case uint64:
		idx = int(k) - 1
	case int64:
		idx = int(k) - 1
	case string:
		for i, f := range tt.Fields {
			if f.Name == k {
				idx = i
			}
		}
	}
	if idx < 0 || idx >= len(tt.Fields) {
		return nil, cherr.New(cherr.BadArguments, "Tuple %s doesn't have element %v", tt.String(), args[1].val)
	}
	ft := tt.Fields[idx].Type
	a := args[0]
	x := &expr{t: ft, fn: func(row []any) (any, error) {
		v, err := a.eval(row)
		if err != nil || v == nil {
			return nil, err
		}
		return v.([]any)[idx], nil
	}}
	return fold(x, args[:1]), nil
}

func buildHas(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 2, 2); err != nil {
		return nil, err
	}
	at := base(args[0])
	if at.Kind != chtype.Array {
		return nil, illegalType(name, args[0].t, 1)
	}
	needle := args[1]
	arr := args[0]
	x := &expr{t: chtype.TUInt8, fn: func(row []any) (any, error) {
		a, err := arr.eval(row)
		if err != nil {
			return nil, err
		}
		n, err := needle.eval(row)
		if err != nil {
			return nil, err
		}
		for _, e := range a.([]any) {
			if (e == nil) != (n == nil) {
				continue
			}
			if e == nil || chtype.Compare(e, n) == 0 {
				return uint64(1), nil
			}
		}
		return uint64(0), nil
	}}
	return fold(x, args), nil
}

func buildIndexOf(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 2, 2); err != nil {
		return nil, err
	}
	if base(args[0]).Kind != chtype.Array {
		return nil, illegalType(name, args[0].t, 1)
	}
	arr, needle := args[0], args[1]
	x := &expr{t: chtype.TUInt64, fn: func(row []any) (any, error) {
		a, err := arr.eval(row)
		if err != nil {
			return nil, err
		}
		n, err := needle.eval(row)
		if err != nil {
			return nil, err
		}
		for i, e := range a.([]any) {
			if (e == nil && n == nil) || (e != nil && n != nil && chtype.Compare(e, n) == 0) {
				return uint64(i + 1), nil
			}
		}
		return uint64(0), nil
	}}
	return fold(x, args), nil
}

func buildFinalizeAggregation(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 1); err != nil {
		return nil, err
	}
	at := base(args[0])
	if at.Kind != chtype.AggregateFunction {
		return nil, illegalType(name, args[0].t, 1)
	}
	fn, stArgs := stateBase(at.AggFunc), at.AggArgs
	if fn != at.AggFunc && len(stArgs) > 0 {
		stArgs = stArgs[:len(stArgs)-1] // -If state: the last argument is the condition
	}
	rt, err := aggResultType(fn, stArgs)
	if err != nil {
		return nil, err
	}
	a := args[0]
	ctx := c.q.ctx
	x := &expr{t: rt, fn: func(row []any) (any, error) {
		v, err := a.eval(row)
		if err != nil {
			return nil, err
		}
		p, ok := v.(*chtype.AggPartial)
		if !ok || p == nil {
			p = aggstate.New(fn, stArgs)
		}
		return aggstate.Finalize(ctx, p, rt)
	}}
	return x, nil
}
