package chtype

import (
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/limits"
)

// smallStack makes a stack overflow fail fast instead of growing towards
// the 1 GiB default.
func smallStack(t *testing.T) {
	old := debug.SetMaxStack(16 << 20)
	t.Cleanup(func() { debug.SetMaxStack(old) })
}

func errCode(err error) int {
	var ce *cherr.Error
	if errors.As(err, &ce) {
		return ce.Code
	}
	return 0
}

// nested wraps inner in n levels of open...close.
func nested(open, inner, close string, n int) string {
	return strings.Repeat(open, n) + inner + strings.Repeat(close, n)
}

// allocated returns the bytes f allocated.
func allocated(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

func wantTooDeep(t *testing.T, what string, err error) {
	t.Helper()
	want := fmt.Sprintf("Maximum parse depth (%d) exceeded", limits.MaxParserDepth)
	if errCode(err) != cherr.TooDeepRecursion || !strings.Contains(err.Error(), want) {
		t.Errorf("%s: got %v, want TOO_DEEP_RECURSION %q", what, err, want)
	}
}

// The parser guards below are tested through ParseStored, which has no
// length cap, so the parser sees the whole input.

func TestParseRejectsTooDeepType(t *testing.T) {
	smallStack(t)
	const n = 1 << 17
	cases := map[string]string{
		"Array":           nested("Array(", "UInt8", ")", n),
		"Nullable":        nested("Nullable(", "UInt8", ")", n),
		"LowCardinality":  nested("LowCardinality(", "UInt8", ")", n),
		"Tuple":           nested("Tuple(", "UInt8", ")", n),
		"named Tuple":     nested("Tuple(a ", "UInt8", ")", n),
		"Map":             nested("Map(String, ", "UInt8", ")", n),
		"AggregateFunc":   nested("AggregateFunction(any, ", "UInt8", ")", n),
		"SimpleAggregate": nested("SimpleAggregateFunction(any, ", "UInt8", ")", n),
		"unclosed":        strings.Repeat("Array(", n),
	}
	for name, s := range cases {
		_, err := ParseStored(s)
		wantTooDeep(t, name, err)
	}
}

// TestParseTypeDepthBoundary checks that MaxParserDepth nested types parse
// and one more does not.
func TestParseTypeDepthBoundary(t *testing.T) {
	d := limits.MaxParserDepth
	ok := nested("Array(", "UInt8", ")", d-1)
	typ, err := Parse(ok)
	if err != nil {
		t.Fatalf("depth %d: %v", d, err)
	}
	if typ.String() != ok {
		t.Errorf("String() does not round-trip at depth %d", d)
	}
	_, err = Parse(nested("Array(", "UInt8", ")", d))
	wantTooDeep(t, "one level too deep", err)
	_, err = Parse(nested("Array(", "UInt8", ")", d+1))
	wantTooDeep(t, "one parenthesis too deep", err)
}

// TestParseTooDeepTypeStopsEarly checks that a deep type string is rejected
// before the rest of it is tokenized.
func TestParseTooDeepTypeStopsEarly(t *testing.T) {
	smallStack(t)
	s := nested("Array(", "UInt8", ")", 1<<20)
	var err error
	n := allocated(func() { _, err = ParseStored(s) })
	wantTooDeep(t, "deep type", err)
	if n > 1<<20 {
		t.Errorf("rejecting a too-deep type allocated %d bytes", n)
	}
}

// TestParseBadTypeStopsEarly checks that a bad type fails at its first bad
// token without the rest of it being tokenized or echoed in the error.
func TestParseBadTypeStopsEarly(t *testing.T) {
	const size = 1 << 20
	for _, unit := range []string{",", ")", "(", "a ", "UInt8 a", "Foo(", "\x00"} {
		s := strings.Repeat(unit, size/len(unit))
		var err error
		n := allocated(func() { _, err = ParseStored(s) })
		if err == nil {
			t.Errorf("%q: want an error", unit)
		}
		if n > 64<<10 {
			t.Errorf("%q x %d: rejecting it allocated %d bytes", unit, size/len(unit), n)
		}
	}
}

// TestParseReportsLexicalErrorFirst checks that a bad character anywhere in
// a type wins over an error found before it, as in ClickHouse, except for
// the depth error raised while parsing.
func TestParseReportsLexicalErrorFirst(t *testing.T) {
	for _, s := range []string{"Foo(@)", "FixedString(0) @", "Array(UInt8, @)"} {
		if _, err := Parse(s); errCode(err) != cherr.SyntaxError {
			t.Errorf("%s: got %v, want SYNTAX_ERROR", s, err)
		}
	}
	_, err := Parse(nested("Array(", "UInt8", ")", limits.MaxParserDepth) + "@")
	wantTooDeep(t, "deep type with a bad character", err)
}

// TestDeepTypeValues runs the recursive value and type paths on the deepest
// type Parse accepts.
func TestDeepTypeValues(t *testing.T) {
	d := limits.MaxParserDepth - 1
	typ := MustParse(nested("Array(", "UInt8", ")", d))
	lit := nested("[", "1", "]", d)
	v, err := ParseText(typ, lit)
	if err != nil {
		t.Fatalf("ParseText: %v", err)
	}
	if _, _, err := ParseQuoted(typ, lit); err != nil {
		t.Fatalf("ParseQuoted: %v", err)
	}
	if got := FormatText(typ, v); got != lit {
		t.Errorf("FormatText does not round-trip")
	}
	if _, err := Convert(lit, TString, typ); err != nil {
		t.Errorf("Convert: %v", err)
	}
	if _, err := Normalize(typ, v); err != nil {
		t.Errorf("Normalize: %v", err)
	}
	if st, err := Supertype(typ, typ); err != nil || !st.Equal(typ) {
		t.Errorf("Supertype: %v, %v", st, err)
	}
	if z := Zero(typ); Compare(z, []any{}) != 0 {
		t.Errorf("Zero: %v", z)
	}
}

// TestValueParsersNestNoDeeperThanType checks that value nesting is bounded
// by the type: extra brackets fail at the first level the type lacks.
func TestValueParsersNestNoDeeperThanType(t *testing.T) {
	smallStack(t)
	arr := MustParse("Array(Array(UInt8))")
	tup := MustParse("Tuple(Tuple(UInt8))")
	m := MustParse("Map(String, Map(String, UInt8))")
	for name, s := range map[string]string{
		"array": strings.Repeat("[", 1<<20),
		"tuple": strings.Repeat("(", 1<<20),
		"map":   strings.Repeat("{'k':", 1<<20),
	} {
		for _, typ := range []Type{arr, tup, m} {
			if _, err := ParseText(typ, s); err == nil {
				t.Errorf("ParseText %s as %s: want an error", name, typ)
			}
			if _, _, err := ParseQuoted(typ, s); err == nil {
				t.Errorf("ParseQuoted %s as %s: want an error", name, typ)
			}
			if _, err := Convert(s, TString, typ); err == nil {
				t.Errorf("Convert %s to %s: want an error", name, typ)
			}
		}
	}
}

func TestParseFixedStringSizeLimit(t *testing.T) {
	if _, err := Parse(fmt.Sprintf("FixedString(%d)", limits.MaxFixedStringSize)); err != nil {
		t.Errorf("FixedString(%d): %v", limits.MaxFixedStringSize, err)
	}
	for _, n := range []uint64{limits.MaxFixedStringSize + 1, 1<<31 - 1, 1 << 31, 1 << 40, 1<<64 - 1} {
		for _, s := range []string{"FixedString(%d)", "Array(FixedString(%d))", "BINARY(%d)"} {
			s = fmt.Sprintf(s, n)
			_, err := Parse(s)
			if errCode(err) != cherr.ArgumentOutOfBound || !strings.Contains(err.Error(), "FixedString size is too large") {
				t.Errorf("%s: got %v, want ARGUMENT_OUT_OF_BOUND", s, err)
			}
		}
	}
}

// explicitEnum is an Enum16 type of n elements that all have a value.
func explicitEnum(n int) string {
	var b strings.Builder
	b.WriteString("Enum16(")
	for v := range n {
		if v > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "'e%d' = %d", v, v-32768)
	}
	b.WriteString(")")
	return b.String()
}

// TestParseEnumTimeIsLinear checks that parsing an Enum scales with its
// element count. The check for mixed implicit and explicit values was
// quadratic in it.
func TestParseEnumTimeIsLinear(t *testing.T) {
	small, large := explicitEnum(1<<14), explicitEnum(1<<16)
	fastest := func(s string) time.Duration {
		best := time.Duration(1 << 62)
		for range 3 {
			start := time.Now()
			if _, err := ParseStored(s); err != nil {
				t.Fatal(err)
			}
			best = min(best, time.Since(start))
		}
		return best
	}
	// 4x the elements: about 4x the time when linear, 16x when quadratic.
	if a, b := fastest(small), fastest(large); b > 8*a {
		t.Errorf("4x the Enum elements took %.1fx the time (%v, %v)", float64(b)/float64(a), a, b)
	}
}

// tupleOfSize is a valid Tuple of UInt8 type name exactly n bytes long.
func tupleOfSize(n int) string {
	fields := (n - len("Tuple(UInt8)")) / len("UInt8,")
	s := "Tuple(" + strings.Repeat("UInt8,", fields) + "UInt8"
	return s + strings.Repeat(" ", n-len(s)-1) + ")"
}

// TestParseTypeNameLengthLimit checks that Parse takes a type name of
// maxTypeNameBytes and rejects a longer one before parsing it, while
// ParseStored takes the longer one.
func TestParseTypeNameLengthLimit(t *testing.T) {
	ok := tupleOfSize(maxTypeNameBytes)
	typ, err := Parse(ok)
	if err != nil {
		t.Fatalf("%d-byte type: %v", len(ok), err)
	}
	if want := (maxTypeNameBytes-len("Tuple(UInt8)"))/len("UInt8,") + 1; len(typ.Fields) != want {
		t.Errorf("%d-byte type: %d fields, want %d", len(ok), len(typ.Fields), want)
	}

	long := tupleOfSize(maxTypeNameBytes + 1)
	n := allocated(func() { _, err = Parse(long) })
	if errCode(err) != cherr.SyntaxError || !strings.Contains(err.Error(), "Max query size exceeded") {
		t.Errorf("%d-byte type: got %v, want SYNTAX_ERROR Max query size exceeded", len(long), err)
	}
	if n > 64<<10 {
		t.Errorf("rejecting a %d-byte type allocated %d bytes", len(long), n)
	}
	if _, err := ParseStored(long); err != nil {
		t.Errorf("ParseStored of a %d-byte type: %v", len(long), err)
	}
}
