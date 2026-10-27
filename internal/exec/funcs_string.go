package exec

import (
	"math"
	"math/bits"
	"regexp"
	"regexp/syntax"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/gfronza/twinhouse/internal/cherr"
	"github.com/gfronza/twinhouse/internal/chtype"
	"github.com/gfronza/twinhouse/internal/limits"
)

func init() {
	regCI(stringMap(asciiLower, true), "lower", "lcase")
	regCI(stringMap(asciiUpper, true), "upper", "ucase")
	reg(stringMap(strings.ToLower, false), "lowerUTF8")
	reg(stringMap(strings.ToUpper, false), "upperUTF8")
	reg(stringMap(reverseBytes, true), "reverse")
	reg(buildToFixedString, "toFixedString")
	regCI(buildLength, "length")
	regCI(buildLengthUTF8, "lengthUTF8", "char_length", "character_length")
	reg(emptyBuilder(true), "empty")
	reg(emptyBuilder(false), "notEmpty")
	regCI(buildConcat, "concat")
	regCI(substringBuilder(false), "substring", "substr", "mid")
	reg(substringBuilder(true), "substringUTF8")
	reg(affixBuilder(true), "startsWith")
	reg(affixBuilder(false), "endsWith")
	regCI(positionBuilder(false), "position")
	regCI(func(c *compiler, name string, args []*expr) (*expr, error) {
		// locate(needle, haystack[, start]) uses MySQL argument order.
		if len(args) >= 2 {
			args = append([]*expr{args[1], args[0]}, args[2:]...)
		}
		return positionBuilder(false)(c, name, args)
	}, "locate")
	reg(positionBuilder(true), "positionCaseInsensitive")
	regCI(replaceBuilder(true), "replaceAll", "replace")
	reg(replaceBuilder(false), "replaceOne")
	reg(regexReplaceBuilder(true), "replaceRegexpAll")
	reg(regexReplaceBuilder(false), "replaceRegexpOne")
	reg(buildSplitByChar, "splitByChar")
	reg(buildSplitByString, "splitByString")
	regCI(trimBuilder(true, true), "trim", "trimBoth")
	regCI(trimBuilder(true, false), "ltrim", "trimLeft")
	regCI(trimBuilder(false, true), "rtrim", "trimRight")
	reg(likeBuilder(false, false), "like")
	reg(likeBuilder(false, true), "notLike")
	reg(likeBuilder(true, false), "ilike")
	reg(likeBuilder(true, true), "notILike")
	reg(buildMatch, "match")
}

func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return string(b)
}

func asciiUpper(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		}
	}
	return string(b)
}

func reverseBytes(s string) string {
	b := []byte(s)
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
	return string(b)
}

func requireString(name string, args []*expr, positions ...int) error {
	for _, p := range positions {
		if p < len(args) && !isStringKind(base(args[p]).Kind) && !isNothing(args[p].t) {
			return illegalType(name, args[p].t, p+1)
		}
	}
	return nil
}

// stringMap builds a String -> String function. Length-preserving maps
// keep FixedString arguments FixedString, like ClickHouse.
func stringMap(f func(string) string, keepsLength bool) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 1, 1); err != nil {
			return nil, err
		}
		if err := requireString(name, args, 0); err != nil {
			return nil, err
		}
		rt := chtype.TString
		if t := base(args[0]); keepsLength && t.Kind == chtype.FixedString {
			rt = t
		}
		return strict(args, rt, func(v []any) (any, error) { return f(v[0].(string)), nil }), nil
	}
}

func buildToFixedString(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 2, 2); err != nil {
		return nil, err
	}
	if err := requireString(name, args, 0); err != nil {
		return nil, err
	}
	n, err := fixedStringSize(name, args[1])
	if err != nil {
		return nil, err
	}
	return strict(args[:1], chtype.Type{Kind: chtype.FixedString, N: n}, func(v []any) (any, error) {
		s := v[0].(string)
		if len(s) > n {
			return nil, cherr.New(cherr.BadArguments, "String too long for type FixedString(%d)", n)
		}
		return s + strings.Repeat("\x00", n-len(s)), nil
	}), nil
}

// fixedStringSize reads the N of toFixedString(s, N), which must be a
// positive constant no larger than limits.MaxFixedStringSize.
func fixedStringSize(name string, a *expr) (int, error) {
	if !a.konst || !isIntKind(base(a).Kind) {
		return 0, errFixedStringSize(name)
	}
	if u, ok := a.val.(uint64); ok && u > limits.MaxFixedStringSize || asInt(a.val) > limits.MaxFixedStringSize {
		return 0, cherr.New(cherr.ArgumentOutOfBound, "FixedString size is too large")
	}
	if asInt(a.val) <= 0 {
		return 0, errFixedStringSize(name)
	}
	return int(asInt(a.val)), nil
}

func errFixedStringSize(name string) error {
	return cherr.New(cherr.IllegalColumn, "Second argument of function %s must be a positive constant integer", name)
}

// checkResultSize rejects n bytes that an argument value would make a
// function build, when n is over limits.MaxFunctionResultBytes. It runs
// before they are built.
func checkResultSize(name string, n uint64) error {
	if n > limits.MaxFunctionResultBytes {
		return cherr.New(cherr.TooLargeStringSize, "Too large string size (%d) in function %s, maximum is: %d",
			n, name, limits.MaxFunctionResultBytes)
	}
	return nil
}

// satAdd returns a+b, or MaxUint64 if that overflows.
func satAdd(a, b uint64) uint64 {
	if s, carry := bits.Add64(a, b, 0); carry == 0 {
		return s
	}
	return math.MaxUint64
}

// satMul returns a*b, or MaxUint64 if that overflows.
func satMul(a, b uint64) uint64 {
	if hi, lo := bits.Mul64(a, b); hi == 0 {
		return lo
	}
	return math.MaxUint64
}

func buildLength(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 1); err != nil {
		return nil, err
	}
	k := base(args[0]).Kind
	if !isStringKind(k) && k != chtype.Array && k != chtype.Map && !isNothing(args[0].t) {
		return nil, illegalType(name, args[0].t, 1)
	}
	return strict(args, chtype.TUInt64, func(v []any) (any, error) {
		switch x := v[0].(type) {
		case string:
			return uint64(len(x)), nil
		case []any:
			return uint64(len(x)), nil
		}
		return uint64(0), nil
	}), nil
}

func buildLengthUTF8(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 1, 1); err != nil {
		return nil, err
	}
	if err := requireString(name, args, 0); err != nil {
		return nil, err
	}
	return strict(args, chtype.TUInt64, func(v []any) (any, error) {
		return uint64(utf8.RuneCountInString(v[0].(string))), nil
	}), nil
}

func emptyBuilder(empty bool) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 1, 1); err != nil {
			return nil, err
		}
		k := base(args[0]).Kind
		if !isStringKind(k) && k != chtype.Array && k != chtype.Map && k != chtype.UUID && !isNothing(args[0].t) {
			return nil, illegalType(name, args[0].t, 1)
		}
		return strict(args, chtype.TUInt8, func(v []any) (any, error) {
			var n int
			switch x := v[0].(type) {
			case string:
				if k == chtype.UUID && x == "00000000-0000-0000-0000-000000000000" {
					x = ""
				}
				n = len(x)
			case []any:
				n = len(x)
			}
			return boolNum((n == 0) == empty), nil
		}), nil
	}
}

func buildConcat(c *compiler, name string, args []*expr) (*expr, error) {
	if len(args) == 0 {
		return constant(chtype.TString, ""), nil
	}
	types := make([]chtype.Type, len(args))
	for i, a := range args {
		types[i] = base(a)
	}
	return strict(args, chtype.TString, func(v []any) (any, error) {
		// The parts are rendered first so Join can size the result
		// exactly: a row holding it holds only its bytes.
		var buf [8]string
		parts := buf[:0]
		for i, x := range v {
			s, ok := x.(string)
			if !ok || !isStringKind(types[i].Kind) {
				s = valueToString(c.q, x, types[i])
			}
			parts = append(parts, s)
		}
		return strings.Join(parts, ""), nil
	}), nil
}

func substringBuilder(utf bool) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 2, 3); err != nil {
			return nil, err
		}
		if err := requireString(name, args, 0); err != nil {
			return nil, err
		}
		for i := 1; i < len(args); i++ {
			if !isIntKind(base(args[i]).Kind) {
				return nil, illegalType(name, args[i].t, i+1)
			}
		}
		return strict(args, chtype.TString, func(v []any) (any, error) {
			s := v[0].(string)
			var units []string
			n := len(s)
			if utf {
				for _, r := range s {
					units = append(units, string(r))
				}
				n = len(units)
			}
			start, end := substringBounds(n, asInt(v[1]), v)
			if start >= end {
				return "", nil
			}
			if utf {
				return strings.Join(units[start:end], ""), nil
			}
			return s[start:end], nil
		}), nil
	}
}

// substringBounds implements ClickHouse's substring offset/length rules:
// the window [start, start+len) is computed first and then clipped.
func substringBounds(n int, off int64, v []any) (int, int) {
	var start int64
	switch {
	case off > 0:
		start = off - 1
	case off < 0:
		start = int64(n) + off
	default:
		return 0, 0
	}
	end := int64(n)
	if len(v) == 3 {
		if l := asInt(v[2]); l >= 0 {
			end = start + l
		} else {
			end = int64(n) + l
		}
	}
	start = max(0, min(start, int64(n)))
	end = max(0, min(end, int64(n)))
	if end <= start {
		return 0, 0
	}
	return int(start), int(end)
}

func affixBuilder(prefix bool) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 2, 2); err != nil {
			return nil, err
		}
		if err := requireString(name, args, 0, 1); err != nil {
			return nil, err
		}
		return strict(args, chtype.TUInt8, func(v []any) (any, error) {
			s, a := v[0].(string), v[1].(string)
			if prefix {
				return boolNum(strings.HasPrefix(s, a)), nil
			}
			return boolNum(strings.HasSuffix(s, a)), nil
		}), nil
	}
}

func positionBuilder(ci bool) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 2, 3); err != nil {
			return nil, err
		}
		if err := requireString(name, args, 0, 1); err != nil {
			return nil, err
		}
		return strict(args, chtype.TUInt64, func(v []any) (any, error) {
			h, n := v[0].(string), v[1].(string)
			if ci {
				h, n = asciiLower(h), asciiLower(n)
			}
			from := 0
			if len(v) == 3 {
				s := asInt(v[2])
				if s < 1 {
					s = 1
				}
				from = int(s - 1)
				if from > len(h) {
					return uint64(0), nil
				}
			}
			i := strings.Index(h[from:], n)
			if i < 0 {
				return uint64(0), nil
			}
			return uint64(from + i + 1), nil
		}), nil
	}
}

func replaceBuilder(all bool) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 3, 3); err != nil {
			return nil, err
		}
		if err := requireString(name, args, 0, 1, 2); err != nil {
			return nil, err
		}
		return strict(args, chtype.TString, func(v []any) (any, error) {
			s, from, to := v[0].(string), v[1].(string), v[2].(string)
			if from == "" {
				return s, nil
			}
			n := 1
			if all {
				n = -1
			}
			if len(to) > len(from) {
				count := strings.Count(s, from)
				if !all {
					count = min(count, 1)
				}
				grow := satMul(uint64(count), uint64(len(to)-len(from)))
				if err := checkResultSize(name, grow); err != nil {
					return nil, err
				}
			}
			return strings.Replace(s, from, to, n), nil
		}), nil
	}
}

// maxRegexpInsts caps the estimated program size of one regexp. RE2's
// default max_mem of 8 MiB allows about 87k instructions; Go uses about
// 130 bytes per instruction while compiling, so one compile stays near
// 13 MB.
const maxRegexpInsts = 100_000

// compileRegexp compiles p and returns its estimated program size. A
// pattern that does not compile, or whose program would pass
// maxRegexpInsts, is an error with the given code.
func compileRegexp(p string, code int) (*regexp.Regexp, int, error) {
	tree, err := syntax.Parse(p, syntax.Perl)
	if err != nil {
		return nil, 0, cherr.Wrap(code, err, "cannot compile regular expression '%s'", p)
	}
	size := progSize(tree)
	if size > maxRegexpInsts {
		return nil, 0, cherr.New(code, "cannot compile regular expression '%s': pattern too large - compile failed", p)
	}
	re, err := regexp.Compile(p)
	if err != nil {
		return nil, 0, cherr.Wrap(code, err, "cannot compile regular expression '%s'", p)
	}
	return re, size, nil
}

// progSize estimates how many instructions re compiles to. The parser
// bounds the tree's height and its repeat counts, so neither the recursion
// nor the products can grow large.
func progSize(re *syntax.Regexp) int {
	switch re.Op {
	case syntax.OpLiteral:
		return len(re.Rune)
	case syntax.OpConcat, syntax.OpAlternate:
		n := 0
		for _, sub := range re.Sub {
			n += progSize(sub)
		}
		return n
	case syntax.OpRepeat:
		if re.Max < 0 {
			return progSize(re.Sub[0]) * (re.Min + 1)
		}
		return progSize(re.Sub[0]) * re.Max
	}
	n := 1
	for _, sub := range re.Sub {
		n += progSize(sub)
	}
	return n
}

// patternArg returns how to get the regexp for each value of a pattern
// argument, after conv; code is the error for a pattern that does not
// compile. A constant pattern is compiled once here; an error is still
// reported per row. Other patterns go through the shared cache.
func patternArg(a *expr, conv func(string) string, code int) func(string) (*regexp.Regexp, error) {
	if p, ok := a.val.(string); ok && a.konst {
		re, _, err := compileRegexp(conv(p), code)
		return func(string) (*regexp.Regexp, error) { return re, err }
	}
	return func(p string) (*regexp.Regexp, error) { return regexps.get(conv(p), code) }
}

func asIs(s string) string { return s }

const (
	// maxCachedPatternBytes is the longest pattern the regexp cache keeps.
	maxCachedPatternBytes = 4 << 10
	// maxCachedRegexpInsts caps the estimated program size of all cached
	// regexps together (about 50 MB).
	maxCachedRegexpInsts = 1_000_000
)

// regexpCache holds regexps compiled from non-constant patterns. It is
// emptied when one more entry would pass limits.MaxRegexpCacheEntries
// entries or maxCachedRegexpInsts instructions. Patterns longer than
// maxCachedPatternBytes are compiled for their row only.
type regexpCache struct {
	mu    sync.RWMutex
	m     map[string]*regexp.Regexp
	insts int
}

var regexps regexpCache

func (c *regexpCache) get(p string, code int) (*regexp.Regexp, error) {
	if len(p) > maxCachedPatternBytes {
		re, _, err := compileRegexp(p, code)
		return re, err
	}
	c.mu.RLock()
	re, ok := c.m[p]
	c.mu.RUnlock()
	if ok {
		return re, nil
	}
	re, size, err := compileRegexp(p, code)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.m[p]; ok {
		return old, nil
	}
	if len(c.m) >= limits.MaxRegexpCacheEntries || c.insts+size > maxCachedRegexpInsts {
		c.m, c.insts = nil, 0
	}
	if c.m == nil {
		c.m = make(map[string]*regexp.Regexp)
	}
	c.m[p] = re
	c.insts += size
	return re, nil
}

// chReplacement converts ClickHouse's \N back-references to Go's ${N}.
func chReplacement(r string) string {
	var b strings.Builder
	for i := 0; i < len(r); i++ {
		if r[i] == '\\' && i+1 < len(r) {
			n := r[i+1]
			if n >= '0' && n <= '9' {
				b.WriteString("${" + string(n) + "}")
				i++
				continue
			}
			b.WriteByte(n)
			i++
			continue
		}
		if r[i] == '$' {
			b.WriteString("$$")
			continue
		}
		b.WriteByte(r[i])
	}
	return b.String()
}

func regexReplaceBuilder(all bool) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 3, 3); err != nil {
			return nil, err
		}
		if err := requireString(name, args, 0, 1, 2); err != nil {
			return nil, err
		}
		pattern := patternArg(args[1], asIs, cherr.BadArguments)
		return strict(args, chtype.TString, func(v []any) (any, error) {
			re, err := pattern(v[1].(string))
			if err != nil {
				return nil, err
			}
			if all {
				return replaceRegexpAll(name, re, v[0].(string), v[2].(string))
			}
			return replaceRegexpOne(name, re, v[0].(string), v[2].(string))
		}), nil
	}
}

// replacementShape returns the literal bytes of a ClickHouse replacement
// string and how many \N group references it has.
func replacementShape(r string) (lit, refs uint64) {
	for i := 0; i < len(r); i++ {
		if r[i] == '\\' && i+1 < len(r) {
			i++
			if r[i] >= '0' && r[i] <= '9' {
				refs++
				continue
			}
		}
		lit++
	}
	return lit, refs
}

// replacementGrowth is the most bytes that replacing matches matched bytes
// adds to the input: a group is never longer than its match, so each
// reference adds at most the matched bytes.
func replacementGrowth(matches, matched, lit, refs uint64) uint64 {
	added := satAdd(satMul(matches, lit), satMul(refs, matched))
	if added <= matched {
		return 0
	}
	return added - matched
}

// replaceRegexpAll replaces every match of re in s. When the growth could
// pass the cap (at most len(s)+1 matches of at most len(s) bytes in all),
// the matches are counted first.
func replaceRegexpAll(name string, re *regexp.Regexp, s, r string) (string, error) {
	lit, refs := replacementShape(r)
	n := uint64(len(s))
	if replacementGrowth(n+1, n, lit, refs) > limits.MaxFunctionResultBytes {
		matches, matched := countMatches(re, s)
		if err := checkResultSize(name, replacementGrowth(matches, matched, lit, refs)); err != nil {
			return "", err
		}
	}
	return re.ReplaceAllString(s, chReplacement(r)), nil
}

// countMatches returns how many matches of re s has and their total bytes.
func countMatches(re *regexp.Regexp, s string) (matches, matched uint64) {
	re.ReplaceAllStringFunc(s, func(m string) string {
		matches++
		matched += uint64(len(m))
		return ""
	})
	return matches, matched
}

func replaceRegexpOne(name string, re *regexp.Regexp, s, r string) (string, error) {
	loc := re.FindStringSubmatchIndex(s)
	if loc == nil {
		return s, nil
	}
	lit, refs := replacementShape(r)
	m := uint64(loc[1] - loc[0])
	if err := checkResultSize(name, replacementGrowth(1, m, lit, refs)); err != nil {
		return "", err
	}
	out := re.ExpandString(nil, chReplacement(r), s, loc)
	return s[:loc[0]] + string(out) + s[loc[1]:], nil
}

func buildSplitByChar(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 2, 3); err != nil {
		return nil, err
	}
	if err := requireString(name, args, 0, 1); err != nil {
		return nil, err
	}
	if !args[0].konst {
		return nil, cherr.New(cherr.IllegalColumn, "Illegal column of first argument of function %s, must be constant string", name)
	}
	if sep, _ := args[0].val.(string); len(sep) != 1 {
		return nil, cherr.New(cherr.BadArguments, "Illegal separator for function %s. Must be exactly one byte", name)
	}
	return splitStrings(args), nil
}

func buildSplitByString(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 2, 3); err != nil {
		return nil, err
	}
	if err := requireString(name, args, 0, 1); err != nil {
		return nil, err
	}
	return splitStrings(args), nil
}

func splitStrings(args []*expr) *expr {
	return strict(args[:2], chtype.NewArray(chtype.TString), func(v []any) (any, error) {
		sep, s := v[0].(string), v[1].(string)
		var parts []string
		if sep == "" {
			for _, r := range s {
				parts = append(parts, string(r))
			}
		} else {
			parts = strings.Split(s, sep)
		}
		out := make([]any, len(parts))
		for i, p := range parts {
			out[i] = p
		}
		return out, nil
	})
}

// trimBuilder implements trimBoth/trimLeft/trimRight(s[, chars]); the
// default trims spaces.
func trimBuilder(left, right bool) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 1, 2); err != nil {
			return nil, err
		}
		if err := requireString(name, args, 0, 1); err != nil {
			return nil, err
		}
		return strict(args, chtype.TString, func(v []any) (any, error) {
			s, cut := v[0].(string), " "
			if len(v) == 2 {
				cut = v[1].(string)
			}
			if left {
				s = strings.TrimLeft(s, cut)
			}
			if right {
				s = strings.TrimRight(s, cut)
			}
			return s, nil
		}), nil
	}
}

// likePattern converts a LIKE pattern to an anchored RE2 expression.
func likePattern(p string, ci bool) string {
	var b strings.Builder
	b.WriteString("(?s)")
	if ci {
		b.WriteString("(?i)")
	}
	b.WriteString("^")
	for i := 0; i < len(p); i++ {
		ch := p[i]
		switch ch {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		case '\\':
			if i+1 < len(p) {
				i++
				b.WriteString(regexp.QuoteMeta(string(p[i])))
			} else {
				b.WriteString(`\\`)
			}
		default:
			r, size := utf8.DecodeRuneInString(p[i:])
			b.WriteString(regexp.QuoteMeta(string(r)))
			i += size - 1
		}
	}
	b.WriteString("$")
	return b.String()
}

func likeBuilder(ci, negate bool) builder {
	return func(c *compiler, name string, args []*expr) (*expr, error) {
		if err := wantArgs(name, args, 2, 2); err != nil {
			return nil, err
		}
		if err := requireString(name, args, 0, 1); err != nil {
			return nil, err
		}
		pattern := patternArg(args[1], func(p string) string { return likePattern(p, ci) }, cherr.CannotCompileRegexp)
		return strict(args, chtype.TUInt8, func(v []any) (any, error) {
			re, err := pattern(v[1].(string))
			if err != nil {
				return nil, err
			}
			return boolNum(re.MatchString(v[0].(string)) != negate), nil
		}), nil
	}
}

func buildMatch(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 2, 2); err != nil {
		return nil, err
	}
	if err := requireString(name, args, 0, 1); err != nil {
		return nil, err
	}
	pattern := patternArg(args[1], asIs, cherr.CannotCompileRegexp)
	return strict(args, chtype.TUInt8, func(v []any) (any, error) {
		re, err := pattern(v[1].(string))
		if err != nil {
			return nil, err
		}
		return boolNum(re.MatchString(v[0].(string))), nil
	}), nil
}
