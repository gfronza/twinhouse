package exec

import (
	"regexp"
	"strings"
	"sync"
	"unicode/utf8"

	"dynamodb-eventstream/internal/cherr"
	"dynamodb-eventstream/internal/chtype"
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
	if !args[1].konst || !isIntKind(base(args[1]).Kind) || asInt(args[1].val) <= 0 {
		return nil, cherr.New(cherr.IllegalColumn, "Second argument of function %s must be a positive constant integer", name)
	}
	n := int(asInt(args[1].val))
	return strict(args[:1], chtype.Type{Kind: chtype.FixedString, N: n}, func(v []any) (any, error) {
		s := v[0].(string)
		if len(s) > n {
			return nil, cherr.New(cherr.BadArguments, "String too long for type FixedString(%d)", n)
		}
		return s + strings.Repeat("\x00", n-len(s)), nil
	}), nil
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
		var b strings.Builder
		for i, x := range v {
			if s, ok := x.(string); ok && isStringKind(types[i].Kind) {
				b.WriteString(s)
				continue
			}
			b.WriteString(valueToString(c.q, x, types[i]))
		}
		return b.String(), nil
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
			if all {
				return strings.ReplaceAll(s, from, to), nil
			}
			return strings.Replace(s, from, to, 1), nil
		}), nil
	}
}

var reCache sync.Map // pattern -> *regexp.Regexp

func compileRegexp(p string) (*regexp.Regexp, error) {
	if re, ok := reCache.Load(p); ok {
		return re.(*regexp.Regexp), nil
	}
	re, err := regexp.Compile(p)
	if err != nil {
		return nil, cherr.Wrap(cherr.BadArguments, err, "cannot compile regular expression '%s'", p)
	}
	reCache.Store(p, re)
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
		return strict(args, chtype.TString, func(v []any) (any, error) {
			re, err := compileRegexp(v[1].(string))
			if err != nil {
				return nil, err
			}
			repl := chReplacement(v[2].(string))
			s := v[0].(string)
			if all {
				return re.ReplaceAllString(s, repl), nil
			}
			loc := re.FindStringSubmatchIndex(s)
			if loc == nil {
				return s, nil
			}
			out := re.ExpandString(nil, repl, s, loc)
			return s[:loc[0]] + string(out) + s[loc[1]:], nil
		}), nil
	}
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
	return splitBuilder(c, name, args)
}

func buildSplitByString(c *compiler, name string, args []*expr) (*expr, error) {
	if err := wantArgs(name, args, 2, 3); err != nil {
		return nil, err
	}
	if err := requireString(name, args, 0, 1); err != nil {
		return nil, err
	}
	return splitBuilder(c, name, args)
}

func splitBuilder(c *compiler, name string, args []*expr) (*expr, error) {
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
	}), nil
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
		return strict(args, chtype.TUInt8, func(v []any) (any, error) {
			re, err := compileRegexp(likePattern(v[1].(string), ci))
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
	return strict(args, chtype.TUInt8, func(v []any) (any, error) {
		re, err := compileRegexp(v[1].(string))
		if err != nil {
			return nil, err
		}
		return boolNum(re.MatchString(v[0].(string))), nil
	}), nil
}
