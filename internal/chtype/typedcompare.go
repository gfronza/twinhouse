package chtype

import "strings"

// Typed comparison. The canonical representation of a few types does not
// carry their ClickHouse order, so comparing the bare values (Compare,
// CompareHint) orders them differently from ClickHouse:
//
//   - UUID values are canonical lower-case text, but ClickHouse compares a
//     UUID as a UInt128 whose high half is the *last* 8 bytes (hex digits
//     19..35 of the dashed form) and whose low half is the first 8 bytes, so
//     00000000-0000-0001-0000-000000000000 < 00000000-0000-0000-0000-000000000001.
//   - Enum8/Enum16 values are element names, but ClickHouse compares them by
//     their numeric value.
//
// compareTyped walks the type next to the value so these orders apply at
// every depth (Array, Tuple, Map entries, Nullable, LowCardinality,
// SimpleAggregateFunction); every other value falls through to
// compareValues, so for types without UUID or Enum it is exactly
// CompareHint.

func compareTyped(t Type, a, b any, hint int) int {
	if hint <= 0 {
		hint = -1
	} else {
		hint = 1
	}
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return hint
	case b == nil:
		return -hint
	}
	u := unwrapType(t)
	switch u.Kind {
	case UUID:
		if x, ok := a.(string); ok {
			if y, ok := b.(string); ok {
				return compareUUIDText(x, y)
			}
		}
	case Enum8, Enum16:
		return compareValues(enumOrderKey(u, a), enumOrderKey(u, b), hint)
	case Array:
		x, xok := a.([]any)
		y, yok := b.([]any)
		if xok && yok && u.Elem != nil {
			for i := 0; i < len(x) && i < len(y); i++ {
				if c := compareTyped(*u.Elem, x[i], y[i], hint); c != 0 {
					return c
				}
			}
			return cmpInt(len(x), len(y))
		}
	case Tuple:
		x, xok := a.([]any)
		y, yok := b.([]any)
		if xok && yok {
			for i := 0; i < len(x) && i < len(y); i++ {
				var c int
				if i < len(u.Fields) {
					c = compareTyped(u.Fields[i].Type, x[i], y[i], hint)
				} else {
					c = compareValues(x[i], y[i], hint)
				}
				if c != 0 {
					return c
				}
			}
			return cmpInt(len(x), len(y))
		}
	case Map:
		x, xok := a.([]any)
		y, yok := b.([]any)
		if xok && yok && u.Key != nil && u.Elem != nil {
			for i := 0; i < len(x) && i < len(y); i++ {
				if c := compareMapEntry(u, x[i], y[i], hint); c != 0 {
					return c
				}
			}
			return cmpInt(len(x), len(y))
		}
	}
	return compareValues(a, b, hint)
}

// compareMapEntry compares two Map entries ([]any{key, value}) as
// Tuple(K, V).
func compareMapEntry(m Type, a, b any, hint int) int {
	x, xok := a.([]any)
	y, yok := b.([]any)
	if !xok || !yok || len(x) != 2 || len(y) != 2 {
		return compareValues(a, b, hint)
	}
	if c := compareTyped(*m.Key, x[0], y[0], hint); c != 0 {
		return c
	}
	return compareTyped(*m.Elem, x[1], y[1], hint)
}

// enumOrderKey maps an enum element name to its numeric value; anything
// else (an unknown name, a value already numeric) is returned unchanged.
func enumOrderKey(t Type, v any) any {
	if s, ok := v.(string); ok {
		for _, ev := range t.EnumValues {
			if ev.Name == s {
				return ev.Value
			}
		}
	}
	return v
}

// isCanonicalUUIDText reports whether s has the dashed 36-character shape
// with lower-case hex digits (the canonical UUID representation).
func isCanonicalUUIDText(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < 36; i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
				return false
			}
		}
	}
	return true
}

// compareUUIDText compares two UUID texts in ClickHouse order: the last 8
// bytes (s[19:36]) first, then the first 8 bytes (s[0:18]). In the canonical
// dashed form the dashes sit at the same offsets in both strings, so a
// bytewise comparison of each half compares the hex digits. Non-canonical
// text (upper case, undashed) is canonicalised first; unparsable text
// compares bytewise.
func compareUUIDText(a, b string) int {
	if !isCanonicalUUIDText(a) || !isCanonicalUUIDText(b) {
		ca, ea := parseUUID(a)
		cb, eb := parseUUID(b)
		if ea != nil || eb != nil {
			return strings.Compare(a, b)
		}
		a, b = ca, cb
	}
	if c := strings.Compare(a[19:], b[19:]); c != 0 {
		return c
	}
	return strings.Compare(a[:18], b[:18])
}

// uuidOrderKey is UUIDOrderKey's implementation.
func uuidOrderKey(s string) string {
	if !isCanonicalUUIDText(s) {
		c, err := parseUUID(s)
		if err != nil {
			return s
		}
		s = c
	}
	var k [32]byte
	j := 0
	for _, part := range [...]string{s[19:23], s[24:36], s[0:8], s[9:13], s[14:18]} {
		j += copy(k[j:], part)
	}
	return string(k[:])
}

// needsOrderKey reports whether values of t (at any depth) compare
// differently under Compare than under ClickHouse.
func needsOrderKey(t Type) bool {
	u := unwrapType(t)
	switch u.Kind {
	case UUID, Enum8, Enum16:
		return true
	case Array:
		return u.Elem != nil && needsOrderKey(*u.Elem)
	case Tuple:
		for _, f := range u.Fields {
			if needsOrderKey(f.Type) {
				return true
			}
		}
	case Map:
		return u.Key != nil && needsOrderKey(*u.Key) || u.Elem != nil && needsOrderKey(*u.Elem)
	}
	return false
}

// orderKey is OrderKey's implementation.
func orderKey(t Type) func(any) any {
	if !needsOrderKey(t) {
		return nil
	}
	u := unwrapType(t)
	ident := func(v any) any { return v }
	sub := func(t Type) func(any) any {
		if k := orderKey(t); k != nil {
			return k
		}
		return ident
	}
	switch u.Kind {
	case UUID:
		return func(v any) any {
			if s, ok := v.(string); ok {
				return uuidOrderKey(s)
			}
			return v
		}
	case Enum8, Enum16:
		return func(v any) any { return enumOrderKey(u, v) }
	case Array:
		ek := sub(*u.Elem)
		return func(v any) any {
			a, ok := v.([]any)
			if !ok {
				return v
			}
			out := make([]any, len(a))
			for i, x := range a {
				out[i] = ek(x)
			}
			return out
		}
	case Tuple:
		keys := make([]func(any) any, len(u.Fields))
		for i, f := range u.Fields {
			keys[i] = sub(f.Type)
		}
		return func(v any) any {
			a, ok := v.([]any)
			if !ok {
				return v
			}
			out := make([]any, len(a))
			for i, x := range a {
				if i < len(keys) {
					x = keys[i](x)
				}
				out[i] = x
			}
			return out
		}
	case Map:
		kk, vk := ident, ident
		if u.Key != nil {
			kk = sub(*u.Key)
		}
		if u.Elem != nil {
			vk = sub(*u.Elem)
		}
		return func(v any) any {
			a, ok := v.([]any)
			if !ok {
				return v
			}
			out := make([]any, len(a))
			for i, e := range a {
				p, ok := e.([]any)
				if !ok || len(p) != 2 {
					out[i] = e
					continue
				}
				out[i] = []any{kk(p[0]), vk(p[1])}
			}
			return out
		}
	}
	return nil
}
