package exec

import "testing"

// BenchmarkStringPieces cuts strings and sums the lengths of the pieces:
// splitByChar parts of 20k short sources, substring and trim results of
// 200k rows (Short cuts sources of a few bytes; TrimNearWhole and
// SubstringLong keep nearly all of theirs).
func BenchmarkStringPieces(b *testing.B) {
	te := newTestEnv(b)
	for _, c := range []struct{ name, sql string }{
		{"Split", "SELECT sum(length(splitByChar(',', repeat(concat(toString(number), ','), 100)))) FROM numbers(20000)"},
		{"Substring", "SELECT sum(length(substring(repeat(toString(number), 30), 5, 10))) FROM numbers(200000)"},
		{"Trim", "SELECT sum(length(trim(BOTH 'x' FROM concat(repeat('x', 60), toString(number), 'x')))) FROM numbers(200000)"},
		{"Short", "SELECT sum(length(substring(toString(number), 2, 3))) FROM numbers(200000)"},
		{"TrimNearWhole", "SELECT sum(length(trimRight(concat(toString(number), repeat('y', 100), ' ')))) FROM numbers(200000)"},
		{"SubstringLong", "SELECT sum(length(substring(repeat(toString(number), 30), 2))) FROM numbers(200000)"},
	} {
		b.Run(c.name, func(b *testing.B) {
			for b.Loop() {
				te.exec(c.sql)
			}
		})
	}
}

// BenchmarkCaseMapUTF8 maps 200k strings with lowerUTF8 and upperUTF8
// and sums their lengths: ASCII, a same-length mapping ('Ä' to 'ä'), one
// that shrinks (KELVIN SIGN to 'k') and one that grows ('ȿ' to 'Ȿ').
func BenchmarkCaseMapUTF8(b *testing.B) {
	te := newTestEnv(b)
	for _, c := range []struct{ name, sql string }{
		{"ASCII", "SELECT sum(length(lowerUTF8(concat('ABC', toString(number), repeat('X', 20))))) FROM numbers(200000)"},
		{"Same", "SELECT sum(length(lowerUTF8(concat('Ä', toString(number), repeat('Ö', 10))))) FROM numbers(200000)"},
		{"Shrink", "SELECT sum(length(lowerUTF8(concat('\\xE2\\x84\\xAA', toString(number), repeat('Ö', 10))))) FROM numbers(200000)"},
		{"Grow", "SELECT sum(length(upperUTF8(concat(toString(number), repeat('ȿ', 10))))) FROM numbers(200000)"},
	} {
		b.Run(c.name, func(b *testing.B) {
			for b.Loop() {
				te.exec(c.sql)
			}
		})
	}
}
