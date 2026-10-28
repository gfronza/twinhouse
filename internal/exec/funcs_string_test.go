package exec

import "testing"

// Cuts that are copied out of their source (or kept as views of it) give
// the values ClickHouse 25.8 gives.
func TestStringPiecesKeepTheirValues(t *testing.T) {
	te := newTestEnv(t)
	te.expectRows(`SELECT replaceRegexpOne('abxxxxx', 'x+$', ''), replaceRegexpOne('xxab', '^x+', ''), `+
		`replaceRegexpOne('xxx', 'x+', ''), replaceRegexpOne('abc', '^', '')`, "ab\tab\t\tabc")
	te.expectRows(`SELECT trimRight(concat(repeat('y', 70), ' ')) = repeat('y', 70), `+
		`substring(repeat('ab', 40), 2) = concat('b', repeat('ab', 39))`, "1\t1")
	te.expectRows(`SELECT toString(toFixedString('ab', 4)) = 'ab', arrayStringConcat([toFixedString('ab', 4), NULL]), `+
		`length(arrayStringConcat([toFixedString('ab', 4)])), toFixedString('ab', 2) IN ('ab'), toFixedString('ab', 3) IN ('ab')`,
		"1\tab\t2\t1\t1")
	te.expectRows(`SELECT lowerUTF8('\xE2\x84\xAA') = 'k', upperUTF8('ı') = 'I', `+
		`lowerUTF8(concat(repeat('\xE2\x84\xAA', 100), 'Ä')) = concat(repeat('k', 100), 'ä'), `+
		`upperUTF8(repeat('ȿ', 40)) = repeat('Ȿ', 40)`, "1\t1\t1\t1")
}
