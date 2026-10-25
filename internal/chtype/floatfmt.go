package chtype

import (
	"math"
	"strconv"
	"strings"
)

// formatFloat renders f the way ClickHouse prints Float32/Float64 in text
// formats: the shortest decimal that round-trips at the given bit size,
// laid out like ECMAScript Number#toString (plain notation for decimal
// exponents in (-7, 21], scientific otherwise) but without a '+' in
// positive exponents: 1e21, 1e-7, 0.000001, 123456789.123, -0, inf, nan.
func formatFloat(f float64, bits int) string {
	return string(appendFloat(nil, f, bits))
}

func appendFloat(dst []byte, f float64, bits int) []byte {
	switch {
	case math.IsNaN(f):
		return append(dst, "nan"...)
	case math.IsInf(f, 1):
		return append(dst, "inf"...)
	case math.IsInf(f, -1):
		return append(dst, "-inf"...)
	case f == 0:
		if math.Signbit(f) {
			return append(dst, "-0"...)
		}
		return append(dst, '0')
	}
	if bits == 32 {
		f = float64(float32(f))
	}
	var buf [32]byte
	sci := strconv.AppendFloat(buf[:0], f, 'e', -1, bits)
	// sci looks like "-d.ddde-XX" / "de+XX".
	neg := sci[0] == '-'
	if neg {
		sci = sci[1:]
	}
	e := strings.IndexByte(string(sci), 'e')
	mant, expPart := sci[:e], sci[e+1:]
	exp, _ := strconv.Atoi(string(expPart))
	digits := make([]byte, 0, len(mant))
	for _, c := range mant {
		if c != '.' {
			digits = append(digits, c)
		}
	}
	k := len(digits)
	n := exp + 1 // decimal point position relative to the digit string
	if neg {
		dst = append(dst, '-')
	}
	switch {
	case k <= n && n <= 21:
		dst = append(dst, digits...)
		for i := k; i < n; i++ {
			dst = append(dst, '0')
		}
	case 0 < n && n <= 21:
		dst = append(dst, digits[:n]...)
		dst = append(dst, '.')
		dst = append(dst, digits[n:]...)
	case -6 < n && n <= 0:
		dst = append(dst, '0', '.')
		for i := 0; i < -n; i++ {
			dst = append(dst, '0')
		}
		dst = append(dst, digits...)
	default:
		dst = append(dst, digits[0])
		if k > 1 {
			dst = append(dst, '.')
			dst = append(dst, digits[1:]...)
		}
		dst = append(dst, 'e')
		dst = strconv.AppendInt(dst, int64(n-1), 10)
	}
	return dst
}
