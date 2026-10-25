-- Arithmetic, integer overflow, division and comparison semantics.

-- query
SELECT 7 / 2, -7 / 2, 1 / 3, 10 / 5, toTypeName(10 / 5);

-- query
SELECT 1 / 0, -1 / 0, 0 / 0;

-- query
SELECT intDiv(7, 2), intDiv(-7, 2), intDiv(7, -2), toTypeName(intDiv(-7, 2));

-- error: 153
SELECT intDiv(1, 0);

-- query
SELECT 7 % 3, -7 % 3, 7 % -3, -7 % -3, toTypeName(-7 % 3);

-- query
SELECT 7.5 % 2, -7.5 % 2;

-- error: 153
SELECT 1 % 0;

-- query
SELECT modulo(10, 3), plus(1, 2), minus(5, 7), multiply(3, 4), divide(9, 3);

-- query: uint8_wraps
SELECT toUInt8(255) + toUInt8(1), toTypeName(toUInt8(255) + toUInt8(1));

-- query: uint64_overflow_wraps
SELECT toUInt64(18446744073709551615) + 1, toUInt64(0) - 1;

-- query: int64_overflow_wraps
SELECT toInt64(9223372036854775807) + 1, toInt64(-9223372036854775808) - 1;

-- query: uint_minus_uint_is_signed_wrap
SELECT toUInt32(1) - toUInt32(2), toTypeName(toUInt32(1) - toUInt32(2)), toUInt8(1) - toUInt8(2);

-- query
SELECT toUInt64(10) * toUInt64(3), toInt32(-4) * toUInt8(3), toTypeName(toInt32(-4) * toUInt8(3));

-- query
SELECT toUInt8(300), toInt8(200), toUInt16(-1), toUInt32(-1), toInt64(18446744073709551615);

-- query
SELECT toUInt8('12'), toInt32('-42'), toUInt64('18446744073709551615'), toFloat64('1.25'), toFloat32('0.1');

-- error
SELECT toUInt8('abc');

-- query
SELECT toInt64(1.9), toInt64(-1.9), toUInt8(2.99), toInt32(-0.5);

-- query
SELECT 0.1 + 0.2, 0.3 - 0.1, 1e100, 1e-100, 123456789.125, -0.0, 1e15, 1e16, 1e21, 1e-5;

-- query
SELECT toFloat32(0.1), toFloat32(1) / 3, toFloat64(1) / 3, 2.5e-7, 100000000000000000000.0;

-- query
SELECT round(2.5), round(3.5), round(-2.5), round(1.2345, 2), round(1234.5, -2), floor(-1.5), ceil(-1.5), floor(1.5), ceil(1.1);

-- query
SELECT abs(-5), abs(5), abs(-1.5), toTypeName(abs(toInt8(-5))), abs(toInt64(-9223372036854775807));

-- query
SELECT greatest(1, 2, 3), least(1, 2, 3), greatest(1.5, 2), least(-1, toUInt8(3)), greatest('a', 'b');

-- query
SELECT 1 = 1.0, toUInt8(1) = toInt64(1), toUInt64(18446744073709551615) > toInt64(-1), -1 < toUInt64(0);

-- query
SELECT '10' > '9', 10 > 9, toString(10) < toString(9);

-- query
SELECT 1 = '1', toUInt8(5) = '5', toDate('2024-01-02') = '2024-01-02', toDateTime('2024-01-02 03:04:05') > '2024-01-01 00:00:00';

-- query
SELECT 3 > 2 > 1, (3 > 2) = 1, 2 + 3 * 4, (2 + 3) * 4, 2 - 3 - 4, 100 / 10 / 5;

-- query
SELECT -9223372036854775808, toTypeName(-9223372036854775808), 9223372036854775807, 9223372036854775808, toTypeName(9223372036854775808);

-- query
SELECT isNaN(0 / 0), isFinite(1 / 0), isNaN(1), isFinite(1);

-- query
SELECT number, number % 3, intDiv(number, 3), number / 4 FROM numbers(7) ORDER BY number;

-- query
SELECT sum(number), avg(number), sum(number * number), sum(toInt64(number) - 5) FROM numbers(11);

-- query: sum_uint64_wraps
SELECT sum(toUInt64(18446744073709551615)) FROM numbers(2);

-- query
SELECT avg(x) FROM (SELECT arrayJoin([1, 2]) AS x);

-- query
SELECT avg(number) FROM numbers(0);

-- query
SELECT sum(number), min(number), max(number), count() FROM numbers(0);

-- query
SELECT toInt8(-128) - toInt8(1), toTypeName(toInt8(-128) - toInt8(1)), toInt16(32767) + toInt16(1);

-- unsupported: bit functions are outside the DESIGN §3 function list
SELECT bitAnd(12, 10), bitOr(12, 10), bitXor(12, 10), bitShiftLeft(1, 10), bitShiftRight(1024, 3);

-- query
SELECT 5 DIV 2, 5 MOD 2;

-- unsupported: math functions beyond round/floor/ceil/abs are outside the DESIGN §3 list
SELECT pow(2, 10), power(2, 0.5), sqrt(16), exp(0), log(1), log10(1000), log2(8);
