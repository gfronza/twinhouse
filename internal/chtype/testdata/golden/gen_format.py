from ch import *
import os
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
from golib import golit
exprs = r"""toUInt8(0)
toUInt8(255)
toUInt16(65535)
toUInt32(4294967295)
toUInt64(18446744073709551615)
toUInt64(0)
toInt8(-128)
toInt8(127)
toInt16(-32768)
toInt32(-2147483648)
toInt64(-9223372036854775808)
toInt64(9223372036854775807)
toInt64(-1)
true
false
toFloat64(0)
toFloat64(-0.)
toFloat64(1)
toFloat64(0.1)
toFloat64(1e-7)
toFloat64(1.5e-7)
toFloat64(1e-6)
toFloat64(0.000001234)
toFloat64(1e21)
toFloat64(1e20)
toFloat64(123456789.123)
toFloat64(1/3)
toFloat64(0.1+0.2)
toFloat64(9007199254740993)
toFloat64(12345678901234567890)
toFloat64(1.7976931348623157e308)
toFloat64(2.2250738585072014e-308)
toFloat64(-1.5e300)
toFloat64(1e100)
toFloat64(123e-20)
toFloat64(inf)
toFloat64(-inf)
toFloat64(nan)
toFloat32(0.1)
toFloat32(1/3)
toFloat32(123456789.123)
toFloat32(16777217)
toFloat32(1e21)
toFloat32(1.2e-6)
toFloat32(3.4e38)
toFloat32(-inf)
toFloat32(nan)
toFloat32(-0.)
''
'hello'
'a\tb\nc\\d\'e"f'
'\0\x01\x7f\b\f\r\v\a\x1b\x1f'
'привет'
'\xff\xfe'
'a/b  <>&'
'a,b'
' lead '
'\\N'
'NULL'
'{"json": [1]}'
toFixedString('ab', 4)
toFixedString('', 2)
toFixedString('a\'b', 3)
toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')
toUUID('00000000-0000-0000-0000-000000000000')
toDate('2024-02-29')
toDate('1970-01-01')
toDate('2149-06-06')
toDate32('1900-01-01')
toDate32('2024-07-04')
toDateTime('2024-01-02 03:04:05', 'UTC')
toDateTime('1970-01-01 00:00:00', 'UTC')
toDateTime('2106-02-07 06:28:15', 'UTC')
toDateTime('2024-01-02 03:04:05')
toDateTime('2024-07-02 03:04:05', 'Europe/Berlin')
toDateTime('2024-01-02 03:04:05', 'Asia/Kolkata')
toDateTime('2024-03-10 07:30:00', 'America/New_York')
toDateTime64('2024-01-02 03:04:05.123456789', 9, 'UTC')
toDateTime64('2024-01-02 03:04:05.123456789', 9)
toDateTime64('2024-01-02 03:04:05.000000001', 9, 'UTC')
toDateTime64('2024-01-02 03:04:05.1', 3, 'UTC')
toDateTime64('2024-01-02 03:04:05', 0, 'UTC')
toDateTime64('2024-01-02 03:04:05.12', 2, 'UTC')
toDateTime64('2024-01-02 03:04:05.123456', 6, 'Asia/Kolkata')
toDateTime64('1960-01-02 03:04:05.5', 3, 'UTC')
toDateTime64('1969-12-31 23:59:59.75', 2, 'UTC')
toDateTime64('1900-01-01 00:00:00', 3, 'UTC')
toDateTime64('2262-04-11 23:47:16.854775807', 9, 'UTC')
CAST('b', 'Enum8(\'a\' = 1, \'b\' = 2)')
CAST('it\'s', 'Enum8(\'it\\\'s\' = 1)')
CAST('x', 'Enum16(\'x\' = -1000, \'y\' = 1000)')
[1,2,3]
[]::Array(UInt8)
['a','b\'c','d\te', 'x"y', 'back\\slash', '\n']
[NULL, 1]::Array(Nullable(UInt8))
[[1],[2,3],[]]
[[['deep']]]
[toUInt64(18446744073709551615), 0]
[toInt64(-1), 5]
[toFloat64(1.5), nan, inf, -inf, -0.]
[toFloat32(0.1)]
[true, false]
[toDate('2024-01-01')]
[toDateTime64('2024-01-02 03:04:05.12', 2, 'UTC')]
[toDateTime('2024-01-02 03:04:05', 'Asia/Tokyo')]
[toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')]
[CAST('b', 'Enum8(\'a\' = 1, \'b\' = 2)')]
[toFixedString('ab', 3)]
[CAST(NULL, 'Nullable(String)'), 'z']
['\0\x01\xff']
[CAST('x', 'LowCardinality(String)')]
[CAST(NULL, 'LowCardinality(Nullable(String))'), 'q']
(1, 'a', [1.5])
CAST((1, 'a'), 'Tuple(a UInt8, b String)')
CAST((1, 'x', [2]), 'Tuple(a UInt8, `b c` String, d Array(UInt8))')
(toUInt64(5), toInt64(-5), 'q')
(1, NULL)
('a\nb', 'c\\d', 'e\'f')
('a', (1, 'b'))
CAST(((1, 'n'), 2), 'Tuple(t Tuple(x UInt8, y String), z UInt8)')
[(12,['1','3'])]
[(1,'a'), (2,'b')]
[CAST((1, 'x'), 'Tuple(a UInt8, b String)')]
CAST(tuple(), 'Tuple()')
(toDate('2024-01-01'), toDateTime('2024-01-01 01:02:03', 'UTC'), toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))
(true, nan)
map('a', 1, 'b', 2)
map('k', [1,2])
map()::Map(String, UInt8)
map(1, 'a')
map(toDate('2024-01-01'), 1)
map(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), 1)
map('q"\'', 1)
map(1.5, 1)
map(toUInt64(9), toInt64(-9))
map('a', NULL)::Map(String, Nullable(UInt8))
map('a', (1, 'x'))
map('a', map('b', 1))
[map('a', 1)]
CAST(NULL, 'Nullable(String)')
CAST('x', 'Nullable(String)')
CAST('', 'Nullable(String)')
CAST('\\N', 'Nullable(String)')
CAST(NULL, 'Nullable(UInt64)')
CAST(5, 'Nullable(UInt64)')
CAST(NULL, 'Nullable(Float64)')
CAST(NULL, 'Nullable(DateTime)')
CAST(NULL, 'Nullable(Bool)')
CAST(true, 'Nullable(Bool)')
CAST('lc', 'LowCardinality(String)')
CAST(NULL, 'LowCardinality(Nullable(String))')
CAST('v', 'LowCardinality(Nullable(String))')
CAST(NULL, 'Nullable(Nothing)')
CAST(5, 'SimpleAggregateFunction(sum, UInt64)')
CAST('s', 'SimpleAggregateFunction(anyLast, Nullable(String))')
CAST(NULL, 'SimpleAggregateFunction(anyLast, Nullable(String))')
""".strip().split("\n")
rows = []
for e in exprs:
    sql = "SELECT %s AS x" % e
    tn = qb("SELECT toTypeName(%s) FORMAT TSVRaw" % e, {"print_pretty_type_names": "0"}).rstrip(b"\n")
    if tn.startswith(b"ERR"):
        raise SystemExit("bad expr %s: %s" % (e, tn))
    out = {}
    for f in ["TSV","CSV","JSONEachRow","Values"]:
        out[f] = qb(sql + " FORMAT " + f)
    out["J64"] = qb(sql + " FORMAT JSONEachRow", {"output_format_json_quote_64bit_integers": "1"})
    out["JNT"] = qb(sql + " FORMAT JSONEachRow", {"output_format_json_named_tuples_as_objects": "0"})
    out["JNS"] = qb(sql + " FORMAT JSONEachRow", {"output_format_json_escape_forward_slashes": "0"})
    out["JSON"] = qb(sql + " FORMAT JSON", {"output_format_write_statistics": "0"})
    for k, v in out.items():
        if v.startswith(b"ERR"): raise SystemExit("err %s %s %s" % (e, k, v))
    def strip_json(b):
        assert b.startswith(b'{"x":') and b.endswith(b'}\n'), b
        return b[5:-2]
    tsv = out["TSV"][:-1]; csv = out["CSV"][:-1]
    vals = out["Values"]; assert vals[:1] == b"(" and vals[-1:] == b")"; vals = vals[1:-1]
    j = out["JSON"]
    marker = b'\t\t\t"x": '
    i = j.index(marker) + len(marker); k = j.index(b'\n\t\t}\n\t],', i)
    pretty = j[i:k]
    rows.append((e, tn.decode(), vals, tsv, csv, strip_json(out["JSONEachRow"]), strip_json(out["J64"]), strip_json(out["JNT"]), strip_json(out["JNS"]), pretty))
with open(OUT_DIR + "/golden_format_data_test.go", "w") as f:
    f.write("// Code generated from ClickHouse 25.8 output by a probe script; DO NOT EDIT.\n\npackage chtype\n\n")
    f.write("// formatGolden: SELECT <expr> AS x rendered by ClickHouse in each format.\n")
    f.write("var formatGolden = []formatCase{\n")
    for r in rows:
        f.write("\t{expr: %s, typ: %s, values: %s, tsv: %s, csv: %s, json: %s, json64: %s, jsonNoNamed: %s, jsonNoSlash: %s, jsonPretty: %s},\n" % tuple(golit(x) for x in r))
    f.write("}\n")
print(len(rows), "rows")
