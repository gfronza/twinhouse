from ch import *
import os
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
from golib import golit
import re
src1 = open("type_cases.py").read(); ts = src1[src1.index('r"""')+4:src1.index('"""\nfor')].split("\n")
extra = r"""MEDIUMINT
YEAR
SET
SINGLE
BINARY(3)
Date(1)
DateTime(3, 'UTC')
TIMESTAMP('UTC')
TIMESTAMP
Bool(1)
CHARACTER LARGE OBJECT
NVARCHAR
VARCHAR2
TINYTEXT
CLOB
Int1
int1
Int1 unsigned
integer unsigned
DOUBLE precision
double   precision
UInt8(3)
String(10)
Float64(2)
FLOAT(10)
UUID(1)
mediumint unsigned
smallint unsigned
bigint
Text
Varchar(10)
DateTime64(3,'UTC',1)
DateTime64('UTC')
DateTime64(3, 'Europe/Berlin')
DateTime64(-1)
DateTime64(9)
DateTime64(1, 'Asia/Kolkata')
FixedString(-1)
FixedString('a')
FixedString(1.5)
FixedString(16)
Enum8('a' = 128)
Enum8('a' = -129)
Enum8('a' = -128, 'b' = 127)
Enum16('a' = 32768)
Enum16('a' = -32768, 'b' = 32767)
Enum('a' = 1, 'b')
Enum('a', 'b' = 5)
Enum8('a' = 1.5)
Enum8('' = 1)
Enum8('a' = +1)
Enum8('a'=-1,'b')
Enum8('b' = 2, 'a' = 1)
Enum8('x\\'y' = 1)
Enum8('back\\\\slash' = 1)
Enum()
Nullable(UUID)
Nullable(Enum8('a'=1))
Nullable(DateTime64(3, 'UTC'))
Nullable(FixedString(2))
LowCardinality(Nullable(UUID))
LowCardinality(FixedString(2))
LowCardinality(Nullable(FixedString(2)))
LowCardinality(Enum8('a'=1))
Array(LowCardinality(Nullable(String)))
Tuple(a Tuple(b UInt8, c Array(String)), d Map(String, Array(UInt8)))
Map(String)
Map(String, UInt8, UInt8)
Map(Nullable(String), UInt8)
Map(LowCardinality(String), Nullable(UInt64))
Array(String, UInt8)
Array()
Nullable()
Tuple(a)
Tuple(`a` `UInt8`)
Tuple("a" UInt8)
Tuple(`a``b` UInt8)
Tuple(`a.b` UInt8, `1` UInt8, _x UInt8, `select` UInt8, `a-b` UInt8)
Tuple(a UInt8, b Tuple(c String))
Tuple(UInt8, Tuple(a String))
SimpleAggregateFunction(sum, UInt64, UInt8)
SimpleAggregateFunction(sum)
SimpleAggregateFunction(anyLast, Array(String))
SimpleAggregateFunction(sum, String)
SimpleAggregateFunction(max, String)
SimpleAggregateFunction(SUM, UInt64)
SimpleAggregateFunction(max, LowCardinality(String))
AggregateFunction(count)
AggregateFunction(count, UInt8)
AggregateFunction(uniqExact, String, UInt8)
AggregateFunction(avg, Float64)
AggregateFunction(quantile, Float64)
AggregateFunction(quantile(0.5), Float64)
AggregateFunction(quantile(1), Float64)
AggregateFunction(quantiles(0.5, 0.9), Float64)
AggregateFunction(quantiles(0.50, 0.900), Float64)
AggregateFunction(topK(10), String)
AggregateFunction(uniqExactIf, String, UInt8)
AggregateFunction(sumState, UInt64)
AggregateFunction(groupArray, String)
AggregateFunction(groupArray(10), String)
AggregateFunction(groupUniqArray, String)
AggregateFunction(min, Nullable(String))
AggregateFunction(minIf, String, UInt8)
AggregateFunction(countIf, UInt8)
AggregateFunction(uniq, String, UInt64)
AggregateFunction( sum , UInt64 )
AggregateFunction(SUM, UInt64)
AggregateFunction(Sum, UInt64)
AggregateFunction(anyLast, String)
AggregateFunction(argMin, String, UInt64)
AggregateFunction(argMax, String, DateTime64(9))
AggregateFunction(argMax, Tuple(a UInt8), UInt8)
AggregateFunction(uniqExact, Array(Tuple(String, UInt8)))
Decimal(10,2)
Decimal32(2)
IPv4
IPv6
Int128
UInt256
JSON
Nested(a UInt8)
Variant(UInt8, String)
Dynamic
Point
BFloat16
Object('json')
Foo
foo(1)
UInt8 garbage
Array(UInt8) x
Nullable(String
(UInt8)
,
Array(Nullable(Array(UInt8)))"""
ts = [t for t in ts + extra.split("\n")]
rows = []
for t in ts:
    r = qb("SELECT toTypeName(CAST(defaultValueOfTypeName(%s), %s)) FORMAT TSVRaw" % (lit(t), lit(t)), {"print_pretty_type_names":"0"})
    if r.startswith(b"ERR"):
        code = int(r.split(b".")[0][len("ERR Code: "):]); want = b""
        if code == 48 and b"getDefault" in r:  # Nothing has no default; ask DESCRIBE-free path
            r2 = qb("SELECT toTypeName(CAST(NULL, %s)) FORMAT TSVRaw" % lit("Nullable(%s)" % t))
            code, want = 0, b"Nothing"
    else:
        code = 0; want = r.rstrip(b"\n")
    rows.append((t, want, code))
with open(OUT_DIR + "/golden_types_data_test.go", "w") as f:
    f.write("// Code generated from ClickHouse 25.8 output by a probe script; DO NOT EDIT.\n\npackage chtype\n\n")
    f.write("// typeGolden: ClickHouse's canonical name for each type string, or the error code it raises.\n")
    f.write("var typeGolden = []typeCase{\n")
    for t, want, code in rows:
        f.write("\t{in: %s, want: %s, code: %d},\n" % (golit(t), golit(want), code))
    f.write("}\n")
print(len(rows))
