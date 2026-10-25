from ch import *
ts = r"""string
uint8
UINT8
datetime
DATETIME
datetime64
DateTime64
datetime64(3)
nullable(String)
array(String)
tuple(UInt8)
map(String,UInt8)
lowcardinality(String)
bool
boolean
BOOL
date
DATE
date32
uuid
float32
FLOAT32
enum8('a'=1)
fixedstring(2)
Nothing
nothing
int64
INT64
Enum('a', 'b')
Enum8('a', 'b')
Enum16('a', 'b'=5, 'c')
Enum('a'=300)
Enum('a'=1,'a'=2)
Enum('a'=1,'b'=1)
Nullable(Array(String))
Nullable(Nullable(String))
Nullable(LowCardinality(String))
LowCardinality(UInt8)
LowCardinality(Date)
LowCardinality(UUID)
LowCardinality(Bool)
LowCardinality(Float64)
Array(Nullable(Nothing))
Nullable(Nothing)
Tuple()
DateTime64(10)
DateTime64(0)
DateTime64()
FixedString(0)
Map(Nullable(String), UInt8)
Map(LowCardinality(String), UInt8)
Map(Array(UInt8), UInt8)
Map(Float64, UInt8)
Map(UUID, UInt8)
Map(Date, UInt8)
Nullable(Tuple(UInt8))
Nullable(Map(String,String))
LowCardinality(Array(String))
SimpleAggregateFunction(sum, Float64)
SimpleAggregateFunction(uniqExact, String)
SimpleAggregateFunction(max, Nullable(String))
AggregateFunction(uniq, String)
AggregateFunction(sumIf, UInt64, UInt8)
AggregateFunction(foo, UInt8)
AggregateFunction(sum)
AggregateFunction(1, UInt8)
AggregateFunction(quantile(0.5,0.9), Float64)
AggregateFunction(quantile('x'), Float64)
AggregateFunction(sumMap, Array(UInt8), Array(UInt64))
Object
JSON
INT(11)
BIGINT(20) UNSIGNED
VARCHAR
CHARACTER VARYING(10)
NATIONAL CHAR
Int
TIMESTAMP(3)
DATETIME(3)
UTINYINT
BOOLEAN
Bit
DateTime64(3,'Foo/Bar')
DateTime('Foo/Bar')
DateTime64(3, UTC)
DateTime64(3, 'utc')
DateTime('Asia/Kolkata')
DateTime( 'UTC' )
Tuple(a UInt8, a String)
Tuple(a UInt8, String)
Tuple(`a b` UInt8, `c` String)
Tuple(UInt8 String)
Tuple(1 UInt8)
Array( String )
Array(String
Array(String))
 String 
Int8 Int8
Decimal(10,2)
Float
Double
DOUBLE
INTEGER
SIGNED
UNSIGNED
INT UNSIGNED
INT SIGNED
TINYINT UNSIGNED
LONGBLOB
BINARY
VARBINARY(10)
CHAR(3)
NCHAR(3)
BYTEA
Int128
UInt256
Nested(a UInt8)"""
for t in ts.split("\n"):
    r = q("SELECT toTypeName(CAST(defaultValueOfTypeName(%s), %s))" % (lit(t), lit(t)))
    print(repr(t).ljust(48), r.strip()[:170])
