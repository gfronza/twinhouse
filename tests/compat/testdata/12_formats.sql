-- Output format edge cases: float rendering, special values, 64-bit integers
-- in JSON, NULLs, nested types, settings that change formatting.

CREATE TABLE nums (
    id UInt8,
    u64 UInt64,
    i64 Int64,
    i8 Int8,
    f64 Float64,
    f32 Float32,
    n Nullable(Int32),
    b Bool,
    s Nullable(String)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO nums VALUES
    (1, 0, 0, 0, 0, 0, NULL, false, NULL),
    (2, 18446744073709551615, -9223372036854775808, -128, 1e100, 16777216, -2147483648, true, ''),
    (3, 9007199254740993, 9223372036854775807, 127, -1e-100, -1.5, 2147483647, true, 'NULL'),
    (4, 1, -1, -1, 0.1, 0.1, 0, false, '\\N'),
    (5, 12345678901234567, 42, 5, 123456789012345680000, 1e-7, 7, true, 'x,y');

INSERT INTO nums (id, f64, f32) VALUES (6, nan, inf), (7, -inf, -nan), (8, -0., 1e10);

-- query
-- formats: TSVWithNamesAndTypes,TabSeparated,CSVWithNames,JSON,JSONCompact,JSONEachRow,JSONCompactEachRow,JSONStrings,JSONCompactStrings,JSONStringsEachRow,Values,Native
SELECT * FROM nums ORDER BY id;

-- query: json_quote_64bit
-- formats: JSON,JSONCompact,JSONEachRow
-- setting: output_format_json_quote_64bit_integers=1
SELECT id, u64, i64, i8 FROM nums ORDER BY id;

-- query: json_no_quote_64bit
-- formats: JSON,JSONEachRow
-- setting: output_format_json_quote_64bit_integers=0
SELECT id, u64, i64 FROM nums ORDER BY id;

-- query: json_quote_denormals
-- formats: JSONEachRow,JSONCompact
-- setting: output_format_json_quote_denormals=1
SELECT id, f64, f32 FROM nums WHERE id >= 6 ORDER BY id;

-- query: json_quote_denormals_settings_clause
-- formats: JSONEachRow
SELECT inf AS a, -inf AS b, nan AS c SETTINGS output_format_json_quote_denormals = 1;

-- query: json_quote_64bit_settings_clause
-- formats: JSONEachRow
SELECT toUInt64(1) AS a, toInt64(-1) AS b SETTINGS output_format_json_quote_64bit_integers = 1;

-- query: json_specials_default
-- formats: JSONEachRow,JSONCompact,TSVWithNamesAndTypes,CSV
SELECT id, f64, f32 FROM nums WHERE id >= 6 ORDER BY id;

-- query: float_rendering
-- formats: TSVWithNamesAndTypes,JSONCompact,CSV,Values,Native
SELECT 0.1 + 0.2, 1e100, -0., 1 / 3, 2 / 3, 1e-7, 123456789.123456789, 1.7976931348623157e308, toFloat32(1) / 3, toFloat32(16777217), 100., 1e6, 1e15 + 0.5;

-- query: float_specials
-- formats: TSVWithNamesAndTypes,JSONCompact,CSV,Values
SELECT nan, inf, -inf, -nan, 0 / 0, toFloat32(inf);

-- query: float_specials_native
SELECT inf, -inf, toFloat32(-inf);

-- query: big_ints
-- formats: TSVWithNamesAndTypes,JSON,JSONCompact,JSONEachRow,CSV,Values,Native
SELECT toUInt64(18446744073709551615) AS u, toInt64(-9223372036854775808) AS i, toUInt32(4294967295) AS u32, toInt32(-2147483648) AS i32, 9007199254740993 AS big;

-- query: nullable_formats
-- formats: TSVWithNamesAndTypes,CSVWithNames,JSONCompact,JSONEachRow,Values,Native
SELECT NULL AS a, toNullable('x') AS b, CAST(NULL AS Nullable(UInt8)) AS c, [NULL, 'y'] AS d, (NULL, 1) AS e;

-- query: bool_formats
-- formats: TSVWithNamesAndTypes,CSV,JSONCompact,JSONEachRow,Values,Native
SELECT true AS t, false AS f, toBool(1) AS b1, CAST(0 AS Bool) AS b0, [true, false] AS arr;

-- query: date_formats
-- formats: TSVWithNamesAndTypes,CSV,JSONCompact,JSONEachRow,Values,Native
SELECT toDate('2024-01-02') AS d, toDateTime('2024-01-02 03:04:05') AS t, toDateTime64('2024-01-02 03:04:05.006', 3) AS t3, toDateTime64('2024-01-02 03:04:05.000007', 6) AS t6, toDateTime('2024-01-02 03:04:05', 'Asia/Tokyo') AS ttz;

-- query: nested_formats
-- formats: TSVWithNamesAndTypes,CSV,JSONCompact,JSONEachRow,Values,JSONStringsEachRow
SELECT [[1, 2], []] AS a, [('x', 1.5)] AS b, map('k', [1]) AS c, ([1], 'y') AS d, [toDate('2024-01-01')] AS e, ['a\'b', 'c\\d'] AS f;

-- query: string_specials
-- formats: TSVWithNamesAndTypes,TSVRaw,CSV,JSONCompact,JSONEachRow,Values,JSONStrings
SELECT '\x01\x1F' AS ctrl, 'é' AS e_acute, '</script>' AS html, '\\' AS bs, '\'' AS sq, '"' AS dq, 'a\rb' AS cr, '\b\f\v' AS misc;

-- query: lowcardinality_format
-- formats: TSVWithNamesAndTypes,JSONCompact,Native
SELECT toLowCardinality('lc') AS l, toTypeName(l), CAST('x' AS LowCardinality(Nullable(String))) AS ln;

-- query: empty_result
-- formats: TSVWithNamesAndTypes,TabSeparated,CSVWithNames,JSON,JSONCompact,JSONEachRow,Values,Native
SELECT id, s FROM nums WHERE id > 100;

-- query: limit_json_rows_before_limit
-- formats: JSON,JSONCompact
SELECT number FROM numbers(10) ORDER BY number LIMIT 2;

-- query: explicit_format_clause
SELECT 1 AS one, 'two' AS two FORMAT JSONEachRow;

-- query: explicit_format_lowercase
SELECT 1 AS one FORMAT tsvwithnames;

-- query: format_alias_tsv
-- formats: TSV,TSVWithNames
SELECT 'a\tb' AS x;

-- query: format_null
-- formats: Null
SELECT * FROM nums;

-- query: pretty_compact
-- formats: PrettyCompact,Pretty
-- known-deviation: Pretty formats are best-effort
SELECT id, s FROM nums ORDER BY id LIMIT 3;

-- query: csv_settings_delimiter
-- formats: CSV
-- setting: format_csv_delimiter=;
SELECT 1 AS a, 'x;y' AS b;

-- query: tsv_null_repr
-- formats: TSV,CSV
-- setting: format_tsv_null_representation=NULL
SELECT NULL AS n, 1 AS one;

-- query: date_time_output_format
-- formats: TSV,JSONEachRow
-- setting: date_time_output_format=iso
SELECT toDateTime('2024-01-02 03:04:05') AS t, toDateTime64('2024-01-02 03:04:05.5', 3) AS t3;

-- query: date_time_output_format_unix
-- formats: TSV
-- setting: date_time_output_format=unix_timestamp
SELECT toDateTime('2024-01-02 03:04:05') AS t;

-- not-implemented: Decimal types
SELECT toDecimal64(1.5, 2);

-- not-implemented: Decimal types
CREATE TABLE dec (x Decimal(10, 2)) ENGINE = MergeTree ORDER BY x;

-- not-implemented: RowBinary output
SELECT 1 FORMAT RowBinary;

-- not-implemented: Parquet output
SELECT 1 FORMAT Parquet;

-- error: 73
SELECT 1 FORMAT NoSuchFormat;
