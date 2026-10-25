-- String functions and string escaping in every text format.

CREATE TABLE s (id UInt32, v String) ENGINE = MergeTree ORDER BY id;

INSERT INTO s VALUES (1, ''), (2, 'plain'), (3, 'tab\there'), (4, 'new\nline'), (5, 'back\\slash'), (6, 'single''quote'), (7, 'double"quote'), (8, 'ünïcødé ✓ 日本語'), (9, 'emoji 🎉'), (10, 'cr\rreturn'), (11, 'comma,semi;colon'), (12, '  padded  '), (13, 'null\0byte'), (14, 'Mixed Case');

-- query
-- formats: TSVWithNamesAndTypes,TabSeparated,TSVRaw,CSVWithNames,CSV,JSONEachRow,JSONCompactEachRow,JSON,JSONCompact,Values,Native
SELECT id, v FROM s ORDER BY id;

-- query
-- formats: JSONStrings,JSONCompactStrings,TSVWithNames,CSVWithNamesAndTypes,JSONStringsEachRow,JSONCompactStringsEachRow
SELECT id, v FROM s ORDER BY id;

-- query
SELECT id, length(v), lengthUTF8(v), empty(v), notEmpty(v) FROM s ORDER BY id;

-- query
SELECT id, upper(v), lower(v), lowerUTF8(v), upperUTF8(v) FROM s WHERE id IN (2, 8, 14) ORDER BY id;

-- query
SELECT id, concat(v, '|', toString(id)), v || '!' FROM s WHERE id <= 4 ORDER BY id;

-- query
SELECT substring('hello world', 1, 5), substring('hello world', 7), substring('hello', 0), substring('hello', -3), substring('hello', 2, 100), substr('abc', 2, 1);

-- query
SELECT substring('日本語テキスト', 1, 3), substringUTF8('日本語テキスト', 1, 3), length('日本語'), lengthUTF8('日本語');

-- query
SELECT startsWith('campaign_42', 'campaign'), endsWith('campaign_42', '42'), startsWith('', ''), endsWith('abc', '');

-- query
SELECT position('hello', 'l'), position('hello', 'z'), position('hello', ''), positionCaseInsensitive('HeLLo', 'll');

-- query
SELECT replaceAll('a-b-c', '-', '+'), replaceOne('a-b-c', '-', '+'), replaceAll('aaa', 'a', ''), replaceAll('abc', '', 'x');

-- query
SELECT splitByChar(',', 'a,b,,c'), splitByChar(',', ''), splitByString('::', 'x::y::z'), splitByChar(',', 'single');

-- query
SELECT trim('  x  '), trimLeft('  x  '), trimRight('  x  '), trim(BOTH 'x' FROM 'xxaxx'), trim(LEADING 'x' FROM 'xxaxx'), trim(TRAILING 'x' FROM 'xxaxx');

-- query
SELECT 'abc' LIKE 'a_c', 'abc' LIKE 'a\\_c', 'a_c' LIKE 'a\\_c', '100%' LIKE '100\\%', 'ABC' ILIKE '%b%', 'x' NOT ILIKE 'X';

-- query
SELECT match('campaign_42', '^campaign_[0-9]+$'), match('abc', 'B'), match('abc', '(?i)B'), match('', '^$');

-- query
SELECT repeat('ab', 3), repeat('x', 0), reverse('abc'), reverse('');

-- query
SELECT toString(123), toString(-1.5), toString(toDate('2024-01-31')), toString(true), toString(NULL), toString([1, 2]);

-- query
SELECT concat('a', NULL), concat('a', 1, 2.5), toTypeName(concat('a', NULL));

-- query
SELECT 'line1\nline2', 'tab\tsep', 'quote\'s', 'back\\slash', 'nul\0x', '\x41\x42';

-- query
-- formats: TabSeparated,CSV,JSONEachRow,Values
SELECT 'line1\nline2' AS a, 'tab\tsep' AS b, 'quote\'s' AS c, 'back\\slash' AS d, '"dq"' AS e, '' AS f;

-- unsupported: format()/leftPad() are outside the DESIGN §3 function list
SELECT format('{} - {}', 'a', 'b'), leftPad('7', 3, '0'), rightPad('7', 3, '0'), lpad('ab', 1, 'x');

-- query
SELECT arrayStringConcat(['a', 'b', 'c'], '-'), arrayStringConcat(['x']), arrayStringConcat([], ',');

-- query
SELECT v FROM s WHERE v LIKE '%quote%' ORDER BY v;

-- query
SELECT v FROM s WHERE v = 'single\'quote';

-- query
SELECT v FROM s ORDER BY v;

-- query
SELECT min(v), max(v), count(DISTINCT v), uniqExact(v) FROM s;

-- query
SELECT id FROM s WHERE empty(v) OR v = 'plain' ORDER BY id;

-- query
SELECT toFixedString('ab', 4), length(toFixedString('ab', 4)), toTypeName(toFixedString('ab', 4));

-- query
SELECT 'a' = 'a ', 'abc' < 'abd', 'Z' < 'a', '' < 'a', 'ä' > 'z';

-- query
SELECT lower('ÀÉ'), lowerUTF8('ÀÉ'), upper('àé'), upperUTF8('àé');

-- unsupported: left/right/base64Encode/hex are outside the DESIGN §3 function list
SELECT left('campaign', 4), right('campaign', 3), base64Encode('hi'), hex('AB');

-- unsupported: concatWithSeparator is outside the DESIGN §3 function list
SELECT concatWithSeparator('-', 'a', 'b', 'c'), concat_ws('-', 'a', 'b');

-- query
-- param: s=it's a "test"\tdone
SELECT {s:String} AS s, length({s:String});

-- query
-- param: s=multi\nline
SELECT {s:String} AS s, position({s:String}, '\n');
