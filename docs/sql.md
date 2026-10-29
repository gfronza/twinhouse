# SQL support

This is a summary. The full contract, with error codes, is in
[DESIGN.md §2–§3](../DESIGN.md#3-sql-surface-internalsqlparse-internalexec).
Anything outside it fails with `NOT_IMPLEMENTED` (48) or
`UNKNOWN_FUNCTION` (46). It never returns a wrong result.

**Engines.** `MergeTree`, `ReplacingMergeTree([ver[, is_deleted]])`,
`AggregatingMergeTree`, `SummingMergeTree([cols])`. `Memory` and `Log` are
accepted and stored like `MergeTree`, which means they are persistent.
`ORDER BY` (including `tuple()`) defines the DynamoDB key.
`SETTINGS dynamodb_partition_key = 'a, b'` changes how many leading
columns form the partition key. `SETTINGS dynamodb_replication =
'global' | 'regional'` overrides the default replication.

**DDL.** `CREATE DATABASE`, `CREATE [OR REPLACE] TABLE [IF NOT EXISTS]`
(also `AS other_table` and `AS SELECT`), `CREATE MATERIALIZED VIEW … TO
target AS SELECT` (with `POPULATE`, except into a rollup in multi-region
mode), `CREATE VIEW`, `DROP TABLE|VIEW|DATABASE`, `TRUNCATE`,
`ALTER TABLE ADD|DROP|MODIFY COLUMN`, `ALTER TABLE … DELETE`,
`DELETE FROM`, `SHOW DATABASES|TABLES|CREATE TABLE`, `DESCRIBE`, `EXISTS`,
`USE`, `SET`. `OPTIMIZE` and `SYSTEM …` are accepted as no-ops. Column
`DEFAULT`/`MATERIALIZED`/`ALIAS` work. `PARTITION BY`, `PRIMARY KEY`,
`CODEC`, `COMMENT` and table `TTL` are accepted with no storage effect.

**Queries.** `WITH`, `DISTINCT`, `FROM` table / subquery / `numbers(N)` /
`system.{one,numbers,databases,tables,columns}`, `FINAL`, `WHERE`,
`PREWHERE`, `GROUP BY`, `HAVING`, `ORDER BY … NULLS FIRST|LAST`,
`LIMIT … OFFSET`, `LIMIT n BY`, `UNION ALL` (and `UNION` per
`union_default_mode`), scalar and `IN` subqueries, `ARRAY JOIN`, hash
`JOIN`s (INNER/LEFT/RIGHT/FULL/CROSS, `ANY`, LEFT `SEMI`/`ANTI`),
`{name:Type}` query parameters (also `Identifier`), `SETTINGS`, `FORMAT`.

**Types.** `(U)Int8–64`, `Float32/64`, `Bool`, `String`, `FixedString`,
`UUID`, `Enum8/16`, `Date`, `Date32`, `DateTime[(tz)]`,
`DateTime64(p[, tz])`, `Nullable`, `LowCardinality`, `Array`, `Tuple`
(named or not), `Map`, `AggregateFunction`, `SimpleAggregateFunction`.

**Functions.** Comparison, logical, arithmetic and conditional functions
(`if`, `multiIf`, `coalesce`, …). Type conversions, including the
`-OrZero`/`-OrNull` variants and `CAST`. Date/time functions (`toDate*`,
`toStartOf*`, `toYYYYMM*`, `dateDiff`, `formatDateTime`,
`parseDateTimeBestEffort*`, `INTERVAL` arithmetic, …). Strings (`concat`,
`substring*`, `lower`/`upper`, `like`/`ilike`, `match`, `replace*`,
`splitBy*`, `trim*`, `position`, …). Arrays (`has`, `indexOf`,
`arraySort`, `arraySlice`, `arrayDistinct`, `arraySum`, `arrayJoin`, …),
maps (`map`, `mapKeys`, `mapValues`, `mapContains`), tuples and
introspection (`version`, `timezone`, `currentDatabase`, `toTypeName`, …).

**Aggregates.** `count`, `sum`, `avg`, `min`, `max`, `any`, `anyLast`,
`argMin`, `argMax`, `uniq`, `uniqExact`, `groupArray`, `groupUniqArray`,
`count(DISTINCT …)`. Combinators `-If`, `-State`, `-Merge`, `-MergeState`
and `finalizeAggregation` are supported. Result types follow ClickHouse.

**Formats.** Output: Native, TabSeparated (and its `Raw`/`WithNames`/
`WithNamesAndTypes` variants), CSV (and variants), JSON, JSONCompact,
JSONStrings, JSONCompactStrings, JSONEachRow, JSONCompactEachRow,
JSONStringsEachRow, JSONCompactStringsEachRow, Values, Pretty,
PrettyCompact, Null. Input: Native, the TabSeparated and CSV variants,
JSONEachRow, JSONCompactEachRow, Values. Common `output_format_*`,
`input_format_*`, `format_csv_*`, `date_time_input_format` and
`date_time_output_format` settings are honoured.

**HTTP protocol.** `/`, `/ping` and `/replicas_status`. GET is read-only.
The query goes in the URL or the body, with INSERT data in the body.
Sessions (`session_id`; not in the serverless deployment, which rejects
them with `NOT_IMPLEMENTED`), `max_execution_time`, ClickHouse response
headers (`X-ClickHouse-Summary`, …), and ClickHouse's error body and HTTP
status for every error code are supported. Request and response
compression works, including ClickHouse's native `compress=1`/`decompress=1`
framing.

## Not supported

- The native TCP protocol and the MySQL/PostgreSQL/gRPC interfaces (HTTP
  only). The server itself has no TLS: terminate it at a load balancer
  (the serverless deployment gets it from API Gateway, the ECS deployment
  from its load balancer's optional HTTPS listener).
- `Decimal`, `Int128/256`, `UInt128/256`, `IPv4/6`, `JSON`, `Variant`,
  `Dynamic`, `Nested`, geo types.
- Window functions, lambdas and higher-order functions (`arrayMap`,
  `arrayFilter`, …), parametric aggregates (`quantile*`, `topK`, …) and
  other aggregates (`stddev*`, `var*`, `uniqCombined`, `sumMap`, …).
- `EXCEPT`/`INTERSECT`, `GROUP BY ROLLUP/CUBE`, `WITH TOTALS`, `EXPLAIN`,
  `SAMPLE`, `ORDER BY … WITH FILL`, `LIMIT … WITH TIES`, `DISTINCT ON`,
  `EXISTS (subquery)`, `ASOF JOIN`.
- `ALTER TABLE … UPDATE`, `RENAME`, dictionaries, table functions other
  than `numbers`, external tables, `INSERT INTO FUNCTION`,
  `INTO OUTFILE`.
- Materialized views without `TO`, refreshable views, cascading views (MV
  output does not trigger other MVs), `JOIN` or `UNION` inside a view's
  `SELECT`, and views that read from an Aggregating/Summing table.
- Output and input formats outside the list above (RowBinary, Parquet,
  Arrow, ORC, Avro, Protobuf, …).
- Most `system.*` tables.
- Settings that would change results but are not implemented
  (`join_use_nulls=1`, `transform_null_in`, …). These are rejected, not
  ignored.

## Deliberate deviations from ClickHouse

- **Everything is always merged.** Reads of a `ReplacingMergeTree` table
  behave like `FINAL`. Summing and Aggregating tables never show unmerged
  rows. `OPTIMIZE` has nothing to do.
- **Views are evaluated per inserted row**, not per inserted block. For
  the usual `GROUP BY` rollups the result is identical. A view whose
  output column feeding the target's *sorting key* depends on `now()`,
  `rand()`, `generateUUIDv4()` and similar is rejected, because its output
  must be a function of the source row.
- **Floats in sets.** `GROUP BY`, `DISTINCT`, `IN` and `uniq*` treat `-0`
  as `0` and all NaNs as equal. ClickHouse compares bit patterns.
- **Range checks.** A JSON number outside an integer column's range, and
  out-of-range `DateTime` text, are rejected instead of wrapped. `Date32`
  and `DateTime64` stop at 2262-04-11 (int64 nanoseconds).
- **Pretty formats are best-effort.** They need not match ClickHouse byte
  for byte.
- **`POPULATE` is restricted.** In multi-region mode it is rejected for a
  regional (rollup) target, because it would back-fill only one region. Back-fill
  with `INSERT INTO target SELECT …` in every region instead.
- **`session_timezone` must be a URL setting.** In a query's `SETTINGS`
  clause it is rejected.
