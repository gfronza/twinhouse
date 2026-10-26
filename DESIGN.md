# twinhouse — design

A ClickHouse-compatible SQL server whose storage is DynamoDB, so that a
ClickHouse workload (append-only events + materialized-view rollups +
ReplacingMergeTree snapshots) can run **multi-region active-active** using
DynamoDB global tables.

Compatibility target: an application that uses ClickHouse over HTTP
(clickhouse-go v2 with `Protocol: clickhouse.HTTP`, curl, Python `requests`)
switches to this server by changing host/port only.

This file is the contract for all contributors. Code-level contracts live in:

| Contract | File |
|---|---|
| Errors (codes, HTTP status) | `internal/cherr/errors.go` |
| Types & value representation | `internal/chtype/type.go`, `api.go`, `aggpartial.go` |
| SQL AST + parser API | `internal/sqlparse/ast.go`, `api.go` |
| Catalog model | `internal/catalog/model.go` |
| Storage interface | `internal/storage/storage.go` |
| Formats API | `internal/format/format.go` |
| Engine API | `internal/exec/api.go` |
| Config / AWS clients | `internal/config/config.go`, `internal/awsx/awsx.go` |
| Reconciler hooks (stream consumers, discovery) | `internal/server/reconcile.go` |
| Lambda event-source mappings | `internal/esm/manager.go`, `consumers.go` |
| Serverless deployment (SAM) | `deploy/aws-lambda/template.yaml`, `deploy/aws-lambda/README.md` |
| Container deployment (ECS Fargate) | `deploy/aws-ecs/template.yaml`, `deploy/aws-ecs/README.md` |

When a change alters behaviour described here, update this file in the
same change. User-facing documentation is in `README.md`.

---

## 1. Architecture

```
            clickhouse-go / curl / requests (ClickHouse HTTP protocol)
                                   │
                    ┌──────────────▼──────────────┐
  region A          │ cmd/chserver                │   (same in region B)
                    │  server → exec → storage    │
                    └──────┬───────────────▲──────┘
                           │ PutItem/Query │
               ┌───────────▼───────────────┴───────────┐
               │ DynamoDB (region A)                    │
               │  global tables:  events, snapshots*,   │◄──── replication ────► DynamoDB (region B)
               │                  __catalog             │   (AWS global tables, or
               │  regional tables: rollups (+members),  │    cmd/replicator locally)
               │                  __mv_markers,         │
               │                  __checkpoints,        │
               │                  __dead_letters        │
               └───────────┬────────────────────────────┘
                           │ DynamoDB Streams (NEW_AND_OLD_IMAGES) of view-source tables
                    ┌──────▼──────────────────────┐
                    │ cmd/mvworker                │ applies materialized views
                    │  streams → exec.ApplyViews  │ to region-local rollups,
                    └─────────────────────────────┘ exactly once
```

\* With `GLOBAL_MODE=aws`, ReplacingMergeTree ("snapshot") tables are not
AWS global tables: every region has its own copy and `cmd/replicator`
copies rows between them with a version-aware condition (see "Consistency
model" below and §6).

The diagram shows the long-running processes of the local stack
(`deploy/local`). The **serverless deployment** (`deploy/aws-lambda`, one AWS SAM
stack per region) runs the same roles as Lambda functions and changes no
storage contract: `cmd/chserver-lambda` serves the HTTP API behind a
private REST API Gateway (§2 "Lambda API mode"), `cmd/mvworker` and
`cmd/replicator` consume DynamoDB streams through Lambda event-source
mappings instead of polling (§5 "Stream worker as a Lambda", §6
"Replicator as a Lambda"), and `cmd/reconciler-lambda` runs the reconciler
on the `__catalog` stream and a one-minute schedule, creating tables and
the event-source mappings at runtime (§2). No DynamoDB table is declared
in the stack. The **container deployment** (`deploy/aws-ecs`, one
CloudFormation stack per region on ECS Fargate) runs the long-running
processes of the diagram unchanged (§6 "Container stack").

### Why rollups are regional

DynamoDB global tables resolve concurrent writes to the same item with
last-writer-wins. Counters (`ADD`) updated in two regions at once would lose
increments. So:

* **Source tables** (MergeTree, ReplacingMergeTree) are **global**: rows are
  immutable (MergeTree) or whole-row LWW snapshots (Replacing), which is
  exactly what global tables handle well.
* **Rollup tables** (AggregatingMergeTree, SummingMergeTree) are
  **regional**: every region maintains its own copy, derived from *its own
  replica* of the source table's stream, which contains both local writes and
  writes replicated from other regions. Every region therefore converges to the
  same aggregates without any cross-region write conflicts.
* The writer region additionally applies views **inline** during INSERT
  (`INLINE_VIEWS=true`) so reads after a write in the same region see it
  immediately (read-your-writes), like a single ClickHouse node. The stream
  worker later sees the same rows; exactly-once markers make that a no-op.
  With `Config.StreamViews` (chserver's `STREAM_VIEWS`, default true: a
  stream worker runs), an inline application that fails does not fail the
  INSERT: the worker applies it (§3 INSERT).

Override: `SETTINGS dynamodb_replication = 'global' | 'regional'` on CREATE TABLE.

### Consistency model (documented for users)

* Raw source tables: read-your-writes in the writing region (strongly
  consistent reads by default); other regions see the row after replication
  (typically < 1–2 s on AWS).
* Rollups: read-your-writes in the writing region (inline views); other regions
  converge after replication + stream processing.
* ReplacingMergeTree: reads always behave like `FINAL`. Across regions,
  conflicting writes for the same key resolve like ClickHouse: the highest
  `ver` wins regardless of write order, write time (`_ts`, then `_origin`)
  only breaks version ties; without a version column, LWW by write time.
  The local write path uses the same order as the replicator (§4 "Engine
  write semantics"): a write that loses only because the stored row has a
  larger `_ts` (written by a clock running ahead in another region) is
  re-stamped just after it and retried, so a later write still wins and
  every region keeps the same row.
  This holds with the emulated replicator (§6) and with `GLOBAL_MODE=aws`:
  real global tables resolve item conflicts by their own last-writer-wins,
  which could let a lower version written later in another region win, and
  silently drop the loser of a concurrent write (which then never reaches
  that region's stream and views), so in that mode a ReplacingMergeTree
  table, with or without a version column, is **not** given global-table
  replicas (`catalog.CopiedByReplicator`). It exists in every region
  (created by each region's reconciler) and `cmd/replicator` runs with
  `GLOBAL_MODE=aws` to copy only those tables, with its version-aware
  conditional put for ReplacingMergeTree(ver), and its conflict hook (below)
  for both (§6); the catalog and every other global table stay real global
  tables. A ReplacingMergeTree table created as a global table by an older
  build keeps its replicas (recreate it to switch).
* Mutations of regional tables (rollups): `TRUNCATE` is recorded in the
  replicated catalog (`Table.ResetAt`) and every region resets its own copy
  (§5 "Resets"); a store refuses (`NOT_IMPLEMENTED`) a TRUNCATE of a
  regional table whose reset was not recorded in the catalog first, and
  `ALTER TABLE ... DELETE` / `DELETE FROM` on a regional table, when
  `GLOBAL_MODE` is `aws` or `emulated`: nothing would carry them to the
  other regions' copies. Rows written within a moment of a TRUNCATE in
  another region (catalog replication plus one reconcile round) may be
  missing from a region's copy.
* Materialized views over a ReplacingMergeTree source (regional rollups):
  rollups converge only if every region's views see the same row versions.
  When two regions write conflicting versions of a key concurrently, the
  region already holding the winner rejects the replicated loser, so the
  loser never enters that region's stream; the origin region did apply its
  views to it. `cmd/replicator` therefore applies the target region's views
  to every replicated version the target rejects as superseded
  (`APPLY_CONFLICT_VIEWS`, default on; same row id, so exactly-once markers
  dedupe it, and the same view-eligibility rule as the stream worker, §5):
  every region counts both versions (in `GLOBAL_MODE=aws` too, since
  ReplacingMergeTree tables are copied by the replicator there). One case
  is **not** convergent: a version that is already superseded when it is written
  (a stale write after the newer version replicated) is not stored and
  never replicates, so only the writer region's views can see it: such a
  row must feed the views of no region or of every region. The engine
  therefore applies views to such a row inline only in a single-region
  deployment (like ClickHouse, whose views see every inserted block); with
  `exec.Config.MultiRegion` (set when `GLOBAL_MODE` is not `none`) it feeds
  no view (§5).
* DDL: catalog writes are conditional only against the local region's copy
  of `__catalog`, which replicates last-writer-wins. Concurrent DDL on the
  *same* object in two regions (two ALTERs, CREATE vs CREATE, DROP vs
  ALTER) can therefore both succeed while one of them is silently
  overwritten; run schema changes for one object from one region at a time.

---

## 2. HTTP protocol (`internal/server`, `cmd/chserver`, `cmd/chserver-lambda`)

Implements the ClickHouse HTTP interface on `LISTEN_ADDR` (default `:8123`;
in the serverless deployment behind API Gateway, see "Lambda API mode"):

* `GET /ping` and `HEAD /ping` → `Ok.\n`; `GET /` (no query) → `Ok.\n`;
  `GET /replicas_status` → `Ok.\n`.
* `GET|POST /?query=...` and `POST /` with the query in the body. Rule
  (ClickHouse): if the `query` URL parameter is present, the body is INSERT
  data appended after the query; otherwise the body is the query (which may
  itself contain inline INSERT data after `FORMAT x` / `VALUES`). GET requests
  are read-only (`readonly=1` semantics: INSERT/DDL rejected with code 164).
* Auth: `X-ClickHouse-User`/`X-ClickHouse-Key` headers, HTTP Basic, or
  `user`/`password` URL params, checked against `CH_USERS`.
* Reserved URL params: `query`, `database`, `default_format`,
  `client_protocol_version`, `query_id`, `session_id`, `session_timeout`,
  `user`, `password`, `compress`, `decompress`, `enable_http_compression`,
  `wait_end_of_query`, `param_*` (query parameters). Every other URL param is a
  setting (passed in `Request.Settings`; unknown ones are accepted). A
  SELECT's own `SETTINGS` clause (outermost statement and its UNION
  branches) is merged over URL/session settings for the output format too
  (e.g. `output_format_json_quote_denormals`), as in ClickHouse.
* Current database: `X-ClickHouse-Database` header, else `database` URL param,
  else the session's, else `default`. It must exist in the catalog, except
  `default` and the virtual databases `system`, `information_schema` and
  `INFORMATION_SCHEMA` (case-sensitive), which always exist.
* Output format: the query's `FORMAT` clause, else `default_format`, else
  `TabSeparated` (names match case-insensitively). Implemented
  (`internal/format/registry.go`): Native, TabSeparated[Raw][WithNames
  [AndTypes]] (aliases TSV…, Raw…), CSV[WithNames[AndTypes]], JSON,
  JSONCompact, JSONStrings, JSONCompactStrings, JSONEachRow (JSONLines,
  NDJSON), JSONCompactEachRow, JSONStringsEachRow,
  JSONCompactStringsEachRow, Values, Pretty, PrettyCompact and Null; input
  is accepted in Native, the TabSeparated and CSV variants, JSONEachRow,
  JSONCompactEachRow and Values. Pretty output is best-effort (it need not
  match ClickHouse byte for byte). Other ClickHouse formats such as
  RowBinary/Parquet/Arrow: `NOT_IMPLEMENTED` (48); names ClickHouse does
  not know: `UNKNOWN_FORMAT` (73). Response headers like ClickHouse: `X-ClickHouse-Query-Id`,
  `X-ClickHouse-Format`, `X-ClickHouse-Timezone`,
  `X-ClickHouse-Server-Display-Name`, `X-ClickHouse-Summary`
  (`{"read_rows":"0","read_bytes":"0","written_rows":"1",...}`),
  `Content-Type` per format.
* Format settings honoured by `internal/format` (verified against
  ClickHouse 25.8): `output_format_json_*` (quote_64bit_integers/floats,
  quote_denormals — a negative NaN prints `"-nan"` —, named_tuples_as_objects,
  escape_forward_slashes, pretty_print, array_of_rows),
  `output_format_write_statistics`, `output_format_{tsv,csv}_crlf_end_of_line`,
  `format_csv_delimiter`, `format_tsv_null_representation` /
  `format_csv_null_representation` (top-level NULL fields, output and input;
  NULL inside arrays stays `NULL`), `date_time_output_format`
  (`simple`|`iso`|`unix_timestamp`; text and JSON formats, nested values too,
  rendered and quoted like strings — `iso` is always UTC with `Z`; Native is
  unaffected; other values → `BAD_ARGUMENTS` 36), `date_time_input_format`
  (`basic`|`best_effort`|`best_effort_us` for DateTime/DateTime64 fields of
  every input format: ISO 8601 with `T`/fraction/`Z`/`±hh[:mm]`, `UTC`/`GMT`,
  DD/MM/YYYY (MM/DD with `_us`), YYYYMMDD[Thhmmss], YYYY, YYYYMM, 9–10 digit
  unix seconds (DateTime64: `.fraction`), 13-digit unix ms, RFC 1123/822;
  anything it does not recognise falls back to the basic rules), and the
  `input_format_*` settings listed in `format.Options`. Row-input errors use
  ClickHouse's codes: a malformed number in TSV/CSV/JSON →
  `CANNOT_PARSE_INPUT_ASSERTION_FAILED` (27), JSON syntax errors → 27, a JSON
  row cut off by end of stream → `CANNOT_READ_ALL_DATA` (33).
* Errors: HTTP status from `cherr.HTTPStatus(code)` (ClickHouse 25.8's
  `exceptionCodeToHTTPStatus`, verified code by code; unlisted codes are 500),
  header `X-ClickHouse-Exception-Code: <code>`, body
  `Code: <code>. DB::Exception: <msg>. (<NAME>) (version <v>)\n`, where
  `<NAME>` comes from `cherr.Name(code)` — the complete ClickHouse 25.8 code
  table (`internal/cherr/names.go`), so every code has its real name.
  Packages raise codes via the `cherr` constants. clickhouse-go parses this
  body (`Exception.Code`/`CodeName`).
* Compression: request `Content-Encoding` gzip/deflate/br/zstd/lz4/bz2 is
  decoded (`xz`/`snappy`: `NOT_IMPLEMENTED`); the response is compressed
  (zstd, br, lz4, gzip or deflate, in that preference) when
  `enable_http_compression=1` and the client sends `Accept-Encoding`.
  ClickHouse's native block framing (CityHash128 checksum + LZ4/ZSTD/none
  frames, `internal/server/native.go`) is supported with `compress=1`
  (response) and `decompress=1` (request body).
* Handshake: clickhouse-go's HTTP dial runs
  `SELECT displayName(), version(), revision(), timezone()` with
  `default_format=Native&client_protocol_version=<rev>` and decodes Native.
  The engine reports `revision()` = 54477 (UInt32) and `version()` =
  SERVER_VERSION. clickhouse-go then decodes Native using that revision.
* `max_execution_time` (seconds, fractional allowed; URL, session or the
  query's `SETTINGS` clause; 0/negative = unlimited) bounds the execution
  context, so storage calls to a stalled DynamoDB endpoint are cancelled;
  expiry answers `TIMEOUT_EXCEEDED` (159, HTTP 408) with ClickHouse's text
  `Timeout exceeded: elapsed <x> ms, maximum: <y> ms`. Unparseable values
  fail like ClickHouse (27 / 72). `MAX_EXECUTION_TIME` (seconds or a Go
  duration, default 0) is the server-wide default a query may override.
* **Lambda API mode** (`cmd/chserver-lambda`, `deploy/aws-lambda`): the same
  `server.Server` wired exactly like `cmd/chserver` (same `exec.Config` and
  `server.Config`), served to API Gateway REST proxy events through
  `aws-lambda-go-api-proxy/httpadapter`, with these additive differences:
  * `server.Config.RejectSessions` (set only here): a request naming
    `session_id`, `session_check` or `session_timeout` fails, after
    authentication and before settings or body are read, with
    `NOT_IMPLEMENTED` (48, HTTP 501) and the message `sessions are not
    supported by this deployment`; no process outlives a request, so
    `SET`/`USE` state cannot be kept. `query_id`, settings and `database`
    work as usual.
  * No background goroutines and no DynamoDB call at initialization: at
    the start of every request (`App.Prepare`) the function checks once
    per execution environment that the system tables exist
    (`Store.CheckSystemTables`: `DescribeTable` only, `UPDATING` counts as
    ready; the reconciler creates them; failure answers `UNAVAILABLE` and
    is retried by the next request) and reloads the catalog snapshot when
    it is older than `CATALOG_REFRESH` (a failed reload logs and serves
    the previous snapshot; with no snapshot yet the request fails). The
    engine's miss-triggered reloads (§4 "Catalog items") work unchanged.
  * Bodies: the API has `BinaryMediaTypes */*`, so every request body
    arrives base64-encoded and the adapter decodes it; a `Content-Length`
    header is restored for POST events (API Gateway does not always
    forward it and the server requires it). Responses are base64-encoded
    (`isBase64Encoded`) when the adapter finds invalid UTF-8 **or** when
    the response has a `Content-Encoding` or a non-text `Content-Type`
    (Native, `compress=1`, compressed output that happens to be valid
    UTF-8), so API Gateway returns the exact bytes. Error bodies, headers
    (`X-ClickHouse-Exception-Code`, …) and HTTP statuses are those above.
  * Limits: request and response bodies are bounded by the Lambda payload
    (6 MB; the whole response is buffered, so the server's streaming
    commit logic only decides the status of a late error); a query is
    bounded by the API Gateway integration timeout (29 s by default; set
    `MAX_EXECUTION_TIME` below it so a slow query fails with 159 rather
    than a 504). DDL runs with `dynamo.Options.AsyncDDL` (below).
  * Configuration is `internal/config` without `LISTEN_ADDR` and
    `RUN_STREAM_WORKER`; the reconciler and the stream worker are separate
    functions.
* The server process also (a) refreshes the catalog every `CATALOG_REFRESH`,
  (b) runs the **reconciler**: ensures every catalog table exists physically
  in this region (`Store.EnsureTable`), drops physical tables whose catalog
  entry vanished, and (c) optionally runs the stream worker in-process
  (`RUN_STREAM_WORKER=true`, default false). The reconciler re-ensures a
  table whenever its definition changes — catalog version, incarnation
  (`CreatedAt`: a dropped and recreated table restarts at version 1) or the
  definition itself (two regions' concurrent DDL can leave a different
  definition at the same version). A physical table is dropped only after
  it was missing for `DropAfter` (2) rounds *and* a fresh catalog re-read
  just before the drop still lacks it, so a table recreated under the same
  name in the meantime is never deleted. Catalog refreshes (1 min) and
  ensure/drop calls (10 min) have deadlines, so an unresponsive endpoint
  cannot stall reconciliation. Limitation: a table recreated under the same
  physical name keeps the previous incarnation's rows in regions that did
  not run the DROP (the reconciler cannot safely replace a physical table
  that other processes, the stream worker and the replicator may already be
  writing); it logs a warning. This only concerns entries written by older
  builds: every table created now has its own physical name per
  incarnation (§4 "Tables"), so a DROP + CREATE or CREATE OR REPLACE gives
  every region a new physical table (ensured by the reconciler) and the old
  one is dropped through the missing/`DropAfter` path. The reconciler never
  drops the physical table of a catalog entry it cannot decode
  (quarantined, §4 "Catalog items"), and drops nothing while an unreadable
  item of unknown physical name exists. Besides the entries it observed
  itself, the reconciler discovers drop candidates from durable state
  (`ReconcilerConfig.Physical`, default the store's `PhysicalTables`
  capability, and `ConsumedTables` of the stream consumers), every
  `DiscoverInterval` (1 min): the region's physical row tables of this
  deployment (`ListTables`, filtered to the `<prefix><db>.<table>.<inc>`
  shape of §4) and the tables whose streams still have event-source
  mappings; those the current catalog does not name enter the same
  missing/`DropAfter`/re-read path (`DropPhysicalTable` by name, replicas
  and side table included) so a process that starts after a `DROP` (a new
  Lambda execution environment, a restarted server) still cleans up.
  Deployments sharing an account must therefore not use prefixes that are
  prefixes of one another. A deletion DynamoDB refuses because the table
  was the source of a replica within the last 24 hours
  (`storage.ErrDeleteDeferred`, `UNAVAILABLE`) is success for the DDL
  once the catalog entry is gone (`exec` logs it) and is retried by the
  reconciler every round at debug level until the window passes.
* **Asynchronous DDL** (`dynamo.Options.AsyncDDL`, the API Lambda): a
  request bounded by the API Gateway timeout (29 s) cannot wait for a
  global-table replica (minutes). With the option `EnsureTable` creates
  the table and waits for it to be `ACTIVE` but skips `ensureReplicas`
  (the home region's reconciler adds them through its own `EnsureTable`),
  and `DropTable` issues the deletion of a table without replicas without
  awaiting it and reports a replicated table as deferred (the reconcilers
  remove replicas and table). `CheckSystemTables` (DescribeTable only,
  `UPDATING` counts as ready) replaces `EnsureSystemTables` there.
* **Stream consumers** (`ReconcilerConfig.Consumers`, the serverless
  deployment): when `MVWORKER_FUNCTION_ARN` is set, the reconciler also
  manages the Lambda event-source mappings (ESMs) of the tables' DynamoDB
  streams (`internal/esm`, §5 "Stream worker as a Lambda"). After a table
  is ensured it resolves the table's stream ARN (`DescribeTable`; rollups
  have no stream and get nothing) and calls `EnsureConsumers`, once per
  table definition (re-done on every catalog version, retried next round
  on failure without re-ensuring the table); right before a physical
  table is dropped it calls `RemoveConsumers` (by stream ARN, or by table
  name when the table is already gone) and postpones the drop while that
  fails, so no mapping outlives its table. Which functions consume a
  stream is decided by `esm.Consumers` from the engine and `GLOBAL_MODE`:
  the stream worker for every table views can read from (MergeTree,
  ReplacingMergeTree, Memory, Log), plus the replicator
  (`REPLICATOR_FUNCTION_ARN`) for global ReplacingMergeTree tables with
  `GLOBAL_MODE=aws` (they are not AWS global tables, §1); regional
  rollups and system tables get none. Without `MVWORKER_FUNCTION_ARN`
  (long-running workers, DynamoDB Local) nothing changes: the workers
  discover streams from the catalog themselves.
* `cmd/reconciler-lambda` runs the reconciler as a Lambda function: each
  invocation, triggered by the `{prefix}__catalog` stream (a DDL in any
  region) or by an EventBridge schedule (`rate(1 minute)`, the safety net;
  both shapes are detected from the event, anything else also runs a
  round), does `EnsureSystemTables` (idempotent; with `GLOBAL_MODE=aws`
  only the home region creates `__catalog`, the others wait for their
  replica as always), wires the catalog stream to itself
  (`RECONCILER_FUNCTION_ARN`, default `AWS_LAMBDA_FUNCTION_NAME`; once per
  execution environment) and runs `Reconciler.RunOnce` under the
  invocation deadline minus `RECONCILER_DEADLINE_MARGIN` (5s). The
  invocation fails only when the system tables are unavailable (so the
  catalog batch is redelivered); per-table failures are logged and retried
  on the next round. `ESM_BATCH_SIZE` (default 100) sets the mappings'
  batch size. It needs `lambda:ListEventSourceMappings`,
  `CreateEventSourceMapping`, `UpdateEventSourceMapping` and
  `DeleteEventSourceMapping`, plus `dynamodb:ListTables` for the
  discovery above, on top of the table permissions below.
* `DISPLAY_NAME` sets `displayName()` / `X-ClickHouse-Server-Display-Name`
  (default: hostname, suffixed with `-<REGION>` unless `REGION=local`). Like
  every other variable it is read by `config.Load`. `REGION` defaults to
  `AWS_REGION`, then `AWS_DEFAULT_REGION` (as injected by ECS/Lambda), then
  `local`. At startup chserver validates (`Config.Validate`) that a real
  region is set when `DYNAMODB_ENDPOINT` is empty, and that
  `GLOBAL_MODE=aws` has no `DYNAMODB_ENDPOINT` and a non-empty
  `REPLICA_REGIONS`; it retries startup calls while DynamoDB is unreachable
  or throttling, but exits at once on permanent errors
  (`ValidationException`, `AccessDenied`, unrecognised/invalid credentials,
  `BAD_ARGUMENTS` layout mismatches).
* Every process builds its AWS SDK clients with a per-attempt response
  header timeout (`DDB_HTTP_TIMEOUT`, Go duration, default 30s, `0`
  disables; the SDK retries a timed-out attempt), and the stream readers
  bound each GetRecords/GetShardIterator/DescribeStream/lease call (1 min),
  so a stalled endpoint cannot hang a background loop. mvworker validates
  its configuration like chserver and stops its startup retries on the same
  permanent errors (`awsx.PermanentError`). `GLOBAL_HOME_REGION` (aws mode)
  overrides the home region that creates global tables (default: the first
  of the sorted `REGION` + `REPLICA_REGIONS`); it must be one of them.
* IAM (on AWS), for `{prefix}*` tables: chserver, mvworker (system tables
  at startup, `__dead_letters` on first use) and the replicator (its
  `__checkpoints` and the copied tables, in every region it lists) need
  `dynamodb:DescribeTable`, `CreateTable`, `UpdateTable`, `DeleteTable`,
  `DescribeTimeToLive`, `UpdateTimeToLive`, `GetItem`, `PutItem`,
  `UpdateItem`, `DeleteItem`, `BatchGetItem`, `BatchWriteItem`,
  `ConditionCheckItem`, `Query` and `Scan` (`TransactWriteItems` is
  authorized through `PutItem`/`UpdateItem`/`ConditionCheckItem`). Stream
  consumers (`RUN_STREAM_WORKER`, mvworker, replicator) also need
  `dynamodb:DescribeStream`, `GetShardIterator` and `GetRecords` on
  `{prefix}*/stream/*`; a Lambda event-source mapping additionally needs
  `dynamodb:ListStreams`. With `GLOBAL_MODE=aws` the DDL-issuing chserver
  also needs `dynamodb:CreateTableReplica` and `DeleteTableReplica`, the
  table and item actions in every replica region, and
  `iam:CreateServiceLinkedRole` for `replication.dynamodb.amazonaws.com`.
  The serverless deployment (`deploy/aws-lambda/template.yaml`, one inline policy
  per function, every DynamoDB statement scoped to `table/{prefix}*`,
  `/index/*` and `/stream/*`) narrows this per function: the API Lambda
  gets the item actions plus `DescribeTable`, `CreateTable`, `UpdateTable`
  and `DeleteTable` (with `AsyncDDL` it never adds or removes replicas and
  never touches TTL); the stream-worker Lambda the item actions in its own
  region, `DescribeTable`, `CreateTable` and the TTL actions (its
  `__dead_letters` table; in Lambda mode it does not run
  `EnsureSystemTables`) and the stream actions; the replicator Lambda the
  same with item and table actions in every region (its write targets);
  the reconciler Lambda the full set including the replica actions, the
  service-linked role, `dynamodb:ListTables` and the
  `lambda:*EventSourceMapping` actions (create/update/delete conditioned on
  `lambda:FunctionArn` naming the three target functions; `List` cannot be
  scoped). `go test ./deploy/aws-lambda/` asserts the scoping.
  The container deployment (`deploy/aws-ecs/template.yaml`, one task role
  per ECS service) grants the long-running binaries: chserver the item
  and table actions (`DeleteTable` included) and TTL in its region,
  `dynamodb:ListTables`, with `GLOBAL_MODE=aws` the replica actions and
  the replica-region table and item actions (conditioned on
  `aws:RequestedRegion` naming the deployment's regions) and the
  service-linked role, and the stream actions only with
  `RUN_STREAM_WORKER`; mvworker the item, table (no `DeleteTable`), TTL
  and stream actions in its region, and in the home region the replica
  actions on `{prefix}__catalog` only (its startup `EnsureSystemTables`
  may add that table's replicas); the replicator the item actions,
  `DescribeTable`, `CreateTable` and the stream actions in every region
  of the deployment (it tails and writes all of them; leases in each
  source region's `__checkpoints`). No ECS role has Lambda or
  `ListStreams` permissions. `go test ./deploy/aws-ecs/` asserts the
  scoping.

---

## 3. SQL surface (`internal/sqlparse`, `internal/exec`)

Supported (beyond this list, return `NOT_IMPLEMENTED` (48) or
`UNKNOWN_FUNCTION` (46), never wrong results):

* **DDL**: `CREATE DATABASE [IF NOT EXISTS]`, `CREATE TABLE [IF NOT EXISTS]`
  with engines MergeTree, ReplacingMergeTree([ver[, is_deleted]]) (reads
  hide keys whose latest row has `is_deleted = 1`), AggregatingMergeTree,
  SummingMergeTree([cols]), Memory, Log (other ClickHouse engines:
  `NOT_IMPLEMENTED` (48); names that are not ClickHouse engines:
  `UNKNOWN_STORAGE` (56)); `PARTITION BY` (accepted, no storage effect;
  shown by SHOW CREATE TABLE),
  `ORDER BY col | (cols) | tuple()`, `PRIMARY KEY` (accepted; must be an
  ORDER BY prefix), `SETTINGS` (accepted; `dynamodb_*` settings honoured),
  column `DEFAULT`/`MATERIALIZED`/`ALIAS`, `CODEC` and `COMMENT` (ignored),
  table `TTL` (accepted and ignored: rows never expire; not shown by SHOW
  CREATE TABLE); `CREATE OR REPLACE TABLE`, `CREATE TABLE t AS other`,
  `CREATE TABLE t ENGINE = ... AS SELECT`;
  `CREATE MATERIALIZED VIEW [IF NOT EXISTS] v TO t AS SELECT ...` and
  `[POPULATE]` (backfill from the source); `CREATE VIEW v AS SELECT`;
  `DROP TABLE|VIEW|DATABASE [IF EXISTS]`; `TRUNCATE`; `SHOW DATABASES|TABLES|CREATE TABLE`;
  `DESCRIBE`; `EXISTS`; `USE`; `SET`; `OPTIMIZE` (no-op); `SYSTEM ...` (no-op);
  `ALTER TABLE ADD|DROP|MODIFY COLUMN` (catalog-only; items written before
  the column existed read its current `DEFAULT`/`MATERIALIZED` expression,
  evaluated at read time over the row — the engine scans with
  `ScanRequest.MarkAbsent` — or the type's zero value without one, like
  ClickHouse parts that predate the column; items written before a MODIFY
  COLUMN type change are converted at read time like ClickHouse's mutation,
  see §4 "Reads"), `ALTER TABLE ... DELETE WHERE`
  and `DELETE FROM` (not on regional tables in multi-region modes, §1).
* **INSERT**: `VALUES (...)` with literals and `{name:Type}` parameters; body
  data in any supported input format (other ClickHouse input formats such as
  RowBinary/Parquet: `NOT_IMPLEMENTED` (48); unknown names:
  `UNKNOWN_FORMAT` (73)); `INSERT INTO t SELECT ...`; column list
  subsets with DEFAULT evaluation (`now64(9)`, constants, expressions over other
  columns); Nullable handling; type conversion to column types
  (`chtype.Normalize`/`ParseText`/`ParseQuoted`). Integer text in
  Values/TSV/CSV wraps modulo 2^width like ClickHouse (300 into UInt8 → 44);
  deliberate deviation: a JSON number outside an integer column's range is
  rejected with `ARGUMENT_OUT_OF_BOUND` (69) instead of being wrapped, and
  out-of-range DateTime text is rejected instead of wrapped modulo 2^32
  (`CAST`/`toDateTime` from String saturate at 2106-02-07 06:28:15 like
  ClickHouse; DateTime64 text before 1900 clamps its date to 1900-01-01,
  keeping the time of day, as ClickHouse does). Date32 and DateTime64 are
  capped at 2262-04-11 23:47:16.854775807 (int64 nanoseconds): later values
  are rejected (69 / 407) where ClickHouse saturates at 2299-12-31.
  Plain `VALUES` literals (numbers, strings, arrays/tuples of them) are
  parsed with the column type's quoted parser, as ClickHouse's Values format
  does, before falling back to expression evaluation: an integer for a
  `DateTime64(3)` column is a tick count (`1714989600250` →
  `2024-05-06 10:00:00.250`), not unix seconds.
  **Atomicity** (like ClickHouse, per block): the engine parses, converts
  and completes (DEFAULTs) a whole block before the first `Store.Insert`, so
  a parse, conversion or DEFAULT error anywhere in it writes nothing and the
  client can fix and resend the data. A block holds up to
  `min_insert_block_size_rows` rows (default 1048449) or about
  `min_insert_block_size_bytes` (default 256 MiB) of values (0 disables a
  bound; both 0 → `max_insert_block_size` rows); an INSERT larger than one
  block can leave earlier blocks written. A block is then written in storage
  batches of 1000 rows, each followed by inline view application; a storage
  failure mid-block can leave part of the block written (DynamoDB has no
  cross-batch transactions). When inline view application fails after the
  rows are stored, the INSERT still succeeds if `Config.StreamViews` is set
  (a stream worker applies the views exactly once; the failure is logged
  and read-your-writes of the rollups lags); otherwise the error is returned
  with the source rows already written, as in ClickHouse when a view fails.
* **SELECT**: `WITH`, `DISTINCT`, expressions, `FROM table [FINAL] | (subquery)
  | system.* | numbers(N)`, `WHERE`, `PREWHERE` (as WHERE), `GROUP BY`
  (expressions, aliases, positions), `HAVING` (without aggregation it filters
  like WHERE, as in ClickHouse), `ORDER BY` (ASC/DESC, NULLS),
  `LIMIT n [OFFSET m]`, `LIMIT n BY`, `UNION ALL`, scalar subqueries,
  `IN (subquery)`, `ARRAY JOIN` (one clause, optionally `LEFT`), `JOIN`
  (hash joins: INNER/LEFT/RIGHT/FULL/CROSS, `ANY`, LEFT `SEMI`/`ANTI`;
  equality conditions), `SETTINGS`, `FORMAT`.
* **Value order** (`chtype.Compare` / `chtype.CompareHint`): arrays, tuples
  and map entries compare lexicographically (a proper prefix first). Inside
  them NULL and NaN follow ClickHouse's `compareAt(nan_direction_hint)`:
  comparison operators and `Compare` use hint +1 (numbers < NaN < NULL, so
  `[1] < [1,2] < [1,nan] < [1,NULL] < [NULL]`); ORDER BY uses hint =
  direction (NULLS LAST) or −direction (NULLS FIRST); max/argMax/greatest
  use −1 and min/argMin/least +1, so a nested NULL/NaN never wins an extremum.
  `Compare` alone puts a *top-level* NULL first (key order, see §4).
  Arrays and maps compare with these rules in the comparison operators too
  (`[nan] = [nan]`, `map(1,NULL) > map(1,2)`); **tuples** do not: like
  ClickHouse, a tuple comparison is decomposed into per-element operators
  combined with three-valued AND/OR (`less = less(a0,b0) OR (equals(a0,b0)
  AND ...)`), so a NaN element is neither less, equal nor greater
  (`(1,nan) = (1,nan)` is 0) and a NULL element yields NULL only when it
  decides the result (`(1,NULL) < (2,1)` = 1, `(1,NULL) < (1,2)` = NULL;
  result type `Nullable(UInt8)`); a bare `NULL` literal element makes the
  whole result `NULL` of type `Nullable(Nothing)`. The DynamoDB extremum
  merge (`applyExtremum`, compare-and-swap path) uses the same rule as the
  in-memory one (`aggstate.ReplacesTyped`, i.e. `chtype.CompareTypedHint`: Enum by
  value, UUID as UInt128, nested NULL/NaN never win).
* **Typed order** (`chtype.CompareTyped` / `CompareTypedHint` / `OrderKey`):
  `Compare` is untyped, and two canonical representations do not carry
  ClickHouse's order. **UUID** text compares as ClickHouse's UInt128: the
  last 8 bytes (hex digits 19..35) are the high half and are compared first,
  then the first 8 bytes. So `00000000-0000-0001-0000-000000000000 <
  00000000-0000-0000-0000-000000000001`, although the text sorts the other
  way. **Enum** names compare by numeric value. The typed comparators apply
  both rules at every depth (Array, Tuple, Map entries, Nullable,
  LowCardinality), and keep `Compare`/`CompareHint` NULL/NaN placement.
  Every ordering use of a typed value must go through them: comparison
  operators, ORDER BY, min/max/argMin/argMax and their CAS keys,
  greatest/least, key-range bounds and the in-memory store's key order.
  The comparison operators switch to the typed comparator only when an
  operand type contains a UUID (also per tuple element), because an Enum
  compared with a non-constant String compares by name ("Comparison
  semantics" below). `chtype.UUIDOrderKey` gives the
  fixed-width 32-hex-digit text (`hex[19:]` digits followed by `hex[:18]`
  digits) whose byte order is this order, for order-preserving key
  encodings. Equality is unchanged.
* **Parameters**: `{name:Type}` anywhere an expression is allowed, values from
  `param_name` parsed with `chtype.ParseText(Type, text)`. Missing param →
  `UNKNOWN_QUERY_PARAMETER` (456). A value that does not parse is reported
  like ClickHouse: a number whose text parser stops early →
  `BAD_QUERY_PARAMETER` (457) `Value X cannot be parsed as T for query
  parameter 'p' because it isn't parsed completely: only N of M bytes was
  parsed: <prefix>`; a parser exception (empty number: 32/72, date/time,
  malformed arrays/tuples/maps, quoted strings, UUID, Bool, Enum: 41, 38,
  27, 26, 130, 376, 467, 691, ...) keeps its code, with `: value X cannot
  be parsed as T for query parameter 'p'` appended. Parameters bind as
  typed constants (an `Array(Tuple(...))` parameter on the right of `IN` is a set of tuples).
  `{name:Identifier}` is substituted by the parser (as in ClickHouse) as one
  quoted identifier wherever a database, table, column or alias name may
  appear; the engine parses with `sqlparse.ParseRequest(query, params,
  settings)`, and plain `sqlparse.Parse` answers NOT_IMPLEMENTED (48) for it.
  A bare `UNION` (no ALL/DISTINCT) takes `union_default_mode` from the
  statement's SETTINGS, then the request/session settings; empty (the 25.8
  default) is `EXPECTED_ALL_OR_DISTINCT` (558).
* **Functions** (case rules as ClickHouse): comparison/logical/arithmetic
  operators; `if`, `multiIf`, `coalesce`, `ifNull`, `nullIf`, `isNull`,
  `isNotNull`, `assumeNotNull`, `toString`, `toUInt8..64`, `toInt8..64`,
  `toFloat32/64`, `toDate`, `toDateTime`, `toDateTime64(x, p)`,
  `parseDateTimeBestEffort[OrNull]`, `parseDateTime64BestEffort`, `now`,
  `now64([p])`, `today`, `toYYYYMM`, `toYYYYMMDD`, `toYear`, `toMonth`,
  `toStartOfDay/Hour/Minute/Month/Week`, `toUnixTimestamp`,
  `formatDateTime`, `dateDiff`, `addSeconds/Minutes/Hours/Days`, `INTERVAL`
  arithmetic, `lower`, `upper`, `length`, `empty`, `notEmpty`, `concat`,
  `substring`, `startsWith`, `endsWith`, `position`, `replaceAll`,
  `splitByChar`, `trim`, `lowerUTF8`, `like`/`ilike`, `match`, `has`,
  `arrayElement`, `length(arr)`, `arrayJoin`, `tuple`,
  `tupleElement`, `toTypeName`, `round`, `floor`, `ceil`, `abs`, `greatest`,
  `least`, `generateUUIDv4`, `version`, `revision`, `timezone`,
  `displayName`, `hostName`, `currentDatabase`, `currentUser`, `uptime`.
  The registry (`reg`/`regCI` in `internal/exec/functions.go` and
  `funcs_*.go`) has more than this minimum, among them the `-OrZero` /
  `-OrNull` conversions, `toDate32`, `toUUID`, `toBool`, `toFixedString`,
  `toTimeZone`, `toStartOf{Second,FiveMinutes,TenMinutes,FifteenMinutes}`,
  `toDayOfWeek/DayOfYear/Quarter/Hour/Minute/Second`, `yesterday`,
  `intDiv`, `trunc`, `isNaN`, `isFinite`, `replaceOne`,
  `replaceRegexpOne/All`, `splitByString`, `substringUTF8`, `left`/`right`,
  `repeat`, `reverse`, `indexOf`, `range`, `arraySort`, `arrayReverseSort`,
  `arraySlice`, `arrayConcat`, `arrayDistinct`, `arrayUniq`, `arrayCount`,
  `arraySum/Min/Max/Avg`, `arrayStringConcat`, `map`, `mapKeys`,
  `mapValues`, `mapContains`, `CAST` and `toTypeName`. Anything not
  registered is `UNKNOWN_FUNCTION` (46), or `NOT_IMPLEMENTED` (48) for
  known-but-unsupported features (lambdas/higher-order functions, window
  functions, parametric functions). Deliberate deviation until the
  per-query memory limit exists: `range` returns at most 2,000,000
  elements per call (`ARGUMENT_OUT_OF_BOUND` 69; ClickHouse allows 500M
  per block), and `repeat`, `replace`/`replaceAll`/`replaceOne`,
  `replaceRegexpOne/All` and `arrayStringConcat` add at most 64 MiB to the
  value they build (`TOO_LARGE_STRING_SIZE` 131; ClickHouse has no such
  cap). A regexp (`match`, `like`/`ilike`, `replaceRegexp*`) may compile
  to about 100,000 instructions, where ClickHouse's RE2 accepts about five
  times that; a larger one fails like a pattern that does not compile.
* **Aggregates**: `count`, `count(x)`, `count(DISTINCT x)`, `countIf`, `sum`,
  `sumIf`, `avg`, `avgIf`, `min`, `max`, `minIf`, `maxIf`, `any`, `anyLast`,
  `argMax`, `argMin`, `uniq`, `uniqExact`, `uniqIf`, `uniqExactIf`,
  `groupArray`, `groupUniqArray`, plus combinators `-If`, `-State`,
  `-Merge`, `-MergeState` and `finalizeAggregation`. Result types
  follow ClickHouse (count/uniq → UInt64; sum(UIntN) → UInt64; sum(IntN) →
  Int64; sum(Float) → Float64; avg → Float64; min/max/any/argMax → arg type).
* Result column names follow `sqlparse.ColumnName` (ClickHouse naming).
* **Settings** (`exec/settings.go`): a `SETTINGS` clause or `SET` naming a
  setting ClickHouse 25.8 does not know (and without the `SQL_` custom
  prefix) fails with `UNKNOWN_SETTING` (115); request (URL) settings may
  carry any name. Settings that change results but are not implemented —
  `limit`, `offset`, `transform_null_in`,
  `aggregate_functions_null_for_empty`, `prefer_column_name_to_alias`,
  `cast_keep_nullable`, `any_join_distinct_right_table_keys`, `final`,
  `data_type_default_nullable`, `group_by_use_nulls`,
  `join_default_strictness` other than ALL, `count_distinct_implementation`
  other than uniqExact — are `NOT_IMPLEMENTED` (48) with a non-default value
  from any source. `session_timezone` as a request/session setting is the
  query's time zone (parsing, Date↔DateTime comparison, `timezone()`;
  `serverTimezone()` stays `TZ_NAME`; invalid zone → `BAD_ARGUMENTS` 36);
  in a `SETTINGS` clause a different zone is `NOT_IMPLEMENTED` (the server
  formats results in the request's zone).
* **Comparison semantics** (verified against ClickHouse 25.8): enums
  compare with enums and numbers by their numeric value, as do ORDER BY,
  `min`/`max`/`argMin`/`argMax` (also their states and
  `SimpleAggregateFunction` merges), `greatest`/`least` and `arraySort`; an
  enum against a constant string converts the string to the enum (an
  unknown element makes the comparison false, except `!=`, which fails with
  691), against a non-constant string it compares the element name. A
  Date/Date32 compared with (or IN-converted to) a DateTime/DateTime64 is
  midnight in the DateTime's explicit zone, else the query's zone (Date32
  vs DateTime always the query's zone, as ClickHouse does); string literals
  compared with a zone-less DateTime parse in the query's zone; key
  pushdown uses the same conversions. FixedString vs String, and
  FixedString(M) vs FixedString(N) with M ≠ N, ignore trailing zero bytes on
  both sides (zero padding; also in IN and key pushdown). Float
  `%` is `x - trunc(x/y)*y`. `toIntN/UIntN` from a string wrap like
  ClickHouse, but reject a `-` sign for unsigned types (72); the `OrNull` /
  `OrZero` variants return NULL / 0 for out-of-range text. LEFT ANTI JOIN
  rows carry the left key values in the right key columns. Deliberate
  deviation: GROUP BY, DISTINCT, `IN` sets, `uniq*`/`count(DISTINCT)` and
  join keys identify floats by value with `-0` = `0` and all NaNs equal
  (`aggstate.Key`), where ClickHouse hashes the bit patterns (`-0. IN (0.)`
  is 1 here, 0 there); persisted `uniqExact` members use the same identity.
* **Rollup arithmetic**: additive state stored in DynamoDB
  (SummingMergeTree columns, `sum`/`avg` parts and
  `SimpleAggregateFunction(sum, …)` of AggregatingMergeTree rows, §4) adds
  integers exactly (DynamoDB `ADD`; read back wrapped to the column width)
  and floats in IEEE arithmetic of the column's width, like ClickHouse
  (`0.1 + 0.2` reads back as `0.30000000000000004`; NaN/±Inf sums are kept;
  §4 "Merged rows" has the compare-and-swap write). Only the order of
  additions can differ from a ClickHouse merge (rows of one key are added
  in application order). A SummingMergeTree row whose summed columns are
  all zero is hidden on read, as ClickHouse drops it when parts merge; the
  test sees the IEEE values, so `0.1 + 0.2 - 0.3` is not zero, as in
  ClickHouse.

**Planner pushdown.** WHERE is split into conjuncts. Predicates on sorting-key
columns become `storage.KeyRange`s:

* `pk = c` / `pk IN (...)` / `(pk, sk1, ...) IN <set of tuples>` give partition
  values (cartesian product, capped at 10 000 ranges; above that fall back
  to a scan);
* equalities on the leading sort-key columns extend `SortPrefix`;
* `BETWEEN`, `<`, `<=`, `>`, `>=` on the next sort-key column set `Lower`/`Upper`;
* tuple-IN sets are grouped by their partition/prefix projection.

Every predicate is still evaluated by the engine on the returned rows.
Without a usable partition predicate the engine performs a full `Scan`
(parallel segments). Subqueries in FROM are planned independently (their own
WHERE is pushed down).

---

## 4. DynamoDB physical layout (`internal/storage/dynamo`, `internal/catalog`)

### Tables

| Physical table | Key | Replication | Purpose |
|---|---|---|---|
| `{prefix}__catalog` | `_pk` S | global | schema entries |
| `{prefix}{db}.{table}.{id}` | `_pk` B, `_sk` B | per engine | table rows |
| `{prefix}{db}.{table}.{id}.members` | `_pk` B, `_sk` B | regional | uniq/uniqExact/groupUniqArray set members of AggregatingMergeTree rows |
| `{prefix}__mv_markers` | `_pk` S, TTL `_exp` | regional | exactly-once markers for view application; per-table reset items `reset#<physical>` (§5 "Resets", no TTL) |
| `{prefix}__checkpoints` | `_pk` S | regional | stream-shard checkpoints and leases (worker, replicator) |
| `{prefix}__dead_letters` | `_pk` S | regional | view applications set aside as poison (§5 "Errors"); created on first use |

All tables use on-demand billing (`PAY_PER_REQUEST`). Streams
(`NEW_AND_OLD_IMAGES`) are enabled on `__catalog` and on every row table
that can have a stream consumer: all except **regional**
Aggregating/Summing tables (rollups), which cannot be view sources (CREATE
VIEW rejects them), are not copied by the replicator and get no AWS
replicas. Rollups are the busiest writers, and DynamoDB Local keeps stream
records (even of dropped tables) for 24 h, slowing every later write. A
rollup with `dynamodb_replication = 'global'` keeps its stream; streams of
existing tables are never disabled.

Physical names must match `[a-zA-Z0-9_.-]{3,255}`; the catalog rejects
database/table names that cannot be mapped. `{id}` is a per-incarnation
suffix (8 random lowercase base32 characters) that the persisted catalog's
`CreateTable` assigns to every new table (CREATE, OR REPLACE, AS/LIKE) and
stores in the entry (`Table.PhysicalName`, included in the length limit);
`UpdateTable` keeps it. Every component resolves tables through the stored
name (`TableByPhysicalName`; checkpoints, pipes and markers keyed by it
belong to one incarnation), so after DROP + CREATE or CREATE OR REPLACE,
rows of the old incarnation still in flight (replicator, stream workers,
regions not yet reconciled) land in the old physical table, which is
dropped, never in the new one; a server writing with a stale snapshot gets
`UNKNOWN_TABLE` ("dropped and created again ...; retry it") once the old
table is gone, and its reloaded snapshot serves the retry. Entries written
by older builds keep their stored name without a suffix; the in-memory
catalog (tests) uses `{db}.{table}`. `catalog.StripIncarnation` removes the
suffix from a name relative to `{prefix}{db}.`.

### Sorting key → DynamoDB key

`ORDER BY (c1, c2, ..., cn)` with partition key length `k`
(default 1; `SETTINGS dynamodb_partition_key = 'c1, c2'` to change):

* `_pk` = `tuple(c1..ck)` encoded; constant `0x01` when k = 0.
* `_sk` = `tuple(ck+1..cn)` encoded, followed for **MergeTree** by a 16-byte
  row uniquifier (8-byte big-endian unix-nanos + 8 random bytes, or for MV
  output into a MergeTree target the first 16 bytes of
  `sha256(view, source row id, index)`); constant `0x01` if empty.

DynamoDB limits the encoded `_pk` to **2048 bytes** and `_sk` to **1024
bytes**, the 16-byte uniquifier included (MergeTree tables and MV append
targets). A string takes its length plus 2 bytes (plus 1 per `0x00` byte),
numbers and dates 9 bytes. A row whose key exceeds a limit is rejected
with `BAD_ARGUMENTS` before anything of its INSERT is written; view output
over the limit fails the view application after the source rows are
stored (the INSERT reports it unless `Config.StreamViews` is set, in which
case it is logged; the stream worker dead-letters it, §5 "Errors").
Long unbounded strings (URLs, paths, user agents) therefore do not belong
in the sorting key; set members over the limit are hashed instead
(`memberSK`: `0xFF ‖ sha256(encoding)`).

Tuple encoding is order-preserving (FoundationDB-tuple style), so
`begins_with` / `BETWEEN` on `_sk` implements prefix equality and ranges:

| Value | Encoding |
|---|---|
| NULL | `0x00` |
| String/FixedString/UUID/Enum | `0x02` bytes (`0x00` → `0x00 0xFF`) `0x00` — UUID as its dashed text, Enum as its name |
| UInt* | `0x16` + 8-byte big-endian |
| Int* | `0x15` + 8-byte big-endian of `v XOR 1<<63` |
| Float* | `0x21` + 8 bytes (positive: flip sign bit; negative: flip all bits) |
| Bool | `0x26` false / `0x27` true |
| Date/DateTime/DateTime64 | `0x33` + 8-byte big-endian of `unixnanos XOR 1<<63` |
| Array/Tuple | `0x05` elements… `0x00` (nested NULL → `0x00 0xFF`) |

The byte order of an Enum (by name) or a UUID (by text; ClickHouse orders
it as a UInt128 whose high half is the last 8 bytes) is not ClickHouse's
order, so `keyenc.RangeSafe` is false for them and for composites holding
them: a range predicate on such a key column uses only the equality prefix
before it, and the engine filters the rest. The engine never relies on
storage key order for ORDER BY, so sorted output is unaffected. A future
per-table key-format version could encode UUIDs as `0x02` +
`chtype.UUIDOrderKey` and re-enable ranges on them.

### Row item attributes

* `_pk`, `_sk` (B) — keys.
* `_rid` (S) — row id (unique per insert; MV-output rows: deterministic).
* `_ts` (N) — write time, unix nanos (LWW tie-break for replication).
* `_origin` (S) — region where the row was written.
* `_mv` (BOOL) — true for rows written by `Merge` (view output).
* `_so` (S) — view output in ReplacingMergeTree targets only: source-order
  tie-break (source row id, output index); `_ts` of such an item is the
  source row's write time.
* `#cas` (S) — merged (Aggregating/Summing) rows with float sums only: the
  compare-and-swap token of their float sum attributes (see "Engine write
  semantics").
* One attribute per column, **named exactly as the column**. Names starting
  with `_` that collide with the system attributes above are rejected at
  CREATE; column names cannot contain `#`.

Column value encoding:

| Type | DynamoDB |
|---|---|
| String/FixedString/UUID/Enum | S (B if not valid UTF-8) |
| (U)Int*, Float* | N (Float NaN/±Inf → S `"nan"`/`"inf"`/`"-inf"`) |
| Bool | BOOL |
| Date/DateTime/DateTime64 | N (unix nanos) |
| NULL | NULL |
| Array, Tuple | L |
| Map | L of 2-element L |
| SimpleAggregateFunction(f,T) | as T |
| AggregateFunction(f, …) column `c` | flattened: `c#n` (N count), `c#s` (N sum), `c#v` (value), `c#k` (argMin/argMax key), `c#h` (BOOL has value), `c#z` (N set size), `c#l` (L groupArray) |

### Engine write semantics

* **MergeTree**: `PutItem` (conditional `attribute_not_exists(_pk)`, the
  uniquifier makes collisions impossible). Batches use `BatchWriteItem`
  (25/item batch, unprocessed-item retry with backoff).
* **ReplacingMergeTree(ver)**: `PutItem` with condition
  `attribute_not_exists(_pk) OR <stored ver NULL/absent> OR #ver < :ver OR
  (#ver = :ver AND <later>)`, without a version column
  `attribute_not_exists(_pk) OR <later>`, where `<later>` is the
  replicator's order `_ts < :ts OR (_ts = :ts AND _origin < :o)`. Write
  times come from a per-process monotonic clock, so rows of one INSERT that
  share a key get increasing `_ts` (the last one wins). On a failed
  condition the stored `_ts` is read (`ReturnValuesOnConditionCheckFailure
  = ALL_OLD`): if it is not older than the row's, only the clock lost and
  the row is re-stamped `stored _ts + 1` and written again (bounded);
  otherwise the row is superseded by a higher version
  (`InsertResult.Skipped`). Real AWS global tables (`GLOBAL_MODE=aws`,
  no version column) keep the plain rule (`#ver <= :ver` / unconditional):
  AWS resolves their conflicts itself.
* **Item size**: every item of a MergeTree/ReplacingMergeTree INSERT or of
  view output into such targets is sized like DynamoDB does (attribute
  names included) before the first write; a row over 400 KB rejects the
  whole statement with `BAD_ARGUMENTS`. (Batches are written in parallel,
  so a late failure would leave an arbitrary subset stored.) For merged
  rows only the bytes one merge adds can be checked up front (the stored
  item grows); DynamoDB's own rejection is the backstop, reported as
  `BAD_ARGUMENTS` (36) with DynamoDB's reason (`ddbError`; inside a
  transaction it is a `TransactionCanceledException` with a
  `ValidationError` reason, `awsx.DeterministicCancel`), and dead-lettered
  by the stream worker (§5 "Errors"). Deterministic
  limits (keys, item size) are thus all-or-nothing; a transient failure
  that outlasts the retries (throttling, network) mid-INSERT can still
  leave part of the INSERT written, as ClickHouse can for a multi-block
  INSERT.
* **Merged rows** (SummingMergeTree, AggregatingMergeTree; direct INSERT and
  `Merge`): the rows of one call that share a key are **pre-aggregated**
  and applied as one group; distinct keys run in parallel. Every row is
  planned and validated before the first write. Step 1 is one update of
  the item carrying, folded over the group's rows in order: `SET` of the
  key columns and system attributes; `ADD` of the integer parts (`count`
  `c#n`, integer `sum` `c#s`, `avg(Int)` `c#s`/`c#n`, integer
  SimpleAggregateFunction `sum` and SummingMergeTree columns), summed
  exactly; `if_not_exists` for `any` (first row) and non-summed columns,
  `SET` for `anyLast` (last row), one `list_append` with every row's
  `groupArray` elements in order. **Float parts** (`Float32/64` sums,
  `avg(Float*)` `c#s`) are not `ADD`ed: DynamoDB sums its 38-digit decimals
  exactly and rejects a sum needing more digits (`1500000.25 + 5.55e-17`),
  which would drop the whole update and keep failing for the key. Instead
  the writer reads the stored values and the row's `#cas` token (strongly
  consistent), adds in Go (IEEE, rounding each step to Float32 for Float32
  columns), and writes `SET c#s = :new, #cas = :fresh` conditioned on `#cas
  = :read` (`attribute_not_exists(#cas)` when absent); NaN/±Inf/out-of-range
  results are stored as S like plain Float columns. A failed condition
  (another writer) is retried over the values returned with the failure
  (`ALL_OLD`); later chunks of the group reuse the values just written.
  The update writes every column of the row in one expression, which
  DynamoDB caps at 4 KB: the catalog (`Options.CheckTable`) rejects at
  CREATE/ALTER, with `NOT_IMPLEMENTED`, a merging table whose longest update
  exceeds it (about 500 summed/count, 250 avg or 150 any/anyLast/groupArray
  columns; min/max/uniq columns are written separately and are not
  limited).
* **AggregatingMergeTree/SummingMergeTree `Merge(rows)`** — exactly once per
  (SourceRef, key, index):
  1. Per chunk of up to 99 rows of the group, one `TransactWriteItems`:
     each row's `Put __mv_markers{_pk: hex(sha256(view, rowID, table
     instance, _pk, _sk, index)), _exp, t[, d]}` with
     `attribute_not_exists(_pk)` (`ReturnValuesOnConditionCheckFailure =
     ALL_OLD`) **+** the folded row update of those rows (above). A marker
     that exists cancels the transaction: its row was already applied
     (atomically with its marker) and is dropped, and the update is rebuilt
     from the remaining rows only, so the deltas always match the markers
     written with them. Duplicate applications within one call count once.
  2. Extremum candidates (min/max/argMin/argMax) of the rows that still
     need it are folded in Go (`aggstate.ReplacesTyped`) to one idempotent
     conditional `UpdateItem` per column (e.g. `SET c#v=:v, c#h=true` if
     `attribute_not_exists(c#h) OR c#v < :v`; compare-and-swap for types
     DynamoDB does not order like ClickHouse); ConditionalCheckFailed is
     ignored.
  3. Set members (uniq/uniqExact/groupUniqArray): the union of the rows'
     members per column, `BatchGetItem` of which exist, then per chunk of
     missing ones `TransactWriteItems`: `Put members{_pk: rowkey‖col, _sk:
     enc(member)}` with `attribute_not_exists(_pk)` **+** `ADD c#z <n>` on
     the row; members added concurrently cancel it and are dropped.
  4. Markers of rows with steps 2/3 are written `d = true` (done) once both
     finished; a row without steps 2/3 gets `d` in step 1. A re-application
     (the stream worker after inline application, redelivery) learns from
     the cancelled step-1 transaction (`ALL_OLD`) which markers are done and
     skips those rows entirely; a marker without `d` (a crash between the
     steps, or written by an older build) runs steps 2/3 again, which are
     idempotent.
  So N rows of one hot key cost about N/99 marker transactions, one
  extremum write per column and one member pass instead of 2N serial
  transactions; a replay costs one cancelled transaction per 99 rows.
  The table instance in the marker id is `physical@CreatedAt[@cut]` (the
  region's reset epoch, §5 "Resets"): a recreated or truncated table does
  not inherit markers, which are never deleted (they expire through `_exp`).
* **Merge into MergeTree targets** (non-aggregating MVs): row uniquifier
  derived from (view, rowID, index), so re-application overwrites the same
  item.
* **Merge into ReplacingMergeTree targets**: view output is ordered by its
  **source row**, not by when it is applied: the item's `_ts` is the source
  row's write time (carried by the source row id, §5) and `_so` = (source
  row id, output index); the condition is the Replacing one above with
  `<later>` = `_ts < :ts OR (_ts = :ts AND (attribute_not_exists(_so) OR
  _so <= :so))`. The stream worker re-applying rows that inline application
  already applied, applying shards in any order, or a region without inline
  application all keep the row ClickHouse keeps; re-applying the same output
  rewrites the same item, an older output is skipped. Output whose source id
  carries no write time is ordered by application time.

### Cost model

On-demand DynamoDB bills a standard write 1 WRU per started KB of the item
(an `UpdateItem` by the larger of the old and new item), a transactional
write 2 WRU per started KB, a strongly consistent read 1 RRU per started
4 KB, and a write whose condition fails still consumes capacity. Global
tables also bill a replicated write in every other region. In those units,
one source row costs (per region unless noted):

| Operation | Cost |
|---|---|
| MergeTree / Memory / Log row (`BatchWriteItem` put) | 1 WRU per KB of the item, plus one replicated write per KB in each other region |
| ReplacingMergeTree row (conditional `PutItem`) | 1 WRU per KB (also when superseded); a re-stamp after a lost clock race writes again. Each other region: the replicator's conditional put (1 WRU per KB) |
| View output into a MergeTree target | 1 WRU per KB of the output item; a re-application rewrites it (another 1 WRU per KB) |
| View output into a Summing/Aggregating target, per output row | 2 WRU marker (transactional put, < 1 KB) |
| … + the folded row update | 2 WRU per KB of the rollup item, once per ≤ 99 rows of one key in one `Merge` call (one 1000-row INSERT batch, or one stream batch) |
| … + views with min/max/argMin/argMax/uniq*/groupUniqArray | 1 WRU `d` (done) marker rewrite; one conditional `UpdateItem` (1 WRU per KB of the rollup item) per extremum column per key group (plus a consistent `GetItem` for compare-and-swap types); per set column a consistent `BatchGetItem` of the members (1 RRU each) and 2 WRU per new member, with a 2 WRU-per-KB `ADD c#z` per 50 new members |
| … + float sums | one consistent `GetItem` of the `#cas` token and float attributes per key group, plus a re-read on contention |
| … + every `Merge` call | one consistent `GetItem` of the target's reset item |
| Re-application (stream worker after inline, redelivery) | one cancelled transaction per ≤ 99 rows of a key (budget it like the original); no extremum, member or done-flag call when every marker is done |
| Direct `INSERT` into a Summing/Aggregating table | one `UpdateItem` (1 WRU per KB of the item) per key group per batch, no markers |

Every region applies a view to every source row once (its own rollups), so
a view's marker and update costs are paid in every region, and markers
(about 150 bytes) stay stored for `MarkerTTLDays` (default 7; TTL deletion
is free). Stream consumers (mvworker, the replicator, `RUN_STREAM_WORKER`)
read DynamoDB Streams with `GetRecords`, billed as stream read requests
except for Lambda triggers. Reads without a usable key predicate are full
parallel `Scan`s, billed by the bytes scanned.

### Reads

* `KeyRange` → `Query` (`_pk = :pk AND begins_with(_sk, :prefix)` or
  `_sk BETWEEN :lo AND :hi`), paginated. Inclusive bounds may be used for
  exclusive predicates (the engine filters).
* No ranges → parallel `Scan` (4 segments by default).
* A `ProjectionExpression` names the requested columns' attributes; one
  that would exceed DynamoDB's 4 KB expression limit (hundreds of columns)
  is dropped and whole items are read (only requested columns are decoded).
  The compare-and-swap `GetItem` of float sums falls back the same way.
* `ConsistentRead` when requested.
* AggregateFunction columns are rebuilt into `*chtype.AggPartial`; for sets
  `SetSize = c#z`, `SetLoaded = (c#z == 0)`, `LoadSet` queries the members
  table for that row/column.
* A column attribute missing from an item (the column was added by
  `ALTER TABLE ADD COLUMN` after the item was written) decodes as the type's
  zero value — or, when `ScanRequest.MarkAbsent` is set and the column has a
  `DEFAULT`/`MATERIALIZED` expression (`storage.MarksAbsent`), as
  `storage.Absent`, and the engine evaluates the default expression over the
  row (ClickHouse computes defaults at read time for parts that predate the
  column). `DeleteRows` treats a missing attribute as matching any value.
* `ALTER TABLE MODIFY COLUMN` is catalog-only; the catalog records the
  column's earlier types (`Column.PrevTypes`, maintained by `UpdateTable`).
  A stored value the current type cannot decode is decoded with the newest
  earlier type that accepts it and converted with `chtype.Convert` (CAST):
  Int64 `5` → String `'5'`, Float64 `2.7` → Int32 `2`, String `'7'` →
  UInt8 `7`, DateTime → String `'2024-01-01 13:00:00'`. Values the new type
  decodes directly need no conversion: integer width changes wrap like
  ClickHouse, and Date/DateTime/DateTime64 values are truncated to the
  type's precision (DateTime → Date drops the time of day, in UTC).
* The scan iterator implements `exec.RowIDIterator`: `RowID()` is the
  item's `_rid` with its `_ts` (never-gated form, §5).

### Catalog items (`__catalog`)

`_pk` = `db#<name>` | `table#<db>.<name>` | `view#<db>.<name>`;
attribute `def` (S, JSON of the model struct with column types as text),
`kind`, `_ts`, `_origin`. Clauses with no storage effect are persisted so
`SHOW CREATE TABLE` / `DESCRIBE` can report them like ClickHouse:
`Column.Codec` (normalized with `catalog.NormalizeCodec`, e.g.
`Delta(8), ZSTD(1)`), `Table.PartitionBy`, `Table.PrimaryKey` and
`Table.SettingsSQL` (declaration order, literals as written;
`Table.HomeRegion` (the region whose `CreateTable` wrote the entry,
`catalog.Options.Region`), `Table.ResetAt` (latest TRUNCATE, §5 "Resets";
`UpdateTable` never lowers it), `Column.PrevTypes` (§4 "Reads");
`Table.ShowSettings()` appends ClickHouse's implicit
`index_granularity = 8192` for MergeTree-family engines). The in-memory snapshot is rebuilt by `Refresh`
(Scan). Database `default` always exists (implicitly). Besides the periodic
refresh, the engine reloads the snapshot when a statement looks up a
database, table or view the snapshot lacks (and when an INSERT names a column
the snapshot's table lacks), then retries once: a reload that started after
the statement began satisfies it, concurrent misses share one reload, and
miss-triggered reloads start at most every 50 ms. So DDL on one server is
visible to the next statement on another server sharing the catalog. A
table *replaced* elsewhere (DROP + CREATE with another layout) still
resolves to the stale entry until the periodic refresh, but writes with it
go to the old incarnation's physical table (see "Tables"), never misencoded
into the new one.

`Refresh` decodes entries independently. An entry it cannot decode (e.g.
written by a newer build with a type this one cannot parse), or an item the
backend cannot read at all (`catalog.KindMalformed`), is **quarantined**
(`catalog.QuarantinedEntries`): it is left out of the snapshot while every
other entry loads, so one bad entry cannot halt refreshes, the stream
workers or startup. A lookup of a quarantined table fails with
`INCORRECT_DATA`; the reconciler logs it once and never drops its physical
table (nothing at all while an item of unknown physical name exists); the
stream worker treats records of a quarantined table, or of a source with a
quarantined view, as transient failures (retried, never dead-lettered or
skipped) until a build that can decode the entry runs. `DROP DATABASE`
removes quarantined entries of the database too. Local DDL made while a
`Refresh` scan runs is replayed onto the loaded snapshot (the scan may have
passed its key before it committed), so a statement on a server always
sees the DDL the server itself executed before it.

---

## 5. Materialized views (`internal/exec`, `cmd/mvworker`)

ClickHouse semantics: an MV is an INSERT trigger — its SELECT runs over each
inserted block of the source table and the output is inserted into the target.
Here, to get exactly-once identity, the engine evaluates the view **per
source row** (a one-row block; aggregate states merge associatively so the
result is identical) and calls `Store.Merge` with
`SourceRef{View, RowID}`. Output columns are matched to target columns **by
name** (ClickHouse behaviour); missing target columns get defaults; values
are converted to target column types.

`Engine.ApplyViews(table, rows, rowIDs)` is called by:

1. the INSERT path when `InlineViews` (after the source rows are persisted);
2. the stream worker for every `INSERT`/`MODIFY` record of a view-source table
   whose image is not MV output (`_mv` absent).

Like ClickHouse, a view only sees rows **inserted after it was created**: the
stream worker applies view `v` to a record only if the row's write time
(`_ts`) is `>= v.CreatedAt` (rows without `_ts`, and views without a creation
time, are not filtered). This matters because a table's stream is read from
`TRIM_HORIZON` when it gets its first view, which replays rows written before
the view existed. `_ts` and `CreatedAt` are replicated unchanged, so every
region makes the same decision. Rows written between two views' creation go
only to the older view (`Engine.ApplyViewList`, when the engine provides it;
otherwise to all of the table's views).

`CREATE MATERIALIZED VIEW ... POPULATE` scans the source and applies the view
to every existing row, using each row's stored row id (`_rid`, exposed by
the store's iterator through the optional `exec.RowIDIterator`) as
`SourceRef.RowID`, so its exactly-once markers coincide with those of the
inline INSERT path and the stream worker for the same rows.

A view's output must be a function of the source row, because the inline
path, POPULATE and every region's stream worker evaluate it separately and
the exactly-once marker of an output row includes the target key: CREATE
MATERIALIZED VIEW rejects (`NOT_IMPLEMENTED`) a SELECT whose output column
feeding a sorting-key column of the target depends on `now()`, `today()`,
`generateUUIDv4()`, `rand*()`, `hostName()`, `currentUser()` and similar
(volatile values in other columns are allowed: the key's marker keeps them
applied once, with the value of whichever evaluation came first).

The view sees every row inserted into a ReplacingMergeTree source,
including versions the store did not keep, in a single-region deployment;
with `Config.MultiRegion` a version that was already superseded when written
feeds no view (§1).

POPULATE fills only the executing region's copy of the target, so with
`Config.MultiRegion` it is rejected (`NOT_IMPLEMENTED`) when the target is
REGIONAL (a global target replicates the backfill).

Source row ids carry the source row's write time: `Insert` and
`DecodeRecord` return `<_rid>@<_ts>`, the scan iterator's `RowID()` (used by
POPULATE) `<_rid>@=<_ts>`; other ids are opaque. Exactly-once markers and
MV-output keys use only `<_rid>`, so all forms name the same source row. The
store uses the write time to order view output into Replacing targets (§4)
and to apply reset epochs; ids in the second form (POPULATE) are never
gated.

### Resets

`TRUNCATE` (and DROP + CREATE) of a view target must not let source rows
written before it reach the new target: the stream worker may still have
them in its backlog, and an inline application may race the TRUNCATE.
Every region keeps, per physical table, a reset item `reset#<physical>` in
its regional `__mv_markers` table: the table instance (`created` =
`CreatedAt`), the region's **cut** (reset epoch) and the newest catalog
`ResetAt` applied, plus a lease while a reset runs.

* `TruncateTable` claims the item (conditional writes on a version
  attribute), stamps the cut *after* the claim landed, deletes the copy's
  rows and members (renewing the lease; a crashed reset is taken over when
  the lease expires) and releases it. Markers are not deleted.
* `Merge` reads the item (strongly consistent) first: it waits while a reset
  runs (like ClickHouse, where TRUNCATE holds the table lock), applies a
  catalog reset not yet applied in this region, then drops view output of
  source rows (gated ids, §5) written before `max(target CreatedAt, cut)`
  and tags its markers with the cut. A rebuild (`INSERT INTO target
  SELECT ...`) or POPULATE after the reset is therefore applied once, the
  worker's backlog is not re-applied, and truncated rows do not come back.
  A caller whose catalog snapshot predates a DROP + CREATE uses the
  instance recorded in the item.
* An inline application that read the item before a local TRUNCATE claimed
  it only carries rows written before the cut: whether it lands before or
  after the deletion, the worker later drops those rows, so a row is never
  counted twice.
* Propagation: TRUNCATE records `Table.ResetAt` in the replicated catalog
  (`catalog.MarkTruncated` + `UpdateTable`) before calling `TruncateTable`.
  Every region's reconciler re-runs `EnsureTable` for the new catalog
  version, which applies the reset to its copy: a regional table's rows are
  deleted, with the catalog `ResetAt` as the cut (the same `_ts` cut in
  every region); for a global table only the cut is recorded (the issuing
  region's deletes replicate). `Merge` applies a pending catalog reset
  itself if it sees it first. A region's copy created later starts with the
  catalog's cut.
* Boundaries are approximate by the writers' clock skew, like the view
  `CreatedAt` filter. Rows inserted between a TRUNCATE and a manual rebuild
  are counted twice, as in ClickHouse.

Limitations (v1): MVs do not cascade (MV output does not trigger other MVs);
JOINs inside MV SELECTs are not supported; a view cannot read FROM a
SummingMergeTree/AggregatingMergeTree table (`NOT_IMPLEMENTED`: its stream
carries merged per-key state under one row id, not the inserted rows; a
view of such a source created by an older build is ignored by `ApplyViews`
and `ApplyViewList`, i.e. by the inline path and the stream worker alike,
and the worker does not read such a table's stream at all: regional rollups
are created without one, §4 "Tables"; a stream reader started on a table
without a stream logs once and keeps polling for one instead of failing).

### Stream worker (`internal/streams`, `cmd/mvworker`)

* Discovers view-source tables from the catalog (refreshing periodically);
  for each, reads its DynamoDB stream (`DescribeTable` → `LatestStreamArn`,
  `DescribeStream` → shards, `GetShardIterator` / `GetRecords`).
* Processes a parent shard to completion before its children; checkpoints
  the last processed sequence number per shard in `__checkpoints` after the
  batch is applied; resumes from `AFTER_SEQUENCE_NUMBER` (or `TRIM_HORIZON`
  for unseen shards). While nothing has been read from a shard, each idle poll
  uses a fresh `TRIM_HORIZON` iterator (DynamoDB Local invalidates the
  iterators of an empty shard as other tables advance its global sequence
  numbers).
* Catalog freshness: besides the periodic refresh (`CATALOG_REFRESH`), the
  worker reloads its catalog snapshot (strongly consistent Scan) before
  applying a batch whose newest row write time is later than the start of the
  last reload, so a `DROP`/`CREATE MATERIALIZED VIEW` committed before a write
  is always honoured for that write. Concurrent batches share reloads.
  Cost: a reload is a consistent Scan of all of `__catalog` (charged per
  4 KB, one partition), and under steady writes nearly every batch is newer
  than the last reload, so reloads are spaced at least `CATALOG_MIN_RELOAD`
  (default 1s) apart: a batch that needs one sooner waits for it, and every
  batch of every table and shard of the process that queued meanwhile is
  covered by that one reload. That bounds the Scans to about one per second
  per worker process (in-process workers, standalone mvworkers, Lambda
  environments, the replicator's conflict appliers) for at most 1s of extra
  view latency; `Stats.CatalogReloads` counts them.
* Leases: one owner per shard via conditional writes on `__checkpoints`
  (`lease_owner`, `lease_expires`), so several workers per region are safe.
* Errors: the reader retries a failed batch with backoff (`Batch.Attempt`,
  `FailingSince`, handler-owned `Scratch`) and never skips records itself.
  The worker classifies the failure:
  * transient (throttling, network, `Unavailable`/`TIMEOUT_EXCEEDED`,
    condition races, a DynamoDB table still being created, a failed catalog
    reload, anything it cannot classify): retried forever;
  * deterministic (an engine error about the data or the view —
    `CANNOT_PARSE_*`, `TYPE_MISMATCH`, `UNKNOWN_TABLE` for a dropped target,
    … — a DynamoDB `ValidationException` such as an item over 400 KB, or
    its transactional form, a `TransactionCanceledException` with a
    `ValidationError` reason and no conflict/throttling reason
    (`awsx.DeterministicCancel`; e.g. a `groupArray` rollup row that would
    grow past 400 KB — rollup state is bounded by the DynamoDB item size
    limit), an undecodable image): the batch is applied record by record, and view by
    view, so the good records and the record's other views land at once
    (markers make re-application a no-op). A record that keeps failing for
    `MV_POISON_AFTER` (default 30s) is written to the regional
    `{prefix}__dead_letters` table (`_pk` = table|seq|view; table, view,
    seq, row_id, origin, error_code/error_name/error, at, image) and logged
    at ERROR (`DEAD LETTER`), and the batch then succeeds so the checkpoint
    advances. This matches the writer region, where the view output was
    not applied either (the source row itself is stored before the views
    run; the INSERT got the error, or with `Config.StreamViews` the
    failure was logged). `MV_POISON_POLICY=block` retries such
    records forever instead.
  The Lambda entry point and the replicator's conflict applier (§6) use the
  same policy (Lambda tracks the failure age per execution environment and
  reports the group's first record as the batch item failure until then).
* Lag: the reader reports `RetryingBatches` and `OldestPendingAt` (creation
  time of the oldest record of a batch being retried); `cmd/mvworker` logs
  them as `retrying_batches`/`pending_age_ms` with `dead_lettered` and
  `catalog_reloads`, at ERROR once the age passes 1h, and the reader logs
  `SHARD WEDGED` per retry past `StuckAlertAge` (1h): stream records are
  trimmed after 24h, and a checkpoint beyond retention restarts from
  `TRIM_HORIZON` (`DATA LOSS`). Alert on these well before 24h.
* Discovery: each reader re-describes its stream every `DiscoveryInterval`
  (at least 5s for the worker and the replicator: DescribeStream is limited
  to ~10 TPS per account); consecutive DescribeTable/DescribeStream failures
  (e.g. `LimitExceededException`) back off exponentially up to
  `MaxDiscoveryBackoff` (60s), while a finished parent shard still triggers
  an immediate rescan.
* `cmd/mvworker` also has a Lambda entry point (`-lambda` / when
  `AWS_LAMBDA_RUNTIME_API` is set) consuming `events.DynamoDBEvent` from an
  event-source mapping.

### Stream worker as a Lambda (`internal/esm`, `cmd/reconciler-lambda`)

In the serverless deployment (`deploy/aws-lambda`) no process polls streams: Lambda
event-source mappings (ESMs) deliver each table's stream records to the
worker. Tables are created at runtime, so their ESMs are too: the
**reconciler** (§2) owns them, idempotently, in every region.

* One ESM per source-table stream → `MVWORKER_FUNCTION_ARN` (MergeTree,
  ReplacingMergeTree, Memory, Log tables, i.e. every table a view can read
  from), with `StartingPosition TRIM_HORIZON` (a new table's stream is read
  from the start; exactly-once markers make replays no-ops), `BatchSize
  ESM_BATCH_SIZE` (100), `FunctionResponseTypes ReportBatchItemFailures`
  (the handler reports the first failed record and Lambda retries from
  there, §5 "Stream worker"), `BisectBatchOnFunctionError false` (the
  handler isolates poison records itself) and `MaximumRetryAttempts -1`
  (retry until the record expires from the stream, 24 h, like the
  long-running worker). With `GLOBAL_MODE=aws`, a second ESM per global
  ReplacingMergeTree table → `REPLICATOR_FUNCTION_ARN` (§6: those tables
  are copied by the replicator, not by AWS). The `{prefix}__catalog` stream
  → the reconciler itself. Regional rollups (no stream) and the other
  system tables get none.
* `esm.Manager.Ensure` lists the mappings of (stream, function), creates
  the mapping when absent, enables it when disabled and updates it when
  its settings drifted; a mapping that already matches costs one
  `ListEventSourceMappings` call, and the reconciler calls it once per
  table definition per process (a cold start re-checks everything). A
  concurrent creation (`ResourceConflictException`: two regions'
  reconcilers never race, each manages its own region, but a schedule
  and a catalog-stream invocation can overlap) converges on the existing
  mapping; a mapping in transition (`Creating`/`Updating`/`Deleting`,
  `ResourceInUseException`) is reported as pending and retried next
  round; throttling (`TooManyRequestsException`) is retried with backoff.
  `Remove` deletes the mapping(s) of a stream, `RemoveByTable` those of a
  table name (used once the table, and its stream ARN, is gone).
* Regional stream ARNs: with `GLOBAL_MODE=aws` every replica of a global
  table has its own stream in its region, so each region's reconciler
  wires its own region's streams to its own region's functions.
* A mapping deleted by hand is recreated by the next cold start of the
  reconciler (or when the table's definition changes); the schedule
  guarantees a round at least every minute. Conversely, the mappings of a
  table dropped while no reconciler of the region was running are found
  by `esm.Consumers.ConsumedTables` (the two functions' mapping lists,
  table name from the stream ARN) and removed by the reconciler's
  discovery path (§2) after the usual `DropAfter` rounds and catalog
  re-read.

---

## 6. Multi-region: local emulation and AWS (`cmd/replicator`, `deploy/local`, `deploy/aws-lambda`, `deploy/aws-ecs`)

The local stack (`deploy/local/docker-compose.yml`, `make up`):

* Two DynamoDB Local containers (`dynamodb-us`, `dynamodb-eu`) represent
  `us-east-1` and `eu-west-1`.
* `chserver-us` (:8123) and `chserver-eu` (:8124), each with a separate
  `mvworker` service, `GLOBAL_MODE=emulated` (the `single` profile runs
  one region on :8125 with the worker in-process, `RUN_STREAM_WORKER=true`).
* `cmd/replicator` emulates global tables: for each ordered region pair
  (A→B) and each **global** table (including `__catalog`), it tails A's stream
  and applies records whose image has `_origin == A` to B:
  * INSERT/MODIFY → `PutItem` with condition
    `attribute_not_exists(_pk) OR _ts < :ts OR (_ts = :ts AND _origin < :o)`
    (LWW), preserving `_ts`/`_origin` (which prevents ping-pong loops);
    for ReplacingMergeTree(ver) tables (the replicator reads `VersionColumn`
    from the catalog) the version decides first:
    `attribute_not_exists(_pk) OR <target ver NULL/absent> OR ver < :ver OR
    (ver = :ver AND <LWW as above>)` — NULL/absent is the smallest version,
    as in the local write path;
  * a rejected INSERT/MODIFY (the target holds a newer version; the put
    asks for the existing item, so a redelivery of the same row version,
    same `_rid`, is recognised and skipped) is handed to the target region's
    materialized views (`APPLY_CONFLICT_VIEWS`, §1): the replicator holds a
    per-region engine and `mvworker.ImageApplier`;
  * REMOVE → `DeleteItem` with condition `_ts <= :ts`.
  * Optional artificial lag `REPLICATION_DELAY` to demo convergence.
  * Table discovery runs every `CATALOG_REFRESH`; each pipe's stream reader
    re-describes its stream every `STREAM_DISCOVERY_INTERVAL` (default and
    minimum 5s), since every (table, from, to) pipe polls DescribeStream.
* With `GLOBAL_MODE=aws`, `EnsureTable` instead creates real global tables
  (`UpdateTable ReplicaUpdates` for `REPLICA_REGIONS`) — except for
  ReplacingMergeTree tables, with or without a version column, which stay
  per region (`catalog.CopiedByReplicator`, §1). Only the
  table's **home region** creates a global table and adds its replicas:
  the region whose DDL created it (`Table.HomeRegion`); for `__catalog`,
  and entries without one, `Options.HomeRegion` or else the alphabetically
  first of `REGION` and `REPLICA_REGIONS` (the same in every region). Every
  other region only joins: `EnsureTable` reports the table pending
  (`UNAVAILABLE`, the reconciler retries) until the replica exists, and
  `EnsureSystemTables` waits for the `__catalog` replica. AWS refuses to add
  a replica where a table of that name already exists, so a region creating
  its own copy first would leave the regions unreplicated for good; that
  refusal is reported with the region holding the stray table. `DropTable`
  removes the other regions' replicas first and then deletes the table;
  AWS refuses that deletion within 24 hours of adding a replica, which
  the store reports as deferred (§2) so `DROP TABLE` still succeeds and
  the reconciler deletes the table once allowed. The replicator
  then runs with `GLOBAL_MODE=aws` and `REGIONS` naming the real regions
  (empty endpoints): it skips `__catalog` and the AWS global tables and
  copies only the ReplacingMergeTree tables, with the version-aware
  condition above for ReplacingMergeTree(ver) and the plain LWW condition
  for the others, and its conflict hook (`APPLY_CONFLICT_VIEWS`) for both.
  It connects to every region it lists, so it can run anywhere; several
  replicator processes can run at once (shard leases), e.g. one per region
  for availability. While it is down, ReplacingMergeTree rows written in
  one region are not visible in the others; they catch up from the streams
  (24 h retention) when it restarts.
* **Replicator as a Lambda** (the serverless deployment, `deploy/aws-lambda`):
  `cmd/replicator -lambda` (automatic when `AWS_LAMBDA_RUNTIME_API` is set)
  runs the same per-record logic without a poller. The per-record logic is
  `replicator.Applier` (`NewApplier(source, target, table, conflict hook)`,
  `Apply(ctx, Record)`), shared by the poller's pipes and the Lambda entry
  point `replicator.Lambda`; a `Record` is normalised from either the
  Streams API or a Lambda event (`RecordFromStream`, `RecordFromLambda`,
  images converted with `mvworker.FromLambdaItem`). One function per
  region, with `GLOBAL_MODE=aws` (validated: `REGION`, `REPLICA_REGIONS`,
  no `DYNAMODB_ENDPOINT`; `REGIONS` is not used), fed by the event-source
  mappings the reconciler keeps on that region's ReplacingMergeTree table
  streams (`REPLICATOR_FUNCTION_ARN`, `ReportBatchItemFailures`). For each
  record it derives the table from `eventSourceARN`, resolves the version
  column through this region's catalog (`replicator.CatalogLookup`:
  snapshot refreshed when older than `CATALOG_REFRESH`, and once more when
  the table is missing; a table still missing was dropped and its records
  are skipped; only global ReplacingMergeTree tables are copied), skips
  images whose `_origin` is not `REGION` (replicas written by the other
  regions' functions), and applies the conditional put / delete above to
  every `REPLICA_REGIONS` region through SDK clients built once per
  execution environment (`awsx.New` per region, standard endpoints). The
  conflict hook keeps its behaviour: a losing row version feeds the target
  region's views through that region's `mvworker.ImageApplier`. Records of
  one item are applied in stream order, distinct items concurrently; a
  failed write (or hook) is reported as a batch item failure for the
  earliest record not applied to every target, so Lambda retries from
  there (`MaximumRetryAttempts -1`: until the records expire): every
  earlier record is applied, and re-applying a later one is a no-op under
  the conditions (a redelivered row version finds itself in the target,
  same `_rid`, and skips the hook). No checkpoints table is used: Lambda
  tracks the shard position. `REPLICATION_DELAY` and the stats log do not
  apply in this mode.

`deploy/local/docker-compose.yml` also has a `clickhouse` service
(`clickhouse/clickhouse-server:25.8-alpine`, port 18123) used by the
differential compatibility tests.

* **Serverless stack** (`deploy/aws-lambda`, AWS SAM, `GLOBAL_MODE=aws`): one
  CloudFormation stack per region with four functions (API, stream
  worker, replicator, reconciler), a private REST API (resource policy
  allowing one `execute-api` VPC endpoint, `BinaryMediaTypes */*`), the
  reconciler's `rate(1 minute)` schedule, log groups and parameters
  (`TablePrefix`, `GlobalMode`, `ReplicaRegions`, `HomeRegion`,
  `VpcEndpointId`, `ChUsers`, timeouts, memory sizes, …). Each function's
  IAM role is scoped to `table/{prefix}*` (§2 "IAM"). The template
  declares no DynamoDB tables: the reconciler's `EnsureSystemTables` and
  `EnsureTable` create them, as the long-running binaries do, which is why
  the **home region's stack is deployed first** (its first reconciler run
  creates `__catalog` with replicas; a replica region's `EnsureSystemTables`
  waits for its replica before creating the regional system tables). The
  reconciler keeps the event-source mappings in step with the tables (§5),
  so no stack resource depends on the schema. `samconfig.toml` has the
  `us` (home) and `eu` environments; `sam build` compiles each `cmd`
  directory in place (`BuildMethod go1.x`, cache disabled because it hashes
  only the function directory). `sam local start-api` runs the API Lambda
  against the local stack's DynamoDB Local (`make aws-local-api`,
  `GLOBAL_MODE=emulated`, event-source mapping management off because
  `MVWORKER_FUNCTION_ARN` is rejected together with `DYNAMODB_ENDPOINT`).
* **Container stack** (`deploy/aws-ecs`, plain CloudFormation,
  `GLOBAL_MODE=aws`): one stack per region on ECS Fargate in an existing
  VPC running the long-running binaries of the local stack: chserver
  (reconciler included) behind an internal Application Load Balancer on
  port 8123 (target group health check `/ping`, optional HTTPS on 8443),
  mvworker as its own service or in-process (`RUN_STREAM_WORKER`), and the
  poller-mode replicator once per deployment (`RunReplicator=auto`: the
  home region) with `REGIONS` listing every region with an empty endpoint.
  `CH_USERS` is injected from Secrets Manager. As for the serverless stack
  the template declares no tables and the home region is deployed first.
  chserver builds its store without `AsyncDDL`, so on the first start the
  home region's chserver does not listen until the `__catalog` replicas it
  adds are `ACTIVE`, and a replica region's chserver does not listen until
  its replica exists (`HealthCheckGracePeriod` 600 s). The stream
  consumers need NAT: DynamoDB Streams is not served by the DynamoDB
  gateway endpoint. Stack Rules check `ReplicaRegions` (this region, the
  home region and another one) and that client access is set. No
  event-source mappings are involved (`MVWORKER_FUNCTION_ARN` unset).
* **Hybrids** (no template): the roles are independent processes sharing
  the storage contract, so they can be mixed. A long-running chserver with
  `MVWORKER_FUNCTION_ARN` (and `REPLICATOR_FUNCTION_ARN`) set manages the
  event-source mappings of Lambda stream consumers from its in-process
  reconciler (§2 "Stream consumers", §5); it does not wire the `__catalog`
  stream, since it reloads the catalog every `CATALOG_REFRESH`. Conversely
  `cmd/reconciler-lambda` with `MVWORKER_FUNCTION_ARN` empty manages
  tables only (schedule-driven), for the API Lambda with long-running
  stream consumers. README "Hybrid deployments" lists them.

---

## 7. Testing

* Unit tests per package (`go test ./...`), no Docker needed (exec uses an
  in-memory `storage/memstore`).
* DynamoDB integration tests (skipped unless their endpoint variables are
  set): `make test-integration` starts two throwaway DynamoDB Local
  containers and runs `internal/streams`, `internal/replicator` and
  `internal/mvworker` with `DDB_TEST_ENDPOINT` / `DDB_TEST_ENDPOINT_B`;
  `internal/storage/dynamo` runs its integration tests with
  `DYNAMODB_TEST_ENDPOINT`.
* `tests/e2e` (build tag `e2e`): clickhouse-go v2 HTTP client (native API
  and database/sql) and plain net/http against a running chserver
  (`TWINHOUSE_URL`) — DDL, inserts with parameters, MV rollups, FINAL, tuple-IN
  probes, latest-wins replays, JSONEachRow bulk insert over raw HTTP. One
  region only, so it also runs against `make up-single`. Against the
  serverless API (`make aws-local-api` then `make aws-local-e2e`, or a
  real deployment) the suite detects Lambda mode (a `session_id` probe
  answered with code 48, or `E2E_LAMBDA=1`), sends `Accept: */*` so API
  Gateway returns binary responses decoded, and skips the session test;
  every other assertion is unchanged.
* Serverless deployment: `cmd/chserver-lambda` unit tests drive the real
  engine and server (memstore) with synthetic API Gateway proxy events and
  compare each response byte for byte with the same server over
  `httptest` (text and base64 bodies, Native, `compress`/`decompress`,
  gzip/zstd, errors, sessions, the stale-catalog reload, the
  `Content-Length` shim); `cmd/reconciler-lambda` and `internal/esm` test
  event classification, deadline handling and the mapping state machine
  with a fake Lambda API; `go test ./deploy/aws-lambda/` checks `template.yaml`
  against the binaries (function code directories, every environment
  variable set is read by some binary, IAM scoped to `TablePrefix`,
  private endpoint, schedule, `samconfig.toml` environments, Makefile
  targets) and runs `sam validate --lint` when the SAM CLI is installed.
* Container deployment: `go test ./deploy/aws-ecs/` parses `template.yaml`
  with the standard library and checks it against the binaries (every
  environment variable set is read by some binary, chserver's port and
  the listener/target-group ports, the `/ping` health check, `CH_USERS`
  only through ECS `Secrets`, IAM scoped to `TablePrefix` per service, the
  conditions of the optional services, outputs, `samconfig.toml`
  placeholders, Makefile targets) and runs `sam validate --lint`;
  `go test -tags ecslocal ./deploy/aws-ecs/` (`make ecs-local`) renders the
  task definitions and runs them as local containers against DynamoDB
  Local, with `tests/e2e` against the emulated chserver.
  `go test -tags samlocal ./cmd/chserver-lambda/` exercises the API
  through `sam local start-api` (API Gateway emulation: base64 handling,
  compression, error headers). Nothing runs against real AWS.
* `tests/compat` (build tag `compat`): differential tests — the scenario
  corpus `tests/compat/testdata/*.sql` (DDL + inserts + queries) runs
  against real ClickHouse 25.8 (`CLICKHOUSE_REF_URL`) and against this
  server; results must match in several output formats (TSV with names and
  types, JSONCompact, Native through clickhouse-go), order-insensitive
  unless ORDER BY. Directives on each statement (`-- query`, `-- error`,
  `-- not-implemented`, `-- unsupported`, `-- known-deviation`, …,
  `tests/compat/corpus_test.go`) mark the expected outcome; a
  `known-deviation` must be documented here.
* `tests/multiregion` (build tag `multiregion`): write in one region, read
  in the other; concurrent writers in both regions; DDL and DROP
  propagation, table re-creation; worker/replicator kill and restart and
  redelivery do not double count; region outage and catch-up; replication
  lag convergence. Its fault tests restart containers, so it runs against
  its own compose project (`dhmr`, see `tests/multiregion/doc.go`), never
  the development stack.
* DynamoDB Local slows down markedly as stream and item history piles up
  (minutes instead of seconds for the multiregion suite after a few runs):
  recreate the stack (`make down && make up`) before investigating
  timeouts.
