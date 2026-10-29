# Consistency model

What a client observes when it writes and reads in one region and across
regions.

**In the region you write to**, behaviour is that of a single ClickHouse
node. After an `INSERT` returns, a `SELECT` in the same region sees the
rows, including their effect on materialized-view rollups. Reads are
strongly consistent by default (`CONSISTENT_READS=true`).

**Across regions**, every region converges to the same data. Source rows
appear in the other regions after replication (typically about a second
on AWS). Rollups there catch up once that region's `mvworker` has
processed the replicated rows.

- **Rollups are exactly-once.** Every source row is counted exactly once in
  every region's rollups, through stream redelivery, worker crashes,
  restarts, inline-plus-stream double application and replicator restarts.
  The rollups never conflict across regions because each region writes
  only its own copy.
- **`ReplacingMergeTree` is latest-wins.** Reads always behave like
  `FINAL`. With a version column, the highest version wins in every region
  regardless of write order. Write time (`_ts`, then the origin region)
  only breaks ties. Without a version column, the latest write wins. A
  write that is already older than the stored version is not stored, as in
  ClickHouse after a merge.
- **Conflicting writes** to the same `ReplacingMergeTree` key in two
  regions at once resolve to the same winner everywhere. Both versions
  still feed the materialized views of every region: the replicator
  applies the target region's views to a replicated version that region
  rejected, so rollups over a Replacing source converge too. One case
  is special in multi-region mode: a write that is *already* superseded
  when it arrives is never stored and never replicates, so it feeds no
  view anywhere. A single-region deployment feeds it to views, as
  ClickHouse does.
- **`MergeTree` rows never conflict.** Every row has a unique key.
- **DDL takes a moment to reach other regions.** That is catalog
  replication plus one reconcile round, a few seconds locally. A statement
  in region B right after `CREATE TABLE` in region A can fail with
  `UNKNOWN_TABLE`, so retry it. Concurrent DDL on the *same* object in two
  regions resolves last-writer-wins, and one of the two changes is lost.
  Run schema changes for an object from one region at a time.
- **Mutations of rollups.** `TRUNCATE` of a rollup is recorded in the
  replicated catalog and every region resets its own copy. Rows written
  within a moment of the `TRUNCATE` in another region may be missing from
  that region's copy. `ALTER TABLE … DELETE` / `DELETE FROM` on a rollup is
  rejected in multi-region mode, because nothing would carry it to the
  other regions. On source tables, deletes replicate.
- **Clock skew.** Some boundaries are decided by write time: whether a row
  predates a view's creation, and whether it predates a `TRUNCATE`. These
  boundaries are as approximate as the skew between the writers' clocks.
- **`INSERT` atomicity.** An `INSERT` is atomic per block (about 1M rows
  or 256 MiB, `min_insert_block_size_rows` / `_bytes`) for parse,
  conversion and `DEFAULT` errors: a bad row fails its block before any of
  it is written. A DynamoDB failure in the middle of a block can leave part
  of the block written. With `STREAM_VIEWS=true` (the default) a failed
  inline view application does not fail the `INSERT`: the source rows are
  stored and the rollups catch up through the stream worker.
