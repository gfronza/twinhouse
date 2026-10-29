# Limitations and roadmap

Known gaps, roughly in priority order:

- **Real-AWS validation.** Run the multiregion scenarios against real
  global tables in two regions, and publish latency and cost numbers.
- **Cost.** Every source row is a DynamoDB write, and every view adds
  marker writes in every region (see the
  [cost model](operations.md#cost-model)).
  Rows of one rollup key are serialized and keys run in parallel. Real
  numbers from AWS are still to be published.
- **Oversized keys.** There is no hashed-key fallback for partition or sort
  key values over DynamoDB's limits, and no `CREATE`-time warning for
  unbounded `String` key columns; such rows are rejected.
- **UUID key ranges.** DynamoDB orders the stored UUID text differently
  from ClickHouse, so a range predicate (`<`, `BETWEEN`, keyset
  pagination) on a `UUID` sorting-key column is evaluated by the engine
  after a query on the equality prefix, not pushed down as a key range.
  Results are correct; an order-preserving UUID key encoding would need a
  new key-format version.
- **More of ClickHouse.** Decimal, window functions, higher-order
  functions, quantiles and other aggregates, `ALTER … UPDATE`, more
  formats (RowBinary, Parquet), and cascading materialized views.
- **Native TCP protocol**, so clients that do not offer HTTP can connect.
- **Multi-region DDL.** Conditional DDL is checked only against the local
  region's catalog. Concurrent changes to one object from two regions can
  silently overwrite each other.
- **Rollup mutations.** `ALTER … DELETE` on regional rollups is not
  available in multi-region mode, and `TRUNCATE` boundaries are approximate
  by clock skew.
- **Older catalog entries.** A table created by a build that predates
  per-incarnation physical names keeps its rows in regions that did not run
  a `DROP` when it is re-created under the same name. Re-create such
  tables once.

Issues and pull requests are welcome. Please keep examples and schemas
generic, and add a compat scenario for any new SQL behaviour.
