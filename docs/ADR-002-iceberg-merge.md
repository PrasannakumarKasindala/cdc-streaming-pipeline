# ADR-002: Apache Iceberg as the sink, written with an LSN-guarded MERGE

**Status:** Accepted

## Context

The pipeline needs a table format for the materialized `orders` table that can:
be upserted and deleted row-by-row from a stream; expose an atomic, consistent
view to readers mid-stream; and let a reconciler compare "what the lakehouse
holds now" against the source. A plain directory of Parquet files gives none of
these — no row-level delete, no atomic commit, no snapshot to reconcile against.

## Decision

Use **Apache Iceberg** as the sink, and apply each micro-batch as a single
**`MERGE INTO` guarded by the source LSN**.

The target table carries an `_lsn` column alongside the business columns. Each
micro-batch is first deduplicated to the highest-LSN row per key, then merged:

```sql
MERGE INTO lakehouse.orders t USING batch s ON t.order_id = s.order_id
WHEN MATCHED AND s.op = 'd'  AND s._lsn > t._lsn THEN DELETE
WHEN MATCHED AND s.op <> 'd' AND s._lsn > t._lsn THEN UPDATE SET *
WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT *
```

The `s._lsn > t._lsn` guard is ADR-001's rule expressed in SQL: it makes the
MERGE safe against late and duplicate events *across* batches, which per-batch
dedup alone cannot guarantee. The local pipeline performs the equivalent
delete-then-upsert against the same Iceberg table via `pyiceberg`, so both paths
produce the same result and the local one is fully runnable in tests and CI.

## Why Iceberg, and why this write strategy

- **Row-level deletes and upserts.** Iceberg supports them as first-class
  operations with a real commit protocol; a bare Parquet lake does not.
- **Snapshots make reconciliation and recovery cheap.** Every micro-batch is an
  atomic snapshot. Readers never see a half-applied batch, the reconciler checks
  a consistent point-in-time, and time-travel allows replay/debugging of drift.
- **Copy-on-write vs merge-on-read.** We default to copy-on-write: writes rewrite
  affected data files, keeping reads fast — the right trade for a
  read-heavy analytical table with moderate change volume. A very high-churn
  table would switch to merge-on-read (position/equality deletes) to cut write
  amplification, at the cost of read-time merging and periodic compaction.
- **Engine- and catalog-neutral.** The same table is written by Spark and read by
  Trino/Flink/DuckDB, and the catalog is swappable: a local SQLite catalog for
  tests, AWS Glue in production (see `deploy/terraform`). No engine lock-in.

## Alternatives considered

- **Delta Lake.** Comparable capabilities and an excellent fit on Databricks. We
  chose Iceberg for its engine-neutral catalog story and hidden partitioning;
  the pipeline's logic would port to Delta with a different MERGE dialect.
- **Overwrite the whole table per batch.** Trivially correct, but rewrites the
  entire dataset every micro-batch — unusable at any real size.
- **Append-only + resolve-on-read.** Keep every change and pick the max-LSN row
  at query time. Simple writes, but pushes cost and correctness onto every reader
  and every downstream consumer. Materializing once is cheaper overall.

## Consequences

- The table schema carries an operational `_lsn` column that consumers ignore;
  it is the price of cross-batch ordering in the store itself.
- Copy-on-write write amplification grows with change rate; the maturity notes
  call out compaction / merge-on-read as the escalation path.
- Small files accumulate (one or more per snapshot); a periodic
  `rewrite_data_files` / expire-snapshots maintenance job is assumed in
  production and left out of this reference build.
