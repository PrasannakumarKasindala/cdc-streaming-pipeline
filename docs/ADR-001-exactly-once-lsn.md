# ADR-001: Exactly-once via source-LSN high-water-mark, not arrival order

**Status:** Accepted

## Context

The pipeline consumes Debezium change events from Kafka. Two facts about that
transport shape everything:

1. **Delivery is at-least-once.** After a failure Kafka re-delivers; the same
   change event can be seen more than once.
2. **Arrival order is not source order.** Partitioning, retries, consumer lag,
   and rebalances mean events for a key can arrive in a different order than they
   were committed in Postgres.

A materialized table must reflect the *latest source state per key*. If we apply
events in the order they arrive, a late-arriving older update overwrites a newer
value, and a re-delivered delete can wipe a row that was legitimately
re-inserted. Both are silent correctness bugs that a row-count check would miss.

Every Debezium event carries `source.lsn` — the Postgres log sequence number,
which is monotonic in true commit order. That is the ordering signal we should
trust, not arrival.

## Decision

Resolve each key by **last-writer-wins on source LSN**, enforced by a per-key
high-water-mark:

> Apply an event only if its LSN is **strictly greater** than the highest LSN
> already recorded for that key. Otherwise drop it.

This single rule delivers three properties at once:

- **Out-of-order correctness.** A late, lower-LSN event loses to the newer state
  already applied.
- **Idempotency / exactly-once effect.** A re-delivered event has an LSN equal to
  (not greater than) what's stored, so it is a no-op. Reprocessing a batch — or
  the entire log — converges to the identical state.
- **Deletes that stay deleted.** A delete is a tombstone carrying its own LSN; a
  later-arriving but older update is rejected, so a deleted row is never
  resurrected.

The rule lives in a small, engine-agnostic Python core (`merge.py`) that is unit
tested directly, including a property test that folds many shuffled, duplicated
orderings of the same log and asserts they all converge to the source truth. The
Spark job encodes the same rule as an LSN-guarded `MERGE INTO` (see ADR-002).

## Alternatives considered

- **Apply in arrival order (naive).** Simplest, and wrong under reordering. We
  keep it as a selectable `--mode arrival` purely so the reconciler can
  *demonstrate* the drift it causes ($142K of value drift and resurrected deletes
  on the demo stream). Not a production option.
- **Kafka offsets / checkpoint only.** Structured Streaming checkpoints give
  exactly-once *source progress* (each offset processed once). That is necessary
  but not sufficient: it says nothing about ordering *between* keys across
  batches. We use checkpoints for progress **and** the LSN guard for correctness.
- **Event-time watermark + windowing.** Watermarks bound lateness by dropping
  data that arrives too late. For a CDC upsert we never want to drop a change; we
  want to order it. LSN ordering keeps every event and resolves it correctly.
- **Kafka log compaction as the store.** Compaction keeps the last value per key
  but by *offset*, not source LSN, so it inherits the arrival-order bug and can't
  express business-value reconciliation.

## Consequences

- The merge core holds per-key state (LSN + tombstone flag). For an unbounded
  key space this is bounded by distinct live keys; production state lives in
  Spark's state store / the Iceberg table's `_lsn` column, not in memory.
- The correctness guarantee is only as good as LSN monotonicity. Postgres
  `pgoutput` provides it; a source without a reliable sequence would need a
  different ordering key (e.g. a commit timestamp plus a tiebreaker).
- Because the rule is pure and tested in isolation, the expensive, hard-to-test
  systems (Kafka, Spark) don't have to be trusted for correctness — only for
  transport and scale.
