# Benchmark results

Synthetic CDC stream: **84,515 delivered events** (20,000 inserts, 60,000 updates, 2,857 deletes; out-of-order + duplicates). Single process. Machine-dependent.

| Stage | Events | Time | Throughput |
|---|---:|---:|---:|
| Merge core (fold in memory) | 84,515 | 0.10s | 887,842 events/s |
| Full pipeline (decode + merge + Iceberg) | 84,515 | 47.35s | 1,785 events/s |
| Reconcile (source vs lakehouse) | 17,143 rows | 1.50s | 11,457 rows/s |

Result: 82,367 applied, 2,148 ignored (out-of-order/duplicate), 17,143 live rows across 81 Iceberg snapshots. Reconcile verdict: **PARITY**.

Kafka ingestion and Spark-cluster throughput are not benchmarked here; they depend on cluster sizing and are out of scope for a single-process harness.
