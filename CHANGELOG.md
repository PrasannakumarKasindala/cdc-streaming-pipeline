# Changelog

## 0.1.0

Initial release.

- Exactly-once, out-of-order CDC merge core keyed on the source LSN
  (last-writer-wins with a per-key high-water-mark); idempotent under
  at-least-once delivery and safe against resurrected deletes.
- Apache Iceberg sink (local `pyiceberg` write path) applying each micro-batch as
  an LSN-guarded upsert/delete with one snapshot per batch.
- Source-to-lakehouse reconciler that quantifies drift in business-value dollars
  and gates via exit code.
- Production Spark Structured Streaming job with an LSN-guarded `MERGE INTO`
  (validated by a structure test), plus a docker-compose CDC stack
  (Postgres → Debezium → Redpanda) and an AWS Terraform example.
