"""Local dev consumer: Kafka (Debezium) -> merge core -> Iceberg.

A lightweight stand-in for the Spark job for local end-to-end runs via
docker-compose. It reuses the exact merge core and Iceberg sink the rest of the
project is built on, so the only new code here is the Kafka plumbing. Not used in
CI (no broker); requires ``pip install '.[kafka]'`` and a running broker.

Usage:
    python deploy/kafka_consumer.py --bootstrap localhost:19092 \\
        --topic dbserver.public.orders --warehouse /data/lakehouse
"""

from __future__ import annotations

import argparse
import json

from cdcpipe.events import from_debezium
from cdcpipe.generate import orders_spec
from cdcpipe.iceberg_sink import IcebergSink
from cdcpipe.logging_setup import get_logger
from cdcpipe.merge import MODE_LSN, MergeCore

log = get_logger()


def main(argv=None):  # pragma: no cover - requires a running Kafka broker
    from kafka import KafkaConsumer

    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--poll-ms", type=int, default=1000)
    args = ap.parse_args(argv)

    spec = orders_spec()
    core = MergeCore(MODE_LSN)
    sink = IcebergSink(args.warehouse, spec)
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        enable_auto_commit=False,               # commit only after a successful write
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b) if b else None,
        group_id="cdcpipe-consumer",
    )
    log.info("consumer.start", extra={"topic": args.topic})

    batch = []
    for msg in consumer:
        if msg.value is None:                    # Debezium tombstone marker
            continue
        payload = msg.value.get("payload", msg.value)   # unwrap if schema present
        batch.append(from_debezium(payload, spec))
        if len(batch) >= args.batch_size:
            changed = core.apply_batch(batch)
            sink.apply_changes(*core.changes_for(changed))
            consumer.commit()                    # exactly-once-ish: offsets after write
            log.info("consumer.batch", extra={"applied": core.stats.applied,
                                              "ignored": core.stats.ignored_stale})
            batch = []


if __name__ == "__main__":  # pragma: no cover
    main()
