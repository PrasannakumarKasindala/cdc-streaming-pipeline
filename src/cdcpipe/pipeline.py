"""Local pipeline runner: stream -> decode -> merge -> Iceberg, in micro-batches.

This is the in-process stand-in for Spark Structured Streaming's ``foreachBatch``.
It reads the delivered (out-of-order, at-least-once) stream, decodes each
envelope, folds it through the merge core, and writes each batch's net changes to
Iceberg as one snapshot. The merge core is the same code the Spark job uses, so
what runs here is the logic that runs in production.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from .events import TableSpec, from_debezium
from .iceberg_sink import IcebergSink
from .logging_setup import get_logger
from .merge import MODE_LSN, MergeCore

log = get_logger()


@dataclass
class PipelineStats:
    delivered: int = 0
    applied: int = 0
    ignored_stale: int = 0
    batches: int = 0
    snapshots: int = 0
    live_rows: int = 0

    def as_dict(self) -> dict:
        return {"delivered": self.delivered, "applied": self.applied,
                "ignored_stale": self.ignored_stale, "batches": self.batches,
                "snapshots": self.snapshots, "live_rows": self.live_rows}


def _batches(path: str, spec: TableSpec, size: int):
    batch = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            batch.append(from_debezium(json.loads(line), spec))
            if len(batch) >= size:
                yield batch
                batch = []
    if batch:
        yield batch


def run(stream_path: str, warehouse_dir: str, spec: TableSpec,
        mode: str = MODE_LSN, batch_size: int = 500) -> PipelineStats:
    core = MergeCore(mode=mode)
    sink = IcebergSink(warehouse_dir, spec)
    stats = PipelineStats()

    for batch in _batches(stream_path, spec, batch_size):
        stats.delivered += len(batch)
        changed = core.apply_batch(batch)
        upserts, deletes = core.changes_for(changed)
        sink.apply_changes(upserts, deletes)
        stats.batches += 1

    stats.applied = core.stats.applied
    stats.ignored_stale = core.stats.ignored_stale
    stats.snapshots = sink.snapshot_count()
    stats.live_rows = len(sink.read_rows())
    log.info("pipeline.done", extra=stats.as_dict() | {"mode": mode})
    return stats
