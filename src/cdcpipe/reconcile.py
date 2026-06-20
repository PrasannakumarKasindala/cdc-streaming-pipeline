"""Continuous correctness check: does the lakehouse equal the source of record?

The source truth is the canonical log folded by LSN -- what the source database
holds. The lakehouse is whatever the pipeline materialized into Iceberg. A
correct pipeline makes them equal; a buggy one (e.g. arrival-order merge) drifts.
The reconciler compares them by key, names the drift, and quantifies it in
dollars using the value column.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal

from .events import TableSpec, from_debezium
from .logging_setup import get_logger
from .merge import MODE_LSN, MergeCore

log = get_logger()


def _canonical_row(row: dict, spec: TableSpec) -> tuple:
    out = []
    for c in spec.columns:
        v = row.get(c.name)
        if v is None:
            out.append(None)
        elif c.type == "decimal":
            out.append(str(Decimal(str(v)).quantize(Decimal(1).scaleb(-c.scale))))
        elif c.type == "int":
            out.append(int(v))
        else:
            out.append(str(v))
    return tuple(out)


@dataclass
class ReconReport:
    rows_source: int = 0
    rows_lake: int = 0
    missing_in_lake: list = field(default_factory=list)   # keys
    extra_in_lake: list = field(default_factory=list)     # keys
    value_mismatch: list = field(default_factory=list)    # keys
    value_drift: float = 0.0
    elapsed_s: float = 0.0

    @property
    def ok(self) -> bool:
        return not (self.missing_in_lake or self.extra_in_lake or self.value_mismatch)


def _source_truth(stream_path: str, spec: TableSpec) -> dict:
    core = MergeCore(mode=MODE_LSN)
    events = []
    with open(stream_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                events.append(from_debezium(json.loads(line), spec))
    for ev in sorted(events, key=lambda e: e.lsn):
        core.apply(ev)
    return {row[spec.key]: row for row in core.materialize()}


def reconcile(stream_path: str, lake_rows: list[dict], spec: TableSpec) -> ReconReport:
    import time
    t0 = time.perf_counter()
    truth = _source_truth(stream_path, spec)
    lake = {r[spec.key]: r for r in lake_rows}

    report = ReconReport(rows_source=len(truth), rows_lake=len(lake))
    vcol = spec.value_column

    def amount(row):
        if not vcol or row is None or row.get(vcol) is None:
            return Decimal(0)
        return Decimal(str(row[vcol]))

    for k, srow in truth.items():
        lrow = lake.get(k)
        if lrow is None:
            report.missing_in_lake.append(k)
            report.value_drift += float(abs(amount(srow)))
        elif _canonical_row(srow, spec) != _canonical_row(lrow, spec):
            report.value_mismatch.append(k)
            report.value_drift += float(abs(amount(srow) - amount(lrow)))
    for k, lrow in lake.items():
        if k not in truth:
            report.extra_in_lake.append(k)
            report.value_drift += float(abs(amount(lrow)))

    report.elapsed_s = time.perf_counter() - t0
    log.info("reconcile.done", extra={
        "rows_source": report.rows_source, "rows_lake": report.rows_lake,
        "missing": len(report.missing_in_lake), "extra": len(report.extra_in_lake),
        "value_mismatch": len(report.value_mismatch),
        "value_drift": round(report.value_drift, 2), "ok": report.ok,
    })
    return report
