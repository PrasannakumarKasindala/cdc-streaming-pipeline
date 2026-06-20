"""Render pipeline stats and the reconciliation verdict (text and JSON)."""

from __future__ import annotations

import json

from .pipeline import PipelineStats
from .reconcile import ReconReport


def render_stats(stats: PipelineStats, mode: str) -> str:
    return (
        f"pipeline ({mode} merge): {stats.delivered:,} events -> "
        f"{stats.applied:,} applied, {stats.ignored_stale:,} ignored "
        f"(out-of-order/duplicate) across {stats.batches:,} batches\n"
        f"iceberg: {stats.live_rows:,} live rows, {stats.snapshots:,} snapshots"
    )


def render_text(r: ReconReport) -> str:
    out = ["=" * 68]
    out.append(f"source -> lakehouse reconciliation :: "
               f"{'PARITY' if r.ok else 'DRIFT DETECTED'}")
    out.append("=" * 68)
    out.append(f"source rows : {r.rows_source:,}")
    out.append(f"lake rows   : {r.rows_lake:,}")
    if not r.ok:
        if r.missing_in_lake:
            out.append(f"  missing in lakehouse : {len(r.missing_in_lake):,} "
                       f"(e.g. {r.missing_in_lake[:5]})")
        if r.extra_in_lake:
            out.append(f"  extra in lakehouse   : {len(r.extra_in_lake):,} "
                       f"(e.g. {r.extra_in_lake[:5]})")
        if r.value_mismatch:
            out.append(f"  stale/incorrect rows : {len(r.value_mismatch):,} "
                       f"(e.g. {r.value_mismatch[:5]})")
    if r.value_drift >= 0.005:
        out.append(f"business value drift : ${r.value_drift:,.2f}")
    out.append("-" * 68)
    out.append(f"reconciled in {r.elapsed_s:.2f}s   verdict: "
               f"{'lakehouse matches source' if r.ok else 'LAKEHOUSE OUT OF SYNC'}")
    return "\n".join(out)


def render_json(r: ReconReport) -> str:
    return json.dumps({
        "verdict": "parity" if r.ok else "drift",
        "rows_source": r.rows_source,
        "rows_lake": r.rows_lake,
        "missing_in_lake": r.missing_in_lake[:50],
        "extra_in_lake": r.extra_in_lake[:50],
        "value_mismatch": r.value_mismatch[:50],
        "value_drift": round(r.value_drift, 2),
        "elapsed_s": round(r.elapsed_s, 4),
    }, indent=2)
