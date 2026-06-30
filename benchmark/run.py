"""Benchmark the parts that run in-process: the merge core, the Iceberg write
path, and reconciliation. Kafka and Spark-cluster throughput are deliberately
NOT reported here -- they depend on a cluster and would be fabricated numbers.

Regenerate with:  python benchmark/run.py --orders 20000 --write
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cdcpipe.events import from_debezium                 # noqa: E402
from cdcpipe.generate import (build_log, deliver, orders_spec,  # noqa: E402
                              source_truth, write_stream)
from cdcpipe.iceberg_sink import IcebergSink             # noqa: E402
from cdcpipe.merge import MODE_LSN, MergeCore            # noqa: E402
from cdcpipe.pipeline import run                         # noqa: E402
from cdcpipe.reconcile import reconcile                  # noqa: E402


def bench(orders: int, write: bool) -> None:
    spec = orders_spec()
    updates, deletes = orders * 3, orders // 7
    work = Path("/tmp/cdc_bench")
    work.mkdir(exist_ok=True)

    canonical = build_log(orders, updates, deletes, seed=11)
    delivered = deliver(canonical, disorder=1000, dup_rate=0.02, seed=5)
    stream = str(work / "stream.jsonl")
    write_stream(stream, delivered)

    # 1) merge core throughput (pure Python, no I/O)
    events = [from_debezium(e, spec) for e in delivered]
    core = MergeCore(MODE_LSN)
    t0 = time.perf_counter()
    for e in events:
        core.apply(e)
    merge_dt = time.perf_counter() - t0

    # 2) full pipeline: decode + merge + Iceberg writes
    for p in (work / "wh", work / "catalog.db"):
        if p.is_dir():
            import shutil
            shutil.rmtree(p)
        elif p.exists():
            p.unlink()
    t0 = time.perf_counter()
    stats = run(stream, str(work), spec, mode=MODE_LSN, batch_size=2000)
    pipe_dt = time.perf_counter() - t0

    # 3) reconcile
    t0 = time.perf_counter()
    rep = reconcile(stream, IcebergSink(str(work), spec).read_rows(), spec)
    rec_dt = time.perf_counter() - t0

    n = len(events)
    lines = [
        "# Benchmark results",
        "",
        f"Synthetic CDC stream: **{n:,} delivered events** "
        f"({orders:,} inserts, {updates:,} updates, {deletes:,} deletes; "
        f"out-of-order + duplicates). Single process. Machine-dependent.",
        "",
        "| Stage | Events | Time | Throughput |",
        "|---|---:|---:|---:|",
        f"| Merge core (fold in memory) | {n:,} | {merge_dt:.2f}s | "
        f"{n / merge_dt:,.0f} events/s |",
        f"| Full pipeline (decode + merge + Iceberg) | {n:,} | {pipe_dt:.2f}s | "
        f"{n / pipe_dt:,.0f} events/s |",
        f"| Reconcile (source vs lakehouse) | {stats.live_rows:,} rows | "
        f"{rec_dt:.2f}s | {stats.live_rows / rec_dt:,.0f} rows/s |",
        "",
        f"Result: {stats.applied:,} applied, {stats.ignored_stale:,} ignored "
        f"(out-of-order/duplicate), {stats.live_rows:,} live rows across "
        f"{stats.snapshots:,} Iceberg snapshots. Reconcile verdict: "
        f"**{'PARITY' if rep.ok else 'DRIFT'}**.",
        "",
        "Kafka ingestion and Spark-cluster throughput are not benchmarked here; "
        "they depend on cluster sizing and are out of scope for a single-process "
        "harness.",
    ]
    report = "\n".join(lines)
    print(report)
    if write:
        Path(__file__).with_name("results.md").write_text(report + "\n",
                                                          encoding="utf-8")
        print("\n[wrote benchmark/results.md]")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", type=int, default=20000)
    ap.add_argument("--write", action="store_true")
    bench(**vars(ap.parse_args()))
