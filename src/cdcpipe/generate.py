"""Generate a synthetic Debezium CDC stream for a retail `orders` table.

Produces a *canonical* event log (inserts, then updates, then deletes, each with
a strictly increasing source LSN) and a *delivered* stream that simulates Kafka:
events shuffled within a sliding window (out-of-order) and some re-delivered
(at-least-once). Folding the canonical log by LSN gives the source truth the
reconciler checks the lakehouse against.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from .events import Column, TableSpec
from .logging_setup import get_logger
from .merge import MODE_LSN, MergeCore

log = get_logger()

STATUSES = ["NEW", "PAID", "SHIPPED", "CANCELLED", "REFUNDED"]


def orders_spec() -> TableSpec:
    return TableSpec(
        name="orders",
        key="order_id",
        columns=[
            Column("order_id", "int"),
            Column("customer_id", "int"),
            Column("status", "str"),
            Column("amount", "decimal", scale=2),
            Column("updated_ms", "int"),
        ],
        value_column="amount",
    )


def _lcg(seed: int):
    x = seed & 0xFFFFFFFF
    while True:
        x = (1103515245 * x + 12345) & 0x7FFFFFFF
        yield x


def _envelope(op, table, lsn, ts_ms, before, after):
    return {"op": op, "before": before, "after": after,
            "ts_ms": ts_ms, "source": {"table": table, "lsn": lsn, "ts_ms": ts_ms}}


def build_log(orders: int = 2000, updates: int = 4000, deletes: int = 300,
              seed: int = 11) -> list[dict]:
    """The canonical, correctly-ordered event log (by LSN)."""
    rng = _lcg(seed)
    lsn = 0
    ts = 1_700_000_000_000
    live: dict[int, dict] = {}
    events: list[dict] = []

    def row(oid):
        return {"order_id": oid, "customer_id": 1 + next(rng) % (orders // 2 + 1),
                "status": STATUSES[next(rng) % len(STATUSES)],
                "amount": str(Decimal(5 + next(rng) % 500000) / Decimal(100)),
                "updated_ms": ts}

    for oid in range(1, orders + 1):
        lsn += 1

        ts += 1000
        r = row(oid)
        r["updated_ms"] = ts
        live[oid] = r
        events.append(_envelope("c", "orders", lsn, ts, None, dict(r)))

    for _ in range(updates):
        if not live:
            break
        oid = list(live)[next(rng) % len(live)]
        lsn += 1

        ts += 1000
        before = dict(live[oid])
        upd = dict(before)
        upd["status"] = STATUSES[next(rng) % len(STATUSES)]
        upd["amount"] = str(Decimal(5 + next(rng) % 500000) / Decimal(100))
        upd["updated_ms"] = ts
        live[oid] = upd
        events.append(_envelope("u", "orders", lsn, ts, before, dict(upd)))

    for _ in range(deletes):
        if not live:
            break
        oid = list(live)[next(rng) % len(live)]
        lsn += 1

        ts += 1000
        before = dict(live.pop(oid))
        events.append(_envelope("d", "orders", lsn, ts, before, None))

    return events


def deliver(events: list[dict], disorder: int = 600, dup_rate: float = 0.02,
            seed: int = 5) -> list[dict]:
    """Simulate Kafka delivery: bounded forward delay (broker/network lag) plus
    re-delivery. Each event is pushed forward by a random amount in
    [0, disorder]; when disorder exceeds the consumer's batch size, events arrive
    late *across* micro-batches -- the case that breaks arrival-order merges."""
    rng = _lcg(seed)
    tagged = []
    for i, ev in enumerate(events):
        delay = next(rng) % (disorder + 1)
        tagged.append((i + delay, i, ev))       # (i, ...) keeps it a stable sort
    tagged.sort(key=lambda t: (t[0], t[1]))
    out = [t[2] for t in tagged]
    # duplicates -> at-least-once
    delivered = []
    dup_every = max(1, int(1 / dup_rate)) if dup_rate > 0 else 0
    for n, ev in enumerate(out):
        delivered.append(ev)
        if dup_every and n % dup_every == 0:
            delivered.append(dict(ev))
    return delivered


def source_truth(canonical_events: list[dict], spec: TableSpec) -> list[dict]:
    """Fold the canonical log by LSN -> the true source-of-record state."""
    from .events import from_debezium
    core = MergeCore(mode=MODE_LSN)
    for env in sorted(canonical_events, key=lambda e: e["source"]["lsn"]):
        core.apply(from_debezium(env, spec))
    return core.materialize()


def write_stream(path: str, delivered: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for ev in delivered:
            fh.write(json.dumps(ev, separators=(",", ":")) + "\n")


def generate(out_dir: str, orders: int = 2000, updates: int = 4000,
             deletes: int = 300, disorder: int = 8, dup_rate: float = 0.02,
             seed: int = 11) -> tuple[str, TableSpec]:
    root = Path(out_dir)
    root.mkdir(parents=True, exist_ok=True)
    canonical = build_log(orders, updates, deletes, seed)
    delivered = deliver(canonical, disorder, dup_rate, seed)
    stream_path = str(root / "stream.jsonl")
    write_stream(stream_path, delivered)
    log.info("generate.done", extra={
        "canonical_events": len(canonical), "delivered_events": len(delivered),
        "orders": orders, "updates": updates, "deletes": deletes,
        "disorder": disorder, "dup_rate": dup_rate,
    })
    return stream_path, orders_spec()
