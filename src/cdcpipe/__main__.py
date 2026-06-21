"""``cdcpipe`` CLI.

Exit codes: 0 = success / parity, 1 = drift detected (CI gate), 2 = bad config,
3 = IO / engine error.
"""

from __future__ import annotations

import argparse
import sys

from . import __version__
from .generate import generate, orders_spec
from .logging_setup import get_logger

log = get_logger()


def _cmd_generate(args) -> int:
    stream, _spec = generate(args.out_dir, orders=args.orders, updates=args.updates,
                             deletes=args.deletes, disorder=args.disorder,
                             dup_rate=args.dup_rate, seed=args.seed)
    print(f"wrote {stream}")
    return 0


def _cmd_run(args) -> int:
    from .pipeline import run
    from .report import render_stats
    stats = run(args.stream, args.warehouse, orders_spec(),
                mode=args.mode, batch_size=args.batch_size)
    print(render_stats(stats, args.mode))
    return 0


def _cmd_reconcile(args) -> int:
    from .iceberg_sink import IcebergSink
    from .reconcile import reconcile
    from .report import render_json, render_text
    sink = IcebergSink(args.warehouse, orders_spec())
    report = reconcile(args.stream, sink.read_rows(), orders_spec())
    print(render_json(report) if args.json else render_text(report))
    return 0 if report.ok else 1


def _cmd_inspect(args) -> int:
    from .iceberg_sink import IcebergSink
    sink = IcebergSink(args.warehouse, orders_spec())
    print(f"table    : {sink.identifier}")
    print(f"live rows: {len(sink.read_rows()):,}")
    print(f"snapshots: {sink.snapshot_count():,}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cdcpipe",
        description="CDC -> Kafka -> streaming merge -> Iceberg, with a "
                    "source-to-lakehouse correctness check.")
    p.add_argument("--version", action="version", version=f"cdcpipe {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="generate a synthetic Debezium CDC stream")
    g.add_argument("out_dir")
    g.add_argument("--orders", type=int, default=2000)
    g.add_argument("--updates", type=int, default=4000)
    g.add_argument("--deletes", type=int, default=300)
    g.add_argument("--disorder", type=int, default=8,
                   help="out-of-order window size (Kafka reordering)")
    g.add_argument("--dup-rate", type=float, default=0.02,
                   help="fraction of events re-delivered (at-least-once)")
    g.add_argument("--seed", type=int, default=11)
    g.set_defaults(func=_cmd_generate)

    r = sub.add_parser("run", help="run the local streaming merge into Iceberg")
    r.add_argument("--stream", required=True)
    r.add_argument("--warehouse", required=True)
    r.add_argument("--mode", default="lsn", choices=["lsn", "arrival"],
                   help="'lsn' = exactly-once/out-of-order-correct; "
                        "'arrival' = naive last-arrival-wins (for demos)")
    r.add_argument("--batch-size", type=int, default=500)
    r.set_defaults(func=_cmd_run)

    rc = sub.add_parser("reconcile", help="check the lakehouse against source truth")
    rc.add_argument("--stream", required=True)
    rc.add_argument("--warehouse", required=True)
    rc.add_argument("--json", action="store_true")
    rc.set_defaults(func=_cmd_reconcile)

    ins = sub.add_parser("inspect", help="show Iceberg table row count and snapshots")
    ins.add_argument("--warehouse", required=True)
    ins.set_defaults(func=_cmd_inspect)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (ValueError, KeyError) as e:
        log.error("config.invalid", extra={"detail": str(e)})
        print(f"error: {e}", file=sys.stderr)
        return 2
    except FileNotFoundError as e:
        log.error("input.not_found", extra={"detail": str(e)})
        print(f"error: {e}", file=sys.stderr)
        return 3
    except Exception as e:
        log.error("engine.error", extra={"detail": str(e)})
        print(f"error: {e}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
