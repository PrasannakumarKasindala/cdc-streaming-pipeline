"""CDC events and the schema they describe.

A single Debezium topic carries change events for one table. Each event is an
envelope with ``op`` (c/u/d/r), a ``before`` and ``after`` image, and a
``source`` block whose **LSN** is the authoritative order the change happened in
the source database -- which is *not* the same as the order events arrive in
Kafka. Everything downstream keys off that LSN, so it is parsed out explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass

# op codes: c=create/insert, u=update, d=delete, r=snapshot read.
UPSERT_OPS = frozenset({"c", "u", "r"})
DELETE_OP = "d"


@dataclass(frozen=True)
class Column:
    name: str
    type: str                # int | str | decimal | ts  (logical type tag)
    scale: int = 2           # for decimal


@dataclass
class TableSpec:
    name: str
    key: str                 # single-column primary key (one topic per table)
    columns: list[Column]
    value_column: str | None = None   # numeric column summed for $-impact framing

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(name)


@dataclass
class CDCEvent:
    table: str
    op: str                  # normalized: "u" for any upsert, "d" for delete
    key: object              # value of the key column
    lsn: int                 # source log sequence number (authoritative order)
    ts_ms: int
    after: dict | None = None
    before: dict | None = None

    @property
    def is_delete(self) -> bool:
        return self.op == DELETE_OP


def from_debezium(envelope: dict, spec: TableSpec) -> CDCEvent:
    """Decode a Debezium envelope into a CDCEvent for the given table."""
    op = envelope.get("op")
    if op not in UPSERT_OPS and op != DELETE_OP:
        raise ValueError(f"unknown Debezium op {op!r}")
    source = envelope.get("source") or {}
    lsn = source.get("lsn")
    if lsn is None:
        raise ValueError("Debezium envelope missing source.lsn")
    after = envelope.get("after")
    before = envelope.get("before")
    image = before if op == DELETE_OP else after
    if image is None or spec.key not in image:
        raise ValueError(f"event has no key column {spec.key!r}")
    return CDCEvent(
        table=spec.name,
        op=DELETE_OP if op == DELETE_OP else "u",
        key=image[spec.key],
        lsn=int(lsn),
        ts_ms=int(envelope.get("ts_ms") or source.get("ts_ms") or 0),
        after=None if op == DELETE_OP else after,
        before=before,
    )
