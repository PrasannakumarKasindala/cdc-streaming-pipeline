"""The lakehouse sink: apply merged changes to an Apache Iceberg table.

Uses ``pyiceberg`` with a local SQL catalog and a filesystem warehouse, so the
write path is real -- real Iceberg metadata, real snapshots, real equality
deletes -- and runs anywhere without a cluster. In production the same changes
are applied by the Spark ``MERGE INTO`` job (see ``spark_job.py``); the local
sink mirrors its semantics: delete tombstoned keys, upsert live rows, one
Iceberg snapshot per micro-batch.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pyarrow as pa

from .events import TableSpec
from .logging_setup import get_logger

log = get_logger()

_DECIMAL_PRECISION = 18


def _arrow_field(col):
    if col.type == "int":
        return pa.field(col.name, pa.int64())
    if col.type == "str":
        return pa.field(col.name, pa.string())
    if col.type == "decimal":
        return pa.field(col.name, pa.decimal128(_DECIMAL_PRECISION, col.scale))
    if col.type == "ts":
        return pa.field(col.name, pa.timestamp("ms"))
    raise ValueError(f"unmapped column type {col.type!r}")


def _arrow_schema(spec: TableSpec) -> pa.Schema:
    return pa.schema([_arrow_field(c) for c in spec.columns])


def _coerce(value, col):
    if value is None:
        return None
    if col.type == "int":
        return int(value)
    if col.type == "decimal":
        return Decimal(str(value)).quantize(Decimal(1).scaleb(-col.scale))
    return value


def rows_to_arrow(rows: list[dict], spec: TableSpec) -> pa.Table:
    cols = {c.name: [_coerce(r.get(c.name), c) for r in rows] for c in spec.columns}
    return pa.table(cols, schema=_arrow_schema(spec))


class IcebergSink:
    def __init__(self, warehouse_dir: str, spec: TableSpec,
                 namespace: str = "lakehouse"):
        from pyiceberg.catalog.sql import SqlCatalog

        root = Path(warehouse_dir)
        (root / "wh").mkdir(parents=True, exist_ok=True)
        self.spec = spec
        self.namespace = namespace
        self.catalog = SqlCatalog(
            "cdc",
            uri=f"sqlite:///{root / 'catalog.db'}",
            warehouse=f"file://{root / 'wh'}",
        )
        self.identifier = f"{namespace}.{spec.name}"
        try:
            self.catalog.create_namespace(namespace)
        except Exception:
            pass
        try:
            self.table = self.catalog.load_table(self.identifier)
        except Exception:
            self.table = self.catalog.create_table(
                self.identifier, schema=_arrow_schema(spec))

    def apply_changes(self, upsert_rows: list[dict], delete_keys: list) -> None:
        from pyiceberg.expressions import In

        if delete_keys:
            self.table.delete(In(self.spec.key, list(delete_keys)))
        if upsert_rows:
            self.table.upsert(rows_to_arrow(upsert_rows, self.spec),
                              join_cols=[self.spec.key])

    def read_rows(self) -> list[dict]:
        return self.table.scan().to_arrow().to_pylist()

    def snapshot_count(self) -> int:
        return len(list(self.table.snapshots()))
