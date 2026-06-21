from cdcpipe.generate import orders_spec
from cdcpipe.iceberg_sink import IcebergSink

SPEC = orders_spec()


def _row(oid, amount):
    return {"order_id": oid, "customer_id": 1, "status": "PAID",
            "amount": amount, "updated_ms": 1000}


def test_upsert_delete_and_snapshots(tmp_path):
    sink = IcebergSink(str(tmp_path), SPEC)
    sink.apply_changes([_row(1, "10.00"), _row(2, "20.00")], [])
    assert {r["order_id"] for r in sink.read_rows()} == {1, 2}

    # upsert changes 1, adds 3; delete removes 2
    sink.apply_changes([_row(1, "11.50"), _row(3, "30.00")], [2])
    rows = {r["order_id"]: r["amount"] for r in sink.read_rows()}
    assert set(rows) == {1, 3}
    assert str(rows[1]) == "11.50"
    assert sink.snapshot_count() >= 3        # append + (delete, upsert)


def test_reopen_persists(tmp_path):
    IcebergSink(str(tmp_path), SPEC).apply_changes([_row(9, "5.00")], [])
    reopened = IcebergSink(str(tmp_path), SPEC)
    assert [r["order_id"] for r in reopened.read_rows()] == [9]
