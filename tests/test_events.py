import pytest

from cdcpipe.events import from_debezium
from cdcpipe.generate import orders_spec

SPEC = orders_spec()


def _env(op, lsn, after=None, before=None):
    return {"op": op, "after": after, "before": before, "ts_ms": lsn,
            "source": {"table": "orders", "lsn": lsn, "ts_ms": lsn}}


def test_decode_insert():
    ev = from_debezium(_env("c", 5, after={"order_id": 7, "amount": "1.00"}), SPEC)
    assert ev.op == "u" and ev.key == 7 and ev.lsn == 5 and not ev.is_delete


def test_decode_delete_uses_before_image():
    ev = from_debezium(_env("d", 9, before={"order_id": 7}), SPEC)
    assert ev.is_delete and ev.key == 7 and ev.after is None


def test_missing_lsn_raises():
    with pytest.raises(ValueError):
        from_debezium({"op": "c", "after": {"order_id": 1}, "source": {}}, SPEC)


def test_unknown_op_raises():
    with pytest.raises(ValueError):
        from_debezium(_env("x", 1, after={"order_id": 1}), SPEC)
