from decimal import Decimal

from cdcpipe.events import CDCEvent, from_debezium
from cdcpipe.generate import build_log, deliver, orders_spec, source_truth
from cdcpipe.merge import MODE_ARRIVAL, MODE_LSN, MergeCore

SPEC = orders_spec()


def _ev(key, lsn, op="u", amount="10.00"):
    after = None if op == "d" else {"order_id": key, "customer_id": 1,
                                    "status": "PAID", "amount": amount,
                                    "updated_ms": lsn}
    return CDCEvent(table="orders", op=op, key=key, lsn=lsn, ts_ms=lsn, after=after)


def _by_key(rows):
    out = {}
    for r in rows:
        out[r["order_id"]] = str(Decimal(str(r["amount"])).quantize(Decimal("0.01")))
    return out


def test_out_of_order_last_writer_wins():
    # Higher-LSN value must win regardless of arrival order.
    for order in ([_ev(1, 10, amount="10.00"), _ev(1, 20, amount="99.00")],
                  [_ev(1, 20, amount="99.00"), _ev(1, 10, amount="10.00")]):
        core = MergeCore(MODE_LSN)
        core.apply_batch(order)
        assert _by_key(core.materialize()) == {1: "99.00"}


def test_duplicate_delivery_is_idempotent():
    core = MergeCore(MODE_LSN)
    e = _ev(1, 5, amount="42.00")
    core.apply(e)
    core.apply(e)                     # re-delivered
    core.apply(e)
    assert core.stats.applied == 1
    assert core.stats.ignored_stale == 2
    assert _by_key(core.materialize()) == {1: "42.00"}


def test_delete_is_not_resurrected_by_late_update():
    core = MergeCore(MODE_LSN)
    core.apply(_ev(1, 30, op="d"))          # delete at LSN 30
    core.apply(_ev(1, 20, amount="5.00"))   # older update arrives late
    assert core.materialize() == []         # stays deleted


def test_exactly_once_is_order_independent():
    """The core property: any delivery order (with duplicates) folds to the
    same state, and that state equals the source truth."""
    canonical = build_log(orders=60, updates=150, deletes=12, seed=3)
    truth = _by_key(source_truth(canonical, SPEC))

    results = []
    for seed in range(6):
        delivered = deliver(canonical, disorder=200, dup_rate=0.05, seed=seed)
        core = MergeCore(MODE_LSN)
        for env in delivered:
            core.apply(from_debezium(env, SPEC))
        results.append(_by_key(core.materialize()))

    for state in results:
        assert state == truth               # every ordering converges to truth


def test_arrival_mode_can_diverge():
    # Sanity: the naive baseline is genuinely wrong under reordering.
    canonical = build_log(orders=60, updates=150, deletes=12, seed=3)
    truth = _by_key(source_truth(canonical, SPEC))
    delivered = deliver(canonical, disorder=200, dup_rate=0.05, seed=1)
    core = MergeCore(MODE_ARRIVAL)
    for env in delivered:
        core.apply(from_debezium(env, SPEC))
    assert _by_key(core.materialize()) != truth
