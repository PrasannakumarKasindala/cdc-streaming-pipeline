import json

from cdcpipe.__main__ import main
from cdcpipe.generate import generate, orders_spec
from cdcpipe.iceberg_sink import IcebergSink
from cdcpipe.pipeline import run
from cdcpipe.reconcile import reconcile

SPEC = orders_spec()


def _gen(tmp_path):
    stream, _ = generate(str(tmp_path / "g"), orders=200, updates=400,
                         deletes=30, disorder=150, dup_rate=0.05)
    return stream


def test_lsn_pipeline_reconciles_parity(tmp_path):
    stream = _gen(tmp_path)
    wh = str(tmp_path / "wh_ok")
    run(stream, wh, SPEC, mode="lsn", batch_size=100)
    rep = reconcile(stream, IcebergSink(wh, SPEC).read_rows(), SPEC)
    assert rep.ok
    assert rep.value_drift == 0


def test_arrival_pipeline_drifts(tmp_path):
    stream = _gen(tmp_path)
    wh = str(tmp_path / "wh_bad")
    run(stream, wh, SPEC, mode="arrival", batch_size=100)
    rep = reconcile(stream, IcebergSink(wh, SPEC).read_rows(), SPEC)
    assert not rep.ok
    assert rep.value_drift > 0


def test_pipeline_ignores_duplicates_and_stale(tmp_path):
    stream = _gen(tmp_path)
    stats = run(stream, str(tmp_path / "wh2"), SPEC, mode="lsn", batch_size=100)
    assert stats.ignored_stale > 0           # duplicates + out-of-order dropped
    assert stats.delivered > stats.applied


def test_cli_flow_exit_codes(tmp_path, capsys):
    gdir = str(tmp_path / "cli")
    assert main(["generate", gdir, "--orders", "150", "--updates", "300",
                 "--deletes", "20"]) == 0
    capsys.readouterr()
    stream = f"{gdir}/stream.jsonl"
    wh = str(tmp_path / "cliwh")
    assert main(["run", "--stream", stream, "--warehouse", wh]) == 0
    capsys.readouterr()
    rc = main(["reconcile", "--stream", stream, "--warehouse", wh, "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0 and payload["verdict"] == "parity"


def test_cli_reconcile_drift_exit_1(tmp_path, capsys):
    gdir = str(tmp_path / "cli2")
    main(["generate", gdir, "--orders", "150", "--updates", "300", "--deletes", "20"])
    capsys.readouterr()
    stream = f"{gdir}/stream.jsonl"
    wh = str(tmp_path / "cliwh2")
    main(["run", "--stream", stream, "--warehouse", wh, "--mode", "arrival"])
    capsys.readouterr()
    assert main(["reconcile", "--stream", stream, "--warehouse", wh]) == 1
