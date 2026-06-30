# Contributing

Thanks for taking a look. This is a portfolio project, but it's built like
something that has to run in production.

## Development setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest                      # 22 tests, ~10s (real local Iceberg writes)
ruff check src tests
```

## Ground rules

- **The merge core is the contract.** Any change to `merge.py` must keep the
  exactly-once property test green — folding any delivery order (with duplicates)
  of a log must converge to the source truth. If you change the rule, prove it.
- **Correctness logic stays engine-agnostic.** Keep the LSN rule in `merge.py`,
  not smeared into the Spark job or the sink. The Spark `MERGE` and the local
  pipeline should both express the *same* rule so tests cover production logic.
- **No fabricated numbers.** Benchmark figures come from `benchmark/run.py` on
  the machine that ran it. Kafka/Spark-cluster throughput is not claimed.
- **Style:** `ruff` is the authority (config in `pyproject.toml`). Conventional
  Commits for messages (`feat:`, `fix:`, `docs:`, `test:`, `perf:`, `build:`,
  `ci:`, `chore:`).

## Tests to add with a change

- New merge behavior → a case in `tests/test_merge.py`, ideally strengthening the
  property test rather than adding a one-off.
- New sink behavior → a roundtrip in `tests/test_iceberg_sink.py`.
- New CLI surface → an exit-code assertion in `tests/test_pipeline_reconcile.py`.
