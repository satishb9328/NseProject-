# Signal Validation System

Read-only validation of the live NIFTY options platform's directional metrics
(**Score**, **Bias**, **Momentum**, **Gamma Boost**). It captures every published
snapshot, enriches it with forward 15/30/60/120s spot and pinned ATM CE/PE
returns, and emits a statistical report showing whether any signal has real
edge over the naive "always long" baseline.

## Layout

```
Signal_Validation/
├── logger/          # capture + enrichment (read-only Redis)
├── evaluator/       # offline labels + metrics + significance + reports
├── configs/         # logger.yaml + evaluator.yaml
├── docs/            # SCHEMA.md + this documentation set
├── tests/           # deterministic unit tests (stdlib unittest)
├── run_logger.py    # long-lived capture process
├── run_evaluator.py # batch evaluation process
├── run_logger.bat / run_evaluator.bat  # Windows launchers
└── register_scheduled_tasks.ps1         # Task Scheduler registration
```

## Quick start

```text
# 1. dependencies (uses .venv, which has redis + pyyaml + numpy + pandas)
.venv\Scripts\python.exe -m pip install -r requirements.txt

# 2. capture live data (long-running)
run_logger.bat

# 3. evaluate after market close
python run_evaluator.py --date 2026-11-19
```

Reports land in `D:/Signal_Validation_Data/reports/`.

## Safety contract

* Strictly read-only against Redis — the client wrapper raises on every write
  command (`set`, `publish`, `delete`, `hset`, `lpush`, `pipeline`, ...), proven
  by `tests/test_no_write.py`.
* No import from, and no write to, anything outside `Signal_Validation/`.
* The raw/enriched JSONL schemas are frozen in `docs/SCHEMA.md`.

## Tests

```text
python -m unittest discover -s tests -v
```

See `docs/INTEGRATION.md`, `docs/OPERATIONS.md`, `docs/METHODOLOGY.md`.
