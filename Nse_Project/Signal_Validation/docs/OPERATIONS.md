# Operations

## Running

* **Logger** (long-lived): `run_logger.bat` — subscribes and captures until stopped.
* **Evaluator** (batch):
  `python run_evaluator.py [--date YYYY-MM-DD | --start YYYY-MM-DD [--end YYYY-MM-DD]]`.
  With no date arguments it evaluates every date found in `processed/` / `raw/`.

## Scheduling

`register_scheduled_tasks.ps1` registers two daily tasks (times are local system
time; set the PC to IST):

* **Logger** daily 09:05, 7h execution limit, restart-on-failure ×3.
* **Evaluator** daily 15:50, 30 min limit.

Use `-Unregister` to remove both.

## Monitoring

* `D:/Signal_Validation_Data/logs/health.json` — state, uptime, and counters
  (`captured`, `duplicates`, `enriched`, `incomplete`, `reconnects`).
* `logs/run_logger.log` — rotating UTF-8 log (10 × 10 MB).
* Launcher logs: `run_logger_log.txt` / `run_evaluator_log.txt`.

## Retention

90 days (`storage.retention_days`). The writer sweeps at startup and on daily
rotation. Reports are never overwritten (timestamped filenames).

## Failure recovery

* Redis drop → exponential backoff 1s..30s; forward values across the gap are
  marked incomplete and recovered by the evaluator's raw self-join.
* Crash → idempotent restart; a trailing partial JSONL line is truncated; the
  lossless raw rows already on disk are retained.
* The validator is a separate process, so its failure never affects the feed.

## Data volumes

~20,000 rows/day, ~17 MB/day total (raw + enriched); ~1.5 GB for the full
90-day retention window.
