"""Signal Validation System — evaluator package (Phase 4).

Pure offline analysis: reads enriched/raw JSONL, computes labels and metrics,
and writes reports. No Redis, no network, no imports outside Signal_Validation/.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

__version__ = "0.1.0"

IST = timezone(timedelta(hours=5, minutes=30))
DEFAULT_HORIZONS = (15, 30, 60, 120)
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "configs" / "evaluator.yaml"
DEFAULT_DATA_DIR = "D:/Signal_Validation_Data"

_SUBDIRS = ("raw", "processed", "reports", "logs")


def now_ist() -> datetime:
    return datetime.now(IST)


def parse_timestamp(value: Any) -> Optional[datetime]:
    """Coerce an ISO string or epoch number to an aware IST datetime (or None)."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=IST)
        except (ValueError, OSError, OverflowError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            if text.endswith("Z"):
                try:
                    parsed = datetime.fromisoformat(text[:-1] + "+00:00")
                except ValueError:
                    return None
            else:
                return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=IST)
        return parsed.astimezone(IST)
    return None


def to_float(value: Any) -> Optional[float]:
    """Coerce to float, rejecting bools/NaN/inf (returns None)."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        f = float(value)
        return f if f == f and f not in (float("inf"), float("-inf")) else None
    if isinstance(value, str):
        try:
            f = float(value.replace(",", "").strip())
        except ValueError:
            return None
        return f if f == f and f not in (float("inf"), float("-inf")) else None
    return None


def load_evaluator_config(path: Any = None, environ: Optional[dict] = None) -> dict:
    """Load ``configs/evaluator.yaml`` and apply ``SIGVAL_*`` env overrides."""
    import yaml  # lazy import

    env = dict(os.environ if environ is None else environ)
    cfg_path = Path(path) if path else DEFAULT_CONFIG_PATH
    config: dict = {}
    if cfg_path.exists():
        with open(cfg_path, "r", encoding="utf-8") as fh:
            loaded = yaml.safe_load(fh)
            if isinstance(loaded, dict):
                config = loaded

    config.setdefault("horizons", list(DEFAULT_HORIZONS))
    config["horizons"] = [int(h) for h in config["horizons"]]

    # --- paths (SIGVAL_DATA_DIR relocates the whole tree) ---
    paths = config.setdefault("paths", {})
    data_dir = env.get("SIGVAL_DATA_DIR") or paths.get("data_dir") or DEFAULT_DATA_DIR
    paths["data_dir"] = data_dir
    if env.get("SIGVAL_DATA_DIR"):
        for sub in _SUBDIRS:
            paths[f"{sub}_dir"] = str(Path(data_dir) / sub)
    else:
        for sub in _SUBDIRS:
            paths.setdefault(f"{sub}_dir", str(Path(data_dir) / sub))

    # --- signals ---
    signals = config.setdefault("signals", {})
    score = signals.setdefault("score", {})
    if env.get("SIGVAL_SCORE_BULL_THRESHOLD"):
        score["bull_threshold"] = float(env["SIGVAL_SCORE_BULL_THRESHOLD"])
    if env.get("SIGVAL_SCORE_BEAR_THRESHOLD"):
        score["bear_threshold"] = float(env["SIGVAL_SCORE_BEAR_THRESHOLD"])
    score.setdefault("bull_threshold", 55.0)
    score.setdefault("bear_threshold", 45.0)

    # --- labels / cost ---
    labels = config.setdefault("labels", {})
    cost = labels.setdefault("cost", {})
    if env.get("SIGVAL_COST_SPOT_POINTS"):
        cost["spot_points"] = float(env["SIGVAL_COST_SPOT_POINTS"])
    if env.get("SIGVAL_COST_OPTION_PCT"):
        cost["option_pct"] = float(env["SIGVAL_COST_OPTION_PCT"])
    cost.setdefault("spot_points", 2.0)
    cost.setdefault("option_pct", 1.0)
    labels.setdefault("mode", "economic")

    # --- significance / sampling / split ---
    significance = config.setdefault("significance", {})
    if env.get("SIGVAL_ALPHA"):
        significance["alpha"] = float(env["SIGVAL_ALPHA"])
    significance.setdefault("alpha", 0.05)
    significance.setdefault("multiple_testing", "bonferroni")
    significance.setdefault("bootstrap_iterations", 2000)
    significance.setdefault("bootstrap_ci", 0.95)

    sampling = config.setdefault("sampling", {})
    if env.get("SIGVAL_MIN_SAMPLE_SIZE"):
        sampling["min_sample_size"] = int(env["SIGVAL_MIN_SAMPLE_SIZE"])
    sampling.setdefault("min_sample_size", 100)
    sampling.setdefault("min_successes", 30)
    sampling.setdefault("min_failures", 30)

    split = config.setdefault("split", {})
    split.setdefault("method", "chronological")
    split.setdefault("train_ratio", 0.70)
    split.setdefault("validation_ratio", 0.15)
    split.setdefault("test_ratio", 0.15)
    split.setdefault("min_train_days", 10)
    split.setdefault("min_test_days", 3)
    split.setdefault("gap_seconds", 120)

    # --- metrics / report ---
    metrics = config.setdefault("metrics", {})
    metrics.setdefault("compute", ["hit_rate", "expectancy", "profit_factor", "drawdown", "sharpe_ratio"])
    metrics.setdefault("annualization_factor", 252)

    report = config.setdefault("report", {})
    report.setdefault("formats", ["json", "markdown"])
    report.setdefault("version_by_timestamp", True)
    report.setdefault("include_cost_sweep", True)
    report.setdefault("benchmarks", ["always_long", "always_short", "hold_spot"])

    return config


def resolve_paths(config: dict) -> dict:
    paths = config.get("paths", {})
    base = Path(paths.get("data_dir", DEFAULT_DATA_DIR))
    return {
        "raw": Path(paths.get("raw_dir", base / "raw")),
        "processed": Path(paths.get("processed_dir", base / "processed")),
        "reports": Path(paths.get("reports_dir", base / "reports")),
        "logs": Path(paths.get("logs_dir", base / "logs")),
    }

