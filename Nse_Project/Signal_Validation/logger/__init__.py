"""Signal Validation System — logger core package (Phase 2).

Phase 2 modules are pure, deterministic, and offline: no Redis I/O, no network,
and no imports from outside ``Signal_Validation/``. They implement the contract
in ``docs/SCHEMA.md`` and ``configs/logger.yaml``. ``SIGVAL_*`` overrides are
honoured only through :func:`load_logger_config` (and ``SIGVAL_DATA_DIR`` in
:mod:`logger.writer`); nothing here performs I/O at import time.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

__version__ = "0.1.0"

# All timestamps are stored in Indian Standard Time (UTC+05:30).
IST = timezone(timedelta(hours=5, minutes=30))

# Forward-return horizons, seconds, ascending. Must match configs/logger.yaml.
DEFAULT_HORIZONS: tuple[int, ...] = (15, 30, 60, 120)

# Default config location: logger/ -> .. -> configs/logger.yaml.
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "configs" / "logger.yaml"

# Default storage base, overridable with SIGVAL_DATA_DIR.
DEFAULT_DATA_DIR = "D:/Signal_Validation_Data"


def now_ist() -> datetime:
    """Return the current time in IST."""
    return datetime.now(IST)


def isoformat_ist(dt: datetime) -> str:
    """Return an ISO-8601 string with the +05:30 offset, in IST."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return dt.astimezone(IST).isoformat()


def parse_timestamp(value: Any) -> Optional[datetime]:
    """Coerce a feed timestamp (ISO string or epoch number) to an aware datetime.

    Returns ``None`` for missing / unparseable / out-of-range values.
    """
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


def _parse_horizons(value: str) -> list[int]:
    parts = [p.strip() for p in value.split(",") if p.strip()]
    return [int(p) for p in parts]


def load_logger_config(path: Any = None, environ: Optional[dict] = None) -> dict:
    """Load ``configs/logger.yaml`` and apply ``SIGVAL_*`` env overrides.

    Returns a plain dict. Never writes; safe to call repeatedly.
    """
    import yaml  # lazy import so the package imports without pyyaml

    env = dict(os.environ if environ is None else environ)
    cfg_path = Path(path) if path else DEFAULT_CONFIG_PATH
    config: dict = {}
    if cfg_path.exists():
        with open(cfg_path, "r", encoding="utf-8") as fh:
            loaded = yaml.safe_load(fh)
            if isinstance(loaded, dict):
                config = loaded

    for section in ("storage", "redis", "writer", "enrichment", "dedup", "quality", "logging"):
        config.setdefault(section, {})

    # --- horizons ---
    if env.get("SIGVAL_HORIZONS"):
        try:
            config["horizons"] = _parse_horizons(env["SIGVAL_HORIZONS"])
        except ValueError:
            pass
    config.setdefault("horizons", list(DEFAULT_HORIZONS))
    config["horizons"] = [int(h) for h in config["horizons"]]

    # --- storage ---
    storage = config["storage"]
    storage["data_dir"] = env.get("SIGVAL_DATA_DIR") or storage.get("data_dir") or DEFAULT_DATA_DIR
    storage.setdefault("raw_subdir", "raw")
    storage.setdefault("processed_subdir", "processed")
    storage.setdefault("reports_subdir", "reports")
    storage.setdefault("logs_subdir", "logs")
    if env.get("SIGVAL_RETENTION_DAYS"):
        try:
            storage["retention_days"] = int(env["SIGVAL_RETENTION_DAYS"])
        except ValueError:
            pass
    storage.setdefault("retention_days", 90)

    # --- writer ---
    writer = config["writer"]
    if env.get("SIGVAL_FLUSH_INTERVAL_SECONDS"):
        try:
            writer["flush_interval_seconds"] = float(env["SIGVAL_FLUSH_INTERVAL_SECONDS"])
        except ValueError:
            pass
    if env.get("SIGVAL_FLUSH_MAX_ROWS"):
        try:
            writer["flush_max_rows"] = int(env["SIGVAL_FLUSH_MAX_ROWS"])
        except ValueError:
            pass
    writer.setdefault("flush_interval_seconds", 5.0)
    writer.setdefault("flush_max_rows", 100)
    writer.setdefault("raw_filename_pattern", "signal_{date}.jsonl")
    writer.setdefault("processed_filename_pattern", "signal_{date}.enriched.jsonl")
    writer.setdefault("encoding", "utf-8")
    writer.setdefault("truncate_trailing_partial_line_on_start", True)

    # --- enrichment ---
    enrichment = config["enrichment"]
    enrichment.setdefault("warmup_seconds", 120.0)
    enrichment.setdefault("ring_buffer_max_seconds", float(max(config["horizons"])))
    enrichment.setdefault("max_gap_seconds", 10.0)
    enrichment.setdefault("pin_contract", True)

    # --- redis ---
    redis = config["redis"]
    redis.setdefault("url", "redis://127.0.0.1:6379/0")
    redis.setdefault("channel", "gamma:live_options_updated")
    redis.setdefault("data_key", "live_options_data")
    redis.setdefault("market_state_key_prefix", "market_state")
    redis.setdefault("read_only", True)
    for key, env_name in (
        ("url", "SIGVAL_REDIS_URL"),
        ("channel", "SIGVAL_REDIS_CHANNEL"),
        ("data_key", "SIGVAL_REDIS_DATA_KEY"),
        ("market_state_key_prefix", "SIGVAL_MARKET_STATE_KEY_PREFIX"),
    ):
        if env.get(env_name):
            redis[key] = env[env_name]

    # --- quality ---
    quality = config["quality"]
    quality.setdefault("min_spot", 0.0)
    quality.setdefault("drop_if_spot_missing", True)
    quality.setdefault("allow_stale", False)
    quality.setdefault("require_feed_connected", False)
    quality.setdefault("atm_selection", "nearest_strike")

    # --- logging ---
    config["logging"].setdefault("level", env.get("SIGVAL_LOG_LEVEL") or "INFO")
    return config


def resolve_storage_paths(config: dict) -> dict[str, Path]:
    """Return ``{raw, processed, reports, logs}`` Paths from a config dict."""
    storage = config.get("storage", {})
    base = Path(storage.get("data_dir", DEFAULT_DATA_DIR))
    return {
        "raw": base / storage.get("raw_subdir", "raw"),
        "processed": base / storage.get("processed_subdir", "processed"),
        "reports": base / storage.get("reports_subdir", "reports"),
        "logs": base / storage.get("logs_subdir", "logs"),
    }

