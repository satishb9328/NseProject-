"""Thread-safe CSV forward-test logger for VWAP Breakdown Sniper (PE · C2-A).

Every live trigger (backend 2m-candle evaluation or the browser UI) is appended to
``Signal_csv/live_sniper_c2a_forward_log.csv``, deduplicated per 2-minute candle so
each signal bar is logged at most once across both sources.
"""

from __future__ import annotations

import csv
import os
import threading
from typing import Any

SIGNAL_CSV_DIR = r"C:\Users\ADMIN\Desktop\MyProject2\Nse_Project\Signal_csv"
SIGNAL_CSV_PATH = os.path.join(SIGNAL_CSV_DIR, "live_sniper_c2a_forward_log.csv")

CSV_HEADERS = [
    "timestamp_ist",
    "candle_2m_bucket",
    "tier",
    "window_tag",
    "spot_price",
    "future_ltp",
    "vwap",
    "vwap_dist_pct",
    "ofi_imbalance",
    "rel_vol_ratio",
    "cvd_change_3",
    "atr_2m_14",
    "spot_sl_pts",
    "spot_target_pts",
    "atm_pe_strike",
    "atm_pe_ltp",
    "est_opt_sl_pts",
    "est_opt_target_pts",
    "source",
    "entry_price",
    "exit_time_ist",
    "exit_price",
    "outcome",
    "net_spot_pts",
    "net_opt_pts",
    "mfe_pts",
    "mae_pts",
]
# Backward-compatible alias for any older importers that referenced ``HEADER``.
HEADER = CSV_HEADERS

_lock = threading.Lock()
_logged_buckets: set = set()
_buckets_loaded = False


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None or isinstance(value, bool):
        return default
    try:
        f = float(value)
    except (TypeError, ValueError):
        return default
    return f if f == f else default


def _load_existing_buckets() -> set:
    """Read already-logged candle buckets from the CSV (file-backed dedup)."""
    buckets = set()
    if not os.path.exists(SIGNAL_CSV_PATH):
        return buckets
    try:
        with open(SIGNAL_CSV_PATH, "r", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                bucket = (row.get("candle_2m_bucket") or "").strip()
                if bucket:
                    buckets.add(bucket)
    except (OSError, csv.Error):
        pass
    return buckets


def migrate_csv_header_if_needed(csv_path: str) -> bool:
    """Upgrade an existing forward-log CSV to the current 27-column schema.

    Older files carried only the 19 live-trigger columns.  Re-writes the file
    atomically (temp file + :func:`os.replace`) so no rows are lost and any new
    post-market outcome columns are added as empty strings.  Returns True if the
    file was rewritten, False when it is already up to date (or unreadable).
    """
    if not csv_path or not os.path.exists(csv_path):
        return False

    try:
        with open(csv_path, "r", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            existing_fields = reader.fieldnames or []
            rows = [dict(row) for row in reader]
    except (OSError, csv.Error):
        return False

    if existing_fields == CSV_HEADERS:
        return False

    tmp_path = csv_path + ".tmp"
    try:
        with open(tmp_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_HEADERS, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        os.replace(tmp_path, csv_path)
        return True
    except (OSError, csv.Error):
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        return False


def log_sniper_c2a_trigger(payload: dict) -> bool:
    """Log a Sniper C2-A trigger. Returns True if written, False if skipped/duplicate."""
    if not isinstance(payload, dict):
        return False

    bucket = str(payload.get("candle_2m_bucket") or "").strip()
    if not bucket:
        return False

    with _lock:
        global _buckets_loaded
        if not _buckets_loaded:
            _logged_buckets.update(_load_existing_buckets())
            _buckets_loaded = True

        if bucket in _logged_buckets:
            return False

        spot_price = _to_float(payload.get("spot_price"))
        future_ltp = _to_float(payload.get("future_ltp"))
        vwap = _to_float(payload.get("vwap"))
        ref_price = future_ltp if future_ltp > 0 else spot_price

        vwap_dist_pct = payload.get("vwap_dist_pct")
        if vwap_dist_pct is None or vwap_dist_pct == "":
            vwap_dist_pct = round(((vwap - ref_price) / vwap) * 100.0, 6) if vwap > 0 else 0.0

        spot_sl = _to_float(payload.get("spot_sl_pts"))
        spot_target = _to_float(payload.get("spot_target_pts"), 45.0)

        est_opt_sl = payload.get("est_opt_sl_pts")
        if est_opt_sl is None or est_opt_sl == "":
            est_opt_sl = round(0.5 * spot_sl, 2)
        est_opt_target = payload.get("est_opt_target_pts")
        if est_opt_target is None or est_opt_target == "":
            est_opt_target = round(0.5 * spot_target, 2)

        row = {
            "timestamp_ist": str(payload.get("timestamp_ist") or ""),
            "candle_2m_bucket": bucket,
            "tier": str(payload.get("tier") or ""),
            "window_tag": str(payload.get("window_tag") or ""),
            "spot_price": spot_price,
            "future_ltp": future_ltp,
            "vwap": vwap,
            "vwap_dist_pct": vwap_dist_pct,
            "ofi_imbalance": _to_float(payload.get("ofi_imbalance")),
            "rel_vol_ratio": _to_float(payload.get("rel_vol_ratio")),
            "cvd_change_3": _to_float(payload.get("cvd_change_3")),
            "atr_2m_14": _to_float(payload.get("atr_2m_14")),
            "spot_sl_pts": spot_sl,
            "spot_target_pts": spot_target,
            "atm_pe_strike": _to_float(payload.get("atm_pe_strike")),
            "atm_pe_ltp": _to_float(payload.get("atm_pe_ltp")),
            "est_opt_sl_pts": est_opt_sl,
            "est_opt_target_pts": est_opt_target,
            "source": str(payload.get("source") or ""),
            # Post-market EOD outcome columns — blank at live-trigger time and
            # filled in later by evaluate_sniper_eod.py.
            "entry_price": "",
            "exit_time_ist": "",
            "exit_price": "",
            "outcome": "",
            "net_spot_pts": "",
            "net_opt_pts": "",
            "mfe_pts": "",
            "mae_pts": "",
        }

        os.makedirs(SIGNAL_CSV_DIR, exist_ok=True)
        migrate_csv_header_if_needed(SIGNAL_CSV_PATH)
        file_exists = os.path.exists(SIGNAL_CSV_PATH)
        with open(SIGNAL_CSV_PATH, "a", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_HEADERS, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

        _logged_buckets.add(bucket)
        return True
