"""Self-contained EOD evaluator test (no network, synthetic candles).

Writes two synthetic VWAP Sniper (PE · C2-A) forward-log rows — one that hits the
-45 pt TP and one that hits the +33 pt SL — passes synthetic 1-minute candles into
``evaluate_sniper_outcomes``, asserts the exact outcomes, then restores
``live_sniper_c2a_forward_log.csv`` to a clean 27-column header-only file.

Run:
    .venv\\Scripts\\python.exe Nse_Project\\TESTING\\test_evaluate_sniper_eod.py
"""

import csv
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd

from Nse_Project.Live_dashboard import sniper_logger
from Nse_Project.Live_dashboard.evaluate_sniper_eod import evaluate_sniper_outcomes

TEST_DATE = "2026-11-13"
CSV_PATH = sniper_logger.SIGNAL_CSV_PATH


def _write_header_only() -> None:
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerow(sniper_logger.CSV_HEADERS)


def _write_rows(rows) -> None:
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=sniper_logger.CSV_HEADERS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _make_row(bucket: str) -> dict:
    return {
        "timestamp_ist": bucket + ":00",
        "candle_2m_bucket": bucket,
        "tier": "TEST",
        "window_tag": "TEST",
        "spot_price": "25005.0",
        "future_ltp": "25010.0",
        "vwap": "25050.0",
        "vwap_dist_pct": "0.18",
        "ofi_imbalance": "0.5",
        "rel_vol_ratio": "1.6",
        "cvd_change_3": "5.0",
        "atr_2m_14": "15.0",
        "spot_sl_pts": "33.0",
        "spot_target_pts": "45.0",
        "atm_pe_strike": "25000",
        "atm_pe_ltp": "0.0",
        "est_opt_sl_pts": "16.5",
        "est_opt_target_pts": "22.5",
        "source": "TEST",
    }


def _read_rows() -> dict:
    with open(CSV_PATH, "r", newline="", encoding="utf-8") as fh:
        return {r["candle_2m_bucket"]: r for r in csv.DictReader(fh)}


def main() -> None:
    try:
        _write_rows([
            _make_row(f"{TEST_DATE} 10:30"),  # TP: low 24950 <= 24955 before SL
            _make_row(f"{TEST_DATE} 11:00"),  # SL: high 25040 >= 25033 before TP
        ])

        candles = pd.DataFrame([
            {"timestamp_ist": f"{TEST_DATE} 10:32", "open": 25000.0, "high": 25020.0, "low": 24970.0, "close": 25005.0},
            {"timestamp_ist": f"{TEST_DATE} 10:33", "open": 25005.0, "high": 25015.0, "low": 24950.0, "close": 24960.0},
            {"timestamp_ist": f"{TEST_DATE} 11:02", "open": 25000.0, "high": 25020.0, "low": 24970.0, "close": 25005.0},
            {"timestamp_ist": f"{TEST_DATE} 11:03", "open": 25005.0, "high": 25040.0, "low": 25000.0, "close": 25010.0},
        ])

        summary = evaluate_sniper_outcomes(target_date=TEST_DATE, candles_df=candles)

        rows = _read_rows()
        tp_row = rows[f"{TEST_DATE} 10:30"]
        sl_row = rows[f"{TEST_DATE} 11:00"]

        assert tp_row["outcome"] == "TP_HIT", f"expected TP_HIT, got {tp_row['outcome']!r}"
        assert abs(float(tp_row["net_spot_pts"]) - 44.00) < 1e-9, f"TP net={tp_row['net_spot_pts']!r}"
        assert abs(float(tp_row["exit_price"]) - 24955.0) < 1e-9, f"TP exit={tp_row['exit_price']!r}"

        assert sl_row["outcome"] == "SL_HIT", f"expected SL_HIT, got {sl_row['outcome']!r}"
        assert abs(float(sl_row["net_spot_pts"]) - (-34.00)) < 1e-9, f"SL net={sl_row['net_spot_pts']!r}"
        assert abs(float(sl_row["exit_price"]) - 25033.0) < 1e-9, f"SL exit={sl_row['exit_price']!r}"

        assert summary["total_evaluated"] == 2
        assert summary["tp_hit"] == 1
        assert summary["sl_hit"] == 1
        assert abs(summary["net_spot_pts"] - 10.00) < 1e-9, f"net={summary['net_spot_pts']!r}"

        print("\n[PASS] TP_HIT = +44.00 net spot pts, SL_HIT = -34.00 net spot pts written correctly.")
        print(f"[PASS] Summary: {summary}")
    finally:
        _write_header_only()
        print("[CLEANUP] live_sniper_c2a_forward_log.csv restored to header-only (27 columns).")


if __name__ == "__main__":
    main()
