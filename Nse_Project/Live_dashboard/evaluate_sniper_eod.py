"""Post-market EOD outcome evaluator for the VWAP Breakdown Sniper (PE · C2-A).

Fills in the exact ``TP_HIT`` (+45 pts) / ``SL_HIT`` / ``EOD_EXIT`` (15:18 IST)
outcome for every live forward-logged Sniper signal using that trading day's
1-minute intraday candles.  The bar-by-bar simulation mirrors ``Model C2-A`` in
``TESTING/backtest_cvd_atr.py`` (short PE, no early breakeven trail, 1.0 pt
round-trip slippage, 0.5 option-delta estimate).

Can be imported (pass ``candles_df`` to avoid network access) or run standalone:

    python Nse_Project/Live_dashboard/evaluate_sniper_eod.py --date 2026-11-14
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import os
import sys
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pandas as pd
import requests

from Nse_Project.Live_dashboard import sniper_logger

IST = timezone(timedelta(hours=5, minutes=30))

INDEX_INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
MASTER_INSTRUMENTS_URL = (
    "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
)
UPSTOX_MASTER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
}

SLIPPAGE_PTS = 1.0          # round-trip slippage/brokerage in NIFTY points
OPTION_DELTA_EST = 0.5       # ATM option delta for the rupee P&L estimate
DEFAULT_TP_PTS = 45.0
EOD_EXIT_HHMM = (15, 18)     # flat time exit

OUTCOME_COLUMNS = [
    "entry_price",
    "exit_time_ist",
    "exit_price",
    "outcome",
    "net_spot_pts",
    "net_opt_pts",
    "mfe_pts",
    "mae_pts",
]

# Per-run caches (module level) so the instrument master / candles are fetched once.
_future_contracts_cache: Optional[List[Dict[str, Any]]] = None
_candles_cache: Dict[tuple, pd.DataFrame] = {}


def _resolve_access_token() -> Optional[str]:
    """Read the active Upstox V3 access token (READ-ONLY, mirrors the live feed)."""
    env_token = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()
    if env_token:
        return env_token

    candidates = [
        _PROJECT_ROOT / "Nse_Project" / "Brokers" / "upstox" / "upstox_v3_token.json",
        _PROJECT_ROOT / "Nse_Project" / "Brokers" / "upstox" / "upstox" / "upstox_token.json",
        _PROJECT_ROOT / "Nse_Project" / "Brokers" / "upstox" / "upstox_login_details.json",
    ]
    for path in candidates:
        try:
            if not path.exists():
                continue
            data = json.loads(path.read_text(encoding="utf-8"))
            token = data.get("access_token") if isinstance(data, dict) else None
            if token:
                return str(token)
        except (OSError, ValueError):
            continue
    return None


def _load_nifty_future_contracts() -> List[Dict[str, Any]]:
    """Download the Upstox NSE master and return NIFTY FUTIDX contracts sorted by expiry."""
    try:
        resp = requests.get(MASTER_INSTRUMENTS_URL, headers=UPSTOX_MASTER_HEADERS, timeout=30)
        resp.raise_for_status()
        df = pd.read_csv(io.BytesIO(gzip.decompress(resp.content)))
    except Exception:
        return []

    cols = {str(c).lower().strip(): c for c in df.columns}
    key_col = cols.get("instrument_key")
    name_col = cols.get("name")
    type_col = cols.get("instrument_type")
    expiry_col = cols.get("expiry")
    if not (key_col and type_col and expiry_col):
        return []

    df = df.copy()
    df[type_col] = df[type_col].astype(str).str.upper().str.strip()
    fut = df[df[type_col].eq("FUTIDX")]
    if name_col:
        fut = fut[fut[name_col].astype(str).str.upper().str.strip().eq("NIFTY")]
    fut = fut.copy()
    fut[expiry_col] = pd.to_datetime(fut[expiry_col], errors="coerce")
    fut = fut.dropna(subset=[expiry_col, key_col])
    fut = fut.sort_values(expiry_col)

    return [
        {"instrument_key": str(row[key_col]), "expiry": row[expiry_col]}
        for _, row in fut.iterrows()
    ]


def _resolve_nifty_future_key(target_date_str: str) -> Optional[str]:
    """Return the nearest (current-month) NIFTY FUTIDX key active on ``target_date_str``."""
    global _future_contracts_cache
    if _future_contracts_cache is None:
        _future_contracts_cache = _load_nifty_future_contracts()
    target = pd.Timestamp(target_date_str)
    for contract in _future_contracts_cache or []:
        if contract["expiry"] >= target:
            return contract["instrument_key"]
    return None


def _fetch_1m_candles(instrument_key: str, target_date_str: str, token: str) -> pd.DataFrame:
    """Fetch one trading day of 1-minute candles (V3 endpoint, V2 fallback)."""
    encoded = urllib.parse.quote(instrument_key, safe="")
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    urls = [
        f"https://api.upstox.com/v3/historical-candle/{encoded}/1minute/{target_date_str}/{target_date_str}",
        f"https://api.upstox.com/v2/historical-candle/{encoded}/1minute/{target_date_str}/{target_date_str}",
    ]
    last_error: Optional[str] = None
    for url in urls:
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code != 200:
                last_error = f"HTTP {resp.status_code}: {resp.text[:160]}"
                continue
            data = resp.json()
            candles = data.get("data", {}).get("candles", []) if isinstance(data, dict) else []
            if not candles:
                last_error = "empty candle payload"
                continue
            df = pd.DataFrame(
                candles,
                columns=["timestamp_ist", "open", "high", "low", "close", "volume", "oi"],
            )
            df["timestamp_ist"] = (
                pd.to_datetime(df["timestamp_ist"], errors="coerce", utc=True)
                .dt.tz_convert(IST)
                .dt.tz_localize(None)
            )
            for col in ("open", "high", "low", "close"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["timestamp_ist", "open", "high", "low", "close"])
            df = df.sort_values("timestamp_ist").reset_index(drop=True)
            return df[["timestamp_ist", "open", "high", "low", "close"]]
        except Exception as exc:  # noqa: BLE001 - keep the feed alive on any fetch error
            last_error = str(exc)
            continue

    if last_error:
        print(
            f"[WARN] 1m candles unavailable for {instrument_key} on "
            f"{target_date_str}: {last_error}"
        )
    return pd.DataFrame(columns=["timestamp_ist", "open", "high", "low", "close"])


def _normalize_candles_df(candles_df) -> pd.DataFrame:
    """Coerce a caller-supplied DataFrame into the canonical 1m candle schema."""
    if candles_df is None:
        return pd.DataFrame()
    df = candles_df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    ts_col = next(
        (c for c in ("timestamp_ist", "timestamp", "datetime", "ist_time", "ts") if c in df.columns),
        None,
    )
    if ts_col is None:
        raise ValueError("candles_df must include a timestamp column (timestamp_ist/timestamp/datetime).")

    o_col = next((c for c in ("open", "o") if c in df.columns), None)
    h_col = next((c for c in ("high", "h") if c in df.columns), None)
    l_col = next((c for c in ("low", "l") if c in df.columns), None)
    c_col = next((c for c in ("close", "c") if c in df.columns), None)
    if not all((o_col, h_col, l_col, c_col)):
        raise ValueError("candles_df must include open/high/low/close columns.")

    ts = pd.to_datetime(df[ts_col], errors="coerce")
    if getattr(ts.dt, "tz", None) is not None:
        ts = ts.dt.tz_convert(IST).dt.tz_localize(None)

    out = pd.DataFrame({
        "timestamp_ist": ts,
        "open": pd.to_numeric(df[o_col], errors="coerce"),
        "high": pd.to_numeric(df[h_col], errors="coerce"),
        "low": pd.to_numeric(df[l_col], errors="coerce"),
        "close": pd.to_numeric(df[c_col], errors="coerce"),
    })
    out = out.dropna(subset=["timestamp_ist", "open", "high", "low", "close"])
    out = out.sort_values("timestamp_ist").reset_index(drop=True)
    return out


def _parse_bucket(bucket: Any) -> Optional[datetime]:
    text = str(bucket or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _fmt_time(ts: Any) -> str:
    if ts is None:
        return ""
    try:
        return pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M")
    except (TypeError, ValueError):
        return str(ts)


def _simulate_row(row: Dict[str, Any], candles: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """Bar-by-bar Model C2-A simulation for a single forward-log row.

    Returns the 8 outcome-column values, or ``None`` when the row cannot be
    evaluated (unparseable bucket or missing entry price).
    """
    bucket = _parse_bucket(row.get("candle_2m_bucket"))
    if bucket is None:
        return None

    signal_close = bucket + timedelta(minutes=2)
    spot_sl = sniper_logger._to_float(row.get("spot_sl_pts"))
    spot_tp = sniper_logger._to_float(row.get("spot_target_pts"), DEFAULT_TP_PTS)
    future_ltp = sniper_logger._to_float(row.get("future_ltp"))
    spot_price = sniper_logger._to_float(row.get("spot_price"))

    eod_deadline = datetime(
        bucket.year, bucket.month, bucket.day, EOD_EXIT_HHMM[0], EOD_EXIT_HHMM[1]
    )

    if candles is None or candles.empty:
        window = pd.DataFrame()
    else:
        window = candles[
            (candles["timestamp_ist"] >= signal_close)
            & (candles["timestamp_ist"] <= eod_deadline)
        ].reset_index(drop=True)

    if window.empty:
        entry_price = future_ltp if future_ltp > 0 else spot_price
        if entry_price <= 0:
            return None
        return {
            "entry_price": round(entry_price, 2),
            "exit_time_ist": "",
            "exit_price": round(entry_price, 2),
            "outcome": "EOD_EXIT",
            "net_spot_pts": round(-SLIPPAGE_PTS, 2),
            "net_opt_pts": round(-OPTION_DELTA_EST * SLIPPAGE_PTS, 2),
            "mfe_pts": 0.0,
            "mae_pts": 0.0,
        }

    entry_price = float(window.iloc[0]["open"])
    if entry_price <= 0:
        entry_price = future_ltp if future_ltp > 0 else spot_price
    if entry_price <= 0:
        return None

    sl_level = entry_price + spot_sl
    tp_level = entry_price - spot_tp

    mfe = 0.0
    mae = 0.0
    outcome = "EOD_EXIT"
    exit_price: Optional[float] = None
    exit_time = None

    for _, bar in window.iterrows():
        high = float(bar["high"])
        low = float(bar["low"])
        mfe = max(mfe, max(0.0, entry_price - low))
        mae = max(mae, max(0.0, high - entry_price))
        if high >= sl_level:  # conservative: SL checked before TP in the same bar
            outcome = "SL_HIT"
            exit_price = sl_level
            exit_time = bar["timestamp_ist"]
            break
        if low <= tp_level:
            outcome = "TP_HIT"
            exit_price = tp_level
            exit_time = bar["timestamp_ist"]
            break

    if exit_price is None:  # neither hit before 15:18 IST -> flat EOD exit
        exit_price = float(window.iloc[-1]["close"])
        exit_time = window.iloc[-1]["timestamp_ist"]

    net_spot = round((entry_price - exit_price) - SLIPPAGE_PTS, 2)
    return {
        "entry_price": round(entry_price, 2),
        "exit_time_ist": _fmt_time(exit_time),
        "exit_price": round(exit_price, 2),
        "outcome": outcome,
        "net_spot_pts": net_spot,
        "net_opt_pts": round(OPTION_DELTA_EST * net_spot, 2),
        "mfe_pts": round(mfe, 2),
        "mae_pts": round(mae, 2),
    }


def _read_all_rows(csv_path: str) -> List[Dict[str, str]]:
    if not os.path.exists(csv_path):
        return []
    with open(csv_path, "r", newline="", encoding="utf-8") as fh:
        return [dict(r) for r in csv.DictReader(fh)]


def _write_all_rows(csv_path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    tmp_path = csv_path + ".tmp"
    with open(tmp_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=sniper_logger.CSV_HEADERS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    os.replace(tmp_path, csv_path)


def _get_day_candles(row_date: str, use_futures: bool) -> pd.DataFrame:
    cache_key = (row_date, bool(use_futures))
    if cache_key in _candles_cache:
        return _candles_cache[cache_key]

    token = _resolve_access_token()
    if not token:
        print("[WARN] No Upstox access token found; skipping intraday candle fetch.")
        _candles_cache[cache_key] = pd.DataFrame()
        return _candles_cache[cache_key]

    if use_futures:
        instrument_key = _resolve_nifty_future_key(row_date) or INDEX_INSTRUMENT_KEY
    else:
        instrument_key = INDEX_INSTRUMENT_KEY

    _candles_cache[cache_key] = _fetch_1m_candles(instrument_key, row_date, token)
    return _candles_cache[cache_key]


def _build_summary(evaluated_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(evaluated_rows)
    tp = sum(1 for r in evaluated_rows if r.get("outcome") == "TP_HIT")
    sl = sum(1 for r in evaluated_rows if r.get("outcome") == "SL_HIT")
    eod = sum(1 for r in evaluated_rows if r.get("outcome") == "EOD_EXIT")
    net_spot = sum(sniper_logger._to_float(r.get("net_spot_pts")) for r in evaluated_rows)
    net_opt = sum(sniper_logger._to_float(r.get("net_opt_pts")) for r in evaluated_rows)
    win_rate = (tp / total * 100.0) if total else 0.0
    return {
        "total_evaluated": total,
        "tp_hit": tp,
        "sl_hit": sl,
        "eod_exit": eod,
        "win_rate_pct": round(win_rate, 2),
        "net_spot_pts": round(net_spot, 2),
        "net_opt_pts": round(net_opt, 2),
    }


def _print_summary(summary: Dict[str, Any]) -> None:
    print("=" * 66)
    print("  VWAP BREAKDOWN SNIPER (PE · C2-A) — FORWARD-TEST SUMMARY")
    print("=" * 66)
    print(f"  Total Evaluated : {summary['total_evaluated']}")
    print(f"  TP_HIT          : {summary['tp_hit']}")
    print(f"  SL_HIT          : {summary['sl_hit']}")
    print(f"  EOD_EXIT        : {summary['eod_exit']}")
    print(f"  Win Rate %      : {summary['win_rate_pct']:.2f}")
    print(f"  Net Spot Pts    : {summary['net_spot_pts']:.2f}")
    print(f"  Est Option Pts  : {summary['net_opt_pts']:.2f}")
    print("=" * 66)


def evaluate_sniper_outcomes(
    target_date: Optional[str] = None,
    force: bool = False,
    candles_df=None,
) -> Dict[str, Any]:
    """Evaluate + persist EOD outcomes for live VWAP Sniper (PE · C2-A) signals.

    ``target_date``  : ISO date to evaluate (``None`` -> every unevaluated date).
    ``force``        : re-evaluate rows that already carry an outcome.
    ``candles_df``   : optional 1-minute DataFrame (timestamp_ist/open/high/low/close);
                       when omitted, candles are fetched from Upstox.
    """
    sniper_logger.migrate_csv_header_if_needed(sniper_logger.SIGNAL_CSV_PATH)

    rows = _read_all_rows(sniper_logger.SIGNAL_CSV_PATH)
    to_evaluate: List[Dict[str, Any]] = []
    for row in rows:
        bucket = _parse_bucket(row.get("candle_2m_bucket"))
        if bucket is None:
            continue
        row_date = bucket.strftime("%Y-%m-%d")
        if target_date and row_date != target_date:
            continue
        if not force and (row.get("outcome") or "").strip():
            continue
        to_evaluate.append(row)

    if not to_evaluate:
        summary = _build_summary([])
        _print_summary(summary)
        return summary

    normalized = _normalize_candles_df(candles_df) if candles_df is not None else None

    results_by_bucket: Dict[str, Dict[str, Any]] = {}
    evaluated_rows: List[Dict[str, Any]] = []
    for row in to_evaluate:
        bucket = _parse_bucket(row.get("candle_2m_bucket"))
        row_date = bucket.strftime("%Y-%m-%d")
        use_futures = sniper_logger._to_float(row.get("future_ltp")) > 0

        if normalized is not None and not normalized.empty:
            day_candles = normalized[normalized["timestamp_ist"].dt.date == bucket.date()]
        else:
            day_candles = _get_day_candles(row_date, use_futures)

        result = _simulate_row(row, day_candles)
        if result is None:
            continue

        bucket_key = (row.get("candle_2m_bucket") or "").strip()
        results_by_bucket[bucket_key] = result
        merged = dict(row)
        merged.update(result)
        evaluated_rows.append(merged)

    if results_by_bucket:
        with sniper_logger._lock:
            fresh_rows = _read_all_rows(sniper_logger.SIGNAL_CSV_PATH)
            for fresh in fresh_rows:
                key = (fresh.get("candle_2m_bucket") or "").strip()
                result = results_by_bucket.get(key)
                if result:
                    fresh.update(result)
            _write_all_rows(sniper_logger.SIGNAL_CSV_PATH, fresh_rows)

    for merged in evaluated_rows:
        print(
            f"  {merged.get('candle_2m_bucket')}  entry={merged.get('entry_price')} "
            f"-> {merged.get('outcome')} @ {merged.get('exit_price')} "
            f"(net {merged.get('net_spot_pts')} pts)"
        )

    summary = _build_summary(evaluated_rows)
    _print_summary(summary)
    return summary


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate VWAP Breakdown Sniper (PE · C2-A) post-market outcomes."
    )
    parser.add_argument("--date", dest="target_date", default=None, help="Trading date YYYY-MM-DD.")
    parser.add_argument("--force", action="store_true", help="Re-evaluate already-evaluated rows.")
    args = parser.parse_args(argv)
    evaluate_sniper_outcomes(target_date=args.target_date, force=args.force)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())




