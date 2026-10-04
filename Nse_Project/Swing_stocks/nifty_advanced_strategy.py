r"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║   NIFTY 500 — SMC PRECISION ENTRY ENGINE (HYBRID ML DATA VER.)                 ║
║   Smart Money Concepts + Minervini SEPA + Institutional Flow                     ║
║                                                                                  ║
║   UPDATES:                                                                       ║
║     • BLAZING FAST: Removed 5m resampling. Reads directly from Day/Week Parquets ║
║     • AUTO-MAPPING: Maps NSE Symbols to Upstox Instrument Keys automatically     ║
║     • DATA SOURCES: D:\Api data\ML_HISTO_DATA\Upstox Data\ (Day Data & Weeks)    ║
╚══════════════════════════════════════════════════════════════════════════════════╝
"""

import pandas as pd
import yfinance as yf
import pandas_ta as ta
import json
import os
import sqlite3
import sys
import threading
import gzip
import requests
from collections import Counter
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import warnings
from scipy.signal import argrelextrema
import numpy as np

try:
    from .combo_filter import filter_combo_setups
except ImportError:
    from combo_filter import filter_combo_setups

# ── గ్లోబల్ వేరియబుల్ (Global Variable) ──────────────────────────
SECTOR_MAP = {
    "Technology": "^CNXIT",                
    "Financial Services": "^NSEBANK",      
    "Consumer Cyclical": "^CNXAUTO",       
    "Healthcare": "^CNXPHARMA",            
    "Consumer Defensive": "^CNXFMCG",      
    "Basic Materials": "^CNXMETAL",        
    "Energy": "^CNXENERGY",                
    "Industrials": "^CNXINFRA",            
    "Real Estate": "^CNXREALTY",           
    "Communication Services": "^CNXMEDIA", 
    "Utilities": "^CNXENERGY"              
}

warnings.filterwarnings("ignore")

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION & NEW DATA PATHS
# ════════════════════════════════════════════════════════════════════════════════

CACHE_FILE         = os.path.join(BASE_DIR, "nifty_yf_cache.json")
OUTPUT_CSV         = os.path.join(BASE_DIR, "nifty200_smc_precision_screen.csv")
COMBO_CONTEXT_FILE = os.path.join(BASE_DIR, "combo_filter_context.json")
STATUS_FILE        = os.path.join(BASE_DIR, "scan_status.json")

# 🔴 NEW PATHS: Pointing to your 25-Year Data
DAY_DATA_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox Data\Day Data"
WEEK_DATA_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox Data\Weeks Data"
NIFTY_CSV_FILE = r"C:\Users\ADMIN\Desktop\MyProject2\Nse_Project\ind_nifty500list.csv"
FNO_DATABASE_PATH = r"D:\FNO DATA\Participant data\Database\fno_swing.db"

CACHE_EXPIRY_HOURS = 6
MAX_WORKERS        = 5
MAX_DATA_AGE_DAYS  = 3

SWING_LOOKBACK     = 10   
ATR_SL_MULTIPLIER  = 0.5  
MIN_RR             = 2.0  

SCORE_MAX_POINTS = 189.0
SCORE_NORMALIZER = SCORE_MAX_POINTS / 100.0

# ════════════════════════════════════════════════════════════════════════════════
# RUN TELEMETRY
# ════════════════════════════════════════════════════════════════════════════════

SKIP_REASONS = {
    "missing_file":       "Parquet file not found in Day/Week folder",
    "unreadable_parquet": "Parquet unreadable / corrupt",
    "empty_parquet":      "Parquet contained no rows",
    "bad_datetime_index": "Datetime index unparseable",
    "stale_data":         "Data older than the freshness limit",
    "missing_columns":    "Required OHLCV column(s) absent",
    "short_history":      "Fewer than 20 daily bars",
    "exception":          "Unhandled exception in worker",
    "ltp_filtered":       "LTP outside configured range",
}

_TALLY = Counter()
_TALLY_LOCK = threading.Lock()
_PRINT_LOCK = threading.Lock()

def _log(message: str) -> None:
    with _PRINT_LOCK:
        print(message)

def _tally(reason: str, count: int = 1) -> None:
    with _TALLY_LOCK:
        _TALLY[reason] += count

def _tally_snapshot() -> dict:
    with _TALLY_LOCK:
        return dict(_TALLY)

# ════════════════════════════════════════════════════════════════════════════════
# CACHE HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def _json_default(obj):
    if isinstance(obj, np.integer): return int(obj)
    if isinstance(obj, np.floating):
        val = float(obj)
        return val if np.isfinite(val) else None
    if isinstance(obj, np.bool_): return bool(obj)
    if isinstance(obj, np.ndarray): return obj.tolist()
    if isinstance(obj, (pd.Timestamp, datetime)): return obj.isoformat()
    if obj is pd.NaT or (isinstance(obj, float) and not np.isfinite(obj)): return None
    return str(obj)

def load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f: return json.load(f)
        except Exception: return {}
    return {}

def save_cache(cache: dict) -> None:
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2, default=_json_default)
    except Exception as e: print(f"Cache save error: {e}")

def save_combo_context(categories: dict) -> None:
    try:
        with open(COMBO_CONTEXT_FILE, "w") as f:
            json.dump(categories, f, indent=2, default=_json_default)
    except Exception as e: print(f"Combo context save error: {e}")

def save_run_status(status: dict) -> None:
    try:
        with open(STATUS_FILE, "w") as f:
            json.dump(status, f, indent=2, default=_json_default)
    except Exception as e: print(f"Run status save error: {e}")

# ════════════════════════════════════════════════════════════════════════════════
# UTILITY
# ════════════════════════════════════════════════════════════════════════════════

def _safe(val) -> float | None:
    try:
        if val is None: return None
        v = float(val)
        return round(v, 4) if not np.isnan(v) else None
    except Exception: return None

def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
    return df

def _safe_float(val, default=0.0) -> float:
    if val is None: return default
    try:
        if hasattr(val, "iloc") and not val.empty: val = val.iloc[-1]
        elif hasattr(val, "item"): val = val.item()
        return float(val)
    except (ValueError, TypeError, IndexError): return default

def _ta_last(obj, prefix: str | None = None) -> float:
    try:
        if obj is None: return 0.0
        if isinstance(obj, pd.DataFrame):
            if obj.empty: return 0.0
            if prefix:
                cols = [c for c in obj.columns if str(c).startswith(prefix)]
                if not cols: return 0.0
                series = obj[cols[0]]
            else: series = obj.iloc[:, 0]
        else: series = obj
        series = series.dropna()
        if series.empty: return 0.0
        val = float(series.iloc[-1])
        return val if np.isfinite(val) else 0.0
    except Exception: return 0.0

def evaluate_freshness(latest_ts, today=None) -> dict:
    if latest_ts is None or pd.isna(latest_ts):
        return {"fresh": False, "sessions_behind": 10 ** 6, "age_days": 10 ** 6, "as_of": None}
    last_date = pd.Timestamp(latest_ts).date()
    today = today or pd.Timestamp.now().floor("D").date()
    age_days = (today - last_date).days
    if age_days <= 0:
        return {"fresh": True, "sessions_behind": 0, "age_days": max(age_days, 0), "as_of": last_date}
    sessions_behind = int(np.busday_count((last_date + timedelta(days=1)).isoformat(), (today + timedelta(days=1)).isoformat()))
    return {"fresh": sessions_behind <= MAX_DATA_AGE_DAYS, "sessions_behind": sessions_behind, "age_days": age_days, "as_of": last_date}

# ════════════════════════════════════════════════════════════════════════════════
# SMC MODULES
# ════════════════════════════════════════════════════════════════════════════════

def find_swing_pivots(high: np.ndarray, low: np.ndarray, n: int = 2):
    sh_idx = argrelextrema(high, np.greater, order=n)[0].tolist()
    sl_idx = argrelextrema(low, np.less, order=n)[0].tolist()
    return sh_idx, sl_idx

def _anchored_vwap(high: np.ndarray, low: np.ndarray, close: np.ndarray, volume: np.ndarray, anchor_idx: int) -> float | None:
    if anchor_idx < 0 or anchor_idx >= len(close):
        return None
    typical_price = (high[anchor_idx:] + low[anchor_idx:] + close[anchor_idx:]) / 3.0
    anchored_volume = volume[anchor_idx:]
    valid = np.isfinite(typical_price) & np.isfinite(anchored_volume) & (anchored_volume >= 0)
    if not valid.any() or float(anchored_volume[valid].sum()) <= 0:
        return None
    return float(np.sum(typical_price[valid] * anchored_volume[valid]) / np.sum(anchored_volume[valid]))

def get_fno_market_context() -> dict:
    context = {"smart_money_score": 0.0, "retail_trap": False}
    try:
        database_uri = f"{Path(FNO_DATABASE_PATH).as_uri()}?mode=ro"
        connection = sqlite3.connect(database_uri, uri=True)
        try:
            connection.row_factory = sqlite3.Row
            latest = connection.execute(
                "SELECT smart_money_score, retail_trap FROM cross_signals ORDER BY date DESC LIMIT 1"
            ).fetchone()
        finally:
            connection.close()
        if latest is not None:
            context["smart_money_score"] = _safe_float(latest["smart_money_score"])
            context["retail_trap"] = bool(latest["retail_trap"])
    except (OSError, sqlite3.Error, ValueError) as exc:
        _log(f"  [FNO] Market context unavailable; using neutral defaults: {exc}")
    return context

def detect_liquidity_sweep(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    open_: np.ndarray,
    sh_idx: list,
    sl_idx: list,
    lookback: int | None = None,
    atr_values: np.ndarray | None = None,
    atr_period: int = 14,
    min_sweep_atr: float = 0.05,
):
    """Detect the newest ATR-scaled liquidity sweep with a reclaim close."""
    result = {"swept": False, "direction": None, "sweep_level": None,
              "sweep_bar": None, "sweep_low": None, "sweep_high": None,
              "atr": None, "lookback": None}
    high, low, close, open_ = map(np.asarray, (high, low, close, open_))
    n = len(close)
    if n < 3:
        return result

    true_range = np.maximum(high[1:] - low[1:], np.maximum(
        np.abs(high[1:] - close[:-1]), np.abs(low[1:] - close[:-1])))
    true_range = np.concatenate(([high[0] - low[0]], true_range))
    atr = (pd.Series(true_range).rolling(atr_period, min_periods=1).mean().to_numpy()
           if atr_values is None else np.asarray(atr_values, dtype=float))
    atr_now = float(atr[-1]) if len(atr) and np.isfinite(atr[-1]) else 0.0
    volatility_ratio = atr_now / max(float(np.nanmedian(true_range[-atr_period:])), 1e-9)
    base = int(lookback or 10)
    dynamic_lookback = int(np.clip(round(base / max(volatility_ratio, 0.25)), 5, max(base * 2, 5)))
    result["atr"], result["lookback"] = round(atr_now, 4), dynamic_lookback
    check_range = range(max(n - dynamic_lookback, 1), n)
    min_penetration = atr_now * float(min_sweep_atr)

    recent_low_pivot = max((int(i) for i in sl_idx if int(i) < n - 1), default=None)
    if recent_low_pivot is not None:
        level = float(low[recent_low_pivot])
        for i in check_range:
            if low[i] <= level - min_penetration and close[i] > level and close[i] > open_[i]:
                result.update({"swept": True, "direction": "bullish",
                               "sweep_level": round(level, 4), "sweep_bar": i,
                               "sweep_low": round(float(low[i]), 4),
                               "sweep_high": round(float(high[i]), 4)})
                return result

    recent_high_pivot = max((int(i) for i in sh_idx if int(i) < n - 1), default=None)
    if recent_high_pivot is not None:
        level = float(high[recent_high_pivot])
        for i in check_range:
            if high[i] >= level + min_penetration and close[i] < level and close[i] < open_[i]:
                result.update({"swept": True, "direction": "bearish",
                               "sweep_level": round(level, 4), "sweep_bar": i,
                               "sweep_low": round(float(low[i]), 4),
                               "sweep_high": round(float(high[i]), 4)})
                return result
    return result


def detect_fvg(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    direction: str | None,
    lookback: int | None = None,
    atr_values: np.ndarray | None = None,
    weekly_high: np.ndarray | None = None,
    weekly_low: np.ndarray | None = None,
    weekly_close: np.ndarray | None = None,
    max_age_bars: int = 30,
):
    """Find the newest unmitigated, time-valid FVG and optional weekly nesting."""
    result = {"found": False, "fvg_high": None, "fvg_low": None, "fvg_mid": None,
              "retesting": False, "direction": direction, "mitigated": False,
              "expired": False, "nested": False, "weekly_fvg_high": None,
              "weekly_fvg_low": None}
    if direction not in {"bullish", "bearish"}:
        return result
    high, low, close = map(lambda x: np.asarray(x, dtype=float), (high, low, close))
    n = len(close)
    if n < 3:
        return result

    tr = np.maximum(high[1:] - low[1:], np.maximum(np.abs(high[1:] - close[:-1]),
                                                     np.abs(low[1:] - close[:-1])))
    tr = np.concatenate(([high[0] - low[0]], tr))
    atr = (pd.Series(tr).rolling(14, min_periods=1).mean().to_numpy()
           if atr_values is None else np.asarray(atr_values, dtype=float))
    atr_now = float(atr[-1]) if len(atr) and np.isfinite(atr[-1]) else 0.0
    vol_ratio = atr_now / max(float(np.nanmedian(tr[-14:])), 1e-9)
    base = int(lookback or 20)
    dynamic_lookback = int(np.clip(round(base / max(vol_ratio, 0.25)), 8, max(base * 2, 8)))

    def _zones(h, l, c, window):
        found = []
        start = max(2, len(c) - window)
        for i in range(len(c) - 1, start - 1, -1):
            if direction == "bullish" and h[i - 2] < l[i]:
                found.append((i, float(h[i - 2]), float(l[i])))
            elif direction == "bearish" and l[i - 2] > h[i]:
                found.append((i, float(h[i]), float(l[i - 2])))
        return found

    def _nested(zone_low, zone_high):
        if weekly_high is None or weekly_low is None or weekly_close is None:
            return False, None
        wh, wl, wc = map(lambda x: np.asarray(x, dtype=float),
                          (weekly_high, weekly_low, weekly_close))
        for _, weekly_low_zone, weekly_high_zone in _zones(wh, wl, wc, len(wc)):
            overlap = max(zone_low, weekly_low_zone) <= min(zone_high, weekly_high_zone)
            inside = zone_low >= weekly_low_zone and zone_high <= weekly_high_zone
            if overlap or inside:
                return True, (weekly_low_zone, weekly_high_zone)
        return False, None

    for bar, zone_low, zone_high in _zones(high, low, close, dynamic_lookback):
        if n - 1 - bar > int(max_age_bars):
            continue
        subsequent_low = low[bar + 1:]
        subsequent_high = high[bar + 1:]
        fully_mitigated = bool(
            np.any(subsequent_low <= zone_low) if direction == "bullish"
            else np.any(subsequent_high >= zone_high)
        )
        if fully_mitigated:
            continue
        nested, weekly_zone = _nested(zone_low, zone_high)
        result.update({"found": True, "fvg_high": round(zone_high, 4),
                       "fvg_low": round(zone_low, 4),
                       "fvg_mid": round((zone_low + zone_high) / 2, 4),
                       "retesting": bool(zone_low <= close[-1] <= zone_high),
                       "nested": nested})
        if weekly_zone:
            result["weekly_fvg_low"], result["weekly_fvg_high"] = map(
                lambda value: round(value, 4), weekly_zone)
        return result
    return result


def detect_mss(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    sh_idx: list,
    sl_idx: list,
    direction: str | None,
    lookback: int | None = None,
    atr_values: np.ndarray | None = None,
    min_break_atr: float = 0.10,
):
    """Confirm MSS with an ATR-scaled displacement beyond the latest pivot."""
    result = {"confirmed": False, "mss_level": None, "mss_bar": None,
              "atr": None, "lookback": None}
    if direction not in {"bullish", "bearish"}:
        return result
    high, low, close = map(lambda x: np.asarray(x, dtype=float), (high, low, close))
    n = len(close)
    if n < 3:
        return result
    tr = np.maximum(high[1:] - low[1:], np.maximum(np.abs(high[1:] - close[:-1]),
                                                     np.abs(low[1:] - close[:-1])))
    tr = np.concatenate(([high[0] - low[0]], tr))
    atr = (pd.Series(tr).rolling(14, min_periods=1).mean().to_numpy()
           if atr_values is None else np.asarray(atr_values, dtype=float))
    atr_now = float(atr[-1]) if len(atr) and np.isfinite(atr[-1]) else 0.0
    vol_ratio = atr_now / max(float(np.nanmedian(tr[-14:])), 1e-9)
    base = int(lookback or 12)
    dynamic_lookback = int(np.clip(round(base / max(vol_ratio, 0.25)), 6, max(base * 2, 6)))
    result["atr"], result["lookback"] = round(atr_now, 4), dynamic_lookback
    noise = max(0.0005, (atr_now * float(min_break_atr)) / max(abs(float(close[-1])), 1e-9))
    pivots = sh_idx if direction == "bullish" else sl_idx
    valid_pivots = [int(i) for i in pivots if 0 <= int(i) < n - 1]
    if not valid_pivots:
        return result
    pivot = valid_pivots[-1]
    level = float(high[pivot] if direction == "bullish" else low[pivot])
    for i in range(n - 1, max(n - dynamic_lookback - 1, pivot + 1), -1):
        broke = (close[i] > level * (1.0 + noise) if direction == "bullish"
                 else close[i] < level * (1.0 - noise))
        if broke:
            result.update({"confirmed": True, "mss_level": round(level, 4),
                           "mss_bar": i})
            return result
    return result


def detect_order_block(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    open_: np.ndarray,
    volume: np.ndarray,
    direction: str | None,
    sweep_bar: int | None,
    lookback: int | None = None,
    atr_values: np.ndarray | None = None,
    profile_price: np.ndarray | None = None,
    profile_volume: np.ndarray | None = None,
    profile_bins: int = 24,
    max_age_bars: int = 30,
):
    """Find a fresh, unmitigated OB aligned with a recent POC/HVN."""
    result = {"found": False, "ob_high": None, "ob_low": None, "ob_mid": None,
              "ob_bar": None, "mitigated": False, "expired": False,
              "profile_node": None, "poc": None, "hvn": []}
    if direction not in {"bullish", "bearish"}:
        return result
    high, low, close, open_, volume = map(
        lambda x: np.asarray(x, dtype=float), (high, low, close, open_, volume))
    n = len(close)
    if n < 4:
        return result

    tr = np.maximum(high[1:] - low[1:], np.maximum(np.abs(high[1:] - close[:-1]),
                                                     np.abs(low[1:] - close[:-1])))
    tr = np.concatenate(([high[0] - low[0]], tr))
    atr = (pd.Series(tr).rolling(14, min_periods=1).mean().to_numpy()
           if atr_values is None else np.asarray(atr_values, dtype=float))
    atr_now = float(atr[-1]) if len(atr) and np.isfinite(atr[-1]) else 0.0
    vol_ratio = atr_now / max(float(np.nanmedian(tr[-14:])), 1e-9)
    base = int(lookback or 15)
    dynamic_lookback = int(np.clip(round(base / max(vol_ratio, 0.25)), 8, max(base * 2, 8)))
    start = min(int(sweep_bar) if sweep_bar is not None else n - 2, n - 2)
    bodies = np.abs(close - open_)
    avg_body = max(float(np.nanmean(bodies[max(0, start - 10):start])), 1e-9)
    avg_vol = max(float(np.nanmean(volume[max(0, start - 10):start])), 1e-9)

    prices = np.asarray(profile_price if profile_price is not None else
                        (high + low + close) / 3.0, dtype=float)
    prof_vol = np.asarray(profile_volume if profile_volume is not None else volume, dtype=float)
    valid = np.isfinite(prices) & np.isfinite(prof_vol) & (prof_vol > 0)
    if valid.any():
        pmin, pmax = float(np.nanmin(prices[valid])), float(np.nanmax(prices[valid]))
        if pmax > pmin:
            edges = np.linspace(pmin, pmax, max(int(profile_bins), 4) + 1)
            totals, _ = np.histogram(prices[valid], bins=edges, weights=prof_vol[valid])
            centers = (edges[:-1] + edges[1:]) / 2.0
            poc_idx = int(np.argmax(totals))
            hvn_idx = np.flatnonzero(totals >= np.percentile(totals, 75))
            poc = float(centers[poc_idx])
            hvn = [float(centers[i]) for i in hvn_idx]
            result["poc"], result["hvn"] = round(poc, 4), [round(x, 4) for x in hvn]
        else:
            poc, hvn = pmin, [pmin]
    else:
        poc, hvn = None, []

    for i in range(start, max(start - dynamic_lookback, 1), -1):
        bullish = close[i] < open_[i] and close[i + 1] > open_[i + 1]
        bearish = close[i] > open_[i] and close[i + 1] < open_[i + 1]
        displacement = abs(close[i + 1] - open_[i + 1]) > avg_body * 1.25
        volume_confirmed = volume[i + 1] > avg_vol * 1.10
        if not ((direction == "bullish" and bullish) or (direction == "bearish" and bearish)):
            continue
        if not (displacement and volume_confirmed) or n - 1 - i > int(max_age_bars):
            continue
        ob_low, ob_high = float(low[i]), float(high[i])
        node_tolerance = max(atr_now * 0.25, (ob_high - ob_low) * 0.10, 1e-9)
        nodes = ([] if poc is None else [poc]) + hvn
        node = next((x for x in nodes if ob_low - node_tolerance <= x <= ob_high + node_tolerance), None)
        if node is None:
            continue
        later_low, later_high = low[i + 1:], high[i + 1:]
        mitigated = bool(np.any(later_low <= ob_low) if direction == "bullish"
                         else np.any(later_high >= ob_high))
        if mitigated:
            continue
        result.update({"found": True, "ob_high": round(ob_high, 4),
                       "ob_low": round(ob_low, 4),
                       "ob_mid": round((ob_low + ob_high) / 2, 4),
                       "ob_bar": i, "profile_node": round(float(node), 4)})
        return result
    return result

def premium_discount_zone(high: np.ndarray, low: np.ndarray, close: np.ndarray, direction: str | None, lookback: int = 50):
    result = {"in_zone": False, "zone_type": None, "fib_50": None, "fib_618": None, "fib_786": None, "range_high": None, "range_low": None}
    if direction is None or len(high) < lookback: return result
    n = len(close)
    window_high = float(high[n - lookback: n].max())
    window_low = float(low[n - lookback: n].min())
    rng = window_high - window_low
    if rng <= 0: return result

    fib_50, fib_618, fib_786 = window_low + rng * 0.500, window_low + rng * 0.382, window_low + rng * 0.214
    current = float(close[n - 1])

    result.update({"fib_50": round(fib_50, 4), "fib_618": round(fib_618, 4), "fib_786": round(fib_786, 4), "range_high": round(window_high, 4), "range_low": round(window_low, 4)})
    if direction == "bullish":
        result.update({"in_zone": current <= fib_50, "zone_type": "discount" if current <= fib_50 else "premium"})
    elif direction == "bearish":
        result.update({"in_zone": current >= fib_50, "zone_type": "premium" if current >= fib_50 else "discount"})
    return result

def displacement_confirmed(close: np.ndarray, open_: np.ndarray, volume: np.ndarray, direction: str | None, lookback: int = 5) -> bool:
    if direction is None: return False
    n = len(close)
    bodies = np.abs(close - open_)
    avg_body = bodies[max(0, n - 20): n].mean()
    avg_vol = volume[max(0, n - 20): n].mean()
    if avg_body == 0 or avg_vol == 0: return False

    for i in range(n - 1, max(n - lookback - 1, -1), -1):
        body, vol = float(bodies[i]), float(volume[i])
        is_bullish_candle, is_bearish_candle = close[i] > open_[i], close[i] < open_[i]
        if direction == "bullish" and is_bullish_candle and body >= avg_body * 1.5 and vol >= avg_vol * 1.5: return True
        if direction == "bearish" and is_bearish_candle and body >= avg_body * 1.5 and vol >= avg_vol * 1.5: return True
    return False

def smc_entry_signal(hist: pd.DataFrame, atr_value: float | None):
    default = {"smc_signal": "NO_SETUP", "smc_direction": None, "smc_score": 0, "entry_price": None, "stop_loss": None, "take_profit": None, "rr_ratio": None, "sweep_level": None, "fvg_high": None, "fvg_low": None, "fvg_mid": None, "ob_high": None, "ob_low": None, "fib_50": None, "in_fib_zone": False, "mss_confirmed": False, "displaced": False, "confluence": 0}
    try:
        h_arr, l_arr, c_arr, o_arr, v_arr = hist["High"].values.astype(float), hist["Low"].values.astype(float), hist["Close"].values.astype(float), hist["Open"].values.astype(float), hist["Volume"].values.astype(float)
        sh_idx, sl_idx = find_swing_pivots(h_arr, l_arr, n=SWING_LOOKBACK)
        if not sh_idx or not sl_idx: return default

        sweep = detect_liquidity_sweep(h_arr, l_arr, c_arr, o_arr, sh_idx, sl_idx)
        if not sweep["swept"]: return default

        direction, sweep_bar, sweep_low, sweep_high, sweep_level = sweep["direction"], sweep["sweep_bar"], sweep["sweep_low"], sweep["sweep_high"], sweep["sweep_level"]
        fvg = detect_fvg(h_arr, l_arr, c_arr, direction)
        mss = detect_mss(h_arr, l_arr, c_arr, sh_idx, sl_idx, direction)
        ob = detect_order_block(h_arr, l_arr, c_arr, o_arr, v_arr, direction, sweep_bar)

        if mss["confirmed"] and mss["mss_bar"] is not None and sweep_bar is not None and mss["mss_bar"] < sweep_bar - 1: return default
        if ob["found"] and ob["ob_bar"] is not None and mss["mss_bar"] is not None and ob["ob_bar"] > mss["mss_bar"] + 1: return default

        fib = premium_discount_zone(h_arr, l_arr, c_arr, direction)
        displaced = displacement_confirmed(c_arr, o_arr, v_arr, direction)

        confluence = sum([sweep["swept"], fvg["found"], fvg["retesting"], mss["confirmed"], ob["found"], fib["in_zone"], displaced])
        avwap_levels = (
            _anchored_vwap(np.asarray(h_arr, dtype=float), np.asarray(l_arr, dtype=float), np.asarray(c_arr, dtype=float), np.asarray(v_arr, dtype=float), sh_idx[-1]),
            _anchored_vwap(np.asarray(h_arr, dtype=float), np.asarray(l_arr, dtype=float), np.asarray(c_arr, dtype=float), np.asarray(v_arr, dtype=float), sl_idx[-1]),
        )
        has_fvg_or_sweep = bool(fvg["found"] or sweep["swept"])
        if direction == "bullish":
            avwap_rejection = any(level is not None and l_arr[-1] <= level < c_arr[-1] for level in avwap_levels)
        else:
            avwap_rejection = any(level is not None and c_arr[-1] < level <= h_arr[-1] for level in avwap_levels)
        if avwap_rejection and has_fvg_or_sweep:
            confluence += 1

        signal = "WEAK_SETUP" if confluence < 3 else "MODERATE_SETUP" if confluence < 5 else "HIGH_CONFLUENCE"

        atr_buf = float(atr_value) * ATR_SL_MULTIPLIER if atr_value else 0.0
        entry_price = stop_loss = take_profit = rr_ratio = None
        risk = 0.0

        if direction == "bullish":
            candidates = [x for x in [fvg.get("fvg_mid"), ob.get("ob_mid")] if x is not None]
            entry_price = max(candidates) if candidates else float(c_arr[-1])
            stop_loss = round(sweep_low - atr_buf, 4)
            risk = max(entry_price - stop_loss, atr_buf if atr_buf > 0 else entry_price * 0.01)
            tp_candidates = [float(h_arr[i]) for i in sh_idx if float(h_arr[i]) > entry_price]
            take_profit = round(min(tp_candidates), 4) if tp_candidates else round(entry_price + risk * MIN_RR, 4)

        elif direction == "bearish":
            candidates = [x for x in [fvg.get("fvg_mid"), ob.get("ob_mid")] if x is not None]
            entry_price = min(candidates) if candidates else float(c_arr[-1])
            stop_loss = round(sweep_high + atr_buf, 4)
            risk = max(stop_loss - entry_price, atr_buf if atr_buf > 0 else entry_price * 0.01)
            tp_candidates = [float(l_arr[i]) for i in sl_idx if float(l_arr[i]) < entry_price]
            take_profit = round(max(tp_candidates), 4) if tp_candidates else round(entry_price - risk * MIN_RR, 4)

        if risk > 0: rr_ratio = round(abs(take_profit - entry_price) / risk, 2)
        if rr_ratio is not None and rr_ratio < MIN_RR: signal = "LOW_RR"

        return {"smc_signal": signal, "smc_direction": direction, "smc_score": confluence, "entry_price": round(entry_price, 4) if entry_price else None, "stop_loss": stop_loss, "take_profit": take_profit, "rr_ratio": rr_ratio, "sweep_level": sweep_level, "fvg_mid": fvg.get("fvg_mid"), "ob_low": ob.get("ob_low"), "ob_high": ob.get("ob_high"), "fib_50": fib.get("fib_50"), "in_fib_zone": fib.get("in_zone", False), "mss_confirmed": mss.get("confirmed", False), "displaced": displaced, "confluence": confluence}
    except Exception:
        return default

# ════════════════════════════════════════════════════════════════════════════════
# LEGACY STRATEGY SCORES
# ════════════════════════════════════════════════════════════════════════════════

def order_flow_score(hist: pd.DataFrame) -> float:
    try:
        if len(hist) < 15: return 0.0
        close = hist["Close"]
        tp = (hist["High"] + hist["Low"] + close) / 3
        mf = tp * hist["Volume"]
        close_diff = close.diff()
        pf = mf.where(close_diff > 0, 0.0).tail(14).sum()
        nf = mf.where(close_diff < 0, 0.0).tail(14).sum()
        mfr = pf / (nf + 1e-9)
        return round(min(mfr * 10.0, 20.0), 2)
    except Exception: return 0.0

def structure_score(hist: pd.DataFrame) -> float:
    try:
        if len(hist) < 20: return 0.0
        highs, lows = hist["High"].tail(20).values, hist["Low"].tail(20).values
        hh_count, hl_count, comparisons = 0, 0, len(highs) - 2
        if comparisons <= 0: return 0.0
        for i in range(2, len(highs)):
            if highs[i] > highs[i - 2]: hh_count += 1
            if lows[i] > lows[i - 2]: hl_count += 1
        score = (hh_count + hl_count) / (2 * comparisons) * 20.0
        return round(min(score, 20.0), 2)
    except Exception: return 0.0

def vcp_score(hist: pd.DataFrame) -> float:
    try:
        if len(hist) < 20: return 0.0
        ranges = (hist["High"] - hist["Low"]).tail(20).values
        contractions = np.sum(np.diff(ranges) < 0)
        score = 15.0 if contractions >= 10 else 8.0 if contractions >= 7 else 4.0 if contractions >= 5 else 0.0
        vols = hist["Volume"].tail(10).values
        if len(vols) > 1 and np.mean(vols[:-1]) > 0 and vols[-1] < (np.mean(vols[:-1]) * 0.7): score += 5.0
        return float(min(round(score, 2), 20.0))
    except Exception: return 0.0

def rs_vs_index_score(symbol_hist: pd.DataFrame, nifty_hist: pd.DataFrame, sector_hist: pd.DataFrame = None) -> float:
    periods = [20, 60]
    try:
        score = 0.0
        s_close = symbol_hist["Close"].values
        n_close = nifty_hist["Close"].values if (nifty_hist is not None and not nifty_hist.empty) else None
        c_close = sector_hist["Close"].values if (sector_hist is not None and not sector_hist.empty) else None
        for p in periods:
            if len(s_close) <= p: continue
            s_ret = (s_close[-1] - s_close[-(p + 1)]) / s_close[-(p + 1)]
            if n_close is not None and len(n_close) > p:
                n_ret = (n_close[-1] - n_close[-(p + 1)]) / n_close[-(p + 1)]
                diff = s_ret - n_ret
                score += 5.0 if diff > 0.05 else 3.0 if diff > 0.02 else 1.0 if diff > 0 else 0.0
            if c_close is not None and len(c_close) > p:
                c_ret = (c_close[-1] - c_close[-(p + 1)]) / c_close[-(p + 1)]
                diff_sec = s_ret - c_ret
                score += 5.0 if diff_sec > 0.05 else 3.0 if diff_sec > 0.02 else 1.0 if diff_sec > 0 else 0.0
        return float(min(round(score, 2), 20.0))
    except Exception: return 0.0

# 🔴 FIX: Modified to read DIRECTLY from the pre-calculated Weekly Parquet dataframe
def weekly_trend_check(weekly_hist: pd.DataFrame) -> dict:
    try:
        if len(weekly_hist) < 20: return {"mtf_score": 0, "mtf_status": "Neutral"}
        
        close_w = weekly_hist['Close']
        sma20_w = ta.sma(close_w, length=20)
        rsi_w   = ta.rsi(close_w, length=14)
        score = 0
        last_close = float(close_w.iloc[-1])
        last_sma   = float(sma20_w.iloc[-1]) if sma20_w is not None else 0.0
        last_rsi   = float(rsi_w.iloc[-1]) if rsi_w is not None else 0.0
        if last_close > last_sma > 0: score += 5
        if last_rsi > 50: score += 5
        status = "Bullish (W)" if score == 10 else "Neutral (W)" if score == 5 else "Bearish (W)"
        return {"mtf_score": score, "mtf_status": f"{status} | W-RSI: {last_rsi:.0f}"}
    except Exception: return {"mtf_score": 0, "mtf_status": "Error"}

def proximity_score(last_price: float, high_52w: float, low_52w: float) -> float:
    try:
        if high_52w <= 0 or last_price <= 0:
            return 0.0

        pfh = last_price / high_52w
        pfl = (last_price - low_52w) / (high_52w - low_52w + 1e-9)

        score = 0.0
        if pfh > 0.95: score += 15.0
        elif pfh > 0.90: score += 8.0
        elif pfh > 0.85: score += 4.0

        if pfl > 0.75: score += 5.0
        elif pfl > 0.60: score += 2.0

        return float(min(round(score, 2), 20.0))
    except Exception:
        return 0.0

def breakout_setup_score(hist: pd.DataFrame) -> float:
    try:
        if len(hist) < 30: return 0.0
        score = 0.0
        highs, lows, closes = hist["High"].tail(30).values, hist["Low"].tail(30).values, hist["Close"].tail(30).values
        last_close = float(closes[-1])
        if last_close <= 0: return 0.0
        recent_range = (highs[-10:].max() - lows[-10:].min()) / last_close
        if recent_range < 0.05: score += 8.0
        if last_close > highs[-5:-1].max(): score += 7.0
        return float(min(round(score, 2), 15.0))
    except Exception: return 0.0

def volume_trend_score(hist: pd.DataFrame) -> float:
    try:
        if len(hist) < 20: return 0.0
        vols = hist["Volume"].values
        avg20 = float(np.mean(vols[-20:]))
        avg60 = float(np.mean(vols[-60:])) if len(vols) >= 60 else avg20
        if avg20 <= 0: return 0.0
        last_v, score = float(vols[-1]), 0.0
        if last_v > (avg20 * 1.3): score += 8.0
        elif last_v > avg20: score += 4.0
        if avg20 > avg60: score += 7.0
        elif avg20 > (avg60 * 0.9): score += 3.0
        return float(min(round(score, 2), 15.0))
    except Exception: return 0.0

# ════════════════════════════════════════════════════════════════════════════════
# DATA FETCH  — DIRECT DAY & WEEK LOAD (NO RESAMPLING)
# ════════════════════════════════════════════════════════════════════════════════

def fetch_stock_data(symbol: str, instrument_key: str, cache: dict, nifty_hist: pd.DataFrame, sector_data_dict: dict) -> dict | None:
    try:
        # 1. YFINANCE: కేవలం ఫండమెంటల్స్ & 52W High కోసం 
        ticker_symbol = symbol + ".NS"
        ticker = yf.Ticker(ticker_symbol)
        try: info = ticker.info
        except Exception: info = {}

        pe_ratio = _safe_float(info.get("trailingPE") or info.get("forwardPE"))
        eps_val = _safe_float(info.get("trailingEps"))
        roe_val = _safe_float(info.get("returnOnEquity"))
        if roe_val > 0 and roe_val < 5: roe_val = roe_val * 100

        sector_name = info.get("sector", "Unknown")
        sector_symbol = SECTOR_MAP.get(sector_name)
        sector_hist = sector_data_dict.get(sector_symbol) if sector_symbol else pd.DataFrame()

        high_52w = _safe_float(info.get("fiftyTwoWeekHigh"))
        low_52w = _safe_float(info.get("fiftyTwoWeekLow"))

        if high_52w == 0.0 or low_52w == 0.0:
            try:
                yf_hist = ticker.history(period="1y")
                if not yf_hist.empty:
                    high_52w = float(yf_hist["High"].max())
                    low_52w = float(yf_hist["Low"].min())
            except Exception: pass

        # 2. LOCAL PARQUET: Reading Pre-calculated Day and Week Data
        safe_filename = instrument_key.replace('|', '_') + '.parquet'
        day_path = os.path.join(DAY_DATA_DIR, safe_filename)
        week_path = os.path.join(WEEK_DATA_DIR, safe_filename)

        if not os.path.exists(day_path) or not os.path.exists(week_path):
            _tally("missing_file")
            _log(f"  [SKIP:missing_file] {symbol} — Missing Day/Week parquet in folders")
            return None

        try:
            hist_day = pd.read_parquet(day_path)
            hist_week = pd.read_parquet(week_path)
        except Exception as exc:
            _tally("unreadable_parquet")
            _log(f"  [SKIP:unreadable_parquet] {symbol} — {exc}")
            return None

        if hist_day is None or hist_day.empty:
            _tally("empty_parquet")
            _log(f"  [SKIP:empty_parquet] {symbol} — 0 rows")
            return None

        # Setup Index for Day Data
        if "Timestamp" in hist_day.columns:
            hist_day["Timestamp"] = pd.to_datetime(hist_day["Timestamp"], errors="coerce")
            hist_day = hist_day.dropna(subset=["Timestamp"]).set_index("Timestamp").sort_index()
            
        # Setup Index for Week Data
        if "Timestamp" in hist_week.columns:
            hist_week["Timestamp"] = pd.to_datetime(hist_week["Timestamp"], errors="coerce")
            hist_week = hist_week.dropna(subset=["Timestamp"]).set_index("Timestamp").sort_index()

        latest_ts = hist_day.index.max()

        freshness = evaluate_freshness(latest_ts)
        if not freshness["fresh"]:
            _tally("stale_data")
            _log(f"  [SKIP:stale_data] {symbol} — last candle {freshness['as_of']} ({freshness['sessions_behind']} sessions / {freshness['age_days']} days behind)")
            return None

        required_cols = ["Open", "High", "Low", "Close", "Volume"]
        missing_cols = [col for col in required_cols if col not in hist_day.columns]
        if missing_cols:
            _tally("missing_columns")
            _log(f"  [SKIP:missing_columns] {symbol} — absent: {missing_cols}")
            return None

        if len(hist_day) < 20:
            _tally("short_history")
            _log(f"  [SKIP:short_history] {symbol} — only {len(hist_day)} daily bars (need 20)")
            return None

        last_price = _safe_float(hist_day["Close"].iloc[-1])

        # 3. ఇండికేటర్స్ మరియు స్కోర్స్ లెక్కింపు
        atr_series = ta.atr(hist_day["High"], hist_day["Low"], hist_day["Close"], length=14)
        atr_val = _ta_last(atr_series)

        rsi_val = _ta_last(ta.rsi(hist_day["Close"], length=14))

        macd_df = ta.macd(hist_day["Close"], fast=12, slow=26, signal=9)
        macd_val = _ta_last(macd_df, "MACD_")     
        macd_sig = _ta_last(macd_df, "MACDs_")    
        macd_hist = _ta_last(macd_df, "MACDh_")   

        adx_val = _ta_last(ta.adx(hist_day["High"], hist_day["Low"], hist_day["Close"], length=14), "ADX_")

        smc = smc_entry_signal(hist_day, atr_val)
        
        # 🔴 NEW: Pass Week Data Directly instead of Resampling Day Data
        mtf_data = weekly_trend_check(hist_week)

        # 4. ఫైనల్ రిజల్ట్
        result = {
            "symbol": symbol,
            "lastPrice": round(last_price, 2),
            "peRatio": round(pe_ratio, 2),
            "eps": round(eps_val, 2),
            "roe": round(roe_val, 2),
            "sector": sector_name,
            "history_data": hist_day.tail(10),
            "data_as_of": str(freshness["as_of"]),
            "data_age_days": int(freshness["age_days"]),
            "sessions_behind": int(freshness["sessions_behind"]),
            "RSI": round(rsi_val, 2),
            "MACD": round(macd_val, 4),
            "MACD_Signal": round(macd_sig, 4),
            "MACD_Hist": round(macd_hist, 4),
            "ADX": round(adx_val, 2),
            "ATR": round(atr_val, 4),
            "order_flow": order_flow_score(hist_day),
            "structure": structure_score(hist_day),
            "vcp": vcp_score(hist_day),
            "rs_vs_index": rs_vs_index_score(hist_day, nifty_hist, sector_hist),
            "mtf_score": mtf_data.get("mtf_score", 0),
            "mtf_status": mtf_data.get("mtf_status", "Neutral"),
            "proximity": proximity_score(last_price, high_52w, low_52w),
            "breakout_setup": breakout_setup_score(hist_day),
            "volume_trend": volume_trend_score(hist_day),
            **smc,
        }

        return result

    except Exception as e:
        _tally("exception")
        _log(f"  [SKIP:exception] {symbol} — {type(e).__name__}: {e}")
        return None

# ════════════════════════════════════════════════════════════════════════════════
# SCORING ENGINE
# ════════════════════════════════════════════════════════════════════════════════

def score_stock_advanced(row: dict, market_context: dict) -> dict:
    score = 0.0
    reasons = []

    def safe_val(key):
        val = row.get(key)
        if val is None: return 0.0
        try: out = float(val)
        except (TypeError, ValueError): return 0.0
        return out if np.isfinite(out) else 0.0

    pe = safe_val("peRatio")
    if pe > 0:
        if 5 < pe < 20: score += 10; reasons.append(f"PE={pe:.1f} (deep value)")
        elif 20 <= pe < 30: score += 7; reasons.append(f"PE={pe:.1f} (fair value)")
        elif 30 <= pe < 50: score += 4; reasons.append(f"PE={pe:.1f} (growth premium)")

    roe = safe_val("roe")
    if roe > 0:
        if roe > 20: score += 10; reasons.append(f"ROE={roe:.1f}% (excellent)")
        elif roe > 15: score += 7; reasons.append(f"ROE={roe:.1f}% (good)")
        elif roe > 10: score += 4; reasons.append(f"ROE={roe:.1f}% (decent)")

    rsi = safe_val("RSI")
    if rsi > 0:
        if 50 < rsi < 70: score += 10; reasons.append(f"RSI={rsi:.1f} (momentum)")
        elif 40 < rsi <= 50: score += 6; reasons.append(f"RSI={rsi:.1f} (healthy)")
        elif 30 < rsi <= 40: score += 4; reasons.append(f"RSI={rsi:.1f} (recovering)")

    macd, macd_sig = safe_val("MACD"), safe_val("MACD_Signal")
    if macd and macd_sig:
        if macd > macd_sig and macd > 0: score += 10; reasons.append("MACD bullish+positive")
        elif macd > macd_sig: score += 6; reasons.append("MACD bullish crossover")

    adx = safe_val("ADX")
    if adx > 0:
        if adx > 30: score += 5; reasons.append(f"ADX={adx:.1f} (strong trend)")
        elif adx > 25: score += 3; reasons.append(f"ADX={adx:.1f} (trend forming)")

    of, st, vcp, rs, prx, bs, vt = safe_val("order_flow"), safe_val("structure"), safe_val("vcp"), safe_val("rs_vs_index"), safe_val("proximity"), safe_val("breakout_setup"), safe_val("volume_trend")
    score += (of + st + vcp + rs + prx + bs + vt)

    if of > 10: reasons.append(f"OFI={of:.0f} (Inst. buying)")
    if st > 12: reasons.append(f"Struct={st:.0f} (Uptrend)")
    if vcp > 10: reasons.append(f"VCP={vcp:.0f} (Breakout ready)")
    if rs > 10: reasons.append(f"RS={rs:.0f} (Beating NIFTY)")
    if prx > 10: reasons.append(f"Prox={prx:.0f} (Near 52wHi)")
    if bs > 8: reasons.append(f"Breakout={bs:.0f} (Setup)")
    if vt > 6: reasons.append(f"VolTrend={vt:.0f} (Accumulation)")

    smc_conf = int(safe_val("confluence"))
    smc_sig = row.get("smc_signal", "NO_SETUP")
    score += smc_conf * 2

    if smc_sig == "HIGH_CONFLUENCE": reasons.append(f"SMC High Confluence ({smc_conf}/8)")
    elif smc_sig == "MODERATE_SETUP": reasons.append(f"SMC Moderate Setup ({smc_conf}/8)")
    if row.get("mss_confirmed"): reasons.append("MSS Confirmed")

    fib_val = row.get('fib_50')
    if row.get("in_fib_zone") and fib_val is not None: reasons.append(f"Discount Zone (50%={float(fib_val):.1f})")
    if row.get("displaced"): reasons.append("Displacement Vol Confirmed")

    final_score = round(score / SCORE_NORMALIZER, 1)
    if str(row.get("smc_direction") or "").strip().lower() == "bullish":
        smart_money_score = _safe_float(market_context.get("smart_money_score"))
        if bool(market_context.get("retail_trap")) or smart_money_score <= -3:
            final_score -= 20.0
            reasons.append("⚠️ AVOID: Market Retail Trap")
        elif smart_money_score >= 4:
            final_score += 10.0
            reasons.append("🚀 FNO Market Tailwind")
    row["score"] = float(max(0.0, min(final_score, 100.0)))
    row["reasons"] = " | ".join(reasons) if reasons else "No strong signals"

    return row

def print_entry_report(df: pd.DataFrame, top_n: int = 20) -> None:
    print("\n" + "=" * 100)
    print(f"  TOP {top_n} STOCKS BY COMPOSITE SCORE")
    print("=" * 100)
    for rank, (_, row) in enumerate(df.head(top_n).iterrows(), 1):
        sym = row.get('symbol', 'UNKNOWN')
        price = row.get('lastPrice', 0.0)
        score = row.get('score', 0.0)
        pe = row.get('peRatio', 0.0)
        roe = row.get('roe', 0.0)
        reasons = row.get('reasons', '')
        print(f"  {rank:2d}. {str(sym):12s}  Rs.{float(price):8.2f}  |  Score:{float(score):6.1f}  |  PE:{float(pe):6.1f}  |  ROE:{float(roe):5.1f}%")
        print(f"      {str(reasons)[:115]}\n")

    if "smc_signal" in df.columns:
        smc_df = df[df["smc_signal"].isin(["HIGH_CONFLUENCE", "MODERATE_SETUP"])].copy()
        sort_cols = [c for c in ["smc_score", "score"] if c in smc_df.columns]
        if sort_cols: smc_df = smc_df.sort_values(sort_cols, ascending=False)
    else: smc_df = pd.DataFrame()

    if smc_df.empty:
        print("  -- No SMC setups detected in current scan --\n")
        return

    print("=" * 100)
    print(f"  SMC PRECISION ENTRY REPORT  ({len(smc_df)} setups found)")
    print(f"  {'Symbol':<12} {'Signal':<18} {'Dir':<8} {'Entry':>9} {'SL':>9} {'TP':>9} {'R:R':>5} {'Conf':>5}  Notes")
    print("-" * 100)

    for _, row in smc_df.iterrows():
        sym = row.get('symbol', 'UNKNOWN')
        entry, sl, tp, rr = row.get("entry_price"), row.get("stop_loss"), row.get("take_profit"), row.get("rr_ratio")
        conf, sig, dirn = row.get("confluence", 0), row.get("smc_signal", ""), row.get("smc_direction", "")
        notes = []
        if row.get("mss_confirmed"):  notes.append("MSS✓")
        if row.get("in_fib_zone"):    notes.append("FibDisc✓")
        if row.get("displaced"):      notes.append("Disp✓")
        fvg_mid, ob_low, ob_high = row.get("fvg_mid"), row.get("ob_low"), row.get("ob_high")
        if fvg_mid and pd.notna(fvg_mid): notes.append(f"FVG@{fvg_mid:.2f}")
        if ob_low and ob_high and pd.notna(ob_low) and pd.notna(ob_high): notes.append(f"OB[{ob_low:.2f}-{ob_high:.2f}]")

        entry_str = f"{entry:.2f}" if entry and pd.notna(entry) else 'N/A'
        sl_str = f"{sl:.2f}" if sl and pd.notna(sl) else 'N/A'
        tp_str = f"{tp:.2f}" if tp and pd.notna(tp) else 'N/A'
        rr_str = f"{rr:.2f}" if rr and pd.notna(rr) else 'N/A'
        print(f"  {str(sym):<12} {str(sig):<18} {str(dirn).upper():<8} {entry_str:>9} {sl_str:>9} {tp_str:>9} {rr_str:>5}  {int(conf) if pd.notna(conf) else 0:>2}  {' | '.join(notes)}")

def preflight_corpus_check() -> dict:
    info = {"dir_exists": False, "file_count": 0, "newest": None, "fresh": False, "sessions_behind": None, "age_days": None}
    if not os.path.isdir(DAY_DATA_DIR):
        print(f"  !! DAY DATA DIRECTORY NOT FOUND: {DAY_DATA_DIR}")
        return info

    info["dir_exists"] = True
    try: files = [f for f in os.listdir(DAY_DATA_DIR) if f.endswith(".parquet")]
    except Exception as exc: return info

    info["file_count"] = len(files)
    if not files: return info

    newest = None
    for name in files[:10]:  
        try:
            sample = pd.read_parquet(os.path.join(DAY_DATA_DIR, name), columns=["Timestamp"])
            if sample.empty: continue
            ts = pd.to_datetime(sample["Timestamp"], errors="coerce").max()
            if pd.notna(ts) and (newest is None or ts > newest): newest = ts
        except Exception: continue

    verdict = evaluate_freshness(newest)
    info.update({"newest": newest, "fresh": verdict["fresh"], "sessions_behind": verdict["sessions_behind"], "age_days": verdict["age_days"]})
    return info

# 🔴 NEW: Fetching Symbol Mapping so the dashboard keeps NSE names but reads Upstox keys
def get_symbol_mapping():
    print("  Mapping NIFTY 500 Symbols to Upstox Instrument Keys...")
    try:
        url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
        response = requests.get(url)
        df_upstox = pd.DataFrame(json.loads(gzip.decompress(response.content).decode('utf-8')))
        df_eq = df_upstox[df_upstox['segment'] == 'NSE_EQ']
        
        df_csv = pd.read_csv(NIFTY_CSV_FILE)
        nifty_symbols = df_csv['Symbol'].dropna().tolist()
        
        df_filtered = df_eq[df_eq['trading_symbol'].isin(nifty_symbols)]
        # Return a list of tuples: (Symbol, Instrument_Key)
        return list(zip(df_filtered['trading_symbol'], df_filtered['instrument_key']))
    except Exception as e:
        print(f"  !! Failed to map symbols: {e}")
        return []

def main():
    mapped_symbols = get_symbol_mapping()
    if not mapped_symbols:
        print("  !! No symbols found or mapped. Exiting.")
        return 1

    market_context = get_fno_market_context()
    cache = load_cache()
    cache.clear() 
    final_data = []
    ltp_max = 5000.0 # Increased filter threshold slightly for 500 stocks

    print("\n" + "=" * 100)
    print("  *  NIFTY 500 - SMC PRECISION ENTRY ENGINE (HYBRID DATA VER)")
    print(f"     Scanning {len(mapped_symbols)} stocks using pre-calculated Parquet Data")
    print("=" * 100)

    preflight = preflight_corpus_check()
    print()

    print("  Fetching NIFTY 50 for relative strength...")
    try:
        nifty_hist = _flatten_columns(yf.Ticker("^NSEI").history(period="1y"))
        if nifty_hist.empty: nifty_hist = pd.DataFrame()
    except Exception: nifty_hist = pd.DataFrame()

    print("  Fetching Sectoral Indices for Advanced RS...")
    sector_data_dict = {}
    for sec_sym in set(SECTOR_MAP.values()):
        try: sector_data_dict[sec_sym] = _flatten_columns(yf.Ticker(sec_sym).history(period="1y"))
        except Exception: sector_data_dict[sec_sym] = pd.DataFrame()
    print("  Sectoral data loaded successfully.\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Pass both Symbol and Instrument Key
        futures = {executor.submit(fetch_stock_data, sym, ikey, cache, nifty_hist, sector_data_dict): sym for sym, ikey in mapped_symbols}
        for i, future in enumerate(as_completed(futures), 1):
            sym = futures[future]
            result = future.result()
            if result is None: continue

            result = score_stock_advanced(result, market_context)
            clean_result = {k: v for k, v in result.items() if not isinstance(v, (pd.DataFrame, pd.Series))}
            clean_result["symbol"] = sym

            ltp = float(clean_result.get("lastPrice", 0) or 0)
            if ltp <= 0 or ltp >= ltp_max:
                _tally("ltp_filtered")
                continue

            final_data.append(clean_result)
            cache[sym] = {"ts": datetime.now().isoformat(), "data": clean_result}
            _tally("scored")

            smc_tag = clean_result.get("smc_signal", "NO_SETUP") or "NO_SETUP"
            smc_dir = (clean_result.get("smc_direction") or "")[:4].upper()
            score = clean_result.get('score', 0)
            last_price = clean_result.get('lastPrice', 0)

            _log(f"  [{i:3d}/{len(mapped_symbols)}] {sym:15s} | Score:{score:6.1f}/100 | Rs.{last_price:8.2f} | SMC:{smc_tag[:14]:<14} {smc_dir}")

    save_cache(cache)

    tally = _tally_snapshot()
    scanned = len(mapped_symbols)
    scored = tally.get("scored", 0)
    skipped = scanned - scored

    print("\n" + "=" * 100 + f"\n  SCAN SUMMARY \n" + "=" * 100)
    print(f"  Symbols scanned : {scanned}\n  Passed all gates: {scored}\n  Skipped         : {skipped}")

    if skipped:
        print("\n  Skip breakdown (reason -> count):")
        for reason, label in SKIP_REASONS.items():
            if tally.get(reason, 0): print(f"    {reason:<20} {tally.get(reason):4d}   ({label})")

    df = pd.DataFrame(final_data)
    default_columns = {
        'symbol': 'UNKNOWN', 'lastPrice': 0.0, 'score': 0.0, 'peRatio': 0.0, 'eps': 0.0,
        'roe': 0.0, 'RSI': 0.0, 'MACD': 0.0, 'MACD_Signal': 0.0, 'ADX': 0.0, 'ATR': 0.0,
        'order_flow': 0.0, 'structure': 0.0, 'vcp': 0.0, 'rs_vs_index': 0.0,
        'proximity': 0.0, 'breakout_setup': 0.0, 'volume_trend': 0.0,
        'mtf_score': 0.0, 'mtf_status': 'Neutral', 'smc_signal': 'NO_SETUP',
        'smc_direction': None, 'confluence': 0, 'entry_price': None, 'stop_loss': None,
        'take_profit': None, 'rr_ratio': None, 'reasons': '',
        'data_as_of': None, 'data_age_days': 0, 'sessions_behind': 0,
    }
    for col, default_val in default_columns.items():
        if col not in df.columns: df[col] = default_val

    if not df.empty: df = df.sort_values("score", ascending=False)

    try: df.to_csv(OUTPUT_CSV, index=False)
    except PermissionError:
        backup_file = os.path.join(BASE_DIR, "nifty200_smc_backup_screen.csv")
        df.to_csv(backup_file, index=False)

    categories = filter_combo_setups(df) if not df.empty else {"PERFECT_COMBO": [], "STRONG_SEPA": [], "SMC_ACTIVE": []}
    save_combo_context(categories)
    
    data_as_of = str(df["data_as_of"].dropna().max()) if not df.empty and df["data_as_of"].notna().any() else None
    save_run_status({
        "run_at": datetime.now().isoformat(timespec="seconds"), "history_dir": DAY_DATA_DIR,
        "max_data_age_days": MAX_DATA_AGE_DAYS, "ltp_max": ltp_max, "symbols_scanned": scanned,
        "stocks_passed": scored, "stocks_skipped": skipped, "status": "OK" if scored else "NO_RESULTS",
    })

    if df.empty: return 1

    try: print_entry_report(df, top_n=20)
    except NameError: pass 

    return 0

if __name__ == "__main__":
    sys.exit(main() or 0)