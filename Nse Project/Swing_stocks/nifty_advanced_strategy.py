"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║   NIFTY 200 — SMC PRECISION ENTRY ENGINE  v5.1                                 ║
║   Smart Money Concepts + Minervini SEPA + Institutional Flow                   ║
║                                                                                  ║
║   UPDATES IN v5.1:                                                              ║
║     • FIXED: Cache race condition (save_cache moved to main thread)             ║
║     • ADDED: Strict chronological sequence validation (Sweep -> MSS -> OB)      ║
║                                                                                  ║
║   SMC Modules:                                                                   ║
║     • Liquidity Sweep  — price sweeps swing H/L then reverses                  ║
║     • Fair Value Gap   — 3-candle imbalance; entry at 50% fill                 ║
║     • Market Structure Shift — body-close above/below swing pivot               ║
║     • Order Block      — last bearish/bullish OB before displacement            ║
║     • Premium/Discount — Fibonacci 0.5 equilibrium zone filter                 ║
║     • Displacement     — high-momentum candle confirming institutional intent   ║
╚══════════════════════════════════════════════════════════════════════════════════╝
"""

import pandas as pd
import numpy as np
import yfinance as yf
import pandas_ta as ta
import time
import json
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

warnings.filterwarnings("ignore")

# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════

CACHE_FILE         = "nifty_yf_cache.json"
CACHE_EXPIRY_HOURS = 6
MAX_WORKERS        = 5
SWING_LOOKBACK     = 10   # bars each side to define a swing high/low pivot
ATR_SL_MULTIPLIER  = 0.5  # extra ATR buffer below OB/sweep low for SL
MIN_RR             = 2.0  # minimum reward-to-risk ratio to flag a trade


# ════════════════════════════════════════════════════════════════════════════════
# CACHE HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def load_cache() -> dict:
    """Load JSON cache from disk; return empty dict on any error."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_cache(cache: dict) -> None:
    """Persist cache to disk."""
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        print(f"Cache save error: {e}")


def is_cache_fresh(cache: dict, key: str) -> bool:
    """Return True if the cached entry exists and is younger than CACHE_EXPIRY_HOURS."""
    if key not in cache:
        return False
    try:
        cached_time = datetime.fromisoformat(cache[key]["ts"])
        return datetime.now() - cached_time < timedelta(hours=CACHE_EXPIRY_HOURS)
    except Exception:
        return False


# ════════════════════════════════════════════════════════════════════════════════
# UTILITY — safe scalar extractor
# ════════════════════════════════════════════════════════════════════════════════

def _safe(val) -> float | None:
    """Convert a pandas scalar / numpy scalar to a plain Python float, or None."""
    try:
        if val is None:
            return None
        v = float(val)
        return round(v, 4) if not np.isnan(v) else None
    except Exception:
        return None


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten yfinance MultiIndex columns to single-level strings."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
    return df


# ════════════════════════════════════════════════════════════════════════════════
# SMC MODULE 1 — SWING PIVOT DETECTION
# ════════════════════════════════════════════════════════════════════════════════

# def find_swing_pivots(high: np.ndarray, low: np.ndarray, n: int = SWING_LOOKBACK):
#     sh_idx, sl_idx = [], []
#     length = len(high)
#     for i in range(n, length - n - 1):
#         window_h = high[i - n: i + n + 1]
#         window_l = low[i - n: i + n + 1]
#         if high[i] == window_h.max():
#             sh_idx.append(i)
#         if low[i] == window_l.min():
#             sl_idx.append(i)
#     return sh_idx, sl_idx

def find_swing_pivots(high: np.ndarray, low: np.ndarray, n: int = 2):
    sh_idx, sl_idx = [], []
    length = len(high)

    # ఎర్రర్ సరిచేయడానికి length - n అని మాత్రమే పెట్టాలి
    for i in range(n, length - n):
        window_h = high[i - n: i + n + 1]
        window_l = low[i - n: i + n + 1]

        # high[i] ఖచ్చితంగా విండో మధ్యలోనే ఉండాలి (np.argmax)
        if high[i] == window_h.max() and np.argmax(window_h) == n:
            sh_idx.append(i)

        # low[i] ఖచ్చితంగా విండో మధ్యలోనే ఉండాలి (np.argmin)
        if low[i] == window_l.min() and np.argmin(window_l) == n:
            sl_idx.append(i)

    return sh_idx, sl_idx

# ════════════════════════════════════════════════════════════════════════════════
# SMC MODULE 2 — LIQUIDITY SWEEP DETECTION
# ════════════════════════════════════════════════════════════════════════════════

def detect_liquidity_sweep(high: np.ndarray, low: np.ndarray,
                           close: np.ndarray, open_: np.ndarray,
                           sh_idx: list, sl_idx: list,
                           lookback: int = 5):
    result = {"swept": False, "direction": None, "sweep_level": None,
              "sweep_bar": None, "sweep_low": None, "sweep_high": None}

    n = len(close)
    check_range = range(max(n - lookback - 1, 1), n - 1)

    if sl_idx:
        recent_sl_level = low[sl_idx[-1]]
        for i in check_range:
            if low[i] < recent_sl_level and close[i] > recent_sl_level:
                result.update({
                    "swept":       True,
                    "direction":   "bullish",
                    "sweep_level": round(float(recent_sl_level), 4),
                    "sweep_bar":   i,
                    "sweep_low":   round(float(low[i]), 4),
                    "sweep_high":  round(float(high[i]), 4),
                })
                return result

    if sh_idx:
        recent_sh_level = high[sh_idx[-1]]
        for i in check_range:
            if high[i] > recent_sh_level and close[i] < recent_sh_level:
                result.update({
                    "swept":       True,
                    "direction":   "bearish",
                    "sweep_level": round(float(recent_sh_level), 4),
                    "sweep_bar":   i,
                    "sweep_low":   round(float(low[i]), 4),
                    "sweep_high":  round(float(high[i]), 4),
                })
                return result

    return result


# ════════════════════════════════════════════════════════════════════════════════
# SMC MODULE 3 — FAIR VALUE GAP (FVG)
# ════════════════════════════════════════════════════════════════════════════════

def detect_fvg(high: np.ndarray, low: np.ndarray, close: np.ndarray,
               direction: str | None, lookback: int = 20):
    result = {"found": False, "fvg_high": None, "fvg_low": None,
              "fvg_mid": None, "retesting": False, "direction": direction}

    if direction is None:
        return result

    n = len(close)
    current_close = close[n - 1]

    for i in range(n - 2, max(n - lookback - 2, 2), -1):
        if direction == "bullish":
            if high[i - 2] < low[i]:
                gap_low  = float(high[i - 2])
                gap_high = float(low[i])
                gap_mid  = round((gap_low + gap_high) / 2, 4)
                retesting = gap_low <= current_close <= gap_high
                result.update({
                    "found":     True,
                    "fvg_high":  round(gap_high, 4),
                    "fvg_low":   round(gap_low, 4),
                    "fvg_mid":   gap_mid,
                    "retesting": bool(retesting),
                })
                return result

        elif direction == "bearish":
            if low[i - 2] > high[i]:
                gap_high = float(low[i - 2])
                gap_low  = float(high[i])
                gap_mid  = round((gap_low + gap_high) / 2, 4)
                retesting = gap_low <= current_close <= gap_high
                result.update({
                    "found":     True,
                    "fvg_high":  round(gap_high, 4),
                    "fvg_low":   round(gap_low, 4),
                    "fvg_mid":   gap_mid,
                    "retesting": bool(retesting),
                })
                return result

    return result


# ════════════════════════════════════════════════════════════════════════════════
# SMC MODULE 4 — MARKET STRUCTURE SHIFT (MSS)
# ════════════════════════════════════════════════════════════════════════════════

def detect_mss(high: np.ndarray, low: np.ndarray,
               close: np.ndarray, open_: np.ndarray,
               sh_idx: list, sl_idx: list,
               direction: str | None):
    result = {"confirmed": False, "mss_level": None, "mss_bar": None}

    if direction is None:
        return result

    n = len(close)
    NOISE_FILTER = 0.0005

    if direction == "bullish" and sh_idx:
        pivot_level = float(high[sh_idx[-1]])
        for i in range(n - 2, max(n - 12, 0), -1):
            body_close = float(close[i])
            if body_close > pivot_level * (1 + NOISE_FILTER):
                result.update({
                    "confirmed": True,
                    "mss_level": round(pivot_level, 4),
                    "mss_bar":   i,
                })
                return result

    elif direction == "bearish" and sl_idx:
        pivot_level = float(low[sl_idx[-1]])
        for i in range(n - 2, max(n - 12, 0), -1):
            body_close = float(close[i])
            if body_close < pivot_level * (1 - NOISE_FILTER):
                result.update({
                    "confirmed": True,
                    "mss_level": round(pivot_level, 4),
                    "mss_bar":   i,
                })
                return result

    return result


# ════════════════════════════════════════════════════════════════════════════════
# SMC MODULE 5 — ORDER BLOCK DETECTION
# ════════════════════════════════════════════════════════════════════════════════

def detect_order_block(high: np.ndarray, low: np.ndarray,
                       close: np.ndarray, open_: np.ndarray,
                       volume: np.ndarray, direction: str | None,
                       sweep_bar: int | None, lookback: int = 15):
    result = {"found": False, "ob_high": None, "ob_low": None,
              "ob_mid": None, "ob_bar": None}

    if direction is None:
        return result

    n = len(close)
    start = sweep_bar if sweep_bar is not None else n - 2
    start = min(start, n - 2)

    bodies = np.abs(close - open_)
    avg_body = bodies[max(0, start - 10): start].mean() if start > 0 else 1.0
    if avg_body == 0:
        avg_body = 1.0

    if direction == "bullish":
        for i in range(start, max(start - lookback, 1), -1):
            is_bearish_ob = close[i] < open_[i]
            next_body     = close[i + 1] - open_[i + 1]
            is_displacement = (next_body > avg_body * 1.5) and (close[i + 1] > open_[i + 1])
            if is_bearish_ob and is_displacement:
                ob_low  = float(low[i])
                ob_high = float(high[i])
                result.update({
                    "found":   True,
                    "ob_high": round(ob_high, 4),
                    "ob_low":  round(ob_low, 4),
                    "ob_mid":  round((ob_low + ob_high) / 2, 4),
                    "ob_bar":  i,
                })
                return result

    elif direction == "bearish":
        for i in range(start, max(start - lookback, 1), -1):
            is_bullish_ob = close[i] > open_[i]
            next_body     = open_[i + 1] - close[i + 1]
            is_displacement = (next_body > avg_body * 1.5) and (close[i + 1] < open_[i + 1])
            if is_bullish_ob and is_displacement:
                ob_low  = float(low[i])
                ob_high = float(high[i])
                result.update({
                    "found":   True,
                    "ob_high": round(ob_high, 4),
                    "ob_low":  round(ob_low, 4),
                    "ob_mid":  round((ob_low + ob_high) / 2, 4),
                    "ob_bar":  i,
                })
                return result

    return result


# ════════════════════════════════════════════════════════════════════════════════
# SMC MODULE 6 — PREMIUM / DISCOUNT FIBONACCI ZONE
# ════════════════════════════════════════════════════════════════════════════════

def premium_discount_zone(high: np.ndarray, low: np.ndarray,
                          close: np.ndarray, direction: str | None,
                          lookback: int = 50):
    result = {"in_zone": False, "zone_type": None, "fib_50": None,
              "fib_618": None, "fib_786": None,
              "range_high": None, "range_low": None}

    if direction is None or len(high) < lookback:
        return result

    n = len(close)
    window_high = float(high[n - lookback: n - 1].max())
    window_low  = float(low[n - lookback: n - 1].min())
    rng         = window_high - window_low
    if rng <= 0:
        return result

    fib_50  = window_low + rng * 0.500
    fib_618 = window_low + rng * 0.382
    fib_786 = window_low + rng * 0.214

    current = float(close[n - 1])

    result.update({
        "fib_50":      round(fib_50, 4),
        "fib_618":     round(fib_618, 4),
        "fib_786":     round(fib_786, 4),
        "range_high":  round(window_high, 4),
        "range_low":   round(window_low, 4),
    })

    if direction == "bullish":
        in_discount = current <= fib_50
        result.update({"in_zone": bool(in_discount), "zone_type": "discount" if in_discount else "premium"})
    elif direction == "bearish":
        in_premium = current >= fib_50
        result.update({"in_zone": bool(in_premium), "zone_type": "premium" if in_premium else "discount"})

    return result


# ════════════════════════════════════════════════════════════════════════════════
# SMC MODULE 7 — DISPLACEMENT VOLUME CONFIRMATION
# ════════════════════════════════════════════════════════════════════════════════

def displacement_confirmed(high: np.ndarray, low: np.ndarray,
                           close: np.ndarray, open_: np.ndarray,
                           volume: np.ndarray, direction: str | None,
                           lookback: int = 5) -> bool:
    if direction is None:
        return False

    n = len(close)
    bodies   = np.abs(close - open_)
    avg_body = bodies[max(0, n - 21): n - 1].mean()
    avg_vol  = volume[max(0, n - 21): n - 1].mean()

    if avg_body == 0 or avg_vol == 0:
        return False

    for i in range(n - 2, max(n - lookback - 2, 0), -1):
        body = float(bodies[i])
        vol  = float(volume[i])
        is_bullish_candle = close[i] > open_[i]
        is_bearish_candle = close[i] < open_[i]

        if (direction == "bullish" and is_bullish_candle
                and body >= avg_body * 1.5 and vol >= avg_vol * 1.5):
            return True
        if (direction == "bearish" and is_bearish_candle
                and body >= avg_body * 1.5 and vol >= avg_vol * 1.5):
            return True

    return False


# ════════════════════════════════════════════════════════════════════════════════
# SMC MASTER FUNCTION — combines all modules into one entry signal
# ════════════════════════════════════════════════════════════════════════════════

def smc_entry_signal(hist: pd.DataFrame, atr_value: float | None):
    """
    Run the full SMC pipeline on a completed OHLCV DataFrame and return
    a structured entry report.
    """
    default = {
        "smc_signal":    "NO_SETUP",
        "smc_direction": None,
        "smc_score":     0,
        "entry_price":   None,
        "stop_loss":     None,
        "take_profit":   None,
        "rr_ratio":      None,
        "sweep_level":   None,
        "fvg_high":      None,
        "fvg_low":       None,
        "fvg_mid":       None,
        "ob_high":       None,
        "ob_low":        None,
        "fib_50":        None,
        "in_fib_zone":   False,
        "mss_confirmed": False,
        "displaced":     False,
        "confluence":    0,
    }

    try:
        # ── Extract numpy arrays ────────────────────────────────────────────────
        h_arr  = hist["High"].values.astype(float)
        l_arr  = hist["Low"].values.astype(float)
        c_arr  = hist["Close"].values.astype(float)
        o_arr  = hist["Open"].values.astype(float)
        v_arr  = hist["Volume"].values.astype(float)

        # ── Step 1: Swing pivots ─────────────────────────────────────────────────
        sh_idx, sl_idx = find_swing_pivots(h_arr, l_arr, n=SWING_LOOKBACK)
        if not sh_idx or not sl_idx:
            return default

        # ── Step 2: Liquidity sweep ──────────────────────────────────────────────
        sweep = detect_liquidity_sweep(h_arr, l_arr, c_arr, o_arr, sh_idx, sl_idx)
        if not sweep["swept"]:
            return default

        direction  = sweep["direction"]
        sweep_bar  = sweep["sweep_bar"]
        sweep_low  = sweep["sweep_low"]
        sweep_high = sweep["sweep_high"]

        # ── Step 3: Fair Value Gap ───────────────────────────────────────────────
        fvg = detect_fvg(h_arr, l_arr, c_arr, direction)

        # ── Step 4: Market Structure Shift ───────────────────────────────────────
        mss = detect_mss(h_arr, l_arr, c_arr, o_arr, sh_idx, sl_idx, direction)

        # ── Step 5: Order Block ──────────────────────────────────────────────────
        ob = detect_order_block(h_arr, l_arr, c_arr, o_arr, v_arr, direction, sweep_bar)

        # ═════════════════════════════════════════════════════════════════════════
        # STRICT SMC CHRONOLOGICAL SEQUENCE VALIDATION
        # ═════════════════════════════════════════════════════════════════════════
        # Enforce: Sweep -> MSS -> OB (Sequence must be logically ordered)

        if mss["confirmed"]:
            # Check 1: MSS must happen AFTER or AT the Liquidity Sweep bar
            if mss["mss_bar"] is not None and sweep_bar is not None:
                if mss["mss_bar"] < sweep_bar:
                    return default  # MSS happened before sweep, invalid sequence

            # Check 2: OB must be formed BEFORE or AT the MSS bar
            if ob["found"] and ob["ob_bar"] is not None:
                if ob["ob_bar"] > mss["mss_bar"]:
                    return default  # OB formed after MSS, invalid sequence
        # ═════════════════════════════════════════════════════════════════════════

        # ── Step 6: Premium / Discount zone ─────────────────────────────────────
        fib = premium_discount_zone(h_arr, l_arr, c_arr, direction)

        # ── Step 7: Displacement volume ──────────────────────────────────────────
        displaced = displacement_confirmed(h_arr, l_arr, c_arr, o_arr, v_arr, direction)

        # ── Step 8: Confluence scoring ───────────────────────────────────────────
        confluence = 0
        if sweep["swept"]:       confluence += 1   # mandatory
        if fvg["found"]:         confluence += 1
        if fvg["retesting"]:     confluence += 1
        if mss["confirmed"]:     confluence += 1
        if ob["found"]:          confluence += 1
        if fib["in_zone"]:       confluence += 1
        if displaced:            confluence += 1

        if confluence < 3:
            signal = "WEAK_SETUP"
        elif confluence < 5:
            signal = "MODERATE_SETUP"
        else:
            signal = "HIGH_CONFLUENCE"

        # ── Step 9: Entry / SL / TP calculation ──────────────────────────────────
        atr_buf = float(atr_value) * ATR_SL_MULTIPLIER if atr_value else 0.0

        entry_price = None
        stop_loss   = None
        take_profit = None
        rr_ratio    = None

        if direction == "bullish":
            candidates = [x for x in [fvg.get("fvg_mid"), ob.get("ob_mid")] if x is not None]
            entry_price = max(candidates) if candidates else float(c_arr[-1])
            stop_loss = round(sweep_low - atr_buf, 4)

            risk = entry_price - stop_loss
            if risk <= 0:
                risk = atr_buf if atr_buf > 0 else entry_price * 0.01

            tp_candidates = [float(h_arr[i]) for i in sh_idx if float(h_arr[i]) > entry_price]
            if tp_candidates:
                take_profit = round(min(tp_candidates), 4)
            else:
                take_profit = round(entry_price + risk * MIN_RR, 4)

            rr_ratio = round((take_profit - entry_price) / risk, 2) if risk > 0 else None

        elif direction == "bearish":
            candidates = [x for x in [fvg.get("fvg_mid"), ob.get("ob_mid")] if x is not None]
            entry_price = min(candidates) if candidates else float(c_arr[-1])
            stop_loss = round(sweep_high + atr_buf, 4)

            risk = stop_loss - entry_price
            if risk <= 0:
                risk = atr_buf if atr_buf > 0 else entry_price * 0.01

            tp_candidates = [float(l_arr[i]) for i in sl_idx if float(l_arr[i]) < entry_price]
            if tp_candidates:
                take_profit = round(max(tp_candidates), 4)
            else:
                take_profit = round(entry_price - risk * MIN_RR, 4)

            rr_ratio = round((entry_price - take_profit) / risk, 2) if risk > 0 else None

        if rr_ratio is not None and rr_ratio < MIN_RR:
            signal = "LOW_RR"

        return {
            "smc_signal":    signal,
            "smc_direction": direction,
            "smc_score":     confluence,
            "entry_price":   round(entry_price, 4) if entry_price else None,
            "stop_loss":     stop_loss,
            "take_profit":   take_profit,
            "rr_ratio":      rr_ratio,
            "sweep_level":   sweep["sweep_level"],
            "fvg_high":      fvg.get("fvg_high"),
            "fvg_low":       fvg.get("fvg_low"),
            "fvg_mid":       fvg.get("fvg_mid"),
            "ob_high":       ob.get("ob_high"),
            "ob_low":        ob.get("ob_low"),
            "fib_50":        fib.get("fib_50"),
            "in_fib_zone":   fib.get("in_zone", False),
            "mss_confirmed": mss.get("confirmed", False),
            "displaced":     displaced,
            "confluence":    confluence,
        }

    except Exception:
        return default


# ════════════════════════════════════════════════════════════════════════════════
# LEGACY STRATEGY SCORES
# ════════════════════════════════════════════════════════════════════════════════

def order_flow_score(hist: pd.DataFrame) -> float:
    try:
        h = hist.copy()
        h["tp"] = (h["High"] + h["Low"] + h["Close"]) / 3
        h["mf"] = h["tp"] * h["Volume"]
        h["pf"] = h["mf"].where(h["Close"] > h["Close"].shift(1), 0)
        h["nf"] = h["mf"].where(h["Close"] < h["Close"].shift(1), 0)
        pf  = h["pf"].tail(14).sum()
        nf  = h["nf"].tail(14).sum()
        mfr = pf / (nf + 1e-9)
        return round(min(mfr * 10, 20), 2)
    except Exception:
        return 0.0

def structure_score(hist: pd.DataFrame) -> float:
    try:
        highs = hist["High"].tail(20).values
        lows  = hist["Low"].tail(20).values
        if len(highs) < 2: return 0.0
        hh = sum(highs[i] > highs[i - 1] for i in range(1, len(highs)))
        hl = sum(lows[i]  > lows[i - 1]  for i in range(1, len(lows)))
        return round((hh + hl) / (2 * (len(highs) - 1)) * 20, 2)
    except Exception:
        return 0.0

def vcp_score(hist: pd.DataFrame) -> float:
    try:
        ranges = (hist["High"] - hist["Low"]).tail(20).values
        if len(ranges) < 2: return 0.0
        contractions = sum(ranges[i] < ranges[i - 1] for i in range(1, len(ranges)))
        score = 15 if contractions >= 12 else 8 if contractions >= 8 else 4 if contractions >= 5 else 0
        vols  = hist["Volume"].tail(10).values
        if len(vols) > 1 and vols[-1] < vols[:-1].mean() * 0.7: score += 5
        return min(round(score, 2), 20)
    except Exception:
        return 0.0

def rs_vs_index_score(symbol_hist: pd.DataFrame, index_hist: pd.DataFrame, periods: list = None) -> float:
    if periods is None: periods = [20, 60]
    try:
        score = 0
        for p in periods:
            if len(symbol_hist) < p or len(index_hist) < p: continue
            s_ret = symbol_hist["Close"].pct_change(p).iloc[-1]
            n_ret = index_hist["Close"].pct_change(p).iloc[-1]
            diff  = s_ret - n_ret
            score += 10 if diff > 0.05 else 6 if diff > 0.02 else 3 if diff > 0 else 0
        return min(round(score, 2), 20)
    except Exception:
        return 0.0

def proximity_score(hist: pd.DataFrame) -> float:
    try:
        if len(hist) < 252: return 0.0
        high_52w = hist["Close"].tail(252).max()
        low_52w  = hist["Close"].tail(252).min()
        last     = hist["Close"].iloc[-1]
        pfh   = last / high_52w if high_52w > 0 else 0
        pfl   = (last - low_52w) / (high_52w - low_52w + 1e-9)
        score = 15 if pfh > 0.95 else 8 if pfh > 0.90 else 4 if pfh > 0.85 else 0
        score += 5 if pfl > 0.75 else 2 if pfl > 0.60 else 0
        return min(round(score, 2), 20)
    except Exception:
        return 0.0

def breakout_setup_score(hist: pd.DataFrame) -> float:
    try:
        if len(hist) < 30: return 0.0
        closes = hist["Close"].tail(30).values
        score  = 0
        recent_range = (closes[-10:].max() - closes[-10:].min()) / closes[-1]
        if recent_range < 0.05: score += 8
        if closes[-1] > closes[-5:-1].max(): score += 7
        return min(round(score, 2), 15)
    except Exception:
        return 0.0

def volume_trend_score(hist: pd.DataFrame) -> float:
    try:
        if len(hist) < 20: return 0.0
        avg20  = hist["Volume"].tail(20).mean()
        avg60  = hist["Volume"].tail(60).mean() if len(hist) >= 60 else avg20
        last_v = hist["Volume"].iloc[-1]
        score  = 8 if last_v > avg20 * 1.3 else 4 if last_v > avg20 else 0
        score += 7 if avg20 > avg60 else 3 if avg20 > avg60 * 0.9 else 0
        return min(round(score, 2), 15)
    except Exception:
        return 0.0


# ════════════════════════════════════════════════════════════════════════════════
# DATA FETCH  — yfinance with multi-index fix, retry, and cache
# ════════════════════════════════════════════════════════════════════════════════

def fetch_stock_data(symbol: str, cache: dict, nifty_hist: pd.DataFrame) -> dict | None:
    """
    Download 1-year daily OHLCV + fundamentals for `symbol`.
    NOTE: Cache saving is handled by the main thread to prevent race conditions.
    """
    # ── Cache hit: refresh live scores only ─────────────────────────────────────
    if is_cache_fresh(cache, symbol):
        cached = cache[symbol]["data"]
        try:
            hist = _flatten_columns(yf.Ticker(symbol + ".NS").history(period="1y"))
            if not hist.empty and len(hist) >= 50:
                atr_series = ta.atr(hist["High"], hist["Low"], hist["Close"], length=14)
                atr_val    = _safe(atr_series.iloc[-1]) if atr_series is not None else None
                smc        = smc_entry_signal(hist, atr_val)
                cached.update({
                    "order_flow":     order_flow_score(hist),
                    "structure":      structure_score(hist),
                    "vcp":            vcp_score(hist),
                    "rs_vs_index":    rs_vs_index_score(hist, nifty_hist),
                    "proximity":      proximity_score(hist),
                    "breakout_setup": breakout_setup_score(hist),
                    "volume_trend":   volume_trend_score(hist),
                    **smc,
                })
        except Exception:
            pass
        return cached

    # ── Full fetch with retry ────────────────────────────────────────────────────
    for attempt in range(3):
        try:
            ticker = yf.Ticker(symbol + ".NS")
            info   = ticker.info
            hist   = _flatten_columns(ticker.history(period="1y"))

            if hist.empty or len(hist) < 50:
                return None

            last_price = (info.get("regularMarketPrice")
                          or info.get("currentPrice")
                          or float(hist["Close"].iloc[-1]))

            close  = hist["Close"]
            high   = hist["High"]
            low    = hist["Low"]
            volume = hist["Volume"]

            rsi      = ta.rsi(close, length=14)
            macd_df  = ta.macd(close)
            sma50    = ta.sma(close, length=50)
            sma200   = ta.sma(close, length=200)
            adx_df   = ta.adx(high, low, close, length=14)
            bb       = ta.bbands(close, length=20, std=2)
            atr      = ta.atr(high, low, close, length=14)

            last_sma200 = _safe(sma200.iloc[-1])
            avg_vol_20  = float(volume.tail(20).mean())
            atr_val     = _safe(atr.iloc[-1]) if atr is not None else None

            pct_above_sma200 = (
                ((last_price - last_sma200) / last_sma200) * 100
                if last_sma200 and last_sma200 > 0 else None
            )

            bb_width = None
            if bb is not None:
                try:
                    bbu_col = [c for c in bb.columns if c.startswith("BBU")][0]
                    bbl_col = [c for c in bb.columns if c.startswith("BBL")][0]
                    bbm_col = [c for c in bb.columns if c.startswith("BBM")][0]
                    bb_width = _safe(
                        (bb[bbu_col].iloc[-1] - bb[bbl_col].iloc[-1])
                        / bb[bbm_col].iloc[-1]
                    )
                except (IndexError, KeyError, ZeroDivisionError):
                    bb_width = None

            macd_val = macd_sig = macd_hist = None
            if macd_df is not None:
                try:
                    macd_val  = _safe(macd_df["MACD_12_26_9"].iloc[-1])
                    macd_sig  = _safe(macd_df["MACDs_12_26_9"].iloc[-1])
                    macd_hist = _safe(macd_df["MACDh_12_26_9"].iloc[-1])
                except KeyError:
                    pass

            adx_val = None
            if adx_df is not None:
                try:
                    adx_val = _safe(adx_df["ADX_14"].iloc[-1])
                except KeyError:
                    pass

            smc = smc_entry_signal(hist, atr_val)

            result = {
                "symbol":           symbol,
                "lastPrice":        round(float(last_price), 2),
                "peRatio":          round(float(info.get("trailingPE") or info.get("forwardPE") or 0), 2),
                "eps":              round(float(info.get("trailingEps") or 0), 2),
                "bookValue":        round(float(info.get("bookValue") or 0), 2),
                "marketCap":        info.get("marketCap") or 0,
                "roe":              round(float((info.get("returnOnEquity") or 0) * 100), 2),
                "debtToEquity":     round(float(info.get("debtToEquity") or 0), 2),
                "revenueGrowth":    round(float((info.get("revenueGrowth") or 0) * 100), 2),
                "earningsGrowth":   round(float((info.get("earningsQuarterlyGrowth") or 0) * 100), 2),
                "sector":           info.get("sector", "Unknown"),
                "industry":         info.get("industry", "Unknown"),
                "RSI":              _safe(rsi.iloc[-1]) if rsi is not None else None,
                "MACD":             macd_val,
                "MACD_Signal":      macd_sig,
                "MACD_Hist":        macd_hist,
                "SMA50":            _safe(sma50.iloc[-1]) if sma50 is not None else None,
                "SMA200":           last_sma200,
                "Pct_Above_SMA200": _safe(pct_above_sma200),
                "ADX":              adx_val,
                "BB_Width":         bb_width,
                "ATR":              atr_val,
                "Volume":           int(volume.iloc[-1]),
                "AvgVolume20":      int(avg_vol_20),
                "order_flow":       order_flow_score(hist),
                "structure":        structure_score(hist),
                "vcp":              vcp_score(hist),
                "rs_vs_index":      rs_vs_index_score(hist, nifty_hist),
                "proximity":        proximity_score(hist),
                "breakout_setup":   breakout_setup_score(hist),
                "volume_trend":     volume_trend_score(hist),
                **smc,
            }

            # RACE CONDITION FIX:
            # We DO NOT write to the cache dict or call save_cache() here.
            # The main thread will safely update the cache and save to disk.
            return result

        except Exception as e:
            wait = 2 ** attempt
            if attempt < 2:
                print(f"      {symbol}: attempt {attempt + 1} failed "
                      f"({str(e)[:60]}) — retry in {wait}s")
                time.sleep(wait)
            else:
                print(f"      {symbol}: failed after 3 attempts")
                return None

    return None


# ════════════════════════════════════════════════════════════════════════════════
# SCORING ENGINE
# ════════════════════════════════════════════════════════════════════════════════

def score_stock_advanced(row: dict) -> dict:
    score   = 0
    reasons = []

    pe = row.get("peRatio")
    if pe and pe > 0:
        if   5  < pe < 20:  score += 10; reasons.append(f"PE={pe:.1f} (deep value)")
        elif 20 <= pe < 30: score +=  7; reasons.append(f"PE={pe:.1f} (fair value)")
        elif 30 <= pe < 50: score +=  4; reasons.append(f"PE={pe:.1f} (growth premium)")

    roe = row.get("roe")
    if roe and roe > 0:
        if   roe > 20: score += 10; reasons.append(f"ROE={roe:.1f}% (excellent)")
        elif roe > 15: score +=  7; reasons.append(f"ROE={roe:.1f}% (good)")
        elif roe > 10: score +=  4; reasons.append(f"ROE={roe:.1f}% (decent)")

    rsi = row.get("RSI")
    if rsi:
        if   50 < rsi < 70:  score += 10; reasons.append(f"RSI={rsi:.1f} (momentum)")
        elif 40 < rsi <= 50: score +=  6; reasons.append(f"RSI={rsi:.1f} (healthy)")
        elif 30 < rsi <= 40: score +=  4; reasons.append(f"RSI={rsi:.1f} (recovering)")

    macd     = row.get("MACD")
    macd_sig = row.get("MACD_Signal")
    if macd and macd_sig:
        if   macd > macd_sig and macd > 0: score += 10; reasons.append("MACD bullish+positive")
        elif macd > macd_sig:              score +=  6; reasons.append("MACD bullish crossover")

    adx = row.get("ADX")
    if adx:
        if   adx > 30: score += 5; reasons.append(f"ADX={adx:.1f} (strong trend)")
        elif adx > 25: score += 3; reasons.append(f"ADX={adx:.1f} (trend forming)")

    of  = row.get("order_flow",     0) or 0
    st  = row.get("structure",      0) or 0
    vcp = row.get("vcp",            0) or 0
    rs  = row.get("rs_vs_index",    0) or 0
    prx = row.get("proximity",      0) or 0
    bs  = row.get("breakout_setup", 0) or 0
    vt  = row.get("volume_trend",   0) or 0

    score += of + st + vcp + rs + prx + bs + vt

    if of  > 10: reasons.append(f"OFI={of:.0f} (institutional buying)")
    if st  > 12: reasons.append(f"Structure={st:.0f} (HH/HL uptrend)")
    if vcp > 10: reasons.append(f"VCP={vcp:.0f} (coiling/breakout)")
    if rs  > 10: reasons.append(f"RS={rs:.0f} (beating NIFTY)")
    if prx > 10: reasons.append(f"Proximity={prx:.0f} (near 52wHi)")
    if bs  >  8: reasons.append(f"Breakout={bs:.0f} (pattern setup)")
    if vt  >  6: reasons.append(f"VolTrend={vt:.0f} (accumulation)")

    smc_conf = row.get("confluence", 0) or 0
    smc_sig  = row.get("smc_signal", "NO_SETUP") or "NO_SETUP"
    score   += smc_conf * 2

    if smc_sig == "HIGH_CONFLUENCE":
        reasons.append(f"SMC HIGH_CONFLUENCE (conf={smc_conf})")
    elif smc_sig == "MODERATE_SETUP":
        reasons.append(f"SMC MODERATE_SETUP (conf={smc_conf})")

    if row.get("mss_confirmed"):
        reasons.append("MSS confirmed (body close)")
    if row.get("in_fib_zone"):
        reasons.append(f"Fib discount zone (50%={row.get('fib_50')})")
    if row.get("displaced"):
        reasons.append("Displacement volume confirmed")

    row["score"]   = min(round(score / 1.64, 1), 100)
    row["reasons"] = " | ".join(reasons) if reasons else "No strong signals"
    return row


# ════════════════════════════════════════════════════════════════════════════════
# ENTRY REPORT PRINTER
# ════════════════════════════════════════════════════════════════════════════════

def print_entry_report(df: pd.DataFrame, top_n: int = 20) -> None:
    print("\n" + "=" * 100)
    print(f"  TOP {top_n} STOCKS BY COMPOSITE SCORE")
    print("=" * 100)
    for rank, (_, row) in enumerate(df.head(top_n).iterrows(), 1):
        print(f"  {rank:2d}. {row['symbol']:12s}  Rs.{row['lastPrice']:8.2f}  |  "
              f"Score:{row['score']:6.1f}  |  PE:{row['peRatio']:6.1f}  |  ROE:{row['roe']:5.1f}%")
        print(f"      {str(row.get('reasons', ''))[:115]}")
        print()

    smc_df = df[df["smc_signal"].isin(["HIGH_CONFLUENCE", "MODERATE_SETUP"])].copy()
    smc_df = smc_df.sort_values(["smc_score", "score"], ascending=False)

    if smc_df.empty:
        print("  ── No SMC setups detected in current scan ──\n")
        return

    print("=" * 100)
    print(f"  SMC PRECISION ENTRY REPORT  ({len(smc_df)} setups found)")
    print(f"  {'Symbol':<12} {'Signal':<18} {'Dir':<8} {'Entry':>9} {'SL':>9} "
          f"{'TP':>9} {'R:R':>5} {'Conf':>5}  Notes")
    print("-" * 100)

    for _, row in smc_df.iterrows():
        entry = row.get("entry_price")
        sl    = row.get("stop_loss")
        tp    = row.get("take_profit")
        rr    = row.get("rr_ratio")
        conf  = row.get("confluence", 0)
        sig   = row.get("smc_signal", "")
        dirn  = row.get("smc_direction", "")

        notes = []
        if row.get("mss_confirmed"):  notes.append("MSS✓")
        if row.get("in_fib_zone"):    notes.append("FibDisc✓")
        if row.get("displaced"):      notes.append("Disp✓")
        if row.get("fvg_mid"):        notes.append(f"FVG@{row['fvg_mid']:.2f}")
        if row.get("ob_low"):         notes.append(f"OB[{row['ob_low']:.2f}-{row['ob_high']:.2f}]")

        print(f"  {row['symbol']:<12} {sig:<18} {str(dirn).upper():<8} "
              f"{entry if entry else 'N/A':>9} "
              f"{sl if sl else 'N/A':>9} "
              f"{tp if tp else 'N/A':>9} "
              f"{rr if rr else 'N/A':>5}  "
              f"{conf:>2}  {' | '.join(notes)}")

    print("=" * 100)
    print()
    print("  ENTRY EXECUTION GUIDE:")
    print("  • Entry  = FVG 50% fill OR Order Block mid (whichever offers better confluence)")
    print("  • SL     = Below sweep candle low − 0.5×ATR  (invalidates the setup)")
    print("  • TP     = Nearest swing high / liquidity pool above entry (min 2:1 R:R)")
    print("  • Filter = Only trade HIGH_CONFLUENCE signals with MSS + Displacement confirmed")
    print()


# ════════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════════

def main():
    symbols = list(dict.fromkeys([
        "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR", "ITC",
        "SBIN", "BHARTIARTL", "KOTAKBANK", "BAJFINANCE", "LT", "WIPRO", "AXISBANK",
        "TITAN", "ASIANPAINT", "MARUTI", "SUNPHARMA", "HCLTECH", "NTPC",
        "ONGC", "POWERGRID", "ULTRACEMCO", "BAJAJFINSV", "ADANIPORTS",
        "TATAMOTORS", "NESTLEIND", "JSWSTEEL", "TATASTEEL", "TECHM",
        "HDFCLIFE", "SBILIFE", "COALINDIA", "BRITANNIA", "GRASIM",
        "EICHERMOT", "DIVISLAB", "DRREDDY", "CIPLA", "BPCL",
        "HINDALCO", "IOC", "HEROMOTOCO", "BAJAJ-AUTO", "UPL",
        "SHREECEM", "TATACONSUM", "ADANIGREEN", "ADANIENT", "TRENT",
        "DABUR", "PIDILITIND", "MARICO", "HAVELLS", "ICICIPRULI",
        "ICICIGI", "HDFCAMC", "INDUSINDBK", "BANDHANBNK", "NAUKRI",
        "DLF", "GODREJCP", "COLPAL", "AMBUJACEM", "SIEMENS",
        "BIOCON", "PAGEIND", "MRF", "BOSCHLTD", "HAL",
        "BEL", "BHEL", "GAIL", "PETRONET", "LICHSGFIN",
        "CHOLAFIN", "VOLTAS", "INDIGO", "JUBLFOOD", "BATAINDIA",
        "APOLLOHOSP", "APOLLOTYRE", "ASHOKLEY", "BANKBARODA", "PNB",
        "IDFCFIRSTB", "FEDERALBNK", "IRCTC", "TVSMOTOR", "VEDL",
        "SAIL", "TATAPOWER", "LUPIN", "AUROPHARMA", "ALKEM",
        "TORNTPHARM", "PFIZER", "SANOFI", "ABBOTINDIA", "GLAXO",
        "NOVARTISIND", "M&MFIN", "HINDZINC", "NATIONALUM",
        "JINDALSTEL", "ADANITRANS", "CESC", "NHPC", "LTI",
        "MINDTREE", "CADILAHC", "CUMMINSIND", "ABB",
        "EXIDEIND", "CANBK", "UNIONBANK", "RBLBANK",
        "AUBANK", "YESBANK", "IEX", "PFC", "RECLTD",
        "CONCOR", "ADANIPOWER", "ABCAPITAL",
        "TORNTPOWER", "GLENMARK", "M&M", "AVENUE", "SRTRANSFIN",
    ]))

    cache      = load_cache()
    final_data = []

    print("\n" + "=" * 100)
    print("  ★  NIFTY 200 — SMC PRECISION ENTRY ENGINE  v5.1")
    print("     Modules: Liquidity Sweep | FVG | MSS | Order Block | Fib Zone | Displacement")
    print("     Legacy : OFI | HH/HL Structure | VCP | RS vs NIFTY | 52wHi Proximity")
    print("=" * 100)
    print(f"  Scanning {len(symbols)} stocks | {MAX_WORKERS} parallel workers | "
          f"cache={CACHE_EXPIRY_HOURS}h\n")

    # ── Fetch NIFTY 50 benchmark ─────────────────────────────────────────────────
    print("  Fetching NIFTY 50 for relative strength...")
    try:
        nifty_hist = _flatten_columns(yf.Ticker("^NSEI").history(period="1y"))
        if nifty_hist.empty:
            print("  !! NIFTY 50 unavailable — RS scores will be 0")
            nifty_hist = pd.DataFrame()
        else:
            print(f"  NIFTY 50 loaded: {len(nifty_hist)} bars\n")
    except Exception as e:
        print(f"  !! NIFTY fetch failed: {e}")
        nifty_hist = pd.DataFrame()

    # ── Parallel fetch ───────────────────────────────────────────────────────────
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_stock_data, sym, cache, nifty_hist): sym
            for sym in symbols
        }
        for i, future in enumerate(as_completed(futures), 1):
            sym    = futures[future]
            result = future.result()

            if result is None:
                print(f"  [{i:3d}/{len(symbols)}] {sym:15s} ✗ SKIPPED")
                continue

            result = score_stock_advanced(result)
            final_data.append(result)

            # RACE CONDITION FIX:
            # Safely update the cache dict in memory.
            # Updating a dict from concurrent threads in Python is inherently thread-safe
            # for individual key assignments due to the GIL.
            cache[sym] = {
                "ts": datetime.now().isoformat(),
                "data": result
            }

            smc_tag = result.get("smc_signal", "NO_SETUP") or "NO_SETUP"
            smc_dir = (result.get("smc_direction") or "")[:4].upper()
            print(
                f"  [{i:3d}/{len(symbols)}] {sym:15s} | "
                f"Score:{result['score']:6.1f}/100 | "
                f"Rs.{result['lastPrice']:8.2f} | "
                f"OFI:{result.get('order_flow', 0):5.1f} "
                f"Str:{result.get('structure', 0):5.1f} "
                f"VCP:{result.get('vcp', 0):5.1f} "
                f"RS:{result.get('rs_vs_index', 0):5.1f} "
                f"Prx:{result.get('proximity', 0):5.1f} | "
                f"SMC:{smc_tag[:14]:<14} {smc_dir}"
            )

    # RACE CONDITION FIX:
    # Save the entire cache to disk ONCE at the end of execution
    # after all threads have safely completed.
    print("\n  Saving cache to disk...")
    save_cache(cache)

    if not final_data:
        print("\n!! No data collected. Check internet connection.")
        return

    df = pd.DataFrame(final_data).sort_values("score", ascending=False)

    # ── Save CSV ─────────────────────────────────────────────────────────────────
    output_file = "nifty200_smc_precision_screen.csv"
    df.to_csv(output_file, index=False)

    # ── Print reports ────────────────────────────────────────────────────────────
    print_entry_report(df, top_n=20)

    print("=" * 100)
    print(f"  Saved: {output_file}  ({len(df)} stocks)\n")
    print(f"  Score Distribution:")
    print(f"    Elite    80-100 : {len(df[df['score'] >= 80]):3d} stocks")
    print(f"    Strong   65-79  : {len(df[(df['score'] >= 65) & (df['score'] < 80)]):3d} stocks")
    print(f"    Moderate 50-64  : {len(df[(df['score'] >= 50) & (df['score'] < 65)]):3d} stocks")
    print(f"    Weak     35-49  : {len(df[(df['score'] >= 35) & (df['score'] < 50)]):3d} stocks")
    print(f"    No setup  <35   : {len(df[df['score'] < 35]):3d} stocks\n")

    smc_counts = df["smc_signal"].value_counts()
    print(f"  SMC Signal Distribution:")
    for sig, cnt in smc_counts.items():
        print(f"    {str(sig):<22}: {cnt:3d} stocks")

    print()
    print("  BEST COMBO (Minervini SEPA + SMC):")
    print("    RS ≥ 10  +  VCP ≥ 10  +  Proximity ≥ 10  +  SMC HIGH_CONFLUENCE")
    print("    Entry trigger: breakout candle on volume > 1.5× average, inside FVG/OB zone")
    print("=" * 100 + "\n")


if __name__ == "__main__":
    main()