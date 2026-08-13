"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║   NIFTY 200 — SMC PRECISION ENTRY ENGINE  v5.1                                 ║
║   Smart Money Concepts + Minervini SEPA + Institutional Flow                   ║
║                                                                                  ║
║   UPDATES IN v5.1:                                                              ║
║     • FIXED: Cache race condition (save_cache moved to main thread)             ║
║     • ADDED: Strict chronological sequence validation (Sweep -> MSS -> OB)      ║
║     • HYBRID: Local 5m Parquet OHLCV + Yahoo Finance Fundamentals & 52W High    ║
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
import yfinance as yf
import pandas_ta as ta
import json
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
from scipy.signal import argrelextrema
import numpy as np


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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════

CACHE_FILE         = os.path.join(BASE_DIR, "nifty_yf_cache.json")
CACHE_EXPIRY_HOURS = 6
MAX_WORKERS        = 5
SWING_LOOKBACK     = 10   
ATR_SL_MULTIPLIER  = 0.5  
MIN_RR             = 2.0  


# ════════════════════════════════════════════════════════════════════════════════
# CACHE HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cache(cache: dict) -> None:
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        print(f"Cache save error: {e}")

def is_cache_fresh(cache: dict, key: str) -> bool:
    if key not in cache:
        return False
    try:
        cached_time = datetime.fromisoformat(cache[key]["ts"])
        return datetime.now() - cached_time < timedelta(hours=CACHE_EXPIRY_HOURS)
    except Exception:
        return False


# ════════════════════════════════════════════════════════════════════════════════
# UTILITY
# ════════════════════════════════════════════════════════════════════════════════

def _safe(val) -> float | None:
    try:
        if val is None: return None
        v = float(val)
        return round(v, 4) if not np.isnan(v) else None
    except Exception:
        return None

def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
    return df

def _safe_float(val, default=0.0) -> float:
    if val is None: return default
    try:
        if hasattr(val, "iloc") and not val.empty:
            val = val.iloc[-1]
        elif hasattr(val, "item"):
            val = val.item()
        return float(val)
    except (ValueError, TypeError, IndexError):
        return default


# ════════════════════════════════════════════════════════════════════════════════
# SMC MODULES
# ════════════════════════════════════════════════════════════════════════════════

def find_swing_pivots(high: np.ndarray, low: np.ndarray, n: int = 2):
    sh_idx = argrelextrema(high, np.greater, order=n)[0].tolist()
    sl_idx = argrelextrema(low, np.less, order=n)[0].tolist()
    return sh_idx, sl_idx

def detect_liquidity_sweep(high: np.ndarray, low: np.ndarray, close: np.ndarray, open_: np.ndarray, sh_idx: list, sl_idx: list, lookback: int = 5):
    result = {"swept": False, "direction": None, "sweep_level": None, "sweep_bar": None, "sweep_low": None, "sweep_high": None}
    n = len(close)
    check_range = range(max(n - lookback, 1), n)

    if sl_idx:
        recent_sl_level = low[sl_idx[-1]]
        for i in check_range:
            if low[i] < recent_sl_level and close[i] > recent_sl_level:
                result.update({"swept": True, "direction": "bullish", "sweep_level": round(float(recent_sl_level), 4), "sweep_bar": i, "sweep_low": round(float(low[i]), 4), "sweep_high": round(float(high[i]), 4)})
                return result

    if sh_idx:
        recent_sh_level = high[sh_idx[-1]]
        for i in check_range:
            if high[i] > recent_sh_level and close[i] < recent_sh_level:
                result.update({"swept": True, "direction": "bearish", "sweep_level": round(float(recent_sh_level), 4), "sweep_bar": i, "sweep_low": round(float(low[i]), 4), "sweep_high": round(float(high[i]), 4)})
                return result
    return result

def detect_fvg(high: np.ndarray, low: np.ndarray, close: np.ndarray, direction: str | None, lookback: int = 20):
    result = {"found": False, "fvg_high": None, "fvg_low": None, "fvg_mid": None, "retesting": False, "direction": direction}
    if direction is None: return result
    n = len(close)
    current_close = close[n - 1]

    for i in range(n - 1, max(n - lookback - 1, 1), -1):
        if direction == "bullish":
            if high[i - 2] < low[i]:
                gap_low, gap_high = float(high[i - 2]), float(low[i])
                result.update({"found": True, "fvg_high": round(gap_high, 4), "fvg_low": round(gap_low, 4), "fvg_mid": round((gap_low + gap_high) / 2, 4), "retesting": gap_low <= current_close <= gap_high})
                return result
        elif direction == "bearish":
            if low[i - 2] > high[i]:
                gap_high, gap_low = float(low[i - 2]), float(high[i])
                result.update({"found": True, "fvg_high": round(gap_high, 4), "fvg_low": round(gap_low, 4), "fvg_mid": round((gap_low + gap_high) / 2, 4), "retesting": gap_low <= current_close <= gap_high})
                return result
    return result

def detect_mss(high: np.ndarray, low: np.ndarray, close: np.ndarray, sh_idx: list, sl_idx: list, direction: str | None):
    result = {"confirmed": False, "mss_level": None, "mss_bar": None}
    if direction is None: return result
    n = len(close)
    NOISE_FILTER = 0.0005  
    check_range = range(n - 1, max(n - 12, 0), -1)

    if direction == "bullish" and sh_idx:
        pivot_level = float(high[sh_idx[-1]])
        for i in check_range:
            if float(close[i]) > pivot_level * (1 + NOISE_FILTER):
                result.update({"confirmed": True, "mss_level": round(pivot_level, 4), "mss_bar": i})
                return result
    elif direction == "bearish" and sl_idx:
        pivot_level = float(low[sl_idx[-1]])
        for i in check_range:
            if float(close[i]) < pivot_level * (1 - NOISE_FILTER):
                result.update({"confirmed": True, "mss_level": round(pivot_level, 4), "mss_bar": i})
                return result
    return result

def detect_order_block(high: np.ndarray, low: np.ndarray, close: np.ndarray, open_: np.ndarray, volume: np.ndarray, direction: str | None, sweep_bar: int | None, lookback: int = 15):
    result = {"found": False, "ob_high": None, "ob_low": None, "ob_mid": None, "ob_bar": None}
    if direction is None: return result
    n = len(close)
    start = min(sweep_bar if sweep_bar is not None else n - 2, n - 2)
    bodies = np.abs(close - open_)
    avg_body = bodies[max(0, start - 10): start].mean() if start > 0 else 1.0
    avg_body = avg_body if avg_body != 0 else 1.0
    avg_vol = volume[max(0, start - 10): start].mean() if start > 0 else 1.0
    avg_vol = avg_vol if avg_vol != 0 else 1.0

    for i in range(start, max(start - lookback, 1), -1):
        if direction == "bullish":
            is_bearish_ob = close[i] < open_[i]
            next_body = close[i + 1] - open_[i + 1]
            if is_bearish_ob and (next_body > avg_body * 1.5) and (close[i + 1] > open_[i + 1]) and (volume[i + 1] > avg_vol * 1.2):
                ob_low, ob_high = float(low[i]), float(high[i])
                result.update({"found": True, "ob_high": round(ob_high, 4), "ob_low": round(ob_low, 4), "ob_mid": round((ob_low + ob_high) / 2, 4), "ob_bar": i})
                return result
        elif direction == "bearish":
            is_bullish_ob = close[i] > open_[i]
            next_body = open_[i + 1] - close[i + 1]
            if is_bullish_ob and (next_body > avg_body * 1.5) and (close[i + 1] < open_[i + 1]) and (volume[i + 1] > avg_vol * 1.2):
                ob_low, ob_high = float(low[i]), float(high[i])
                result.update({"found": True, "ob_high": round(ob_high, 4), "ob_low": round(ob_low, 4), "ob_mid": round((ob_low + ob_high) / 2, 4), "ob_bar": i})
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

        if mss["confirmed"] and mss["mss_bar"] is not None and sweep_bar is not None and mss["mss_bar"] < sweep_bar: return default
        if ob["found"] and ob["ob_bar"] is not None and mss["mss_bar"] is not None and ob["ob_bar"] > mss["mss_bar"]: return default

        fib = premium_discount_zone(h_arr, l_arr, c_arr, direction)
        displaced = displacement_confirmed(c_arr, o_arr, v_arr, direction)

        confluence = sum([sweep["swept"], fvg["found"], fvg["retesting"], mss["confirmed"], ob["found"], fib["in_zone"], displaced])
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

def weekly_trend_check(hist: pd.DataFrame) -> dict:
    try:
        if len(hist) < 100: return {"mtf_score": 0, "mtf_status": "Neutral"}
        weekly = hist.resample('W-FRI').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}).dropna()
        close_w = weekly['Close']
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

# 🔴 NEW: Updated Proximity Score (Uses 52W High from Yahoo directly)
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
# DATA FETCH  — HYBRID (Yahoo Fundamentals + Local Parquet OHLCV)
# ════════════════════════════════════════════════════════════════════════════════

def fetch_stock_data(symbol: str, cache: dict, nifty_hist: pd.DataFrame, sector_data_dict: dict) -> dict | None:
    try:
        # 1. YFINANCE: కేవలం ఫండమెంటల్స్ & 52W High కోసం 
        ticker_symbol = symbol + ".NS"
        ticker = yf.Ticker(ticker_symbol)

        try:
            info = ticker.info
        except Exception:
            info = {}

        pe_ratio = _safe_float(info.get("trailingPE") or info.get("forwardPE"))
        eps_val = _safe_float(info.get("trailingEps"))
        roe_val = _safe_float(info.get("returnOnEquity"))
        if roe_val > 0 and roe_val < 5:
            roe_val = roe_val * 100

        sector_name = info.get("sector", "Unknown")
        sector_symbol = SECTOR_MAP.get(sector_name)
        sector_hist = sector_data_dict.get(sector_symbol) if sector_symbol else pd.DataFrame()

        # 🔴 NEW: Yahoo Finance 52-Week High & Fallback Backup
        high_52w = _safe_float(info.get("fiftyTwoWeekHigh"))
        low_52w = _safe_float(info.get("fiftyTwoWeekLow"))

        # ఒకవేళ Yahoo బ్లాక్ చేసి 0 వస్తే, హిస్టరీ నుండి లాగుతుంది
        if high_52w == 0.0 or low_52w == 0.0:
            try:
                yf_hist = ticker.history(period="1y")
                if not yf_hist.empty:
                    high_52w = float(yf_hist["High"].max())
                    low_52w = float(yf_hist["Low"].min())
            except Exception:
                pass


        # 2. LOCAL PARQUET: OHLCV హిస్టారికల్ డేటా కోసం
        parquet_path = rf"D:\Api data\ML_HISTO_DATA\Upstox_History\{symbol}_5m.parquet"
        if not os.path.exists(parquet_path):
            parquet_path = rf"D:\Api data\ML_HISTO_DATA\Upstox_History\{symbol}_1m.parquet"

        if not os.path.exists(parquet_path):
            return None

        try:
            df = pd.read_parquet(parquet_path)
        except Exception:
            return None

        if df is None or df.empty:
            return None

        if "Datetime" in df.columns:
            df = df.copy()
            df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce")
            df = df.dropna(subset=["Datetime"]).set_index("Datetime")
        else:
            if not isinstance(df.index, pd.DatetimeIndex):
                try:
                    df.index = pd.to_datetime(df.index, errors="coerce")
                except Exception:
                    return None
            df = df.copy()

        required_cols = ["Open", "High", "Low", "Close", "Volume"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return None

        # 5-Min డేటాని డైలీ (1D) గా మారుస్తున్నాం
        hist = (
            df[required_cols]
            .sort_index()
            .resample("1D")
            .agg({
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
            })
            .dropna()
        )

        if hist is None or hist.empty or len(hist) < 20:
            return None

        last_price = _safe_float(hist["Close"].iloc[-1])

        # 3. ఇండికేటర్స్ మరియు స్కోర్స్ లెక్కింపు
        atr_series = ta.atr(hist["High"], hist["Low"], hist["Close"], length=14)
        atr_val = float(atr_series.iloc[-1]) if atr_series is not None and not atr_series.empty else 0.0

        smc = smc_entry_signal(hist, atr_val)
        mtf_data = weekly_trend_check(hist)

        # 4. ఫైనల్ రిజల్ట్
        result = {
            "symbol": symbol,
            "lastPrice": round(last_price, 2),
            "peRatio": round(pe_ratio, 2),
            "eps": round(eps_val, 2),
            "roe": round(roe_val, 2),
            "sector": sector_name,
            "history_data": hist.tail(10),
            "order_flow": order_flow_score(hist),
            "structure": structure_score(hist),
            "vcp": vcp_score(hist),
            "rs_vs_index": rs_vs_index_score(hist, nifty_hist, sector_hist),
            "mtf_score": mtf_data.get("mtf_score", 0),
            "mtf_status": mtf_data.get("mtf_status", "Neutral"),
            "proximity": proximity_score(last_price, high_52w, low_52w), # 🔴 Updated Proximity Call
            "breakout_setup": breakout_setup_score(hist),
            "volume_trend": volume_trend_score(hist),
            **smc,
        }

        return result

    except Exception as e:
        print(f"⚠️ Error fetching data for {symbol}: {e}")
        return None

# ════════════════════════════════════════════════════════════════════════════════
# SCORING ENGINE
# ════════════════════════════════════════════════════════════════════════════════

def score_stock_advanced(row: dict) -> dict:
    score = 0.0
    reasons = []

    def safe_val(key):
        val = row.get(key)
        return float(val) if val is not None and str(val).replace('.', '', 1).isdigit() else 0.0

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

    if smc_sig == "HIGH_CONFLUENCE": reasons.append(f"SMC High Confluence ({smc_conf}/7)")
    elif smc_sig == "MODERATE_SETUP": reasons.append(f"SMC Moderate Setup ({smc_conf}/7)")
    if row.get("mss_confirmed"): reasons.append("MSS Confirmed")

    fib_val = row.get('fib_50')
    if row.get("in_fib_zone") and fib_val is not None: reasons.append(f"Discount Zone (50%={float(fib_val):.1f})")
    if row.get("displaced"): reasons.append("Displacement Vol Confirmed")

    final_score = round(score / 1.64, 1)
    row["score"] = float(min(final_score, 100.0))
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
    else:
        smc_df = pd.DataFrame()

    if smc_df.empty:
        print("  ── No SMC setups detected in current scan ──\n")
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

    print("=" * 100 + "\n  ENTRY EXECUTION GUIDE:")
    print("  • Entry  = FVG 50% fill OR Order Block mid (whichever offers better confluence)")
    print("  • SL     = Below sweep candle low − 0.5×ATR  (invalidates the setup)")
    print("  • TP     = Nearest swing high / liquidity pool above entry (min 2:1 R:R)")
    print("  • Filter = Only trade HIGH_CONFLUENCE signals with MSS + Displacement confirmed\n")

# ════════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════════

def get_nifty200_symbols():
    print("  Fetching latest NIFTY 200 list from NSE...")
    try:
        url = "https://archives.nseindia.com/content/indices/ind_nifty200list.csv"
        df = pd.read_csv(url)
        symbols = df['Symbol'].tolist()
        print(f"  Successfully loaded {len(symbols)} stocks from NSE.\n")
        return symbols
    except Exception as e:
        print(f"  !! Failed to fetch NIFTY 200 list: {e}")
        return ["RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "ITC", "SBIN", "LTIM"]

def main():
    symbols = list(dict.fromkeys(get_nifty200_symbols()))
    cache = load_cache()
    cache.clear() 
    final_data = []
    ltp_max = 2000.0

    print("\n" + "=" * 100)
    print("  ★  NIFTY 200 — SMC PRECISION ENTRY ENGINE  v5.1")
    print("     Modules: Liquidity Sweep | FVG | MSS | Order Block | Fib Zone | Displacement")
    print("     Legacy : OFI | HH/HL Structure | VCP | RS vs NIFTY | 52wHi Proximity")
    print("=" * 100)

    print(f"  Scanning {len(symbols)} stocks | {MAX_WORKERS} parallel workers | cache=12h\n")

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

    print("  Fetching Sectoral Indices for Advanced RS...")
    sector_data_dict = {}
    unique_sector_symbols = list(set(SECTOR_MAP.values()))

    for sec_sym in unique_sector_symbols:
        try:
            sec_hist = _flatten_columns(yf.Ticker(sec_sym).history(period="1y"))
            sector_data_dict[sec_sym] = sec_hist
        except Exception:
            sector_data_dict[sec_sym] = pd.DataFrame()
    print("  Sectoral data loaded successfully.\n")

    processed_symbols = set() 

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_stock_data, sym, cache, nifty_hist, sector_data_dict): sym for sym in symbols}
        for i, future in enumerate(as_completed(futures), 1):
            sym = futures[future]
            if sym in processed_symbols: continue
            processed_symbols.add(sym)

            result = future.result()
            if result is None:
                print(f"  [{i:3d}/{len(symbols)}] {sym:15s} ✗ SKIPPED")
                continue

            result = score_stock_advanced(result)
            clean_result = {k: v for k, v in result.items() if not isinstance(v, (pd.DataFrame, pd.Series))}
            clean_result["symbol"] = sym

            ltp = float(clean_result.get("lastPrice", 0) or 0)
            if ltp >= ltp_max: continue

            final_data.append(clean_result)
            cache[sym] = {"ts": datetime.now().isoformat(), "data": clean_result}

            smc_tag = clean_result.get("smc_signal", "NO_SETUP") or "NO_SETUP"
            smc_dir = (clean_result.get("smc_direction") or "")[:4].upper()
            score = clean_result.get('score', 0)
            last_price = clean_result.get('lastPrice', 0)

            print(
                f"  [{i:3d}/{len(symbols)}] {sym:15s} | Score:{score:6.1f}/100 | Rs.{last_price:8.2f} | "
                f"OFI:{clean_result.get('order_flow', 0):5.1f} Str:{clean_result.get('structure', 0):5.1f} "
                f"VCP:{clean_result.get('vcp', 0):5.1f} RS:{clean_result.get('rs_vs_index', 0):5.1f} "
                f"Prx:{clean_result.get('proximity', 0):5.1f} | SMC:{smc_tag[:14]:<14} {smc_dir}"
            )

    print("\n  Saving cache to disk...")
    save_cache(cache)
    if not final_data:
        print(f"\n!! No stocks matched the LTP filter (< {ltp_max:.0f}).")
        return

    df = pd.DataFrame(final_data)

    default_columns = {'symbol': 'UNKNOWN', 'lastPrice': 0.0, 'score': 0.0, 'peRatio': 0.0, 'eps': 0.0, 'RSI': 0.0, 'order_flow': 0.0, 'structure': 0.0, 'vcp': 0.0, 'rs_vs_index': 0.0, 'proximity': 0.0, 'mtf_score': 0.0, 'smc_signal': 'NO_SETUP'}
    for col, default_val in default_columns.items():
        if col not in df.columns: df[col] = default_val

    df = df.sort_values("score", ascending=False)
    output_file = os.path.join(BASE_DIR, "nifty200_smc_precision_screen.csv")
    try:
        df.to_csv(output_file, index=False)
    except PermissionError:
        backup_file = os.path.join(BASE_DIR, "nifty200_smc_backup_screen.csv")
        print(f"\n  ⚠️ Warning: '{os.path.basename(output_file)}' is open. Saving to {os.path.basename(backup_file)} instead.")
        df.to_csv(backup_file, index=False)

    try: print_entry_report(df, top_n=20)
    except NameError: pass 

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

    print("\n  BEST COMBO (Minervini SEPA + SMC):")
    print("    RS ≥ 10  +  VCP ≥ 10  +  Proximity ≥ 10  +  SMC HIGH_CONFLUENCE")
    print("    Entry trigger: breakout candle on volume > 1.5× average, inside FVG/OB zone")
    print("=" * 100 + "\n")

if __name__ == "__main__":
    main()