"""
trend_ready.py — Market Regime & Setup Scanner (Stage 2 of the swing pipeline).

Reads historical Daily + Weekly OHLCV (Upstox parquet) for the fundamentally
cleared universe emitted by funda_data.py (``nifty500_swing_candidates.csv``)
and buckets each stock into one of eight high-probability swing setups:

    1. Bottom Fishing   — within 10% of its 52-week low AND printing a reversal
                          signature (long lower wick / liquidity sweep / strong
                          bullish close after a downtrend).
    2. Oversold BB20 Reclaim — RSI(14) dipped below 35 within the last 15 days,
                          reclaimed the BB20 midline with RSI > 50, and printed
                          dual-timeframe volume dry-up (daily < 0.75, weekly < 0.80).
    3. Mega Reversal (Weekly BB20) — macro reversal: weekly RSI dipped below 35
                          within the last 15 weeks, daily close reclaiming the
                          weekly BB20 midline with weekly volume exhaustion (< 0.85).
    4. BB Lower Band Snap (Weekly) — weekly low pierced the weekly BB20 lower band,
                          then the weekly close reclaimed it with weekly RSI < 40.
    5. BB Lower Band Snap (Daily) — daily low pierced the daily BB20 lower band,
                          then a bullish close reclaimed it with daily RSI < 40.
    6. Mid-Range Bounce — sitting in the 40-60% retracement band of the 52-week
                          High-Low range AND pulling back to the daily 20/50 EMA
                          while support holds.
    7. VCP Breakout     — price crossing / bouncing off the *weekly* 20 EMA with
                          a Volatility Contraction Pattern or Inside Bar (the
                          "Weekly Trend Resumption" bucket). EMA contact must be
                          FRESH: a cross within the last 3 bars, or ±2% proximity.
    8. Consolidation/Flag — established daily uptrend (50 EMA > 200 EMA) with a
                          recent sideways/slightly-down contraction (Bollinger
                          Band squeeze, ADX < 20, or true-range contraction).

A universal "volume dry-up" check (latest volume / 20-day average) flags the
highest-odds contractions (ratio < 0.75), and a 0-100 Momentum Score blends
daily RSI(14) with the 5-bar slopes of the 20/50 EMA.

INPUT
-----
Daily dir   : D:/Api data/ML_HISTO_DATA/Upstox Data/Day Data/NSE_EQ_<ISIN>.parquet
Weekly dir  : D:/Api data/ML_HISTO_DATA/Upstox Data/Weeks Data/NSE_EQ_<ISIN>.parquet
Candidates  : nifty500_swing_candidates.csv   (same directory as this script)
ISIN map    : ind_nifty500list.csv            (symbol -> ISIN, for file lookup)

OUTPUT
------
master_trend_setups.csv  — one row per qualifying setup, keyed by Setup_Category.

RUN
---
python trend_ready.py
python trend_ready.py --limit 50
python trend_ready.py --symbols RELIANCE,TCS,ACC
"""

import argparse
import os
import sys

import numpy as np
import pandas as pd

# ─────────────────────────────── CONFIG ───────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CANDIDATES_FILE = os.path.join(BASE_DIR, "nifty500_swing_candidates.csv")
UNIVERSE_FILE = os.path.join(BASE_DIR, "ind_nifty500list.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "master_trend_setups.csv")

DAY_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox Data\Day Data"
WEEK_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox Data\Weeks Data"

# ── Bucket thresholds ─────────────────────────────────────────────────────────
LOW_PROXIMITY_PCT = 10.0              # Bottom Fishing: within this % of 52w low
MID_RANGE_LO = 40.0                   # Mid-Range Bounce band (retracement %)
MID_RANGE_HI = 60.0
DAILY_EMA_FAST = 20
DAILY_EMA_SLOW = 50
WEEKLY_EMA_FAST = 20
WEEKLY_EMA_SLOW = 50
EMA_TOUCH_TOL = 0.02                  # ±2% of an EMA counts as "touching" it
RSI_PERIOD = 14
VOL_AVG_WINDOW = 20                   # 20-day average-volume denominator
VOL_DRY_THRESH = 0.75                 # flag volume contraction below this ratio
LOOKBACK_52W = 252                    # ~52 weeks of trading days
SLOPE_LOOKBACK = 5                    # bars used to estimate EMA slope
MIN_DAILY_BARS = 60                   # minimum history for reliable daily signals
MIN_WEEKLY_BARS = 40                  # minimum history for weekly signals

# ── "Consolidation / Flag" bucket + fresh-EMA-contact gauges ──────────────────
DAILY_EMA_LONG = 200                  # 200-day EMA (uptrend definition)
ADX_PERIOD = 14                       # ADX smoothing period
ADX_FLAT = 20.0                       # ADX below this => no trend / sideways
BB_PERIOD = 20                        # Bollinger Band window
BB_SQUEEZE_LOOKBACK = 100             # rolling window for squeeze percentile
BB_SQUEEZE_QUANTILE = 0.25            # band width in the bottom 25% => squeeze
TR_RECENT = 10                        # recent true-range window (days)
TR_PRIOR = 10                         # prior true-range window (days)
EMA_CROSS_BARS = 3                    # fresh EMA cross must occur within N bars

# ── "Oversold BB20 Reclaim" bucket ────────────────────────────────────────────
BB20_MID_PERIOD = 20                  # BB20 midline = 20-period SMA
RSI_OVERSOLD = 35.0                   # RSI below this marks the oversold dip
RSI_DIP_LOOKBACK = 15                 # lookback (days) for the oversold RSI dip
RSI_RECLAIM = 50.0                    # RSI must be above this for the trigger
WEEKLY_VOL_AVG = 10                   # 10-week average-volume denominator
WEEKLY_VOL_DRY_THRESH = 0.80          # weekly volume contraction threshold

# ── "Mega Reversal (Weekly BB20)" bucket ──────────────────────────────────────
WEEKLY_BB20_PERIOD = 20                # weekly BB20 midline = 20-week SMA
WEEKLY_RSI_DIP_WEEKS = 15              # lookback (weeks) for weekly oversold RSI dip
MEGA_WEEKLY_VOL_DRY = 0.85             # weekly volume exhaustion threshold

# ── "BB Lower Band Snap" buckets ──────────────────────────────────────────────
BB_SNAP_RSI_MAX = 40.0                 # RSI must still be below this (oversold-ish)
BB_SNAP_STD_MULT = 2.0                 # lower band = 20-SMA - 2 * std

CATEGORY_ORDER = ["Bottom Fishing", "Oversold BB20 Reclaim",
                  "Mega Reversal (Weekly BB20)", "BB Lower Band Snap (Weekly)",
                  "BB Lower Band Snap (Daily)", "Mid-Range Bounce",
                  "VCP Breakout", "Consolidation / Flag"]

OUTPUT_COLUMNS = [
    "symbol", "name", "industry", "Setup_Category", "funda_current_price",
    "last_close", "last_date", "pct_above_52w_low", "range_position_pct",
    "near_52w_low", "reversal_signature", "daily_ema_support", "daily_ema20",
    "daily_ema50", "daily_ema200", "uptrend_50_200", "adx_14", "bb_squeeze",
    "tr_contraction", "flag_consolidation",
    "weekly_ema20_signal", "weekly_ema20", "weekly_ema50",
    "vcp", "inside_bar", "Volume_Dry_Ratio", "Volume_Dry_Up", "Weekly_Vol_Ratio",
    "RSI_14",
    "EMA20_slope_5d_pct", "EMA50_slope_5d_pct", "Momentum_Score",
]

def _check_parquet_support():
    """Return True if pandas can read parquet (pyarrow or fastparquet installed)."""
    try:
        import pyarrow  # noqa: F401
        return True
    except ImportError:
        pass
    try:
        import fastparquet  # noqa: F401
        return True
    except ImportError:
        return False


def _normalize_columns(df):
    """Lowercase/strip column names and map common OHLCV aliases to canonical names."""
    df = df.copy()
    df.columns = [str(c).strip().split("(")[0].strip().lower() for c in df.columns]
    aliases = {
        "o": "open", "open price": "open",
        "h": "high", "high price": "high",
        "l": "low", "low price": "low",
        "c": "close", "close price": "close", "last": "close",
        "ltp": "close", "adj close": "close",
        "v": "volume", "vol": "volume", "qty": "volume",
        "volume traded": "volume", "traded volume": "volume",
    }
    return df.rename(columns=aliases)


def _read_ohlcv(path):
    """Read a parquet/CSV OHLCV file into a normalized, chronologically-sorted
    DataFrame. Returns None when missing, unreadable, empty, or lacking columns."""
    if not path or not os.path.exists(path):
        return None

    try:
        if str(path).lower().endswith(".parquet"):
            df = pd.read_parquet(path)
        else:
            df = pd.read_csv(path)
    except Exception as exc:
        print(f"  ⚠️  Could not read {os.path.basename(path)}: {exc}")
        return None

    if df is None or df.empty:
        return None

    df = _normalize_columns(df)

    date_col = next(
        (c for c in df.columns if c in ("timestamp", "date", "datetime", "time")),
        None,
    )
    if date_col is not None:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col]).sort_values(by=date_col)

    required = {"open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        print(f"  ⚠️  {os.path.basename(path)} missing columns: {sorted(missing)} "
              f"(have {sorted(df.columns)})")
        return None

    for col in required:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["close", "high", "low", "volume"])
    return df if len(df) else None


def _find_symbol_file(folder, symbol, isin=None):
    """Locate a symbol's data file. Upstox stores files as ``NSE_EQ_<ISIN>.parquet``;
    ISIN- or symbol-named parquet/CSV files are also accepted."""
    candidates = []
    if isin:
        candidates.extend([
            f"NSE_EQ_{isin}.parquet", f"NSE_EQ_{isin}.csv",
            f"{isin}.parquet", f"{isin}.csv",
        ])
    candidates.extend([f"{symbol}.parquet", f"{symbol}.csv"])

    for name in candidates:
        path = os.path.join(folder, name)
        if os.path.exists(path):
            return path
    return None


def _load_isin_map():
    """Build {symbol: ISIN} from ind_nifty500list.csv for file-name resolution."""
    if not os.path.exists(UNIVERSE_FILE):
        return {}
    try:
        df = pd.read_csv(UNIVERSE_FILE, usecols=["Symbol", "ISIN Code"])
        df.columns = [str(c).strip() for c in df.columns]
        sym_col = next((c for c in df.columns if c.lower() == "symbol"), None)
        isin_col = next((c for c in df.columns if "isin" in c.lower()), None)
        if sym_col is None or isin_col is None:
            return {}
        mapping = {}
        for sym, isin in zip(df[sym_col], df[isin_col]):
            key = str(sym).strip().upper()
            if key and pd.notna(isin):
                mapping[key] = str(isin).strip()
        return mapping
    except Exception as exc:
        print(f"  ⚠️  Could not load ISIN map from {UNIVERSE_FILE}: {exc}")
        return {}

def _ema(series, span):
    """Exponential moving average (adjust=False, min_periods=span)."""
    return series.ewm(span=span, adjust=False, min_periods=span).mean()


def _rsi(close, period=RSI_PERIOD):
    """Wilder's RSI. Returns a Series aligned to ``close`` (flat price -> 50)."""
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi_vals = 100.0 - 100.0 / (1.0 + rs)
    rsi_vals = rsi_vals.mask((avg_loss == 0.0) & (avg_gain > 0.0), 100.0)
    rsi_vals = rsi_vals.mask((avg_loss == 0.0) & (avg_gain == 0.0), 50.0)
    return rsi_vals.fillna(50.0)


def _slope_pct(series, lookback=SLOPE_LOOKBACK):
    """% change of a series over ``lookback`` bars (latest vs lookback bars ago)."""
    if len(series) <= lookback:
        return np.nan
    now = series.iloc[-1]
    then = series.iloc[-lookback - 1]
    if not (np.isfinite(now) and np.isfinite(then)) or then == 0:
        return np.nan
    return float((now - then) / then * 100.0)


def _momentum_score(rsi_val, s20, s50):
    """Blend RSI(14) and EMA slopes into a single 0-100 momentum gauge."""
    def slope_score(s):
        if not np.isfinite(s):
            return 50.0
        return float(np.clip(50.0 + s * 10.0, 0.0, 100.0))

    r = rsi_val if np.isfinite(rsi_val) else 50.0
    score = 0.5 * r + 0.3 * slope_score(s20) + 0.2 * slope_score(s50)
    return round(float(np.clip(score, 0.0, 100.0)), 1)

def _has_reversal_signature(df):
    """Detect a bottoming reversal on the most recent daily bars.

    True if ANY of the following appear in the last few bars:
      1. Long lower wick (bottom wick >= 45% of the bar's range).
      2. Liquidity sweep: the last bar undercuts the prior 5-bar low but closes
         back above it (a stop-hunt that reclaimed).
      3. Strong bullish close after a downtrend (>= 1.5% gain, close in the top
         40% of the range, while the 10-bar return is still negative).
    """
    if len(df) < 12:
        return False
    o = df["open"].astype(float)
    h = df["high"].astype(float)
    l = df["low"].astype(float)
    c = df["close"].astype(float)

    # 1) Long lower wick on any of the last 3 bars
    for i in range(-3, 0):
        rng = h.iloc[i] - l.iloc[i]
        if rng <= 0:
            continue
        lower_wick = min(o.iloc[i], c.iloc[i]) - l.iloc[i]
        if lower_wick > 0 and (lower_wick / rng) >= 0.45:
            return True

    # 2) Liquidity sweep + reclaim
    prior_low = float(l.iloc[-6:-1].min())
    if float(l.iloc[-1]) < prior_low and float(c.iloc[-1]) > prior_low:
        return True

    # 3) Strong bullish close after a downtrend
    last = df.iloc[-1]
    rng = float(last["high"]) - float(last["low"])
    if float(last["close"]) > float(last["open"]) and rng > 0:
        gain = (float(last["close"]) - float(last["open"])) / float(last["open"]) * 100.0
        ret10 = (float(c.iloc[-1]) - float(c.iloc[-11])) / float(c.iloc[-11]) * 100.0
        close_pos = (float(last["close"]) - float(last["low"])) / rng
        if gain >= 1.5 and ret10 < 0.0 and close_pos >= 0.6:
            return True

    return False


def _fresh_ema_contact(close, ema, tol=EMA_TOUCH_TOL, bars_back=EMA_CROSS_BARS):
    """True ONLY for a FRESH EMA signal — not an extended price that crossed long ago.

    Valid when either (a) price sits within ±tol of the EMA right now (tight
    proximity / bounce zone), or (b) price crossed ABOVE the EMA within the last
    ``bars_back`` bars (below the EMA ``bars_back`` bars ago, above it now).
    """
    if len(close) <= bars_back:
        return False
    now = float(close.iloc[-1])
    ema_now = float(ema.iloc[-1])
    if not (np.isfinite(now) and np.isfinite(ema_now) and ema_now > 0):
        return False
    # (a) strict proximity to the EMA
    if abs(now - ema_now) / ema_now <= tol:
        return True
    # (b) fresh cross above the EMA within the last ``bars_back`` bars
    then_close = float(close.iloc[-bars_back - 1])
    then_ema = float(ema.iloc[-bars_back - 1])
    if not (np.isfinite(then_close) and np.isfinite(then_ema)):
        return False
    return bool(now > ema_now and then_close < then_ema)


def _daily_ema_support(close, ema20, ema50):
    """Daily pullback to the 20/50 EMA with support still intact.

    Requires the trend to be holding (close >= 50 EMA * 0.98) AND a FRESH
    contact with the 20 or 50 EMA (proximity now, or a cross within the last 3
    bars). Rejects prices extended far above the moving averages.
    """
    if close is None or len(close) <= EMA_CROSS_BARS:
        return False
    now = float(close.iloc[-1])
    e50 = float(ema50.iloc[-1])
    if not (np.isfinite(now) and np.isfinite(e50) and e50 > 0):
        return False
    if now < e50 * 0.98:               # broken support => breakdown, not a pullback
        return False
    return bool(_fresh_ema_contact(close, ema20) or _fresh_ema_contact(close, ema50))


def _true_range(high, low, close):
    """True Range (max of high-low, |high-prevClose|, |low-prevClose|)."""
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr


def _adx(high, low, close, period=ADX_PERIOD):
    """Wilder's Average Directional Index. Low ADX (< 20) => no trend / sideways."""
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0), up_move, 0.0),
        index=high.index,
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0), down_move, 0.0),
        index=high.index,
    )
    tr = _true_range(high, low, close)
    atr = tr.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    plus_di = 100.0 * plus_dm.ewm(alpha=1.0 / period, adjust=False,
                                  min_periods=period).mean() / atr
    minus_di = 100.0 * minus_dm.ewm(alpha=1.0 / period, adjust=False,
                                    min_periods=period).mean() / atr
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)
    adx = dx.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    return adx.fillna(0.0)


def _bb_squeeze(close, period=BB_PERIOD, lookback=BB_SQUEEZE_LOOKBACK,
                quantile=BB_SQUEEZE_QUANTILE):
    """Bollinger Band squeeze: current band width in the bottom ``quantile``
    of the last ``lookback`` bars (tight, low-volatility compression)."""
    mid = close.rolling(period).mean()
    std = close.rolling(period).std()
    bbw = (4.0 * std) / mid                # full band width as fraction of middle
    window = bbw.dropna()
    if window.empty:
        return False
    if len(window) > lookback:
        window = window.iloc[-lookback:]
    return bool(float(bbw.iloc[-1]) <= float(window.quantile(quantile)))


def _tr_contraction(high, low, close, recent=TR_RECENT, prior=TR_PRIOR):
    """True Range contraction: recent average TR is lower than the prior window's."""
    tr = _true_range(high, low, close)
    if len(tr) < recent + prior:
        return False
    recent_avg = float(tr.iloc[-recent:].mean())
    prior_avg = float(tr.iloc[-(recent + prior):-recent].mean())
    if not (np.isfinite(recent_avg) and np.isfinite(prior_avg) and prior_avg > 0):
        return False
    return recent_avg < prior_avg


def _is_vcp(high, low):
    """Volatility Contraction Pattern: ranges are tightening.

    True when the last 3 weekly ranges are strictly descending, OR the last
    week's range has contracted to < 75% of the prior 5-week average range.
    """
    rng = (high - low).astype(float)
    if len(rng) < 6:
        return False
    last3 = rng.iloc[-3:].tolist()
    descending = last3[0] > last3[1] > last3[2] and all(v > 0 for v in last3)
    avg_prev = float(rng.iloc[-6:-1].mean())
    contracted = bool(rng.iloc[-1] < avg_prev * 0.75)
    return bool(descending or contracted)


def _is_inside_bar(high, low):
    """True when the last bar's range is fully inside the previous bar's range."""
    if len(high) < 2:
        return False
    return bool(
        float(high.iloc[-1]) <= float(high.iloc[-2])
        and float(low.iloc[-1]) >= float(low.iloc[-2])
        and float(high.iloc[-1]) > float(low.iloc[-1])
    )

def _daily_metrics(df):
    """Compute all daily-frame metrics. Returns a dict, or None if insufficient."""
    if df is None or len(df) < MIN_DAILY_BARS:
        return None

    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    vol = df["volume"].astype(float)

    last_close = float(close.iloc[-1])
    low52 = float(low.iloc[-LOOKBACK_52W:].min())
    high52 = float(high.iloc[-LOOKBACK_52W:].max())
    if not (np.isfinite(last_close) and last_close > 0
            and np.isfinite(low52) and low52 > 0
            and np.isfinite(high52) and high52 >= low52):
        return None

    rng52 = high52 - low52
    pct_above_low = (last_close - low52) / low52 * 100.0
    range_pos = (last_close - low52) / rng52 * 100.0 if rng52 > 0 else 0.0
    recent_5_low = float(low.iloc[-5:].min())
    break_prev_high = bool(last_close > float(high.iloc[-2]))
    avg_vol_10 = vol.rolling(10, min_periods=10).mean().shift(1).iloc[-1]
    vol_above_avg10 = bool(
        np.isfinite(avg_vol_10) and float(vol.iloc[-1]) > float(avg_vol_10)
    )

    ema20 = _ema(close, DAILY_EMA_FAST)
    ema50 = _ema(close, DAILY_EMA_SLOW)
    ema200 = _ema(close, DAILY_EMA_LONG)
    e20 = float(ema20.iloc[-1])
    e50 = float(ema50.iloc[-1])
    e200 = float(ema200.iloc[-1])
    prev_low = float(low.iloc[-2])
    curr_low = float(low.iloc[-1])
    prev_close = float(close.iloc[-2])
    curr_open = float(df["open"].iloc[-1])
    curr_green_and_rising = last_close > curr_open and last_close > prev_close
    strict_ema_bounce = bool(
        curr_green_and_rising
        and (
            (
                (prev_low <= float(ema20.iloc[-2]) * 1.01
                 or curr_low <= e20 * 1.01)
                and last_close > e20
                and last_close <= e20 * 1.02
            )
            or (
                (prev_low <= float(ema50.iloc[-2]) * 1.01
                 or curr_low <= e50 * 1.01)
                and last_close > e50
                and last_close <= e50 * 1.02
            )
        )
    )

    rsi_series = _rsi(close, RSI_PERIOD)
    r = float(rsi_series.iloc[-1])
    s20 = _slope_pct(ema20, SLOPE_LOOKBACK)
    s50 = _slope_pct(ema50, SLOPE_LOOKBACK)

    # "Oversold BB20 Reclaim" gauges: BB20 midline (20-period SMA) and whether
    # RSI(14) dipped into oversold territory within the last 15 trading days.
    bb20_mid_series = close.rolling(BB20_MID_PERIOD, min_periods=BB20_MID_PERIOD).mean()
    bb20_mid = float(bb20_mid_series.iloc[-1])
    rsi_dipped = bool((rsi_series.iloc[-RSI_DIP_LOOKBACK:] < RSI_OVERSOLD).any())
    strict_bb20_mid_reclaim = bool(
        np.isfinite(bb20_mid_series.iloc[-2])
        and np.isfinite(bb20_mid_series.iloc[-1])
        and close.iloc[-2] < bb20_mid_series.iloc[-2]
        and close.iloc[-1] > bb20_mid_series.iloc[-1]
        and close.iloc[-1] <= bb20_mid_series.iloc[-1] * 1.02
        and close.iloc[-1] > df["open"].iloc[-1]
    )

    # BB lower band (20-SMA - 2*std) + daily "snap" detection: the prior day's
    # low pierced the lower band, and today's close reclaimed it with a bullish
    # close while RSI is still depressed (< 40).
    bb20_std_series = close.rolling(BB20_MID_PERIOD, min_periods=BB20_MID_PERIOD).std()
    bb20_lower_series = bb20_mid_series - BB_SNAP_STD_MULT * bb20_std_series
    daily_bb20_lower = float(bb20_lower_series.iloc[-1])
    prev_low = float(low.iloc[-2])
    prev_lower = float(bb20_lower_series.iloc[-2])
    curr_open = float(df["open"].iloc[-1])
    proximity_limit = daily_bb20_lower * 1.02
    avg_vol_10_series = vol.rolling(10, min_periods=10).mean().shift(1)
    avg_vol_10 = float(avg_vol_10_series.iloc[-1])
    last_vol = float(vol.iloc[-1])
    is_daily_snap = bool(
        np.isfinite(prev_low) and np.isfinite(prev_lower) and prev_low < prev_lower
        and np.isfinite(last_close) and np.isfinite(daily_bb20_lower)
        and last_close > daily_bb20_lower and last_close <= proximity_limit
        and np.isfinite(curr_open) and last_close > curr_open
        and np.isfinite(r) and r < BB_SNAP_RSI_MAX
        and np.isfinite(last_vol) and np.isfinite(avg_vol_10)
        and last_vol > avg_vol_10)

    # "Consolidation / Flag" gauges: established uptrend + sideways contraction.
    uptrend = bool(np.isfinite(e50) and np.isfinite(e200) and e200 > 0 and e50 > e200)
    adx_series = _adx(high, low, close, ADX_PERIOD)
    adx = float(adx_series.iloc[-1])
    bb_squeeze = _bb_squeeze(close)
    tr_contract = _tr_contraction(high, low, close)
    adx_low = bool(np.isfinite(adx) and adx < ADX_FLAT)
    flag_consolidation = bool(uptrend and (adx_low or bb_squeeze or tr_contract))

    pivot_10d = high.shift(1).rolling(10, min_periods=10).max()
    pivot_value = float(pivot_10d.iloc[-1])
    prev_close = float(close.iloc[-2])
    fresh_flag_breakout = bool(
        np.isfinite(pivot_value)
        and pivot_value > 0
        and np.isfinite(prev_close)
        and prev_close < pivot_value
        and last_close > pivot_value
        and last_close <= pivot_value * 1.03
    )
    avg_vol_10_value = float(avg_vol_10_series.iloc[-1])
    vol_surge_15x = bool(
        np.isfinite(avg_vol_10_value)
        and avg_vol_10_value > 0
        and last_vol >= avg_vol_10_value * 1.5
    )

    previous_df = df.iloc[:-1]
    previous_close_series = previous_df["close"].astype(float)
    previous_high_series = previous_df["high"].astype(float)
    previous_low_series = previous_df["low"].astype(float)
    previous_adx_series = _adx(
        previous_high_series, previous_low_series, previous_close_series, ADX_PERIOD
    )
    previous_adx = float(previous_adx_series.iloc[-1])
    consolidation_yesterday = bool(
        (np.isfinite(previous_adx) and previous_adx < ADX_FLAT)
        or _bb_squeeze(previous_close_series)
        or _tr_contraction(
            previous_high_series, previous_low_series, previous_close_series
        )
    )

    # Volume dry-up: latest volume vs prior 20-day average (today excluded).
    avg20 = vol.rolling(VOL_AVG_WINDOW, min_periods=VOL_AVG_WINDOW).mean().shift(1)
    avg = float(avg20.iloc[-1]) if len(avg20) and np.isfinite(avg20.iloc[-1]) else np.nan
    latest_vol = float(vol.iloc[-1])
    vol_ratio = latest_vol / avg if (np.isfinite(avg) and avg > 0) else np.nan
    vol_dry = bool(np.isfinite(vol_ratio) and vol_ratio < VOL_DRY_THRESH)

    ts_col = "timestamp" if "timestamp" in df.columns else None
    last_date = (pd.Timestamp(df[ts_col].iloc[-1]).date().isoformat()
                 if ts_col else "")

    return {
        "last_close": last_close,
        "low52": low52,
        "high52": high52,
        "pct_above_low": pct_above_low,
        "range_pos": range_pos,
        "near_low": pct_above_low <= LOW_PROXIMITY_PCT,
        "reversal": _has_reversal_signature(df),
        "recent_5_low": recent_5_low,
        "break_prev_high": break_prev_high,
        "vol_above_avg10": vol_above_avg10,
        "ema_support": _daily_ema_support(close, ema20, ema50),
        "strict_ema_bounce": strict_ema_bounce,
        "ema20": e20,
        "ema50": e50,
        "ema200": e200,
        "uptrend": uptrend,
        "adx": adx,
        "bb_squeeze": bb_squeeze,
        "tr_contract": tr_contract,
        "flag_consolidation": flag_consolidation,
        "pivot_10d": pivot_value,
        "fresh_flag_breakout": fresh_flag_breakout,
        "vol_surge_15x": vol_surge_15x,
        "consolidation_yesterday": consolidation_yesterday,
        "rsi": r,
        "bb20_mid": bb20_mid,
        "rsi_dipped": rsi_dipped,
        "rsi_oversold": rsi_dipped,
        "strict_bb20_mid_reclaim": strict_bb20_mid_reclaim,
        "daily_bb20_lower": daily_bb20_lower,
        "is_daily_snap": is_daily_snap,
        "s20": s20,
        "s50": s50,
        "vol_ratio": vol_ratio,
        "vol_dry": vol_dry,
        "momentum": _momentum_score(r, s20, s50),
        "last_date": last_date,
    }


def _weekly_metrics(df):
    """Compute weekly-frame metrics. Returns a dict, or None if insufficient."""
    if df is None or len(df) < MIN_WEEKLY_BARS:
        return None

    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    vol = df["volume"].astype(float)

    ema20 = _ema(close, WEEKLY_EMA_FAST)
    ema50 = _ema(close, WEEKLY_EMA_SLOW)

    last_close = float(close.iloc[-1])
    last_ema20 = float(ema20.iloc[-1])
    last_ema50 = float(ema50.iloc[-1])

    if not all(np.isfinite(x) for x in (last_close, last_ema20, last_ema50)):
        return None

    # FRESH signal only: a cross above the weekly 20 EMA within the last 3 bars,
    # or price sitting within ±2% of it. Rejects prices extended far above the
    # EMA that crossed it months ago.
    proximity = bool(abs(last_close - last_ema20) / last_ema20 <= EMA_TOUCH_TOL)
    ema_contact = _fresh_ema_contact(close, ema20)
    trend_ok = bool(last_close > last_ema50)
    vcp = _is_vcp(high, low)
    inside = _is_inside_bar(high, low)

    signal = "Touch" if proximity else ("Cross" if ema_contact else "")

    # Weekly volume ratio: latest weekly volume vs prior 10-week average
    # (latest week excluded from the average).
    avg10 = vol.rolling(WEEKLY_VOL_AVG, min_periods=WEEKLY_VOL_AVG).mean().shift(1)
    avg_week = float(avg10.iloc[-1]) if len(avg10) and np.isfinite(avg10.iloc[-1]) else np.nan
    latest_week_vol = float(vol.iloc[-1])
    weekly_vol_ratio = (latest_week_vol / avg_week
                        if (np.isfinite(avg_week) and avg_week > 0) else np.nan)

    # MTF "Mega Reversal" weekly indicators: BB20 midline (20-week SMA) and
    # RSI(14) on weekly closes. The series stay aligned to week_df rows for the
    # later daily forward-fill alignment step.
    bb20_series = close.rolling(WEEKLY_BB20_PERIOD, min_periods=WEEKLY_BB20_PERIOD).mean()
    rsi_series = _rsi(close, RSI_PERIOD)
    weekly_bb20_mid = float(bb20_series.iloc[-1])
    weekly_rsi_dipped = bool((rsi_series.iloc[-WEEKLY_RSI_DIP_WEEKS:] < RSI_OVERSOLD).any())

    # Weekly BB lower band + weekly "snap" flag (prior week's low pierced the
    # lower band, current week's close reclaimed it while weekly RSI < 40). The
    # boolean series is aligned onto the daily frame later via merge_asof.
    bb20_std_series = close.rolling(WEEKLY_BB20_PERIOD, min_periods=WEEKLY_BB20_PERIOD).std()
    bb20_lower_series = bb20_series - BB_SNAP_STD_MULT * bb20_std_series
    weekly_snap = (
        (low.shift(1) < bb20_lower_series.shift(1))
        & (close > bb20_lower_series)
        & (close <= bb20_lower_series * 1.03)
        & (rsi_series < BB_SNAP_RSI_MAX)
        & (vol > avg10)
    ).astype(bool).fillna(False)
    weekly_bb20_lower = float(bb20_lower_series.iloc[-1])

    return {
        "last_close": last_close,
        "ema20": last_ema20,
        "ema50": last_ema50,
        "ema_contact": ema_contact,
        "trend_ok": trend_ok,
        "vcp": vcp,
        "inside": inside,
        "signal": signal,
        "weekly_vol_ratio": weekly_vol_ratio,
        "weekly_bb20_mid": weekly_bb20_mid,
        "weekly_rsi_dipped": weekly_rsi_dipped,
        "weekly_bb20_series": bb20_series,
        "weekly_bb20_lower": weekly_bb20_lower,
        "weekly_snap_series": weekly_snap,
    }

def _align_weekly_to_daily(day_df, week_df, weekly_values):
    """Forward-fill a weekly indicator onto the daily timeline (as-of join).

    Each daily bar receives the most recent weekly value whose timestamp is on or
    before that day, so a daily evaluation never sees a weekly bar that closed
    after it (no look-ahead bias). ``weekly_values`` is a Series aligned to
    ``week_df`` rows. Returns a Series aligned to ``day_df`` rows, or None.
    """
    if weekly_values is None or len(weekly_values) != len(week_df):
        return None
    if "timestamp" not in day_df.columns or "timestamp" not in week_df.columns:
        return None
    try:
        daily = (pd.DataFrame({"ts": pd.to_datetime(day_df["timestamp"], errors="coerce").values})
                   .sort_values("ts").reset_index(drop=True))
        weekly = (pd.DataFrame({
                      "ts": pd.to_datetime(week_df["timestamp"], errors="coerce").values,
                      "val": pd.to_numeric(weekly_values, errors="coerce").values,
                  }).dropna(subset=["ts", "val"]).sort_values("ts").reset_index(drop=True))
        if weekly.empty or daily.empty:
            return None
        return pd.merge_asof(daily, weekly, on="ts", direction="backward")["val"]
    except Exception:
        return None


def _classify(d, w):
    """Assign Setup_Category using priority Bottom Fishing > Oversold BB20 Reclaim
    > Mega Reversal (Weekly BB20) > BB Lower Band Snap (Weekly)
    > BB Lower Band Snap (Daily) > Mid-Range Bounce > VCP Breakout
    > Consolidation / Flag."""
    if (d["near_low"] and d["reversal"] and d["break_prev_high"]
            and d["last_close"] <= d["recent_5_low"] * 1.03
            and d["vol_above_avg10"]):
        return "Bottom Fishing"
    elif (d["rsi_oversold"] and d["strict_bb20_mid_reclaim"]
          and d["vol_above_avg10"]):
        return "Oversold BB20 Reclaim"
    if (w["weekly_rsi_dipped"]
            and d["rsi"] > RSI_RECLAIM
            and np.isfinite(w["aligned_weekly_bb20_mid"])
            and d["last_close"] >= w["aligned_weekly_bb20_mid"]
            and np.isfinite(w["weekly_vol_ratio"])
            and w["weekly_vol_ratio"] < MEGA_WEEKLY_VOL_DRY):
        return "Mega Reversal (Weekly BB20)"
    if w["is_weekly_snap"]:
        return "BB Lower Band Snap (Weekly)"
    if d["is_daily_snap"]:
        return "BB Lower Band Snap (Daily)"
    elif (d["range_pos"] >= MID_RANGE_LO and d["range_pos"] <= MID_RANGE_HI
          and d["strict_ema_bounce"] and d["vol_above_avg10"]):
        return "Mid-Range Bounce"
    if w["ema_contact"] and w["trend_ok"] and (w["vcp"] or w["inside"]):
        return "VCP Breakout"
    if (d["uptrend"]
            and (d["flag_consolidation"] or d["consolidation_yesterday"])
            and d["fresh_flag_breakout"]
            and d["vol_surge_15x"]):
        return "Consolidation / Flag"
    return None


def _fmt(x, nd=2):
    """Round finite floats; pass None/NaN through as None for clean CSV output."""
    try:
        if x is None or not np.isfinite(x):
            return None
        return round(float(x), nd)
    except (TypeError, ValueError):
        return None


def main():
    print("=" * 78)
    print("  MARKET REGIME & SETUP SCANNER  —  trend_ready.py")
    print("=" * 78)

    if not _check_parquet_support():
        print("❌ pandas cannot read parquet. Install pyarrow or fastparquet.")
        return 1

    ap = argparse.ArgumentParser(description="Market Regime & Setup Scanner")
    ap.add_argument("--symbols", type=str, default="",
                    help="comma-separated subset, e.g. RELIANCE,TCS,ACC")
    ap.add_argument("--limit", type=int, default=0, help="only first N candidates")
    args = ap.parse_args()

    if not os.path.exists(CANDIDATES_FILE):
        print(f"❌ Candidates file not found: {CANDIDATES_FILE}")
        print("   Run funda_data.py first to generate nifty500_swing_candidates.csv.")
        return 1

    try:
        cand = pd.read_csv(CANDIDATES_FILE)
    except Exception as exc:
        print(f"❌ Failed to read {CANDIDATES_FILE}: {exc}")
        return 1
    cand.columns = [str(c).strip() for c in cand.columns]
    sym_col = next((c for c in cand.columns if c.lower() == "symbol"), None)
    if sym_col is None:
        sym_col = cand.columns[0]

    sym_series = cand[sym_col].astype(str).str.strip().str.upper()
    name_map = (dict(zip(sym_series, cand["name"].astype(str).str.strip()))
                if "name" in cand.columns else {})
    industry_map = (dict(zip(sym_series, cand["industry"].astype(str).str.strip()))
                    if "industry" in cand.columns else {})
    price_map = (dict(zip(sym_series, cand["current_price"]))
                 if "current_price" in cand.columns else {})

    symbols = list(dict.fromkeys(
        cand[sym_col].astype(str).str.strip().str.upper().tolist()))
    symbols = [s for s in symbols if s]

    if args.symbols:
        want = {s.strip().upper() for s in args.symbols.split(",") if s.strip()}
        symbols = [s for s in symbols if s in want]
    if args.limit:
        symbols = symbols[: args.limit]

    isin_map = _load_isin_map()
    print(f"📊 Candidates : {len(symbols)}  (from {os.path.basename(CANDIDATES_FILE)})")
    print(f"   ISIN map   : {len(isin_map)} symbols\n")

    results = []
    counts = {
        "no_file": 0, "insufficient": 0, "no_setup": 0, "error": 0,
        "Bottom Fishing": 0, "Oversold BB20 Reclaim": 0,
        "Mega Reversal (Weekly BB20)": 0, "BB Lower Band Snap (Weekly)": 0,
        "BB Lower Band Snap (Daily)": 0, "Mid-Range Bounce": 0,
        "VCP Breakout": 0, "Consolidation / Flag": 0,
    }

    for i, symbol in enumerate(symbols, 1):
        isin = isin_map.get(symbol)
        day_file = _find_symbol_file(DAY_DIR, symbol, isin)
        week_file = _find_symbol_file(WEEK_DIR, symbol, isin)

        if not day_file or not week_file:
            counts["no_file"] += 1
            continue

        try:
            day_df = _read_ohlcv(day_file)
            week_df = _read_ohlcv(week_file)
            if day_df is None or week_df is None:
                counts["insufficient"] += 1
                continue

            d = _daily_metrics(day_df)
            w = _weekly_metrics(week_df)
            if d is None or w is None:
                counts["insufficient"] += 1
                continue

            # Align the weekly BB20 midline onto the daily timeline (as-of/ffill)
            # and capture the current aligned value for the MTF reversal bucket.
            aligned_bb20 = _align_weekly_to_daily(day_df, week_df,
                                                  w.get("weekly_bb20_series"))
            w["aligned_weekly_bb20_mid"] = (
                float(aligned_bb20.iloc[-1])
                if aligned_bb20 is not None and len(aligned_bb20)
                and np.isfinite(aligned_bb20.iloc[-1]) else np.nan)

            # Align the weekly "snap" flag onto the daily timeline (as-of/ffill);
            # the current daily row inherits the most recent weekly snap state.
            aligned_snap = _align_weekly_to_daily(day_df, week_df,
                                                  w.get("weekly_snap_series"))
            w["is_weekly_snap"] = bool(
                aligned_snap is not None and len(aligned_snap)
                and float(aligned_snap.iloc[-1]) > 0.5)

            category = _classify(d, w)
            if category is None:
                counts["no_setup"] += 1
                continue

            row = {
                "symbol": symbol,
                "name": name_map.get(symbol, ""),
                "industry": industry_map.get(symbol, ""),
                "Setup_Category": category,
                "funda_current_price": _fmt(price_map.get(symbol)),
                "last_close": _fmt(d["last_close"]),
                "last_date": d["last_date"],
                "pct_above_52w_low": _fmt(d["pct_above_low"]),
                "range_position_pct": _fmt(d["range_pos"]),
                "near_52w_low": bool(d["near_low"]),
                "reversal_signature": bool(d["reversal"]),
                "daily_ema_support": bool(d["ema_support"]),
                "daily_ema20": _fmt(d["ema20"]),
                "daily_ema50": _fmt(d["ema50"]),
                "daily_ema200": _fmt(d["ema200"]),
                "uptrend_50_200": bool(d["uptrend"]),
                "adx_14": _fmt(d["adx"], 1),
                "bb_squeeze": bool(d["bb_squeeze"]),
                "tr_contraction": bool(d["tr_contract"]),
                "flag_consolidation": bool(d["flag_consolidation"]),
                "weekly_ema20_signal": w["signal"],
                "weekly_ema20": _fmt(w["ema20"]),
                "weekly_ema50": _fmt(w["ema50"]),
                "vcp": bool(w["vcp"]),
                "inside_bar": bool(w["inside"]),
                "Volume_Dry_Ratio": _fmt(d["vol_ratio"]),
                "Volume_Dry_Up": bool(d["vol_dry"]),
                "Weekly_Vol_Ratio": _fmt(w["weekly_vol_ratio"]),
                "RSI_14": _fmt(d["rsi"], 1),
                "EMA20_slope_5d_pct": _fmt(d["s20"]),
                "EMA50_slope_5d_pct": _fmt(d["s50"]),
                "Momentum_Score": d["momentum"],
            }
            results.append(row)
            counts[category] += 1
            print(f"  ✅ {symbol:12s} → {category}")
        except Exception as exc:  # noqa: BLE001
            counts["error"] += 1
            print(f"  ⚠️  {symbol}: {exc}")

    out = pd.DataFrame(results, columns=OUTPUT_COLUMNS)
    if not out.empty:
        out["_order"] = out["Setup_Category"].map(
            {c: i for i, c in enumerate(CATEGORY_ORDER)})
        out = (out.sort_values(["_order", "Momentum_Score"], ascending=[True, False])
                  .drop(columns="_order")
                  .reset_index(drop=True))

    processed = len(results) + counts["no_setup"]

    # ── summary ────────────────────────────────────────────────────────────────
    print("\n" + "─" * 78)
    print("  SUMMARY")
    print(f"    candidates           : {len(symbols)}")
    print(f"    data file missing    : {counts['no_file']}")
    print(f"    insufficient history : {counts['insufficient']}")
    print(f"    errors               : {counts['error']}")
    print(f"    processed (both frames): {processed}")
    print(f"    no setup matched     : {counts['no_setup']}")
    print("    ── setups ──")
    for cat in CATEGORY_ORDER:
        print(f"    {cat:22s}: {counts[cat]}")
    print(f"    total setups         : {len(out)}")
    if not out.empty:
        dry = int(out["Volume_Dry_Up"].sum())
        print(f"    volume dry-up flagged: {dry}")
    print("─" * 78)

    out.to_csv(OUTPUT_FILE, index=False)

    if out.empty:
        print("\n❌ No setups matched. Loosen thresholds in the CONFIG section "
              "if the market regime is weak.")
        print(f"📁 (empty) {OUTPUT_FILE} written with headers only.")
    else:
        print(f"\n📁 {OUTPUT_FILE}  ({len(out)} setups)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
