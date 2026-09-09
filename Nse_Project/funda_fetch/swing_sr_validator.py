"""
SWING S/R VALIDATOR - Multi-Timeframe Support/Resistance + Fundamental Gate.

Pipeline:
  1. Read fundamental swing candidates  (nifty500_swing_candidates.csv)
  2. Load Daily + Weekly OHLCV  (NSE_EQ_<ISIN>.parquet  from Upstox Data)
  3. Weekly S/R: swing highs/lows + classic pivot points
  4. Daily trend: Price > 20EMA > 50EMA, RSI(14) 55-75, volume surge (>=1.5x)
  5. Relative momentum: Sector RSI(14) > NIFTY 50 RSI(14)
  6. Risk/Reward: SL (swing low / 1.5*ATR) -> target (next weekly resistance),
     accept only R:R >= 1:2

Outputs:
  * validated_swing_setups.csv
  * validated_swing_setups.json  (Queue Ticks / Performance renderer friendly)

RUN:
  python swing_sr_validator.py
  python swing_sr_validator.py --symbols GLENMARK,IPCALAB

NEED: pip install pandas numpy pyarrow yfinance
"""

import argparse
import json
import os
import sys
import warnings
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

# Windows console hardening: emoji/box chars must not crash a cp1252 console.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).resolve().parent

# Inputs
CANDIDATES_CSV = BASE_DIR / "nifty500_swing_candidates.csv"
UNIVERSE_CSV = BASE_DIR / "ind_nifty500list.csv"

# Historical data locations (stocks only — index data is fetched via yfinance).
DAY_DATA_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox Data\Day Data"
WEEK_DATA_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox Data\Weeks Data"

# Outputs
OUTPUT_CSV = BASE_DIR / "validated_swing_setups.csv"
OUTPUT_JSON = BASE_DIR / "validated_swing_setups.json"

# ── Technical parameters ────────────────────────────────────────────────────────
EMA_FAST = 20
EMA_SLOW = 50
RSI_PERIOD = 14
RSI_MIN = 50.0
RSI_MAX = 80.0
VOL_SURGE_MULT = 1.2          # latest volume >= 1.2x 20-day average
VOL_AVG_PERIOD = 20
ATR_PERIOD = 14
ATR_SL_MULT = 1.5             # 1.5x ATR stop-loss cap
MIN_RR = 2.0                  # >= 1:2
DEFAULT_RR_TARGET = 2.5       # fallback target multiple when no resistance found

MIN_DAILY_BARS = 60
MIN_WEEKLY_BARS = 30

SWING_LOOKBACK = 3            # bars each side to confirm a swing high/low
RECENT_SWING_LOOKBACK = 20    # window to find the "recent swing low" for SL
DAILY_RESISTANCE_LOOKBACK = 20  # window for the daily breakout resistance
WEEKLY_LEVEL_LOOKBACK = 52    # how many weekly bars back to collect swing levels

SL_BUFFER = 0.001             # small % buffer below the structural swing low
BREAKOUT_ATR_CUSHION = 0.2    # breakout must clear resistance by >= 0.2*ATR

# Relative momentum gate: a stock is selected only when its sector's RSI(14) is
# strictly above the NIFTY 50 RSI(14), i.e. the sector is outperforming the
# broader market, regardless of the market's absolute trend.

# Number of daily bars fetched from yfinance for the relative-momentum (RSI)
# comparison (RSI(14) needs ~15 bars; 50 keeps it current without clutter).
INDEX_LOOKBACK_DAYS = 50

# ── Industry -> Nifty sector index mapping (NSE industry names) ─────────────────
# Industries mapped to None have no direct Nifty sector index; their alignment
# check falls back to NIFTY 50 (plus a proxy of the industry itself).
INDUSTRY_TO_SECTOR_INDEX = {
    "Financial Services": "NIFTY BANK",
    "Information Technology": "NIFTY IT",
    "Healthcare": "NIFTY PHARMA",
    "Automobile and Auto Components": "NIFTY AUTO",
    "Fast Moving Consumer Goods": "NIFTY FMCG",
    "Metals & Mining": "NIFTY METAL",
    "Realty": "NIFTY REALTY",
    "Power": "NIFTY ENERGY",
    "Oil Gas & Consumable Fuels": "NIFTY ENERGY",
    "Consumer Durables": "NIFTY CONSUMER DURABLES",
    "Capital Goods": "NIFTY INFRA",
    "Construction": "NIFTY INFRA",
    "Construction Materials": "NIFTY INFRA",
    "Media Entertainment & Publication": "NIFTY MEDIA",
    # No direct Nifty sector index for these:
    "Chemicals": None,
    "Consumer Services": None,
    "Services": None,
    "Telecommunication": None,
    "Textiles": None,
    "Diversified": None,
}

# Index name -> yfinance ticker (NSE indices). Fetched live via yf.download().
INDEX_TO_YF_SYMBOL = {
    "NIFTY 50": "^NSEI",
    "NIFTY BANK": "^NSEBANK",
    "NIFTY IT": "^CNXIT",
    "NIFTY PHARMA": "^CNXPHARMA",
    "NIFTY AUTO": "^CNXAUTO",
    "NIFTY FMCG": "^CNXFMCG",
    "NIFTY METAL": "^CNXMETAL",
    "NIFTY REALTY": "^CNXREALTY",
    "NIFTY ENERGY": "^CNXENERGY",
    "NIFTY INFRA": "^CNXINFRA",
    "NIFTY MEDIA": "^CNXMEDIA",
    "NIFTY CONSUMER DURABLES": "^CNXCONSUMPTION",
}

# ════════════════════════════════════════════════════════════════════════════════
# LIGHTWEIGHT LOGGING
# ════════════════════════════════════════════════════════════════════════════════

def _log(msg: str) -> None:
    print(msg)


def _warn(msg: str) -> None:
    print(f"  ⚠️  {msg}")


# ════════════════════════════════════════════════════════════════════════════════
# UNIVERSE + CANDIDATES LOADING
# ════════════════════════════════════════════════════════════════════════════════

def load_universe() -> pd.DataFrame:
    """Load the Nifty-500 universe list (Company Name, Industry, Symbol, ISIN)."""
    if not UNIVERSE_CSV.exists():
        raise FileNotFoundError(f"Universe file not found: {UNIVERSE_CSV}")

    df = pd.read_csv(UNIVERSE_CSV)
    df.columns = [str(c).strip() for c in df.columns]

    sym_col = next((c for c in df.columns if c.lower() == "symbol"), None)
    isin_col = next((c for c in df.columns if "isin" in c.lower()), None)
    ind_col = next((c for c in df.columns if c.lower() == "industry"), None)

    if sym_col is None:
        raise ValueError(f"Universe CSV has no 'Symbol' column. Columns: {list(df.columns)}")
    if isin_col is None:
        raise ValueError(f"Universe CSV has no 'ISIN' column. Columns: {list(df.columns)}")

    out = pd.DataFrame({
        "symbol": df[sym_col].astype(str).str.strip().str.upper(),
        "isin": df[isin_col].astype(str).str.strip(),
        "industry": df[ind_col].astype(str).str.strip() if ind_col else "Unknown",
    })
    return out.drop_duplicates(subset=["symbol"]).set_index("symbol")


def load_candidates() -> pd.DataFrame:
    """Load the fundamental swing candidates CSV."""
    if not CANDIDATES_CSV.exists():
        raise FileNotFoundError(
            f"Candidates file not found: {CANDIDATES_CSV}\n"
            f"Run funda_data.py first to generate it."
        )

    df = pd.read_csv(CANDIDATES_CSV)
    df.columns = [str(c).strip() for c in df.columns]

    sym_col = next((c for c in df.columns if c.lower() == "symbol"), None)
    if sym_col is None:
        raise ValueError(f"Candidates CSV has no 'symbol' column. Columns: {list(df.columns)}")

    df["symbol"] = df[sym_col].astype(str).str.strip().str.upper()
    return df.drop_duplicates(subset=["symbol"])

# ════════════════════════════════════════════════════════════════════════════════
# OHLCV DATA LOADER
# ════════════════════════════════════════════════════════════════════════════════

def _read_ohlcv(path, need_volume: bool = True):
    """Read a Parquet/CSV into a datetime-indexed OHLCV DataFrame, or None.

    Handles both ``Timestamp`` and ``Datetime``-style columns and normalises the
    column names to ``Open/High/Low/Close/Volume``.
    """
    if path is None:
        return None
    try:
        if str(path).lower().endswith(".parquet"):
            df = pd.read_parquet(path)
        else:
            df = pd.read_csv(path)
    except Exception as exc:
        _warn(f"Unreadable data file {path}: {exc}")
        return None

    if df is None or df.empty:
        return None

    if not pd.api.types.is_datetime64_any_dtype(df.index):
        dcol = next(
            (c for c in df.columns if "date" in str(c).lower() or "time" in str(c).lower()),
            None,
        )
        if dcol is None:
            return None
        df[dcol] = pd.to_datetime(df[dcol], errors="coerce")
        df = df.dropna(subset=[dcol]).set_index(dcol)

    df.columns = [str(c).strip().split("(")[0].strip().lower() for c in df.columns]
    rename = {
        "open": "Open", "high": "High", "low": "Low",
        "close": "Close", "volume": "Volume",
    }
    df = df.rename(columns=rename)

    need = ["Open", "High", "Low", "Close"] + (["Volume"] if need_volume else [])
    if not all(c in df.columns for c in need):
        return None

    df = df[need].apply(pd.to_numeric, errors="coerce")
    df = df.dropna(subset=["Close"] + (["Volume"] if need_volume else []))

    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    df = df[~df.index.duplicated(keep="first")].sort_index()
    return df if len(df) else None


def _find_symbol_file(folder, symbol: str, isin: str):
    """Locate a symbol's data file in ``folder`` (ISIN naming first)."""
    cands = []
    if isin:
        cands.append(os.path.join(folder, f"NSE_EQ_{isin}.parquet"))
        cands.append(os.path.join(folder, f"{isin}.parquet"))
    cands.append(os.path.join(folder, f"{symbol}.parquet"))
    cands.append(os.path.join(folder, f"{symbol}.csv"))
    for p in cands:
        if os.path.exists(p):
            return p
    return None


def load_symbol_daily_weekly(symbol: str, isin: str):
    """Load daily + weekly DataFrames for a symbol; either may be None.

    Weekly falls back to resampling the daily frame when the weekly file is
    missing, stale, or ends on an incomplete week.
    """
    day_path = _find_symbol_file(DAY_DATA_DIR, symbol, isin)
    daily = _read_ohlcv(day_path, need_volume=True) if day_path else None
    if daily is None or len(daily) < MIN_DAILY_BARS:
        return None, None

    week_path = _find_symbol_file(WEEK_DATA_DIR, symbol, isin)
    weekly = _read_ohlcv(week_path, need_volume=False) if week_path else None

    if weekly is not None:
        while len(weekly) and weekly.index[-1] > daily.index[-1]:
            weekly = weekly.iloc[:-1]
        if len(weekly) and (daily.index[-1] - weekly.index[-1]).days < 4:
            weekly = weekly.iloc[:-1]
        if len(weekly) < MIN_WEEKLY_BARS or (daily.index[-1] - weekly.index[-1]).days > 10:
            weekly = None

    if weekly is None:
        weekly = daily.resample("W-FRI").agg({
            "Open": "first", "High": "max", "Low": "min", "Close": "last",
        }).dropna()

    return daily, weekly

# ════════════════════════════════════════════════════════════════════════════════
# INDEX DATA LOADER  (NIFTY 50 + sector indices via yfinance)
# ════════════════════════════════════════════════════════════════════════════════

def fetch_index_close(symbol: str):
    """Fetch the last ``INDEX_LOOKBACK_DAYS`` daily closes for a yfinance symbol.

    Returns a datetime-indexed pandas Series of closes, or None on any failure.
    Handles both flat and MultiIndex yfinance column layouts.
    """
    try:
        data = yf.download(
            symbol, period="3mo", interval="1d",
            progress=False, auto_adjust=False,
        )
    except Exception as exc:
        _warn(f"yfinance download failed for {symbol}: {exc}")
        return None

    if data is None or data.empty:
        return None

    close = None
    if isinstance(data.columns, pd.MultiIndex):
        for col in data.columns:
            if str(col[0]).lower() == "close":
                close = data[col]
                break
    else:
        for col in data.columns:
            if str(col).lower() == "close":
                close = data[col]
                break

    if close is None:
        return None

    close = close.dropna()
    if close.empty:
        return None
    if not pd.api.types.is_datetime64_any_dtype(close.index):
        close.index = pd.to_datetime(close.index, errors="coerce")
        close = close.dropna()
    close = close[~close.index.duplicated(keep="last")].sort_index()
    if len(close) > INDEX_LOOKBACK_DAYS:
        close = close.iloc[-INDEX_LOOKBACK_DAYS:]
    return close


def load_index_daily(index_name: str):
    """Fetch a DAILY close series for an index via yfinance.

    Returns a dict:
        {"available", "close", "above_ema", "rsi", "source", "path"}
    """
    symbol = INDEX_TO_YF_SYMBOL.get(index_name)
    if not symbol:
        return {"available": False, "close": None, "above_ema": None, "rsi": None,
                "source": None, "path": None}

    close = fetch_index_close(symbol)
    if close is None or len(close) < EMA_FAST + 1:
        _warn(f"{index_name} ({symbol}) yfinance data unavailable/insufficient.")
        return {"available": False, "close": None, "above_ema": None, "rsi": None,
                "source": None, "path": None}

    above = _close_above_ema(close, EMA_FAST)
    rsi_value = float(rsi(close, RSI_PERIOD).iloc[-1])
    return {"available": True, "close": close, "above_ema": above, "rsi": rsi_value,
            "source": "yfinance", "path": symbol}


# ════════════════════════════════════════════════════════════════════════════════
# INDICATORS  (pure pandas/numpy — no external TA dependency)
# ════════════════════════════════════════════════════════════════════════════════

def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False, min_periods=period).mean()


def rsi(close: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    out = 100.0 - (100.0 / (1.0 + rs))
    return out.fillna(50.0)


def atr(df: pd.DataFrame, period: int = ATR_PERIOD) -> pd.Series:
    high, low, close = df["High"], df["Low"], df["Close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def swing_points(df: pd.DataFrame, lookback: int = SWING_LOOKBACK):
    """Return (swing_highs, swing_lows) as lists of (index, price)."""
    highs, lows = [], []
    h, l = df["High"].values, df["Low"].values
    n = len(df)
    for i in range(lookback, n - lookback):
        if h[i] == max(h[i - lookback:i + lookback + 1]):
            highs.append((df.index[i], h[i]))
        if l[i] == min(l[i - lookback:i + lookback + 1]):
            lows.append((df.index[i], l[i]))
    return highs, lows


def weekly_pivot_levels(weekly: pd.DataFrame) -> dict:
    """Classic pivot levels from the last COMPLETED weekly bar."""
    last = weekly.iloc[-1]
    h, l, c = float(last["High"]), float(last["Low"]), float(last["Close"])
    p = (h + l + c) / 3.0
    return {
        "P": p,
        "R1": 2 * p - l, "S1": 2 * p - h,
        "R2": p + (h - l), "S2": p - (h - l),
        "R3": h + 2 * (p - l), "S3": l - 2 * (h - p),
    }


def _close_above_ema(close: pd.Series, period: int):
    """Return True/False/None for ``last close > EMA(period)``."""
    close = close.dropna()
    if len(close) < period:
        return None
    e = ema(close, period).iloc[-1]
    if pd.isna(e):
        return None
    return bool(close.iloc[-1] > e)


def detect_bullish_fvg(daily: pd.DataFrame):
    """Detect a recent 3-candle bullish Fair Value Gap (Low[i] > High[i-2])."""
    lows, highs = daily["Low"].values, daily["High"].values
    n = len(daily)
    for i in range(2, n):
        if lows[i] > highs[i - 2]:
            return True, daily.index[i], (highs[i - 2], lows[i])
    return False, None, None

# ════════════════════════════════════════════════════════════════════════════════
# CANDIDATE VALIDATION HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def _first_weekly_resistance(weekly: pd.DataFrame, entry: float):
    """Next major weekly resistance strictly above ``entry`` (pivots + swing highs)."""
    pivots = weekly_pivot_levels(weekly)
    levels = [pivots["R1"], pivots["R2"], pivots["R3"]]

    highs, _ = swing_points(weekly, SWING_LOOKBACK)
    for _ts, price in highs[-WEEKLY_LEVEL_LOOKBACK:]:
        levels.append(price)

    above = sorted({round(float(x), 2) for x in levels if x > entry * 1.005})
    if above:
        return above[0]
    return None


def _recent_swing_low(daily: pd.DataFrame) -> float:
    """Lowest confirmed swing low within the recent window (structural support)."""
    _highs, lows = swing_points(daily, SWING_LOOKBACK)
    recent_lows = [price for _ts, price in lows[-RECENT_SWING_LOOKBACK:]]
    if recent_lows:
        return min(recent_lows)
    return float(daily["Low"].iloc[-RECENT_SWING_LOOKBACK:].min())

# ════════════════════════════════════════════════════════════════════════════════
# CANDIDATE VALIDATION
# ════════════════════════════════════════════════════════════════════════════════

def validate_symbol(symbol: str, candidate_row: pd.Series, daily: pd.DataFrame,
                    weekly: pd.DataFrame, market_data: dict, sector_data: dict,
                    sector_index_name):
    """Run the full multi-timeframe + fundamental + relative-momentum gate.

    Returns ``(result_dict_or_None, reject_reason_or_None)``.
    """
    close = float(daily["Close"].iloc[-1])
    ema20 = float(ema(daily["Close"], EMA_FAST).iloc[-1])
    ema50 = float(ema(daily["Close"], EMA_SLOW).iloc[-1])

    # ── 1. Daily trend: Price > 20EMA > 50EMA ─────────────────────────────────
    if not (close > ema20 > ema50):
        return None, "trend_not_bullish"

    # ── 2. RSI(14) 55-75 ──────────────────────────────────────────────────────
    rsi_now = float(rsi(daily["Close"], RSI_PERIOD).iloc[-1])
    if not (RSI_MIN <= rsi_now <= RSI_MAX):
        return None, "rsi_out_of_band"

    # ── 3. Volume surge: latest >= 1.5x 20-day avg ────────────────────────────
    avg_vol = float(daily["Volume"].rolling(VOL_AVG_PERIOD).mean().iloc[-1])
    last_vol = float(daily["Volume"].iloc[-1])
    if avg_vol <= 0 or last_vol < VOL_SURGE_MULT * avg_vol:
        return None, "no_volume_surge"
    volume_ratio = last_vol / avg_vol if avg_vol else 0.0

    # ── 4. Breakout / retest logic ────────────────────────────────────────────
    daily_resistance = float(daily["High"].iloc[-(DAILY_RESISTANCE_LOOKBACK + 1):-1].max())
    atr_now = float(atr(daily, ATR_PERIOD).iloc[-1])
    breakout = close > daily_resistance + BREAKOUT_ATR_CUSHION * atr_now

    _whighs, wlows = swing_points(weekly, SWING_LOOKBACK)
    weekly_supports = [float(x[1]) for x in wlows if x[1] < close]
    retest_support = bool(weekly_supports) and \
        any(abs(close - s) / s <= 0.02 for s in weekly_supports)

    if not (breakout or retest_support):
        return None, "no_breakout_or_retest"

    # ── 5. Sector vs. market relative momentum (RSI) ───────────────────────────────
    nifty_rsi = market_data.get("rsi")
    sector_rsi = sector_data.get("rsi")

    if sector_index_name is None:
        return None, "sector_index_unavailable"
    if nifty_rsi is None:
        return None, "nifty_rsi_missing"
    if sector_rsi is None:
        return None, "sector_rsi_missing"
    if not (sector_rsi > nifty_rsi):
        return None, "sector_not_outperforming"

    reasons = ["Sector Outperforming Nifty (RSI)"]

    # ── 6. Stop loss / target / R:R ───────────────────────────────────────────
    swing_low = _recent_swing_low(daily)
    structural_risk = close - swing_low if swing_low < close else None
    atr_risk = ATR_SL_MULT * atr_now

    if structural_risk is not None and structural_risk > 0:
        risk = min(structural_risk, atr_risk)
        stop_type = "swing_low" if structural_risk <= atr_risk else "atr"
    else:
        risk = atr_risk
        stop_type = "atr"

    stop_loss = close - risk
    if risk <= 0:
        return None, "bad_risk"

    target = _first_weekly_resistance(weekly, close)
    if target is None:
        target = close + DEFAULT_RR_TARGET * risk

    rr = (target - close) / risk
    if rr < MIN_RR:
        return None, "low_rr"

    # ── Assemble reasons in a human-friendly order ────────────────────────────
    reasons += [
        "Price > 20EMA > 50EMA",
        f"RSI {rsi_now:.1f} Bullish Momentum",
        f"Volume Surge ({volume_ratio:.1f}x)",
    ]
    if breakout:
        reasons.append("Breakout Above Resistance")
    if retest_support:
        reasons.append("Retest of Weekly Support")

    fvg_hit, _fvg_date, _fvg_zone = detect_bullish_fvg(daily)
    if fvg_hit:
        reasons.append("Bullish FVG")

    weekly_bias = "BULLISH" if weekly["Close"].iloc[-1] > ema(weekly["Close"], 20).iloc[-1] \
        else "NEUTRAL"

    # Confidence heuristic (0-100) — a rough "high-accuracy" estimate.
    confidence = 60.0
    if sector_rsi is not None and nifty_rsi is not None:
        confidence += min(10.0, max(0.0, (sector_rsi - nifty_rsi) * 2.0))
    confidence += 5.0 if breakout else 0.0
    confidence += 5.0 if fvg_hit else 0.0
    confidence += min(10.0, (rr - MIN_RR) * 4.0)
    confidence = round(min(confidence, 99.0), 1)

    result = {
        "symbol": symbol,
        "name": candidate_row.get("name", ""),
        "industry": candidate_row.get("industry", ""),
        "sector": candidate_row.get("industry", ""),
        "sector_index": sector_index_name or "NIFTY 50",
        "cmp": round(close, 2),
        "entry": round(close, 2),
        "target": round(target, 2),
        "stop_loss": round(stop_loss, 2),
        "rr_ratio": round(rr, 2),
        "risk_per_share": round(risk, 2),
        "reasons": reasons,
        "reason_string": " | ".join(reasons),
        "weekly_bias": weekly_bias,
        "stop_loss_type": stop_type,
        "atr": round(atr_now, 2),
        "rsi": round(rsi_now, 2),
        "sector_rsi": round(sector_rsi, 2) if sector_rsi is not None else None,
        "nifty_rsi": round(nifty_rsi, 2) if nifty_rsi is not None else None,
        "ema20": round(ema20, 2),
        "ema50": round(ema50, 2),
        "volume_ratio": round(volume_ratio, 2),
        "daily_resistance": round(daily_resistance, 2),
        "weekly_target": round(target, 2),
        "fvg": fvg_hit,
        "confidence": confidence,
        "data_date": str(daily.index[-1].date()),
        "pe_ratio": candidate_row.get("pe_ratio"),
        "roe": candidate_row.get("roe"),
        "qtr_sales_growth": candidate_row.get("qtr_sales_growth"),
        "qtr_profit_growth": candidate_row.get("qtr_profit_growth"),
    }
    return result, None

# ════════════════════════════════════════════════════════════════════════════════
# MAIN ORCHESTRATION
# ════════════════════════════════════════════════════════════════════════════════

UNIVERSE_MAP = {}


def main(args) -> int:
    global UNIVERSE_MAP

    _log("═" * 74)
    _log("  SWING S/R VALIDATOR — Multi-Timeframe + Fundamental + Sector Gate")
    _log("═" * 74)

    # ── Load universe + candidates ────────────────────────────────────────────
    try:
        universe = load_universe()
        UNIVERSE_MAP = {sym: {"isin": row["isin"], "industry": row["industry"]}
                        for sym, row in universe.iterrows()}
    except (FileNotFoundError, ValueError) as exc:
        _warn(str(exc))
        return 1

    try:
        candidates = load_candidates()
    except (FileNotFoundError, ValueError) as exc:
        _warn(str(exc))
        return 1

    if args.symbols:
        wanted = {s.strip().upper() for s in args.symbols.split(",") if s.strip()}
        candidates = candidates[candidates["symbol"].isin(wanted)]
        if candidates.empty:
            _warn(f"None of the requested symbols found in {CANDIDATES_CSV.name}.")
            return 1

    _log(f"  📊 Candidates  : {len(candidates)}")
    _log(f"  🌐 Universe    : {len(UNIVERSE_MAP)} symbols mapped")

    # ── Load NIFTY 50 + sector index data ─────────────────────────────────────
    _log("  🔎 Loading index data (NIFTY 50 + sector indices) ...")
    market_data = load_index_daily("NIFTY 50")
    if not market_data["available"]:
        _warn("NIFTY 50 index data unavailable via yfinance (^NSEI).")

    sector_cache = {}

    def get_sector_data(index_name):
        if index_name is None:
            return {"available": False, "close": None, "above_ema": None, "rsi": None,
                    "source": None, "path": None}
        if index_name not in sector_cache:
            sector_cache[index_name] = load_index_daily(index_name)
        return sector_cache[index_name]

    # ── Preload daily/weekly frames for every candidate ────────────────────────
    daily_map = {}
    weekly_map = {}
    _log(f"  📂 Loading Daily/Weekly data for {len(candidates)} candidates ...")
    for _idx, row in candidates.iterrows():
        sym = row["symbol"]
        uni = UNIVERSE_MAP.get(sym)
        if uni is None:
            continue
        daily, weekly = load_symbol_daily_weekly(sym, uni["isin"])
        if daily is not None:
            daily_map[sym] = daily
        if weekly is not None:
            weekly_map[sym] = weekly

    # ── Validate each candidate ────────────────────────────────────────────────
    validated, skipped = [], {}
    _log(f"  🧪 Validating {len(candidates)} candidates "
         f"(sector RSI > NIFTY RSI gate) ...")

    for _idx, row in candidates.iterrows():
        sym = row["symbol"]
        uni = UNIVERSE_MAP.get(sym)
        daily = daily_map.get(sym)
        weekly = weekly_map.get(sym)

        if uni is None:
            skipped.setdefault("unknown_symbol", []).append(sym)
            continue
        if daily is None or weekly is None:
            skipped.setdefault("missing_symbol_data", []).append(sym)
            continue

        sector_index_name = INDUSTRY_TO_SECTOR_INDEX.get(uni["industry"])
        sector_data = get_sector_data(sector_index_name)

        row_enriched = pd.Series({**row.to_dict(), "industry": uni["industry"]})

        result, reject = validate_symbol(
            sym, row_enriched, daily, weekly,
            market_data, sector_data, sector_index_name,
        )
        if result is not None:
            validated.append(result)
        else:
            skipped.setdefault(reject, []).append(sym)

    # ── Output ─────────────────────────────────────────────────────────────────
    _log("\n" + "═" * 74)
    _log("  RESULTS")
    _log("═" * 74)
    _log(f"  ✅ Validated setups : {len(validated)}")
    _log(f"  ⏭️  Skipped          : {sum(len(v) for v in skipped.values())}")
    if skipped:
        _log("  ── Skip breakdown ──")
        for reason, syms in sorted(skipped.items(), key=lambda kv: -len(kv[1])):
            _log(f"     {reason:24s} : {len(syms)}")
            if len(syms) <= 12:
                _log(f"        {', '.join(syms)}")

    # Write CSV.
    if validated:
        df_out = pd.DataFrame(validated)
        df_csv = df_out.copy()
        df_csv["reasons"] = df_csv["reasons"].apply(lambda r: " | ".join(r))
        df_csv.to_csv(OUTPUT_CSV, index=False)
        _log(f"\n  📁 CSV saved  -> {OUTPUT_CSV}")
    else:
        pd.DataFrame().to_csv(OUTPUT_CSV, index=False)
        _log(f"\n  📁 CSV saved  -> {OUTPUT_CSV} (empty)")

    # Write JSON (dashboard-friendly).
    json_payload = {
        "source": "swing_sr_validator",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total_candidates": len(candidates),
        "validated_count": len(validated),
        "skipped_count": sum(len(v) for v in skipped.values()),
        "relative_momentum_gate": "sector_rsi > nifty_rsi",
        "market_context": {
            "nifty_50": {
                "available": market_data["available"],
                "above_ema_20": market_data["above_ema"],
                "rsi_14": market_data["rsi"],
                "source": market_data["source"],
            },
            "sector_indices": {
                name: {
                    "available": data["available"],
                    "above_ema_20": data["above_ema"],
                    "rsi_14": data["rsi"],
                    "source": data["source"],
                }
                for name, data in sector_cache.items()
            },
        },
        "skipped_reasons": {k: v for k, v in sorted(skipped.items())},
        "setups": validated,
        # Flat, renderer-friendly rows keyed like the Queue Ticks normaliser
        # (symbol / price / entry / sl / target / rrr / reasons / sector_rsi / nifty_rsi).
        "queue_ticks": [
            {
                "symbol": s["symbol"],
                "price": s["cmp"],
                "entry": s["entry"],
                "sl": s["stop_loss"],
                "target": s["target"],
                "rrr": s["rr_ratio"],
                "reasons": s["reason_string"],
                "sector_rsi": s["sector_rsi"],
                "nifty_rsi": s["nifty_rsi"],
            }
            for s in validated
        ],
    }

    with open(OUTPUT_JSON, "w", encoding="utf-8") as fh:
        json.dump(json_payload, fh, indent=2, default=_json_default)
    _log(f"  📁 JSON saved -> {OUTPUT_JSON}")

    # ── Quick console table ────────────────────────────────────────────────────
    if validated:
        _log("\n  ── Validated Swing Setups ──")
        rows = []
        for s in sorted(validated, key=lambda x: -x["rr_ratio"]):
            rows.append({
                "Symbol": s["symbol"],
                "CMP": s["cmp"],
                "Target": s["target"],
                "SL": s["stop_loss"],
                "R:R": s["rr_ratio"],
                "Conf": s["confidence"],
                "Reasons": s["reason_string"],
            })
        print(pd.DataFrame(rows).to_string(index=False))

    _log("\n  ✔ Done.")
    return 0


def _json_default(obj):
    """JSON serializer for numpy/pandas scalars."""
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    return str(obj)


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Swing S/R validator")
    parser.add_argument("--symbols", type=str, default="",
                        help="Comma-separated symbols to limit the scan, "
                             "e.g. GLENMARK,IPCALAB")
    return parser.parse_args(argv)


if __name__ == "__main__":
    _args = parse_args(sys.argv[1:])
    try:
        _code = main(_args)
    except KeyboardInterrupt:
        _log("\n  ⚠️  Interrupted by user.")
        _code = 130
    except Exception:
        import traceback
        traceback.print_exc()
        _code = 1
    sys.exit(_code)









