"""
Universal 2H Live Sniper Bot — v2 (refactored)
==============================================
Drop-in replacement for live_ml_scanner.py with:

  * Concurrent per-symbol scanning (ThreadPoolExecutor)
  * Disk-cached Upstox instrument dump (refreshed daily)
  * In-memory cached weekly BB20 midlines (per scan run)
  * Retry / exponential backoff on 429 and 5xx from Upstox
  * Market-hours guard (skips off-hours scans unless overridden)
  * Live FORMING 2H bar watchlist — alerts the moment price enters
    the 2H_EMA_50 support zone, long before the 2H candle closes
  * logging module instead of raw prints (per-symbol noise -> DEBUG)
  * Config via environment variables, hard-coded paths only as defaults
  * Tightened Morning Star condition + zero-body guard

Patterns detected on the latest CLOSED 2H candle (must form within
±1.5% of the 2H_EMA_50 support zone):
    🎯 2H Pullback (Hammer)       — long lower wick, small body at the top
    🎯 2H Pullback (Engulfing)    — green body fully covers the previous red body
    🎯 2H Pullback (Piercing)     — green body closes > 50% into the previous red body
    🎯 2H Pullback (Morning Star) — Red → Doji/Small → Green (3 candles)
    🎯 2H Pullback (Marubozu)     — large green candle with minimal wicks

Usage
-----
    python live_ml_scanner_v2.py                     # standalone
    from .live_ml_scanner_v2 import main             # callable from views.py / scheduler

Environment overrides (all optional):
    SNIPER_TOKEN_FILE        path to Upstox token JSON
    SNIPER_HISTORY_DIR       5m parquet cache directory
    SNIPER_MASTER_CSV        VIP universe CSV (needs a Symbol column)
    SNIPER_WEEKLY_DIR        weekly history directory
    SNIPER_ISIN_CSV          NIFTY 500 symbol/ISIN map CSV
    SNIPER_MAX_WORKERS       concurrent Upstox fetch threads   (default 8)
    SNIPER_SCAN_OFFHOURS=1   allow scanning outside 09:15–15:30 IST
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import time
import urllib.parse
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------

logger = logging.getLogger("sniper")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(_handler)
logger.setLevel(os.environ.get("SNIPER_LOG_LEVEL", "INFO").upper())

# Indian Standard Time (UTC+5:30) — the exchange timezone. All "today"/session
# date math MUST use IST so candle stitching and date comparisons stay correct
# regardless of the host machine's local timezone.
IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

MARKET_OPEN = datetime.time(9, 15)
MARKET_CLOSE = datetime.time(15, 30)

# =====================================================================
# CONFIGURATION
# =====================================================================

TOKEN_FILE_PATH = os.environ.get(
    "SNIPER_TOKEN_FILE",
    r"C:\Users\ADMIN\Desktop\MyProject2\Nse_Project\Brokers\upstox\upstox_v3_token.json",
)
HISTORY_DIR = os.environ.get(
    "SNIPER_HISTORY_DIR",
    r"D:\Api data\ML_HISTO_DATA\Upstox Data\5M Data",
)
MASTER_CSV_PATH = os.environ.get(
    "SNIPER_MASTER_CSV",
    r"C:\Users\ADMIN\Desktop\MyProject2\Nse_Project\funda_fetch\master_trend_setups.csv",
)
WEEKLY_HISTORY_DIR = os.environ.get(
    "SNIPER_WEEKLY_DIR",
    r"D:\Api data\ML_HISTO_DATA\Upstox Data\Weeks Data",
)
ISIN_MAP_PATH = os.environ.get(
    "SNIPER_ISIN_CSV",
    os.path.join(os.path.dirname(MASTER_CSV_PATH), "ind_nifty500list.csv"),
)
INSTRUMENT_CACHE_PATH = os.environ.get(
    "SNIPER_INSTRUMENT_CACHE",
    os.path.join(os.path.dirname(TOKEN_FILE_PATH), "upstox_instruments_cache.json"),
)

MAX_WORKERS = int(os.environ.get("SNIPER_MAX_WORKERS", "8"))
ALLOW_OFFHOURS = os.environ.get("SNIPER_SCAN_OFFHOURS", "") == "1"

EMA_SPAN = 50        # 2H_EMA_50
EMA_MARGIN = 0.015   # ±1.5% proximity band around the EMA (support zone)
WEEKLY_BB20_PERIOD = 20

# Retry behaviour for Upstox HTTP calls.
HTTP_RETRIES = 3
HTTP_BACKOFF = 1.5   # seconds; wait = BACKOFF ** attempt

# =====================================================================
# MARKET HOURS GUARD
# =====================================================================

def is_market_open(now: datetime.datetime | None = None) -> bool:
    """True during NSE cash-session hours (Mon–Fri, 09:15–15:30 IST)."""
    now = now or datetime.datetime.now(IST)
    if now.tzinfo is not None:
        now = now.astimezone(IST)
    if now.weekday() >= 5:
        return False
    return MARKET_OPEN <= now.time() <= MARKET_CLOSE

# =====================================================================
# WEEKLY BB20 MIDLINE (with per-run in-memory cache)
# =====================================================================

_midline_cache: dict[tuple[str, str], float | None] = {}


def load_symbol_isin_map() -> dict[str, str]:
    """Load the symbol-to-ISIN map used to locate weekly Upstox history."""
    if not os.path.exists(ISIN_MAP_PATH):
        logger.warning("[WARN] ISIN map not found: %s", ISIN_MAP_PATH)
        return {}

    try:
        universe = pd.read_csv(ISIN_MAP_PATH)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[WARN] Could not read ISIN map %s: %s", ISIN_MAP_PATH, exc)
        return {}
    universe.columns = [str(column).strip() for column in universe.columns]
    symbol_col = next(
        (column for column in universe.columns if column.lower() == "symbol"),
        None,
    )
    isin_col = next(
        (column for column in universe.columns if "isin" in column.lower()),
        None,
    )
    if symbol_col is None or isin_col is None:
        logger.warning("[WARN] ISIN map is missing Symbol or ISIN columns.")
        return {}

    result = {
        str(symbol).strip().upper(): str(isin).strip()
        for symbol, isin in zip(universe[symbol_col], universe[isin_col])
        if pd.notna(symbol) and pd.notna(isin)
    }
    logger.info("[INFO] Loaded %d symbol-to-ISIN mappings.", len(result))
    return result


def _compute_weekly_bb20_midline(
    symbol: str,
    isin_map: dict[str, str],
    as_of: datetime.date,
) -> float | None:
    """Calculate the 20-week SMA using completed weekly candles only."""
    isin = isin_map.get(symbol.upper())
    candidates = []
    if isin:
        candidates.extend([
            os.path.join(WEEKLY_HISTORY_DIR, f"NSE_EQ_{isin}.parquet"),
            os.path.join(WEEKLY_HISTORY_DIR, f"NSE_EQ_{isin}.csv"),
            os.path.join(WEEKLY_HISTORY_DIR, f"{isin}.parquet"),
            os.path.join(WEEKLY_HISTORY_DIR, f"{isin}.csv"),
        ])
    candidates.extend([
        os.path.join(WEEKLY_HISTORY_DIR, f"{symbol}.parquet"),
        os.path.join(WEEKLY_HISTORY_DIR, f"{symbol}.csv"),
    ])
    weekly_path = next((path for path in candidates if os.path.exists(path)), None)
    if weekly_path is None:
        logger.debug("[DATA] %s: no weekly history file found; Mega Reversal unavailable.", symbol)
        return None

    try:
        weekly = (
            pd.read_parquet(weekly_path)
            if weekly_path.lower().endswith(".parquet")
            else pd.read_csv(weekly_path)
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[WARN] %s: could not read weekly history %s: %s", symbol, weekly_path, exc)
        return None
    weekly.columns = [
        str(column).strip().split("(")[0].strip().lower()
        for column in weekly.columns
    ]
    close_col = next(
        (column for column in ("close", "c", "last", "ltp") if column in weekly.columns),
        None,
    )
    date_col = next(
        (column for column in ("timestamp", "date", "datetime", "time")
         if column in weekly.columns),
        None,
    )
    if close_col is None or date_col is None:
        logger.warning(
            "[WARN] %s: weekly history is missing a date or close column (columns=%s).",
            symbol, list(weekly.columns),
        )
        return None

    weekly[date_col] = pd.to_datetime(weekly[date_col], errors="coerce")
    weekly[close_col] = pd.to_numeric(weekly[close_col], errors="coerce")
    invalid_rows = int(weekly[[date_col, close_col]].isna().any(axis=1).sum())
    weekly = weekly.dropna(subset=[date_col, close_col]).sort_values(date_col)
    if weekly.empty:
        logger.warning("[WARN] %s: weekly history contains no valid date/close rows.", symbol)
        return None
    if invalid_rows:
        logger.debug("[DATA] %s: discarded %d invalid weekly rows.", symbol, invalid_rows)

    week_periods = weekly[date_col].dt.to_period("W-SUN")
    current_period = pd.Timestamp(as_of).to_period("W-SUN")
    if as_of.weekday() >= 5:
        completed_closes = weekly.loc[week_periods <= current_period, close_col]
    else:
        completed_closes = weekly.loc[week_periods < current_period, close_col]
    if len(completed_closes) < WEEKLY_BB20_PERIOD:
        logger.debug(
            "[DATA] %s: only %d completed weekly closes; %d are required for Mega Reversal.",
            symbol, len(completed_closes), WEEKLY_BB20_PERIOD,
        )
        return None

    midline = completed_closes.rolling(WEEKLY_BB20_PERIOD).mean().iloc[-1]
    if pd.isna(midline):
        logger.warning("[WARN] %s: weekly BB20 midline evaluated to NaN.", symbol)
        return None
    return float(midline)


def get_weekly_midline(
    symbol: str,
    isin_map: dict[str, str],
    as_of: datetime.date,
) -> float | None:
    """Cached wrapper — one disk read per symbol per scan run, not per call."""
    key = (symbol.upper(), as_of.isoformat())
    if key not in _midline_cache:
        try:
            _midline_cache[key] = _compute_weekly_bb20_midline(symbol, isin_map, as_of)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[WARN] %s: weekly BB20 failed: %s", symbol, exc)
            _midline_cache[key] = None
    return _midline_cache[key]


def previous_session_close(
    df: pd.DataFrame,
    session_date: datetime.date,
) -> float | None:
    """Return the final available close before the current trading date."""
    if df.empty or "Datetime" not in df.columns or "Close" not in df.columns:
        return None

    timestamps = pd.to_datetime(df["Datetime"], errors="coerce")
    previous_rows = df.loc[timestamps.dt.date < session_date]
    if previous_rows.empty:
        return None
    return float(previous_rows["Close"].iloc[-1])


def qualifies_mega_reversal(
    prev_close: float,
    live_ltp: float,
    weekly_midline: float,
) -> bool:
    """Check the previous-close reclaim and 3% maximum-extension conditions."""
    values = (prev_close, live_ltp, weekly_midline)
    if not all(np.isfinite(value) for value in values) or weekly_midline <= 0:
        return False
    return (
        prev_close < weekly_midline
        and live_ltp >= weekly_midline
        and live_ltp <= weekly_midline * 1.03
    )

# =====================================================================
# UPSTOX AUTH / INSTRUMENTS (disk-cached daily)
# =====================================================================

def get_access_token() -> str | None:
    """Securely read the Upstox access token from the token JSON file."""
    try:
        with open(TOKEN_FILE_PATH, "r") as file:
            data = json.load(file)
            token = data.get("access_token")
            if token:
                return token
            logger.error("[WARN] Token file found, but 'access_token' is missing inside it.")
            return None
    except FileNotFoundError:
        logger.error("[ERROR] Token file not found at: %s", TOKEN_FILE_PATH)
        return None
    except Exception as exc:  # noqa: BLE001
        logger.error("[ERROR] Error reading token JSON: %s", exc)
        return None


def get_upstox_instrument_dict(cache_hours: int = 24) -> dict:
    """
    Build {TRADING_SYMBOL: instrument_key} from Upstox's NSE instrument dump.

    The dump is large and changes once a day, so the parsed mapping is cached
    to disk and refreshed only when the cache is older than ``cache_hours``.
    """
    if os.path.exists(INSTRUMENT_CACHE_PATH):
        age_hours = (time.time() - os.path.getmtime(INSTRUMENT_CACHE_PATH)) / 3600.0
        if age_hours < cache_hours:
            try:
                with open(INSTRUMENT_CACHE_PATH, "r", encoding="utf-8") as fh:
                    cache = json.load(fh)
                result = {str(k).strip().upper(): v for k, v in cache.items()}
                logger.info(
                    "[INFO] Loaded %d NSE instrument keys from cache (%.1f h old).",
                    len(result), age_hours,
                )
                return result
            except Exception as exc:  # noqa: BLE001 — corrupt cache, re-fetch
                logger.warning("[WARN] Instrument cache unreadable (%s); re-fetching.", exc)

    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
    try:
        df = pd.read_csv(url, usecols=["instrument_key", "tradingsymbol"])
    except Exception as exc:  # noqa: BLE001
        logger.error("[ERROR] Could not fetch Upstox instrument list: %s", exc)
        return {}

    result = {
        str(symbol).strip().upper(): key
        for symbol, key in pd.Series(df.instrument_key.values, index=df.tradingsymbol).to_dict().items()
    }
    try:
        with open(INSTRUMENT_CACHE_PATH, "w", encoding="utf-8") as fh:
            json.dump(result, fh)
    except Exception as exc:  # noqa: BLE001 — cache write failure is non-fatal
        logger.warning("[WARN] Could not write instrument cache: %s", exc)
    logger.info("[INFO] Fetched and cached %d NSE instrument keys.", len(result))
    return result

# =====================================================================
# UPSTOX HTTP WITH RETRY / BACKOFF
# =====================================================================

def _request_with_retry(
    url: str,
    headers: dict,
    timeout: float = 15.0,
) -> requests.Response | None:
    """GET with retry + exponential backoff on 429 and 5xx responses."""
    response = None
    for attempt in range(HTTP_RETRIES):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
        except requests.RequestException:
            if attempt < HTTP_RETRIES - 1:
                time.sleep(HTTP_BACKOFF ** attempt)
                continue
            raise
        if response.status_code in (429, 500, 502, 503, 504):
            retry_after = response.headers.get("Retry-After", "")
            wait = (
                float(retry_after)
                if retry_after.replace(".", "", 1).isdigit()
                else HTTP_BACKOFF ** attempt
            )
            logger.debug("[HTTP] %s -> %d; retrying in %.1fs", url, response.status_code, wait)
            time.sleep(wait)
            continue
        return response
    return response  # last response (still an error status) after retries

# =====================================================================
# LIVE 1-MINUTE DATA
# =====================================================================

def _valid_ohlc_mask(df: pd.DataFrame) -> pd.Series:
    open_price = df["Open"]
    high = df["High"]
    low = df["Low"]
    close = df["Close"]
    return (
        (open_price > 0)
        & (high > 0)
        & (low > 0)
        & (close > 0)
        & (high >= np.maximum(open_price, close))
        & (low <= np.minimum(open_price, close))
        & (high >= low)
    )


def fetch_latest_1m_data(
    instrument_key: str,
    headers: dict,
    symbol: str | None = None,
) -> pd.DataFrame:
    """Fetch the latest 1-minute candles for ``instrument_key`` (last 2 sessions)."""
    label = symbol or instrument_key
    end_date = datetime.datetime.now(IST).date()
    start_date = end_date - datetime.timedelta(days=2)

    interval = "1minute"
    safe_key = urllib.parse.quote(instrument_key)
    url = (
        "https://api.upstox.com/v2/historical-candle/"
        f"{safe_key}/{interval}/{end_date.strftime('%Y-%m-%d')}"
        f"/{start_date.strftime('%Y-%m-%d')}"
    )

    try:
        res = _request_with_retry(url, headers)
    except requests.RequestException as exc:
        logger.error("[ERROR] %s: Upstox 1m request failed: %s", label, exc)
        return pd.DataFrame()

    if res is None or res.status_code != 200:
        status = res.status_code if res is not None else "no response"
        body = res.text[:300].replace("\n", " ") if res is not None else ""
        logger.error("[ERROR] %s: Upstox 1m request returned HTTP %s: %s", label, status, body)
        return pd.DataFrame()

    try:
        data = res.json()
    except ValueError as exc:
        logger.error("[ERROR] %s: Upstox returned invalid JSON: %s", label, exc)
        return pd.DataFrame()

    candles = (
        data.get("data", {}).get("candles", [])
        if isinstance(data, dict) and isinstance(data.get("data"), dict)
        else []
    )
    if not candles:
        logger.debug("[DATA] %s: Upstox returned 0 1m candles.", label)
        return pd.DataFrame()

    try:
        df = pd.DataFrame(
            candles,
            columns=["Datetime", "Open", "High", "Low", "Close", "Volume", "OI"],
        )
        timestamps = pd.to_datetime(df["Datetime"], errors="coerce", utc=True)
        df["Datetime"] = timestamps.dt.tz_convert(IST).dt.tz_localize(None)
        for column in ("Open", "High", "Low", "Close", "Volume", "OI"):
            df[column] = pd.to_numeric(df[column], errors="coerce")
    except (KeyError, TypeError, ValueError) as exc:
        logger.error("[ERROR] %s: could not parse Upstox candles: %s", label, exc)
        return pd.DataFrame()

    valid_ohlc = _valid_ohlc_mask(df)
    df = df.loc[df["Datetime"].notna() & valid_ohlc]
    df = df.sort_values("Datetime").drop_duplicates(subset=["Datetime"], keep="last")
    logger.debug(
        "[DATA] %s: %d candles received, %d valid; latest=%s.",
        label, len(candles), len(df),
        df["Datetime"].iloc[-1] if not df.empty else "none",
    )
    return df

# =====================================================================
# DATA STITCHING & RESAMPLING
# =====================================================================

def resample_data(
    df: pd.DataFrame,
    timeframe: str,
    *,
    offset: str | None = None,
) -> pd.DataFrame:
    """Resample an OHLC frame (with a 'Datetime' column or index) to ``timeframe``."""
    df = df.set_index("Datetime") if "Datetime" in df.columns else df
    df.index = pd.to_datetime(df.index)
    agg_dict = {
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
        "Volume": "sum",
        "OI": "last",
    }
    resampler = df.resample(timeframe, origin="start_day", offset=offset)
    return resampler.agg(agg_dict).dropna(subset=["Open", "High", "Low", "Close"])


def build_2h_frame(
    df_5m: pd.DataFrame,
    now: datetime.datetime | None = None,
) -> pd.DataFrame:
    """Build 2H candles aligned to NSE open, excluding the still-forming candle."""
    df_2h = resample_data(df_5m, "2h", offset="9h15min")
    current_time = now or datetime.datetime.now(IST)
    if current_time.tzinfo is not None:
        current_time = current_time.astimezone(IST).replace(tzinfo=None)
    bar_end_times = pd.Series(df_2h.index, index=df_2h.index) + pd.Timedelta(hours=2)
    df_2h = df_2h.loc[bar_end_times <= pd.Timestamp(current_time)]
    df_2h["2H_EMA_50"] = (
        df_2h["Close"].ewm(span=EMA_SPAN, adjust=False, min_periods=EMA_SPAN).mean()
    )
    return df_2h

# =====================================================================
# FORMING 2H BAR — built live from today's 1-minute candles
# =====================================================================

def build_forming_2h_bar(
    df_combined_5m: pd.DataFrame,
    df_live_1m: pd.DataFrame,
) -> dict | None:
    """
    Reconstruct the currently-forming 2H candle from today's 1m data.

    Returns live OHLC of the forming bar plus a live EMA50 estimate
    (closed 2H closes + the forming bar's partial close appended).
    The live EMA converges to the exact value as the bar completes.
    """
    if df_live_1m.empty:
        return None

    today = datetime.datetime.now(IST).date()
    df_1m_today = df_live_1m.loc[
        pd.to_datetime(df_live_1m["Datetime"]).dt.date == today
    ]
    if df_1m_today.empty:
        return None

    df_2h_closed = build_2h_frame(df_combined_5m)

    now_ist = datetime.datetime.now(IST).replace(tzinfo=None)
    minutes_since_open = (now_ist.hour * 60 + now_ist.minute) - (9 * 60 + 15)
    if minutes_since_open < 0:
        return None
    bucket_idx = minutes_since_open // 120
    bucket_start = datetime.datetime.combine(
        today, MARKET_OPEN
    ) + pd.Timedelta(hours=2 * bucket_idx)
    bucket_end = bucket_start + pd.Timedelta(hours=2)

    live_slice = df_1m_today.loc[
        (df_1m_today["Datetime"] >= bucket_start)
        & (df_1m_today["Datetime"] < min(bucket_end, now_ist))
    ]
    if live_slice.empty:
        return None

    forming = {
        "bucket_start": bucket_start,
        "bucket_end": bucket_end,
        "open": float(live_slice["Open"].iloc[0]),
        "high": float(live_slice["High"].max()),
        "low": float(live_slice["Low"].min()),
        "close": float(live_slice["Close"].iloc[-1]),
        "pct_of_bar_complete": round(min(100.0, len(live_slice) / 120 * 100), 1),
    }

    if not df_2h_closed.empty and len(df_2h_closed) >= EMA_SPAN:
        closes = list(df_2h_closed["Close"].astype(float)) + [forming["close"]]
        live_ema = float(
            pd.Series(closes)
            .ewm(span=EMA_SPAN, adjust=False, min_periods=EMA_SPAN)
            .mean()
            .iloc[-1]
        )
        forming["ema50_live"] = live_ema
        forming["low_to_ema_pct"] = (
            forming["low"] - live_ema
        ) / abs(live_ema) * 100.0
        forming["dist_from_zone"] = abs(forming["low_to_ema_pct"])
        forming["in_support_zone"] = forming["dist_from_zone"] <= EMA_MARGIN * 100.0
    return forming


def classify_forming_alert(symbol: str, forming: dict | None) -> dict | None:
    """
    Classify how close the forming 2H bar's LOW is to the EMA50 zone.

    Three tiers:
      🚨 IN ZONE   — forming low is inside the ±1.5% support band
      👀 WATCH     — within 2× the band (about to enter)
      None         — too far away, stay quiet
    """
    if not forming or forming.get("ema50_live") is None:
        return None

    zone_pct = EMA_MARGIN * 100.0
    dist = forming["dist_from_zone"]

    if forming["in_support_zone"]:
        status = "🚨 IN ZONE — sniper setup forming NOW"
    elif dist <= zone_pct * 2:
        status = "👀 WATCH — approaching EMA50 zone"
    else:
        return None

    return {
        "symbol": symbol,
        "ltp": forming["close"],
        "ema50_live": forming["ema50_live"],
        "forming_low": forming["low"],
        "low_to_ema_pct": forming["low_to_ema_pct"],
        "bar_complete_pct": forming["pct_of_bar_complete"],
        "status": status,
        "action": "SNIPER WATCH 🎯",
    }

# =====================================================================
# SNIPER PATTERN DETECTION (closed 2H candles, all at EMA50 support)
# =====================================================================

def detect_sniper_pattern(
    df_2h: pd.DataFrame,
    diagnostics: dict[str, object] | None = None,
) -> str | None:
    """Return the bullish-reversal pattern firing on the latest 2H candle, else None.

    Every pattern is vectorized across the whole 2H frame, then only the latest
    completed candle is inspected. The location filter is shared by
    all patterns: the candle's Low must touch / be within ``EMA_MARGIN`` of the
    2H_EMA_50.
    """
    if df_2h.empty or len(df_2h) < 3 or "2H_EMA_50" not in df_2h.columns:
        if diagnostics is not None:
            diagnostics["reason"] = (
                "fewer than 3 closed 2H candles" if not df_2h.empty
                else "no closed 2H candles"
            )
        return None

    o = df_2h["Open"].astype(float)
    h = df_2h["High"].astype(float)
    l = df_2h["Low"].astype(float)
    c = df_2h["Close"].astype(float)
    ema = df_2h["2H_EMA_50"].astype(float)

    # Candle anatomy (vectorized).
    body = (c - o).abs()
    lower_wick = np.minimum(o, c) - l
    upper_wick = h - np.maximum(o, c)
    rng = (h - l).replace(0.0, np.nan)

    # Support zone: the Low is within ±1.5% of the 2H_EMA_50.
    near_ema = (
        ((l - ema).abs() / ema.abs().replace(0.0, np.nan)) <= EMA_MARGIN
    ).fillna(False)

    prev_open = o.shift(1)
    prev_close = c.shift(1)
    prev_red = prev_close < prev_open
    curr_green = c > o

    # a) Hammer / Pin Bar — long lower wick, small body at the top.
    hammer = (body > 0) & (lower_wick >= 2.0 * body) & (upper_wick <= 0.5 * body)

    # b) Bullish Engulfing — green body fully covers the previous red body.
    bullish_engulfing = prev_red & curr_green & (o <= prev_close) & (c >= prev_open)

    # c) Piercing Line — green body closes more than 50% into the previous red body.
    prev_mid = (prev_open + prev_close) / 2.0
    piercing_line = (
        prev_red & curr_green & (o <= prev_close) & (c > prev_mid) & (c < prev_open)
    )

    # d) Morning Star — Red → Doji/Small → Green; green closes deep into C1.
    #    v2: tightened — the middle candle must close BELOW C1's close, and
    #    the small-body test is guarded against a near-zero C1 body.
    o1, c1 = o.shift(2), c.shift(2)
    o2, c2 = o.shift(1), c.shift(1)
    body1 = body.shift(2)
    body2 = body.shift(1)
    c1_red = c1 < o1
    c2_small = body2 <= np.maximum(body1, 1e-9) * 0.5
    c2_lower = c2 < c1
    c1_mid = (o1 + c1) / 2.0
    morning_star = c1_red & c2_small & c2_lower & curr_green & (c > c1_mid)

    # e) Strong Bullish Momentum / Marubozu — large green candle, minimal wicks.
    avg_body = body.rolling(20, min_periods=1).mean()
    marubozu = (
        curr_green
        & (body >= 1.5 * avg_body)
        & (lower_wick <= 0.10 * rng)
        & (upper_wick <= 0.10 * rng)
    )

    patterns = {
        "🎯 2H Pullback (Hammer)": hammer,
        "🎯 2H Pullback (Engulfing)": bullish_engulfing,
        "🎯 2H Pullback (Piercing)": piercing_line,
        "🎯 2H Pullback (Morning Star)": morning_star,
        "🎯 2H Pullback (Marubozu)": marubozu,
    }

    latest = df_2h.index[-1]
    condition_results = {
        name: bool(series.iloc[-1])
        for name, series in patterns.items()
    }
    near_ema_latest = bool(near_ema.iloc[-1])
    if diagnostics is not None:
        diagnostics.update({
            "latest_closed_candle": str(latest),
            "open": float(o.iloc[-1]),
            "high": float(h.iloc[-1]),
            "low": float(l.iloc[-1]),
            "close": float(c.iloc[-1]),
            "ema50": float(ema.iloc[-1]) if pd.notna(ema.iloc[-1]) else None,
            "low_to_ema_pct": (
                float((l.iloc[-1] - ema.iloc[-1]) / abs(ema.iloc[-1]) * 100)
                if pd.notna(ema.iloc[-1]) and ema.iloc[-1] != 0 else None
            ),
            "near_ema": near_ema_latest,
            "pattern_matches": condition_results,
        })

    for name, series in patterns.items():
        if bool((series & near_ema).iloc[-1]):
            if diagnostics is not None:
                diagnostics["reason"] = f"matched {name}"
            return name
    if diagnostics is not None:
        diagnostics["reason"] = (
            "latest closed candle is outside EMA50 support band"
            if not near_ema_latest
            else "no bullish reversal candle structure matched"
        )
    return None

# =====================================================================
# PER-SYMBOL SCAN WORKER (runs concurrently)
# =====================================================================

def scan_symbol(
    symbol: str,
    file_name: str,
    headers: dict,
    upstox_keys: dict[str, str],
    isin_map: dict[str, str],
    today: datetime.date,
) -> dict:
    """
    Scan one symbol end-to-end. Returns a result dict with keys:
    alert / watch / mega (each optional), plus pipeline counters.
    Never raises — failures are reported in the result.
    """
    result: dict = {
        "symbol": symbol,
        "alert": None,
        "watch": None,
        "mega": None,
        "evaluated": False,
        "counters": {
            "missing_instrument_key": 0,
            "history_read_errors": 0,
            "empty_history": 0,
            "live_candles": 0,
            "live_symbols_today": 0,
            "stale_history": 0,
            "closed_2h_candidates": 0,
            "mega_reversal_candidates": 0,
        },
    }

    inst_key = upstox_keys.get(symbol.upper())
    if not inst_key:
        result["counters"]["missing_instrument_key"] = 1
        logger.warning("[WARN] %s: no matching Upstox instrument key; skipped.", symbol)
        return result

    try:
        df_hist = pd.read_parquet(
            os.path.join(HISTORY_DIR, file_name),
            columns=["Datetime", "Open", "High", "Low", "Close", "Volume", "OI"],
        )
        if df_hist.empty:
            result["counters"]["empty_history"] = 1
            logger.warning("[WARN] %s: cached 5m parquet is empty.", symbol)
        df_hist["Datetime"] = pd.to_datetime(df_hist["Datetime"], errors="coerce")
        for column in ("Open", "High", "Low", "Close", "Volume", "OI"):
            df_hist[column] = pd.to_numeric(df_hist[column], errors="coerce")
        valid_hist_ohlc = _valid_ohlc_mask(df_hist)
        df_hist = df_hist.loc[df_hist["Datetime"].notna() & valid_hist_ohlc]
        hist_latest = df_hist["Datetime"].max() if not df_hist.empty else None
        if hist_latest is not None and (today - hist_latest.date()).days > 1:
            result["counters"]["stale_history"] = 1
            logger.warning(
                "[WARN] %s: cached 5m history is stale; latest=%s, today=%s.",
                symbol, hist_latest, today,
            )

        df_live_1m = fetch_latest_1m_data(inst_key, headers, symbol)
        result["counters"]["live_candles"] = len(df_live_1m)

        if not df_live_1m.empty:
            df_live_5m = resample_data(df_live_1m, "5min").reset_index()
            df_combined = pd.concat([df_hist, df_live_5m]).drop_duplicates(
                subset=["Datetime"], keep="last"
            )
        else:
            df_combined = df_hist
        df_combined = df_combined.sort_values("Datetime").reset_index(drop=True)

        # --- Live LTP (must be a same-day 1m close) ---
        if not df_live_1m.empty:
            latest_live_time = pd.to_datetime(
                df_live_1m["Datetime"].iloc[-1], errors="coerce"
            )
            live_ltp = (
                float(df_live_1m["Close"].iloc[-1])
                if pd.notna(latest_live_time) and latest_live_time.date() == today
                else None
            )
        else:
            live_ltp = None
        if live_ltp is not None:
            result["counters"]["live_symbols_today"] = 1

        # --- Mega Reversal (weekly 20-SMA reclaim) ---
        prev_close = previous_session_close(df_combined, today)
        weekly_midline = get_weekly_midline(symbol, isin_map, today)
        if (
            live_ltp is not None
            and prev_close is not None
            and weekly_midline is not None
            and qualifies_mega_reversal(prev_close, live_ltp, weekly_midline)
        ):
            result["mega"] = {
                "symbol": symbol,
                "signal_type": "Mega Reversal",
                "trigger_price": live_ltp,
                "weekly_midline": weekly_midline,
            }
            result["counters"]["mega_reversal_candidates"] = 1

        # --- Forming 2H bar watchlist (the early alert) ---
        if live_ltp is not None:
            forming = build_forming_2h_bar(df_combined, df_live_1m)
            watch = classify_forming_alert(symbol, forming)
            if watch:
                result["watch"] = watch
                logger.info(
                    "[WATCH] %s: %s | low=%.2f EMA50=%.2f (%.2f%%) | bar %.0f%% complete",
                    symbol, _console_safe(watch["status"]), watch["forming_low"],
                    watch["ema50_live"], watch["low_to_ema_pct"],
                    watch["bar_complete_pct"],
                )

        if live_ltp is None:
            logger.debug(
                "[SCAN] %s: skipped pattern evaluation — no same-day live candle.", symbol
            )
            return result

        # --- Closed 2H pattern confirmation ---
        df_2h = build_2h_frame(df_combined)
        result["counters"]["closed_2h_candidates"] = len(df_2h)
        if df_2h.empty:
            return result

        pattern_diagnostics: dict[str, object] = {}
        pattern = detect_sniper_pattern(df_2h, pattern_diagnostics)
        if pattern_diagnostics.get("ema50") is not None:
            result["evaluated"] = True
        if pattern is None:
            logger.debug(
                "[SCAN] %s: %s", symbol, pattern_diagnostics.get("reason")
            )
            return result

        ltp = float(df_combined["Close"].iloc[-1])
        ema_val = float(df_2h["2H_EMA_50"].iloc[-1])
        low_val = float(df_2h["Low"].iloc[-1])
        dist_pct = (low_val - ema_val) / ema_val * 100.0 if ema_val else 0.0

        result["alert"] = {
            "symbol": symbol,
            "ltp": ltp,
            "ema": ema_val,
            "dist_pct": dist_pct,
            "pattern": pattern,
            "action": "SNIPER BUY 🚀",
        }
        logger.info(
            "[SIGNAL] %s: %s | LTP=%.2f EMA50=%.2f (%.2f%%)",
            symbol, _console_safe(pattern), ltp, ema_val, dist_pct,
        )
    except Exception as exc:  # noqa: BLE001
        result["counters"]["history_read_errors"] = 1
        logger.error("[ERROR] %s: scan failed and was skipped: %s: %s",
                     symbol, type(exc).__name__, exc)
    return result

# =====================================================================
# OUTPUT
# =====================================================================

def _console_safe(value: str) -> str:
    return (
        value.replace("🎯", "[TARGET]")
        .replace("👁", "[WATCH]")
        .replace("🚨", "[ALERT]")
        .replace("👀", "[WATCH]")
        .replace("🚀", "[BUY]")
    )


def _print_table(alerts: list[dict]) -> None:
    width = 96
    print("=" * width)
    print("  [TARGET] UNIVERSAL 2H LIVE SNIPER BOT v2 — Confirmed Bullish Reversals @ 2H_EMA_50")
    print("=" * width)
    print(f"  Scanned @ {datetime.datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
    print(f"  Support zone : 2H_EMA_50 ± {EMA_MARGIN * 100:.1f}%")
    print(f"  Alerts found : {len(alerts)}")
    print("-" * width)

    if alerts:
        print(f"  {'SYMBOL':<16} | {'LTP':>12} | {'2H EMA':>12} | {'% FROM EMA':>10} | PATTERN")
        print("-" * width)
        for a in sorted(alerts, key=lambda x: abs(x["dist_pct"])):
            pct = f"{a['dist_pct']:+.2f}%"
            print(
                f"  {a['symbol']:<16} | {a['ltp']:>12,.2f} | "
                f"{a['ema']:>12,.2f} | {pct:>10} | {_console_safe(a['pattern'])}"
            )
    else:
        print("  No confirmed sniper setups right now.")
    print("=" * width)


def _print_watchlist(watchlist: list[dict]) -> None:
    if not watchlist:
        return
    width = 96
    print("-" * width)
    print("  [WATCH] FORMING-BAR WATCHLIST — live 2H bars near the EMA50 zone")
    print("-" * width)
    print(f"  {'SYMBOL':<16} | {'LTP':>12} | {'EMA50 LIVE':>12} | {'LOW vs EMA':>10} | {'BAR':>5} | STATUS")
    print("-" * width)
    for w in sorted(watchlist, key=lambda x: abs(x["low_to_ema_pct"])):
        pct = f"{w['low_to_ema_pct']:+.2f}%"
        print(
            f"  {w['symbol']:<16} | {w['ltp']:>12,.2f} | "
            f"{w['ema50_live']:>12,.2f} | {pct:>10} | "
            f"{w['bar_complete_pct']:>4.0f}% | {_console_safe(w['status'])}"
        )
    print("=" * width)

# =====================================================================
# MAIN LIVE SCANNER ENGINE
# =====================================================================

def main() -> dict:
    print("\n" + "=" * 96)
    print("  [TARGET] UNIVERSAL 2H LIVE SNIPER BOT v2 — Scanning the market...")
    print("=" * 96)

    failed_result = {
        "signals": [],
        "total_count": 0,
        "mega_reversal_signals": [],
        "watchlist": [],
        "watch_count": 0,
        "scan_status": "failed",
    }

    # --- Market-hours guard ---
    if not is_market_open() and not ALLOW_OFFHOURS:
        logger.info(
            "[INFO] Outside NSE trading hours (09:15–15:30 IST, Mon–Fri). "
            "Set SNIPER_SCAN_OFFHOURS=1 to force a scan."
        )
        failed_result["scan_status"] = "skipped_offhours"
        return failed_result

    token = get_access_token()
    if not token:
        logger.error("[ERROR] Upstox Token invalid or missing. Exiting.")
        return failed_result
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    try:
        upstox_keys = get_upstox_instrument_dict()
    except Exception as exc:  # noqa: BLE001
        logger.error("[ERROR] Could not fetch the Upstox instrument list: %s", exc)
        return failed_result
    if not upstox_keys:
        logger.error("[ERROR] Upstox instrument list is empty — cannot scan.")
        return failed_result

    # Load the VIP universe (master_trend_setups.csv) and only scan those symbols.
    vip_symbols: set[str] = set()
    try:
        if os.path.exists(MASTER_CSV_PATH):
            master = pd.read_csv(MASTER_CSV_PATH)
            col = next((c for c in master.columns if c.strip().lower() == "symbol"), None)
            if col:
                vip_symbols = {str(s).strip().upper() for s in master[col].dropna()}
    except Exception as exc:  # noqa: BLE001
        logger.warning("[WARN] Could not load the VIP list from %s: %s", MASTER_CSV_PATH, exc)

    if not vip_symbols:
        logger.warning("[WARN] VIP list is empty — nothing to scan.")
        _print_table([])
        return failed_result
    logger.info("[INFO] Loaded %d VIP symbols from %s.", len(vip_symbols), MASTER_CSV_PATH)

    isin_map = load_symbol_isin_map()
    if not os.path.isdir(HISTORY_DIR):
        logger.error("[ERROR] 5m history directory does not exist: %s", HISTORY_DIR)
        return failed_result
    files = sorted(
        f for f in os.listdir(HISTORY_DIR)
        if f.endswith("_5m.parquet")
        and f.replace("_5m.parquet", "").upper() in vip_symbols
    )
    logger.info(
        "[INFO] Matched %d cached 5m files for %d VIP symbols; %d VIP symbols "
        "have no matching file.",
        len(files), len(vip_symbols), len(vip_symbols) - len(files),
    )
    logger.info(
        "[INFO] Scanning %d VIP stocks (%d workers, EMA50 ±%.1f%% support zone)...",
        len(files), MAX_WORKERS, EMA_MARGIN * 100,
    )

    alerts: list[dict] = []
    mega_reversal_signals: list[dict] = []
    watchlist: list[dict] = []
    evaluated_symbols: list[str] = []
    summary = {
        "files": len(files),
        "missing_instrument_key": 0,
        "history_read_errors": 0,
        "empty_history": 0,
        "live_candles": 0,
        "live_symbols_today": 0,
        "stale_history": 0,
        "closed_2h_candidates": 0,
        "evaluated_symbols": 0,
        "sniper_patterns": 0,
        "mega_reversal_candidates": 0,
        "watchlist_candidates": 0,
    }
    today = datetime.datetime.now(IST).date()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(
                scan_symbol, f.replace("_5m.parquet", ""), f,
                headers, upstox_keys, isin_map, today,
            ): f
            for f in files
        }
        for fut in as_completed(futures):
            file = futures[fut]
            try:
                result = fut.result()
            except Exception as exc:  # noqa: BLE001 — worker should never raise
                summary["history_read_errors"] += 1
                logger.error("[ERROR] %s: unexpected worker failure: %s", file, exc)
                continue

            symbol = result["symbol"]
            for key, value in result["counters"].items():
                summary[key] += value
            if result["evaluated"]:
                evaluated_symbols.append(symbol.upper())
                summary["evaluated_symbols"] += 1
            if result["alert"]:
                summary["sniper_patterns"] += 1
                alerts.append(result["alert"])
            if result["mega"]:
                mega_reversal_signals.append(result["mega"])
            if result["watch"]:
                summary["watchlist_candidates"] += 1
                watchlist.append(result["watch"])

    _print_table(alerts)
    _print_watchlist(watchlist)
    logger.info("[SUMMARY] Pipeline counts: %s", summary)

    scan_status = (
        "completed"
        if summary["live_symbols_today"] and evaluated_symbols
        else "failed"
    )
    if scan_status == "failed":
        logger.error(
            "[ERROR] No symbol completed a same-day live 2H evaluation; refusing "
            "to treat cached history as a current live scan."
        )

    return {
        "signals": alerts,
        "total_count": len(alerts),
        "mega_reversal_signals": mega_reversal_signals,
        "watchlist": watchlist,
        "watch_count": len(watchlist),
        "evaluated_symbols": evaluated_symbols,
        "scan_status": scan_status,
        "diagnostics": summary,
    }


if __name__ == "__main__":
    main()
