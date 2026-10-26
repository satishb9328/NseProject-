"""
Universal 2H Live Sniper Bot
============================
Scans the entire 5-minute history store, stitches in the latest live 1-minute
candles from Upstox, resamples everything to 2-hour candles and hunts for a broad
family of high-probability bullish reversal patterns that print at the 2H_EMA_50
support zone (the "Sniper Entry").

Patterns detected (each must form within ±1.5% of the 2H_EMA_50):
    🎯 2H Pullback (Hammer)       — long lower wick, small body at the top
    🎯 2H Pullback (Engulfing)    — green body fully covers the previous red body
    🎯 2H Pullback (Piercing)     — green body closes > 50% into the previous red body
    🎯 2H Pullback (Morning Star) — Red → Doji/Small → Green (3 candles)
    🎯 2H Pullback (Marubozu)     — large green candle with minimal wicks

Usage
-----
    python live_ml_scanner.py             # standalone — prints a live table
    from .live_ml_scanner import main     # callable from views.py / scheduler
"""

from __future__ import annotations

import datetime
import json
import os
import time
import urllib.parse
import warnings

import numpy as np
import pandas as pd
import requests

warnings.filterwarnings("ignore")

# Indian Standard Time (UTC+5:30) — the exchange timezone. All "today"/session
# date math MUST use IST so candle stitching and date comparisons stay correct
# regardless of the host machine's local timezone.
IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

# =====================================================================
# CONFIGURATION
# =====================================================================

TOKEN_FILE_PATH = r"C:\Users\ADMIN\Desktop\MyProject2\Nse_Project\Brokers\upstox\upstox_v3_token.json"
HISTORY_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox Data\5M Data"
MASTER_CSV_PATH = r"C:\Users\ADMIN\Desktop\MyProject2\Nse_Project\funda_fetch\master_trend_setups.csv"
WEEKLY_HISTORY_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox Data\Weeks Data"
ISIN_MAP_PATH = os.path.join(os.path.dirname(MASTER_CSV_PATH), "ind_nifty500list.csv")

EMA_SPAN = 50        # 2H_EMA_50
EMA_MARGIN = 0.015   # ±1.5% proximity band around the EMA (support zone)
WEEKLY_BB20_PERIOD = 20


def load_symbol_isin_map() -> dict[str, str]:
    """Load the symbol-to-ISIN map used to locate weekly Upstox history."""
    if not os.path.exists(ISIN_MAP_PATH):
        print(f"[WARN] ISIN map not found: {ISIN_MAP_PATH}")
        return {}

    try:
        universe = pd.read_csv(ISIN_MAP_PATH)
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] Could not read ISIN map {ISIN_MAP_PATH}: {exc}")
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
        print("[WARN] ISIN map is missing Symbol or ISIN columns.")
        return {}

    result = {
        str(symbol).strip().upper(): str(isin).strip()
        for symbol, isin in zip(universe[symbol_col], universe[isin_col])
        if pd.notna(symbol) and pd.notna(isin)
    }
    print(f"[INFO] Loaded {len(result)} symbol-to-ISIN mappings.")
    return result


def load_weekly_bb20_midline(
    symbol: str,
    isin_map: dict[str, str],
    as_of: datetime.date | None = None,
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
        print(f"[DATA] {symbol}: no weekly history file found; Mega Reversal unavailable.")
        return None

    try:
        weekly = (
            pd.read_parquet(weekly_path)
            if weekly_path.lower().endswith(".parquet")
            else pd.read_csv(weekly_path)
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] {symbol}: could not read weekly history {weekly_path}: {exc}")
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
        print(
            f"[WARN] {symbol}: weekly history is missing a date or close column "
            f"(columns={list(weekly.columns)})."
        )
        return None

    weekly[date_col] = pd.to_datetime(weekly[date_col], errors="coerce")
    weekly[close_col] = pd.to_numeric(weekly[close_col], errors="coerce")
    invalid_rows = int(weekly[[date_col, close_col]].isna().any(axis=1).sum())
    weekly = weekly.dropna(subset=[date_col, close_col]).sort_values(date_col)
    if weekly.empty:
        print(f"[WARN] {symbol}: weekly history contains no valid date/close rows.")
        return None
    if invalid_rows:
        print(f"[DATA] {symbol}: discarded {invalid_rows} invalid weekly rows.")

    as_of = as_of or datetime.datetime.now(IST).date()
    week_periods = weekly[date_col].dt.to_period("W-SUN")
    current_period = pd.Timestamp(as_of).to_period("W-SUN")
    if as_of.weekday() >= 5:
        completed_closes = weekly.loc[week_periods <= current_period, close_col]
    else:
        completed_closes = weekly.loc[week_periods < current_period, close_col]
    if len(completed_closes) < WEEKLY_BB20_PERIOD:
        print(
            f"[DATA] {symbol}: only {len(completed_closes)} completed weekly closes; "
            f"{WEEKLY_BB20_PERIOD} are required for Mega Reversal."
        )
        return None

    midline = completed_closes.rolling(WEEKLY_BB20_PERIOD).mean().iloc[-1]
    if pd.isna(midline):
        print(f"[WARN] {symbol}: weekly BB20 midline evaluated to NaN.")
        return None
    return float(midline)


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
# UPSTOX AUTH / INSTRUMENTS
# =====================================================================

def get_access_token() -> str | None:
    """Securely read the Upstox access token from the token JSON file."""
    try:
        with open(TOKEN_FILE_PATH, "r") as file:
            data = json.load(file)
            token = data.get("access_token")
            if token:
                return token
            print("[WARN] Token file found, but 'access_token' is missing inside it.")
            return None
    except FileNotFoundError:
        print(f"[ERROR] Token file not found at: {TOKEN_FILE_PATH}")
        return None
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] Error reading token JSON: {exc}")
        return None


def get_upstox_instrument_dict() -> dict:
    """Build {trading_symbol: instrument_key} from Upstox's NSE instrument dump."""
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
    try:
        # Only the two columns we need — the NSE instrument dump is huge.
        df = pd.read_csv(url, usecols=["instrument_key", "tradingsymbol"])
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] Could not fetch Upstox instrument list: {exc}")
        return {}
    return pd.Series(df.instrument_key.values, index=df.tradingsymbol).to_dict()


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
    """Fetch the latest 1-minute candles for ``instrument_key`` (last 5 days)."""
    label = symbol or instrument_key
    end_date = datetime.datetime.now(IST).date()
    start_date = end_date - datetime.timedelta(days=5)

    interval = "1minute"
    safe_key = urllib.parse.quote(instrument_key)
    url = (
        "https://api.upstox.com/v2/historical-candle/"
        f"{safe_key}/{interval}/{end_date.strftime('%Y-%m-%d')}"
        f"/{start_date.strftime('%Y-%m-%d')}"
    )

    try:
        res = requests.get(url, headers=headers, timeout=15)
    except requests.RequestException as exc:
        print(f"[ERROR] {label}: Upstox 1m request failed: {exc}")
        return pd.DataFrame()

    if res.status_code != 200:
        body = res.text[:300].replace("\n", " ")
        print(
            f"[ERROR] {label}: Upstox 1m request returned HTTP "
            f"{res.status_code}: {body}"
        )
        return pd.DataFrame()

    try:
        data = res.json()
    except ValueError as exc:
        print(f"[ERROR] {label}: Upstox returned invalid JSON: {exc}")
        return pd.DataFrame()

    candles = (
        data.get("data", {}).get("candles", [])
        if isinstance(data, dict) and isinstance(data.get("data"), dict)
        else []
    )
    if not candles:
        print(
            f"[DATA] {label}: Upstox returned 0 1m candles; "
            f"response keys={list(data) if isinstance(data, dict) else type(data).__name__}."
        )
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
        print(f"[ERROR] {label}: could not parse Upstox candles: {exc}")
        return pd.DataFrame()

    invalid_time = int(df["Datetime"].isna().sum())
    valid_ohlc = _valid_ohlc_mask(df)
    invalid_ohlc = int((~valid_ohlc).sum())
    missing_volume = int(df["Volume"].isna().sum())
    if invalid_time or invalid_ohlc or missing_volume:
        print(
            f"[DATA] {label}: {len(df)} raw 1m candles; "
            f"{invalid_time} invalid timestamps, {invalid_ohlc} invalid OHLC rows, "
            f"{missing_volume} missing-volume rows."
        )
    df = df.loc[df["Datetime"].notna() & valid_ohlc]
    df = df.sort_values("Datetime").drop_duplicates(subset=["Datetime"], keep="last")
    print(
        f"[DATA] {label}: HTTP 200, {len(candles)} candles received, "
        f"{len(df)} valid OHLC candles; latest="
        f"{df['Datetime'].iloc[-1] if not df.empty else 'none'}."
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
# SNIPER PATTERN DETECTION (all patterns must hold the EMA50 support)
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
    o1, c1 = o.shift(2), c.shift(2)
    o2, c2 = o.shift(1), c.shift(1)
    body1 = body.shift(2)
    body2 = body.shift(1)
    c1_red = c1 < o1
    c2_small = body2 <= 0.5 * body1
    c2_lower = (o2 < c1) | (c2 < c1)
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
# OUTPUT
# =====================================================================

def _print_table(alerts: list[dict]) -> None:
    width = 96
    print("=" * width)
    print("  🎯 UNIVERSAL 2H LIVE SNIPER BOT  —  Bullish Reversals @ 2H_EMA_50 Support")
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
                f"{a['ema']:>12,.2f} | {pct:>10} | {a['pattern']}"
            )
    else:
        print("  No sniper setups right now. Waiting for a 2H pullback to the EMA50...")

    print("=" * width)


# =====================================================================
# MAIN LIVE SCANNER ENGINE
# =====================================================================

def main() -> dict:
    print("\n" + "=" * 96)
    print("  🎯 UNIVERSAL 2H LIVE SNIPER BOT  —  Scanning the market...")
    print("=" * 96)

    token = get_access_token()
    if not token:
        print("[ERROR] Upstox Token invalid or missing. Exiting.")
        return {
            "signals": [],
            "total_count": 0,
            "mega_reversal_signals": [],
            "scan_status": "failed",
        }
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    try:
        upstox_keys = {
            str(symbol).strip().upper(): instrument_key
            for symbol, instrument_key in get_upstox_instrument_dict().items()
        }
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] Could not fetch the Upstox instrument list: {exc}")
        return {
            "signals": [],
            "total_count": 0,
            "mega_reversal_signals": [],
            "scan_status": "failed",
        }
    if not upstox_keys:
        print("[ERROR] Upstox instrument list is empty — cannot scan.")
        return {
            "signals": [],
            "total_count": 0,
            "mega_reversal_signals": [],
            "scan_status": "failed",
        }
    print(f"[INFO] Loaded {len(upstox_keys)} NSE instrument keys.")

    # Load the VIP universe (master_trend_setups.csv) and only scan those symbols.
    vip_symbols: set[str] = set()
    try:
        if os.path.exists(MASTER_CSV_PATH):
            master = pd.read_csv(MASTER_CSV_PATH)
            col = next((c for c in master.columns if c.strip().lower() == "symbol"), None)
            if col:
                vip_symbols = {str(s).strip().upper() for s in master[col].dropna()}
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] Could not load the VIP list from {MASTER_CSV_PATH}: {exc}")

    if not vip_symbols:
        print("[WARN] VIP list is empty — nothing to scan.")
        _print_table([])
        return {
            "signals": [],
            "total_count": 0,
            "mega_reversal_signals": [],
            "scan_status": "failed",
        }
    print(f"[INFO] Loaded {len(vip_symbols)} VIP symbols from {MASTER_CSV_PATH}.")

    isin_map = load_symbol_isin_map()
    if not os.path.isdir(HISTORY_DIR):
        print(f"[ERROR] 5m history directory does not exist: {HISTORY_DIR}")
        return {
            "signals": [],
            "total_count": 0,
            "mega_reversal_signals": [],
            "scan_status": "failed",
        }
    files = sorted(
        f for f in os.listdir(HISTORY_DIR)
        if f.endswith("_5m.parquet")
        and f.replace("_5m.parquet", "").upper() in vip_symbols
    )
    print(
        f"[INFO] Matched {len(files)} cached 5m files for {len(vip_symbols)} VIP "
        f"symbols; {len(vip_symbols) - len(files)} VIP symbols have no matching file."
    )
    print(f"[INFO] Scanning {len(files)} VIP stocks live "
          f"(EMA50 ±{EMA_MARGIN * 100:.1f}% support zone)...\n")

    alerts: list[dict] = []
    mega_reversal_signals: list[dict] = []
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
    }
    today = datetime.datetime.now(IST).date()

    for file in files:
        symbol = file.replace("_5m.parquet", "")
        inst_key = upstox_keys.get(symbol.upper())
        if not inst_key:
            summary["missing_instrument_key"] += 1
            print(f"[WARN] {symbol}: no matching Upstox instrument key; skipped.")
            continue

        try:
            df_hist = pd.read_parquet(
                os.path.join(HISTORY_DIR, file),
                columns=["Datetime", "Open", "High", "Low", "Close", "Volume", "OI"],
            )
            if df_hist.empty:
                summary["empty_history"] += 1
                print(f"[WARN] {symbol}: cached 5m parquet is empty.")
            df_hist["Datetime"] = pd.to_datetime(
                df_hist["Datetime"], errors="coerce"
            )
            for column in ("Open", "High", "Low", "Close", "Volume", "OI"):
                df_hist[column] = pd.to_numeric(df_hist[column], errors="coerce")
            invalid_hist_time = int(df_hist["Datetime"].isna().sum())
            valid_hist_ohlc = _valid_ohlc_mask(df_hist)
            invalid_hist_ohlc = int((~valid_hist_ohlc).sum())
            missing_hist_volume = int(df_hist["Volume"].isna().sum())
            df_hist = df_hist.loc[df_hist["Datetime"].notna() & valid_hist_ohlc]
            hist_latest = df_hist["Datetime"].max() if not df_hist.empty else None
            if hist_latest is not None and (today - hist_latest.date()).days > 1:
                summary["stale_history"] += 1
                print(
                    f"[WARN] {symbol}: cached 5m history is stale; latest="
                    f"{hist_latest}, current IST date={today}."
                )
            print(
                f"[DATA] {symbol}: cached 5m rows={len(df_hist)}, "
                f"invalid timestamps={invalid_hist_time}, invalid OHLC="
                f"{invalid_hist_ohlc}, missing volume={missing_hist_volume}, "
                f"latest={hist_latest}."
            )
            df_live_1m = fetch_latest_1m_data(inst_key, headers, symbol)
            summary["live_candles"] += len(df_live_1m)

            if not df_live_1m.empty:
                df_live_5m = resample_data(df_live_1m, "5min").reset_index()
                df_combined = pd.concat([df_hist, df_live_5m]).drop_duplicates(
                    subset=["Datetime"], keep="last"
                )
            else:
                df_combined = df_hist

            df_combined = df_combined.sort_values("Datetime").reset_index(drop=True)
            combined_latest = (
                df_combined["Datetime"].iloc[-1] if not df_combined.empty else None
            )
            print(
                f"[DATA] {symbol}: live 1m rows={len(df_live_1m)}, combined 5m "
                f"rows={len(df_combined)}, latest={combined_latest}."
            )

            # Mega Reversal uses the latest fetched 1-minute close as the live
            # price; the existing 2H pattern scan below remains unchanged.
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
                summary["live_symbols_today"] += 1

            prev_close = previous_session_close(df_combined, today)
            try:
                weekly_midline = load_weekly_bb20_midline(symbol, isin_map, today)
            except Exception as exc:  # Keep weekly-data failures from skipping 2H scans.
                print(f"[WARN] {symbol}: could not load weekly BB20 data: {exc}")
                weekly_midline = None
            if (
                live_ltp is not None
                and prev_close is not None
                and weekly_midline is not None
                and qualifies_mega_reversal(prev_close, live_ltp, weekly_midline)
            ):
                mega_reversal_signals.append({
                    "symbol": symbol,
                    "signal_type": "Mega Reversal",
                    "trigger_price": live_ltp,
                    "weekly_midline": weekly_midline,
                })
                summary["mega_reversal_candidates"] += 1
                mega_reason = "matched previous-close reclaim and <=3% extension"
            elif live_ltp is None:
                mega_reason = "no valid same-day live 1m close"
            elif prev_close is None:
                mega_reason = "no previous-session close in cached/merged 5m data"
            elif weekly_midline is None:
                mega_reason = "weekly BB20 midline unavailable"
            elif prev_close >= weekly_midline:
                mega_reason = "previous-session close is not below weekly midline"
            elif live_ltp < weekly_midline:
                mega_reason = "live price has not reclaimed weekly midline"
            else:
                mega_reason = "live price is more than 3% above weekly midline"
            print(
                f"[MEGA] {symbol}: prev_close={prev_close}, live_ltp={live_ltp}, "
                f"weekly_midline={weekly_midline}, result={mega_reason}."
            )

            if live_ltp is None:
                print(
                    f"[SCAN] {symbol}: skipped pattern evaluation because no "
                    "same-day live candle was returned."
                )
                continue

            df_2h = build_2h_frame(df_combined)
            summary["closed_2h_candidates"] += len(df_2h)
            if df_2h.empty:
                print(
                    f"[SCAN] {symbol}: no completed 2H candle available; "
                    "forming bars are excluded."
                )
                continue

            pattern_diagnostics: dict[str, object] = {}
            pattern = detect_sniper_pattern(df_2h, pattern_diagnostics)
            print(
                f"[SCAN] {symbol}: closed 2H bars={len(df_2h)}, latest="
                f"{pattern_diagnostics.get('latest_closed_candle')}, close="
                f"{pattern_diagnostics.get('close')}, EMA50="
                f"{pattern_diagnostics.get('ema50')}, low-distance="
                f"{pattern_diagnostics.get('low_to_ema_pct')}%, near_EMA="
                f"{pattern_diagnostics.get('near_ema')}, patterns="
                f"{pattern_diagnostics.get('pattern_matches')}, result="
                f"{pattern_diagnostics.get('reason')}."
            )
            if pattern_diagnostics.get("ema50") is not None:
                evaluated_symbols.append(symbol.upper())
                summary["evaluated_symbols"] += 1
            if pattern is None:
                continue

            summary["sniper_patterns"] += 1
            ltp = float(df_combined["Close"].iloc[-1])
            ema_val = float(df_2h["2H_EMA_50"].iloc[-1])
            low_val = float(df_2h["Low"].iloc[-1])
            dist_pct = (low_val - ema_val) / ema_val * 100.0 if ema_val else 0.0

            alerts.append({
                "symbol": symbol,
                "ltp": ltp,
                "ema": ema_val,
                "dist_pct": dist_pct,
                "pattern": pattern,
                "action": "SNIPER BUY 🚀",
            })
        except Exception as exc:  # noqa: BLE001
            summary["history_read_errors"] += 1
            print(
                f"[ERROR] {symbol}: scan failed and was skipped: "
                f"{type(exc).__name__}: {exc}"
            )
            continue

        time.sleep(0.1)

    _print_table(alerts)
    print(f"[SUMMARY] Pipeline counts: {summary}")
    scan_status = (
        "completed"
        if summary["live_symbols_today"] and evaluated_symbols
        else "failed"
    )
    if scan_status == "failed":
        print(
            "[ERROR] No symbol completed a same-day live 2H evaluation; refusing "
            "to treat cached history as a current live scan."
        )

    return {
        "signals": alerts,
        "total_count": len(alerts),
        "mega_reversal_signals": mega_reversal_signals,
        "evaluated_symbols": evaluated_symbols,
        "scan_status": scan_status,
        "diagnostics": summary,
    }


if __name__ == "__main__":
    main()