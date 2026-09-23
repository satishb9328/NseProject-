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

# =====================================================================
# CONFIGURATION
# =====================================================================

TOKEN_FILE_PATH = r"C:\Users\ADMIN\Desktop\MyProject2\Nse_Project\Brokers\upstox\upstox_v3_token.json"
HISTORY_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox Data\5M Data"
MASTER_CSV_PATH = r"C:\Users\ADMIN\Desktop\MyProject2\Nse_Project\funda_fetch\master_trend_setups.csv"

EMA_SPAN = 50        # 2H_EMA_50
EMA_MARGIN = 0.015   # ±1.5% proximity band around the EMA (support zone)

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
    df = pd.read_csv(url)
    return pd.Series(df.instrument_key.values, index=df.tradingsymbol).to_dict()


def fetch_latest_1m_data(instrument_key: str, headers: dict) -> pd.DataFrame:
    """Fetch the latest 1-minute candles for ``instrument_key`` (last 5 days)."""
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=5)

    interval = "1minute"
    safe_key = urllib.parse.quote(instrument_key)
    url = (
        "https://api.upstox.com/v2/historical-candle/"
        f"{safe_key}/{interval}/{end_date.strftime('%Y-%m-%d')}"
        f"/{start_date.strftime('%Y-%m-%d')}"
    )

    try:
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            data = res.json()
            if "data" in data and "candles" in data["data"] and data["data"]["candles"]:
                df = pd.DataFrame(
                    data["data"]["candles"],
                    columns=["Datetime", "Open", "High", "Low", "Close", "Volume", "OI"],
                )
                df["Datetime"] = pd.to_datetime(df["Datetime"]).dt.tz_localize(None)
                return df
    except Exception:  # noqa: BLE001
        pass
    return pd.DataFrame()


# =====================================================================
# DATA STITCHING & RESAMPLING
# =====================================================================

def resample_data(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
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
    return df.resample(timeframe).agg(agg_dict).dropna()


def build_2h_frame(df_5m: pd.DataFrame) -> pd.DataFrame:
    """Resample a 5-minute frame to 2-hour candles and add the 2H_EMA_50."""
    df_2h = resample_data(df_5m, "2h")
    df_2h["2H_EMA_50"] = (
        df_2h["Close"].ewm(span=EMA_SPAN, adjust=False, min_periods=EMA_SPAN).mean()
    )
    return df_2h


# =====================================================================
# SNIPER PATTERN DETECTION (all patterns must hold the EMA50 support)
# =====================================================================

def detect_sniper_pattern(df_2h: pd.DataFrame) -> str | None:
    """Return the bullish-reversal pattern firing on the latest 2H candle, else None.

    Every pattern is vectorized across the whole 2H frame, then only the latest
    (completed or forming) candle is inspected. The location filter is shared by
    all patterns: the candle's Low must touch / be within ``EMA_MARGIN`` of the
    2H_EMA_50.
    """
    if df_2h.empty or len(df_2h) < 3 or "2H_EMA_50" not in df_2h.columns:
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

    for name, series in patterns.items():
        if bool((series & near_ema).iloc[-1]):
            return name
    return None


# =====================================================================
# OUTPUT
# =====================================================================

def _print_table(alerts: list[dict]) -> None:
    width = 96
    print("=" * width)
    print("  🎯 UNIVERSAL 2H LIVE SNIPER BOT  —  Bullish Reversals @ 2H_EMA_50 Support")
    print("=" * width)
    print(f"  Scanned @ {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
        return {"signals": [], "total_count": 0}
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    try:
        upstox_keys = get_upstox_instrument_dict()
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] Could not fetch the Upstox instrument list: {exc}")
        return {"signals": [], "total_count": 0}

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
        return {"signals": [], "total_count": 0}

    files = sorted(
        f for f in os.listdir(HISTORY_DIR)
        if f.endswith("_5m.parquet")
        and f.replace("_5m.parquet", "").upper() in vip_symbols
    )
    print(f"[INFO] Scanning {len(files)} VIP stocks live "
          f"(EMA50 ±{EMA_MARGIN * 100:.1f}% support zone)...\n")

    alerts: list[dict] = []

    for file in files:
        symbol = file.replace("_5m.parquet", "")
        inst_key = upstox_keys.get(symbol)
        if not inst_key:
            continue

        try:
            df_hist = pd.read_parquet(os.path.join(HISTORY_DIR, file))
            df_live_1m = fetch_latest_1m_data(inst_key, headers)

            if not df_live_1m.empty:
                df_live_5m = resample_data(df_live_1m, "5min").reset_index()
                df_combined = pd.concat([df_hist, df_live_5m]).drop_duplicates(
                    subset=["Datetime"], keep="last"
                )
            else:
                df_combined = df_hist

            df_combined = df_combined.sort_values("Datetime").reset_index(drop=True)

            df_2h = build_2h_frame(df_combined)
            if df_2h.empty:
                continue

            pattern = detect_sniper_pattern(df_2h)
            if pattern is None:
                continue

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
        except Exception:  # noqa: BLE001
            continue

        time.sleep(0.1)

    _print_table(alerts)

    return {"signals": alerts, "total_count": len(alerts)}


if __name__ == "__main__":
    main()