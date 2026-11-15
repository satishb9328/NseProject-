import os
import glob
import time
import urllib.parse
import json
from datetime import datetime
import requests
import numpy as np
import pandas as pd

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DATA_DIR = r"D:\Nifty_ML_Data"
CACHE_OHLC_PATH = os.path.join(DATA_DIR, "nifty_fut_1m_ohlc_2026_08_10.parquet")

# ------------------------------------------------------------------------------
# Upstox access-token resolution (READ-ONLY).
# Mirrors Live_dashboard/upstox_live_feed.py: the active token is read from
# Nse_Project/Brokers/upstox/upstox_v3_token.json (freshest valid file wins).
# An explicit UPSTOX_ACCESS_TOKEN environment variable still takes precedence.
# ------------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_TOKEN_FILE_PATHS = [
    os.path.join(_PROJECT_ROOT, "Nse_Project", "Brokers", "upstox", "upstox_v3_token.json"),
    os.path.join(_PROJECT_ROOT, "Nse_Project", "Brokers", "upstox", "upstox", "upstox_token.json"),
    os.path.join(_PROJECT_ROOT, "Nse_Project", "Brokers", "upstox", "upstox_login_details.json"),
    os.path.join(_PROJECT_ROOT, "upstox", "upstox_token.json"),
]


def _read_token_from_file(path: str):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        token = data.get("access_token") if isinstance(data, dict) else None
        return token if token else None
    except (OSError, ValueError):
        return None


def resolve_upstox_access_token() -> str:
    env_token = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()
    if env_token and "PASTE_YOUR" not in env_token:
        return env_token

    candidates = []
    for path in _TOKEN_FILE_PATHS:
        token = _read_token_from_file(path)
        if token:
            try:
                mtime = os.path.getmtime(path)
            except OSError:
                mtime = 0.0
            candidates.append((mtime, path, token))

    if candidates:
        candidates.sort(key=lambda item: item[0], reverse=True)
        _, path, token = candidates[0]
        print(f"[AUTH] Using Upstox access token from: {path}")
        return token

    raise RuntimeError(
        "No active Upstox access token found. Either set UPSTOX_ACCESS_TOKEN or "
        "ensure Nse_Project/Brokers/upstox/upstox_v3_token.json contains 'access_token'."
    )


UPSTOX_ACCESS_TOKEN = resolve_upstox_access_token()

START_DATE = "2026-08-11"
END_DATE = "2026-10-23"

# Monthly NIFTY Futures contract windows covering 2026-08-11 to 2026-10-23
CONTRACT_WINDOWS = [
    {"expiry_prefix": "2026-08", "from_date": "2026-08-11", "to_date": "2026-08-27"},
    {"expiry_prefix": "2026-09", "from_date": "2026-08-28", "to_date": "2026-09-24"},
    {"expiry_prefix": "2026-10", "from_date": "2026-09-25", "to_date": "2026-10-23"},
]

# Backtest Parameters
SLIPPAGE_PTS = 1.0          # Round-trip slippage/brokerage in NIFTY points
ATR_PERIOD = 14
ATR_MULTIPLIER = 2.2
ATR_SL_MIN = 15.0
ATR_SL_MAX = 35.0
RR_TARGET_MULT = 2.0        # 1:2 Risk-to-Reward target on dynamic ATR SL
OPTION_DELTA_EST = 0.50     # ATM option delta approximation for rupee P&L estimate


# ==============================================================================
# STEP 1: FETCH NIFTY FUTURES 1-MIN OHLC FROM UPSTOX EXPIRED INSTRUMENTS API
# ==============================================================================
def get_upstox_headers():
    if not UPSTOX_ACCESS_TOKEN or "PASTE_YOUR" in UPSTOX_ACCESS_TOKEN:
        raise ValueError("Please set UPSTOX_ACCESS_TOKEN in the script or environment variables.")
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}",
    }


def resolve_expired_nifty_fut_key(expiry_prefix: str, headers: dict) -> str:
    """Finds the expired NIFTY Futures instrument_key for a given YYYY-MM expiry."""
    exp_url = "https://api.upstox.com/v2/expired-instruments/expiries"
    params = {"instrument_key": "NSE_INDEX|Nifty 50"}
    resp = requests.get(exp_url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    expiries = resp.json().get("data", [])

    matching_expiries = [e for e in expiries if str(e).startswith(expiry_prefix)]
    if not matching_expiries:
        raise RuntimeError(f"No expired NIFTY expiry found matching {expiry_prefix}. Available: {expiries[:6]}")
    expiry_date = sorted(matching_expiries)[-1]

    fut_url = "https://api.upstox.com/v2/expired-instruments/future/contract"
    fut_params = {"instrument_key": "NSE_INDEX|Nifty 50", "expiry_date": expiry_date}
    fut_resp = requests.get(fut_url, headers=headers, params=fut_params, timeout=15)
    fut_resp.raise_for_status()
    contracts = fut_resp.json().get("data", [])

    if isinstance(contracts, dict):
        return contracts["instrument_key"]
    if isinstance(contracts, list) and len(contracts) > 0:
        return contracts[0]["instrument_key"]
    raise RuntimeError(f"Could not resolve NIFTY Futures contract for expiry {expiry_date}")


def fetch_expired_1m_candles(expired_instrument_key: str, from_date: str, to_date: str, headers: dict) -> pd.DataFrame:
    """Fetches 1-minute historical candles for an expired futures contract."""
    encoded_key = urllib.parse.quote(expired_instrument_key, safe="")
    url = f"https://api.upstox.com/v2/expired-instruments/historical-candle/{encoded_key}/1minute/{to_date}/{from_date}"
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    candles = resp.json().get("data", {}).get("candles", [])

    if not candles:
        return pd.DataFrame()

    # Upstox candle format: [timestamp, open, high, low, close, volume, oi]
    cols = ["timestamp", "open", "high", "low", "close", "volume", "oi"]
    df = pd.DataFrame(candles, columns=cols[: len(candles[0])])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.set_index("timestamp").sort_index()
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[["open", "high", "low", "close", "volume"]]


def list_ohlc_cache_files():
    """Return all cached 1m NIFTY Futures OHLC parquet files."""
    files = sorted(glob.glob(os.path.join(DATA_DIR, "nifty_fut_1m_ohlc_*.parquet")))
    if not files and os.path.exists(CACHE_OHLC_PATH):
        files = [CACHE_OHLC_PATH]
    return files


def _ist_max_date(frame: pd.DataFrame):
    """Latest IST trading date present in a 1m OHLC frame."""
    return frame.index.tz_convert("Asia/Kolkata").max().date()


def load_best_ohlc_cache():
    """Load the cached 1m OHLC frame that extends furthest in time.

    Returns ``(file_path, frame)`` or ``(None, None)`` when no cache exists.
    """
    best_file, best_frame = None, None
    best_max = None
    for cf in list_ohlc_cache_files():
        try:
            frame = pd.read_parquet(cf)
        except Exception:
            continue
        if len(frame) == 0:
            continue
        mx = frame.index.max()
        if best_max is None or mx > best_max:
            best_file, best_frame, best_max = cf, frame, mx
    return best_file, best_frame


def _last_thursday(d):
    """Last Thursday of the month containing ``d`` (NIFTY monthly expiry)."""
    import calendar
    from datetime import timedelta

    last_day = calendar.monthrange(d.year, d.month)[1]
    dt = datetime(d.year, d.month, last_day).date()
    while dt.weekday() != 3:  # 3 == Thursday
        dt -= timedelta(days=1)
    return dt


def _covering_expiry(d):
    """First-of-month date whose contract is the front month on date ``d``."""
    if d <= _last_thursday(d):
        return datetime(d.year, d.month, 1).date()
    y, m = (d.year, d.month + 1) if d.month < 12 else (d.year + 1, 1)
    return datetime(y, m, 1).date()


def build_contract_windows(from_date: str, to_date: str):
    """Build monthly NIFTY Futures contract windows covering ``from_date``..``to_date``."""
    from datetime import timedelta

    start = datetime.strptime(from_date, "%Y-%m-%d").date()
    end = datetime.strptime(to_date, "%Y-%m-%d").date()

    def prev_expiry(month_first):
        y, m = (month_first.year, month_first.month - 1) if month_first.month > 1 else (month_first.year - 1, 12)
        return _last_thursday(datetime(y, m, 1).date())

    # Collect the distinct front-month contracts touched by the range.
    expiries = []
    d = start
    while d <= end:
        cf = _covering_expiry(d)
        if not expiries or expiries[-1] != cf:
            expiries.append(cf)
        d += timedelta(days=1)

    windows = []
    for cf in expiries:
        win_start = max(prev_expiry(cf) + timedelta(days=1), start)
        win_end = min(_last_thursday(cf), end)
        windows.append({
            "expiry_prefix": cf.strftime("%Y-%m"),
            "from_date": win_start.strftime("%Y-%m-%d"),
            "to_date": win_end.strftime("%Y-%m-%d"),
        })
    return windows


def fetch_1m_ohlc_range(from_date: str, to_date: str) -> pd.DataFrame:
    """Fetch 1m NIFTY Futures OHLC for an arbitrary date range from Upstox."""
    headers = get_upstox_headers()
    frames = []

    for win in build_contract_windows(from_date, to_date):
        print(f"  -> Resolving NIFTY Futures contract for {win['expiry_prefix']} ({win['from_date']} to {win['to_date']})...")
        inst_key = resolve_expired_nifty_fut_key(win["expiry_prefix"], headers)
        print(f"     Found key: {inst_key}")
        df_chunk = fetch_expired_1m_candles(inst_key, win["from_date"], win["to_date"], headers)
        print(f"     Fetched {len(df_chunk)} 1-min bars.")
        if not df_chunk.empty:
            frames.append(df_chunk)
        time.sleep(0.4)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames).sort_index()
    return out[~out.index.duplicated(keep="last")]


def load_or_fetch_1m_ohlc(feature_min_date=None, feature_max_date=None) -> pd.DataFrame:
    """Dynamically ensure the 1m NIFTY Futures OHLC cache covers the full feature date range.

    - Loads the cached 1m OHLC parquet that extends furthest in time.
    - If it already covers ``feature_max_date``, returns it directly.
    - Otherwise fetches only the missing date range from Upstox (using the
      existing expired-instruments helpers), merges it with the existing cache
      and saves ``nifty_fut_1m_ohlc_<max_date>.parquet`` for subsequent runs.
    """
    best_file, ohlc = load_best_ohlc_cache()

    if feature_max_date is None:
        # Derive the feature date range directly from the ML feature files.
        ml_df = load_deduplicated_ml_features()
        ist_idx = ml_df.index.tz_convert("Asia/Kolkata")
        feature_max_date = ist_idx.max().date()
        if feature_min_date is None:
            feature_min_date = ist_idx.min().date()

    if ohlc is not None and _ist_max_date(ohlc) >= feature_max_date:
        print(f"[CACHE] Cached 1m NIFTY Futures OHLC ({best_file}) already covers feature max date {feature_max_date}.")
        return ohlc

    cache_max = _ist_max_date(ohlc) if ohlc is not None else None
    if cache_max is None:
        from_date = (feature_min_date if feature_min_date is not None else feature_max_date).strftime("%Y-%m-%d")
    else:
        from_date = cache_max.strftime("%Y-%m-%d")

    to_date = feature_max_date.strftime("%Y-%m-%d")
    print(f"[API] 1m OHLC cache ends {cache_max}; fetching missing range {from_date} -> {to_date} from Upstox Expired Instruments API...")
    missing = fetch_1m_ohlc_range(from_date, to_date)

    if missing.empty:
        raise RuntimeError(f"No 1-minute OHLC candles returned from Upstox API for {from_date} -> {to_date}.")

    if ohlc is not None:
        ohlc = pd.concat([ohlc, missing]).sort_index()
    else:
        ohlc = missing.sort_index()
    ohlc = ohlc[~ohlc.index.duplicated(keep="last")]

    new_cache_path = os.path.join(DATA_DIR, f"nifty_fut_1m_ohlc_{feature_max_date.strftime('%Y_%m_%d')}.parquet")
    ohlc.to_parquet(new_cache_path)
    print(f"[CACHE] Saved {len(ohlc)} 1m bars to {new_cache_path}")
    return ohlc


# ==============================================================================
# STEP 2: RESAMPLE TO 2-MIN & MERGE WITH DEDUPLICATED D:\Nifty_ML_Data
# ==============================================================================
def load_deduplicated_ml_features() -> pd.DataFrame:
    pattern = os.path.join(DATA_DIR, "nifty_features_2min_*.parquet")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No parquet files found matching {pattern}")

    dfs = [pd.read_parquet(f) for f in files]
    ml_df = pd.concat(dfs).sort_index()
    ml_df.index = pd.to_datetime(ml_df.index, utc=True)
    before = len(ml_df)
    ml_df = ml_df[~ml_df.index.duplicated(keep="last")]
    print(f"[DATA] Loaded {len(files)} ML feature files: {before} rows -> {len(ml_df)} unique 2m bars.")
    return ml_df


def build_merged_2m_dataset(ohlc_1m: pd.DataFrame, ml_df: pd.DataFrame) -> pd.DataFrame:
    # Filter to NSE session hours (09:15 to 15:30 IST)
    ist_idx = ohlc_1m.index.tz_convert("Asia/Kolkata")
    session_mask = (ist_idx.time >= pd.to_datetime("09:15").time()) & (
        ist_idx.time < pd.to_datetime("15:30").time()
    )
    ohlc_1m = ohlc_1m[session_mask]

    # Resample to 2-minute bars aligned on even UTC minutes (:00, :02, :04...) matching ML timestamps
    ohlc_2m = (
        ohlc_1m.resample("2min", label="left", closed="left")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna(subset=["open", "close"])
    )

    # Inner join with deduplicated ML feature dataset on exact 2-min timestamp index
    merged = ohlc_2m.join(ml_df, how="inner")
    print(f"[MERGE] Merged 2-minute dataset contains {len(merged)} aligned bars across {merged.index.tz_convert('Asia/Kolkata').normalize().nunique()} trading days.")
    return merged


# ==============================================================================
# STEP 3: COMPUTE INTRADAY VWAP, DAILY-RESET CVD, WILDER ATR(14), & TRAP FLAGS
# ==============================================================================
def compute_dashboard_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    ist_dt = df.index.tz_convert("Asia/Kolkata")
    df["ist_date"] = ist_dt.date
    df["ist_time"] = ist_dt.time
    df["bar_num"] = df.groupby("ist_date").cumcount()

    # 1. Daily-reset Cumulative Volume Delta (true TBT buy_vol - sell_vol)
    df["cvd"] = df.groupby("ist_date")["volume_delta"].cumsum()

    # 2. Intraday VWAP (weighted by true TBT total volume)
    tbt_vol = (df["candle_buy_vol"] + df["candle_sell_vol"]).replace(0, np.nan).fillna(df["volume"]).replace(0, 1)
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    df["vwap"] = (tp * tbt_vol).groupby(df["ist_date"]).cumsum() / tbt_vol.groupby(df["ist_date"]).cumsum()

    # 3. Wilder's 14-Period ATR on 2-minute candles (exact match to dashboard)
    prev_close = df.groupby("ist_date")["close"].shift(1).fillna(df["open"])
    tr = np.maximum(
        df["high"] - df["low"],
        np.maximum((df["high"] - prev_close).abs(), (df["low"] - prev_close).abs()),
    )
    df["atr_2m_14"] = tr.ewm(alpha=1.0 / ATR_PERIOD, adjust=False).mean()
    df["spot_sl_pts"] = np.clip(ATR_MULTIPLIER * df["atr_2m_14"], ATR_SL_MIN, ATR_SL_MAX)

    # 4. 2-Minute CVD Divergence Trap Filter (3-candle completed lookback within same day)
    price_change_3 = df.groupby("ist_date")["close"].diff(3)
    cvd_change_3 = df.groupby("ist_date")["cvd"].diff(3)
    df["cvd_change_3"] = cvd_change_3

    df["bull_trap"] = (df["close"] > df["vwap"]) & (price_change_3 > 0) & (cvd_change_3 < 0)
    df["bear_trap"] = (df["close"] < df["vwap"]) & (price_change_3 < 0) & (cvd_change_3 > 0)

    # 5. Raw VWAP Breakout / Close Confirmation Signals (after 15-bar warm-up, before 15:00 IST)
    prev_c = df.groupby("ist_date")["close"].shift(1)
    prev_v = df.groupby("ist_date")["vwap"].shift(1)
    valid_window = (
        (df["bar_num"] >= 14)
        & (df["ist_time"] >= pd.to_datetime("09:45").time())
        & (df["ist_time"] <= pd.to_datetime("14:55").time())
    )

    df["raw_long_sig"] = valid_window & (df["close"] > df["vwap"]) & (prev_c <= prev_v)
    df["raw_short_sig"] = valid_window & (df["close"] < df["vwap"]) & (prev_c >= prev_v)

    # 6. ML-validated Model C flags (winning thresholds from analyze_ml_thresholds.py)
    cvd_aligned = np.sign(df["volume_delta"]) == np.sign(df["cvd_change_3"])
    df["cvd_aligned"] = cvd_aligned

    if "vwap_distance" in df.columns:
        vwap_dist = df["vwap_distance"].abs()
    else:
        vwap_dist = (df["close"] - df["vwap"]).abs() / df["vwap"].replace(0, np.nan)
    df["vwap_dist"] = vwap_dist

    # High-probability extension + flow filter (no CVD alignment => divergence present)
    ext_flow_ok = (
        valid_window
        & (vwap_dist >= 0.0015)
        & (df["ofi_imbalance"].abs() >= 0.35)
        & (df["volume_delta"].abs() >= 1_500_000)
        & (~cvd_aligned)
    )

    # Model C1 — Trap Mean-Reversion (fade the overextension)
    df["c1_short_sig"] = ext_flow_ok & (df["close"] > df["vwap"]) & (df["cvd_change_3"] < 0)
    df["c1_long_sig"] = ext_flow_ok & (df["close"] < df["vwap"]) & (df["cvd_change_3"] > 0)

    # Model C2 — Continuation (same direction as the CVD divergence)
    df["c2_long_sig"] = ext_flow_ok & (df["close"] > df["vwap"]) & (df["cvd_change_3"] < 0)
    df["c2_short_sig"] = ext_flow_ok & (df["close"] < df["vwap"]) & (df["cvd_change_3"] > 0)

    # 7. Scale-free live-ready features (transfer identical to cvd_engine.py raw vtt)
    if "volume" in df.columns:
        bar_vol = df["volume"].astype(float)
    else:
        bar_vol = (df["candle_buy_vol"] + df["candle_sell_vol"]).astype(float)
    df["bar_vol"] = bar_vol

    rolling_med_vol = df.groupby("ist_date")["bar_vol"].transform(
        lambda s: s.rolling(20, min_periods=5).median()
    )
    df["rolling_med_vol"] = rolling_med_vol
    df["rel_vol_ratio"] = bar_vol / rolling_med_vol.replace(0, np.nan)

    # Opening-safe warmup fallbacks (09:15-09:45 IST): no silent NaN drops.
    df["atr_open_safe"] = df["atr_2m_14"].bfill().fillna(20.0)
    rolling_med_open = df.groupby("ist_date")["bar_vol"].transform(
        lambda s: s.rolling(20, min_periods=1).median()
    )
    df["rel_vol_open_safe"] = bar_vol / rolling_med_open.replace(0, np.nan)

    # Signed VWAP distance (negative => below VWAP); below_vwap_ext uses the
    # authoritative close<vwap comparison plus a >=0.15% (~38pt) extension.
    df["vwap_signed_dist"] = df["vwap_distance"]
    below_vwap_ext = (df["close"] < df["vwap"]) & (df["vwap_distance"].abs() >= 0.0015)

    flow_surge = (df["ofi_imbalance"].abs() >= 0.35) & (df["rel_vol_ratio"] >= 1.5)
    cvd_div_trap = (df["cvd_change_3"] > 0) & (~df["cvd_aligned"])

    # PE-only short signal (falling knife + buyers' CVD divergence + volume surge)
    df["c2_live_pe_sig"] = (
        valid_window
        & below_vwap_ext
        & flow_surge
        & cvd_div_trap
    )

    return df


# ==============================================================================
# STEP 4: RUN A/B BACKTEST (BEFORE vs AFTER CVD TRAP + 2.2x ATR STOP LOSS)
# ==============================================================================
def simulate_strategy(df: pd.DataFrame, use_cvd_filter: bool, use_atr_sl: bool) -> pd.DataFrame:
    trades = []
    eod_exit_time = pd.to_datetime("15:18").time()
    trap_start = pd.to_datetime("10:30").time()
    trap_end = pd.to_datetime("13:00").time()

    for trade_date, day_df in df.groupby("ist_date"):
        day_df = day_df.reset_index()
        in_pos = False
        direction = 0       # +1 Long (CE), -1 Short (PE)
        entry_price = 0.0
        entry_time = None
        sl_price = 0.0
        tp_price = 0.0
        sl_dist = 0.0

        for i in range(len(day_df) - 1):
            row = day_df.iloc[i]
            next_bar = day_df.iloc[i + 1]  # Execute on next 2m bar open (no lookahead bias)

            # Manage open position
            if in_pos:
                exit_price = None
                exit_reason = None

                if direction == 1:
                    if row["low"] <= sl_price:
                        exit_price = sl_price
                        exit_reason = "SL_HIT"
                    elif use_atr_sl and row["high"] >= tp_price:
                        exit_price = tp_price
                        exit_reason = "TP_HIT"
                    elif not use_atr_sl and row["close"] < row["vwap"]:
                        exit_price = row["close"]
                        exit_reason = "VWAP_FLIP"
                else:
                    if row["high"] >= sl_price:
                        exit_price = sl_price
                        exit_reason = "SL_HIT"
                    elif use_atr_sl and row["low"] <= tp_price:
                        exit_price = tp_price
                        exit_reason = "TP_HIT"
                    elif not use_atr_sl and row["close"] > row["vwap"]:
                        exit_price = row["close"]
                        exit_reason = "VWAP_FLIP"

                if exit_price is None and row["ist_time"] >= eod_exit_time:
                    exit_price = row["close"]
                    exit_reason = "EOD_EXIT"

                if exit_price is not None:
                    raw_pts = (exit_price - entry_price) * direction
                    net_pts = raw_pts - SLIPPAGE_PTS
                    trades.append({
                        "date": trade_date,
                        "entry_time": entry_time,
                        "exit_time": row["timestamp"],
                        "side": "LONG_CE" if direction == 1 else "SHORT_PE",
                        "is_11am_window": trap_start <= entry_time.tz_convert("Asia/Kolkata").time() <= trap_end,
                        "entry": round(entry_price, 2),
                        "exit": round(exit_price, 2),
                        "sl_dist": round(sl_dist, 2),
                        "net_pts": round(net_pts, 2),
                        "est_opt_pts": round(net_pts * OPTION_DELTA_EST, 2),
                        "reason": exit_reason,
                    })
                    in_pos = False
                continue

            # Check new signal on completed 2m candle close
            long_ok = bool(row["raw_long_sig"]) and (not use_cvd_filter or not bool(row["bull_trap"]))
            short_ok = bool(row["raw_short_sig"]) and (not use_cvd_filter or not bool(row["bear_trap"]))

            if long_ok or short_ok:
                direction = 1 if long_ok else -1
                entry_price = float(next_bar["open"])
                entry_time = next_bar["timestamp"]

                if use_atr_sl:
                    sl_dist = float(row["spot_sl_pts"])
                    tp_dist = sl_dist * RR_TARGET_MULT
                else:
                    # Old dashboard: no tight ATR stop (catastrophic 70-pt pain threshold)
                    sl_dist = 70.0
                    tp_dist = 9999.0

                sl_price = entry_price - (direction * sl_dist)
                tp_price = entry_price + (direction * tp_dist)
                in_pos = True

    return pd.DataFrame(trades)


def simulate_model_c(df: pd.DataFrame, long_col: str, short_col: str) -> pd.DataFrame:
    """Model C: ML-validated extension+flow signals with realistic 2m management.

    - Enter on next 2m bar open (no lookahead bias).
    - SL = clip(2.2 * ATR14, 15, 35), TP = min(1.2 * SL, 30).
    - +15 pt favourable intrabar move locks a breakeven stop (SL -> entry).
    - EOD exit at 15:18 IST, 1.0 pt round-trip slippage.
    """
    trades = []
    eod_exit_time = pd.to_datetime("15:18").time()
    trap_start = pd.to_datetime("10:30").time()
    trap_end = pd.to_datetime("13:00").time()
    breakeven_trigger = 15.0

    for trade_date, day_df in df.groupby("ist_date"):
        day_df = day_df.reset_index()
        in_pos = False
        direction = 0
        entry_price = 0.0
        entry_time = None
        sl_price = 0.0
        tp_price = 0.0
        sl_dist = 0.0
        be_locked = False

        for i in range(len(day_df) - 1):
            row = day_df.iloc[i]
            next_bar = day_df.iloc[i + 1]

            if in_pos:
                # Breakeven trailing: +15 pt favourable intrabar move -> SL to entry
                if not be_locked:
                    if direction == 1 and row["high"] >= entry_price + breakeven_trigger:
                        sl_price = entry_price
                        be_locked = True
                    elif direction == -1 and row["low"] <= entry_price - breakeven_trigger:
                        sl_price = entry_price
                        be_locked = True

                exit_price = None
                exit_reason = None
                if direction == 1:
                    if row["low"] <= sl_price:
                        exit_price = sl_price
                        exit_reason = "SL_HIT"
                    elif row["high"] >= tp_price:
                        exit_price = tp_price
                        exit_reason = "TP_HIT"
                else:
                    if row["high"] >= sl_price:
                        exit_price = sl_price
                        exit_reason = "SL_HIT"
                    elif row["low"] <= tp_price:
                        exit_price = tp_price
                        exit_reason = "TP_HIT"

                if exit_price is None and row["ist_time"] >= eod_exit_time:
                    exit_price = row["close"]
                    exit_reason = "EOD_EXIT"

                if exit_price is not None:
                    raw_pts = (exit_price - entry_price) * direction
                    net_pts = raw_pts - SLIPPAGE_PTS
                    trades.append({
                        "date": trade_date,
                        "entry_time": entry_time,
                        "exit_time": row["timestamp"],
                        "side": "LONG_CE" if direction == 1 else "SHORT_PE",
                        "is_11am_window": trap_start <= entry_time.tz_convert("Asia/Kolkata").time() <= trap_end,
                        "entry": round(entry_price, 2),
                        "exit": round(exit_price, 2),
                        "sl_dist": round(sl_dist, 2),
                        "net_pts": round(net_pts, 2),
                        "est_opt_pts": round(net_pts * OPTION_DELTA_EST, 2),
                        "reason": exit_reason,
                    })
                    in_pos = False
                continue

            long_ok = bool(row[long_col])
            short_ok = bool(row[short_col])
            if long_ok or short_ok:
                direction = 1 if long_ok else -1
                entry_price = float(next_bar["open"])
                entry_time = next_bar["timestamp"]
                sl_dist = float(np.clip(ATR_MULTIPLIER * row["atr_2m_14"], ATR_SL_MIN, ATR_SL_MAX))
                tp_dist = min(1.2 * sl_dist, 30.0)
                sl_price = entry_price - (direction * sl_dist)
                tp_price = entry_price + (direction * tp_dist)
                be_locked = False
                in_pos = True

    return pd.DataFrame(trades)


def diagnose_c2_breakeven(df: pd.DataFrame):
    """STEP 1 diagnostic: measure premature breakeven shakeouts in the prior C2 log."""
    csv_path = os.path.join(DATA_DIR, "backtest_trades_model_c2_continuation.csv")
    if not os.path.exists(csv_path):
        print("[DIAG] No prior C2 continuation log found; skipping breakeven diagnosis.")
        return

    trades = pd.read_csv(csv_path)
    total = int(len(trades))
    be = trades[(trades["net_pts"] >= -1.5) & (trades["net_pts"] <= 0.0)]
    n_be = int(len(be))

    o = df[["open", "high", "low", "close", "ist_date"]].copy()
    pos = {ts: i for i, ts in enumerate(list(o.index))}

    hit25 = 0
    for _, t in be.iterrows():
        side = t["side"]
        entry = float(t["entry"])
        ext = pd.to_datetime(t["exit_time"], utc=True)
        if ext not in pos:
            continue
        pi = pos[ext]
        day = ext.tz_convert("Asia/Kolkata").date()
        fut = o.iloc[pi + 1: pi + 11]
        fut = fut[fut["ist_date"] == day]
        if len(fut) == 0:
            continue
        fav = (float(fut["high"].max()) - entry) if side == "LONG_CE" else (entry - float(fut["low"].min()))
        if fav >= 25.0:
            hit25 += 1

    print("\n" + "=" * 72)
    print("  C2 BREAKEVEN-SHAKEOUT DIAGNOSIS (prior continuation log)")
    print("=" * 72)
    print(f"  Total trades                             : {total}")
    print(f"  Breakeven-choked trades (net -1.5..0.0)  : {n_be}")
    print(f"  Of those, would have hit +25 pts in the  : {hit25}")
    print(f"  next 10 bars (20 min) if not stopped out :")


def simulate_model_c2_fixed(df: pd.DataFrame, variant: str, tp_pts: float = 26.0) -> pd.DataFrame:
    """Model C2 continuation without the premature +15pt breakeven choke.

    C2-A: pure ATR SL (clip 2.2*ATR14, 15, 35) + fixed tp_pts TP, EOD 15:18.
    C2-B: ATR SL untouched until +22pt favourable, then lock SL to entry+6
          (net +5 after slippage) and set TP = 32pt.
    """
    trades = []
    eod_exit_time = pd.to_datetime("15:18").time()
    trap_start = pd.to_datetime("10:30").time()
    trap_end = pd.to_datetime("13:00").time()

    for trade_date, day_df in df.groupby("ist_date"):
        day_df = day_df.reset_index()
        in_pos = False
        direction = 0
        entry_price = 0.0
        entry_time = None
        sl_price = 0.0
        tp_price = None
        sl_dist = 0.0
        locked = False

        for i in range(len(day_df) - 1):
            row = day_df.iloc[i]
            next_bar = day_df.iloc[i + 1]

            if in_pos:
                exit_price = None
                exit_reason = None

                if variant == "B" and not locked:
                    if direction == 1 and row["high"] >= entry_price + 22.0:
                        sl_price = entry_price + 6.0
                        tp_price = entry_price + 32.0
                        locked = True
                    elif direction == -1 and row["low"] <= entry_price - 22.0:
                        sl_price = entry_price - 6.0
                        tp_price = entry_price - 32.0
                        locked = True

                if direction == 1:
                    if row["low"] <= sl_price:
                        exit_price = sl_price
                        exit_reason = "SL_HIT"
                    elif tp_price is not None and row["high"] >= tp_price:
                        exit_price = tp_price
                        exit_reason = "TP_HIT"
                else:
                    if row["high"] >= sl_price:
                        exit_price = sl_price
                        exit_reason = "SL_HIT"
                    elif tp_price is not None and row["low"] <= tp_price:
                        exit_price = tp_price
                        exit_reason = "TP_HIT"

                if exit_price is None and row["ist_time"] >= eod_exit_time:
                    exit_price = row["close"]
                    exit_reason = "EOD_EXIT"

                if exit_price is not None:
                    raw_pts = (exit_price - entry_price) * direction
                    net_pts = raw_pts - SLIPPAGE_PTS
                    trades.append({
                        "date": trade_date,
                        "entry_time": entry_time,
                        "exit_time": row["timestamp"],
                        "side": "LONG_CE" if direction == 1 else "SHORT_PE",
                        "is_11am_window": trap_start <= entry_time.tz_convert("Asia/Kolkata").time() <= trap_end,
                        "entry": round(entry_price, 2),
                        "exit": round(exit_price, 2),
                        "sl_dist": round(sl_dist, 2),
                        "net_pts": round(net_pts, 2),
                        "est_opt_pts": round(net_pts * OPTION_DELTA_EST, 2),
                        "reason": exit_reason,
                    })
                    in_pos = False
                continue

            long_ok = bool(row["c2_long_sig"])
            short_ok = bool(row["c2_short_sig"])
            if long_ok or short_ok:
                direction = 1 if long_ok else -1
                entry_price = float(next_bar["open"])
                entry_time = next_bar["timestamp"]
                sl_dist = float(np.clip(ATR_MULTIPLIER * row["atr_2m_14"], ATR_SL_MIN, ATR_SL_MAX))
                sl_price = entry_price - (direction * sl_dist)
                if variant == "A":
                    tp_price = entry_price + (direction * tp_pts)
                else:
                    tp_price = None
                locked = False
                in_pos = True

    return pd.DataFrame(trades)


def _tod_summary(tdf: pd.DataFrame) -> pd.DataFrame:
    """Group trades by IST entry-time windows."""
    if tdf.empty:
        return pd.DataFrame()
    d = tdf.copy()
    ist_time = pd.to_datetime(d["entry_time"], utc=True).dt.tz_convert("Asia/Kolkata").dt.time

    def bucket(t):
        if t < pd.to_datetime("10:30").time():
            return "09:45-10:30"
        if t < pd.to_datetime("11:30").time():
            return "10:30-11:30"
        if t < pd.to_datetime("13:00").time():
            return "11:30-13:00"
        if t < pd.to_datetime("14:00").time():
            return "13:00-14:00"
        return "14:00-14:55"

    d["bucket"] = [bucket(t) for t in ist_time]
    order = ["09:45-10:30", "10:30-11:30", "11:30-13:00", "13:00-14:00", "14:00-14:55"]
    rows = []
    for b in order:
        sub = d[d["bucket"] == b]
        n = int(len(sub))
        wr = float((sub["net_pts"] > 0).mean() * 100.0) if n else float("nan")
        net = float(sub["net_pts"].sum()) if n else 0.0
        rows.append((b, n, wr, net))
    return pd.DataFrame(rows, columns=["window", "trades", "win_rate%", "net_pts"]).set_index("window")


def print_time_of_day(ta: pd.DataFrame, tb: pd.DataFrame):
    """Print a combined C2-A vs C2-B time-of-day breakdown table."""
    print("\n" + "=" * 100)
    print("  TIME-OF-DAY BREAKDOWN (IST entry windows)")
    print("=" * 100)
    sa = _tod_summary(ta)
    sb = _tod_summary(tb)
    if sa.empty or sb.empty:
        return
    combined = pd.concat([sa.add_prefix("C2-A "), sb.add_prefix("C2-B ")], axis=1)
    with pd.option_context("display.float_format", lambda x: f"{x:,.2f}", "display.width", 250):
        print(combined.to_string())


def _live_pe_signal(df: pd.DataFrame, min_rel_vol: float) -> pd.Series:
    """Recompute the scale-free PE-only short signal for any rel-vol threshold."""
    valid_window = (
        (df["bar_num"] >= 14)
        & (df["ist_time"] >= pd.to_datetime("09:45").time())
        & (df["ist_time"] <= pd.to_datetime("14:55").time())
    )
    below_vwap_ext = (df["close"] < df["vwap"]) & (df["vwap_distance"].abs() >= 0.0015)
    flow_surge = (df["ofi_imbalance"].abs() >= 0.35) & (df["rel_vol_ratio"] >= min_rel_vol)
    cvd_div_trap = (df["cvd_change_3"] > 0) & (~df["cvd_aligned"])
    return valid_window & below_vwap_ext & flow_surge & cvd_div_trap


def simulate_model_c2_live_pe(df: pd.DataFrame, tp_pts: float = 45.0, min_rel_vol: float = 1.5) -> pd.DataFrame:
    """Scale-free PE-only C2-LIVE-READY: SHORT_PE on the rel-vol gated signal.

    - Enter SHORT_PE on next 2m bar open (no lookahead bias).
    - SL = clip(2.2 * ATR14, 15, 35), TP = 45pt, EOD 15:18, 1pt slippage.
    """
    sig = _live_pe_signal(df, min_rel_vol)
    trades = []
    eod_exit_time = pd.to_datetime("15:18").time()
    trap_start = pd.to_datetime("10:30").time()
    trap_end = pd.to_datetime("13:00").time()

    for trade_date, day_df in df.groupby("ist_date"):
        day_df = day_df.reset_index()
        in_pos = False
        entry_price = 0.0
        entry_time = None
        sl_price = 0.0
        tp_price = 0.0
        sl_dist = 0.0

        for i in range(len(day_df) - 1):
            row = day_df.iloc[i]
            next_bar = day_df.iloc[i + 1]

            if in_pos:
                exit_price = None
                exit_reason = None
                if row["high"] >= sl_price:
                    exit_price = sl_price
                    exit_reason = "SL_HIT"
                elif row["low"] <= tp_price:
                    exit_price = tp_price
                    exit_reason = "TP_HIT"
                if exit_price is None and row["ist_time"] >= eod_exit_time:
                    exit_price = row["close"]
                    exit_reason = "EOD_EXIT"
                if exit_price is not None:
                    net_pts = (entry_price - exit_price) - SLIPPAGE_PTS
                    trades.append({
                        "date": trade_date,
                        "entry_time": entry_time,
                        "exit_time": row["timestamp"],
                        "side": "SHORT_PE",
                        "is_11am_window": trap_start <= entry_time.tz_convert("Asia/Kolkata").time() <= trap_end,
                        "entry": round(entry_price, 2),
                        "exit": round(exit_price, 2),
                        "sl_dist": round(sl_dist, 2),
                        "net_pts": round(net_pts, 2),
                        "est_opt_pts": round(net_pts * OPTION_DELTA_EST, 2),
                        "reason": exit_reason,
                    })
                    in_pos = False
                continue

            if bool(sig.loc[row["timestamp"]]):
                entry_price = float(next_bar["open"])
                entry_time = next_bar["timestamp"]
                sl_dist = float(np.clip(ATR_MULTIPLIER * row["atr_2m_14"], ATR_SL_MIN, ATR_SL_MAX))
                sl_price = entry_price + sl_dist   # short SL above entry
                tp_price = entry_price - tp_pts    # short TP below entry
                in_pos = True

    return pd.DataFrame(trades)


def summarize_live_ready(name: str, tdf: pd.DataFrame):
    """Custom summary with max-consecutive-losses + 11:30-14:00 IST window."""
    if tdf.empty:
        print(f"\n=== {name}: 0 Trades ===")
        return
    tdf = tdf.sort_values("entry_time").reset_index(drop=True)
    wins = tdf[tdf["net_pts"] > 0]
    losses = tdf[tdf["net_pts"] <= 0]
    win_rate = len(wins) / len(tdf) * 100.0
    net_pts = tdf["net_pts"].sum()
    gp = wins["net_pts"].sum()
    gl = abs(losses["net_pts"].sum())
    pf = gp / gl if gl > 0 else float("inf")
    max_dd = (tdf["net_pts"].cumsum() - tdf["net_pts"].cumsum().cummax()).min()

    def streak(vals, cond):
        best = cur = 0
        for v in vals:
            if cond(v):
                cur += 1
                best = max(best, cur)
            else:
                cur = 0
        return best
    max_consec_losses = streak(tdf["net_pts"].values, lambda v: v <= 0)

    ist_time = pd.to_datetime(tdf["entry_time"], utc=True).dt.tz_convert("Asia/Kolkata").dt.time
    t1130 = pd.to_datetime("11:30").time()
    t1400 = pd.to_datetime("14:00").time()
    mask = [(t >= t1130) and (t < t1400) for t in ist_time]
    win = tdf[mask]
    win_pts = win["net_pts"].sum()
    win_n = len(win)
    win_wr = (win["net_pts"] > 0).mean() * 100.0 if win_n else 0.0

    print(f"\n{'=' * 66}")
    print(f"  {name}")
    print(f"{'=' * 66}")
    print(f"  Total Trades (Aug 11 - Oct 23) : {len(tdf)}")
    print(f"  Win Rate                       : {win_rate:.1f}% ({len(wins)}W / {len(losses)}L)")
    print(f"  Net NIFTY Points               : {net_pts:+.2f} pts (Est. Option Pts: {tdf['est_opt_pts'].sum():+.2f})")
    print(f"  Profit Factor                  : {pf:.2f}")
    print(f"  Max Peak-to-Trough Drawdown    : {max_dd:.2f} pts")
    print(f"  Max Consecutive Losses         : {max_consec_losses}")
    print(f"  11:30 AM - 2:00 PM Window      : {win_n} trades | WR: {win_wr:.1f}% | Net: {win_pts:+.2f} pts")


def summarize_results(name: str, tdf: pd.DataFrame):
    if tdf.empty:
        print(f"\n=== {name}: 0 Trades ===")
        return

    wins = tdf[tdf["net_pts"] > 0]
    losses = tdf[tdf["net_pts"] <= 0]
    win_rate = len(wins) / len(tdf) * 100.0
    net_pts = tdf["net_pts"].sum()
    worst_trade = tdf["net_pts"].min()
    avg_win = wins["net_pts"].mean() if not wins.empty else 0.0
    avg_loss = losses["net_pts"].mean() if not losses.empty else 0.0
    gross_profit = wins["net_pts"].sum()
    gross_loss = abs(losses["net_pts"].sum())
    pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    cum_curve = tdf["net_pts"].cumsum()
    max_dd = (cum_curve - cum_curve.cummax()).min()

    midday = tdf[tdf["is_11am_window"]]
    midday_pts = midday["net_pts"].sum() if not midday.empty else 0.0
    midday_wr = (len(midday[midday["net_pts"] > 0]) / len(midday) * 100.0) if not midday.empty else 0.0

    print(f"\n{'=' * 62}")
    print(f"  {name}")
    print(f"{'=' * 62}")
    print(f"  Total Trades (Aug 11 - Oct 23) : {len(tdf)}")
    print(f"  Win Rate                       : {win_rate:.1f}% ({len(wins)}W / {len(losses)}L)")
    print(f"  Net NIFTY Points               : {net_pts:+.2f} pts (Est. Option Pts: {tdf['est_opt_pts'].sum():+.2f})")
    print(f"  Profit Factor                  : {pf:.2f}")
    print(f"  Avg Win / Avg Loss             : +{avg_win:.2f} pts / {avg_loss:.2f} pts")
    print(f"  Worst Single-Trade Loss        : {worst_trade:.2f} pts")
    print(f"  Max Peak-to-Trough Drawdown    : {max_dd:.2f} pts")
    print(f"  10:30 AM - 1:00 PM Window      : {len(midday)} trades | WR: {midday_wr:.1f}% | Net: {midday_pts:+.2f} pts")


TARGET_SWEEP = [26.0, 30.0, 35.0, 40.0, 45.0, 50.0]


def _best_window_1130_1400(tdf: pd.DataFrame) -> float:
    """Net points for entries in the 11:30-14:00 IST window."""
    if tdf.empty:
        return 0.0
    d = tdf.copy()
    ist_time = pd.to_datetime(d["entry_time"], utc=True).dt.tz_convert("Asia/Kolkata").dt.time
    t1130 = pd.to_datetime("11:30").time()
    t1400 = pd.to_datetime("14:00").time()
    mask = [(t >= t1130) and (t < t1400) for t in ist_time]
    return float(d[mask]["net_pts"].sum())


def run_target_sweep(enriched: pd.DataFrame):
    """Sweep C2-A take-profit levels while keeping the 2.2x ATR SL fixed."""
    print("\n" + "=" * 100)
    print("  MODEL C2-A TARGET SWEEP (Fixed 2.2x ATR SL 15-35 pts)")
    print("=" * 100)
    rows = []
    for tp in TARGET_SWEEP:
        tdf = simulate_model_c2_fixed(enriched, variant="A", tp_pts=tp)
        n = int(len(tdf))
        if n == 0:
            rows.append([tp, 0, 0, 0, float("nan"), 0.0, 0.0, float("nan"), 0.0, 0.0])
            continue
        tp_hits = int((tdf["reason"] == "TP_HIT").sum())
        sl_hits = int((tdf["reason"] == "SL_HIT").sum())
        wins = tdf[tdf["net_pts"] > 0]
        losses = tdf[tdf["net_pts"] <= 0]
        wr = len(wins) / n * 100.0
        net = float(tdf["net_pts"].sum())
        est_opt = float(tdf["est_opt_pts"].sum())
        gp = float(wins["net_pts"].sum())
        gl = abs(float(losses["net_pts"].sum()))
        pf = gp / gl if gl > 0 else float("inf")
        max_dd = float((tdf["net_pts"].cumsum() - tdf["net_pts"].cumsum().cummax()).min())
        bw = _best_window_1130_1400(tdf)
        rows.append([tp, n, tp_hits, sl_hits, wr, net, est_opt, pf, max_dd, bw])

    cols = ["TP_Pts", "Trades", "TP_Hits", "SL_Hits", "Win_Rate%", "Net_Pts",
            "Est_Opt_Pts", "Profit_Factor", "Max_DD", "Best_Window_1130_1400_Pts"]
    tab = pd.DataFrame(rows, columns=cols)
    with pd.option_context("display.float_format", lambda x: f"{x:,.2f}", "display.width", 250):
        print(tab.to_string(index=False))


REL_VOL_SWEEP = [1.1, 1.2, 1.3, 1.5]


def _max_consec_losses(tdf: pd.DataFrame) -> int:
    if tdf.empty:
        return 0
    best = cur = 0
    for v in tdf.sort_values("entry_time")["net_pts"].values:
        if v <= 0:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def run_rel_vol_sweep(enriched: pd.DataFrame):
    """Sweep the relative-volume gate while keeping every other PE-only rule fixed."""
    print("\n" + "=" * 100)
    print("  MODEL C2-LIVE-READY RELATIVE VOLUME SWEEP (PE-Only · 45pt TP)")
    print("=" * 100)
    rows = []
    for mv in REL_VOL_SWEEP:
        sig = _live_pe_signal(enriched, mv)
        sig_bars = int(sig.sum())
        tdf = simulate_model_c2_live_pe(enriched, min_rel_vol=mv)
        n = int(len(tdf))
        if n == 0:
            rows.append([mv, sig_bars, 0, 0, 0, float("nan"), 0.0, 0.0, float("nan"), 0.0, 0, 0.0])
            continue
        tp_hits = int((tdf["reason"] == "TP_HIT").sum())
        sl_hits = int((tdf["reason"] == "SL_HIT").sum())
        wins = tdf[tdf["net_pts"] > 0]
        losses = tdf[tdf["net_pts"] <= 0]
        wr = len(wins) / n * 100.0
        net = float(tdf["net_pts"].sum())
        est_opt = float(tdf["est_opt_pts"].sum())
        gp = float(wins["net_pts"].sum())
        gl = abs(float(losses["net_pts"].sum()))
        pf = gp / gl if gl > 0 else float("inf")
        max_dd = float((tdf["net_pts"].cumsum() - tdf["net_pts"].cumsum().cummax()).min())
        mcl = _max_consec_losses(tdf)
        bw = _best_window_1130_1400(tdf)
        rows.append([mv, sig_bars, n, tp_hits, sl_hits, wr, net, est_opt, pf, max_dd, mcl, bw])

    cols = ["Min_Rel_Vol", "Sig_Bars", "Trades", "TP_Hits", "SL_Hits", "Win_Rate%",
            "Net_Pts", "Est_Opt_Pts", "Profit_Factor", "Max_DD", "Max_Consec_Loss",
            "Window_1130_1400_Pts"]
    tab = pd.DataFrame(rows, columns=cols)
    with pd.option_context("display.float_format", lambda x: f"{x:,.2f}", "display.width", 250):
        print(tab.to_string(index=False))


def _opening_pe_signal(df: pd.DataFrame, min_rel_vol: float, window: str) -> pd.Series:
    """PE-only signal with opening-safe warmup features, for a given time window."""
    t = df["ist_time"]
    t0915 = pd.to_datetime("09:15").time()
    t0945 = pd.to_datetime("09:45").time()
    t1455 = pd.to_datetime("14:55").time()
    if window == "opening":
        win = (t >= t0915) & (t < t0945)
    elif window == "post_open":
        win = (t >= t0945) & (t <= t1455)
    else:
        win = (t >= t0915) & (t <= t1455)
    below_vwap_ext = (df["close"] < df["vwap"]) & (df["vwap_distance"].abs() >= 0.0015)
    flow_surge = (df["ofi_imbalance"].abs() >= 0.35) & (df["rel_vol_open_safe"] >= min_rel_vol)
    cvd_div_trap = (df["cvd_change_3"] > 0) & (~df["cvd_aligned"])
    return win & below_vwap_ext & flow_surge & cvd_div_trap


def simulate_opening_pe(df: pd.DataFrame, window: str, min_rel_vol: float = 1.5, tp_pts: float = 45.0) -> pd.DataFrame:
    """SHORT_PE on the opening-window signal, with opening-safe ATR stop."""
    sig = _opening_pe_signal(df, min_rel_vol, window)
    trades = []
    eod_exit_time = pd.to_datetime("15:18").time()
    t1030 = pd.to_datetime("10:30").time()
    t1300 = pd.to_datetime("13:00").time()

    for trade_date, day_df in df.groupby("ist_date"):
        day_df = day_df.reset_index()
        in_pos = False
        entry_price = 0.0
        entry_time = None
        sl_price = 0.0
        tp_price = 0.0
        sl_dist = 0.0

        for i in range(len(day_df) - 1):
            row = day_df.iloc[i]
            next_bar = day_df.iloc[i + 1]

            if in_pos:
                exit_price = None
                exit_reason = None
                if row["high"] >= sl_price:
                    exit_price = sl_price
                    exit_reason = "SL_HIT"
                elif row["low"] <= tp_price:
                    exit_price = tp_price
                    exit_reason = "TP_HIT"
                if exit_price is None and row["ist_time"] >= eod_exit_time:
                    exit_price = row["close"]
                    exit_reason = "EOD_EXIT"
                if exit_price is not None:
                    net_pts = (entry_price - exit_price) - SLIPPAGE_PTS
                    trades.append({
                        "date": trade_date,
                        "entry_time": entry_time,
                        "exit_time": row["timestamp"],
                        "side": "SHORT_PE",
                        "is_11am_window": t1030 <= entry_time.tz_convert("Asia/Kolkata").time() <= t1300,
                        "entry": round(entry_price, 2),
                        "exit": round(exit_price, 2),
                        "sl_dist": round(sl_dist, 2),
                        "net_pts": round(net_pts, 2),
                        "est_opt_pts": round(net_pts * OPTION_DELTA_EST, 2),
                        "reason": exit_reason,
                    })
                    in_pos = False
                continue

            if bool(sig.loc[row["timestamp"]]):
                entry_price = float(next_bar["open"])
                entry_time = next_bar["timestamp"]
                sl_dist = float(np.clip(ATR_MULTIPLIER * row["atr_open_safe"], ATR_SL_MIN, ATR_SL_MAX))
                sl_price = entry_price + sl_dist
                tp_price = entry_price - tp_pts
                in_pos = True

    return pd.DataFrame(trades)


OPENING_SWEEP_REL_VOL = [1.20, 1.50]
OPENING_WINDOWS = [
    ("09:15 - 09:45 IST (OPENING ONLY)", "opening"),
    ("09:45 - 14:55 IST (POST-OPEN BASELINE)", "post_open"),
    ("09:15 - 14:55 IST (FULL SESSION COMBINED)", "full"),
]


def run_opening_window_audit(enriched: pd.DataFrame):
    """Compare OPENING vs POST-OPEN vs FULL-session for the PE-only model."""
    print("\n" + "=" * 100)
    print("  OPENING WINDOW (09:15-09:45 IST) IMPACT ON MODEL C2-LIVE-READY")
    print("=" * 100)
    rows = []
    for mv in OPENING_SWEEP_REL_VOL:
        for wname, wkey in OPENING_WINDOWS:
            sig = _opening_pe_signal(enriched, mv, wkey)
            sig_bars = int(sig.sum())
            tdf = simulate_opening_pe(enriched, wkey, min_rel_vol=mv)
            n = int(len(tdf))
            if n == 0:
                rows.append([mv, wname, sig_bars, 0, 0, 0, float("nan"), 0.0, float("nan"), 0.0, 0])
                continue
            tp_hits = int((tdf["reason"] == "TP_HIT").sum())
            sl_hits = int((tdf["reason"] == "SL_HIT").sum())
            wins = tdf[tdf["net_pts"] > 0]
            losses = tdf[tdf["net_pts"] <= 0]
            wr = len(wins) / n * 100.0
            net = float(tdf["net_pts"].sum())
            gp = float(wins["net_pts"].sum())
            gl = abs(float(losses["net_pts"].sum()))
            pf = gp / gl if gl > 0 else float("inf")
            max_dd = float((tdf["net_pts"].cumsum() - tdf["net_pts"].cumsum().cummax()).min())
            mcl = _max_consec_losses(tdf)
            rows.append([mv, wname, sig_bars, n, tp_hits, sl_hits, wr, net, pf, max_dd, mcl])

    cols = ["Min_Rel_Vol", "Window", "Sig_Bars", "Trades", "TP_Hits", "SL_Hits",
            "Win_Rate%", "Net_Pts", "Profit_Factor", "Max_DD", "Max_Consec_Loss"]
    tab = pd.DataFrame(rows, columns=cols)
    with pd.option_context("display.float_format", lambda x: f"{x:,.2f}", "display.width", 250):
        print(tab.to_string(index=False))


def _ce_breakout_signal(df: pd.DataFrame, min_ofi: float, min_rel_vol: float) -> pd.Series:
    """Bullish CE breakout signal (aligned flow above VWAP), full session."""
    t = df["ist_time"]
    t0915 = pd.to_datetime("09:15").time()
    t1455 = pd.to_datetime("14:55").time()
    full_session_window = (t >= t0915) & (t <= t1455)
    above_vwap_ext = (df["close"] > df["vwap"]) & (df["vwap_distance"].abs() >= 0.0015)
    bullish_flow_surge = (df["ofi_imbalance"] >= min_ofi) & (df["rel_vol_open_safe"] >= min_rel_vol)
    aligned_cvd_surge = (df["cvd_change_3"] > 0)
    return full_session_window & above_vwap_ext & bullish_flow_surge & aligned_cvd_surge


def simulate_model_c2_ce(df: pd.DataFrame, min_ofi: float = 0.15, min_rel_vol: float = 1.5, tp_pts: float = 45.0) -> pd.DataFrame:
    """LONG_CE on the CE breakout signal, opening-safe ATR stop, no BE trail."""
    sig = _ce_breakout_signal(df, min_ofi, min_rel_vol)
    trades = []
    eod_exit_time = pd.to_datetime("15:18").time()
    t1030 = pd.to_datetime("10:30").time()
    t1300 = pd.to_datetime("13:00").time()

    for trade_date, day_df in df.groupby("ist_date"):
        day_df = day_df.reset_index()
        in_pos = False
        entry_price = 0.0
        entry_time = None
        sl_price = 0.0
        tp_price = 0.0
        sl_dist = 0.0

        for i in range(len(day_df) - 1):
            row = day_df.iloc[i]
            next_bar = day_df.iloc[i + 1]

            if in_pos:
                exit_price = None
                exit_reason = None
                if row["low"] <= sl_price:
                    exit_price = sl_price
                    exit_reason = "SL_HIT"
                elif row["high"] >= tp_price:
                    exit_price = tp_price
                    exit_reason = "TP_HIT"
                if exit_price is None and row["ist_time"] >= eod_exit_time:
                    exit_price = row["close"]
                    exit_reason = "EOD_EXIT"
                if exit_price is not None:
                    net_pts = (exit_price - entry_price) - SLIPPAGE_PTS
                    trades.append({
                        "date": trade_date,
                        "entry_time": entry_time,
                        "exit_time": row["timestamp"],
                        "side": "LONG_CE",
                        "is_11am_window": t1030 <= entry_time.tz_convert("Asia/Kolkata").time() <= t1300,
                        "entry": round(entry_price, 2),
                        "exit": round(exit_price, 2),
                        "sl_dist": round(sl_dist, 2),
                        "net_pts": round(net_pts, 2),
                        "est_opt_pts": round(net_pts * OPTION_DELTA_EST, 2),
                        "reason": exit_reason,
                    })
                    in_pos = False
                continue

            if bool(sig.loc[row["timestamp"]]):
                entry_price = float(next_bar["open"])
                entry_time = next_bar["timestamp"]
                sl_dist = float(np.clip(ATR_MULTIPLIER * row["atr_open_safe"], ATR_SL_MIN, ATR_SL_MAX))
                sl_price = entry_price - sl_dist   # long SL below entry
                tp_price = entry_price + tp_pts    # long TP above entry
                in_pos = True

    return pd.DataFrame(trades)


CE_SWEEP_MIN_OFI = [0.10, 0.15, 0.20]
CE_SWEEP_REL_VOL = [1.20, 1.50]
CE_SWEEP_TP = [26.0, 35.0, 45.0]


def run_ce_breakout_sweep(enriched: pd.DataFrame):
    """Sweep (min_ofi, min_rel_vol, tp_pts) for the CE breakout (LONG_CE) model."""
    print("\n" + "=" * 100)
    print("  MODEL C2-CE-BREAKOUT SWEEP (Realistic Bullish OFI · 09:15-14:55 IST)")
    print("=" * 100)
    rows = []
    candidates = []
    for mo in CE_SWEEP_MIN_OFI:
        for mv in CE_SWEEP_REL_VOL:
            for tp in CE_SWEEP_TP:
                sig = _ce_breakout_signal(enriched, mo, mv)
                sig_bars = int(sig.sum())
                tdf = simulate_model_c2_ce(enriched, min_ofi=mo, min_rel_vol=mv, tp_pts=tp)
                n = int(len(tdf))
                if n == 0:
                    rows.append([mo, mv, tp, sig_bars, 0, 0, 0, float("nan"), 0.0, 0.0, float("nan"), 0.0, 0, 0.0])
                    continue
                tp_hits = int((tdf["reason"] == "TP_HIT").sum())
                sl_hits = int((tdf["reason"] == "SL_HIT").sum())
                wins = tdf[tdf["net_pts"] > 0]
                losses = tdf[tdf["net_pts"] <= 0]
                wr = len(wins) / n * 100.0
                net = float(tdf["net_pts"].sum())
                est_opt = float(tdf["est_opt_pts"].sum())
                gp = float(wins["net_pts"].sum())
                gl = abs(float(losses["net_pts"].sum()))
                pf = gp / gl if gl > 0 else float("inf")
                max_dd = float((tdf["net_pts"].cumsum() - tdf["net_pts"].cumsum().cummax()).min())
                mcl = _max_consec_losses(tdf)
                bw = _best_window_1130_1400(tdf)
                rows.append([mo, mv, tp, sig_bars, n, tp_hits, sl_hits, wr, net, est_opt, pf, max_dd, mcl, bw])
                candidates.append({"net": net, "pf": pf, "trades": n, "tdf": tdf,
                                   "min_ofi": mo, "min_rel_vol": mv, "tp": tp})

    cols = ["Min_OFI", "Min_Rel_Vol", "TP_Pts", "Sig_Bars", "Trades", "TP_Hits", "SL_Hits",
            "Win_Rate%", "Net_Pts", "Est_Opt_Pts", "Profit_Factor", "Max_DD", "Max_Consec_Loss",
            "Window_1130_1400_Pts"]
    tab = pd.DataFrame(rows, columns=cols)
    with pd.option_context("display.float_format", lambda x: f"{x:,.2f}", "display.width", 250):
        print(tab.to_string(index=False))

    # Save the best CE variant: highest Net_Pts among positive-Net variants; else
    # best Profit_Factor among variants with >= 5 trades.
    best = None
    positive = [c for c in candidates if c["net"] > 0]
    if positive:
        best = max(positive, key=lambda c: c["net"])
    else:
        qualified = [c for c in candidates if c["trades"] >= 5]
        if qualified:
            best = max(qualified, key=lambda c: c["pf"])

    if best is not None and len(best["tdf"]) > 0:
        out_ce = os.path.join(DATA_DIR, "backtest_trades_model_c2_ce_breakout.csv")
        best["tdf"].to_csv(out_ce, index=False)
        print(f"\n[SAVED] Best CE variant (min_ofi={best['min_ofi']}, min_rel_vol={best['min_rel_vol']}, tp={best['tp']}, net={best['net']:+.2f}) -> {out_ce}")


def _ce_pullback_signal(df: pd.DataFrame, max_vwap_ext: float, min_ofi: float, min_rel_vol: float) -> pd.Series:
    """MODEL C2-CE-PULLBACK: Early/Mid-Trend CE VWAP Pullback & Reclaim (LONG_CE).

    - ``vwap_pullback_zone`` : signed VWAP distance in [-0.0005, +max_vwap_ext]
      (within 0.05% below VWAP up to +0.08%/+0.12% above VWAP).
    - ``prior_dip_or_touch`` : previous 2m bar low dipped/touched near VWAP and
      the current bar closes bullish above VWAP.
    - ``flow_and_volume``    : positive 3-bar CVD change, sufficient relative
      volume and OFI imbalance.
    """
    t = df["ist_time"]
    t0915 = pd.to_datetime("09:15").time()
    t1455 = pd.to_datetime("14:55").time()
    full_session_window = (t >= t0915) & (t <= t1455)

    signed_vwap_dist = (df["close"] - df["vwap"]) / df["vwap"]
    vwap_pullback_zone = (signed_vwap_dist >= -0.0005) & (signed_vwap_dist <= max_vwap_ext)

    prev_low = df.groupby("ist_date")["low"].shift(1)
    prev_vwap = df.groupby("ist_date")["vwap"].shift(1)
    prior_dip_or_touch = (
        ((prev_low - prev_vwap) / prev_vwap <= 0.0005)
        & (df["close"] > df["open"])
        & (df["close"] > df["vwap"])
    )

    flow_and_volume = (
        (df["cvd_change_3"] > 0)
        & (df["rel_vol_open_safe"] >= min_rel_vol)
        & (df["ofi_imbalance"] >= min_ofi)
    )

    return full_session_window & vwap_pullback_zone & prior_dip_or_touch & flow_and_volume


def simulate_model_c2_ce_pullback(df: pd.DataFrame, max_vwap_ext: float = 0.0012,
                                  min_ofi: float = 0.05, min_rel_vol: float = 1.5,
                                  tp_pts: float = 45.0) -> pd.DataFrame:
    """LONG_CE on the VWAP pullback/reclaim signal (no premature breakeven trail).

    - Enter LONG_CE on next 2m bar open (no lookahead bias).
    - SL = clip(2.2 * atr_open_safe, 15, 35), TP = tp_pts.
    - EOD exit at 15:18 IST, 1.0 pt round-trip slippage.
    """
    sig = _ce_pullback_signal(df, max_vwap_ext, min_ofi, min_rel_vol)
    trades = []
    eod_exit_time = pd.to_datetime("15:18").time()
    t1030 = pd.to_datetime("10:30").time()
    t1300 = pd.to_datetime("13:00").time()

    for trade_date, day_df in df.groupby("ist_date"):
        day_df = day_df.reset_index()
        in_pos = False
        entry_price = 0.0
        entry_time = None
        sl_price = 0.0
        tp_price = 0.0
        sl_dist = 0.0

        for i in range(len(day_df) - 1):
            row = day_df.iloc[i]
            next_bar = day_df.iloc[i + 1]

            if in_pos:
                exit_price = None
                exit_reason = None
                if row["low"] <= sl_price:
                    exit_price = sl_price
                    exit_reason = "SL_HIT"
                elif row["high"] >= tp_price:
                    exit_price = tp_price
                    exit_reason = "TP_HIT"
                if exit_price is None and row["ist_time"] >= eod_exit_time:
                    exit_price = row["close"]
                    exit_reason = "EOD_EXIT"
                if exit_price is not None:
                    net_pts = (exit_price - entry_price) - SLIPPAGE_PTS
                    trades.append({
                        "date": trade_date,
                        "entry_time": entry_time,
                        "exit_time": row["timestamp"],
                        "side": "LONG_CE",
                        "is_11am_window": t1030 <= entry_time.tz_convert("Asia/Kolkata").time() <= t1300,
                        "entry": round(entry_price, 2),
                        "exit": round(exit_price, 2),
                        "sl_dist": round(sl_dist, 2),
                        "net_pts": round(net_pts, 2),
                        "est_opt_pts": round(net_pts * OPTION_DELTA_EST, 2),
                        "reason": exit_reason,
                    })
                    in_pos = False
                continue

            if bool(sig.loc[row["timestamp"]]):
                entry_price = float(next_bar["open"])
                entry_time = next_bar["timestamp"]
                sl_dist = float(np.clip(ATR_MULTIPLIER * row["atr_open_safe"], ATR_SL_MIN, ATR_SL_MAX))
                sl_price = entry_price - sl_dist   # long SL below entry
                tp_price = entry_price + tp_pts    # long TP above entry
                in_pos = True

    return pd.DataFrame(trades)


CE_PULLBACK_MAX_VWAP_EXT = [0.0008, 0.0012]
CE_PULLBACK_MIN_OFI = [-0.15, 0.00, 0.05]
CE_PULLBACK_MIN_REL_VOL = [1.20, 1.50]
CE_PULLBACK_TP = [26.0, 35.0, 45.0]


def run_ce_pullback_sweep(enriched: pd.DataFrame):
    """Sweep (max_vwap_ext, min_ofi, min_rel_vol, tp_pts) for the CE VWAP pullback model."""
    print("\n" + "=" * 100)
    print("  MODEL C2-CE-PULLBACK SWEEP (VWAP Reclaim · 09:15-14:55 IST)")
    print("=" * 100)
    rows = []
    candidates = []
    for mx in CE_PULLBACK_MAX_VWAP_EXT:
        for mo in CE_PULLBACK_MIN_OFI:
            for mv in CE_PULLBACK_MIN_REL_VOL:
                for tp in CE_PULLBACK_TP:
                    sig = _ce_pullback_signal(enriched, mx, mo, mv)
                    sig_bars = int(sig.sum())
                    tdf = simulate_model_c2_ce_pullback(enriched, max_vwap_ext=mx, min_ofi=mo, min_rel_vol=mv, tp_pts=tp)
                    n = int(len(tdf))
                    if n == 0:
                        rows.append([mx * 100, mo, mv, tp, sig_bars, 0, 0, 0, float("nan"), 0.0, 0.0, float("nan"), 0.0, 0])
                        continue
                    tp_hits = int((tdf["reason"] == "TP_HIT").sum())
                    sl_hits = int((tdf["reason"] == "SL_HIT").sum())
                    wins = tdf[tdf["net_pts"] > 0]
                    losses = tdf[tdf["net_pts"] <= 0]
                    wr = len(wins) / n * 100.0
                    net = float(tdf["net_pts"].sum())
                    est_opt = float(tdf["est_opt_pts"].sum())
                    gp = float(wins["net_pts"].sum())
                    gl = abs(float(losses["net_pts"].sum()))
                    pf = gp / gl if gl > 0 else float("inf")
                    max_dd = float((tdf["net_pts"].cumsum() - tdf["net_pts"].cumsum().cummax()).min())
                    mcl = _max_consec_losses(tdf)
                    rows.append([mx * 100, mo, mv, tp, sig_bars, n, tp_hits, sl_hits, wr, net, est_opt, pf, max_dd, mcl])
                    candidates.append({"net": net, "pf": pf, "trades": n, "tdf": tdf,
                                       "max_vwap_ext": mx, "min_ofi": mo, "min_rel_vol": mv, "tp": tp})

    cols = ["Max_Ext%", "Min_OFI", "Min_Rel_Vol", "TP_Pts", "Sig_Bars", "Trades", "TP_Hits", "SL_Hits",
            "Win_Rate%", "Net_Pts", "Est_Opt_Pts", "Profit_Factor", "Max_DD", "Max_Consec_Loss"]
    tab = pd.DataFrame(rows, columns=cols)
    tab = tab.sort_values("Net_Pts", ascending=False).reset_index(drop=True)
    with pd.option_context("display.float_format", lambda x: f"{x:,.2f}", "display.width", 250):
        print(tab.head(12).to_string(index=False))

    # Save the best variant meeting the save criteria: Net_Pts > 0 AND Trades >= 6.
    qualified = [c for c in candidates if c["net"] > 0 and c["trades"] >= 6]
    best = max(qualified, key=lambda c: c["net"]) if qualified else None

    if best is not None and len(best["tdf"]) > 0:
        out_pb = os.path.join(DATA_DIR, "backtest_trades_model_c2_ce_pullback.csv")
        best["tdf"].to_csv(out_pb, index=False)
        print(f"\n[SAVED] Best C2-CE-PULLBACK variant (max_ext={best['max_vwap_ext']:.4f}, min_ofi={best['min_ofi']}, "
              f"min_rel_vol={best['min_rel_vol']}, tp={best['tp']}, net={best['net']:+.2f}, trades={best['trades']}) -> {out_pb}")
    else:
        print("\n[SAVED] No C2-CE-PULLBACK variant met the save criteria (Net_Pts > 0 and Trades >= 6).")


def main():
    ml_df = load_deduplicated_ml_features()
    ist_idx = ml_df.index.tz_convert("Asia/Kolkata")
    min_date = ist_idx.min().date()
    max_date = ist_idx.max().date()
    unique_days = ist_idx.normalize().nunique()
    print(f"DATASET LOADED: {unique_days} trading days ({min_date} to {max_date})")

    ohlc_1m = load_or_fetch_1m_ohlc(feature_min_date=min_date, feature_max_date=max_date)
    merged = build_merged_2m_dataset(ohlc_1m, ml_df)
    enriched = compute_dashboard_indicators(merged)

    diagnose_c2_breakeven(enriched)

    total_bull_traps = int(enriched["bull_trap"].sum())
    total_bear_traps = int(enriched["bear_trap"].sum())
    print(f"\n[FILTER STATS] Detected {total_bull_traps} Bull Traps & {total_bear_traps} Bear Traps across 2m bars.")

    n_c1 = int(enriched["c1_long_sig"].sum() + enriched["c1_short_sig"].sum())
    print(f"[MODEL C] ML-validated extension+flow signal bars (C1 & C2 share these): {n_c1}")

    n_live_pe = int(enriched["c2_live_pe_sig"].sum())
    print(f"[MODEL C] Scale-free PE-only signal bars (c2_live_pe_sig): {n_live_pe}")

    trades_before = simulate_strategy(enriched, use_cvd_filter=False, use_atr_sl=False)
    trades_after = simulate_strategy(enriched, use_cvd_filter=True, use_atr_sl=True)
    trades_c1 = simulate_model_c(enriched, long_col="c1_long_sig", short_col="c1_short_sig")
    trades_c2a = simulate_model_c2_fixed(enriched, variant="A")
    trades_c2b = simulate_model_c2_fixed(enriched, variant="B")
    trades_c2_live = simulate_model_c2_live_pe(enriched)

    summarize_results("MODEL A — BEFORE (No CVD Trap Filter, Wide 70pt SL)", trades_before)
    summarize_results("MODEL B — AFTER  (2m CVD Trap Filter + 2.2x ATR SL)", trades_after)

    summarize_results("MODEL C1 — TRAP MEAN-REVERSION (Fade Overextended VWAP + CVD Div)", trades_c1)
    summarize_results("MODEL C2-A — CONTINUATION (No Early BE, ATR SL + 26pt TP)", trades_c2a)
    summarize_results("MODEL C2-B — CONTINUATION (Delayed +22pt Lock, +6pt Trail, 32pt TP)", trades_c2b)

    summarize_live_ready("MODEL C2-LIVE-READY (Scale-Free PE-Only · 45pt TP)", trades_c2_live)

    print_time_of_day(trades_c2a, trades_c2b)

    run_target_sweep(enriched)

    run_rel_vol_sweep(enriched)

    run_opening_window_audit(enriched)

    run_ce_breakout_sweep(enriched)

    run_ce_pullback_sweep(enriched)

    out_before = os.path.join(DATA_DIR, "backtest_trades_before.csv")
    out_after = os.path.join(DATA_DIR, "backtest_trades_after.csv")
    out_c1 = os.path.join(DATA_DIR, "backtest_trades_model_c1_reversion.csv")
    out_c2a = os.path.join(DATA_DIR, "backtest_trades_model_c2a.csv")
    out_c2b = os.path.join(DATA_DIR, "backtest_trades_model_c2b.csv")
    out_c2_live = os.path.join(DATA_DIR, "backtest_trades_model_c2_live_pe.csv")
    trades_before.to_csv(out_before, index=False)
    trades_after.to_csv(out_after, index=False)
    trades_c1.to_csv(out_c1, index=False)
    trades_c2a.to_csv(out_c2a, index=False)
    trades_c2b.to_csv(out_c2b, index=False)
    trades_c2_live.to_csv(out_c2_live, index=False)
    print(f"\n[SAVED] Detailed trade logs written to:\n  - {out_before}\n  - {out_after}\n  - {out_c1}\n  - {out_c2a}\n  - {out_c2b}\n  - {out_c2_live}")


if __name__ == "__main__":
    main()