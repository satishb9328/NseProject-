"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║   NIFTY 200 — SMC & PRICE ACTION REVERSAL SCANNER                                ║
║   VERSION 4.1 — DTYPE-AWARE TIMESTAMP PARSING                                    ║
║                                                                                  ║
║   HOW TO KNOW v4.1 IS RUNNING:                                                   ║
║     • Banner below says  v4.1  and prints  SCRIPT VERSION: 4.1                   ║
║     • probe_files (if ever shown) prints  dtype=...  and NO  first=...           ║
║     • Daily files load with ZERO "(dropped NNNN corrupt)" lines                  ║
║                                                                                  ║
║   v4.1 CORE FIX:                                                                 ║
║     pd.to_numeric on a datetime64 column silently converts it to raw integers    ║
║     → epoch heuristic misreads it → year ~30,000 → every row dropped.            ║
║     Now: datetime64 columns are used AS-IS; only genuinely numeric columns are   ║
║     epoch-decoded via a s/ms/µs/ns magnitude ladder (weekly files are epoch µs). ║
║                                                                                  ║
║   DATA     : Local Upstox parquet (Day + Weeks folders, NSE_EQ_<ISIN>.parquet)   ║
║   Yahoo    : ONLY for the NIFTY regime check (trimmed to the data's as-of date)  ║
║   "TODAY"  : The data's own last candle — stale DB = HISTORICAL snapshot + banner║
║   'Open Interest' column in your files is ignored (stocks have no OI).           ║
║   ⚠ EOD scanner. Backtest before trading anything this flags. Not advice.        ║
╚══════════════════════════════════════════════════════════════════════════════════╝
"""

import time
import warnings
from datetime import datetime
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import yfinance as yf   # regime check only

warnings.filterwarnings("ignore", category=FutureWarning)

__version__ = "4.1"

# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION — every tunable in one place
# ════════════════════════════════════════════════════════════════════════════════

UPSTOX_ROOT = Path(r"D:\Api data\ML_HISTO_DATA\Upstox Data")
DAY_DIR  = UPSTOX_ROOT / "Day Data"
WEEK_DIR = UPSTOX_ROOT / "Weeks Data"

UNIVERSE_CACHE_FILE     = Path("nifty200_symbol_isin_map.csv")
UNIVERSE_CACHE_TTL_DAYS = 7

# ── Freshness policy ──
SKIP_IF_OLDER_THAN_DAYS = None   # None = never skip (scan as-of data date). Set 7 for live-only.
STALE_WARN_DAYS = 10             # banner threshold: system clock vs data date
LAG_WARN_DAYS   = 7              # warn if one stock lags the universe consensus date

# ── Data hygiene ──
SPLIT_GAP_WARN   = 0.25          # warn on >25% overnight gaps (unadjusted split/bonus?)
MIN_ROWS_HARD    = 120           # below this, a file is unusable
DATE_SANITY_LO   = "1990-01-01"  # timestamps outside this window are corrupt → dropped
DATE_SANITY_HI   = "2100-01-01"

# ── Qualification ──
MIN_SCORE          = 5           # proximity alone (max 3 pts) can NEVER qualify
MIN_CONFIRMATIONS  = 1           # ≥1 of: sweep / MSS / FVG / hammer / weekly confluence
MAX_SCORE          = 15

# ── Signal tuning ──
LOOKBACK_52W       = 252
SWEEP_WINDOW       = 3
SWEEP_MIN_RECLAIM  = 0.01        # close must reclaim old low by ≥1%
SWING_LOOKBACK     = 3           # fractal width each side (no lookahead: shifted)
FVG_LOOKBACK       = 5
VOL_MA             = 60
VOL_DRYUP_RATIO    = 0.7

# ── Hard filters ──
REQUIRE_UP_CLOSE   = True
RSI_PERIOD         = 14
RSI_MAX            = 45
CRASH_LOOKBACK     = 10
CRASH_MAX_DROP     = 0.25
LOOKBACK_MAX_PROX  = 0.10

FALLBACK_SYMBOLS = [
    "RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "INFY", "SBIN", "ITC", "LT",
    "BHARTIARTL", "AXISBANK", "KOTAKBANK", "MARUTI", "TITAN", "ASIANPAINT", "BAJFINANCE",
]

# ════════════════════════════════════════════════════════════════════════════════
# UNIVERSE: Symbol → ISIN RESOLUTION
# ════════════════════════════════════════════════════════════════════════════════

def _http_get(url: str) -> str:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                             "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"}
    r = requests.get(url, headers=headers, timeout=10)
    r.raise_for_status()
    return r.text


def _find_col(df: pd.DataFrame, needle: str) -> str | None:
    for c in df.columns:
        if needle in str(c).strip().lower():
            return c
    return None


def _save_universe_cache(recs: list[dict]):
    """Cache only if ≥80% resolved — never poison the cache with Nones."""
    try:
        with_isin = sum(1 for r in recs if r.get("isin"))
        if with_isin >= 0.8 * len(recs):
            pd.DataFrame(recs).to_csv(UNIVERSE_CACHE_FILE, index=False)
            print(f"  💾 Cached symbol↔ISIN map → {UNIVERSE_CACHE_FILE}")
    except Exception:
        pass


def get_nifty200_universe() -> list[dict]:
    """Returns [{'symbol': 'RELIANCE', 'isin': 'INE002A01018'}, ...]"""
    # 1) Local cache — works fully offline
    if UNIVERSE_CACHE_FILE.exists():
        age = time.time() - UNIVERSE_CACHE_FILE.stat().st_mtime
        if age < UNIVERSE_CACHE_TTL_DAYS * 86400:
            m = pd.read_csv(UNIVERSE_CACHE_FILE, dtype=str).dropna(subset=["symbol"])
            if len(m) >= 150:
                print(f"  ✅ Universe from cache: {len(m)} symbols (ISINs included)")
                return m.to_dict("records")

    # 2) NSE NIFTY-200 list (contains 'ISIN Code' column)
    try:
        df = pd.read_csv(StringIO(_http_get(
            "https://archives.nseindia.com/content/indices/ind_nifty200list.csv")))
        sym_col, isin_col = _find_col(df, "symbol"), _find_col(df, "isin")
        recs = [{"symbol": str(s).strip().upper(),
                 "isin":  (str(i).strip().upper() if isin_col and pd.notna(i) else None)}
                for s, i in zip(df[sym_col],
                                df[isin_col] if isin_col else [None] * len(df))]
        print(f"  ✅ NIFTY 200 list from NSE: {len(recs)} symbols | "
              f"ISIN column: {'✓ found' if isin_col else '✗ MISSING'}")
        _save_universe_cache(recs)
        return recs
    except Exception as e:
        print(f"  ⚠ NIFTY-200 list fetch failed: {e}")

    # 3) Fallback: NSE full equity master (has SYMBOL + ISIN for everything)
    eq_map = {}
    try:
        df = pd.read_csv(StringIO(_http_get(
            "https://archives.nseindia.com/content/equities/EQUITY_L.csv")))
        df.columns = [str(c).strip() for c in df.columns]
        sym_col, isin_col = _find_col(df, "symbol"), _find_col(df, "isin")
        ser_col = _find_col(df, "series")
        if ser_col:
            df = df[df[ser_col].astype(str).str.strip() == "EQ"]
        eq_map = dict(zip(df[sym_col].astype(str).str.strip().str.upper(),
                          df[isin_col].astype(str).str.strip().str.upper()))
        print(f"  ✅ EQUITY_L.csv mapping: {len(eq_map)} symbols → ISIN")
    except Exception as e:
        print(f"  ⚠ EQUITY_L.csv also failed: {e}")

    recs = [{"symbol": s, "isin": eq_map.get(s)} for s in FALLBACK_SYMBOLS]
    _save_universe_cache(recs)
    return recs

# ════════════════════════════════════════════════════════════════════════════════
# LOCAL PARQUET DATA LAYER
# ════════════════════════════════════════════════════════════════════════════════

COLUMN_ALIASES = {
    "date": "Date", "datetime": "Date", "time": "Date", "timestamp": "Date",
    "ts": "Date", "candle_time": "Date",
    "open": "Open", "high": "High", "low": "Low",
    "close": "Close", "close_price": "Close",
    "volume": "Volume", "vol": "Volume", "total_quantity": "Volume",
}   # 'Open Interest' intentionally NOT mapped → dropped by the column subset


def _parse_dates(series: pd.Series) -> pd.Series:
    """
    ★ v4.1 core fix — dtype-aware, never corrupts real datetimes ★

      • datetime64 (any unit)   → use directly. NEVER call to_numeric on these:
        pandas would return raw ints (ns/µs since epoch) and any epoch heuristic
        then mangles them into year-30,000 garbage.
      • numeric (int/float)     → epoch numbers. Decode via magnitude ladder:
            seconds ≤ ~4e9  |  ms ≤ ~4e12  |  µs ≤ ~4e15  |  else ns
        (your WEEKLY parquet files are epoch MICROSECONDS — this decodes them right)
      • object/strings          → try numeric; if mostly non-numeric, parse as text.
    """
    s = series

    # ── Branch 1: already real datetimes → AS-IS ──
    if pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s, errors="coerce", utc=True)

    # ── Branch 2: numeric epoch values → magnitude ladder ──
    if pd.api.types.is_numeric_dtype(s):
        nums = s
    else:
        nums = pd.to_numeric(s, errors="coerce")
        if nums.notna().mean() < 0.9:                    # strings like '2024-01-05'
            return pd.to_datetime(s, errors="coerce", utc=True)

    vals = nums.dropna()
    if vals.empty:
        return pd.to_datetime(pd.Series([pd.NaT] * len(s), index=s.index), utc=True)

    m = float(vals.abs().max())
    if   m > 1e17: unit = "ns"
    elif m > 1e14: unit = "us"       # ← your weekly files live here (~1.79e15)
    elif m > 1e11: unit = "ms"
    else:          unit = "s"

    return pd.to_datetime(nums, unit=unit, errors="coerce", utc=True)


def build_file_index(folder: Path) -> dict[str, Path]:
    """{stem.upper(): path} built ONCE — O(1) lookups for all 200 symbols."""
    idx = {}
    if folder.exists():
        for p in folder.glob("*.parquet"):
            idx[p.stem.strip().upper()] = p
    return idx


def resolve_file(index: dict[str, Path], isin) -> Path | None:
    if isin is None:
        return None
    isin = str(isin).strip().upper()
    if isin in ("", "NAN", "NONE"):
        return None
    p = index.get(f"NSE_EQ_{isin}")              # exact: NSE_EQ_<ISIN>.parquet
    if p:
        return p
    for k, v in index.items():                   # loose: stem contains ISIN
        if isin in k:
            return v
    return None


def _normalise_ohlcv(df: pd.DataFrame) -> pd.DataFrame | None:
    """Any parquet → tz-aware IST DataFrame[Open, High, Low, Close, Volume]."""
    try:
        df = df.copy()
        df.columns = [str(c).strip().lower() for c in df.columns]
        df = df.rename(columns={k: v for k, v in COLUMN_ALIASES.items() if k in df.columns})
        required = {"Date", "Open", "High", "Low", "Close", "Volume"}
        if not required.issubset(df.columns):
            return None

        df["Date"] = _parse_dates(df["Date"])   # ← the fix: dtype-aware, never to_numeric a datetime col

        # Sanitisation backstop: NSE data must fall inside 1990–2100
        lo = pd.Timestamp(DATE_SANITY_LO, tz="UTC")
        hi = pd.Timestamp(DATE_SANITY_HI, tz="UTC")
        sane = (df["Date"] >= lo) & (df["Date"] <= hi)
        if not sane.all():
            print(f"     (dropped {int((~sane).sum())} corrupt timestamp row(s))")
            df = df[sane]

        df = df.dropna(subset=["Date"]).set_index("Date").sort_index()
        df = df[~df.index.duplicated(keep="last")]
        df.index = df.index.tz_convert("Asia/Kolkata")
        df = df[["Open", "High", "Low", "Close", "Volume"]].astype(float).dropna()
        return df if len(df) else None
    except Exception:
        return None


def drop_partial_candle(df: pd.DataFrame) -> pd.DataFrame:
    """Remove today's incomplete candle until 15:30 IST close + 90 min buffer.
    No-op for historical snapshots. Hardened: one weird file can't kill the run."""
    if df.empty:
        return df
    try:
        now_ist = pd.Timestamp.now(tz="Asia/Kolkata")
        cutoff = now_ist.normalize() + pd.Timedelta(hours=15, minutes=30) \
                 + pd.Timedelta(minutes=90)
        if df.index[-1].date() == now_ist.date() and now_ist < cutoff:
            return df.iloc[:-1]
    except (ValueError, OverflowError):
        return df
    return df


def load_universe(universe: list[dict], day_idx, week_idx):
    """
    Loads every resolvable stock. Staleness never silently drops a stock
    (unless SKIP_IF_OLDER_THAN_DAYS is set).
    Returns (frames, weekly_frames, failed, as_of) where as_of = consensus
    last-candle date → the scanner's reference 'today'.
    """
    today_ist = pd.Timestamp.now(tz="Asia/Kolkata").normalize()
    meta, failed = [], []

    for rec in universe:
        sym, isin = rec["symbol"], rec.get("isin")

        f_day = resolve_file(day_idx, isin)
        if f_day is None:
            failed.append((sym, "no parquet for ISIN"))
            continue
        df = _normalise_ohlcv(pd.read_parquet(f_day))
        if df is None:
            failed.append((sym, "unreadable columns"))
            continue
        if len(df) < MIN_ROWS_HARD:
            failed.append((sym, f"too few rows ({len(df)})"))
            continue

        wall_age = (today_ist - df.index[-1].normalize()).days
        if SKIP_IF_OLDER_THAN_DAYS is not None and wall_age > SKIP_IF_OLDER_THAN_DAYS:
            failed.append((sym, f"stale vs system clock ({wall_age}d)"))
            continue

        # Unadjusted-split detector
        gaps = df["Close"].pct_change().dropna()
        gaps = gaps[gaps.abs() > SPLIT_GAP_WARN]
        if len(gaps):
            print(f"  ⚠ {sym}: gap(s) >{SPLIT_GAP_WARN:.0%} "
                  f"(last {gaps.index[-1].date()}) — possible UNADJUSTED split/bonus!")

        wk = None
        f_wk = resolve_file(week_idx, isin)
        if f_wk:
            wtmp = _normalise_ohlcv(pd.read_parquet(f_wk))
            if wtmp is not None and len(wtmp) >= 10:
                wk = drop_partial_candle(wtmp)

        meta.append((sym, drop_partial_candle(df), wk, wall_age))

    if not meta:
        return {}, {}, failed, None

    # ★ Consensus as-of date — the scanner's reference 'today' ★
    as_of = max(m[1].index[-1] for m in meta).normalize()

    frames, weekly_frames = {}, {}
    laggers, short_hist = [], []
    for sym, df, wk, _wall_age in meta:
        lag = (as_of - df.index[-1].normalize()).days
        if lag > LAG_WARN_DAYS:
            laggers.append((sym, lag))          # still scanned; Signal_Date shows its date
        if len(df) < LOOKBACK_52W + 10:
            short_hist.append((sym, len(df)))
        frames[sym] = df
        if wk is not None:
            weekly_frames[sym] = wk

    if short_hist:
        ex = ", ".join(f"{s}({n}r)" for s, n in short_hist[:5])
        print(f"  ℹ {len(short_hist)} stocks <260 rows → adaptive lookback. e.g. {ex}")
    if laggers:
        ex = ", ".join(f"{s}(-{l}d)" for s, l in laggers[:5])
        print(f"  ℹ {len(laggers)} stocks lag the as-of date. e.g. {ex}")

    return frames, weekly_frames, failed, as_of

# ════════════════════════════════════════════════════════════════════════════════
# INDICATORS
# ════════════════════════════════════════════════════════════════════════════════

def compute_rsi(close: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
    """Wilder-smoothed RSI."""
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1/period, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/period, min_periods=period).mean()
    return 100 - (100 / (1 + gain / loss))


def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Wilder-smoothed Average True Range."""
    tr = pd.concat([
        df["High"] - df["Low"],
        (df["High"] - df["Close"].shift()).abs(),
        (df["Low"] - df["Close"].shift()).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, min_periods=period).mean()


def has_unmitigated_bullish_fvg(df: pd.DataFrame, lookback: int = FVG_LOOKBACK) -> bool:
    """Bullish FVG = Low[t] > High[t-2]. Counts only if price hasn't traded
    back below the gap bottom since formation (unfilled = real demand zone)."""
    lows, highs = df["Low"].values, df["High"].values
    for i in range(len(df) - lookback, len(df)):
        gap_bottom, gap_top = highs[i - 2], lows[i]
        if gap_top > gap_bottom:
            post = lows[i + 1:]
            if post.size == 0 or post.min() > gap_bottom:
                return True
    return False


def weekly_context(daily_df: pd.DataFrame, weekly_df: pd.DataFrame | None) -> tuple[bool, str]:
    """HTF confluence — uses your real weekly parquet when available,
    falls back to resampling daily. Bonus: weekly holding swing low + up-close."""
    if weekly_df is not None and len(weekly_df) >= 10:
        wk = weekly_df
    else:
        wk = daily_df.resample("W").agg(
            {"Open": "first", "High": "max", "Low": "min",
             "Close": "last", "Volume": "sum"}).dropna()
    if len(wk) < 10:
        return False, ""
    last = wk.iloc[-1]
    holding = last["Low"] > wk["Low"].tail(4).min() * 0.995
    up_week = last["Close"] > last["Open"]
    return (holding and up_week), "Weekly HTF Confluence" if (holding and up_week) else ""

# ════════════════════════════════════════════════════════════════════════════════
# SIGNAL ENGINE
# ════════════════════════════════════════════════════════════════════════════════

def scan(df: pd.DataFrame, symbol: str, weekly_df: pd.DataFrame | None = None) -> dict | None:
    if df.empty or len(df) < MIN_ROWS_HARD:
        return None

    # Adaptive lookback — use whatever history the file actually has
    eff_lb = min(LOOKBACK_52W, len(df) - SWEEP_WINDOW - 2)
    lb_note = "" if eff_lb >= LOOKBACK_52W else f" (lookback {eff_lb}d)"

    close = df["Close"]
    last_close, prev_close = close.iloc[-1], close.iloc[-2]

    # ── HARD FILTER 1: demand must exist today ──
    if REQUIRE_UP_CLOSE and last_close <= prev_close:
        return None

    # ── HARD FILTER 2: falling knife (waterfall declines are usually news) ──
    if len(df) > CRASH_LOOKBACK + 1:
        crash_ret = last_close / close.iloc[-(CRASH_LOOKBACK + 1)] - 1
        if crash_ret < -CRASH_MAX_DROP:
            return None

    # ── HARD FILTER 3: oversold-but-not-dead ──
    rsi_val = compute_rsi(close).iloc[-1]
    if pd.isna(rsi_val) or rsi_val > RSI_MAX:
        return None

    # ── 52W-low proximity (gatekeeper, max 3 pts — cannot qualify alone) ──
    low_52w = df["Low"].tail(eff_lb).min()
    proximity = (last_close - low_52w) / low_52w
    if proximity > LOOKBACK_MAX_PROX:
        return None

    # ── SMC 1: STRICT liquidity sweep ──
    old_low = df["Low"].shift(SWEEP_WINDOW).rolling(eff_lb).min().iloc[-1]
    recent_low = df["Low"].tail(SWEEP_WINDOW).min()
    sweep = (pd.notna(old_low) and recent_low < old_low
             and last_close >= old_low * (1 + SWEEP_MIN_RECLAIM))

    # ── SMC 2: MSS — 3-bar fractal swings, shifted = confirmation lag, no lookahead ──
    w = SWING_LOOKBACK
    is_swing = df["High"] >= df["High"].rolling(2 * w + 1, center=True).max()
    swing_known = (pd.Series(np.where(is_swing, df["High"], np.nan), index=df.index)
                     .shift(w).ffill())
    last_swing_high = swing_known.iloc[-1]
    mss = pd.notna(last_swing_high) and last_close > last_swing_high

    # ── SMC 3: FVG still unfilled ──
    fvg = has_unmitigated_bullish_fvg(df)

    # ── Price action: hammer ──
    c = df.iloc[-1]
    body = abs(c["Close"] - c["Open"])
    lower_wick = min(c["Open"], c["Close"]) - c["Low"]
    upper_wick = c["High"] - max(c["Open"], c["Close"])
    hammer = body > 0 and lower_wick >= 2 * body and upper_wick < body

    # ── Volume dry-up ──
    vol_ma = df["Volume"].rolling(min(VOL_MA, max(20, len(df) // 3))).mean().iloc[-1]
    dry_up = pd.notna(vol_ma) and c["Volume"] < vol_ma * VOL_DRYUP_RATIO

    # ── SCORING ──
    score, reasons, confirmations = 0, [], 0

    if proximity <= 0.03:
        score += 3; reasons.append(f"AT 52W LOW (+{proximity*100:.1f}%){lb_note}")
    elif proximity <= 0.05:
        score += 2; reasons.append(f"NEAR 52W LOW (+{proximity*100:.1f}%){lb_note}")
    else:
        score += 1; reasons.append(f"CLOSE TO 52W LOW (+{proximity*100:.1f}%){lb_note}")

    if sweep:  score += 3; confirmations += 1; reasons.append("Liquidity Sweep (strict)")
    if mss:    score += 3; confirmations += 1; reasons.append("MSS Breakout")
    if fvg:    score += 2; confirmations += 1; reasons.append("Bullish FVG (unfilled)")
    if hammer: score += 2; confirmations += 1; reasons.append("Hammer Candle")
    if dry_up: score += 1; reasons.append("Volume Dry-Up")

    wk_ok, wk_reason = weekly_context(df, weekly_df)
    if wk_ok:
        score += 1; confirmations += 1; reasons.append(wk_reason)

    # ★ Proximity alone can never qualify ★
    if score < MIN_SCORE or confirmations < MIN_CONFIRMATIONS:
        return None

    # ── Actionable trade plan ──
    atr_val = compute_atr(df).iloc[-1]
    stop = recent_low - 0.5 * atr_val
    target = last_swing_high if pd.notna(last_swing_high) else last_close + 2 * atr_val
    if target <= last_close:
        target = last_close + 2 * atr_val
    risk = last_close - stop
    rr = round((target - last_close) / risk, 2) if risk > 0 else None

    return {
        "Symbol": symbol, "Price": round(last_close, 2),
        "52W_Low": round(low_52w, 2), "Proximity_%": round(proximity * 100, 2),
        "RSI": round(rsi_val, 1), "ATR": round(atr_val, 2),
        "Stop_Loss": round(stop, 2), "Target": round(target, 2), "R:R": rr,
        "Score": score, "Confirms": confirmations,
        "Signal": "🚀 SMC REVERSAL SETUP", "Reasons": " | ".join(reasons),
        "Signal_Date": df.index[-1].date(),
    }

# ════════════════════════════════════════════════════════════════════════════════
# REGIME — computed AS-OF the data date (no time-period mixing)
# ════════════════════════════════════════════════════════════════════════════════

def get_index_regime(as_of) -> tuple[bool, str]:
    try:
        n = yf.download("^NSEI", period="5y", interval="1d", auto_adjust=True, progress=False)
        if isinstance(n.columns, pd.MultiIndex):
            n.columns = n.columns.get_level_values(0)
        n = n.dropna(subset=["Close"])
        if n.index.tz is None:
            n.index = n.index.tz_localize("UTC")
        n.index = n.index.tz_convert("Asia/Kolkata")
        if as_of is not None:
            n = n[n.index.normalize() <= as_of]
        as_of_txt = f" as-of {as_of.date()}" if as_of is not None else ""
        if len(n) < 210:
            return True, f"NIFTY regime UNKNOWN{as_of_txt} (insufficient trimmed history)"
        sma = n["Close"].rolling(200).mean().iloc[-1]
        last = n["Close"].iloc[-1]
        return (last > sma,
                f"NIFTY {'ABOVE' if last > sma else 'BELOW'} 200-SMA{as_of_txt} "
                f"({last:.0f} vs {sma:.0f})")
    except Exception as e:
        return True, f"NIFTY regime UNKNOWN ({e})"

# ════════════════════════════════════════════════════════════════════════════════
# DIAGNOSTICS
# ════════════════════════════════════════════════════════════════════════════════

def probe_files(folder: Path):
    """v4.1 signature: prints dtype= and NO first= — proves which version is running."""
    print("  🔍 FILE PROBE (first 2 parquet files):")
    for p in sorted(folder.glob("*.parquet"))[:2]:
        try:
            d = pd.read_parquet(p)
            print(f"     {p.name!r}: rows={len(d)}  cols={list(d.columns)}  "
                  f"dtype={d[d.columns[0]].dtype}")
        except Exception as e:
            print(f"     {p.name!r}: READ ERROR → {e}")


def print_failure_summary(failed: list[tuple[str, str]]):
    if not failed:
        return
    groups = {}
    for sym, why in failed:
        groups.setdefault(why.split(" (")[0], []).append(sym)
    print(f"  ⚠ SKIPPED {len(failed)}: " +
          ", ".join(f"{len(v)}× {k}" for k, v in groups.items()))
    for k, v in groups.items():
        print(f"     • {k}: {', '.join(v[:6])}{' ...' if len(v) > 6 else ''}")

# ════════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════════

def main():
    t0 = time.time()

    print("=" * 100)
    print(f"  ★  NIFTY 200 — SMC ACCUMULATION SCANNER  VERSION {__version__}  "
          f"(DTYPE-AWARE PARSING)")
    print(f"      SCRIPT VERSION: {__version__}   ← if this does not say 4.1, you ran an old file")
    print("=" * 100)

    universe = get_nifty200_universe()

    day_idx, week_idx = build_file_index(DAY_DIR), build_file_index(WEEK_DIR)
    print(f"  📂 Indexed {len(day_idx)} day files / {len(week_idx)} week files")

    isins = [str(r.get("isin")).strip().upper() for r in universe
             if r.get("isin") and str(r.get("isin")).upper() not in ("NAN", "NONE", "")]
    exact = sum(1 for i in isins if f"NSE_EQ_{i}" in day_idx)
    print(f"  🔍 ISIN match probe: exact {exact}/{len(isins)}")

    print(f"\n  Loading local parquet for {len(universe)} symbols...")
    frames, weekly_frames, failed, as_of = load_universe(universe, day_idx, week_idx)
    print(f"  ✅ Usable: {len(frames)}/{len(universe)}  |  weekly files: {len(weekly_frames)}")

    print_failure_summary(failed)

    if not frames:
        probe_files(DAY_DIR)
        print("\n  ❌ Nothing loaded — paste this full output back.")
        return

    # ── ★ STALENESS BANNER — impossible to miss ★ ──
    db_age = (pd.Timestamp.now(tz="Asia/Kolkata").normalize() - as_of).days
    print("\n  " + "▲" * 98)
    print(f"  ▲ DATA AS-OF: {as_of.date()}   (system clock is {db_age} days ahead of it)")
    if db_age > STALE_WARN_DAYS:
        print(f"  ▲ ⚠ LOCAL DB IS STALE → all signals below are a HISTORICAL snapshot of "
              f"{as_of.date()}, NOT current setups.")
        print("  ▲   Re-sync your Upstox downloader for live signals. If today is NOT "
              "actually that far ahead, check your Windows clock.")
    print("  " + "▲" * 98 + "\n")

    above, regime_txt = get_index_regime(as_of)
    print(f"  Regime    : {regime_txt}")
    print(f"  Thresholds: score ≥ {MIN_SCORE} AND ≥ {MIN_CONFIRMATIONS} confirm(s) | "
          f"RSI ≤ {RSI_MAX} | crash filter {CRASH_MAX_DROP:.0%}/{CRASH_LOOKBACK}d")

    print("  Sample (verify prices vs a chart for that date!):")
    for sym in list(frames)[:3]:
        d = frames[sym]
        print(f"     {sym:12s} → {len(d)} candles, close Rs.{d['Close'].iloc[-1]:.2f} "
              f"on {d.index[-1].date()}")

    # ── Scan ──
    results = []
    for i, (sym, df) in enumerate(frames.items(), 1):
        try:
            r = scan(df, sym, weekly_frames.get(sym))
            if r:
                results.append(r)
                print(f"  ⚡ SETUP: {r['Symbol']:12s} | Score {r['Score']}/{MAX_SCORE} | "
                      f"{r['Confirms']} confirms | {r['Reasons']}")
        except Exception:
            pass
        if i % 50 == 0:
            print(f"  ... scanned {i}/{len(frames)}")

    results.sort(key=lambda r: (r["Score"], -r["Proximity_%"]), reverse=True)

    print("\n" + "=" * 100)
    hdr = (f"  TOP SMC REVERSAL CANDIDATES  (AS OF {as_of.date()} — HISTORICAL)"
           if db_age > STALE_WARN_DAYS else
           f"  TOP SMC REVERSAL CANDIDATES  ({as_of.date()})")
    print(hdr)
    print("=" * 100)

    if not results:
        print("  ❌ No stock passed the filters for this data period.")
        print("     (A quiet day can be a GOOD sign — the filters are doing their job.)")
    else:
        for rank, r in enumerate(results, 1):
            print(f"  {rank}. {r['Symbol']:12s} | Rs.{r['Price']:>9.2f} | "
                  f"Low: Rs.{r['52W_Low']:>9.2f} (+{r['Proximity_%']}%) | RSI {r['RSI']}")
            print(f"     Score {r['Score']}/{MAX_SCORE} ({r['Confirms']} confirms) | "
                  f"SL: Rs.{r['Stop_Loss']:.2f} | Tgt: Rs.{r['Target']:.2f} | R:R 1:{r['R:R']}")
            print(f"     {r['Reasons']}")
            print("-" * 100)

    # ── Save (timestamped, includes as-of column, never overwrites) ──
    if results:
        out = f"smc_scan_{as_of.date()}_{datetime.now():%H%M}.csv"
        out_df = pd.DataFrame(results)
        out_df["Data_As_Of"] = as_of.date()
        out_df.to_csv(out, index=False)
        print(f"\n  ✅ Saved {len(results)} setups to {out}")

    print(f"  Done in {time.time() - t0:.1f}s\n")


if __name__ == "__main__":
    main()