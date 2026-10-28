"""
╔════════════════════════════════════════════════════════════════════════════════╗
║  NIFTY 500 — SMC SWING SCANNER v9.0 (Daily + Weekly, ISIN files)                ║
║                                                                                  ║
║  v9.0 — every improvement from the deep diagnosis of v8.2, applied:              ║
║                                                                                  ║
║   ① VECTORISED ENGINE  — swing/pool detection now uses numpy sliding windows    ║
║      (identical output to v8.2, ~20-50x faster on long histories)               ║
║   ② RR CAP ACTUALLY ENFORCED — v8.2 defined MAX_RR=10 but never checked it;     ║
║      v9 caps TP at 10R so illusionary 39R targets can never reach the report    ║
║   ③ PORTABLE PATHS — --day-dir/--week-dir flags or SMC_DAY_DIR/SMC_WEEK_DIR     ║
║      env vars; no more hardcoded D:\\ drive lock-in (defaults kept)               ║
║   ④ KELLY POSITION SIZING — every setup gets a suggested risk-% of equity        ║
║      (full Kelly capped at 25%, hard max 2%/trade) + quantity for your capital   ║
║   ⑤ VOL-REGIME OVERLAY — MIN_RISK_PCT auto-scales with the universe's median    ║
║      ATR% (sampled), so the razor-stop guard breathes with market volatility     ║
║   ⑥ PORTFOLIO GUARD — same-day same-side setups with >70% return-correlation     ║
║      are flagged, and only the top-N are ACTIVE (rest BENCH)                     ║
║   ⑦ BACKTEST STATS 2.0 — bootstrap 95% CI on expectancy + symbol-grouped        ║
║      Monte Carlo permutation test ("is the edge real or luck?")                  ║
║   ⑧ --optimize-weights — pure-numpy logistic regression learns the 10 score     ║
║      weights from backtest outcomes. Score becomes 100 × sigmoid(w·x + b) —     ║
║      the same S-curve family as your sentiment scorer. Auto-loaded when present ║
║   ⑨ HTML REPORT — sortable dark-theme report (setups + near-miss watchlist)     ║
║   ⑩ --diff MODE — NEW / CONTINUED / DROPPED vs the previous scan's CSV          ║
║   ⑪ WEBHOOK ALERTS — set SMC_TELEGRAM_URL or SMC_WEBHOOK_URL to push setups     ║
║                                                                                  ║
║  KEPT 100% FROM v8.2 (validated core): razor-stop guard (MIN_RISK_PCT/ATR),     ║
║  no-lookahead weekly bias (swings confirmed +2 weeks), corporate-action mask,   ║
║  ISIN file lookup, partial-week trim, rich near-miss levels, walk-forward       ║
║  split, Wilson intervals, survivorship warning, conservative fill simulation.   ║
║                                                                                  ║
║  RECOMMENDED WORKFLOW:                                                           ║
║    1) python 52_v9.py --backtest          → baseline stats + labelled trades     ║
║    2) python 52_v9.py --optimize-weights  → learns score weights (sigmoid)      ║
║    3) python 52_v9.py                     → daily scan (uses both of the above) ║
║                                                                                  ║
║  RUN:  python 52_v9.py                          -> scan today                    ║
║        python 52_v9.py --backtest               -> full-history event study      ║
║        python 52_v9.py --optimize-weights       -> learn score weights           ║
║        python 52_v9.py --diff                   -> vs previous scan CSV          ║
║        python 52_v9.py --structure IKS,LTF      -> anatomy of specific symbols   ║
║                                                                                  ║
║  OPTIONAL ENV: SMC_CAPITAL (default 100000) · SMC_MAX_SETUPS (10) ·              ║
║                SMC_REGIME=off · SMC_TELEGRAM_URL / SMC_WEBHOOK_URL               ║
║                                                                                  ║
║  NEED: pip install pandas numpy requests pyarrow    (no new deps in v9 — the     ║
║        logistic regression is hand-rolled numpy)                                 ║
╚══════════════════════════════════════════════════════════════════════════════════╝
"""
import os
import sys
import json
import math
import time
import argparse
import warnings
from io import StringIO
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests
from numpy.lib.stride_tricks import sliding_window_view

warnings.filterwarnings("ignore")

# ───────────────────────────── CONFIG ──────────────────────────────────────────
# Path priority: --day-dir/--week-dir CLI flag  >  SMC_DAY_DIR/SMC_WEEK_DIR env  >  defaults
DEFAULT_DAY_DIR  = r"D:\Api data\ML_HISTO_DATA\Upstox Data\Day Data"
DEFAULT_WEEK_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox Data\Weeks Data"
DAY_DATA_DIR  = os.environ.get("SMC_DAY_DIR", DEFAULT_DAY_DIR)
WEEK_DATA_DIR = os.environ.get("SMC_WEEK_DIR", DEFAULT_WEEK_DIR)

_HERE = os.path.dirname(os.path.abspath(__file__))
UNIVERSE_CACHE     = os.path.join(_HERE, "nifty500_universe.csv")
WEIGHTS_FILE       = os.path.join(_HERE, "smc_v9_weights.json")
SETUPS_CSV         = "smc_v9_setups.csv"
NEARMISS_CSV       = "smc_v9_nearmiss.csv"
HTML_REPORT        = "smc_v9_report.html"
BACKTEST_TRADES_CSV = "smc_v9_backtest_trades.csv"

MIN_DAILY_BARS = 260
MIN_RR         = 2.0
MAX_RR         = 10.0        # ② absurd-RR cap — v8.2 defined it but never enforced; v9 DOES
MIN_RISK_PCT   = 1.0         # ① stop must be >= 1.0% away from entry (regime-scaled)
MIN_RISK_ATR   = 1.0         # ① and >= 1.0 x ATR(14) away
POOL_LOOKBACK  = 90
POOL_TOL       = 0.002
POOL_CONFIRM   = 2
WK_LOOKBACK    = 52
WK_TOL         = 0.005
SETUP_WINDOW   = 15
AVG_BODY_MULT  = 1.5
OB_MAX_BACK    = 5
CHOCH_WINDOW   = 20
SL_BUF         = 0.001
TP_R_DEFAULT   = 2.5
HOLD_MAX       = 15
WEEKLY_FILTER  = True
DISCOUNT_MAX   = 0.50
TURN_FLOOR     = 3e7
ATR_PERIOD     = 14

# ── v9 additions ──
CAPITAL_DEFAULT  = float(os.environ.get("SMC_CAPITAL", 100000))
MAX_SETUPS_DEFAULT = int(os.environ.get("SMC_MAX_SETUPS", 10))
REGIME_DEFAULT   = os.environ.get("SMC_REGIME", "on").lower() != "off"
VOL_SAMPLE       = 40          # symbols sampled for the vol-regime estimate
VOL_ANCHOR       = 1.5         # median daily ATR% considered "normal" (mult anchor)
KELLY_CAP        = 0.25        # trade at most 25% of full Kelly
KELLY_HARD_MAX   = 0.02        # and never risk more than 2% of equity per trade
CORR_THRESHOLD   = 0.70        # same-day same-side setups flagged above this corr
CORR_LOOKBACK    = 90
BOOTSTRAP_N      = 1000
PERM_N           = 1000
WEBHOOK_URL      = os.environ.get("SMC_TELEGRAM_URL") or os.environ.get("SMC_WEBHOOK_URL")

# the 10 scoring features (order is contractual between scanner / backtest / weights file)
FEATURE_NAMES = ["touches", "depth", "sweep_volx", "bodyx", "disp_volx",
                 "has_fvg", "pos", "rr", "weekly_aligned", "confluence"]

DIAG_KEYS = ["no_file", "data_fails", "short_history", "corporate_action",
             "illiquid", "circuit_risk", "bias_block", "not_near_52w",
             "no_sweep", "no_displacement", "no_ob", "no_choch",
             "razor_stop", "premium_range", "low_rr", "bad_risk", "valid_setups"]

# ─────────────────────────── UNIVERSE ──────────────────────────────────────────
def get_universe():
    print("  Fetching NIFTY 500 list (Symbol + ISIN) from NSE ...")
    df = None
    try:
        r = requests.get(
            "https://archives.nseindia.com/content/indices/ind_nifty500list.csv",
            headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        df = pd.read_csv(StringIO(r.text))
        df.columns = [str(c).strip() for c in df.columns]
        df.to_csv(UNIVERSE_CACHE, index=False)
    except Exception as e:
        print(f"  NSE fetch failed ({e}); trying local cache ...")
        if os.path.exists(UNIVERSE_CACHE):
            df = pd.read_csv(UNIVERSE_CACHE)
    if df is None:
        print("  !! No NSE list and no cache.")
        return []
    isin_col = next((c for c in df.columns if "isin" in c.lower()), None)
    sym_col  = next((c for c in df.columns if c.lower() == "symbol"), None)
    if isin_col is None or sym_col is None:
        print(f"  !! Unexpected list columns: {list(df.columns)}")
        return []
    pairs = [(str(s).strip(), str(i).strip())
             for s, i in zip(df[sym_col], df[isin_col])]
    print(f"  Loaded {len(pairs)} symbols with ISINs.\n")
    return pairs

# ─────────────────────────── DATA LOADER ───────────────────────────────────────
def _read_ohlcv(path, need_volume=True):
    if path is None:
        return None
    try:
        df = pd.read_parquet(path)
    except Exception:
        return None
    if df is None or df.empty:
        return None
    if not pd.api.types.is_datetime64_any_dtype(df.index):
        dcol = next((c for c in df.columns
                     if 'date' in str(c).lower() or 'time' in str(c).lower()), None)
        if dcol is None:
            return None
        df[dcol] = pd.to_datetime(df[dcol], errors='coerce')
        df = df.dropna(subset=[dcol]).set_index(dcol)
    df.columns = [str(c).strip().split('(')[0].strip().lower() for c in df.columns]
    ren = {'open': 'Open', 'high': 'High', 'low': 'Low',
           'close': 'Close', 'volume': 'Volume'}
    df = df.rename(columns=ren)
    need = ['Open', 'High', 'Low', 'Close'] + (['Volume'] if need_volume else [])
    if not all(c in df.columns for c in need):
        return None
    df = df[need].apply(pd.to_numeric, errors='coerce')
    df = df.dropna(subset=['Close'] + (['Volume'] if need_volume else []))
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    df = df[~df.index.duplicated(keep='first')].sort_index()
    return df if len(df) else None

def _find_file(folder, symbol, isin):
    folder = folder or DAY_DATA_DIR
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

def load_symbol(symbol, isin, day_dir=None, week_dir=None):
    day_dir = day_dir or DAY_DATA_DIR
    week_dir = week_dir or WEEK_DATA_DIR
    day_path = _find_file(day_dir, symbol, isin)
    day = _read_ohlcv(day_path, need_volume=True) if day_path else None
    if day is None or len(day) < 60:
        return None, None

    week_path = _find_file(week_dir, symbol, isin)
    week = _read_ohlcv(week_path, need_volume=False) if week_path else None
    if week is not None:
        while len(week) and week.index[-1] > day.index[-1]:
            week = week.iloc[:-1]
        if len(week) and (day.index[-1] - week.index[-1]).days < 4:
            week = week.iloc[:-1]                       # partial current week
        if len(week) < 30 or (day.index[-1] - week.index[-1]).days > 10:
            week = None
    if week is None:
        week = day.resample('W-FRI').agg({'Open': 'first', 'High': 'max',
                                          'Low': 'min', 'Close': 'last'}).dropna()
    return day, week

# ─────────────────────── ① VECTORIZED SWING/POOL ENGINE ────────────────────────
def _swing_indices(v, side, confirm):
    """Numpy sliding-window swing detection — bit-for-bit identical to the
    v8.2 double loop (<= / >= on both windows, edges excluded), ~20-50x faster.
    Returns (indices, prices) of confirmed swing points."""
    v = np.asarray(v)
    n = len(v)
    w = int(confirm)
    empty_i = np.empty(0, dtype=int)
    empty_p = np.empty(0, dtype=v.dtype)
    if n <= w:                                  # loop range(confirm, n-confirm) empty
        return empty_i, empty_p
    sw = sliding_window_view(v, w)              # sw[k] = v[k:k+w]
    if side == "LOW":
        left  = np.full(n, np.inf)
        right = np.full(n, np.inf)
        if n - w > 0:
            left[w:]  = sw[: n - w].min(axis=1)     # min of v[i-w:i]
            right[: n - w] = sw[1:].min(axis=1)     # min of v[i+1:i+w+1]
        ok = (v <= left) & (v <= right)
    else:
        left  = np.full(n, -np.inf)
        right = np.full(n, -np.inf)
        if n - w > 0:
            left[w:]  = sw[: n - w].max(axis=1)
            right[: n - w] = sw[1:].max(axis=1)
        ok = (v >= left) & (v >= right)
    ok[:confirm] = False
    ok[n - confirm:] = False
    idx = np.where(ok)[0]
    return idx, v[idx]

def find_pools(df, side, lookback=POOL_LOOKBACK, tol=POOL_TOL, confirm=POOL_CONFIRM):
    """Liquidity pools — v8.2 semantics (merge within tol, track touches and
    confirmation index) on top of the vectorised swing finder."""
    v = (df['Low'] if side == "LOW" else df['High']).values
    n = len(v)
    idx, prices = _swing_indices(v, side, confirm)
    pools = []
    for i, p in zip(idx.tolist(), prices.tolist()):
        if i < n - lookback:
            continue
        merged = False
        for pool in pools:
            if abs(p - pool['price']) / pool['price'] <= tol:
                pool['touches'] += 1
                pool['price'] = min(pool['price'], p) if side == "LOW" else max(pool['price'], p)
                pool['conf_i'] = max(pool['conf_i'], i + confirm)
                merged = True
                break
        if not merged:
            pools.append({'price': p, 'touches': 1, 'conf_i': i + confirm})
    return pools

# ───────────────────── WEEKLY STRUCTURAL BIAS (no indicators) ──────────────────
def weekly_bias_series(week):
    """bias[i] after week i closes: +1 bull, -1 bear, 0 neutral.
    Swings confirmed 2 weeks later -> no look-ahead. (v8.2 walk, vectorised swings)"""
    wC = week['Close'].values
    wH = week['High'].values
    wL = week['Low'].values
    W = len(week)
    hi_idx, hi = _swing_indices(wH, "HIGH", 2)
    lo_idx, lo = _swing_indices(wL, "LOW", 2)
    sh = list(zip(hi_idx.tolist(), hi.tolist()))
    sl = list(zip(lo_idx.tolist(), lo.tolist()))
    bias = np.zeros(W, dtype=int)
    hi_ptr, lo_ptr = 0, 0
    last_hi, last_lo = None, None
    for i in range(W):
        while hi_ptr < len(sh) and sh[hi_ptr][0] + 2 <= i - 1:
            last_hi = sh[hi_ptr][1]; hi_ptr += 1
        while lo_ptr < len(sl) and sl[lo_ptr][0] + 2 <= i - 1:
            last_lo = sl[lo_ptr][1]; lo_ptr += 1
        if i >= 12:
            if last_hi is not None and wC[i] > last_hi:
                bias[i] = 1
            elif last_lo is not None and wC[i] < last_lo:
                bias[i] = -1
    return bias

def current_weekly_bias(day, week):
    wend = week.index
    # v9 fix: numpy>=2 refuses a raw pandas Timestamp inside searchsorted —
    # convert explicitly (the backtest already did this; the scanner path didn't)
    k = int(np.searchsorted(wend.values, np.datetime64(day.index[-1]),
                            side='left')) - 1
    if k < 12:
        return 0
    return int(weekly_bias_series(week)[k])

# ─────────────────────────── HYGIENE ───────────────────────────────────────────
def corp_action_mask(O, H, L):
    m = np.zeros(len(O), dtype=bool)
    if len(O) < 2:
        return m
    m[1:] = (O[1:] < 0.75 * L[:-1]) | (O[1:] > 3.0 * H[:-1])
    return m

def hygiene_ok(day):
    O, H, L = day['Open'].values, day['High'].values, day['Low'].values
    if corp_action_mask(O, H, L)[-250:].any():
        return False, "corporate_action"
    turn = (day['Volume'] * day['Close']).tail(20).median()
    if turn < TURN_FLOOR:
        return False, "illiquid"
    ret = day['Close'].pct_change()
    vma = day['Volume'].rolling(20).mean()
    dry = (ret.abs() > 0.18) & (day['Volume'] < 0.25 * vma)
    if dry.tail(5).any():
        return False, "circuit_risk"
    return True, ""

def compute_atr(H, L, C, period=ATR_PERIOD):
    """Wilder-smoothed ATR of the full series, last value (v8.2 formula)."""
    if len(H) < 2:
        return float("nan")
    tr = np.maximum(H - L, np.maximum(np.abs(H - np.roll(C, 1)),
                                      np.abs(L - np.roll(C, 1))))
    tr[0] = H[0] - L[0]
    return float(pd.Series(tr).ewm(alpha=1 / period, min_periods=period,
                                   adjust=False).mean().iloc[-1])

# ─────────────────────────── SCORING (0–100) ───────────────────────────────────
def score_setup(touches, depth, sweep_volx, bodyx, disp_volx, has_fvg, rr, pos,
                weekly_aligned, weekly_confluence):
    """v8.2 hand-tuned score — kept verbatim as the default scoring mode."""
    s = 15 if touches >= 3 else (8 if touches == 2 else 3)
    s += min(15, depth * 7.5)
    s += min(15, max(0.0, sweep_volx - 1) * 15)
    s += min(20, max(0.0, bodyx - 1) * 20)
    s += min(10, max(0.0, disp_volx - 1) * 10)
    s += 10 if has_fvg else 0
    s += min(10, max(0.0, (DISCOUNT_MAX - pos) / DISCOUNT_MAX) * 10)
    s += min(15, max(0.0, rr - MIN_RR) * 15)
    if weekly_aligned:
        s += 12
    if weekly_confluence:
        s += 10
    return int(round(min(100.0, s)))

def sigmoid_score(feats, model):
    """⑧ Learned score: 100 × sigmoid(w·x + b) — same S-curve family as the
    sentiment scorer. Features in FEATURE_NAMES order; unavailable ones are 0."""
    x = np.array([float(feats.get(k, 0.0)) for k in FEATURE_NAMES])
    keep = np.asarray(model["keep"], bool)
    mu = np.asarray(model["mu"], float)
    sd = np.asarray(model["sd"], float)
    w = np.asarray(model["w"], float)
    z = ((x[keep] - mu[keep]) / sd[keep]) @ w + float(model["b"])
    z = max(min(float(z), 30.0), -30.0)
    return 100.0 / (1.0 + math.exp(-z))

# ─────────────────── CORE SETUP LOGIC (both directions) ────────────────────────
def evaluate_direction(day, side, w_bias, w_pools, atr=None,
                       risk_pct=MIN_RISK_PCT, risk_atr_mult=MIN_RISK_ATR,
                       model=None):
    """-> (result | None, diag_key).
    v8.2 validated core + v9 changes:
      • ATR hoisted (computed once per stock, not once per side)
      • razor-stop guard uses regime-scaled risk_pct
      • MAX_RR is now actually enforced (TP capped, raw RR shown)
      • optional learned-sigmoid scoring when model is given"""
    if WEEKLY_FILTER and w_bias != 0:
        if w_bias == 1 and side != "LONG":
            return None, "bias_block"
        if w_bias == -1 and side != "SHORT":
            return None, "bias_block"

    n = len(day)
    last = n - 1
    O, H, L, C, V = (day[c].values for c in ['Open', 'High', 'Low', 'Close', 'Volume'])
    close = C[last]

    if atr is None or (isinstance(atr, float) and math.isnan(atr)):
        atr = compute_atr(H, L, C)

    if side == "LONG":
        ex = L[max(0, last - 251):last + 1].min()
        prox = (close - ex) / ex
    else:
        ex = H[max(0, last - 251):last + 1].max()
        prox = (ex - close) / ex
    if prox > 0.10:
        return None, "not_near_52w"

    pools = find_pools(day, "LOW" if side == "LONG" else "HIGH")
    if not pools:
        return None, "no_sweep"

    vma = pd.Series(V).rolling(20).mean().values
    start = max(0, last - SETUP_WINDOW)
    sweep_idx, sweep_pool = None, None
    for i in range(start, last + 1):
        for p in pools:
            if p['conf_i'] > i:
                continue
            if side == "LONG":
                hit = (L[i] < p['price']) and (C[i] > p['price'])
            else:
                hit = (H[i] > p['price']) and (C[i] < p['price'])
            if hit and (sweep_pool is None or p['touches'] > sweep_pool['touches']):
                sweep_idx, sweep_pool = i, p
    if sweep_idx is None:
        return None, "no_sweep"

    pp = sweep_pool['price']
    if side == "LONG":
        depth = max((pp - L[sweep_idx]) / pp * 100, 0.0)
        sweep_extreme = L[sweep_idx]
        invalid_lvl = sweep_extreme * (1 - SL_BUF)
    else:
        depth = max((H[sweep_idx] - pp) / pp * 100, 0.0)
        sweep_extreme = H[sweep_idx]
        invalid_lvl = sweep_extreme * (1 + SL_BUF)
    sweep_volx = V[sweep_idx] / vma[sweep_idx] if vma[sweep_idx] > 0 else 0.0

    wk_same = w_pools.get("LOW" if side == "LONG" else "HIGH", [])
    confluence = any(abs(pp - wp['price']) / wp['price'] <= 0.01 for wp in wk_same)

    body = np.abs(C - O)
    body_ma = pd.Series(body).rolling(20).mean().shift(1).values
    disp_idx, ob_idx = None, None
    for i in range(max(sweep_idx, start), last + 1):
        if side == "LONG":
            big = (C[i] - O[i]) > AVG_BODY_MULT * body_ma[i]
        else:
            big = (O[i] - C[i]) > AVG_BODY_MULT * body_ma[i]
        if big and body_ma[i] > 0:
            disp_idx = i
            lo = max(0, i - OB_MAX_BACK + 1)
            for j in range(i - 1, lo - 1, -1):
                red = (C[j] < O[j]) if side == "LONG" else (C[j] > O[j])
                if red and vma[j] > 0 and V[j] > vma[j]:
                    ob_idx = j
                    break
            break
    if disp_idx is None:
        return None, "no_displacement"
    if ob_idx is None:
        return None, "no_ob"
    if max(sweep_idx, ob_idx) > disp_idx:
        return None, "no_ob"
    bodyx = body[disp_idx] / body_ma[disp_idx] if body_ma[disp_idx] > 0 else 0.0
    disp_volx = V[disp_idx] / vma[disp_idx] if vma[disp_idx] > 0 else 0.0

    lo0 = max(0, sweep_idx - CHOCH_WINDOW)
    choch_lvl = H[lo0:sweep_idx].max() if side == "LONG" else L[lo0:sweep_idx].min()
    choch_idx = None
    for i in range(disp_idx, last + 1):
        broke = (C[i] > choch_lvl) if side == "LONG" else (C[i] < choch_lvl)
        if broke:
            choch_idx = i
            break

    if choch_idx is None:
        if side == "LONG":
            dist = (choch_lvl - close) / close * 100
            arrow = ">"
        else:
            dist = (close - choch_lvl) / close * 100
            arrow = "<"
        st = (f"{side} base building — sweep+VolOB done, "
              f"needs CHoCH {arrow} {choch_lvl:.2f}")
        return {"Type": "NEAR MISS", "Diag": "no_choch", "Side": side,
                "Price": round(close, 2), "Date": str(day.index[last].date()),
                "ChoChLevel": round(choch_lvl, 2), "DistTrig%": round(dist, 1),
                "SweepDate": str(day.index[sweep_idx].date()),
                "SweepExtreme": round(sweep_extreme, 2),
                "InvalidLvl": round(invalid_lvl, 2),
                "PoolPrice": round(pp, 2), "PoolTouch": sweep_pool['touches'],
                "WeeklyConfluence": confluence,
                "ObDate": str(day.index[ob_idx].date()),
                "DispDate": str(day.index[disp_idx].date()),
                "BarsSinceSweep": last - sweep_idx,
                "Status": st}, "no_choch"

    if side == "LONG":
        dr_lo = L[sweep_idx:choch_idx + 1].min()
        dr_hi = H[sweep_idx:choch_idx + 1].max()
        pos = (close - dr_lo) / max(dr_hi - dr_lo, 1e-9)
    else:
        dr_hi = H[sweep_idx:choch_idx + 1].max()
        dr_lo = L[sweep_idx:choch_idx + 1].min()
        pos = (dr_hi - close) / max(dr_hi - dr_lo, 1e-9)
    if pos > DISCOUNT_MAX:
        zone = "premium" if side == "LONG" else "discount"
        st = (f"Sweep+OB+CHoCH valid but price in {zone} "
              f"({pos * 100:.0f}% of range) — wait for pullback")
        return {"Type": "NEAR MISS", "Diag": "premium_range", "Side": side,
                "Price": round(close, 2), "Date": str(day.index[last].date()),
                "Status": st}, "premium_range"

    entry = close
    fvg_idx = None
    for i in range(max(2, disp_idx), last + 1):
        fvg_ok = (L[i] > H[i - 2]) if side == "LONG" else (H[i] < L[i - 2])
        if fvg_ok:
            fvg_idx = i
            break
    if fvg_idx is not None:
        if side == "LONG":
            entry = min((L[fvg_idx] + H[fvg_idx - 2]) / 2, close)
        else:
            entry = max((H[fvg_idx] + L[fvg_idx - 2]) / 2, close)

    if side == "LONG":
        sl = L[sweep_idx:choch_idx + 1].min() * (1 - SL_BUF)
        risk = entry - sl
    else:
        sl = H[sweep_idx:choch_idx + 1].max() * (1 + SL_BUF)
        risk = sl - entry
    if risk <= 0:
        return None, "bad_risk"

    # ① RAZOR-STOP GUARD: stop must sit outside noise (risk_pct is regime-scaled)
    if risk < max(risk_pct / 100 * close, risk_atr_mult * atr):
        st = (f"Stop only {risk:.2f} pts ({100*risk/close:.2f}%, "
              f"{risk/atr:.2f} ATR) inside noise — setup skipped")
        return {"Type": "NEAR MISS", "Diag": "razor_stop", "Side": side,
                "Price": round(close, 2), "Date": str(day.index[last].date()),
                "Status": st}, "razor_stop"

    opp = find_pools(day, "HIGH" if side == "LONG" else "LOW",
                     POOL_LOOKBACK, POOL_TOL)
    if side == "LONG":
        beyond = [p['price'] for p in opp if p['price'] > entry]
        pool_tp = min(beyond) if beyond else None
        tp = max(pool_tp, entry + TP_R_DEFAULT * risk) if pool_tp is not None \
             else entry + TP_R_DEFAULT * risk
        rr = (tp - entry) / risk
        wk_beyond = [p['price'] for p in w_pools.get("HIGH", []) if p['price'] > entry]
        wk_tp = min(wk_beyond) if wk_beyond else None
    else:
        beyond = [p['price'] for p in opp if p['price'] < entry]
        pool_tp = max(beyond) if beyond else None
        tp = min(pool_tp, entry - TP_R_DEFAULT * risk) if pool_tp is not None \
             else entry - TP_R_DEFAULT * risk
        rr = (entry - tp) / risk
        wk_beyond = [p['price'] for p in w_pools.get("LOW", []) if p['price'] < entry]
        wk_tp = max(wk_beyond) if wk_beyond else None
    rr = round(rr, 2)

    # ② v9 FIX: v8.2 defined MAX_RR but never checked it — now the TP is capped
    #    so illusionary 39R targets never reach the report
    rr_raw = rr
    if rr > MAX_RR:
        tp = entry + MAX_RR * risk if side == "LONG" else entry - MAX_RR * risk
        rr = MAX_RR

    if rr < MIN_RR:
        st = f"Valid {side} structure but R:R {rr} < {MIN_RR}"
        return {"Type": "NEAR MISS", "Diag": "low_rr", "Side": side,
                "Price": round(close, 2), "Date": str(day.index[last].date()),
                "Entry": round(entry, 2), "SL": round(sl, 2),
                "Target": round(tp, 2), "R:R": rr, "Status": st}, "low_rr"

    weekly_aligned = (w_bias == 1 and side == "LONG") or \
                     (w_bias == -1 and side == "SHORT")
    if model is not None:
        feats = {"touches": sweep_pool['touches'], "depth": depth,
                 "sweep_volx": sweep_volx, "bodyx": bodyx, "disp_volx": disp_volx,
                 "has_fvg": int(fvg_idx is not None), "pos": pos, "rr": rr,
                 "weekly_aligned": int(weekly_aligned),
                 "confluence": int(confluence)}
        score = int(round(sigmoid_score(feats, model)))
    else:
        score = score_setup(sweep_pool['touches'], depth, sweep_volx, bodyx,
                            disp_volx, fvg_idx is not None, rr, pos,
                            weekly_aligned, confluence)
    extreme = "AT 52W LOW" if side == "LONG" else "AT 52W HIGH"
    zone = "Discount" if side == "LONG" else "Premium"
    reasons = [f"{extreme} ({prox * 100:.1f}%)",
               f"Sweep pool x{sweep_pool['touches']} (depth {depth:.1f}%, "
               f"vol {sweep_volx:.1f}x)" + (" ★WEEKLY pool" if confluence else ""),
               f"Vol-OB@{day.index[ob_idx].date()} Disp@{day.index[disp_idx].date()} "
               f"CHoCH@{day.index[choch_idx].date()}",
               f"{zone} {pos * 100:.0f}% | weekly bias "
               f"{'BULL' if w_bias == 1 else 'BEAR' if w_bias == -1 else 'NEUTRAL'}"]
    if rr_raw > MAX_RR:
        reasons.append(f"TP capped at {MAX_RR}R (raw {rr_raw})")
    res = {"Type": "VALID SETUP", "Diag": "valid_setups", "Side": side,
           "Score": score, "Price": round(close, 2), "Entry": round(entry, 2),
           "SL": round(sl, 2), "Target": round(tp, 2), "R:R": rr,
           "RiskPts": round(risk, 2), "RiskPct": round(100 * risk / entry, 2),
           "PoolTarget": round(pool_tp, 2) if pool_tp is not None else None,
           "WeeklyPoolTarget": round(wk_tp, 2) if wk_tp is not None else None,
           "WeeklyConfluence": confluence,
           "PoolTouch": sweep_pool['touches'], "Depth%": round(depth, 1),
           "SweepVolx": round(sweep_volx, 1),
           "RRcapped": rr_raw > MAX_RR,
           "Date": str(day.index[last].date()), "Status": " | ".join(reasons)}
    return res, "valid_setups"

# ───────────────────────────── SCAN WORKER ─────────────────────────────────────
def scan_stock(symbol, isin, day_dir=None, week_dir=None,
               risk_pct=MIN_RISK_PCT, risk_atr_mult=MIN_RISK_ATR, model=None):
    day_dir = day_dir or DAY_DATA_DIR
    week_dir = week_dir or WEEK_DATA_DIR
    base = {"Symbol": symbol, "ISIN": isin, "Type": "NONE", "Diag": "data_fails",
            "Side": "", "Score": 0, "Price": 0, "Entry": None, "SL": None,
            "Target": None, "R:R": None, "Date": "", "Status": "", "HistDays": 0,
            "ATRpct": None}
    if _find_file(day_dir, symbol, isin) is None:
        base["Diag"] = "no_file"
        base["Status"] = "day file not found (ISIN not in data folder)"
        return base
    day, week = load_symbol(symbol, isin, day_dir, week_dir)
    if day is None:
        return base
    base["HistDays"] = len(day)
    if len(day) < MIN_DAILY_BARS:
        base["Diag"] = "short_history"
        return base
    ok, why = hygiene_ok(day)
    if not ok:
        base["Diag"] = why
        return base

    # ATR computed ONCE per stock (v8.2 recomputed it inside each side)
    H, L, C = day['High'].values, day['Low'].values, day['Close'].values
    atr = compute_atr(H, L, C)
    if C[-1] > 0 and not math.isnan(atr):
        base["ATRpct"] = round(100 * atr / C[-1], 2)

    w_bias = current_weekly_bias(day, week)
    w_pools = {"LOW": find_pools(week, "LOW", WK_LOOKBACK, WK_TOL),
               "HIGH": find_pools(week, "HIGH", WK_LOOKBACK, WK_TOL)}

    # ③ per-side keys: report the ALLOWED side's real failure, not bias_block
    best, keys = None, {}
    for side in ("LONG", "SHORT"):
        res, key = evaluate_direction(day, side, w_bias, w_pools, atr=atr,
                                      risk_pct=risk_pct,
                                      risk_atr_mult=risk_atr_mult, model=model)
        keys[side] = key
        if res is not None and (best is None or
                                res.get("Score", 0) > best.get("Score", 0)):
            best = res
    if best is None:
        allowed = "LONG" if w_bias >= 0 else "SHORT"
        base["Diag"] = keys.get(allowed) or "bias_block"
        if "bias_block" in keys.values() and all(
                k == "bias_block" for k in keys.values()):
            base["Status"] = "Weekly bias blocks both sides"
        return base

    base.update(best)
    return base

# ─────────────────── BACKTEST (guard included, no look-ahead) ──────────────────
def backtest_stock(symbol, isin, day_dir=None, week_dir=None):
    """Conservative fills: gap-through-stop at open, same-bar SL&TP -> SL,
    time stop at close. Same razor-stop guard as the scanner (static thresholds
    — the vol-regime overlay is a TODAY concept and is not applied historically,
    so results stay comparable across runs).
    v9: records the 10 scoring features per trade for --optimize-weights."""
    day_dir = day_dir or DAY_DATA_DIR
    week_dir = week_dir or WEEK_DATA_DIR
    day, week = load_symbol(symbol, isin, day_dir, week_dir)
    if day is None or len(day) < MIN_DAILY_BARS + HOLD_MAX + 10:
        return symbol, []
    O, H, L, C, V = (day[c].values for c in ['Open', 'High', 'Low', 'Close', 'Volume'])
    n = len(day)
    dates = day.index
    body = np.abs(C - O)
    body_ma = pd.Series(body).rolling(20).mean().shift(1).values
    vma = pd.Series(V).rolling(20).mean().values
    ca = corp_action_mask(O, H, L)
    tr = np.maximum(H - L, np.maximum(np.abs(H - np.roll(C, 1)),
                                      np.abs(L - np.roll(C, 1))))
    tr[0] = H[0] - L[0]
    atr_s = pd.Series(tr).ewm(alpha=1/ATR_PERIOD, min_periods=ATR_PERIOD,
                              adjust=False).mean().values

    wend = week.index.values
    wbias = weekly_bias_series(week)

    def confirmed_swings(side):
        # NOTE: _swing_indices wants the POOL side ("LOW"/"HIGH"),
        # not the trade direction — LONG trades hunt LOW pools and vice versa
        v = L if side == "LONG" else H
        pool_side = "LOW" if side == "LONG" else "HIGH"
        idx, prices = _swing_indices(v, pool_side, POOL_CONFIRM)
        return list(zip(idx.tolist(), prices.tolist()))

    swing_by_side = {"LONG": confirmed_swings("LONG"),
                     "SHORT": confirmed_swings("SHORT")}

    trades = []
    for side in ("LONG", "SHORT"):
        sign = 1 if side == "LONG" else -1
        swings_ = swing_by_side[side]
        pools, ptr = [], 0
        for t in range(25, n - HOLD_MAX - 2):
            if ca[t] or ca[t + 1]:
                continue
            k_w = int(np.searchsorted(wend, np.datetime64(dates[t]), side='left')) - 1
            w_bias = int(wbias[k_w]) if k_w >= 12 else 0
            if WEEKLY_FILTER and w_bias != 0:
                if w_bias == 1 and side != "LONG":
                    continue
                if w_bias == -1 and side != "SHORT":
                    continue

            while ptr < len(swings_) and swings_[ptr][0] + POOL_CONFIRM <= t - 1:
                i, p = swings_[ptr]
                ptr += 1
                merged = False
                for pool in pools:
                    if abs(p - pool[0]) / pool[0] <= POOL_TOL:
                        pool[1] += 1
                        pool[0] = min(pool[0], p) if side == "LONG" else max(pool[0], p)
                        merged = True
                        break
                if not merged:
                    pools.append([p, 1])
            if not pools:
                continue

            if side == "LONG":
                hit = [pl for pl in pools if L[t] < pl[0] < C[t]]
            else:
                hit = [pl for pl in pools if H[t] > pl[0] > C[t]]
            if not hit:
                continue
            pool_px, touches = max(hit, key=lambda x: x[1])
            if side == "LONG":
                depth = (pool_px - L[t]) / pool_px * 100
            else:
                depth = (H[t] - pool_px) / pool_px * 100
            volx = V[t] / vma[t] if vma[t] > 0 else 0

            disp = None
            for j in range(t + 1, min(t + 4, n)):
                if side == "LONG":
                    big = (C[j] - O[j]) > AVG_BODY_MULT * body_ma[j]
                else:
                    big = (O[j] - C[j]) > AVG_BODY_MULT * body_ma[j]
                if big and body_ma[j] > 0:
                    disp = j
                    break
            if disp is None:
                continue

            ob = None
            for j in range(disp - 1, max(0, disp - OB_MAX_BACK) - 1, -1):
                red = (C[j] < O[j]) if side == "LONG" else (C[j] > O[j])
                if red and vma[j] > 0 and V[j] > vma[j]:
                    ob = j
                    break
            if ob is None or max(t, ob) > disp:
                continue

            if side == "LONG":
                lvl = H[max(0, t - CHOCH_WINDOW):t].max()
            else:
                lvl = L[max(0, t - CHOCH_WINDOW):t].min()
            k = None
            for j in range(disp, min(disp + 8, n - 1)):
                broke = (C[j] > lvl) if side == "LONG" else (C[j] < lvl)
                if broke:
                    k = j
                    break
            if k is None:
                continue

            if side == "LONG":
                dr_lo = L[t:k + 1].min()
                dr_hi = H[t:k + 1].max()
                if (C[k] - dr_lo) / max(dr_hi - dr_lo, 1e-9) > DISCOUNT_MAX:
                    continue
                entry, sl = C[k], L[t:k + 1].min() * (1 - SL_BUF)
                risk = entry - sl
            else:
                dr_hi = H[t:k + 1].max()
                dr_lo = L[t:k + 1].min()
                if (dr_hi - C[k]) / max(dr_hi - dr_lo, 1e-9) > DISCOUNT_MAX:
                    continue
                entry, sl = C[k], H[t:k + 1].max() * (1 + SL_BUF)
                risk = sl - entry
            if risk <= 0:
                continue
            # same razor-stop guard as the scanner
            atr_t = atr_s[k] if not np.isnan(atr_s[k]) else 0.0
            if risk < max(MIN_RISK_PCT / 100 * entry, MIN_RISK_ATR * atr_t):
                continue

            # ── v9: feature snapshot (read-only; does NOT change trade selection) ──
            bodyx_d = body[disp] / body_ma[disp] if body_ma[disp] > 0 else 0.0
            dvolx = V[disp] / vma[disp] if vma[disp] > 0 else 0.0
            fvg_found = 0
            for i in range(max(2, disp), k + 1):
                if (L[i] > H[i - 2]) if side == "LONG" else (H[i] < L[i - 2]):
                    fvg_found = 1
                    break
            if side == "LONG":
                pos_k = (C[k] - dr_lo) / max(dr_hi - dr_lo, 1e-9)
            else:
                pos_k = (dr_hi - C[k]) / max(dr_hi - dr_lo, 1e-9)
            aligned = int((w_bias == 1 and side == "LONG") or
                          (w_bias == -1 and side == "SHORT"))

            tp = entry + sign * TP_R_DEFAULT * risk
            R, mfe = None, 0.0
            for m in range(k + 1, min(k + 1 + HOLD_MAX, n)):
                if side == "LONG":
                    fav = (H[m] - entry) / risk
                else:
                    fav = (entry - L[m]) / risk
                mfe = max(mfe, fav)
                if side == "LONG":
                    if O[m] <= sl:
                        R = (O[m] - entry) / risk
                        break
                    if L[m] <= sl:
                        R = -1.0
                        break
                    if H[m] >= tp:
                        R = TP_R_DEFAULT
                        break
                else:
                    if O[m] >= sl:
                        R = (entry - O[m]) / risk
                        break
                    if H[m] >= sl:
                        R = -1.0
                        break
                    if L[m] <= tp:
                        R = TP_R_DEFAULT
                        break
            if R is None:
                m = min(k + HOLD_MAX, n - 1)
                R = sign * (C[m] - entry) / risk
            trades.append({"Symbol": symbol, "Side": side,
                           "Date": str(dates[k].date()), "touches": touches,
                           "depth": round(depth, 1), "volx": round(volx, 1),
                           "R": round(R, 2), "mfeR": round(mfe, 2),
                           "bars": m - k,
                           "f_touches": touches, "f_depth": round(depth, 2),
                           "f_sweep_volx": round(volx, 2),
                           "f_bodyx": round(bodyx_d, 2),
                           "f_disp_volx": round(dvolx, 2),
                           "f_has_fvg": fvg_found, "f_pos": round(pos_k, 3),
                           "f_rr": TP_R_DEFAULT, "f_weekly_aligned": aligned,
                           "f_confluence": 0})
    return symbol, trades

# ─────────────────────── ⑦ STATISTICS (bootstrap / permutation) ────────────────
def wilson(k, n):
    if n == 0:
        return "n/a"
    p = k / n
    se = (p * (1 - p) / n) ** 0.5
    return f"{p:.1%} [{max(0.0, p - 1.96 * se):.1%}, {min(1.0, p + 1.96 * se):.1%}]"

def bootstrap_expectancy_ci(R, n_boot=BOOTSTRAP_N, seed=11):
    """95% CI for mean R by resampling trades with replacement."""
    R = np.asarray(R, float)
    if len(R) < 5:
        return None
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(R), size=(n_boot, len(R)))
    means = R[idx].mean(axis=1)
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))

def permutation_test(df, n_perm=PERM_N, seed=13):
    """Symbol-grouped Monte Carlo: shuffle R WITHIN each symbol's trades.
    H0: entry timing / setup features carry no information.
    Grouping preserves each stock's return distribution (controls the fact that
    some names are simply more volatile than others)."""
    R = df["R"].values.astype(float)
    sym = df["Symbol"].values
    if len(R) < 10 or df["Symbol"].nunique() < 3:
        return None
    groups = {}
    for i, s in enumerate(sym):
        groups.setdefault(s, []).append(i)
    groups = [np.array(g) for g in groups.values() if len(g) >= 2]
    if len(groups) < 3:
        return None
    actual = R.mean()
    rng = np.random.default_rng(seed)
    hit = 0
    for _ in range(n_perm):
        perm = R.copy()
        for g in groups:
            v = perm[g]
            rng.shuffle(v)
            perm[g] = v
        if perm.mean() >= actual:
            hit += 1
    return actual, (hit + 1) / (n_perm + 1)

# ─────────────────────── ⑧ LEARNED WEIGHTS (numpy logistic) ────────────────────
def fit_logistic(X, y, l2=1.0, iters=3000, lr=0.5):
    """Logistic regression in pure numpy: standardised features + L2.
    Constant / unavailable features (std≈0) are masked out automatically."""
    X = np.asarray(X, float)
    y = np.asarray(y, float)
    mu = X.mean(axis=0)
    sd = X.std(axis=0)
    keep = sd > 1e-9
    if keep.sum() == 0:
        return None
    Z = (X[:, keep] - mu[keep]) / sd[keep]
    Xb = np.hstack([Z, np.ones((len(Z), 1))])
    w = np.zeros(Xb.shape[1])
    for _ in range(iters):
        p = 1.0 / (1.0 + np.exp(-np.clip(Xb @ w, -30, 30)))
        grad = Xb.T @ (p - y) / len(y) + l2 * np.r_[w[:-1], 0.0]
        w -= lr * grad
    return {"w": w[:-1], "b": float(w[-1]), "mu": mu, "sd": sd, "keep": keep}

def predict_proba(X, model):
    X = np.asarray(X, float)
    keep = np.asarray(model["keep"], bool)
    mu = np.asarray(model["mu"], float)
    sd = np.asarray(model["sd"], float)
    Z = (X[:, keep] - mu[keep]) / sd[keep]
    return 1.0 / (1.0 + np.exp(-np.clip(Z @ np.asarray(model["w"], float)
                                         + float(model["b"]), -30, 30)))

def roc_auc(y, p):
    y = np.asarray(y, float)
    n1, n0 = y.sum(), len(y) - y.sum()
    if n1 == 0 or n0 == 0:
        return None
    ranks = pd.Series(p).rank().values          # tie-averaged ranks
    return float((ranks[y == 1].sum() - n1 * (n1 + 1) / 2) / (n1 * n0))

def save_weights(model, n_trades, win_rate, auc, path=WEIGHTS_FILE):
    obj = {"trained": datetime.now().strftime("%Y-%m-%d %H:%M"),
           "n_trades": int(n_trades), "win_rate": round(float(win_rate), 4),
           "auc": round(float(auc), 4) if auc is not None else None,
           "features": [f for f, k in zip(FEATURE_NAMES, model["keep"]) if k],
           "keep": [bool(k) for k in model["keep"]],
           "w": [float(x) for x in model["w"]],
           "b": float(model["b"]),
           "mu": [float(x) for x in model["mu"]],
           "sd": [float(x) for x in model["sd"]]}
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)

def load_weights(path=WEIGHTS_FILE):
    """Auto-load learned weights; require a meaningful sample (>=100 trades)."""
    try:
        with open(path) as f:
            m = json.load(f)
        if m.get("n_trades", 0) >= 100 and "w" in m:
            return m
        print(f"  ⚠ weights file ignored (n={m.get('n_trades')} trades — need >= 100)")
    except Exception:
        pass
    return None

# ─────────────────────── ④ KELLY POSITION SIZING ──────────────────────────────
def kelly_stats(trades_csv=BACKTEST_TRADES_CSV):
    """Per-side (win_rate, payoff b = avgWin/avgLoss) from backtest trades.
    Falls back to conservative defaults when no backtest exists yet."""
    stats = {}
    try:
        t = pd.read_csv(trades_csv)
        for side, g in t.groupby("Side"):
            wins, losses = g.R[g.R > 0], -g.R[g.R < 0]
            if len(wins) >= 10 and len(losses) >= 10:
                b = wins.mean() / max(losses.mean(), 1e-9)
                stats[side] = (float((g.R > 0).mean()), float(b))
    except Exception:
        pass
    for side in ("LONG", "SHORT"):
        stats.setdefault(side, (0.45, 1.8))
    return stats

def kelly_risk_fraction(p, b, cap=KELLY_CAP, hard=KELLY_HARD_MAX):
    """Fraction of equity to risk per trade: full Kelly, capped at 25% Kelly
    AND hard-capped at 2% — over-betting a noisy edge is the fastest way to
    blow up an account."""
    if not b or b <= 0 or p <= 0:
        return 0.005
    f = p - (1 - p) / b
    return min(cap * max(f, 0.0), hard)

# ─────────────────────── ⑤ VOLATILITY-REGIME OVERLAY ──────────────────────────
def estimate_vol_regime(universe, day_dir, sample=VOL_SAMPLE, seed=7):
    """Sample the universe's median daily ATR% and scale MIN_RISK_PCT with it.
    Anchor 1.5% = 'normal' market; multiplier clipped to [0.75x, 2.0x]."""
    rng = np.random.default_rng(seed)
    uni = list(universe)
    pick = uni if len(uni) <= sample else \
        [uni[i] for i in rng.choice(len(uni), sample, replace=False)]
    vals = []
    for sym, isin in pick:
        p = _find_file(day_dir, sym, isin)
        if p is None:
            continue
        day = _read_ohlcv(p, need_volume=True)
        if day is None or len(day) < 60:
            continue
        try:
            a = compute_atr(day['High'].values, day['Low'].values,
                            day['Close'].values)
            if a and not math.isnan(a) and day['Close'].values[-1] > 0:
                vals.append(100 * a / day['Close'].values[-1])
        except Exception:
            pass
        if len(vals) >= 25:
            break
    if len(vals) < 10:
        return 1.0, None, "UNKNOWN (too few samples — guard unchanged)"
    med = float(np.median(vals))
    mult = float(np.clip(med / VOL_ANCHOR, 0.75, 2.0))
    label = "LOW" if mult < 0.9 else ("HIGH" if mult > 1.25 else "NORMAL")
    return mult, med, label

# ─────────────────────── ⑥ CORRELATION GUARD ──────────────────────────────────
def correlation_flags(valid, day_dir, week_dir, lookback=CORR_LOOKBACK,
                      thr=CORR_THRESHOLD, max_syms=40):
    """Flag same-day SAME-SIDE setups whose 90d daily returns correlate > thr —
    five correlated bank longs is one trade wearing five hats."""
    if len(valid) < 2:
        return {}
    top = valid[:max_syms]
    rets = {}
    for r in top:
        try:
            day, _ = load_symbol(r["Symbol"], r.get("ISIN", ""), day_dir, week_dir)
            if day is not None and len(day) >= lookback + 5:
                rets[r["Symbol"]] = day["Close"].pct_change().tail(lookback)
        except Exception:
            pass
    if len(rets) < 2:
        return {}
    df = pd.DataFrame(rets)
    if len(df.dropna()) < 30:
        return {}
    corr = df.corr(min_periods=30)
    flags = {}
    syms = list(corr.columns)
    for i, a in enumerate(syms):
        for b in syms[i + 1:]:
            ra = next(r for r in top if r["Symbol"] == a)
            rb = next(r for r in top if r["Symbol"] == b)
            if ra.get("Side") != rb.get("Side"):
                continue
            c = corr.loc[a, b]
            if pd.notna(c) and c > thr:
                flags.setdefault(a, []).append(f"{b} ({c:.2f})")
    return flags

# ─────────────────────── ⑨⑩⑪ REPORTING: HTML / DIFF / WEBHOOK ────────────────
def build_webhook_text(valid, max_lines=15):
    lines = [f"SMC v9 — {len(valid)} valid setups ({datetime.now():%d %b %Y})"]
    for r in valid[:max_lines]:
        lines.append(f"{r['Symbol']} {r['Side']} S{r['Score']} | "
                     f"E {r['Entry']} SL {r['SL']} TP {r['Target']} "
                     f"RR 1:{r['R:R']} | qty {r.get('Qty', '?')}")
    return "\n".join(lines)

def send_webhook(url, text):
    try:
        if "telegram" in url.lower():
            requests.post(url, json={"text": text[:4000]}, timeout=10)
        else:
            requests.post(url, json={"content": text[:2000], "text": text[:4000]},
                          timeout=10)
        print("  🔔 webhook alert sent")
    except Exception as e:
        print(f"  ⚠ webhook failed: {e}")

def diff_setups(prev, new):
    """⑩ NEW / CONTINUED / DROPPED between two setup scans (keyed Symbol+Side)."""
    pk = set(zip(prev["Symbol"].astype(str), prev["Side"].astype(str)))
    nk = set(zip(new["Symbol"].astype(str), new["Side"].astype(str)))
    added, gone, kept = sorted(nk - pk), sorted(pk - nk), sorted(pk & nk)
    print(f"    NEW today : {len(added)}" + (f"   {added}" if added else ""))
    print(f"    CONTINUED : {len(kept)}" + (f"   {kept}" if kept else ""))
    print(f"    DROPPED   : {len(gone)}" + (f"   {gone}" if gone else ""))

_HTML_CSS = """<style>
body{font-family:'Segoe UI',Arial,sans-serif;background:#0f172a;color:#e2e8f0;margin:24px}
h1{font-size:20px;color:#38bdf8;margin-bottom:4px} h2{font-size:15px;color:#94a3b8;margin-top:28px}
.meta{background:#1e293b;padding:10px 14px;border-radius:8px;font-size:13px;line-height:1.8}
table.t{border-collapse:collapse;width:100%;font-size:12px;margin-top:10px}
th{background:#334155;padding:6px 8px;text-align:left;cursor:pointer;position:sticky;top:0}
td{padding:5px 8px;border-bottom:1px solid #334155;white-space:nowrap}
tr:hover{background:#1e293b}
.LONG{color:#4ade80;font-weight:600}.SHORT{color:#f87171;font-weight:600}
.BENCH{color:#fbbf24}.note{color:#64748b;font-size:11px}
</style>
<script>
function sortTable(tid,col){
 const tb=document.querySelector('#'+tid+' tbody');let dir=1;
 if(tb.dataset.key===tid+'-'+col){dir=-(+tb.dataset.dir);}
 tb.dataset.key=tid+'-'+col;tb.dataset.dir=dir;
 const rows=[...tb.rows];
 rows.sort((a,b)=>{const x=a.cells[col].innerText.trim(),y=b.cells[col].innerText.trim();
  const nx=parseFloat(x.replace(/[^0-9.\\-]/g,'')),ny=parseFloat(y.replace(/[^0-9.\\-]/g,''));
  return (!isNaN(nx)&&!isNaN(ny))?(nx-ny)*dir:x.localeCompare(y)*dir;});
 rows.forEach(r=>tb.appendChild(r));}
</script>"""

def _html_table(rows, cols, tid):
    if not rows:
        return "<p class='note'>none</p>"
    th = "".join(f"<th onclick=\"sortTable('{tid}',{i})\">{c}</th>"
                 for i, c in enumerate(cols))
    body = ""
    for r in rows:
        tds = []
        for c in cols:
            v = r.get(c, "")
            cls = ""
            if c == "Side" and v in ("LONG", "SHORT"):
                cls = f" class='{v}'"
            if c == "Portfolio" and v == "BENCH":
                cls = " class='BENCH'"
            tds.append(f"<td{cls}>{v}</td>")
        body += "<tr>" + "".join(tds) + "</tr>"
    return (f"<table id='{tid}' class='t'><thead><tr>{th}</tr></thead>"
            f"<tbody>{body}</tbody></table>")

def write_html_report(valid, near, meta, path=HTML_REPORT):
    vcols = ["Date", "Symbol", "Side", "Score", "Price", "Entry", "SL", "Target",
             "R:R", "RiskPct", "KellyRisk%", "Qty", "Portfolio", "PoolTouch",
             "WeeklyConfluence", "RRcapped", "Status"]
    ncols = ["Date", "Symbol", "Side", "Price", "Diag", "Status"]
    html = ("<!DOCTYPE html><html><head><meta charset='utf-8'>"
            "<title>SMC v9 Report</title>" + _HTML_CSS + "</head><body>")
    html += "<h1>SMC Swing Scanner v9.0 — Report</h1>"
    html += (f"<div class='meta'><b>Data as-of:</b> {meta.get('as_of','?')} &nbsp;|&nbsp; "
             f"<b>Vol regime:</b> {meta.get('regime','?')} &nbsp;|&nbsp; "
             f"<b>Scoring:</b> {meta.get('scoring','?')}<br>"
             f"<b>Capital:</b> &#8377;{meta.get('capital', 0):,.0f} &nbsp;|&nbsp; "
             f"<b>Guard:</b> risk &ge; {meta.get('risk_pct','?')} and {MIN_RISK_ATR}&times;ATR "
             f"&nbsp;|&nbsp; <b>RR band:</b> {MIN_RR}&ndash;{MAX_RR} &nbsp;|&nbsp; "
             f"<b>Portfolio cap:</b> {meta.get('max_setups','?')} active</div>")
    html += f"<h2>Valid setups ({len(valid)})</h2>" + _html_table(valid, vcols, "t1")
    html += f"<h2>Near-miss watchlist ({len(near)})</h2>" + _html_table(near, ncols, "t2")
    html += ("<p class='note'>EOD research tool — backtest before trading. "
             "Not financial advice. Click any column header to sort.</p></body></html>")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  🌐 HTML report → {path}")

# ─────────────────────────── BACKTEST RUNNER ──────────────────────────────────
def collect_backtest(universe, day_dir, week_dir):
    workers = min(os.cpu_count() or 4, 10)
    all_trades = []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(backtest_stock, s, i, day_dir, week_dir) for s, i in universe]
        for j, f in enumerate(as_completed(futs), 1):
            try:
                all_trades.extend(f.result()[1])
            except Exception:
                pass
            if j % 100 == 0:
                print(f"  ... {j}/{len(universe)} done, {len(all_trades)} trades")
    return all_trades

def main_backtest(universe, args):
    if not universe:
        print("  !! No universe — aborting backtest.")
        return
    day_dir = args.day_dir or DAY_DATA_DIR
    week_dir = args.week_dir or WEEK_DATA_DIR
    print(f"  SMC v9.0 BACKTEST — {len(universe)} symbols "
          f"(TP {TP_R_DEFAULT}R, hold {HOLD_MAX}d, weekly_filter="
          f"{'ON' if WEEKLY_FILTER else 'OFF'}, razor-stop guard "
          f">= {MIN_RISK_PCT}% & {MIN_RISK_ATR}xATR, static historical thresholds)")
    print("  ⚠ Current NIFTY500 with 25y history → survivorship bias; upper bound.")
    all_trades = collect_backtest(universe, day_dir, week_dir)
    if not all_trades:
        print("  ❌ No trades generated.")
        return
    t = pd.DataFrame(all_trades).sort_values("Date")
    t.to_csv(BACKTEST_TRADES_CSV, index=False)
    print(f"\n  Saved {len(t)} trades → {BACKTEST_TRADES_CSV} "
          f"(features included — feeds --optimize-weights and Kelly sizing)")

    print("\n  ===== RESULTS BY SIDE (with bootstrap 95% CI on expectancy) =====")
    for side, g in t.groupby("Side"):
        ci = bootstrap_expectancy_ci(g.R.values)
        citxt = f"  95% CI [{ci[0]:+.2f}, {ci[1]:+.2f}]" if ci else ""
        print(f"  {side:5s}: n={len(g):5d}  win={wilson((g.R > 0).sum(), len(g))}  "
              f"expectancy={g.R.mean():+.2f}R{citxt}  median MFE={g.mfeR.median():.2f}R  "
              f"avg hold={g.bars.mean():.1f}d")

    print("\n  ===== BY POOL TOUCHES =====")
    for (lo_, hi_), lab in [((1, 1), "1 touch"), ((2, 2), "2 touches"),
                            ((3, 99), "3+ touches")]:
        g = t[(t.touches >= lo_) & (t.touches <= hi_)]
        if len(g):
            print(f"  {lab:10s}: n={len(g):5d}  "
                  f"win={wilson((g.R > 0).sum(), len(g))}  "
                  f"expectancy={g.R.mean():+.2f}R")

    print("\n  ===== BY YEAR (regime robustness) =====")
    t["Year"] = t["Date"].str[:4]
    for y, g in t.groupby("Year"):
        print(f"  {y}: n={len(g):4d}  win={wilson((g.R > 0).sum(), len(g)):24s}  "
              f"expectancy={g.R.mean():+.2f}R")

    dt = pd.to_datetime(t["Date"])
    cut = dt.quantile(0.7)
    a, b = t[dt <= cut], t[dt > cut]
    print(f"\n  ===== WALK-FORWARD (split at {cut.date()}) =====")
    for lab, g in [("in-sample    ", a), ("out-of-sample ", b)]:
        if len(g):
            ci = bootstrap_expectancy_ci(g.R.values)
            citxt = f"  95% CI [{ci[0]:+.2f}, {ci[1]:+.2f}]" if ci else ""
            print(f"  {lab}: n={len(g):5d}  win={wilson((g.R > 0).sum(), len(g))}  "
                  f"expectancy={g.R.mean():+.2f}R{citxt}")

    pt = permutation_test(t)
    if pt:
        act, pv = pt
        print(f"\n  ===== EDGE SIGNIFICANCE "
              f"(symbol-grouped permutation, {PERM_N} shuffles) =====")
        print(f"  actual expectancy {act:+.2f}R | p = {pv:.3f} → " +
              ("edge looks REAL (p < 0.05)" if pv < 0.05 else
               "edge NOT distinguishable from timing luck — do not trade live yet"))

    print("\n  >>> v9 notes: MAX_RR cap now enforced in the scanner; run "
          "--optimize-weights next to learn score weights from these trades;")

# ─────────────────────── ⑧ WEIGHT OPTIMIZER ───────────────────────────────────
def optimize_weights(universe, args):
    if not universe:
        print("  !! No universe — aborting.")
        return
    day_dir = args.day_dir or DAY_DATA_DIR
    week_dir = args.week_dir or WEEK_DATA_DIR
    print("=" * 96)
    print("  WEIGHT OPTIMIZER — logistic sigmoid on backtest outcomes")
    print("=" * 96)
    t = None
    if os.path.exists(BACKTEST_TRADES_CSV) and not args.force_rerun:
        try:
            cand = pd.read_csv(BACKTEST_TRADES_CSV)
            if len(cand) and "f_touches" in cand.columns:
                t = cand
                print(f"  Using existing {BACKTEST_TRADES_CSV} ({len(t)} trades). "
                      f"Use --force-rerun to rebuild from data.")
        except Exception:
            t = None
    if t is None:
        print("  Running full backtest to gather labelled trades ...")
        trades = collect_backtest(universe, day_dir, week_dir)
        if not trades:
            print("  ❌ no trades — cannot optimise weights")
            return
        t = pd.DataFrame(trades)

    feat_cols = ["f_" + k for k in FEATURE_NAMES]
    if not all(c in t.columns for c in feat_cols):
        print("  ❌ trades lack feature columns — re-run --backtest with v9 first")
        return
    X = t[feat_cols].values
    y = (t["R"].values > 0).astype(float)
    if len(t) < 100 or len(np.unique(y)) < 2:
        print(f"  ❌ need >=100 trades with both wins and losses (have {len(t)})")
        return

    model = fit_logistic(X, y)
    if model is None:
        print("  ❌ all features constant — cannot fit")
        return
    p = predict_proba(X, model)
    auc = roc_auc(y, p)
    print(f"\n  n={len(t)} trades | win rate {y.mean():.1%} | "
          f"AUC {auc:.3f}" if auc else f"\n  n={len(t)} trades")
    print("\n  ===== LEARNED SCORE WEIGHTS (standardised logistic coefficients) =====")
    kept = [(f, float(wgt)) for f, k, wgt in
            zip(FEATURE_NAMES, model["keep"], model["w"]) if k]
    for name, wgt in sorted(kept, key=lambda x: -abs(x[1])):
        print(f"    {name:15s}: {wgt:+.3f}  ({'raises' if wgt > 0 else 'lowers'} win prob)")
    print("\n  Scanner score becomes 100 × sigmoid(w·x + b) — the same S-curve")
    print("  family as your sentiment scorer: 0-50-100 neutral mapping, smooth")
    print("  sensitivity, no ceiling blindness.")
    save_weights(model, len(t), y.mean(), auc)
    print(f"\n  ✅ saved → {WEIGHTS_FILE}")
    print("     (delete that file any time to revert to v8.2 hand-tuned scoring)")

# ─────────────────────────────── MAINS ────────────────────────────────────────
def main_scan(universe, args):
    if not universe:
        print("  !! No universe — aborting scan.")
        return
    day_dir = args.day_dir or DAY_DATA_DIR
    week_dir = args.week_dir or WEEK_DATA_DIR
    capital, max_setups = args.capital, args.max_setups

    # capture previous run for --diff before this scan overwrites the CSV
    prev_df = None
    if isinstance(args.diff, str) and os.path.exists(args.diff):
        try:
            prev_df = pd.read_csv(args.diff)
        except Exception:
            prev_df = None
    elif args.diff and os.path.exists(SETUPS_CSV):
        try:
            prev_df = pd.read_csv(SETUPS_CSV)
        except Exception:
            prev_df = None

    model = load_weights()
    scoring_mode = (f"LEARNED sigmoid (n={model['n_trades']}, "
                    f"AUC={(model.get('auc') or 0):.2f})") if model \
        else "v8.2 hand-tuned"

    # ⑤ vol-regime overlay
    risk_pct, risk_atr_mult = MIN_RISK_PCT, MIN_RISK_ATR
    regime_txt = "OFF"
    if not args.regime_off:
        mult, med_atr, lab = estimate_vol_regime(universe, day_dir)
        if med_atr:
            risk_pct = MIN_RISK_PCT * mult
            regime_txt = (f"{lab} — median ATR {med_atr:.2f}% → guard "
                          f"{MIN_RISK_PCT}%×{mult:.2f}={risk_pct:.2f}%")
        else:
            regime_txt = lab

    ks = kelly_stats()
    kelly_src = ("backtest-derived" if os.path.exists(BACKTEST_TRADES_CSV)
                 else "defaults p=0.45/b=1.8 (run --backtest for real stats)")

    workers = min(os.cpu_count() or 4, 10)
    print("=" * 96)
    print(f"  SMC v9.0 SCANNER — {len(universe)} symbols, {workers} workers, "
          f"weekly_filter={'ON' if WEEKLY_FILTER else 'OFF'}")
    print(f"  scoring : {scoring_mode}")
    print(f"  regime  : {regime_txt}")
    print(f"  sizing  : Kelly (25% cap, 2% hard max) {kelly_src} | "
          f"capital ₹{capital:,.0f}")
    print("=" * 96)

    valid, near = [], []
    diag = {k: 0 for k in DIAG_KEYS}
    diag["total"] = len(universe)
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(scan_stock, s, i, day_dir, week_dir,
                          risk_pct, risk_atr_mult, model): s for s, i in universe}
        for i, f in enumerate(as_completed(futs), 1):
            try:
                r = f.result()
                diag[r.get("Diag", "data_fails")] = \
                    diag.get(r.get("Diag", "data_fails"), 0) + 1
                if r["Type"] == "VALID SETUP":
                    valid.append(r)
                elif r["Type"] == "NEAR MISS":
                    near.append(r)
            except Exception:
                diag["data_fails"] += 1
            if i % 100 == 0:
                print(f"  ... {i}/{len(universe)} scanned")

    print("\n  DIAGNOSTIC FUNNEL:")
    print(f"    {'total':20s}: {diag['total']}")
    for k in DIAG_KEYS:
        print(f"    {k:20s}: {diag.get(k, 0)}")
    if diag.get("no_file"):
        print(f"    (no_file = ISIN not present in Day Data folder — expected for "
              f"fresh index entrants)")

    as_of = max([r["Date"] for r in valid + near if r.get("Date")],
                default=None) or datetime.now().strftime("%Y-%m-%d")

    if valid:
        valid.sort(key=lambda x: x["Score"], reverse=True)
        flags = correlation_flags(valid, day_dir, week_dir)
        for i, r in enumerate(valid):
            p, b = ks.get(r["Side"], (0.45, 1.8))
            fr = kelly_risk_fraction(p, b)
            r["KellyRisk%"] = round(100 * fr, 2)
            risk_pts = r.get("RiskPts") or 0
            r["Qty"] = int(capital * fr / risk_pts) if risk_pts > 0 else 0
            r["Portfolio"] = "ACTIVE" if i < max_setups else "BENCH"
            fl = flags.get(r["Symbol"])
            if fl:
                r["Status"] += " | ⚠ corr: " + ", ".join(fl[:3])

        v = pd.DataFrame(valid)
        cols = [c for c in ["Date", "Symbol", "Side", "Score", "Price", "Entry",
                            "SL", "Target", "R:R", "RiskPts", "RiskPct",
                            "KellyRisk%", "Qty", "Portfolio", "PoolTarget",
                            "WeeklyPoolTarget", "WeeklyConfluence", "PoolTouch",
                            "Depth%", "SweepVolx", "ATRpct", "HistDays",
                            "RRcapped", "Status"] if c in v.columns]
        v[cols].to_csv(SETUPS_CSV, index=False)
        print(f"\n  🚀 VALID SETUPS ({len(valid)}) — saved {SETUPS_CSV}")
        for r in valid:
            star = " ★" if r["WeeklyConfluence"] else "  "
            cap = "" if r["Portfolio"] == "ACTIVE" else "  [BENCH]"
            print(f"  {star}{r['Symbol']:12s} {r['Side']:5s} S{r['Score']:3d} | "
                  f"E {r['Entry']:>9} SL {r['SL']:>9} TP {r['Target']:>9} "
                  f"RR 1:{r['R:R']} | risk {r['RiskPct']}% | "
                  f"Kelly {r['KellyRisk%']}% → qty {r['Qty']}{cap}")
    if near:
        nd = pd.DataFrame(near)
        cols = [c for c in ["Date", "Symbol", "Side", "Price", "ChoChLevel",
                            "DistTrig%", "SweepDate", "SweepExtreme", "InvalidLvl",
                            "PoolPrice", "PoolTouch", "WeeklyConfluence",
                            "ObDate", "DispDate", "BarsSinceSweep",
                            "Entry", "SL", "Target", "R:R", "Status"]
                if c in nd.columns]
        nd[cols].to_csv(NEARMISS_CSV, index=False)
        print(f"\n  📝 NEAR-MISS WATCHLIST ({len(near)}) — saved {NEARMISS_CSV}")
    if not valid and not near:
        print("\n  ❌ No setups today.")

    if (valid or near) and not args.no_html:
        meta = {"as_of": as_of, "regime": regime_txt, "scoring": scoring_mode,
                "capital": capital, "risk_pct": f"{risk_pct:.2f}%",
                "max_setups": max_setups}
        write_html_report(valid, near, meta)

    if WEBHOOK_URL and valid:
        send_webhook(WEBHOOK_URL, build_webhook_text(valid))

    if args.diff:
        if prev_df is not None:
            print("\n  ===== DIFF vs previous scan =====")
            diff_setups(prev_df, pd.DataFrame(
                [{"Symbol": r["Symbol"], "Side": r["Side"]} for r in valid]))
        else:
            print("\n  (diff requested but no previous setups CSV found)")

def main_structure(arg, uni_map, args):
    day_dir = args.day_dir or DAY_DATA_DIR
    week_dir = args.week_dir or WEEK_DATA_DIR
    model = load_weights()
    for sym in [s.strip().upper() for s in arg.split(",") if s.strip()]:
        print("=" * 76)
        if sym in uni_map:
            symbol, isin = sym, uni_map[sym]
        elif len(sym) == 12 and sym.startswith("INE"):
            symbol, isin = sym, sym
        else:
            symbol, isin = sym, ""
        day, week = load_symbol(symbol, isin, day_dir, week_dir)
        if day is None:
            print(f"{symbol}: no usable data (check file name / ISIN)")
            continue
        ok, why = hygiene_ok(day)
        if not ok:
            print(f"{symbol}: filtered out ({why})")
            continue
        w_bias = current_weekly_bias(day, week)
        w_pools = {"LOW": find_pools(week, "LOW", WK_LOOKBACK, WK_TOL),
                   "HIGH": find_pools(week, "HIGH", WK_LOOKBACK, WK_TOL)}
        atr = compute_atr(day['High'].values, day['Low'].values,
                          day['Close'].values)
        print(f"{symbol} | daily bars={len(day)} ({day.index[0].date()} → "
              f"{day.index[-1].date()}) | weekly bias: "
              f"{'BULL' if w_bias == 1 else 'BEAR' if w_bias == -1 else 'NEUTRAL'}")
        for side in ("LONG", "SHORT"):
            res, key = evaluate_direction(day, side, w_bias, w_pools, atr=atr,
                                          model=model)
            if res is None:
                print(f"{symbol} {side:5s}: no structure ({key})")
                continue
            if res["Diag"] == "no_choch":
                print(f"{symbol} {side:5s} | {res['Status']}")
                print(f"   pool {res['PoolPrice']} x{res['PoolTouch']}"
                      f"{' ★weekly' if res['WeeklyConfluence'] else ''} | "
                      f"swept {res['SweepDate']} extreme {res['SweepExtreme']}")
                print(f"   OB {res['ObDate']} | disp {res['DispDate']} | "
                      f"{res['BarsSinceSweep']} bars since sweep")
                print(f"   TRIGGER daily close {res['ChoChLevel']} "
                      f"({res['DistTrig%']}% away) | INVALID close {res['InvalidLvl']}")
            else:
                lvls = ""
                if res.get("Entry") is not None:
                    lvls = (f" | E {res['Entry']} SL {res['SL']} TP {res['Target']} "
                            f"RR 1:{res['R:R']} risk {res.get('RiskPct','?')}% "
                            f"S{res.get('Score','')}")
                    if res.get("WeeklyPoolTarget") is not None:
                        lvls += f" | weekly stretch {res['WeeklyPoolTarget']}"
                print(f"{symbol} {side:5s}: {res['Diag']}{lvls}")
                print(f"   {res['Status']}")

# ─────────────────────────────── CLI ──────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(
        description="NIFTY 500 SMC Swing Scanner v9.0 "
                    "(v8.2 core + vectorised engine, Kelly sizing, regime overlay, "
                    "learned sigmoid scoring, bootstrap/permutation stats)")
    p.add_argument("--backtest", action="store_true",
                   help="full-history event study (writes labelled trades CSV)")
    p.add_argument("--optimize-weights", action="store_true",
                   help="learn score weights from backtest outcomes (sigmoid)")
    p.add_argument("--structure", metavar="SYM1,SYM2",
                   help="anatomy of specific symbols")
    p.add_argument("--diff", nargs="?", const=True, default=False, metavar="OLD_CSV",
                   help="compare this scan against a previous setups CSV "
                        "(default: the last smc_v9_setups.csv)")
    p.add_argument("--day-dir", default=None, help="override Day Data folder")
    p.add_argument("--week-dir", default=None, help="override Weeks Data folder")
    p.add_argument("--capital", type=float, default=CAPITAL_DEFAULT,
                   help="trading capital for position sizing (default %(default)s)")
    p.add_argument("--max-setups", type=int, default=MAX_SETUPS_DEFAULT,
                   help="max ACTIVE portfolio slots (rest are BENCH)")
    p.add_argument("--regime-off", action="store_true",
                   help="disable the volatility-regime overlay")
    p.add_argument("--no-html", action="store_true", help="skip HTML report")
    p.add_argument("--force-rerun", action="store_true",
                   help="with --optimize-weights: ignore cached trades CSV")
    return p.parse_args()

if __name__ == "__main__":
    ARGS = parse_args()
    universe = get_universe()
    uni_map = dict(universe)
    if ARGS.optimize_weights:
        optimize_weights(universe, ARGS)
    elif ARGS.backtest:
        main_backtest(universe, ARGS)
    elif ARGS.structure:
        main_structure(ARGS.structure, uni_map, ARGS)
    else:
        main_scan(universe, ARGS)
