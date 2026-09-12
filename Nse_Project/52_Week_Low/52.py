"""
╔════════════════════════════════════════════════════════════════════════════════╗
║  NIFTY 500 — SMC SWING SCANNER v8.2 (Daily + Weekly, ISIN files)               ║
║                                                                                ║
║  v8.2: razor-stop guard (MIN_RISK_PCT/ATR) + RR cap, bias_block labeling fix,  ║
║        backtest shares the same stop guard                                     ║
║  v8.1: ISIN file lookup, partial-week trim, rich near-miss levels              ║
║                                                                                ║
║  RUN:  python 52.py                        -> scan today                       ║
║        python 52.py --backtest             -> full-history event study         ║
║        python 52.py --structure IKS,LTF    -> anatomy of specific symbols      ║
║  NEED: pip install pandas numpy requests pyarrow                               ║
╚════════════════════════════════════════════════════════════════════════════════╝
"""
import os, sys, warnings
import numpy as np
import pandas as pd
import requests
from concurrent.futures import ProcessPoolExecutor, as_completed
from io import StringIO

warnings.filterwarnings("ignore")

# ───────────────────────────── CONFIG ──────────────────────────────────────────
DAY_DATA_DIR  = r"D:\Api data\ML_HISTO_DATA\Upstox Data\Day Data"
WEEK_DATA_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox Data\Weeks Data"
UNIVERSE_CACHE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "nifty500_universe.csv")

MIN_DAILY_BARS = 260
MIN_RR         = 2.0
MAX_RR         = 10.0        # ② absurd-RR cap (e.g. 39.7R illusions)
MIN_RISK_PCT   = 1.0         # ① stop must be >= 1.0% away from entry
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

def load_symbol(symbol, isin):
    day_path = _find_file(DAY_DATA_DIR, symbol, isin)
    day = _read_ohlcv(day_path, need_volume=True) if day_path else None
    if day is None or len(day) < 60:
        return None, None

    week_path = _find_file(WEEK_DATA_DIR, symbol, isin)
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

# ─────────────────────── LIQUIDITY POOLS (no look-ahead) ───────────────────────
def find_pools(df, side, lookback=POOL_LOOKBACK, tol=POOL_TOL, confirm=POOL_CONFIRM):
    v = (df['Low'] if side == "LOW" else df['High']).values
    n = len(v)
    swings = []
    for i in range(confirm, n - confirm):
        if side == "LOW":
            if v[i] <= v[i - confirm:i].min() and v[i] <= v[i + 1:i + confirm + 1].min():
                swings.append((i, v[i]))
        else:
            if v[i] >= v[i - confirm:i].max() and v[i] >= v[i + 1:i + confirm + 1].max():
                swings.append((i, v[i]))
    pools = []
    for i, p in [s for s in swings if s[0] >= n - lookback]:
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
    Swings confirmed 2 weeks later -> no look-ahead."""
    wC, wH, wL = week['Close'].values, week['High'].values, week['Low'].values
    W = len(week)
    sh, sl = [], []
    for i in range(2, W - 2):
        if wH[i] >= wH[i-1] and wH[i] >= wH[i-2] and wH[i] >= wH[i+1] and wH[i] >= wH[i+2]:
            sh.append((i, wH[i]))
        if wL[i] <= wL[i-1] and wL[i] <= wL[i-2] and wL[i] <= wL[i+1] and wL[i] <= wL[i+2]:
            sl.append((i, wL[i]))
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
    k = int(np.searchsorted(wend.values, day.index[-1], side='left')) - 1
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

# ─────────────────────────── SCORING (0–100) ───────────────────────────────────
def score_setup(touches, depth, sweep_volx, bodyx, disp_volx, has_fvg, rr, pos,
                weekly_aligned, weekly_confluence):
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

# ─────────────────── CORE SETUP LOGIC (both directions) ────────────────────────
def evaluate_direction(day, side, w_bias, w_pools):
    """-> (result | None, diag_key)."""
    if WEEKLY_FILTER and w_bias != 0:
        if w_bias == 1 and side != "LONG":
            return None, "bias_block"
        if w_bias == -1 and side != "SHORT":
            return None, "bias_block"

    n = len(day)
    last = n - 1
    O, H, L, C, V = (day[c].values for c in ['Open', 'High', 'Low', 'Close', 'Volume'])
    close = C[last]

    # ATR(14) for the razor-stop guard
    tr = np.maximum(H - L, np.maximum(np.abs(H - np.roll(C, 1)),
                                      np.abs(L - np.roll(C, 1))))
    tr[0] = H[0] - L[0]
    atr = float(pd.Series(tr).ewm(alpha=1/ATR_PERIOD,
                                  min_periods=ATR_PERIOD, adjust=False).mean().iloc[-1])

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

    # ① RAZOR-STOP GUARD: stop must sit outside noise
    if risk < max(MIN_RISK_PCT / 100 * close, MIN_RISK_ATR * atr):
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

    if rr < MIN_RR:
        st = f"Valid {side} structure but R:R {rr} < {MIN_RR}"
        return {"Type": "NEAR MISS", "Diag": "low_rr", "Side": side,
                "Price": round(close, 2), "Date": str(day.index[last].date()),
                "Entry": round(entry, 2), "SL": round(sl, 2),
                "Target": round(tp, 2), "R:R": rr, "Status": st}, "low_rr"

    score = score_setup(sweep_pool['touches'], depth, sweep_volx, bodyx,
                        disp_volx, fvg_idx is not None, rr, pos,
                        (w_bias == 1 and side == "LONG") or
                        (w_bias == -1 and side == "SHORT"), confluence)
    extreme = "AT 52W LOW" if side == "LONG" else "AT 52W HIGH"
    zone = "Discount" if side == "LONG" else "Premium"
    reasons = [f"{extreme} ({prox * 100:.1f}%)",
               f"Sweep pool x{sweep_pool['touches']} (depth {depth:.1f}%, "
               f"vol {sweep_volx:.1f}x)" + (" ★WEEKLY pool" if confluence else ""),
               f"Vol-OB@{day.index[ob_idx].date()} Disp@{day.index[disp_idx].date()} "
               f"CHoCH@{day.index[choch_idx].date()}",
               f"{zone} {pos * 100:.0f}% | weekly bias "
               f"{'BULL' if w_bias == 1 else 'BEAR' if w_bias == -1 else 'NEUTRAL'}"]
    res = {"Type": "VALID SETUP", "Diag": "valid_setups", "Side": side,
           "Score": score, "Price": round(close, 2), "Entry": round(entry, 2),
           "SL": round(sl, 2), "Target": round(tp, 2), "R:R": rr,
           "RiskPts": round(risk, 2), "RiskPct": round(100 * risk / entry, 2),
           "PoolTarget": round(pool_tp, 2) if pool_tp is not None else None,
           "WeeklyPoolTarget": round(wk_tp, 2) if wk_tp is not None else None,
           "WeeklyConfluence": confluence,
           "PoolTouch": sweep_pool['touches'], "Depth%": round(depth, 1),
           "SweepVolx": round(sweep_volx, 1),
           "Date": str(day.index[last].date()), "Status": " | ".join(reasons)}
    return res, "valid_setups"

# ───────────────────────────── WORKER ──────────────────────────────────────────
def scan_stock(symbol, isin):
    base = {"Symbol": symbol, "Type": "NONE", "Diag": "data_fails", "Side": "",
            "Score": 0, "Price": 0, "Entry": None, "SL": None, "Target": None,
            "R:R": None, "Date": "", "Status": "", "HistDays": 0}
    if _find_file(DAY_DATA_DIR, symbol, isin) is None:
        base["Diag"] = "no_file"
        base["Status"] = "day file not found (ISIN not in data folder)"
        return base
    day, week = load_symbol(symbol, isin)
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

    w_bias = current_weekly_bias(day, week)
    w_pools = {"LOW": find_pools(week, "LOW", WK_LOOKBACK, WK_TOL),
               "HIGH": find_pools(week, "HIGH", WK_LOOKBACK, WK_TOL)}

    # ③ per-side keys: report the ALLOWED side's real failure, not bias_block
    best, keys = None, {}
    for side in ("LONG", "SHORT"):
        res, key = evaluate_direction(day, side, w_bias, w_pools)
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
def backtest_stock(symbol, isin):
    """Conservative fills: gap-through-stop at open, same-bar SL&TP -> SL,
    time stop at close. ④ Same razor-stop guard as the scanner."""
    day, week = load_symbol(symbol, isin)
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
        v = L if side == "LONG" else H
        out = []
        for i in range(POOL_CONFIRM, n - POOL_CONFIRM):
            if side == "LONG":
                if v[i] <= v[i - POOL_CONFIRM:i].min() and \
                   v[i] <= v[i + 1:i + POOL_CONFIRM + 1].min():
                    out.append((i, v[i]))
            else:
                if v[i] >= v[i - POOL_CONFIRM:i].max() and \
                   v[i] >= v[i + 1:i + POOL_CONFIRM + 1].max():
                    out.append((i, v[i]))
        return out

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
            # ④ same razor-stop guard
            atr_t = atr_s[k] if not np.isnan(atr_s[k]) else 0.0
            if risk < max(MIN_RISK_PCT / 100 * entry, MIN_RISK_ATR * atr_t):
                continue

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
                           "bars": m - k})
    return symbol, trades

# ─────────────────── STRUCTURE MODE (--structure SYM1,SYM2) ────────────────────
def main_structure(arg, uni_map):
    for sym in [s.strip().upper() for s in arg.split(",") if s.strip()]:
        print("=" * 76)
        if sym in uni_map:
            symbol, isin = sym, uni_map[sym]
        elif len(sym) == 12 and sym.startswith("INE"):
            symbol, isin = sym, sym
        else:
            symbol, isin = sym, ""
        day, week = load_symbol(symbol, isin)
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
        print(f"{symbol} | daily bars={len(day)} ({day.index[0].date()} → "
              f"{day.index[-1].date()}) | weekly bias: "
              f"{'BULL' if w_bias == 1 else 'BEAR' if w_bias == -1 else 'NEUTRAL'}")
        for side in ("LONG", "SHORT"):
            res, key = evaluate_direction(day, side, w_bias, w_pools)
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

# ──────────────────────────── REPORTING ────────────────────────────────────────
def wilson(k, n):
    if n == 0:
        return "n/a"
    p = k / n
    se = (p * (1 - p) / n) ** 0.5
    return f"{p:.1%} [{max(0.0, p - 1.96 * se):.1%}, {min(1.0, p + 1.96 * se):.1%}]"

def main_scan(universe):
    workers = min(os.cpu_count() or 4, 10)
    print("=" * 96)
    print(f"  SMC v8.2 SCANNER (Daily+Weekly, ISIN files) — {len(universe)} symbols, "
          f"{workers} workers, weekly_filter={'ON' if WEEKLY_FILTER else 'OFF'}")
    print("=" * 96)
    valid, near = [], []
    diag = {k: 0 for k in DIAG_KEYS}
    diag["total"] = len(universe)
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(scan_stock, s, i): s for s, i in universe}
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

    if valid:
        valid.sort(key=lambda x: x["Score"], reverse=True)
        v = pd.DataFrame(valid)[["Date", "Symbol", "Side", "Score", "Price",
                                 "Entry", "SL", "Target", "R:R", "RiskPts",
                                 "RiskPct", "PoolTarget", "WeeklyPoolTarget",
                                 "WeeklyConfluence", "PoolTouch", "Depth%",
                                 "SweepVolx", "HistDays", "Status"]]
        v.to_csv("smc_v8_setups.csv", index=False)
        print(f"\n  🚀 VALID SETUPS ({len(valid)}) — saved smc_v8_setups.csv")
        for r in valid:
            star = " ★" if r["WeeklyConfluence"] else "  "
            print(f"  {star}{r['Symbol']:12s} {r['Side']:5s} S{r['Score']:3d} | "
                  f"E {r['Entry']:>9} SL {r['SL']:>9} TP {r['Target']:>9} "
                  f"RR 1:{r['R:R']} | risk {r['RiskPct']}% | pool x{r['PoolTouch']}")
    if near:
        nd = pd.DataFrame(near)
        cols = [c for c in ["Date", "Symbol", "Side", "Price", "ChoChLevel",
                            "DistTrig%", "SweepDate", "SweepExtreme", "InvalidLvl",
                            "PoolPrice", "PoolTouch", "WeeklyConfluence",
                            "ObDate", "DispDate", "BarsSinceSweep",
                            "Entry", "SL", "Target", "R:R", "Status"]
                if c in nd.columns]
        nd[cols].to_csv("smc_v8_nearmiss.csv", index=False)
        print(f"\n  📝 NEAR-MISS WATCHLIST ({len(near)}) — saved smc_v8_nearmiss.csv")
    if not valid and not near:
        print("\n  ❌ No setups today.")

def main_backtest(universe):
    workers = min(os.cpu_count() or 4, 10)
    print(f"  SMC v8.2 BACKTEST — {len(universe)} symbols, {workers} workers "
          f"(TP {TP_R_DEFAULT}R, hold {HOLD_MAX}d, weekly_filter="
          f"{'ON' if WEEKLY_FILTER else 'OFF'}, razor-stop guard "
          f">= {MIN_RISK_PCT}% & {MIN_RISK_ATR}xATR)")
    print("  ⚠ Current NIFTY500 with 25y history → survivorship bias; upper bound.")
    all_trades = []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(backtest_stock, s, i) for s, i in universe]
        for j, f in enumerate(as_completed(futs), 1):
            try:
                all_trades.extend(f.result()[1])
            except Exception:
                pass
            if j % 100 == 0:
                print(f"  ... {j}/{len(universe)} done, {len(all_trades)} trades")
    if not all_trades:
        print("  ❌ No trades generated.")
        return
    t = pd.DataFrame(all_trades).sort_values("Date")
    t.to_csv("smc_v8_backtest_trades.csv", index=False)

    print("\n  ===== RESULTS BY SIDE =====")
    for side, g in t.groupby("Side"):
        print(f"  {side:5s}: n={len(g):5d}  win={wilson((g.R > 0).sum(), len(g))}  "
              f"expectancy={g.R.mean():+.2f}R  median MFE={g.mfeR.median():.2f}R  "
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
            print(f"  {lab}: n={len(g):5d}  win={wilson((g.R > 0).sum(), len(g))}  "
                  f"expectancy={g.R.mean():+.2f}R")

    print("\n  >>> Compare v8.1 vs v8.2: the guard should REMOVE degenerate trades;")
    print("      expectancy should stay similar or improve, n drops slightly.")

# ─────────────────────────────── MAIN ──────────────────────────────────────────
if __name__ == "__main__":
    universe = get_universe()
    uni_map = dict(universe)
    if "--backtest" in sys.argv:
        main_backtest(universe)
    elif "--structure" in sys.argv:
        i = sys.argv.index("--structure")
        arg = sys.argv[i + 1] if i + 1 < len(sys.argv) else ""
        main_structure(arg, uni_map)
    else:
        main_scan(universe)