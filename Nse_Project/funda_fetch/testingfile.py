"""
swing_sr_validator_v2.3.py — SEPA-style swing validator, fully local, no yfinance.

DATA    : local parquets  Day Data / Weeks Data  (NSE_EQ_<ISIN>.parquet)
UNIVERSE: NSE nifty500 list (Symbol, Industry, ISIN) — auto-located
AS-OF   : snapshot date = latest bar date in the data (NOT the wall clock)
GATES   : trend -> RSI band -> relative volume(5-bar event) ->
          breakout(5-bar window, level must still hold) or retest(52w, ATR band)
          -> sector strength (LOCAL industry RSI/breadth, thin-bucket policy)
          -> structural stop + R:R
RUN     : python testingfile.py
NEEDS   : pip install pandas numpy pyarrow
"""
import os, json, warnings
import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed

warnings.filterwarnings("ignore")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def _first_existing(*paths):
    for p in paths:
        if p and os.path.exists(p):
            return p
    raise FileNotFoundError(
        "File not found. Searched:\n" + "\n".join(f"  - {p}" for p in paths))

# ═══════════════════════ CONFIG (single source of truth) ═══════════════════════
CONFIG = {
    "CANDIDATES_CSV": _first_existing(
        os.path.join(SCRIPT_DIR, "nifty500_swing_candidates.csv"),
        r"C:\Users\ADMIN\Desktop\MyProject2\Nse_Project\funda_fetch\nifty500_swing_candidates.csv",
    ),
    "UNIVERSE_CSV": _first_existing(
        os.path.join(SCRIPT_DIR, "nifty500_universe.csv"),
        os.path.join(SCRIPT_DIR, "ind_nifty500list.csv"),
        r"C:\Users\ADMIN\Desktop\MyProject2\Nse_Project\52_Week_Low\nifty500_universe.csv",
        r"C:\Users\ADMIN\Desktop\MyProject2\Nse_Project\funda_fetch\ind_nifty500list.csv",
    ),

    "DAY_DIR":  r"D:\Api data\ML_HISTO_DATA\Upstox Data\Day Data",
    "WEEK_DIR": r"D:\Api data\ML_HISTO_DATA\Upstox Data\Weeks Data",

    "OUT_JSON":    os.path.join(SCRIPT_DIR, "validated_swing_setups.json"),
    "OUT_METRICS": os.path.join(SCRIPT_DIR, "all_candidates_metrics.csv"),

    "MIN_BARS": 120,
    "MAX_STALE_DAYS": 5,      # vs SNAPSHOT date, not clock

    "EMA_FAST": 20, "EMA_SLOW": 50,
    "RSI_PERIOD": 14, "RSI_MIN": 50, "RSI_MAX": 80,

    "RVOL_BASE_WINDOW": 20,
    "RVOL_LOOKBACK": 5,
    "RVOL_MIN": 1.2,

    "BRK_RES_LOOKBACK": 20,
    "BRK_ATR_CUSHION": 0.2,
    "BRK_EVENT_WINDOW": 5,

    "SWING_CONFIRM": 3,
    "RETEST_LOOKBACK_WEEKS": 52,
    "RETEST_ATR_MULT": 1.0,

    "ATR_PERIOD": 14,
    "WIDE_STOP_ATR_MULT": 3.0,
    "MIN_RR": 2.0,
    "TARGET_RR_FALLBACK": 2.5,

    "SECTOR_RSI_SPREAD": 3.0,
    "FULL_MIN": 15,
    "THIN_MIN": 6,
}

# ═══════════════════════════ small technical helpers ═══════════════════════════
def ema(s: pd.Series, span: int) -> float:
    """Last value of the EMA. (.ewm is lazy — .mean() computes it.)"""
    return float(s.ewm(span=span, adjust=False).mean().iloc[-1])

def wilder_rsi(close: pd.Series, period: int) -> float:
    """Wilder RSI. NOTE: all day-file columns are LOWERCASE ('close')."""
    d = close.diff()
    ag = d.clip(lower=0).ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    al = (-d.clip(upper=0)).ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = ag / al.replace(0, np.nan)
    r = 100 - 100 / (1 + rs)
    return float(r.iloc[-1]) if r.notna().any() else np.nan

def wilder_atr(df: pd.DataFrame, period: int) -> float:
    """Wilder ATR. NOTE: all day-file columns are LOWERCASE ('high','low','close')."""
    h, l, c = df["high"], df["low"], df["close"]
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    a = tr.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    return float(a.iloc[-1]) if a.notna().any() else np.nan

def read_day(isin: str):
    path = os.path.join(CONFIG["DAY_DIR"], f"NSE_EQ_{isin}.parquet")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_parquet(path)
        tcol = next((c for c in df.columns
                     if "time" in str(c).lower() or "date" in str(c).lower()), None)
        if tcol is None:
            return None
        df[tcol] = pd.to_datetime(df[tcol], errors="coerce")
        df = df.dropna(subset=[tcol]).set_index(tcol).sort_index()
        df.columns = [str(c).strip().lower() for c in df.columns]   # -> lowercase
        need = ["open", "high", "low", "close", "volume"]
        if not all(c in df.columns for c in need):
            return None
        df = df[need].apply(pd.to_numeric, errors="coerce").dropna()
        return df if len(df) else None
    except Exception:
        return None

def load_weekly(isin: str, day: pd.DataFrame) -> pd.DataFrame:
    """Prefer local weekly file; trim beyond daily range and drop partial week;
    fall back to resampling daily into Friday-ending weeks."""
    week = None
    path = os.path.join(CONFIG["WEEK_DIR"], f"NSE_EQ_{isin}.parquet")
    if os.path.exists(path):
        try:
            w = pd.read_parquet(path)
            tcol = next((c for c in w.columns
                         if "time" in str(c).lower() or "date" in str(c).lower()), None)
            if tcol is not None:
                w[tcol] = pd.to_datetime(w[tcol], errors="coerce")
                w = w.dropna(subset=[tcol]).set_index(tcol).sort_index()
                w.columns = [str(c).strip().lower() for c in w.columns]
                if all(c in w.columns for c in ["open", "high", "low", "close"]):
                    week = w[["open", "high", "low", "close"]].dropna()
        except Exception:
            week = None
    if week is not None:
        while len(week) and week.index[-1] > day.index[-1]:
            week = week.iloc[:-1]
        if len(week) and (day.index[-1] - week.index[-1]).days < 4:
            week = week.iloc[:-1]                       # drop partial current week
        if len(week) < 30 or (day.index[-1] - week.index[-1]).days > 10:
            week = None
    if week is None:
        week = (day.resample("W-FRI")
                   .agg({"open": "first", "high": "max",
                         "low": "min", "close": "last"}).dropna())
    return week

def confirmed_weekly_swings(week: pd.DataFrame, lookback: int):
    """-> (swing_lows, swing_highs) as [(index_ts, price)]; each swing uses data
    up to i+lookback only (no look-ahead)."""
    H, L = week["high"].values, week["low"].values
    n = len(week)
    lo, hi = [], []
    for i in range(lookback, n - lookback):
        if L[i] <= L[i - lookback:i + lookback + 1].min():
            lo.append((week.index[i], L[i]))
        if H[i] >= H[i - lookback:i + lookback + 1].max():
            hi.append((week.index[i], H[i]))
    return lo, hi

def detect_snapshot_date(universe: dict, sample: int = 30) -> pd.Timestamp:
    """The data's own 'today': max last-bar date across a sample of files."""
    last = pd.Timestamp(0)
    checked = 0
    for sym, v in universe.items():
        df = read_day(v["isin"])
        if df is not None and len(df):
            last = max(last, df.index[-1].normalize())
        checked += 1
        if checked >= sample:
            break
    if last == pd.Timestamp(0):
        raise RuntimeError("Could not read any day files — check DAY_DIR path.")
    return last

# ═══════════════ PASS 1: market/industry stats (ONCE per run, local) ═══════════
def _stats_worker(args):
    sym, industry, isin, asof = args
    df = read_day(isin)
    if df is None:
        return {"reason": "file_missing_or_unreadable"}
    if len(df) < CONFIG["MIN_BARS"]:
        return {"reason": "short_history"}
    gap = (asof - df.index[-1].normalize()).days
    if gap > CONFIG["MAX_STALE_DAYS"]:
        return {"reason": "stale_vs_snapshot"}
    r = wilder_rsi(df["close"], CONFIG["RSI_PERIOD"])
    if np.isnan(r):
        return {"reason": "rsi_nan"}
    e20 = ema(df["close"], CONFIG["EMA_FAST"])
    return {"industry": industry, "rsi": r,
            "above20": bool(df["close"].iloc[-1] > e20)}

def compute_market_stats(universe: dict, asof: pd.Timestamp):
    jobs = [(s, v["industry"], v["isin"], asof) for s, v in universe.items()]
    rows, reasons = [], {}
    with ProcessPoolExecutor(max_workers=min(os.cpu_count() or 4, 10)) as ex:
        for f in as_completed([ex.submit(_stats_worker, j) for j in jobs]):
            r = f.result()
            if "reason" in r:
                reasons[r["reason"]] = reasons.get(r["reason"], 0) + 1
            else:
                rows.append(r)
    if reasons:
        print("  exclusions:", dict(sorted(reasons.items())))
    mem = pd.DataFrame(rows)
    if mem.empty:
        raise RuntimeError("Zero usable stocks even vs snapshot date — "
                           "check DAY_DIR contents with a probe script.")
    mkt = {"market_rsi": float(mem["rsi"].median()),
           "market_breadth": float(mem["above20"].mean()),
           "members_used": len(mem)}
    g = mem.groupby("industry")
    st = g.agg(n=("industry", "count"), industry_rsi=("rsi", "median"),
               breadth20=("above20", "mean")).reset_index()
    st["rs_spread"] = st["industry_rsi"] - mkt["market_rsi"]
    st["quality"] = np.where(st["n"] >= CONFIG["FULL_MIN"], "FULL",
                    np.where(st["n"] >= CONFIG["THIN_MIN"], "THIN", "TOO_SMALL"))
    return mem, st, mkt

def sector_gate(industry: str, stats: pd.DataFrame, mkt: dict):
    """Explicit policy: no members or TOO_SMALL bucket -> pass-through with flag."""
    row = stats.loc[stats["industry"] == industry]
    if row.empty:
        return True, "sector: pass-through (no members found)"
    r = row.iloc[0]
    if r["quality"] == "TOO_SMALL":
        return True, f"sector: pass-through (thin bucket n={int(r['n'])})"
    ok = r["industry_rsi"] > mkt["market_rsi"] + CONFIG["SECTOR_RSI_SPREAD"]
    why = (f"sector {'PASS' if ok else 'FAIL'}: {industry} n={int(r['n'])} "
           f"[{r['quality']}] RSI {r['industry_rsi']:.1f} vs mkt "
           f"{mkt['market_rsi']:.1f} (spread {r['rs_spread']:+.1f}, "
           f"need >+{CONFIG['SECTOR_RSI_SPREAD']}); breadth {r['breadth20']:.0%} "
           f"vs mkt {mkt['market_breadth']:.0%}")
    return bool(ok), why

# ═══════════════ PASS 2: candidate validation (all metrics recorded) ═══════════
def validate_candidate(args):
    sym, industry, isin, asof, stats, mkt = args
    M = {"Symbol": sym, "Industry": industry, "Verdict": "", "FirstFail": "",
         "Close": np.nan, "EMA20": np.nan, "EMA50": np.nan, "RSI14": np.nan,
         "ATR14": np.nan, "RVOL5": np.nan, "BrokenRes": np.nan,
         "Event": "", "RetestLevel": np.nan, "Stop": np.nan,
         "RiskPct": np.nan, "Target": np.nan, "RR": np.nan,
         "SectorNote": "", "Flags": ""}
    df = read_day(isin)
    if df is None or len(df) < CONFIG["MIN_BARS"]:
        M["Verdict"], M["FirstFail"] = "SKIP", "data_insufficient"
        return M
    if (asof - df.index[-1].normalize()).days > CONFIG["MAX_STALE_DAYS"]:
        M["Verdict"], M["FirstFail"] = "SKIP", "data_stale"
        return M

    close, vol = df["close"], df["volume"]
    last = close.iloc[-1]
    e20 = ema(close, CONFIG["EMA_FAST"])
    e50 = ema(close, CONFIG["EMA_SLOW"])
    rsi = wilder_rsi(close, CONFIG["RSI_PERIOD"])
    atr = wilder_atr(df, CONFIG["ATR_PERIOD"])
    base_vol = vol.iloc[-(CONFIG["RVOL_BASE_WINDOW"] + CONFIG["RVOL_LOOKBACK"]):
                       -CONFIG["RVOL_LOOKBACK"]].mean()
    rvol = float(vol.iloc[-CONFIG["RVOL_LOOKBACK"]:].max() / base_vol) \
        if base_vol > 0 else np.nan
    M.update({"Close": round(float(last), 2), "EMA20": round(e20, 2),
              "EMA50": round(e50, 2), "RSI14": round(rsi, 1),
              "ATR14": round(atr, 2), "RVOL5": round(rvol, 2)})

    # ── gate 1: trend
    if not (last > e20 > e50):
        M["Verdict"], M["FirstFail"] = "REJECT", "trend_not_bullish"
        return M
    # ── gate 2: momentum band
    if not (CONFIG["RSI_MIN"] <= rsi <= CONFIG["RSI_MAX"]):
        M["Verdict"], M["FirstFail"] = "REJECT", "rsi_out_of_band"
        return M
    # ── gate 3: relative volume (5-bar event window)
    if not (rvol >= CONFIG["RVOL_MIN"]):
        M["Verdict"], M["FirstFail"] = "REJECT", "no_volume_surge"
        return M

    # ── gate 4: BREAKOUT EVENT within last N bars (level must still hold)
    C, H = close.values, df["high"].values
    n = len(df)
    brk_level = None
    idx0 = max(n - CONFIG["BRK_EVENT_WINDOW"], CONFIG["BRK_RES_LOOKBACK"])
    for k in range(idx0, n):
        res = H[k - CONFIG["BRK_RES_LOOKBACK"]:k].max()
        if C[k] > res + CONFIG["BRK_ATR_CUSHION"] * atr:
            brk_level = res
            break
    if brk_level is not None and last < brk_level:
        brk_level = None                                   # breakout already lost
    if brk_level is not None:
        M["Event"] = "breakout"
        M["BrokenRes"] = round(float(brk_level), 2)
    else:
        # ── gate 4b: RETEST of a confirmed weekly swing low (52w, ATR band)
        week = load_weekly(isin, df)
        sw_lo, _ = confirmed_weekly_swings(week, CONFIG["SWING_CONFIRM"])
        cutoff = week.index[-1] - pd.Timedelta(weeks=CONFIG["RETEST_LOOKBACK_WEEKS"])
        levels = [p for ts, p in sw_lo if ts >= cutoff and p < last]
        band = CONFIG["RETEST_ATR_MULT"] * atr
        near = [p for p in levels if (last - p) <= band]
        if near:
            lvl = max(near)                                # nearest support below
            M["Event"] = "retest"
            M["RetestLevel"] = round(float(lvl), 2)
        else:
            M["Verdict"], M["FirstFail"] = "REJECT", "no_breakout_or_retest"
            return M

    # ── gate 5: sector strength (LOCAL industry stats)
    ok, why = sector_gate(industry, stats, mkt)
    M["SectorNote"] = why
    if not ok:
        M["Verdict"], M["FirstFail"] = "REJECT", "sector_not_strong"
        return M

    # ── gate 6: structural risk / reward (stop at swing low, never min'd with ATR)
    L = df["low"].values
    conf = CONFIG["SWING_CONFIRM"]
    swing_low = None
    for k in range(n - conf - 1, n - conf - 60, -1):
        if k - conf < 0:
            break
        if L[k] <= L[k - conf:k + conf + 1].min():
            swing_low = L[k]
            break
    if swing_low is None or swing_low >= last:
        M["Verdict"], M["FirstFail"] = "NEAR MISS", "no_structural_stop"
        return M
    stop = float(swing_low)
    risk = last - stop
    risk_pct = risk / last * 100
    M["Stop"], M["RiskPct"] = round(stop, 2), round(risk_pct, 2)
    if risk > CONFIG["WIDE_STOP_ATR_MULT"] * atr:
        M["Flags"] = "wide_stop;"

    # target: weekly pivots + weekly swing highs above entry
    week = load_weekly(isin, df)
    wh, wl, wc = week["high"].iloc[-1], week["low"].iloc[-1], week["close"].iloc[-1]
    P = (wh + wl + wc) / 3.0
    cand = [2 * P - wl, P + (wh - wl), wh + 2 * (P - wl)]
    _, sw_hi = confirmed_weekly_swings(week, CONFIG["SWING_CONFIRM"])
    cand += [p for ts, p in sw_hi if p > last]
    cand = sorted(p for p in cand if p >= last * 1.005)
    target = cand[0] if cand else last + CONFIG["TARGET_RR_FALLBACK"] * risk
    rr = (target - last) / risk
    M["Target"], M["RR"] = round(float(target), 2), round(float(rr), 2)
    if rr < CONFIG["MIN_RR"]:
        M["Verdict"], M["FirstFail"] = "NEAR MISS", "rr_too_low"
        return M

    M["Verdict"] = "VALID"
    return M

# ═════════════════════════════════ MAIN ════════════════════════════════════════
def main():
    print("FILES:")
    print(f"  universe   : {CONFIG['UNIVERSE_CSV']}")
    print(f"  candidates : {CONFIG['CANDIDATES_CSV']}\n")

    uni = pd.read_csv(CONFIG["UNIVERSE_CSV"])
    uni.columns = [c.strip() for c in uni.columns]
    ucol = next(c for c in uni.columns if "isin" in c.lower())
    universe = {str(r["Symbol"]).strip().upper():
                {"industry": str(r["Industry"]).strip(),
                 "isin": str(r[ucol]).strip()}
                for _, r in uni.iterrows()}

    cands = pd.read_csv(CONFIG["CANDIDATES_CSV"])
    sym_col = "Symbol" if "Symbol" in cands.columns else cands.columns[0]
    symbols = [str(s).strip().upper() for s in cands[sym_col]]

    asof = detect_snapshot_date(universe)
    print(f"  data snapshot date: {asof.date()}  (validator evaluates as of this date)\n")

    print(f"PASS 1 | computing LOCAL industry RSI/breadth over {len(universe)} stocks ...")
    mem, stats, mkt = compute_market_stats(universe, asof)
    print(f"  market: RSI(med)={mkt['market_rsi']:.1f}  "
          f"breadth>{CONFIG['EMA_FAST']}EMA={mkt['market_breadth']:.0%}  "
          f"members={mkt['members_used']}")
    top = stats.sort_values("rs_spread", ascending=False).head(8)
    for _, r in top.iterrows():
        print(f"    {r['industry'][:34]:36s} n={int(r['n']):3d} [{r['quality']:9s}] "
              f"RSI {r['industry_rsi']:5.1f} ({r['rs_spread']:+.1f})  "
              f"breadth {r['breadth20']:.0%}")

    print(f"\nPASS 2 | validating {len(symbols)} candidates ...")
    jobs = []
    for s in symbols:
        info = universe.get(s)
        if info is None:
            jobs.append(None)
        else:
            jobs.append((s, info["industry"], info["isin"], asof, stats, mkt))

    results = []
    with ProcessPoolExecutor(max_workers=min(os.cpu_count() or 4, 10)) as ex:
        futs = {ex.submit(validate_candidate, j): j[0]
                for j in jobs if j is not None}
        for f in as_completed(futs):
            results.append(f.result())

    done = {r["Symbol"] for r in results}
    for s in symbols:
        if s not in done:
            results.append({"Symbol": s, "Industry": "", "Verdict": "SKIP",
                            "FirstFail": "no_universe_mapping"})

    res = pd.DataFrame(results)
    res.to_csv(CONFIG["OUT_METRICS"], index=False)

    print("\nFUNNEL:")
    for v, g in res.groupby("Verdict"):
        print(f"  {v:9s}: {len(g)}")
    print("\nREJECTION REASONS:")
    for r, g in res[res.FirstFail != ""].groupby("FirstFail"):
        print(f"  {r:22s}: {len(g)}")

    valid = res[res["Verdict"] == "VALID"].sort_values("RR", ascending=False)
    out = []
    for _, r in valid.iterrows():
        out.append({"symbol": r["Symbol"], "industry": r["Industry"],
                    "event": r["Event"], "entry": r["Close"],
                    "stop": r["Stop"], "target": r["Target"], "rr": r["RR"],
                    "risk_pct": r["RiskPct"], "rsi": r["RSI14"],
                    "rvol5": r["RVOL5"], "sector": r["SectorNote"],
                    "flags": r["Flags"]})
    with open(CONFIG["OUT_JSON"], "w") as fh:
        json.dump({"asof": str(asof.date()), "market": mkt,
                   "valid_setups": out,
                   "near_miss": res[res["Verdict"] == "NEAR MISS"]
                                [["Symbol", "FirstFail", "SectorNote"]]
                                .to_dict("records")},
                  fh, indent=2)

    print("\n" + "=" * 88)
    if len(valid):
        print(f"  VALID SETUPS ({len(valid)}) -> {CONFIG['OUT_JSON']}")
        for _, r in valid.iterrows():
            print(f"  {r['Symbol']:12s} {r['Event']:8s} E {r['Close']:>9} "
                  f"SL {r['Stop']:>9} TP {r['Target']:>9} RR 1:{r['RR']} "
                  f"risk {r['RiskPct']:.1f}% "
                  f"{('[' + r['Flags'] + ']') if r['Flags'] else ''}")
    else:
        print("  No valid setups — market state or gates. Metrics CSV has every value.")
    print(f"\n  Full metrics for ALL {len(res)} candidates -> {CONFIG['OUT_METRICS']}")
    print(f"  Config live: RSI {CONFIG['RSI_MIN']}-{CONFIG['RSI_MAX']}, "
          f"RVOL {CONFIG['RVOL_MIN']}x (max/{CONFIG['RVOL_LOOKBACK']} bars), "
          f"breakout {CONFIG['BRK_RES_LOOKBACK']}d+{CONFIG['BRK_ATR_CUSHION']}ATR "
          f"(window {CONFIG['BRK_EVENT_WINDOW']}), RR>={CONFIG['MIN_RR']}")

if __name__ == "__main__":
    main()