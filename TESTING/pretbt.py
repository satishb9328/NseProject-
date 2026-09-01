"""
NSE PRE-OPEN TBT ANALYZER  (09:00-09:08 indicative price)
Input : 141 csv files, each with 2 columns -> datetime, price
Folder: C:\\Users\\ADMIN\\Desktop\\Fari Preopen
Run   : python preopen_clean.py
Output: analysis_output\\daily_features.csv   (1 row per day)
        analysis_output\\quality_report.csv   (per-file status)
        analysis_output\\sample_paths.png     (first 12 auction paths)
Needs : pip install pandas numpy matplotlib
"""
import os, re, glob
import numpy as np
import pandas as pd

FOLDER = r"C:\Users\ADMIN\Desktop\Fair Preopen"
OUTDIR = os.path.join(FOLDER, "analysis_output")
WIN = (0, 500)                       # seconds after 09:00:00 (auction ~09:08)

# ---------------------------------------------------------------- read + parse
def read_file(path):
    """-> DataFrame(secs, px, date) or raises ValueError with reason."""
    df = pd.read_csv(path, header=None, encoding="utf-8-sig", sep=None,
                     engine="python", on_bad_lines="skip", dtype=str)
    if df.shape[1] < 2:
        raise ValueError(f"expected 2 columns, found {df.shape[1]}")
    df = df.iloc[:, :2]
    df.columns = ["dt", "px"]

    # drop header row if present (non-numeric price in row 0)
    if pd.to_numeric(df["px"].iloc[0], errors="coerce") is np.nan or \
       pd.to_numeric(df["px"].iloc[0], errors="coerce") != \
       pd.to_numeric(df["px"].iloc[0], errors="coerce"):
        df = df.iloc[1:]

    df["px"] = pd.to_numeric(df["px"].astype(str)
                             .str.replace(",", "", regex=False).str.strip(),
                             errors="coerce")

    # datetime -> parse
    s = df["dt"].astype(str).str.strip()
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if dt.notna().mean() < 0.9:
        for fmt in ("%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S",
                    "%d-%m-%Y %H:%M", "%H:%M:%S", "%H:%M"):
            d2 = pd.to_datetime(s, format=fmt, errors="coerce")
            if d2.notna().mean() >= 0.9:
                dt = d2
                break
    if dt.notna().mean() < 0.9:
        raise ValueError("datetime not parseable")

    secs = dt.dt.hour * 3600 + dt.dt.minute * 60 + dt.dt.second \
           + dt.dt.microsecond / 1e6 - 9 * 3600          # 0 = 09:00:00

    # date: from the file itself if datetime has a full date, else filename
    if dt.dt.year.notna().all() and dt.dt.year.nunique() == 1 \
       and dt.dt.year.iloc[0] > 2000:
        date = dt.dt.date.iloc[0].isoformat()
    else:
        date = date_from_filename(os.path.basename(path))

    w = pd.DataFrame({"secs": secs, "px": df["px"], "date": date}).dropna()
    w = w[(w.secs >= WIN[0]) & (w.secs <= WIN[1])]
    if len(w) < 10:
        raise ValueError(f"only {len(w)} ticks inside 09:00-09:08")
    return w

MONTHS = {m: i+1 for i, m in enumerate(
    ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"])}

def date_from_filename(name):
    m = re.search(r"(20\d{2})[-_. ]?(\d{2})[-_. ]?(\d{2})", name)
    if m: return f"{m[1]}-{m[2]}-{m[3]}"
    m = re.search(r"(\d{1,2})[-_. ]?([A-Za-z]{3,9})[-_. ]?(20\d{2})", name)
    if m and m[2][:3].lower() in MONTHS:
        return f"{m[3]}-{MONTHS[m[2][:3].lower()]:02d}-{int(m[1]):02d}"
    m = re.search(r"(\d{1,2})[-_. ](\d{1,2})[-_. ](20\d{2})", name)
    if m: return f"{m[3]}-{int(m[2]):02d}-{int(m[1]):02d}"
    return None

# ---------------------------------------------------------------- features
def day_features(w):
    t = w["secs"].to_numpy(float)
    p = w.sort_values("secs")["px"].to_numpy(float)
    o = np.argsort(t, kind="stable"); t = t[o]
    first, last = p[0], p[-1]
    hi, lo = p.max(), p.min(); rng = hi - lo
    span = max(t[-1] - t[0], 1e-9)
    moves = np.diff(p); clean = moves[moves != 0]

    sgn = np.sign(clean); streak = 0                    # final same-direction run
    if len(sgn):
        for x in sgn[::-1]:
            if x == sgn[-1]: streak += 1
            else: break

    grid = np.arange(int(np.ceil(t[0])), int(np.floor(t[-1])) + 1)   # 1-sec TWAP
    if len(grid):
        idx = np.clip(np.searchsorted(t, grid, side="right") - 1, 0, len(p)-1)
        twap = p[idx].mean()
    else:
        twap = last

    m_late = t >= t[-1] - 120                           # last 2 minutes
    m_early = t <= t[0] + 120                           # first 2 minutes
    return {
        "n_ticks":   len(p),
        "n_levels":  len(np.unique(p)),
        "px_first":  first, "px_last": last,
        "px_high":   hi,    "px_low":  lo,
        "range_pct":  100 * rng / last if last else np.nan,
        "drift_pct":  100 * (last - first) / first if first else np.nan,
        "close_loc":  (last - lo) / rng if rng else 0.5,
        "efficiency": abs(last - first) / np.abs(moves).sum()
                      if np.abs(moves).sum() else 0.0,
        "reversals":  int((np.diff(np.sign(clean)) != 0).sum())
                      if len(clean) > 1 else 0,
        "hi_time":    (t[np.argmax(p)] - t[0]) / span,
        "lo_time":    (t[np.argmin(p)] - t[0]) / span,
        "late_drift_pct":  100*(last - p[m_late][0])/p[m_late][0]
                           if m_late.any() and p[m_late][0] else 0.0,
        "early_drift_pct": 100*(p[m_early][-1] - first)/first
                           if m_early.any() and first else np.nan,
        "twap_dev_pct":    100*(last - twap)/twap if twap else np.nan,
        "final_streak":    int(streak * (sgn[-1] if len(sgn) else 0)),
        "max_jump_pct":    100*np.abs(moves).max()/last
                           if len(moves) and last else 0.0,
    }

# ---------------------------------------------------------------- main
def main():
    os.makedirs(OUTDIR, exist_ok=True)
    files = sorted(glob.glob(os.path.join(FOLDER, "*.csv")))
    print(f"Found {len(files)} csv files in {FOLDER}\n")
    if not files: return

    # preview first file so you can verify parsing before trusting the rest
    w0 = read_file(files[0])
    print(f"PREVIEW {os.path.basename(files[0])}:  {len(w0)} ticks, "
          f"date={w0.date.iloc[0]},  secs {w0.secs.min():.0f}..{w0.secs.max():.0f}, "
          f"px {w0.px.min():.2f}..{w0.px.max():.2f}\n")

    feats, reports, filemap = {}, [], {}
    for i, path in enumerate(files, 1):
        name = os.path.basename(path)
        try:
            w = read_file(path)
            f = day_features(w)
            key = w["date"].iloc[0] or f"UNKNOWN_{name}"
            feats[key], filemap[key] = f, path
            reports.append({"file": name, "date": key, "status": "OK",
                            "ticks": len(w), "px_last": f["px_last"]})
            print(f"[{i:3d}/{len(files)}] {name[:40]:42s} OK  ticks={len(w):4d} "
                  f"last={f['px_last']:.2f}")
        except Exception as e:
            reports.append({"file": name, "date": None, "status": "FAIL",
                            "ticks": 0, "px_last": np.nan})
            print(f"[{i:3d}/{len(files)}] {name[:40]:42s} FAIL  {e}")

    rep = pd.DataFrame(reports)
    print(f"\nQuality: OK={sum(rep.status=='OK')}  FAIL={sum(rep.status=='FAIL')}")

    # ---- daily table, sorted, prev-close chain -> gap
    fdf = pd.DataFrame.from_dict(feats, orient="index")
    fdf["_d"] = pd.to_datetime(fdf.index, errors="coerce")
    fdf = fdf.sort_values("_d")
    fdf["prev_close"] = fdf["px_last"].shift(1)
    gap_days = fdf["_d"].diff().dt.days
    broken = gap_days.isna() | (gap_days > 4)            # weekend/holiday safe
    fdf.loc[broken, "prev_close"] = np.nan
    fdf["chain_flag"] = np.where(broken, "BROKEN", "OK")
    fdf["gap_pct"] = 100*(fdf.px_last - fdf.prev_close)/fdf.prev_close

    fdf.drop(columns="_d").round(4).to_csv(os.path.join(OUTDIR, "daily_features.csv"))
    rep.to_csv(os.path.join(OUTDIR, "quality_report.csv"), index=False)

    # ---- summary
    cols = ["gap_pct","drift_pct","range_pct","close_loc","efficiency",
            "reversals","hi_time","lo_time","late_drift_pct",
            "early_drift_pct","twap_dev_pct","n_ticks"]
    print("\n=== FEATURE SUMMARY ===")
    print(fdf[cols].describe().T.round(3).to_string())
    med = fdf["n_ticks"].median()
    print(f"\nn_ticks median={med:.0f} -> "
          + ("1-sec SNAPSHOTS" if med > 380 else
             "true change-only TBT" if med > 15 else
             "VERY sparse - check the window filter"))

    show = ["drift_pct","close_loc","efficiency","late_drift_pct","gap_pct"]
    print("\n=== MOST DECISIVE auctions (efficiency high = one-way) ===")
    print(fdf.sort_values("efficiency", ascending=False).head(5)[show].round(3).to_string())
    print("\n=== CHOPPIEST auctions (efficiency low = two-sided fight) ===")
    print(fdf.sort_values("efficiency").head(5)[show].round(3).to_string())

    # ---- plot 12 paths
    try:
        import matplotlib; matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        sample = [d for d in fdf.index if d in filemap][:12]
        fig, axes = plt.subplots(3, 4, figsize=(16, 8), sharex=True)
        for ax, d in zip(axes.ravel(), sample):
            w = read_file(filemap[d])
            ax.plot(w.secs/60, w.px, lw=1)
            ax.set_title(str(d), fontsize=9); ax.tick_params(labelsize=7)
        fig.suptitle("Pre-open indicative price 09:00-09:08 (x = minutes after 9:00)")
        fig.tight_layout()
        fig.savefig(os.path.join(OUTDIR, "sample_paths.png"), dpi=120)
        plt.close(fig)
        print(f"\nPlot saved: {os.path.join(OUTDIR,'sample_paths.png')}")
    except Exception as e:
        print(f"(plots skipped: {e})")

    print(f"\nSaved:\n  {os.path.join(OUTDIR,'daily_features.csv')}"
          f"\n  {os.path.join(OUTDIR,'quality_report.csv')}")

if __name__ == "__main__":
    main()