import pandas as pd, glob, os

D = r"D:\Api data\ML_HISTO_DATA\Upstox Data\Day Data"
files = sorted(glob.glob(os.path.join(D, "*.parquet")))
print(f"{len(files)} parquet files found\n")

stats, bad = [], []
for f in files[:30]:                       # remove [:30] to audit ALL files
    name = os.path.basename(f)
    try:
        df = pd.read_parquet(f)
        if pd.api.types.is_datetime64_any_dtype(df.index):
            idx = df.index
        else:
            dcol = next((c for c in df.columns
                         if 'date' in str(c).lower() or 'time' in str(c).lower()), None)
            if dcol is None:
                bad.append((name, "no datetime index and no date column")); continue
            idx = pd.DatetimeIndex(pd.to_datetime(df[dcol], errors="coerce"))
        if idx.tz is not None:
            idx = idx.tz_localize(None)
        idx = idx.dropna()
        if len(idx) == 0:
            bad.append((name, "all dates unparseable")); continue
        days = idx.normalize().nunique()
        stats.append(days)
        verdict = ("FULL YEAR   " if days >= 252 else
                   "PARTIAL     " if days >= 105 else
                   "INSUFFICIENT")
        print(f"{name:30s} days={days:5d}  {idx.min().date()} -> {idx.max().date()}  {verdict}")
    except Exception as e:
        bad.append((name, str(e)))

print("\nSUMMARY (audited files)")
if stats:
    d = pd.Series(stats)
    print(f"  median days={d.median():.0f}  min={d.min()}  max={d.max()}")
    print(f"  full-year={sum(1 for x in stats if x >= 252)}  "
          f"partial={sum(1 for x in stats if 105 <= x < 252)}  "
          f"insufficient(<105d)={sum(1 for x in stats if x < 105)}")
if bad:
    print("\nPROBLEM FILES:")
    for nm, why in bad:
        print(f"  {nm}: {why}")