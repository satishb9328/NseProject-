import pandas as pd
f = pd.read_csv(r"C:\Users\ADMIN\Desktop\Fair Preopen\analysis_output\daily_features.csv",
                index_col=0, parse_dates=True)

# 1) Which months do your files ACTUALLY come from?
print(f.index.to_period("M").value_counts().sort_index())

# 2) The suspicious days - price levels exposed
print(f.loc[["2026-09-02", "2026-09-03"],
      ["px_first","px_last","px_high","px_low","drift_pct","gap_pct"]].round(2))

# 3) Jitter ratio: distinct price levels vs total ticks
print((f["n_levels"] / f["n_ticks"]).describe().round(3))

# 4) Verify known days against your pasted table
#    Aug 18 2026 should be: first 24287.65, high 24812.85, low 24217.10, last 24219.45
print(f[["px_first","px_high","px_low","px_last"]].tail(20).round(2))