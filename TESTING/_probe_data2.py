import pandas as pd

f = r"D:\Nifty_ML_Data\nifty_features_2min_2026-11-15.parquet"
df = pd.read_parquet(f)
print("rows:", len(df))
df.index = pd.to_datetime(df.index, utc=True)
ist = df.index.tz_convert("Asia/Kolkata")
print("min", ist.min(), "max", ist.max())
print("all timestamps:")
for t in ist:
    print(" ", t)
