# datatesting.py — recon: what's actually inside the Day/Week folders?
import os, glob
import pandas as pd

FOLDERS = (("DAY",  r"D:\Api data\ML_HISTO_DATA\Upstox Data\Day Data"),
           ("WEEK", r"D:\Api data\ML_HISTO_DATA\Upstox Data\Weeks Data"))

for tag, D in FOLDERS:
    print("=" * 74)
    print(tag, "->", D)
    if not os.path.isdir(D):
        print("  !! FOLDER NOT FOUND — check spelling/spacing of the path")
        continue
    files = sorted(glob.glob(os.path.join(D, "**", "*.*"), recursive=True))
    print(f"  {len(files)} files found.")
    if not files:
        print("  !! folder is empty")
        continue
    for f in files[:8]:
        print("   ", os.path.relpath(f, D))
    try:
        first = files[0]
        if first.lower().endswith(".parquet"):
            df = pd.read_parquet(first)
        else:
            df = pd.read_csv(first)
        print("  first file:", os.path.basename(first))
        print("  columns:", list(df.columns))
        print(df.head(3).to_string())
    except Exception as e:
        print("  could not read first file:", e)