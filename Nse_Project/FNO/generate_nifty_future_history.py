import os
import glob
import pandas as pd
from datetime import datetime

SOURCE_DIR = r'D:\FNO DATA\Nifty Future'
OUTPUT_FILE = r'D:\FNO DATA\FUTUERE DATA\nifty_future_history.csv'

# Step 1: Find last date in existing output file (if any)
last_date = None
if os.path.exists(OUTPUT_FILE):
    try:
        existing = pd.read_csv(OUTPUT_FILE, parse_dates=['TradDt'])
        if not existing.empty:
            last_date = existing['TradDt'].max()
    except Exception as e:
        print('Could not read existing output file:', e)

all_files = sorted(glob.glob(os.path.join(SOURCE_DIR, 'fo*bhav.csv')))
records = []
for path in all_files:
    try:
        df = pd.read_csv(path)
    except Exception as e:
        print('skip', path, e)
        continue
    if 'FinInstrmTp' not in df.columns or 'TckrSymb' not in df.columns:
        continue

    df = df[(df['FinInstrmTp'] == 'IDF') & (df['TckrSymb'] == 'NIFTY')].copy()
    if df.empty:
        continue

    # Build date fields
    df['TradDt'] = pd.to_datetime(df['TradDt'], errors='coerce')
    df['XpryDt'] = pd.to_datetime(df['XpryDt'], errors='coerce')
    df = df.dropna(subset=['TradDt', 'XpryDt'])

    # Only keep records after last_date (if any)
    if last_date is not None:
        df = df[df['TradDt'] > last_date]
    if df.empty:
        continue

    # For each trade date include current/next/next2 expiry only
    for trade_date, group in df.groupby('TradDt'):
        target_expiries = sorted(group['XpryDt'].unique())[:3]
        group3 = group[group['XpryDt'].isin(target_expiries)]
        if group3.empty:
            continue
        records.append(group3)

if not records:
    print('No new records found')
else:
    out = pd.concat(records, ignore_index=True)
    # If file exists and has data, append to it
    if last_date is not None and not existing.empty:
        out = pd.concat([existing, out], ignore_index=True)
    out.to_csv(OUTPUT_FILE, index=False)
    print('Saved', OUTPUT_FILE, 'rows', len(out))
