import requests
import datetime
import os
import time
import zipfile
import glob
from io import BytesIO

import pandas as pd


# ==========================================
# CONFIGURATION & PATHS (DYNAMIC & PORTABLE)
# ==========================================
# Automatically finds the folder where this script is running
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Creates a clean 'nse_futures_data' directory inside the script folder
OUTPUT_DIR = os.path.join(BASE_DIR, "nse_futures_data")
HISTORY_FILE = os.path.join(OUTPUT_DIR, "nifty_future_history.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.nseindia.com/',
}

END_DATE = datetime.date.today()
START_DATE = END_DATE - datetime.timedelta(days=30)  # Safe 30-day sliding window


# ==========================================
# STAGE 1: DOWNLOADER ENGINE
# ==========================================
def is_working_day(d: datetime.date):
    return d.weekday() < 5


def download_nse_bhavcopy(date: datetime.date, session: requests.Session):
    ymd = date.strftime('%Y%m%d')
    zip_name = f'BhavCopy_NSE_FO_0_0_0_{ymd}_F_0000.csv.zip'
    url = f'https://nsearchives.nseindia.com/content/fo/{zip_name}'

    csv_name = f'fo{ymd}bhav.csv'
    local_path = os.path.join(OUTPUT_DIR, csv_name)
    if os.path.exists(local_path):
        return True

    try:
        resp = session.get(url, timeout=30)
        if resp.status_code != 200:
            return False

        z = zipfile.ZipFile(BytesIO(resp.content))
        candidates = [name for name in z.namelist() if name.lower().endswith('.csv')]
        if not candidates:
            return False

        inner_name = candidates[0]
        data = z.read(inner_name)
        with open(local_path, 'wb') as f:
            f.write(data)

        print(f'   -> Downloaded: {csv_name}')
        return True
    except Exception as e:
        print(f'   -> Error downloading {date}: {e}')
        return False


def run_downloader_pipeline():
    print("--- STARTING STAGE 1: NSE BHAVCOPY DOWNLOADER ---")
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        session.get('https://www.nseindia.com', timeout=20)
    except Exception as e:
        print('Warning: Session priming failed', e)

    cur = START_DATE
    while cur <= END_DATE:
        if not is_working_day(cur):
            cur += datetime.timedelta(days=1)
            continue

        download_nse_bhavcopy(cur, session)
        cur += datetime.timedelta(days=1)
    print("STAGE 1 COMPLETE: All missing files checked.")


# ==========================================
# STAGE 2: INCREMENTAL HISTORY GENERATOR
# ==========================================
def run_history_generator_pipeline():
    print("\n--- STARTING STAGE 2: INCREMENTAL HISTORY GENERATOR ---")

    last_date = None
    existing = pd.DataFrame()
    if os.path.exists(HISTORY_FILE):
        try:
            existing = pd.read_csv(HISTORY_FILE, parse_dates=['TradDt'])
            if not existing.empty:
                last_date = existing['TradDt'].max()
                print(f"Existing history found. Last recorded trade date: {last_date.strftime('%Y-%m-%d')}")
        except Exception as e:
            print('Could not read existing history file:', e)

    all_files = sorted(glob.glob(os.path.join(OUTPUT_DIR, 'fo*bhav.csv')))
    records = []

    for path in all_files:
        try:
            df = pd.read_csv(path)
        except Exception:
            continue

        # Standardize modern NSE column variations safely
        df.columns = [c.strip() for c in df.columns]
        rename_dict = {
            'TRAD_DT': 'TradDt',
            'XPIRY_DT': 'XpryDt',
            'FININSTRM_TP': 'FinInstrmTp',
            'TSYMB': 'TckrSymb',
        }
        df = df.rename(columns={k: v for k, v in rename_dict.items() if k in df.columns})

        if 'FinInstrmTp' not in df.columns or 'TckrSymb' not in df.columns:
            continue

        # Filter out NIFTY Index Futures (IDF)
        df = df[(df['FinInstrmTp'] == 'IDF') & (df['TckrSymb'] == 'NIFTY')].copy()
        if df.empty:
            continue

        df['TradDt'] = pd.to_datetime(df['TradDt'], errors='coerce')
        df['XpryDt'] = pd.to_datetime(df['XpryDt'], errors='coerce')
        df = df.dropna(subset=['TradDt', 'XpryDt'])

        # Incremental filter
        if last_date is not None:
            df = df[df['TradDt'] > last_date]
        if df.empty:
            continue

        # Capture current, next, far expiries
        for trade_date, group in df.groupby('TradDt'):
            target_expiries = sorted(group['XpryDt'].unique())[:3]
            group3 = group[group['XpryDt'].isin(target_expiries)]
            if group3.empty:
                continue
            records.append(group3)

    if not records:
        print('No new NIFTY futures rows to append.')
    else:
        out = pd.concat(records, ignore_index=True)
        if last_date is not None and not existing.empty:
            out = pd.concat([existing, out], ignore_index=True)

        out.to_csv(HISTORY_FILE, index=False)
        print(f"STAGE 2 COMPLETE: Successfully synced! Total rows in master file: {len(out)}")


# ==========================================
# MAIN EXECUTION MASTER CONTROLLER
# ==========================================
if __name__ == '__main__':
    # Step 1: Run Downloader first
    run_downloader_pipeline()

    # Polite breathing room for file system I/O
    time.sleep(2)

    # Step 2: Run History Generator next using the downloaded files
    run_history_generator_pipeline()
