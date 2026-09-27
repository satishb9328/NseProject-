import json
import gzip
import time
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION & PATHS ---
TOKEN_FILE = Path(r"C:\Users\ADMIN\Desktop\MyProject2\Nse_Project\Brokers\upstox\upstox_v3_token.json")
NIFTY_CSV_FILE = Path(r"C:\Users\ADMIN\Desktop\MyProject2\Nse_Project\ind_nifty500list.csv")

DAY_DATA_DIR = Path(r"D:\Api data\ML_HISTO_DATA\Upstox Data\Day Data")
WEEK_DATA_DIR = Path(r"D:\Api data\ML_HISTO_DATA\Upstox Data\Weeks Data")

DAY_DATA_DIR.mkdir(parents=True, exist_ok=True)
WEEK_DATA_DIR.mkdir(parents=True, exist_ok=True)

# --- CONSTANTS ---
CURRENT_DATE_STR = datetime.today().strftime('%Y-%m-%d')
DAILY_DATE_CHUNKS = [
    ("2000-01-01", "2009-12-31"),
    ("2010-01-01", "2019-12-31"),
    ("2020-01-01", CURRENT_DATE_STR)
]
COLUMNS = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest']
MAX_WORKERS = 5 # Adjust based on your Upstox API rate limits

def load_access_token(token_file):
    try:
        with token_file.open(encoding='utf-8') as file:
            access_token = json.load(file).get('access_token')
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Unable to load Upstox token from {token_file}") from error
    if not access_token:
        raise RuntimeError(f"Missing 'access_token' in {token_file}")
    return access_token

# Initialize a global session for connection pooling
session = requests.Session()
session.headers.update({
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Authorization': f'Bearer {load_access_token(TOKEN_FILE)}'
})

def get_nifty_500_instrument_keys(csv_path):
    print("Downloading and mapping Upstox instruments...")
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
    
    response = requests.get(url)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to download instruments. Status: {response.status_code}")
        
    decompressed_data = gzip.decompress(response.content)
    df_upstox = pd.DataFrame(json.loads(decompressed_data.decode('utf-8')))
    df_eq = df_upstox[df_upstox['segment'] == 'NSE_EQ']
    
    df_csv = pd.read_csv(csv_path)
    if 'Symbol' not in df_csv.columns:
        raise KeyError(f"CSV file must contain a 'Symbol' column.")
        
    nifty_symbols = set(df_csv['Symbol'].dropna().tolist())
    df_filtered = df_eq[df_eq['trading_symbol'].isin(nifty_symbols)]
    
    instrument_keys = df_filtered['instrument_key'].tolist()
    print(f"Total mapped Nifty 500 stocks: {len(instrument_keys)}")
    return instrument_keys

def fetch_upstox_data_with_retry(instrument_key, unit, interval, from_date, to_date, retries=3):
    """Fetches data using the global session, with exponential backoff for rate limits (429)."""
    url = f"https://api.upstox.com/v3/historical-candle/{instrument_key}/{unit}/{interval}/{to_date}/{from_date}"
    
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success' and data.get('data'):
                    return data['data']['candles']
                return []
            
            if response.status_code == 429:
                wait_time = (attempt + 1) * 2 # Exponential backoff: 2s, 4s, 6s
                time.sleep(wait_time)
                continue
                
            print(f"  [API Error] {unit} for {instrument_key}: {response.status_code} - {response.text}")
            break # Break on non-retriable errors (400, 401, 404)
            
        except requests.RequestException as e:
            if attempt == retries - 1:
                print(f"  [Network Exception] {instrument_key}: {e}")
            time.sleep(1)
            
    return []

def process_instrument(instrument_key):
    safe_filename = instrument_key.replace('|', '_') + '.parquet'
    
    # 1. FETCH WEEKLY DATA
    weekly_candles = fetch_upstox_data_with_retry(instrument_key, 'weeks', '1', "2000-01-01", CURRENT_DATE_STR)
    
    if weekly_candles:
        df_week = pd.DataFrame(weekly_candles, columns=COLUMNS)
        df_week['Timestamp'] = pd.to_datetime(df_week['Timestamp']).dt.tz_localize(None)
        df_week.sort_values(by='Timestamp', inplace=True)
        df_week.drop_duplicates(subset=['Timestamp'], inplace=True)
        
        week_filepath = WEEK_DATA_DIR / safe_filename
        df_week.to_parquet(week_filepath, index=False, engine='pyarrow')
    
    # 2. FETCH DAILY DATA
    all_daily_candles = []
    for from_d, to_d in DAILY_DATE_CHUNKS:
        candles = fetch_upstox_data_with_retry(instrument_key, 'days', '1', from_d, to_d)
        if candles:
            all_daily_candles.extend(candles)

    if all_daily_candles:
        df_day = pd.DataFrame(all_daily_candles, columns=COLUMNS)
        df_day['Timestamp'] = pd.to_datetime(df_day['Timestamp']).dt.tz_localize(None)
        df_day.sort_values(by='Timestamp', inplace=True)
        df_day.drop_duplicates(subset=['Timestamp'], inplace=True)
        
        day_filepath = DAY_DATA_DIR / safe_filename
        df_day.to_parquet(day_filepath, index=False, engine='pyarrow')
        
    return instrument_key

def main():
    try:
        nifty_instruments = get_nifty_500_instrument_keys(NIFTY_CSV_FILE)
    except Exception as e:
        print(f"Error initializing: {e}")
        return

    print(f"Starting concurrent downloads with {MAX_WORKERS} workers...")
    
    # Execute API calls concurrently
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_instrument, inst): inst for inst in nifty_instruments}
        
        completed = 0
        for future in as_completed(futures):
            completed += 1
            inst = futures[future]
            try:
                future.result()
                print(f"[{completed}/{len(nifty_instruments)}] Successfully processed: {inst}")
            except Exception as exc:
                print(f"[{completed}/{len(nifty_instruments)}] Error processing {inst}: {exc}")

if __name__ == "__main__":
    main()