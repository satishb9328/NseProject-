import os
import time
import json
import gzip
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

# --- CONFIGURATION & PATHS ---
TOKEN_FILE = Path(r"C:\Users\ADMIN\Desktop\MyProject2\Nse_Project\Brokers\upstox\upstox_v3_token.json")
NIFTY_CSV_FILE = Path(r"C:\Users\ADMIN\Desktop\MyProject2\Nse_Project\ind_nifty500list.csv")

DAY_DATA_DIR = Path(r"D:\Api data\ML_HISTO_DATA\Upstox Data\Day Data")
WEEK_DATA_DIR = Path(r"D:\Api data\ML_HISTO_DATA\Upstox Data\Weeks Data")

# Create directories if they don't exist
DAY_DATA_DIR.mkdir(parents=True, exist_ok=True)
WEEK_DATA_DIR.mkdir(parents=True, exist_ok=True)

def load_access_token(token_file):
    try:
        with token_file.open(encoding='utf-8') as file:
            access_token = json.load(file).get('access_token')
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Unable to load Upstox token from {token_file}") from error

    if not access_token:
        raise RuntimeError(f"Missing 'access_token' in {token_file}")

    return access_token

ACCESS_TOKEN = load_access_token(TOKEN_FILE)
HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Authorization': f'Bearer {ACCESS_TOKEN}'
}

def get_nifty_500_instrument_keys(csv_path):
    print("Upstox నుండి ఇన్‌స్ట్రుమెంట్స్ జాబితాను డౌన్‌లోడ్ చేసి మ్యాప్ చేస్తోంది...")
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
    response = requests.get(url)
    
    if response.status_code != 200:
        raise RuntimeError(f"Failed to download Upstox instruments list. Status: {response.status_code}")
        
    decompressed_data = gzip.decompress(response.content)
    df_upstox = pd.DataFrame(json.loads(decompressed_data.decode('utf-8')))
    df_eq = df_upstox[df_upstox['segment'] == 'NSE_EQ']
    
    # Read the user's Nifty 500 CSV file
    df_csv = pd.read_csv(csv_path)
    
    # Assuming the column containing ticker symbols is named 'Symbol'
    if 'Symbol' not in df_csv.columns:
        raise KeyError(f"CSV file must contain a 'Symbol' column. Found columns: {df_csv.columns.tolist()}")
        
    nifty_symbols = df_csv['Symbol'].dropna().tolist()
    
    # Filter Upstox keys matching Nifty symbols
    df_filtered = df_eq[df_eq['trading_symbol'].isin(nifty_symbols)]
    instrument_keys = df_filtered['instrument_key'].tolist()
    
    print(f"మొత్తం మ్యాప్ చేయబడిన Nifty 500 స్టాక్స్ సంఖ్య: {len(instrument_keys)}")
    return instrument_keys

# Current Date for the final chunk
CURRENT_DATE_STR = datetime.today().strftime('%Y-%m-%d')

# The 10-year chunks for Daily data to avoid hitting the Upstox API limit
DAILY_DATE_CHUNKS = [
    ("2000-01-01", "2009-12-31"),
    ("2010-01-01", "2019-12-31"),
    ("2020-01-01", CURRENT_DATE_STR)
]

COLUMNS = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open Interest']

def fetch_upstox_data(instrument_key, unit, interval, from_date, to_date):
    """Makes the API call to Upstox and returns the candle list (to_date first, then from_date)."""
    url = f"https://api.upstox.com/v3/historical-candle/{instrument_key}/{unit}/{interval}/{to_date}/{from_date}"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'success' and data.get('data'):
                return data['data']['candles']
        else:
            print(f"  [API Error] {unit} for {instrument_key}: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"  [Exception] {instrument_key}: {e}")
    
    return []

def process_instrument(instrument_key):
    print(f"Processing: {instrument_key}...")
    safe_filename = instrument_key.replace('|', '_') + '.parquet'
    
    # -----------------------------------------
    # 1. FETCH WEEKLY DATA (No Limit)
    # -----------------------------------------
    weekly_candles = fetch_upstox_data(instrument_key, 'weeks', '1', "2000-01-01", CURRENT_DATE_STR)
    
    if weekly_candles:
        df_week = pd.DataFrame(weekly_candles, columns=COLUMNS)
        df_week['Timestamp'] = pd.to_datetime(df_week['Timestamp']).dt.tz_localize(None)
        df_week.sort_values(by='Timestamp', inplace=True)
        df_week.drop_duplicates(subset=['Timestamp'], inplace=True)
        
        week_filepath = WEEK_DATA_DIR / safe_filename
        df_week.to_parquet(week_filepath, index=False, engine='pyarrow')
        print(f"  -> Saved Weekly data to {week_filepath.name}")
    else:
        print(f"  -> No Weekly data found for {instrument_key}")
        
    time.sleep(0.3) 
    
    # -----------------------------------------
    # 2. FETCH DAILY DATA (10-Year Limit)
    # -----------------------------------------
    all_daily_candles = []
    
    for from_d, to_d in DAILY_DATE_CHUNKS:
        candles = fetch_upstox_data(instrument_key, 'days', '1', from_d, to_d)
        if candles:
            all_daily_candles.extend(candles)
        time.sleep(0.3) 

    if all_daily_candles:
        df_day = pd.DataFrame(all_daily_candles, columns=COLUMNS)
        df_day['Timestamp'] = pd.to_datetime(df_day['Timestamp']).dt.tz_localize(None)
        
        df_day.sort_values(by='Timestamp', inplace=True)
        df_day.drop_duplicates(subset=['Timestamp'], inplace=True)
        
        day_filepath = DAY_DATA_DIR / safe_filename
        df_day.to_parquet(day_filepath, index=False, engine='pyarrow')
        print(f"  -> Saved Daily data to {day_filepath.name}")
    else:
        print(f"  -> No Daily data found for {instrument_key}")
        
    print("-" * 40)

def main():
    try:
        nifty_instruments = get_nifty_500_instrument_keys(NIFTY_CSV_FILE)
    except Exception as e:
        print(f"Error loading instrument keys from CSV: {e}")
        return

    for idx, instrument in enumerate(nifty_instruments, 1):
        print(f"[{idx}/{len(nifty_instruments)}] ", end="")
        process_instrument(instrument)
        time.sleep(0.5) 

if __name__ == "__main__":
    main()