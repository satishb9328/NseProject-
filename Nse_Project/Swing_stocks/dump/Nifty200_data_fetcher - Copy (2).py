import os
import time
import datetime
import json
import urllib.parse
import requests
import pandas as pd

# =====================================================================
# 1. CONFIGURATION
# =====================================================================
TOKEN_FILE_PATH = r"C:\Program Files\MyProject2\Brokers\upstox\upstox_login_details.json"
SAVE_PATH = r"D:\Api data\ML_HISTO_DATA\Upstox_History"

def get_access_token():
    """నేరుగా JSON ఫైల్ నుండి టోకెన్‌ను చదివే ఫంక్షన్"""
    try:
        with open(TOKEN_FILE_PATH, 'r') as file:
            data = json.load(file)
            token = data.get("access_token")
            if token:
                print(f"✅ Successfully loaded Upstox token from JSON!\n")
                return token
            else:
                print("❌ Token file exists, but 'access_token' is missing.")
                return None
    except FileNotFoundError:
        print(f"❌ File not found at: {TOKEN_FILE_PATH}")
        return None
    except Exception as e:
        print(f"❌ Error reading JSON file: {e}")
        return None

# =====================================================================
# 2. HELPER FUNCTIONS
# =====================================================================

def get_nifty200_symbols():
    """NSE అఫీషియల్ వెబ్‌సైట్ నుండి లేటెస్ట్ NIFTY 200 స్టాక్స్ లిస్ట్ తీసుకునే ఫంక్షన్."""
    print("📥 Fetching NIFTY 200 list from NSE...")
    try:
        url = "https://archives.nseindia.com/content/indices/ind_nifty200list.csv"
        df = pd.read_csv(url)
        symbols = df['Symbol'].tolist()
        print(f"✅ Successfully loaded {len(symbols)} NIFTY 200 stocks.\n")
        return symbols
    except Exception as e:
        print(f"❌ Error fetching NIFTY 200 list: {e}")
        return []

def get_upstox_instrument_dict():
    """Upstox మాస్టర్ ఫైల్ నుండి అన్ని స్టాక్స్ యొక్క Instrument Keys ని తీసుకురావడం."""
    print("📥 Downloading Upstox Master Instrument Keys (This takes a few seconds)...")
    try:
        url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
        df = pd.read_csv(url)
        instrument_dict = pd.Series(df.instrument_key.values, index=df.tradingsymbol).to_dict()
        print("✅ Upstox Keys loaded successfully!\n")
        return instrument_dict
    except Exception as e:
        print(f"❌ Error fetching instrument keys: {e}")
        return {}

def fetch_stock_5m_data(symbol, instrument_key, headers):
    """1-నిమిషం డేటా తెచ్చి, దాన్ని 5-నిమిషాల డేటాగా మార్చే ఫంక్షన్."""
    all_candles = []
    end_date = datetime.date.today()
    
    interval = "1minute" 
    safe_instrument_key = urllib.parse.quote(instrument_key)
    
    # 🔴 ఇక్కడ మార్పు: 100 రోజుల బదులు 30 రోజులు, 4 సార్లు బదులు 12 సార్లు లూప్ (1 సంవత్సరం కోసం)
    for _ in range(12): 
        start_date = end_date - datetime.timedelta(days=30)
        
        url = f"https://api.upstox.com/v2/historical-candle/{safe_instrument_key}/{interval}/{end_date.strftime('%Y-%m-%d')}/{start_date.strftime('%Y-%m-%d')}"
        
        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'candles' in data['data'] and data['data']['candles']:
                    all_candles.extend(data['data']['candles'])
            
            elif response.status_code == 401:
                print(f"   ❌ Token Expired or Invalid! దయచేసి కొత్త టోకెన్ జనరేట్ చేయండి.")
                return "TOKEN_EXPIRED" 
            
            elif response.status_code == 429:
                print(f"   ⚠️ Rate Limit Hit for {symbol}. Sleeping for 2 seconds...")
                time.sleep(2)
            
            else:
                print(f"   ❌ API Error {response.status_code}: {response.text}")
                
        except Exception as e:
            print(f"   ❌ Network error for {symbol}: {e}")
            
        # తర్వాతి లూప్ కోసం ఎండ్ డేట్ ని మార్చడం
        end_date = start_date - datetime.timedelta(days=1)
        
        # Upstox బ్లాక్ అవ్వకుండా ప్రతి రిక్వెస్ట్ మధ్య 0.5 సెకన్ల గ్యాప్
        time.sleep(0.5)

    if all_candles:
        columns = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI']
        df = pd.DataFrame(all_candles, columns=columns)
        
        # టైమ్ జోన్ మార్చడం మరియు సార్ట్ చేయడం
        df['Datetime'] = pd.to_datetime(df['Datetime']).dt.tz_localize(None)
        df = df.sort_values(by="Datetime").drop_duplicates(subset=['Datetime'])
        
        # 1-నిమిషం డేటాని 5-నిమిషాల డేటాగా కన్వర్ట్ చేయడం
        df.set_index('Datetime', inplace=True)
        df_5m = df.resample('5min').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum',
            'OI': 'last'
        }).dropna()
        
        df_5m.reset_index(inplace=True)
        return df_5m
    
    return None
# =====================================================================
# 3. MAIN EXECUTION ENGINE
# =====================================================================

def main():
    ACCESS_TOKEN = get_access_token()
    if not ACCESS_TOKEN:
        print("❌ Access token not found or invalid. Exiting gracefully...")
        return

    os.makedirs(SAVE_PATH, exist_ok=True)
    print(f"📁 Save Directory Ready: {SAVE_PATH}\n")
    
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }

    symbols = get_nifty200_symbols()
    upstox_keys = get_upstox_instrument_dict()
    
    if not symbols or not upstox_keys:
        print("❌ Initialization failed. Exiting...")
        return

    print("🚀 Starting 5-Minute Data Fetch for NIFTY 200...")
    print("=" * 60)
    
    successful = 0
    failed = 0

    for idx, symbol in enumerate(symbols, 1):
        instrument_key = upstox_keys.get(symbol)
        
        if not instrument_key:
            print(f"[{idx:3d}/{len(symbols)}] ⚠️ {symbol:12s} | No Upstox Key Found. Skipping.")
            failed += 1
            continue
            
        print(f"[{idx:3d}/{len(symbols)}] ⏳ {symbol:12s} | Fetching 1-Year 5m data...")
        
        df = fetch_stock_5m_data(symbol, instrument_key, headers)
        
        if isinstance(df, str) and df == "TOKEN_EXPIRED":
            print("\n🚨 STOPPING PROCESS: Please update your Upstox Token and run again.")
            break
            
        if df is not None and not df.empty:
            filename = os.path.join(SAVE_PATH, f"{symbol}_5m.parquet")
            df.to_parquet(filename, index=False)
            print(f"            ✅ Saved {len(df)} candles.")
            successful += 1
        else:
            print(f"            ❌ Failed to fetch data.")
            failed += 1
            
    print("=" * 60)
    print("🎉 DATA FETCH COMPLETED!")
    print(f"📊 Successfully Downloaded : {successful} stocks")
    print(f"⚠️ Failed / Skipped      : {failed} stocks")
    print(f"📁 All files saved in    : {SAVE_PATH}")
    print("=" * 60)

if __name__ == "__main__":
    main()