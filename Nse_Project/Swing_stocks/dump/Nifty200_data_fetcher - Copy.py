import os
import time
import datetime
import json
import importlib.util
from pathlib import Path
import requests
import pandas as pd

# =====================================================================
# 1. CONFIGURATION (మీ టోకెన్ ఇక్కడ ఇవ్వండి)
# =====================================================================
TOKEN_FILE_PATH = r"C:\Program Files\MyProject2\Brokers\upstox\upstox_login_details.json"
SAVE_PATH = r"D:\Api data\ML_HISTO_DATA\Upstox_History"


def get_token_from_manager():
    """Try loading get_access_token() from upstox_token_manager.py if available."""
    candidate_paths = [
        Path(__file__).with_name("upstox_token_manager.py"),
        Path(__file__).resolve().parents[2] / "Brokers" / "upstox" / "upstox_token_manager.py",
    ]

    for module_path in candidate_paths:
        if not module_path.exists():
            continue

        try:
            spec = importlib.util.spec_from_file_location("upstox_token_manager", str(module_path))
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                if hasattr(module, "get_access_token"):
                    token = module.get_access_token()
                    if token:
                        print(f"✅ Token loaded from: {module_path}\n")
                        return token
        except Exception as e:
            print(f"⚠️ Could not load token from {module_path}: {e}")

    return None

def get_token_directly():
    """మీరు ఇచ్చిన పాత్ నుండి నేరుగా టోకెన్ ని చదివే ఫంక్షన్"""
    try:
        with open(TOKEN_FILE_PATH, 'r') as file:
            data = json.load(file)
            token = data.get("access_token")
            if token:
                print("✅ Successfully loaded Upstox token from JSON file!\n")
                return token
            else:
                print("❌ Token file exists, but 'access_token' is missing inside it.")
                return None
    except FileNotFoundError:
        print(f"❌ File not found at: {TOKEN_FILE_PATH}")
        return None
    except Exception as e:
        print(f"❌ Error reading JSON file: {e}")
        return None

# టోకెన్ ని లోడ్ చేయడం
ACCESS_TOKEN = get_token_from_manager() or get_token_directly()

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
    """Upstox మాస్టర్ ఫైల్ నుండి అన్ని స్టాక్స్ యొక్క Instrument Keys ని ఒకేసారి తెచ్చుకోవడం."""
    print("📥 Downloading Upstox Master Instrument Keys (This takes a few seconds)...")
    try:
        url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
        df = pd.read_csv(url)
        
        # 'tradingsymbol' ని కీ (Key) గా, 'instrument_key' ని వాల్యూ (Value) గా మారుస్తున్నాం
        instrument_dict = pd.Series(df.instrument_key.values, index=df.tradingsymbol).to_dict()
        print("✅ Upstox Keys loaded successfully!\n")
        return instrument_dict
    except Exception as e:
        print(f"❌ Error fetching instrument keys: {e}")
        return {}

def fetch_stock_5m_data(symbol, instrument_key, headers):
    """ఒక స్టాక్ కి సంబంధించిన 1 సంవత్సరం 5-నిమిషాల డేటాని ముక్కలు ముక్కలుగా తెచ్చే ఫంక్షన్."""
    all_candles = []
    end_date = datetime.date.today()
    interval = "5minute"
    
    # 1 సంవత్సరం (365 రోజులు) డేటా కావాలి కాబట్టి 100-రోజుల చొప్పున 4 సార్లు అడుగుతాం
    for _ in range(4):
        start_date = end_date - datetime.timedelta(days=100)
        
        url = f"https://api.upstox.com/v2/historical-candle/{instrument_key}/{interval}/{end_date.strftime('%Y-%m-%d')}/{start_date.strftime('%Y-%m-%d')}"
        
        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'candles' in data['data'] and data['data']['candles']:
                    all_candles.extend(data['data']['candles'])
            elif response.status_code == 429:
                print(f"   ⚠️ Rate Limit Hit for {symbol}. Sleeping for 2 seconds...")
                time.sleep(2)
        except Exception as e:
            print(f"   ❌ Network error for {symbol}: {e}")
            
        # తర్వాతి లూప్ కోసం ఎండ్ డేట్ ని మార్చడం
        end_date = start_date - datetime.timedelta(days=1)
        
        # Upstox API బ్లాక్ అవ్వకుండా ప్రతి రిక్వెస్ట్ కి మధ్య 0.5 సెకన్లు బ్రేక్
        time.sleep(0.5)

    if all_candles:
        # డేటాని క్లీన్ చేసి DataFrame గా మార్చడం
        columns = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI']
        df = pd.DataFrame(all_candles, columns=columns)
        
        # టైమ్‌స్టాంప్ ని మార్చడం
        df['Datetime'] = pd.to_datetime(df['Datetime']).dt.tz_localize(None)
        
        # పాత తేదీ నుండి కొత్త తేదీకి ఆర్డర్ చేసి డూప్లికేట్స్ తీసేయడం
        df = df.sort_values(by="Datetime").drop_duplicates(subset=['Datetime'])
        return df
    return None

# =====================================================================
# 3. MAIN EXECUTION ENGINE
# =====================================================================

def main():
    if ACCESS_TOKEN is None:
        print("❌ Access token not found. Exiting gracefully...")
        return

    # మీరు ఇచ్చిన ఫోల్డర్ పాత్ సిస్టమ్ లో లేకపోతే దాన్ని క్రియేట్ చేస్తుంది
    os.makedirs(SAVE_PATH, exist_ok=True)
    print(f"📁 Save Directory Ready: {SAVE_PATH}\n")
    
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }

    # 1. NIFTY 200 లిస్ట్ మరియు Upstox కీస్ తెచ్చుకోవడం
    symbols = get_nifty200_symbols()
    upstox_keys = get_upstox_instrument_dict()
    
    if not symbols or not upstox_keys:
        print("❌ Initialization failed. Exiting...")
        return

    # 2. ప్రతి స్టాక్ కి లూప్ రన్ చేయడం
    print("🚀 Starting 5-Minute Data Fetch for NIFTY 200...")
    print("=" * 60)
    
    successful = 0
    failed = 0

    for idx, symbol in enumerate(symbols, 1):
        # NIFTY 200 సింబల్ కి Upstox కీ ఉందో లేదో చెక్ చేయడం
        instrument_key = upstox_keys.get(symbol)
        
        if not instrument_key:
            print(f"[{idx:3d}/{len(symbols)}] ⚠️ {symbol:12s} | No Upstox Key Found. Skipping.")
            failed += 1
            continue
            
        print(f"[{idx:3d}/{len(symbols)}] ⏳ {symbol:12s} | Fetching 1-Year 5m data...")
        
        # డేటా డౌన్‌లోడ్ చేయడం
        df = fetch_stock_5m_data(symbol, instrument_key, headers)
        
        if df is not None and not df.empty:
            # ఫైల్ ని Parquet ఫార్మాట్ లో సేవ్ చేయడం
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