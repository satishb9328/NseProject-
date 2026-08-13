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
TOKEN_FILE_PATH = r"C:\Program Files\MyProject2\Nse_Project\Brokers\upstox\upstox\upstox_token.json"
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

def fetch_stock_5m_data(symbol, instrument_key, headers, existing_last_date=None):
    """అవసరమైన డేటాని మాత్రమే (Incremental) ఫెచ్ చేసే ఫంక్షన్."""
    all_candles = []
    end_date = datetime.date.today()
    
    interval = "1minute" 
    safe_instrument_key = urllib.parse.quote(instrument_key)
    
    # 🔴 డేటా ఎక్కడి నుండి ఫెచ్ చేయాలో నిర్ణయించడం
    if existing_last_date:
        # పాత డేటా ఉంటే, ఆ లాస్ట్ డేట్ నుండి ఈరోజు వరకు...
        final_start_date = existing_last_date.date()
    else:
        # పాత డేటా లేకపోతే, 1 సంవత్సరం (360 రోజులు) వెనక్కి వెళ్తాం
        final_start_date = end_date - datetime.timedelta(days=360)

    # డేటా ఇప్పటికే అప్‌డేట్ అయి ఉంటే తిరిగి పంపేయడం
    if end_date <= final_start_date:
        return pd.DataFrame() # ఎంప్టీ DataFrame పంపిస్తే అప్‌డేట్ అవసరం లేదని అర్థం

    current_end = end_date
    
    # గ్యాప్ కవర్ అయ్యే వరకు 30 రోజుల చొప్పున లూప్ రన్ అవుతుంది
    while current_end > final_start_date:
        current_start = max(current_end - datetime.timedelta(days=30), final_start_date)
        
        url = f"https://api.upstox.com/v2/historical-candle/{safe_instrument_key}/{interval}/{current_end.strftime('%Y-%m-%d')}/{current_start.strftime('%Y-%m-%d')}"
        
        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'candles' in data['data'] and data['data']['candles']:
                    all_candles.extend(data['data']['candles'])
            
            elif response.status_code == 401:
                print(f"   ❌ Token Expired or Invalid! దయచేసి కొత్త టోకెన్ జనరేట్ చేయండి.")
                raise RuntimeError("TOKEN_EXPIRED")
            
            elif response.status_code == 429:
                print(f"   ⚠️ Rate Limit Hit for {symbol}. Sleeping for 2 seconds...")
                time.sleep(2)
            
            elif response.status_code == 400 and "UDAPI1148" in response.text:
                # హిస్టరీ లిమిట్ దాటిపోతే లూప్ ఆపేస్తాం
                break
            else:
                print(f"   ❌ API Error {response.status_code}: {response.text}")
                
        except Exception as e:
            print(f"   ❌ Network error for {symbol}: {e}")
            
        current_end = current_start - datetime.timedelta(days=1)
        time.sleep(0.5)

    if all_candles:
        columns = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI']
        df = pd.DataFrame(all_candles, columns=columns)
        
        df['Datetime'] = pd.to_datetime(df['Datetime']).dt.tz_localize(None)
        df = df.sort_values(by="Datetime").drop_duplicates(subset=['Datetime'])
        
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
    
    return pd.DataFrame() # డేటా ఏమీ రాకపోతే

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

    print("🚀 Starting Daily EOD Data Fetch for NIFTY 200...")
    print("=" * 60)
    
    successful = 0
    updated = 0
    up_to_date = 0
    failed = 0

    for idx, symbol in enumerate(symbols, 1):
        instrument_key = upstox_keys.get(symbol)
        
        if not instrument_key:
            print(f"[{idx:3d}/{len(symbols)}] ⚠️ {symbol:12s} | No Upstox Key Found. Skipping.")
            failed += 1
            continue
            
        filename = os.path.join(SAVE_PATH, f"{symbol}_5m.parquet")
        existing_df = None
        existing_last_date = None
        
        # 🔴 1. లోకల్ ఫైల్ ఉందో లేదో చెక్ చేయడం
        if os.path.exists(filename):
            try:
                existing_df = pd.read_parquet(filename)
                if not existing_df.empty:
                    existing_last_date = existing_df['Datetime'].max()
            except Exception as e:
                print(f"   ⚠️ Could not read existing file for {symbol}: {e}")

        if existing_last_date:
            print(f"[{idx:3d}/{len(symbols)}] ⏳ {symbol:12s} | Updating from {existing_last_date.date()} to Today...")
        else:
            print(f"[{idx:3d}/{len(symbols)}] ⏳ {symbol:12s} | Fetching full 1-Year 5m data...")
            
        # 🔴 2. కొత్త డేటాని మాత్రమే ఫెచ్ చేయడం
        try:
            new_df = fetch_stock_5m_data(symbol, instrument_key, headers, existing_last_date)
        except RuntimeError as e:
            if str(e) == "TOKEN_EXPIRED":
                print("\n🚨 STOPPING PROCESS: Please update your Upstox Token and run again.")
                break
            raise
            
        # 🔴 3. డేటాని కలపడం (Merge / Append)
        if new_df is not None and not new_df.empty:
            if existing_df is not None:
                # పాత డేటా + కొత్త డేటా కలుపుతున్నాం
                combined_df = pd.concat([existing_df, new_df])
                # డూప్లికేట్స్ ఉంటే తీసేసి, ఆర్డర్ లో పెడుతున్నాం
                combined_df.drop_duplicates(subset=['Datetime'], keep='last', inplace=True)
                combined_df.sort_values(by="Datetime", inplace=True)
                
                combined_df.to_parquet(filename, index=False)
                print(f"            ✅ Updated! Added {len(new_df)} new candles. Total: {len(combined_df)}")
                updated += 1
            else:
                # మొదటిసారి సేవ్ చేస్తున్నప్పుడు
                new_df.to_parquet(filename, index=False)
                print(f"            ✅ Saved full data ({len(new_df)} candles).")
                successful += 1
        else:
            if existing_last_date and (datetime.date.today() == existing_last_date.date() or datetime.date.today().weekday() >= 5):
                print(f"            ⚡ Already Up-to-Date! (No new market data)")
                up_to_date += 1
            else:
                print(f"            ❌ Failed to fetch new data.")
                failed += 1
            
    print("=" * 60)
    print("🎉 EOD DATA FETCH COMPLETED!")
    print(f"📊 Full 1-Year Downloads : {successful} stocks")
    print(f"🔄 Incrementally Updated : {updated} stocks")
    print(f"⚡ Already Up-to-Date    : {up_to_date} stocks")
    print(f"⚠️ Failed / Skipped      : {failed} stocks")
    print(f"📁 All files saved in    : {SAVE_PATH}")
    print("=" * 60)

if __name__ == "__main__":
    main()