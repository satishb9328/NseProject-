# fyers_api.py
import requests
import pandas as pd
from datetime import datetime, timedelta
import config

def fetch_historical_oi(token):
    """Fetches 1-minute historical OI data from Fyers REST API."""
    print("📊 Fetching Historical OI data from Fyers...")
    access_token = f"{config.FYERS_APP_ID}:{token}"
    url = "https://api-t1.fyers.in/data/history"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    # Fetch last 7 days of data
    end_date = int(datetime.now().timestamp())
    start_date = int((datetime.now() - timedelta(days=7)).timestamp())
    
    params = {
        "symbol": config.FUT_KEY,
        "resolution": "1", # 1-minute candles
        "date_format": "0",
        "range_from": start_date,
        "range_to": end_date,
        "cont_flag": "1"
    }
    
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json().get('candles', [])
        # Fyers returns: [timestamp, open, high, low, close, volume, oi]
        df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df.set_index('timestamp', inplace=True)
        print(f"✅ Downloaded {len(df)} historical OI points.")
        return df[['oi']]
    else:
        print(f"❌ Error fetching history: {response.text}")
        return pd.DataFrame()