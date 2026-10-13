# fyers_api.py
import requests
import pandas as pd
from datetime import datetime, timedelta
import config

IST = "Asia/Kolkata"


def fetch_historical_oi(token):
    """
    Fetch 1-minute historical OI data from Fyers REST API.
    Returns a DataFrame indexed by tz-aware IST timestamps with an 'oi' column.
    """
    print("📊 Fetching Historical OI data from Fyers...")

    access_token = f"{config.FYERS_APP_ID}:{token}"
    url = "https://api-t1.fyers.in/data/history"
    headers = {"Authorization": f"Bearer {access_token}"}

    end_date = int(datetime.now().timestamp())
    start_date = int((datetime.now() - timedelta(days=7)).timestamp())

    params = {
        "symbol": config.FUT_KEY,
        "resolution": "1",          # 1-minute candles
        "date_format": "0",
        "range_from": start_date,
        "range_to": end_date,
        "cont_flag": "1"
    }

    # ---- HTTP call with safety net ----
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
    except requests.RequestException as e:
        print(f"❌ Network error while fetching Fyers history: {e}")
        return pd.DataFrame()

    if response.status_code != 200:
        print(f"❌ Fyers API error [{response.status_code}]: {response.text}")
        return pd.DataFrame()

    payload = response.json()
    if payload.get("s") != "ok":
        print(f"⚠️ Fyers returned non-OK status: {payload}")
        return pd.DataFrame()

    data = payload.get("candles", [])
    if not data:
        print("⚠️ No historical data returned from Fyers.")
        return pd.DataFrame()

    # Fyers format:
    #   Equity    -> [ts, o, h, l, c, v]
    #   F&O       -> [ts, o, h, l, c, v, oi]
    ncols = len(data[0])
    if ncols == 7:
        cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi']
    elif ncols == 6:
        cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    else:
        print(f"⚠️ Unexpected column count ({ncols}) in Fyers response.")
        return pd.DataFrame()

    df = pd.DataFrame(data, columns=cols)

    # ---- CRITICAL: Fyers returns UTC epoch seconds.
    # Convert to tz-aware IST so it matches the tick DataFrame's index.
    df['timestamp'] = (
        pd.to_datetime(df['timestamp'], unit='s', utc=True)
          .dt.tz_convert(IST)
    )
    df.set_index('timestamp', inplace=True)

    # ---- Ensure 'oi' column exists ----
    if 'oi' not in df.columns:
        print("ℹ️ Fyers did not return OI column — filling with NaN.")
        df['oi'] = pd.NA

    # ---- Safety cleanup ----
    df = df[~df.index.duplicated(keep='last')].sort_index()

    print(f"✅ Downloaded {len(df)} historical OI points "
          f"({df.index[0]} → {df.index[-1]}).")

    return df[['oi']]