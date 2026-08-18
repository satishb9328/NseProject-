import os
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Paste your exact connection string here
DB_CONNECTION_STRING = "postgresql://postgres.jryrijkauzlmuzcehlof:dZVAOHh1fe9RcauJ@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres"

TIMEFRAME = "2min"
SAVE_DIRECTORY = "D:/Nifty_ML_Data"

def fetch_raw_ticks():
    """Downloads all TBT Futures data from Supabase."""
    print("🔌 Connecting to Supabase to download TBT data...")
    
    # Use the connection string directly
    conn = psycopg2.connect(DB_CONNECTION_STRING)
    
    # We only download the FUTURES data (Index doesn't have Bid/Ask/Volume)
    query = """
        SELECT timestamp, ltp, bid_price, ask_price, total_buy_qty, total_sell_qty, volume, oi 
        FROM tbt_ticks 
        WHERE instrument LIKE 'NSE:NIFTY%FUT%' 
        ORDER BY timestamp ASC;
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("❌ No data found in the database.")
        return df
        
    print(f"✅ Downloaded {len(df)} raw ticks.")
    return df

def apply_tick_rule(df):
    """
    Tick Rule: Classifies volume as Aggressive Buy or Sell based on price movement.
    """
    print("🧠 Applying Tick Rule to calculate Order Flow...")
    df['price_diff'] = df['ltp'].diff()
    df['trade_direction'] = np.sign(df['price_diff'])
    
    # Assign volume to buy/sell based on direction
    df['buy_volume'] = np.where(df['trade_direction'] > 0, df['volume'], 0)
    df['sell_volume'] = np.where(df['trade_direction'] < 0, df['volume'], 0)
    
    return df

def build_candles(df):
    """Aggregates raw ticks into 2-minute OHLC + Order Flow candles."""
    print(f"📈 Building {TIMEFRAME} candles with ML features...")
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)
    
    agg_dict = {
        'ltp': ['first', 'max', 'min', 'last'],
        'volume': 'last',          # Cumulative day volume at end of candle
        'oi': 'last',              # Cumulative day OI at end of candle
        'total_buy_qty': 'last',   # Market depth at end of candle
        'total_sell_qty': 'last',  # Market depth at end of candle
        'buy_volume': 'sum',       # Aggressive buy volume IN THIS CANDLE
        'sell_volume': 'sum'       # Aggressive sell volume IN THIS CANDLE
    }
    
    candles = df.resample(TIMEFRAME).agg(agg_dict)
    candles.columns = ['open', 'high', 'low', 'close', 
                       'cum_volume', 'cum_oi', 'total_buy_qty', 'total_sell_qty',
                       'candle_buy_vol', 'candle_sell_vol']
    
    candles.dropna(subset=['open'], inplace=True)
    
    # Calculate actual volume traded inside this 2-min candle
    candles['candle_volume'] = candles['cum_volume'].diff().fillna(candles['cum_volume'])
    
    # Calculate change in OI inside this 2-min candle
    candles['oi_change'] = candles['cum_oi'].diff().fillna(candles['cum_oi'])
    
    # --- ORDER FLOW FEATURES ---
    candles['volume_delta'] = candles['candle_buy_vol'] - candles['candle_sell_vol']
    candles['ofi'] = candles['total_buy_qty'] - candles['total_sell_qty']
    candles['ofi_imbalance'] = candles['ofi'] / (candles['total_buy_qty'] + candles['total_sell_qty'])
    
    # --- VWAP FEATURES ---
    typical_price = (candles['high'] + candles['low'] + candles['close']) / 3
    daily_group = candles.groupby(candles.index.date)
    cum_vol = daily_group['candle_volume'].cumsum()
    cum_typ_price_vol = (typical_price * candles['candle_volume']).groupby(candles.index.date).cumsum()
    candles['vwap'] = cum_typ_price_vol / cum_vol
    candles['vwap_distance'] = (candles['close'] - candles['vwap']) / candles['vwap']
    
    # --- LAG FEATURES (Crucial for ML time-series) ---
    # ML needs to see what happened in the PREVIOUS candles to predict the next one
    candles['prev_delta'] = candles['volume_delta'].shift(1)
    candles['prev_oi_change'] = candles['oi_change'].shift(1)
    candles['prev_ofi_imbalance'] = candles['ofi_imbalance'].shift(1)
    
    # --- LABELING (What are we predicting?) ---
    # We want to predict if the NEXT 2 candles (4 mins) go UP or DOWN by > 0.1%
    candles['future_return'] = candles['close'].shift(-2) / candles['close'] - 1
    
    # 1 = UP, 0 = DOWN/NEUTRAL
    candles['target'] = np.where(candles['future_return'] > 0.001, 1, 0)
    
    # Drop rows with NaN values created by shifting
    candles.dropna(inplace=True)
    
    # Select only the final ML features
    ml_features = candles[['candle_volume', 'oi_change', 'volume_delta', 'ofi', 'ofi_imbalance', 
                           'vwap_distance', 'prev_delta', 'prev_oi_change', 'prev_ofi_imbalance', 'target']].copy()
    
    return ml_features

def main():
    # 1. Fetch Data
    raw_df = fetch_raw_ticks()
    if raw_df.empty:
        return
        
    # 2. Apply Tick Rule
    ticked_df = apply_tick_rule(raw_df)
    
    # 3. Build 2-min Candles & Features
    features_df = build_candles(ticked_df)
    
    # 4. Save to D Drive
    os.makedirs(SAVE_DIRECTORY, exist_ok=True)
    filename = os.path.join(SAVE_DIRECTORY, f"nifty_features_{TIMEFRAME}_{datetime.now().strftime('%Y-%m-%d')}.parquet")
    
    features_df.to_parquet(filename)
    print(f"\n💾 SUCCESS! Saved {len(features_df)} ML-ready 2-min candles to {filename}")
    print("\nPreview of your ML Data (First 5 rows):")
    print(features_df.head())
    print("\nTarget Distribution (1=UP, 0=DOWN):")
    print(features_df['target'].value_counts())

if __name__ == "__main__":
    main()