# build_features.py
import os
from datetime import datetime
import config
import db_manager
import fyers_api
import feature_engine

def main():
    print("--- STARTING ML FEATURE ENGINEERING ---")
    
    # 1. Get Token from DB
    token = db_manager.get_fyers_token()
    if not token:
        print("❌ No Fyers token found. Please login on Render first.")
        return
        
    # 2. Fetch Historical OI from Fyers
    hist_oi = fyers_api.fetch_historical_oi(token)
    
    # 3. Fetch TBT Data from Supabase
    raw_df = db_manager.fetch_raw_ticks()
    if raw_df.empty:
        return
        
    # 4. Apply Tick Rule & Build Features
    ticked_df = feature_engine.apply_tick_rule(raw_df)
    features_df = feature_engine.build_candles(ticked_df, hist_oi)
    
    # 5. Save to D Drive
    os.makedirs(config.SAVE_DIRECTORY, exist_ok=True)
    filename = os.path.join(config.SAVE_DIRECTORY, f"nifty_features_{config.TIMEFRAME}_{datetime.now().strftime('%Y-%m-%d')}.parquet")
    
    features_df.to_parquet(filename)
    print(f"\n💾 SUCCESS! Saved {len(features_df)} ML-ready candles to {filename}")
    print("\nPreview of your ML Data (First 5 rows):")
    print(features_df.head())
    print("\nTarget Distribution (1=UP, 0=DOWN):")
    print(features_df['target'].value_counts())
    print("\n--- FEATURE ENGINEERING COMPLETE ---")

if __name__ == "__main__":
    main()