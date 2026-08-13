import os
import sys
import json
import time
import datetime
import urllib.parse
import requests
import pandas as pd
import pandas_ta as ta
import numpy as np
import xgboost as xgb
import joblib
import warnings

warnings.filterwarnings("ignore")

# =====================================================================
# 1. CONFIGURATION & STRATEGY LINKING
# =====================================================================
# SMC స్ట్రాటజీ లింకింగ్
STRATEGY_PATH = r"C:\Program Files\MyProject2\Nse_Project\Swing_stocks"
if STRATEGY_PATH not in sys.path:
    sys.path.append(STRATEGY_PATH)

try:
    from nifty_advanced_strategy import smc_entry_signal
    print("✅ Advanced SMC Strategy Linked Successfully!")
except ImportError:
    print("⚠️ Warning: nifty_advanced_strategy.py not found. SMC filtering will be skipped.")
    smc_entry_signal = None

# 🔴 కరెక్ట్ చేసిన టోకెన్ పాత్ (User Provided Path)
TOKEN_FILE_PATH = r"C:\Program Files\MyProject2\Nse_Project\Brokers\upstox\upstox\upstox_token.json"

HISTORY_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox_History"
MODEL_PATH = r"D:\Api data\ML_HISTO_DATA\Model_Weights\xgboost_swing_model.pkl"

ML_FEATURES = [
    '1H_EMA_20', '1H_EMA_50', '1H_RSI_14', '1H_ATR', '1H_Trend_Bullish', 
    '1D_EMA_20', '1D_RSI_14', '1D_Trend_Bullish', 
    '4H_EMA_20', '4H_RSI_14', '4H_Trend_Bullish'
]

# =====================================================================
# 2. HELPER FUNCTIONS
# =====================================================================
def get_access_token():
    """టోకెన్ ఫైల్ నుండి టోకెన్ ని సురక్షితంగా చదివే ఫంక్షన్"""
    try:
        with open(TOKEN_FILE_PATH, 'r') as file:
            data = json.load(file)
            token = data.get("access_token")
            if token:
                return token
            else:
                print(f"⚠️ Token file found, but 'access_token' is missing inside it.")
                return None
    except FileNotFoundError:
        print(f"❌ Token file not found at: {TOKEN_FILE_PATH}")
        return None
    except Exception as e:
        print(f"❌ Error reading token JSON: {e}")
        return None

def get_upstox_instrument_dict():
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
    df = pd.read_csv(url)
    return pd.Series(df.instrument_key.values, index=df.tradingsymbol).to_dict()

def fetch_latest_1m_data(instrument_key, headers):
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=5) 
    
    interval = "1minute" 
    safe_key = urllib.parse.quote(instrument_key)
    url = f"https://api.upstox.com/v2/historical-candle/{safe_key}/{interval}/{end_date.strftime('%Y-%m-%d')}/{start_date.strftime('%Y-%m-%d')}"
    
    try:
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            data = res.json()
            if 'data' in data and 'candles' in data['data'] and data['data']['candles']:
                df = pd.DataFrame(data['data']['candles'], columns=['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI'])
                df['Datetime'] = pd.to_datetime(df['Datetime']).dt.tz_localize(None)
                return df
    except Exception:
        pass
    return pd.DataFrame()

# =====================================================================
# 3. DATA PROCESSING (Multi-Timeframe)
# =====================================================================
def resample_data(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    df = df.set_index('Datetime') if 'Datetime' in df.columns else df
    df.index = pd.to_datetime(df.index)
    agg_dict = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum', 'OI': 'last'}
    return df.resample(timeframe).agg(agg_dict).dropna()

def add_technical_features(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    if len(df) < 50: return df
    df[f'{prefix}_EMA_20'] = ta.ema(df['Close'], length=20)
    df[f'{prefix}_EMA_50'] = ta.ema(df['Close'], length=50)
    df[f'{prefix}_RSI_14'] = ta.rsi(df['Close'], length=14)
    df[f'{prefix}_ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)
    df[f'{prefix}_Trend_Bullish'] = np.where(df['Close'] > df[f'{prefix}_EMA_20'], 1, 0)
    return df.dropna()

def prepare_live_features(df_5m):
    df_1h = resample_data(df_5m, '1h')
    df_4h = resample_data(df_5m, '4h')
    df_1d = resample_data(df_5m, '1D')
    
    df_1h = add_technical_features(df_1h, '1H')
    df_4h = add_technical_features(df_4h, '4H')
    df_1d = add_technical_features(df_1d, '1D')
    
    df_1h = df_1h.sort_index()
    
    features_1d = df_1d[['1D_EMA_20', '1D_RSI_14', '1D_Trend_Bullish']].shift(1).dropna().sort_index()
    features_4h = df_4h[['4H_EMA_20', '4H_RSI_14', '4H_Trend_Bullish']].shift(1).dropna().sort_index()
    
    df_1h = pd.merge_asof(df_1h, features_1d, left_index=True, right_index=True, direction='backward')
    df_1h = pd.merge_asof(df_1h, features_4h, left_index=True, right_index=True, direction='backward')
    
    return df_1h.dropna()

# =====================================================================
# 4. MAIN LIVE SCANNER ENGINE (Dual Output & LTP Filter)
# =====================================================================
def main():
    print("\n" + "="*85)
    print(" 🚀 NIFTY 200 - AI + SMC LIVE TRADING SCANNER (< ₹2000 ONLY)")
    print("="*85)

    if not os.path.exists(MODEL_PATH):
        print("❌ AI Model not found! Please run train_model.py first.")
        return
    model = joblib.load(MODEL_PATH)
    print("✅ AI Brain (XGBoost) Loaded Successfully.")

    token = get_access_token()
    if not token:
        print("❌ Upstox Token Invalid or Missing. Exiting.")
        return
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {token}'}
    
    upstox_keys = get_upstox_instrument_dict()
    files = [f for f in os.listdir(HISTORY_DIR) if f.endswith("_5m.parquet")]
    
    print(f"📡 Scanning {len(files)} Stocks Live. Please wait...\n")
    
    tier_1_trades = [] 
    tier_2_trades = [] 

    for file in files:
        symbol = file.replace("_5m.parquet", "")
        inst_key = upstox_keys.get(symbol)
        if not inst_key: continue
            
        try:
            df_hist = pd.read_parquet(os.path.join(HISTORY_DIR, file))
            df_live_1m = fetch_latest_1m_data(inst_key, headers)
            
            if not df_live_1m.empty:
                df_live_5m = df_live_1m.set_index('Datetime').resample('5min').agg({
                    'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum', 'OI': 'last'
                }).dropna().reset_index()
                
                df_combined = pd.concat([df_hist, df_live_5m]).drop_duplicates(subset=['Datetime'], keep='last')
            else:
                df_combined = df_hist

            df_combined = df_combined.sort_values('Datetime')
            
            df_1h = prepare_live_features(df_combined)
            if df_1h.empty: continue
            
            latest_row = df_1h.iloc[-1]
            features_for_ml = latest_row[ML_FEATURES].to_frame().T
            
            win_prob = model.predict_proba(features_for_ml)[0][1] * 100
            
            smc_tag = "NO_SETUP"
            if smc_entry_signal:
                smc_result = smc_entry_signal(df_1h.reset_index(), atr_value=latest_row['1H_ATR'])
                smc_tag = smc_result.get("smc_signal", "NO_SETUP")
            
            ltp = latest_row['Close']
            
            if ltp >= 2000.0:
                continue
            
            if smc_tag != "NO_SETUP" and win_prob >= 60.0:
                tier_1_trades.append({
                    'Symbol': symbol, 'LTP': ltp, 'Win_Prob': win_prob, 'SMC': smc_tag, 'Action': 'STRONG BUY 🚀'
                })
            
            elif smc_tag == "NO_SETUP" and win_prob >= 65.0:
                tier_2_trades.append({
                    'Symbol': symbol, 'LTP': ltp, 'Win_Prob': win_prob, 'SMC': smc_tag, 'Action': 'AI BUY 🟢'
                })
                
        except Exception as e:
            continue
        
        time.sleep(0.1)

    # =====================================================================
    # OUTPUT DISPLAY FORMATTING
    # =====================================================================
    print("="*85)
    print(" 🏆 TIER-1: THE ULTIMATE TRADES (SMC + AI CONFLUENCE)")
    print("    (Order Block/FVG + AI > 60% probability | LTP < 2000)")
    print("="*85)
    if tier_1_trades:
        print(f"{'SYMBOL':<15} | {'LTP':<8} | {'AI WIN %':<10} | {'SMC SIGNAL':<18} | {'ACTION'}")
        print("-" * 85)
        for t in tier_1_trades:
            print(f"{t['Symbol']:<15} | ₹{t['LTP']:<7.2f} | {t['Win_Prob']:>5.1f}%     | {t['SMC']:<18} | {t['Action']}")
    else:
        print("   ⏳ No Tier-1 setups found right now. Wait for the market to form order blocks.")

    print("\n" + "="*85)
    print(" 🟢 TIER-2: AI MOMENTUM TRADES (AI CONFIDENCE ONLY)")
    print("    (No strict SMC setup, but AI > 65% win chance | LTP < 2000)")
    print("="*85)
    if tier_2_trades:
        print(f"{'SYMBOL':<15} | {'LTP':<8} | {'AI WIN %':<10} | {'SMC SIGNAL':<18} | {'ACTION'}")
        print("-" * 85)
        for t in tier_2_trades:
            print(f"{t['Symbol']:<15} | ₹{t['LTP']:<7.2f} | {t['Win_Prob']:>5.1f}%     | {t['SMC']:<18} | {t['Action']}")
    else:
        print("   ⏳ No Tier-2 momentum setups found.")

    print("\n" + "="*85)
    print(f"🎯 Total Opportunities (< ₹2000): {len(tier_1_trades) + len(tier_2_trades)} (Tier-1: {len(tier_1_trades)}, Tier-2: {len(tier_2_trades)})")
    print("="*85)

if __name__ == "__main__":
    main()