import os
import json
import time
import datetime
import urllib.parse
import sys
import importlib.util
import requests
import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
import warnings
from typing import Any, cast

# మీ పాత స్ట్రాటజీ ఫైల్ నుండి SMC లాజిక్ ఇంపోర్ట్ చేసుకోవడం (అదే ఫోల్డర్ లో ఆ ఫైల్ ఉండాలి)
STRATEGY_PATH = r"C:\Program Files\MyProject2\Nse_Project\Swing_stocks"
if STRATEGY_PATH not in sys.path:
    sys.path.append(STRATEGY_PATH)

try:
    strategy_file = os.path.join(STRATEGY_PATH, "nifty_advanced_strategy.py")
    if os.path.exists(strategy_file):
        spec = importlib.util.spec_from_file_location("nifty_advanced_strategy", strategy_file)
        if spec and spec.loader:
            strategy_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(strategy_module)
            smc_entry_signal = getattr(strategy_module, "smc_entry_signal", None)
        else:
            smc_entry_signal = None
    else:
        smc_entry_signal = None

    if smc_entry_signal is not None:
        print("✅ Advanced SMC Strategy Linked Successfully!")
    else:
        print("⚠️ Warning: smc_entry_signal not found in nifty_advanced_strategy.py. SMC filtering will be skipped.")
except Exception:
    print("⚠️ Warning: nifty_advanced_strategy.py not found. SMC filtering will be skipped.")
    smc_entry_signal = None

# =====================================================================
# 1. CONFIGURATION
# =====================================================================
TOKEN_FILE_PATH = r"C:\Program Files\MyProject2\Brokers\upstox\upstox_login_details.json"
HISTORY_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox_History"
MODEL_PATH = r"D:\Api data\ML_HISTO_DATA\Model_Weights\xgboost_swing_model.pkl"

# ML మోడల్ ట్రైనింగ్ లో వాడిన కచ్చితమైన ఫీచర్స్
ML_FEATURES = [
    '1H_EMA_20', '1H_EMA_50', '1H_RSI_14', '1H_ATR', '1H_Trend_Bullish', 
    '1D_EMA_20', '1D_RSI_14', '1D_Trend_Bullish', 
    '4H_EMA_20', '4H_RSI_14', '4H_Trend_Bullish'
]

def get_access_token():
    try:
        with open(TOKEN_FILE_PATH, 'r') as file:
            return json.load(file).get("access_token")
    except Exception:
        return None

def get_upstox_instrument_dict():
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
    df = pd.read_csv(url)
    return pd.Series(df.instrument_key.values, index=df.tradingsymbol).to_dict()

# =====================================================================
# 2. LIVE DATA FETCH & MERGE
# =====================================================================
def fetch_latest_1m_data(instrument_key, headers):
    """ఈరోజు లేటెస్ట్ డేటాని తెస్తుంది"""
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=5) # గత 5 రోజుల డేటా (సేఫ్టీ కోసం)
    
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
    
    agg_dict: dict[str, Any] = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
        'OI': 'last'
    }
    return df.resample(timeframe).agg(cast(Any, agg_dict)).dropna()


def calc_rsi(close: pd.Series, length: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def calc_atr(high: pd.Series, low: pd.Series, close: pd.Series, length: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()

def add_technical_features(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    if len(df) < 50:
        return df
    df[f'{prefix}_EMA_20'] = df['Close'].ewm(span=20, adjust=False).mean()
    df[f'{prefix}_EMA_50'] = df['Close'].ewm(span=50, adjust=False).mean()
    df[f'{prefix}_RSI_14'] = calc_rsi(df['Close'], length=14)
    df[f'{prefix}_ATR'] = calc_atr(df['High'], df['Low'], df['Close'], length=14)
    df[f'{prefix}_Trend_Bullish'] = np.where(df['Close'] > df[f'{prefix}_EMA_20'], 1, 0)
    return df.dropna()

def prepare_live_features(df_5m):
    """లైవ్ డేటాని ML మోడల్ కి అర్థమయ్యేలా మారుస్తుంది"""
    df_1h = resample_data(df_5m, '1h')
    df_4h = resample_data(df_5m, '4h')
    df_1d = resample_data(df_5m, '1D')
    
    df_1h = add_technical_features(df_1h, '1H')
    df_4h = add_technical_features(df_4h, '4H')
    df_1d = add_technical_features(df_1d, '1D')
    
    df_1h = df_1h.sort_index()
    
    # టాప్-డౌన్ మెర్జ్
    features_1d = df_1d[['1D_EMA_20', '1D_RSI_14', '1D_Trend_Bullish']].shift(1).dropna().sort_index()
    features_4h = df_4h[['4H_EMA_20', '4H_RSI_14', '4H_Trend_Bullish']].shift(1).dropna().sort_index()
    
    df_1h = pd.merge_asof(df_1h, features_1d, left_index=True, right_index=True, direction='backward')
    df_1h = pd.merge_asof(df_1h, features_4h, left_index=True, right_index=True, direction='backward')
    
    return df_1h.dropna()

# =====================================================================
# 4. MAIN LIVE SCANNER ENGINE
# =====================================================================
def main():
    print("\n" + "="*80)
    print(" 🚀 NIFTY 200 - AI + SMC LIVE TRADING SCANNER")
    print("="*80)

    # 1. Load ML Model
    if not os.path.exists(MODEL_PATH):
        print("❌ AI Model not found! Please run train_model.py first.")
        return
    model = joblib.load(MODEL_PATH)
    print("✅ AI Brain (XGBoost) Loaded Successfully.")

    # 2. Get API Token
    token = get_access_token()
    if not token:
        print("❌ Upstox Token Invalid. Exiting.")
        return
    headers = {'Accept': 'application/json', 'Authorization': f'Bearer {token}'}
    
    upstox_keys = get_upstox_instrument_dict()
    
    # లోకల్ హిస్టరీ ఫైల్స్ చదవడం (Nifty 200 కోసం)
    files = [f for f in os.listdir(HISTORY_DIR) if f.endswith("_5m.parquet")]
    
    print(f"📡 Scanning {len(files)} Stocks Live. Please wait...\n")
    print(f"{'SYMBOL':<15} | {'LTP':<8} | {'AI WIN %':<10} | {'SMC SIGNAL':<18} | {'ACTION'}")
    print("-" * 80)
    
    trade_opportunities = []

    for file in files:
        symbol = file.replace("_5m.parquet", "")
        inst_key = upstox_keys.get(symbol)
        if not inst_key: continue
            
        try:
            # పాత డేటా + లేటెస్ట్ డేటా కలపడం
            df_hist = pd.read_parquet(os.path.join(HISTORY_DIR, file))
            df_live_1m = fetch_latest_1m_data(inst_key, headers)
            
            if not df_live_1m.empty:
                # 1m ని 5m కి మార్చి పాత డేటాకి జతచేయడం
                df_live_5m = df_live_1m.set_index('Datetime').resample('5min').agg({
                    'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum', 'OI': 'last'
                }).dropna().reset_index()
                
                df_combined = pd.concat([df_hist, df_live_5m]).drop_duplicates(subset=['Datetime'], keep='last')
            else:
                df_combined = df_hist

            df_combined = df_combined.sort_values('Datetime')
            
            # ఫీచర్స్ ప్రిపేర్ చేయడం
            df_1h = prepare_live_features(df_combined)
            if df_1h.empty: continue
            
            latest_row = df_1h.iloc[-1]
            features_for_ml = latest_row[ML_FEATURES].to_frame().T
            
            # 🤖 ML Model Prediction (Probability of Success)
            win_prob = model.predict_proba(features_for_ml)[0][1] * 100
            
            # 🧠 SMC Strategy Check
            smc_tag = "NO_SETUP"
            if smc_entry_signal:
                # SMC కి కావలసిన ఫార్మాట్ లో 1H డేటాని పంపుతున్నాం
                smc_result = smc_entry_signal(df_1h.reset_index(), atr_value=latest_row['1H_ATR'])
                smc_tag = smc_result.get("smc_signal", "NO_SETUP")
            
            action = "WAIT"
            if win_prob >= 65.0:
                action = "BUY 🟢"
                if "HIGH_CONFLUENCE" in smc_tag:
                    action = "STRONG BUY 🚀"
            elif win_prob <= 30.0:
                action = "SELL 🔴"

            # 60% పైన ఛాన్స్ ఉన్న ట్రేడ్స్ మాత్రమే ప్రింట్ అవుతాయి (స్క్రీన్ క్లీన్ గా ఉండటానికి)
            if win_prob >= 60.0 or "HIGH" in smc_tag:
                ltp = latest_row['Close']
                print(f"{symbol:<15} | ₹{ltp:<7.2f} | {win_prob:>5.1f}%     | {smc_tag:<18} | {action}")
                
                if action in ["BUY 🟢", "STRONG BUY 🚀"]:
                    trade_opportunities.append({'Symbol': symbol, 'LTP': ltp, 'Win_Prob': win_prob, 'SMC': smc_tag})
                    
        except Exception as e:
            continue
        
        time.sleep(0.1) # రేట్ లిమిట్ బ్లాక్ అవ్వకుండా

    print("\n" + "="*80)
    print(f"🎯 Total Opportunities Found: {len(trade_opportunities)}")
    print("="*80)

if __name__ == "__main__":
    main()