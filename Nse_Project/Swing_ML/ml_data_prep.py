import os
import glob
import pandas as pd
import pandas_ta as ta
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# =====================================================================
# 1. CONFIGURATION
# =====================================================================
INPUT_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox_History"
OUTPUT_DIR = r"D:\Api data\ML_HISTO_DATA\ML_Master_Data"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =====================================================================
# 2. HELPER FUNCTIONS
# =====================================================================

def resample_data(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """5-నిమిషాల డేటాని కావాల్సిన టైమ్‌ఫ్రేమ్ కి మారుస్తుంది."""
    if 'Datetime' in df.columns:
        df = df.set_index('Datetime')
    
    # ఇండెక్స్ ని datetime ఫార్మాట్ లోకి మార్చడం
    df.index = pd.to_datetime(df.index)
    
    agg_dict = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
        'OI': 'last'
    }
    
    resampled_df = df.resample(timeframe).agg(agg_dict).dropna()
    return resampled_df

def add_technical_features(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """టెక్నికల్ ఇండికేటర్స్ ని ఫీచర్స్ లాగా యాడ్ చేస్తుంది."""
    if len(df) < 50:
        return df
    
    df[f'{prefix}_EMA_20'] = ta.ema(df['Close'], length=20)
    df[f'{prefix}_EMA_50'] = ta.ema(df['Close'], length=50)
    df[f'{prefix}_RSI_14'] = ta.rsi(df['Close'], length=14)
    df[f'{prefix}_ATR'] = ta.atr(df['High'], df['Low'], df['Close'], length=14)
    df[f'{prefix}_Trend_Bullish'] = np.where(df['Close'] > df[f'{prefix}_EMA_20'], 1, 0)
    
    return df.dropna()

# =====================================================================
# 3. CORE LOGIC 
# =====================================================================

def process_stock_for_ml(filepath: str):
    symbol = os.path.basename(filepath).replace("_5m.parquet", "")
    print(f"⏳ Processing {symbol} for ML...")
    
    try:
        df_5m = pd.read_parquet(filepath)
        if df_5m.empty or len(df_5m) < 1000:
            print(f"   ⚠️ Not enough data for {symbol}. Skipping.")
            return
            
        # 1. Resampling
        df_1h = resample_data(df_5m, '1h')
        df_4h = resample_data(df_5m, '4h')
        df_1d = resample_data(df_5m, '1D') 
        
        # 2. Features క్యాలిక్యులేషన్
        df_1h = add_technical_features(df_1h, '1H')
        df_4h = add_technical_features(df_4h, '4H')
        df_1d = add_technical_features(df_1d, '1D')
        
        # 3. TOP-DOWN MERGE (The Magic Fix)
        # టైమ్స్ మిస్ అవ్వకుండా కచ్చితంగా పాత సమయంతో కలపడానికి sort చేసి merge_asof వాడాలి
        df_1h = df_1h.sort_index()
        
        # shift(1) అంటే నిన్నటి డేటాని ఈరోజు ఉదయం వాడుకో అని మోడల్ కి చెప్పడం (No Cheating)
        features_1d = df_1d[['1D_EMA_20', '1D_RSI_14', '1D_Trend_Bullish']].shift(1).dropna().sort_index()
        features_4h = df_4h[['4H_EMA_20', '4H_RSI_14', '4H_Trend_Bullish']].shift(1).dropna().sort_index()
        
        # 1-Hour క్యాండిల్ సమయానికి సరిగ్గా ముందున్న 1D, 4H వాల్యూస్ ఆటోమేటిక్ గా కలుస్తాయి 
        df_1h = pd.merge_asof(df_1h, features_1d, left_index=True, right_index=True, direction='backward')
        df_1h = pd.merge_asof(df_1h, features_4h, left_index=True, right_index=True, direction='backward')
        
        df_1h = df_1h.dropna()
        
        # 4. ML TARGET CREATION 
        target_pct = 0.005 # 0.5% ప్రాఫిట్ టార్గెట్
        
        # రాబోయే 3 క్యాండిల్స్ (3 గంటల) లో హైయెస్ట్ పాయింట్ ఎంత ఉందో వెనక్కి లాగుతుంది
        df_1h['Next_3H_High'] = df_1h['High'].rolling(window=3).max().shift(-3)
        
        # భవిష్యత్తు High.. 0.5% ఎక్కువ ఉంటే 1, లేకపోతే 0
        df_1h['Target_Hit'] = np.where(
            df_1h['Next_3H_High'] >= (df_1h['Close'] * (1 + target_pct)), 
            1, 0
        )
        
        df_1h = df_1h.dropna()
        
        # 5. ఫైల్ సేవ్
        if not df_1h.empty:
            output_file = os.path.join(OUTPUT_DIR, f"{symbol}_master.parquet")
            df_1h.reset_index().to_parquet(output_file, index=False)
            print(f"   ✅ Saved {symbol} ML Data (Rows: {len(df_1h)}, Target Hits: {df_1h['Target_Hit'].sum()})")
        else:
            print(f"   ⚠️ Result empty after merge for {symbol}")
            
    except Exception as e:
        print(f"   ❌ Error processing {symbol}: {e}")

def main():
    print("🚀 Starting ML Data Preparation (Top-Down Approach)...")
    print("=" * 60)
    
    parquet_files = glob.glob(os.path.join(INPUT_DIR, "*_5m.parquet"))
    
    if not parquet_files:
        print("❌ No 5-minute data files found. Please check your Upstox_History folder.")
        return
        
    for filepath in parquet_files:
        process_stock_for_ml(filepath)
        
    print("=" * 60)
    print(f"🎉 ML Master Data successfully saved to: {OUTPUT_DIR}")

if __name__ == "__main__":
    main()