"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║   NIFTY 500 — PURE SMC REVERSAL SCANNER (Robust Date Parser + YF Bypass)        ║
╚══════════════════════════════════════════════════════════════════════════════════╝
"""

import pandas as pd
import yfinance as yf
import numpy as np
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import warnings

warnings.filterwarnings("ignore")

# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════
FUNDAMENTAL_WORKERS = 2   
SMC_WORKERS = 10          
UPSTOX_DATA_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox_History"

MAX_PE_RATIO = 50.0   
MIN_ROE_RATIO = 0.10  
MIN_RR_RATIO = 2.0    

SWING_LOOKBACK = 40       
SETUP_WINDOW = 15         

diag = {
    "total": 0, "data_fails": 0, "fundamental_fails": 0, "not_near_52w_low": 0,
    "no_sweep": 0, "no_displacement": 0, "no_choch": 0, "invalid_sequence": 0, "valid_setups": 0
}

def get_nifty500_symbols():
    print("  Fetching latest NIFTY 500 list from NSE...")
    try:
        url = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        from io import StringIO
        df = pd.read_csv(StringIO(response.text))
        symbols = df['Symbol'].tolist()
        print(f"  Successfully loaded {len(symbols)} stocks.\n")
        return symbols
    except Exception as e:
        print(f"  Failed to fetch NSE list. Using fallback.")
        return ["RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "SBIN", "LTIM", "ITC"]

# ════════════════════════════════════════════════════════════════════════════════
# PRE-SCAN DATA DIAGNOSIS
# ════════════════════════════════════════════════════════════════════════════════

def diagnose_data_file(symbols):
    print("\n" + "=" * 80)
    print("  PRE-SCAN DATA DIAGNOSTIC (Checking 1 sample file)")
    print("=" * 80)
    
    for symbol in symbols:
        possible_files = [
            os.path.join(UPSTOX_DATA_DIR, f"{symbol}_5m.parquet"),
            os.path.join(UPSTOX_DATA_DIR, f"{symbol}.parquet"),
            os.path.join(UPSTOX_DATA_DIR, f"{symbol}_EQ.parquet")
        ]
        file_path = next((f for f in possible_files if os.path.exists(f)), None)
        
        if file_path:
            try:
                df = pd.read_parquet(file_path)
                print(f"  Sample File Found: {file_path}")
                print(f"  Index Type        : {df.index.dtype}")
                print(f"  Columns           : {list(df.columns)}")
                print(f"  Total Rows        : {len(df)}")
                print(f"  First 3 Rows      :")
                print(df.head(3).to_string())
                print("=" * 80 + "\n")
                return
            except Exception as e:
                print(f"  Error reading sample file: {e}")
                print("=" * 80 + "\n")
                return
                
    print("  No Parquet files found in directory!")
    print("=" * 80 + "\n")

# ════════════════════════════════════════════════════════════════════════════════
# PHASE 1: FUNDAMENTAL FETCHER (With Bypass)
# ════════════════════════════════════════════════════════════════════════════════

def fetch_fundamental_data(symbol):
    try:
        info = yf.Ticker(f"{symbol}.NS").info
        pe = float(info.get("trailingPE") or info.get("forwardPE") or 0)
        roe = float(info.get("returnOnEquity") or 0)
        time.sleep(0.2)
        return symbol, pe, roe
    except Exception:
        return symbol, 0.0, 0.0

def run_phase_1_fundamentals(symbols):
    print("=" * 80)
    print("  PHASE 1: Fetching Fundamentals from Yahoo Finance (Anti-Ban Mode)")
    print("=" * 80)
    fund_data = {}
    with ThreadPoolExecutor(max_workers=FUNDAMENTAL_WORKERS) as executor:
        futures = [executor.submit(fetch_fundamental_data, sym) for sym in symbols]
        for i, future in enumerate(as_completed(futures), 1):
            sym, pe, roe = future.result()
            # If PE is 0, Yahoo blocked us. We still save it so Phase 2 can flag it as "Unknown"
            fund_data[sym] = {"pe": pe, "roe": roe}
            print(f"\r  Progress: {i}/{len(symbols)} | Stocks Processed: {len(fund_data)}", end="")
    print("\n  Phase 1 Complete.\n")
    return fund_data

# ════════════════════════════════════════════════════════════════════════════════
# LOCAL PARQUET LOADER & AUTO-RESAMPLER
# ════════════════════════════════════════════════════════════════════════════════

def load_upstox_history(symbol: str) -> pd.DataFrame:    
    possible_files = [
        os.path.join(UPSTOX_DATA_DIR, f"{symbol}_5m.parquet"),
        os.path.join(UPSTOX_DATA_DIR, f"{symbol}.parquet"),
        os.path.join(UPSTOX_DATA_DIR, f"{symbol}_EQ.parquet")
    ]
    file_path = next((f for f in possible_files if os.path.exists(f)), None)
    
    if file_path:
        try:
            df = pd.read_parquet(file_path)
            
            # If index is not datetime, find the column and set it
            if not pd.api.types.is_datetime64_any_dtype(df.index):
                date_col = None
                for col in df.columns:
                    if 'date' in str(col).lower() or 'time' in str(col).lower():
                        date_col = col
                        break
                if date_col:
                    # errors='coerce' turns unparseable dates into NaT instead of crashing
                    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                    df.dropna(subset=[date_col], inplace=True)
                    df.set_index(date_col, inplace=True)
                else:
                    return pd.DataFrame()
            
            df.columns = [str(col).strip().split('(')[0].strip().lower() for col in df.columns]
            rename_map = {
                'open': 'Open', 'o': 'Open',
                'high': 'High', 'h': 'High',
                'low': 'Low', 'l': 'Low',
                'close': 'Close', 'c': 'Close', 'last price': 'Close', 'ltp': 'Close',
                'volume': 'Volume', 'v': 'Volume', 'vol': 'Volume', 'total trade quantity': 'Volume'
            }
            df.rename(columns=rename_map, inplace=True)
            
            required = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in required:
                if col not in df.columns:
                    return pd.DataFrame()
                
            df = df[required].astype(float)
            df = df[~df.index.duplicated(keep='first')]
            df.sort_index(inplace=True)
            
            if len(df) > 1000:
                if df.index.tz is not None:
                    df.index = df.index.tz_localize(None)
                
                df = df.resample('1D').agg({
                    'Open': 'first', 'High': 'max', 'Low': 'min', 
                    'Close': 'last', 'Volume': 'sum'
                })
                df.dropna(subset=['Open', 'Close'], inplace=True)
                df = df[df['Volume'] > 0]
            
            return df
        except Exception:
            pass
            
    return pd.DataFrame()

# ════════════════════════════════════════════════════════════════════════════════
# PHASE 2: PURE SMC SCANNER
# ════════════════════════════════════════════════════════════════════════════════

def find_swing_points(df: pd.DataFrame, lookback: int = 2):
    swings = []
    for i in range(lookback, len(df) - lookback):
        is_high = True
        is_low = True
        for j in range(1, lookback + 1):
            if df['High'].iloc[i] <= df['High'].iloc[i - j] or df['High'].iloc[i] <= df['High'].iloc[i + j]:
                is_high = False
            if df['Low'].iloc[i] >= df['Low'].iloc[i - j] or df['Low'].iloc[i] >= df['Low'].iloc[i + j]:
                is_low = False
        if is_high: swings.append({'index': i, 'price': df['High'].iloc[i], 'type': 'HIGH'})
        if is_low: swings.append({'index': i, 'price': df['Low'].iloc[i], 'type': 'LOW'})
    return swings

def scan_stock_for_smc_reversal(symbol: str, fund_dict: dict) -> dict | None:
    diag["total"] += 1
    
    fund = fund_dict.get(symbol)
    if not fund:
        diag["fundamental_fails"] += 1
        return None
        
    pe_ratio = fund["pe"]
    roe_val = fund["roe"]
    
    # FUNDAMENTAL BYPASS LOGIC:
    # If Yahoo provided valid data, apply the filter. If Yahoo blocked us (pe=0), bypass the filter.
    if pe_ratio > 0:
        if pe_ratio > MAX_PE_RATIO or roe_val < MIN_ROE_RATIO:
            diag["fundamental_fails"] += 1
            return None

    df = load_upstox_history(symbol)
    if df.empty or len(df) < 260:
        diag["data_fails"] += 1
        return None

    df = df.reset_index(drop=True)
    last_idx = len(df) - 1
    last_close = df['Close'].iloc[-1]
    
    low_52w = df['Low'].tail(252).min()
    proximity_pct = (last_close - low_52w) / low_52w
    if proximity_pct > 0.10:
        diag["not_near_52w_low"] += 1
        return None

    swings = find_swing_points(df, lookback=2)
    if len(swings) < 3:
        diag["no_sweep"] += 1
        return None
        
    recent_swings = [s for s in swings if s['index'] >= (last_idx - SWING_LOOKBACK)]
    if len(recent_swings) < 2:
        diag["no_sweep"] += 1
        return None
    
    swing_lows = [s for s in recent_swings if s['type'] == 'LOW']
    liquidity_sweep = False
    sweep_idx = None
    if swing_lows:
        target_low = swing_lows[-1]
        for i in range(max(0, last_idx - SETUP_WINDOW), last_idx + 1):
            if df['Low'].iloc[i] < target_low['price'] and df['Close'].iloc[i] > target_low['price']:
                liquidity_sweep = True
                sweep_idx = i
                break
    if not liquidity_sweep:
        diag["no_sweep"] += 1
        return None

    avg_body = (abs(df['Close'] - df['Open'])).tail(20).mean()
    displacement_idx = None
    ob_idx = None
    for i in range(max(0, last_idx - SETUP_WINDOW), last_idx + 1):
        body = df['Close'].iloc[i] - df['Open'].iloc[i]
        if body > 0 and body > (avg_body * 1.5):
            displacement_idx = i
            for j in range(i - 1, max(0, i - 5), -1):
                if df['Close'].iloc[j] < df['Open'].iloc[j]:
                    ob_idx = j
                    break
            break
    if not displacement_idx or not ob_idx:
        diag["no_displacement"] += 1
        return None

    fvg_idx = None
    for i in range(max(2, last_idx - SETUP_WINDOW), last_idx + 1):
        if df['Low'].iloc[i] > df['High'].iloc[i - 2]:
            fvg_idx = i
            break

    swing_highs = [s for s in recent_swings if s['type'] == 'HIGH']
    choch_idx = None
    if swing_highs:
        target_high = swing_highs[-1]
        for i in range(max(0, last_idx - SETUP_WINDOW), last_idx + 1):
            if df['Close'].iloc[i] > target_high['price']:
                choch_idx = i
                break
    if not choch_idx:
        diag["no_choch"] += 1
        return None

    if not (ob_idx <= displacement_idx and displacement_idx <= choch_idx):
        diag["invalid_sequence"] += 1
        return None

    vol_spike = False
    disp_vol = df['Volume'].iloc[displacement_idx]
    prev_vol = df['Volume'].iloc[displacement_idx - 1] if displacement_idx > 0 else 0
    if disp_vol > prev_vol * 1.2:
        vol_spike = True

    entry_price = last_close
    if fvg_idx:
        entry_price = (df['Low'].iloc[fvg_idx] + df['High'].iloc[fvg_idx - 2]) / 2
    elif ob_idx:
        entry_price = (df['High'].iloc[ob_idx] + df['Low'].iloc[ob_idx]) / 2

    sl_price = low_52w
    if ob_idx:
        sl_price = df['Low'].iloc[ob_idx] * 0.99 
    elif sweep_idx:
        sl_price = df['Low'].iloc[sweep_idx] * 0.99

    risk_amount = abs(entry_price - sl_price)
    tp_price = entry_price + (risk_amount * 2.5) 
    if swing_highs:
        targets = [s['price'] for s in swing_highs if s['price'] > entry_price]
        if targets:
            tp_price = min(targets) 
    
    rr_ratio = round(abs(tp_price - entry_price) / risk_amount, 2) if risk_amount > 0 else 0
    if rr_ratio < MIN_RR_RATIO:
        diag["invalid_sequence"] += 1
        return None

    current_close = last_close
    if not vol_spike:
        diag["invalid_sequence"] += 1
        return None
    if not (sl_price <= current_close <= entry_price * 1.02):
        diag["invalid_sequence"] += 1
        return None
    if last_idx - choch_idx > 3:
        diag["invalid_sequence"] += 1
        return None

    diag["valid_setups"] += 1
    
    # Format PE/ROE for display
    pe_display = round(pe_ratio, 1) if pe_ratio > 0 else "Unknown"
    roe_display = round(roe_val * 100, 1) if roe_val > 0 else "Unknown"
    
    reasons = [
        f"AT 52W LOW ({proximity_pct*100:.1f}%)",
        "Liquidity Sweep",
        f"Valid SMC Sequence (OB@{ob_idx}, Disp@{displacement_idx}, CHoCH@{choch_idx})",
        "CHoCH Confirmed"
    ]
    if fvg_idx: reasons.append("Bullish FVG Formed")
    if vol_spike: reasons.append("Volume Spike on Disp.")

    return {
        "Symbol": symbol,
        "Price": round(last_close, 2),
        "PE": pe_display,
        "ROE_%": roe_display,
        "Entry": round(entry_price, 2),
        "SL": round(sl_price, 2),
        "Target": round(tp_price, 2),
        "R:R": rr_ratio,
        "52W_Low": round(low_52w, 2),
        "Proximity_%": round(proximity_pct * 100, 2),
        "Signal": "🚀 PURE SMC REVERSAL",
        "Reasons": " | ".join(reasons)
    }

# ════════════════════════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ════════════════════════════════════════════════════════════════════════════════

def main():
    symbols = get_nifty500_symbols()
    
    diagnose_data_file(symbols)
    
    fund_dict = run_phase_1_fundamentals(symbols)
    
    print("=" * 80)
    print("  PHASE 2: Scanning Local Upstox Data (Pure SMC Strict Mode)")
    print(f"     Local Data Directory: {UPSTOX_DATA_DIR}")
    print("=" * 80)
    print(f"  Scanning {len(symbols)} stocks with {SMC_WORKERS} parallel workers...\n")

    results = []

    with ThreadPoolExecutor(max_workers=SMC_WORKERS) as executor:
        futures = {executor.submit(scan_stock_for_smc_reversal, sym, fund_dict): sym for sym in symbols}
        for i, future in enumerate(as_completed(futures), 1):
            sym = futures[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
                    print(f"  [{i:3d}/{len(symbols)}] ⚡ SETUP FOUND: {result['Symbol']} | Entry: {result['Entry']} | SL: {result['SL']} | TP: {result['Target']} | R:R: {result['R:R']}")
            except Exception:
                pass

    results.sort(key=lambda x: x['R:R'], reverse=True)

    print("\n" + "=" * 80)
    print("  DIAGNOSTIC REPORT:")
    print(f"  Total Stocks Scanned    : {diag['total']}")
    print(f"  Failed Data Fetch       : {diag['data_fails']} (Parquet missing or format error)")
    print(f"  Failed Fundamentals     : {diag['fundamental_fails']}")
    print(f"  Not Near 52W Low        : {diag['not_near_52w_low']}")
    print(f"  No Liquidity Sweep      : {diag['no_sweep']}")
    print(f"  No Displacement/OB      : {diag['no_displacement']}")
    print(f"  No CHoCH                : {diag['no_choch']}")
    print(f"  Invalid Sequence/RR     : {diag['invalid_sequence']}")
    print(f"  Valid Strict Setups     : {diag['valid_setups']}")
    print("=" * 80)
    
    if not results:
        print("  ❌ No stocks met the STRICT Pure SMC criteria today.")
        print("=" * 80 + "\n")
        return

    print("  TOP PURE SMC REVERSAL CANDIDATES")
    print("=" * 80)
    
    df_results = pd.DataFrame(results)
    for index, row in df_results.iterrows():
        print(f"  {index+1}. {row['Symbol']:12s} | Rs.{row['Price']:8.2f} | PE: {row['PE']:5} | ROE: {row['ROE_%']:5}%")
        print(f"     👉 Entry: {row['Entry']} | 🛑 SL: {row['SL']} | 🎯 Target: {row['Target']} | 📐 R:R: 1:{row['R:R']}")
        print(f"     {row['Reasons']}")
        print("-" * 80)

    output_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "nifty500_pure_smc_trade_setups.csv",
    )
    df_results.to_csv(output_file, index=False)
    print(f"\n  ✅ Saved {len(df_results)} trade setups to {output_file}\n")

if __name__ == "__main__":
    main()
    