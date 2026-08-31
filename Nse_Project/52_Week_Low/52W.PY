"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║   NIFTY 500 — PURE SMC SCANNER + 5M INTRADAY DISPLACEMENT (v6.0 Optimized)      ║
║   1. ProcessPoolExecutor (True CPU Parallelization)                             ║
║   2. Volume-Backed Order Blocks                                                 ║
║   3. Market Hours Data Cleaning (09:15 - 15:30)                                 ║
║   4. 3-Period Cumulative Volume Delta (CVD_3) on 5m Displacement                ║
║   5. Near-Miss Watchlist CSV Export                                             ║
╚══════════════════════════════════════════════════════════════════════════════════╝
"""

import pandas as pd
import numpy as np
import os
import requests
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed

warnings.filterwarnings("ignore")

# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════
UPSTOX_DATA_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox_History"
MIN_RR_RATIO = 2.0    

# Daily SMC Parameters
SWING_LOOKBACK = 40       
SETUP_WINDOW = 15         

# 5-Minute Displacement Parameters
SWING_LOOKBACK_5M = 5
VOL_LOOKBACK_5M = 20
VOL_MULT_5M = 2.5
BODY_RATIO_THRESH = 0.85
UPPER_WICK_THRESH = 0.10

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
        return ["360ONE", "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "SBIN", "LTIM", "ITC"]

# ════════════════════════════════════════════════════════════════════════════════
# 5-MINUTE VECTORIZED DISPLACEMENT DETECTION (WITH CVD)
# ════════════════════════════════════════════════════════════════════════════════

def calculate_volume_delta_proxy(df: pd.DataFrame) -> pd.Series:
    high = df['High']
    low = df['Low']
    close = df['Close']
    volume = df['Volume']
    
    range_val = (high - low).replace(0, np.nan)
    delta_proxy = volume * (((close - low) - (high - close)) / range_val)
    return delta_proxy.fillna(0)

def detect_bullish_displacement_5m(df_5m: pd.DataFrame) -> pd.DataFrame:
    if len(df_5m) < VOL_LOOKBACK_5M + 2:
        df_5m['Bullish_Displacement_5m'] = False
        return df_5m
        
    df_5m = df_5m.copy()
    
    # 1. Volume Delta Spike
    df_5m['Volume_Delta'] = calculate_volume_delta_proxy(df_5m)
    df_5m['VD_Rolling_Mean'] = df_5m['Volume_Delta'].rolling(window=VOL_LOOKBACK_5M).mean().shift(1)
    cond_vol_spike = (df_5m['Volume_Delta'] > 0) & (df_5m['Volume_Delta'] > (df_5m['VD_Rolling_Mean'] * VOL_MULT_5M))
    
    # 4. Cumulative Volume Delta (CVD_3)
    df_5m['CVD_3'] = df_5m['Volume_Delta'].rolling(3).sum()
    cond_cvd = df_5m['CVD_3'] > 0
    
    # 2. Market Structure Shift (MSS)
    df_5m['Prev_Swing_High'] = df_5m['High'].rolling(window=SWING_LOOKBACK_5M).max().shift(1)
    cond_mss = df_5m['Close'] > df_5m['Prev_Swing_High']
    
    # 3. Fair Value Gap (FVG) Creation
    cond_fvg = df_5m['Low'] > df_5m['High'].shift(2)
    
    # 5. Strong Bullish Candle
    body = (df_5m['Close'] - df_5m['Open']).abs()
    total_range = (df_5m['High'] - df_5m['Low']).replace(0, np.nan)
    upper_wick = df_5m['High'] - df_5m[['Open', 'Close']].max(axis=1)
    
    body_ratio = body / total_range
    upper_wick_ratio = upper_wick / total_range
    
    cond_strong_candle = (
        (df_5m['Close'] > df_5m['Open']) & 
        (body_ratio >= BODY_RATIO_THRESH) & 
        (upper_wick_ratio <= UPPER_WICK_THRESH)
    )
    
    # Combine conditions
    df_5m['Bullish_Displacement_5m'] = (cond_vol_spike & cond_cvd & cond_mss & cond_fvg & cond_strong_candle).fillna(False)
    
    df_5m.drop(columns=['Volume_Delta', 'VD_Rolling_Mean', 'Prev_Swing_High', 'CVD_3'], inplace=True, errors='ignore')
    return df_5m

# ════════════════════════════════════════════════════════════════════════════════
# LOCAL PARQUET LOADER (WITH MARKET HOURS CLEANING)
# ════════════════════════════════════════════════════════════════════════════════

def load_and_process_data(symbol: str):    
    possible_files = [
        os.path.join(UPSTOX_DATA_DIR, f"{symbol}_5m.parquet"),
        os.path.join(UPSTOX_DATA_DIR, f"{symbol}.parquet"),
        os.path.join(UPSTOX_DATA_DIR, f"{symbol}_EQ.parquet")
    ]
    file_path = next((f for f in possible_files if os.path.exists(f)), None)
    
    if file_path:
        try:
            df_5m = pd.read_parquet(file_path)
            
            if not pd.api.types.is_datetime64_any_dtype(df_5m.index):
                date_col = None
                for col in df_5m.columns:
                    if 'date' in str(col).lower() or 'time' in str(col).lower():
                        date_col = col
                        break
                if date_col:
                    df_5m[date_col] = pd.to_datetime(df_5m[date_col], errors='coerce')
                    df_5m.dropna(subset=[date_col], inplace=True)
                    df_5m.set_index(date_col, inplace=True)
                else:
                    return pd.DataFrame(), pd.DataFrame()
            
            df_5m.columns = [str(col).strip().split('(')[0].strip().lower() for col in df_5m.columns]
            rename_map = {
                'open': 'Open', 'high': 'High', 'low': 'Low', 
                'close': 'Close', 'volume': 'Volume'
            }
            df_5m.rename(columns=rename_map, inplace=True)
            
            required = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in required:
                if col not in df_5m.columns:
                    return pd.DataFrame(), pd.DataFrame()
                
            df_5m = df_5m[required].astype(float)
            df_5m = df_5m[~df_5m.index.duplicated(keep='first')]
            df_5m.sort_index(inplace=True)
            
            # MARKET HOURS CLEANING: Keep only regular NSE trading hours
            if not df_5m.empty:
                if df_5m.index.tz is not None:
                    df_5m.index = df_5m.index.tz_localize(None)
                df_5m = df_5m.between_time('09:15', '15:30')
            
            # Run 5m Displacement Detection on cleaned raw 5m data
            df_5m = detect_bullish_displacement_5m(df_5m)
            
            # Resample to Daily for Macro SMC Logic
            if len(df_5m) > 1000:
                df_daily = df_5m.resample('1D').agg({
                    'Open': 'first', 'High': 'max', 'Low': 'min', 
                    'Close': 'last', 'Volume': 'sum'
                })
                df_daily.dropna(subset=['Open', 'Close'], inplace=True)
                df_daily = df_daily[df_daily['Volume'] > 0]
                
                return df_5m, df_daily
            
            return pd.DataFrame(), pd.DataFrame()
        except Exception:
            return pd.DataFrame(), pd.DataFrame()
            
    return pd.DataFrame(), pd.DataFrame()

# ════════════════════════════════════════════════════════════════════════════════
# DAILY PURE SMC SCANNER (WITH VOLUME-BACKED OB)
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

def scan_stock_for_smc_reversal(symbol: str) -> dict:
    # Worker function returns a dictionary containing the result AND the diagnostic status
    result = {"Symbol": symbol, "Type": "NONE", "Diag": "data_fails", "Status": "", "Price": 0, "Entry": None, "SL": None, "Target": None, "R:R": None, "5m_Trigger": False}
    
    df_5m, df_daily = load_and_process_data(symbol)
    
    if df_daily.empty or len(df_daily) < 60:
        return result

    df_daily = df_daily.reset_index(drop=True)
    last_idx = len(df_daily) - 1
    last_close = df_daily['Close'].iloc[-1]
    
    low_52w = df_daily['Low'].tail(252).min()
    proximity_pct = (last_close - low_52w) / low_52w
    if proximity_pct > 0.10:
        result["Diag"] = "not_near_52w_low"
        return result

    swings = find_swing_points(df_daily, lookback=2)
    if len(swings) < 3:
        result["Diag"] = "no_sweep"
        return result
        
    recent_swings = [s for s in swings if s['index'] >= (last_idx - SWING_LOOKBACK)]
    if len(recent_swings) < 2:
        result["Diag"] = "no_sweep"
        return result
    
    # 1. Liquidity Sweep
    swing_lows = [s for s in recent_swings if s['type'] == 'LOW']
    liquidity_sweep = False
    sweep_idx = None
    if swing_lows:
        target_low = swing_lows[-1]
        for i in range(max(0, last_idx - SETUP_WINDOW), last_idx + 1):
            if df_daily['Low'].iloc[i] < target_low['price'] and df_daily['Close'].iloc[i] > target_low['price']:
                liquidity_sweep = True
                sweep_idx = i
                break
    if not liquidity_sweep:
        result["Diag"] = "no_sweep"
        return result

    # 2. Displacement & VOLUME-BACKED OB
    df_daily['Vol_MA20'] = df_daily['Volume'].rolling(20).mean()
    avg_body = (abs(df_daily['Close'] - df_daily['Open'])).tail(20).mean()
    displacement_idx = None
    ob_idx = None
    
    for i in range(max(0, last_idx - SETUP_WINDOW), last_idx + 1):
        body = df_daily['Close'].iloc[i] - df_daily['Open'].iloc[i]
        if body > 0 and body > (avg_body * 1.5):
            displacement_idx = i
            for j in range(i - 1, max(0, i - 5), -1):
                # VOLUME-BACKED OB: Red candle AND Volume > 20-MA Volume
                if (df_daily['Close'].iloc[j] < df_daily['Open'].iloc[j]) and \
                   (df_daily['Volume'].iloc[j] > df_daily['Vol_MA20'].iloc[j]):
                    ob_idx = j
                    break
            break
            
    if not displacement_idx or not ob_idx:
        result.update({
            "Type": "NEAR MISS", "Diag": "no_displacement",
            "Price": round(last_close, 2),
            "Status": f"Near 52W Low + Sweep. Waiting for Volume-Backed Displacement."
        })
        return result

    # 3. CHoCH
    swing_highs = [s for s in recent_swings if s['type'] == 'HIGH']
    choch_idx = None
    if swing_highs:
        target_high = swing_highs[-1]
        for i in range(max(0, last_idx - SETUP_WINDOW), last_idx + 1):
            if df_daily['Close'].iloc[i] > target_high['price']:
                choch_idx = i
                break
    if not choch_idx:
        result.update({
            "Type": "NEAR MISS", "Diag": "no_choch",
            "Price": round(last_close, 2),
            "Status": f"Sweep + Vol-OB formed. Waiting for CHoCH (> {target_high:.2f})."
        })
        return result

    # 4. Sequence Validation
    if not (ob_idx <= displacement_idx and displacement_idx <= choch_idx):
        result["Diag"] = "invalid_sequence"
        return result

    # 5. Trade Execution Calculation
    entry_price = last_close
    fvg_idx = None
    for i in range(max(2, last_idx - SETUP_WINDOW), last_idx + 1):
        if df_daily['Low'].iloc[i] > df_daily['High'].iloc[i - 2]:
            fvg_idx = i
            break

    if fvg_idx:
        entry_price = (df_daily['Low'].iloc[fvg_idx] + df_daily['High'].iloc[fvg_idx - 2]) / 2
    elif ob_idx:
        entry_price = (df_daily['High'].iloc[ob_idx] + df_daily['Low'].iloc[ob_idx]) / 2

    sl_price = low_52w
    if ob_idx:
        sl_price = df_daily['Low'].iloc[ob_idx] * 0.99 
    elif sweep_idx:
        sl_price = df_daily['Low'].iloc[sweep_idx] * 0.99

    risk_amount = abs(entry_price - sl_price)
    tp_price = entry_price + (risk_amount * 2.5) 
    if swing_highs:
        targets = [s['price'] for s in swing_highs if s['price'] > entry_price]
        if targets:
            tp_price = min(targets) 
    
    rr_ratio = round(abs(tp_price - entry_price) / risk_amount, 2) if risk_amount > 0 else 0
    if rr_ratio < MIN_RR_RATIO:
        result.update({
            "Type": "NEAR MISS", "Diag": "invalid_sequence",
            "Price": round(last_close, 2),
            "Status": f"Valid Daily Setup, but R:R ({rr_ratio}) too low.",
            "Entry": round(entry_price, 2), "SL": round(sl_price, 2), "Target": round(tp_price, 2), "R:R": rr_ratio
        })
        return result

    # ── CHECK 5-MINUTE LIVE TRIGGER ──
    is_5m_trigger = False
    if not df_5m.empty and 'Bullish_Displacement_5m' in df_5m.columns:
        is_5m_trigger = bool(df_5m['Bullish_Displacement_5m'].iloc[-1])
    
    reasons = [
        f"AT 52W LOW ({proximity_pct*100:.1f}%)",
        "Liquidity Sweep",
        f"Valid SMC Sequence (Vol-OB@{ob_idx}, Disp@{displacement_idx}, CHoCH@{choch_idx})",
        "CHoCH Confirmed"
    ]
    if fvg_idx: reasons.append("Bullish FVG Formed")
    if is_5m_trigger: reasons.append("⚡ 5m Entry Trigger Active")

    result.update({
        "Type": "VALID SETUP", "Diag": "valid_setups",
        "Price": round(last_close, 2), "Status": " | ".join(reasons),
        "Entry": round(entry_price, 2), "SL": round(sl_price, 2), "Target": round(tp_price, 2),
        "R:R": rr_ratio, "5m_Trigger": is_5m_trigger
    })
    return result

# ════════════════════════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ════════════════════════════════════════════════════════════════════════════════

def main():
    symbols = get_nifty500_symbols()
    
    # Determine CPU cores to use
    cpu_workers = os.cpu_count() or 4
    # Cap at 10 workers to avoid disk I/O bottlenecking if CPU count is very high
    workers = min(cpu_workers, 10)
    
    print("=" * 90)
    print(f"  PURE SMC SCANNER (ProcessPoolExecutor: {workers} cores)")
    print(f"     Local Data Directory: {UPSTOX_DATA_DIR}")
    print("=" * 90)
    print(f"  Scanning {len(symbols)} stocks...\n")

    valid_setups = []
    near_misses = []
    
    diag = {
        "total": len(symbols), "data_fails": 0, "not_near_52w_low": 0,
        "no_sweep": 0, "no_displacement": 0, "no_choch": 0, "invalid_sequence": 0, "valid_setups": 0
    }

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(scan_stock_for_smc_reversal, sym): sym for sym in symbols}
        for i, future in enumerate(as_completed(futures), 1):
            sym = futures[future]
            try:
                res = future.result()
                # Aggregate diagnostics safely in the main process
                diag_key = res.get("Diag", "data_fails")
                if diag_key in diag:
                    diag[diag_key] += 1
                
                if res["Type"] == "VALID SETUP":
                    valid_setups.append(res)
                    trigger_tag = "⚡ 5m TRIGGER" if res['5m_Trigger'] else "Daily Setup"
                    print(f"  [{i:3d}/{len(symbols)}] ⚡ {trigger_tag}: {res['Symbol']} | Entry: {res['Entry']} | SL: {res['SL']} | TP: {res['Target']} | R:R: {res['R:R']}")
                elif res["Type"] == "NEAR MISS":
                    near_misses.append(res)
            except Exception:
                diag["data_fails"] += 1

    print("\n" + "=" * 90)
    print("  DIAGNOSTIC REPORT:")
    print(f"  Total Stocks Scanned    : {diag['total']}")
    print(f"  Failed Data Fetch       : {diag['data_fails']}")
    print(f"  Not Near 52W Low        : {diag['not_near_52w_low']}")
    print(f"  No Liquidity Sweep      : {diag['no_sweep']}")
    print(f"  No Displacement/OB      : {diag['no_displacement']}")
    print(f"  No CHoCH                : {diag['no_choch']}")
    print(f"  Invalid Sequence/RR     : {diag['invalid_sequence']}")
    print(f"  Valid Strict Setups     : {diag['valid_setups']}")
    print("=" * 90)
    
    # 5. NEAR-MISS CSV EXPORT
    if near_misses:
        near_miss_file = "near_miss_watchlist.csv"
        # Keep only relevant columns for the watchlist
        nm_df = pd.DataFrame(near_misses)[['Symbol', 'Price', 'Status', 'Entry', 'SL', 'Target', 'R:R']]
        nm_df.to_csv(near_miss_file, index=False)
        
        print("\n" + "=" * 90)
        print(f"  📝 NEAR-MISS WATCHLIST ({len(near_misses)} stocks building a base)")
        print("=" * 90)
        for row in near_misses:
            print(f"  {row['Symbol']:12s} | Rs.{row['Price']:8.2f} | {row['Status']}")
            print("-" * 90)
        print(f"\n  ✅ Saved {len(near_misses)} near-miss stocks to {near_miss_file}\n")

    # PRINT VALID SETUPS
    if valid_setups:
        print("\n" + "=" * 90)
        print("  🚀 TOP PURE SMC REVERSAL CANDIDATES (READY TO TRADE)")
        print("=" * 90)
        
        valid_setups.sort(key=lambda x: (x['5m_Trigger'], x['R:R']), reverse=True)
        
        for row in valid_setups:
            print(f"  {row['Symbol']:12s} | Rs.{row['Price']:8.2f} | {'⚡ 5m LIVE TRIGGER' if row['5m_Trigger'] else 'Daily Setup Only'}")
            print(f"     👉 Entry: {row['Entry']} | 🛑 SL: {row['SL']} | 🎯 Target: {row['Target']} | 📐 R:R: 1:{row['R:R']}")
            print(f"     {row['Status']}")
            print("-" * 90)

        output_file = "nifty500_pure_smc_trade_setups.csv"
        pd.DataFrame(valid_setups).to_csv(output_file, index=False)
        print(f"\n  ✅ Saved {len(valid_setups)} trade setups to {output_file}\n")
    else:
        print("\n  ❌ No fully valid setups today. Check the Near-Miss list for tomorrow's potentials.\n")

if __name__ == "__main__":
    # Safeguard for multiprocessing on Windows
    main()