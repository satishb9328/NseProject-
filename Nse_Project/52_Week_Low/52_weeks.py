"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║   NIFTY 200 — SMC & PRICE ACTION REVERSAL SCANNER                                ║
║   Finds stocks at 52-week lows showing Liquidity Sweeps, FVGs, and MSS.          ║
╚══════════════════════════════════════════════════════════════════════════════════╝
"""

import pandas as pd
import yfinance as yf
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import warnings

warnings.filterwarnings("ignore")

# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════
MAX_WORKERS = 5
SCAN_UNIVERSE = "NIFTY_200"

# ════════════════════════════════════════════════════════════════════════════════
# DATA FETCHING
# ════════════════════════════════════════════════════════════════════════════════

def get_nifty200_symbols():
    print("  Fetching latest NIFTY 200 list from NSE...")
    try:
        url = "https://archives.nseindia.com/content/indices/ind_nifty200list.csv"
        # Stronger headers to prevent NSE from blocking the request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        response = requests.get(url, headers=headers, timeout=10)
        from io import StringIO
        df = pd.read_csv(StringIO(response.text))
        symbols = df['Symbol'].tolist()
        print(f"  Successfully loaded {len(symbols)} stocks.\n")
        return symbols
    except Exception as e:
        print(f"  Failed to fetch NSE list: {e}. Using fallback list.")
        return ["RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "SBIN", "LTIM", "ITC"]

# ════════════════════════════════════════════════════════════════════════════════
# SMC SCORING ENGINE
# ════════════════════════════════════════════════════════════════════════════════

def scan_stock_for_spring(symbol: str) -> dict | None:
    try:
        # Download 2 years of data to ensure rolling 252-day calculations work accurately for the current day
        df = yf.Ticker(f"{symbol}.NS").history(period="2y", interval="1d")
        df.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'], inplace=True)
        if df.empty or len(df) < 260:
            return None

        last_close = df['Close'].iloc[-1]
        
        # 1. Identify 52-Week Low Proximity
        # Get the minimum low of the last 252 trading days
        low_52w = df['Low'].tail(252).min()
        
        proximity_pct = (last_close - low_52w) / low_52w
        if proximity_pct > 0.10:  # > 10% away from low, skip it
            return None

        # 2. SMC: Liquidity Sweep (Stop Hunt)
        # Did the price drop below the older 52-week low recently but close back above it?
        old_52w_low = df['Low'].shift(3).rolling(252).min().iloc[-1]
        recent_low = df['Low'].tail(3).min()
        liquidity_sweep = (recent_low < old_52w_low) and (last_close > old_52w_low)

        # 3. SMC: Bullish Fair Value Gap (FVG)
        # Formed when Low of current candle > High of candle 2 days ago
        df['Bullish_FVG'] = df['Low'] > df['High'].shift(2)
        recent_fvg = df['Bullish_FVG'].tail(5).any() # Check if FVG formed in the last 5 days

        # 4. SMC: Market Structure Shift (MSS)
        # Identify swing highs (higher than prev and next day)
        is_swing_high = (df['High'] > df['High'].shift(1)) & (df['High'] > df['High'].shift(-1))
        df['Swing_High'] = np.where(is_swing_high, df['High'], np.nan)
        df['Swing_High'] = df['Swing_High'].ffill() # Forward fill to track the active swing high
        
        last_swing_high = df['Swing_High'].iloc[-2] # Compare against yesterday's active swing high
        mss_breakout = (last_close > last_swing_high) if pd.notna(last_swing_high) else False

        # 5. Corrected Hammer / Reversal Candle Logic
        last_candle = df.iloc[-1]
        body = abs(last_candle['Close'] - last_candle['Open'])
        lower_wick = min(last_candle['Open'], last_candle['Close']) - last_candle['Low']
        upper_wick = last_candle['High'] - max(last_candle['Open'], last_candle['Close'])
        
        is_hammer = (body > 0) and (lower_wick >= 2 * body) and (upper_wick < body)

        # 6. Volume Exhaustion Check
        vol_ma60 = df['Volume'].rolling(60).mean().iloc[-1]
        last_vol = df['Volume'].iloc[-1]
        vol_dry_up = last_vol < (vol_ma60 * 0.7)

        # ── SCORING SYSTEM ──
        score = 0
        reasons = []

        if proximity_pct <= 0.03:
            score += 3; reasons.append("AT 52W LOW (<3%)")
        elif proximity_pct <= 0.05:
            score += 2; reasons.append("NEAR 52W LOW (<5%)")
        else:
            score += 1; reasons.append("CLOSE TO 52W LOW (<10%)")

        if liquidity_sweep:
            score += 3; reasons.append("Liquidity Sweep (Stop Hunt)")

        if recent_fvg:
            score += 2; reasons.append("Bullish FVG Formed")

        if mss_breakout:
            score += 3; reasons.append("MSS Confirmed (Broke Swing High)")

        if is_hammer:
            score += 2; reasons.append("Hammer Candle")
            
        if vol_dry_up:
            score += 1; reasons.append("Volume Exhausted")

        # Minimum score threshold for a valid setup (Adjustable based on strictness)
        if score >= 3:
            return {
                "Symbol": symbol,
                "Price": round(last_close, 2),
                "52W_Low": round(low_52w, 2),
                "Proximity_%": round(proximity_pct * 100, 2),
                "Score": score,
                "Signal": "🚀 SMC REVERSAL SETUP",
                "Reasons": " | ".join(reasons)
            }

    except Exception as e:
        # Uncomment the line below if you want to see exactly why a specific stock failed to process
        # print(f"Error processing {symbol}: {e}")
        pass

    return None

# ════════════════════════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ════════════════════════════════════════════════════════════════════════════════

def main():
    symbols = get_nifty200_symbols()
    
    print("=" * 100)
    print("  ★  NIFTY 200 — SMC ACCUMULATION SCANNER")
    print("     Scanning for Liquidity Sweeps + FVGs + Market Structure Shifts")
    print("=" * 100)
    print(f"  Scanning {len(symbols)} stocks with {MAX_WORKERS} parallel workers...\n")

    results = []

    # Parallel execution
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scan_stock_for_spring, sym): sym for sym in symbols}
        
        for i, future in enumerate(as_completed(futures), 1):
            sym = futures[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
                    print(f"  [{i:3d}/{len(symbols)}] ⚡ SETUP FOUND: {result['Symbol']} | Score: {result['Score']}/14 | {result['Reasons']}")
            except Exception:
                pass

    # Sort by highest score
    results.sort(key=lambda x: x['Score'], reverse=True)

    print("\n" + "=" * 100)
    print("  TOP SMC REVERSAL CANDIDATES AT 52-WEEK LOWS")
    print("=" * 100)
    
    if not results:
        print("  ❌ No stocks met the SMC criteria for a 52W Low Reversal today.")
        print("=" * 100 + "\n")
        return

    df_results = pd.DataFrame(results)
    
    for index, row in df_results.iterrows():
        print(f"  {index+1}. {row['Symbol']:12s} | Rs.{row['Price']:8.2f} | 52wLow: Rs.{row['52W_Low']:8.2f} ({row['Proximity_%']}%)")
        print(f"     Score: {row['Score']}/14 | {row['Reasons']}")
        print("-" * 100)

    output_file = "nifty200_smc_reversals.csv"
    df_results.to_csv(output_file, index=False)
    print(f"\n  ✅ Saved {len(df_results)} setups to {output_file}\n")

if __name__ == "__main__":
    main()