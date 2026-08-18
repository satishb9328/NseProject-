import pandas as pd
import yfinance as yf
import pandas_ta as ta
import time
import json
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

warnings.filterwarnings('ignore')

# ════════════════════════════════════════════════════════════════════════════════
# CACHE CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════

CACHE_FILE = "nifty_yf_cache.json"
CACHE_EXPIRY_HOURS = 6

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_cache(cache):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        print(f"Cache save error: {e}")

def is_cache_fresh(cache, key):
    if key not in cache:
        return False
    try:
        cached_time = datetime.fromisoformat(cache[key]["ts"])
        return datetime.now() - cached_time < timedelta(hours=CACHE_EXPIRY_HOURS)
    except:
        return False

# ════════════════════════════════════════════════════════════════════════════════
# ADVANCED STRATEGY FUNCTIONS (Non-Indicator Based)
# ════════════════════════════════════════════════════════════════════════════════

def order_flow_score(hist):
    """Order Flow Imbalance — detects institutional accumulation. Max: 20"""
    try:
        h = hist.copy()
        h['tp'] = (h['High'] + h['Low'] + h['Close']) / 3
        h['mf'] = h['tp'] * h['Volume']
        h['pf'] = h['mf'].where(h['Close'] > h['Close'].shift(1), 0)
        h['nf'] = h['mf'].where(h['Close'] < h['Close'].shift(1), 0)
        pf = h['pf'].tail(14).sum()
        nf = h['nf'].tail(14).sum()
        mfr = pf / (nf + 1e-9)
        return round(min(mfr * 10, 20), 2)
    except:
        return 0

def structure_score(hist):
    """Higher Highs / Higher Lows — pure price structure. Max: 20"""
    try:
        highs = hist['High'].tail(20).values
        lows  = hist['Low'].tail(20).values
        if len(highs) < 2:
            return 0
        hh = sum(highs[i] > highs[i-1] for i in range(1, len(highs)))
        hl = sum(lows[i]  > lows[i-1]  for i in range(1, len(lows)))
        return round((hh + hl) / (2 * (len(highs) - 1)) * 20, 2)
    except:
        return 0

def vcp_score(hist):
    """Volatility Contraction Pattern (Minervini) — coiled spring. Max: 20"""
    try:
        ranges = (hist['High'] - hist['Low']).tail(20).values
        if len(ranges) < 2:
            return 0
        contractions = sum(ranges[i] < ranges[i-1] for i in range(1, len(ranges)))
        score = 15 if contractions >= 12 else 8 if contractions >= 8 else 4 if contractions >= 5 else 0
        vols = hist['Volume'].tail(10).values
        if len(vols) > 1 and vols[-1] < vols[:-1].mean() * 0.7:
            score += 5
        return min(round(score, 2), 20)
    except:
        return 0

def rs_vs_index_score(symbol_hist, index_hist, periods=[20, 60]):
    """Relative Strength vs NIFTY 50 — outperformance signal. Max: 20"""
    try:
        score = 0
        for p in periods:
            if len(symbol_hist) < p or len(index_hist) < p:
                continue
            s_ret = symbol_hist['Close'].pct_change(p).iloc[-1]
            n_ret = index_hist['Close'].pct_change(p).iloc[-1]
            diff  = s_ret - n_ret
            score += 10 if diff > 0.05 else 6 if diff > 0.02 else 3 if diff > 0 else 0
        return min(round(score, 2), 20)
    except:
        return 0

def proximity_score(hist):
    """52-Week High Proximity — breakout readiness. Max: 20"""
    try:
        if len(hist) < 252:
            return 0
        high_52w = hist['Close'].tail(252).max()
        low_52w  = hist['Close'].tail(252).min()
        last     = hist['Close'].iloc[-1]
        pfh = last / high_52w if high_52w > 0 else 0
        pfl = (last - low_52w) / (high_52w - low_52w + 1e-9)
        score  = 15 if pfh > 0.95 else 8 if pfh > 0.90 else 4 if pfh > 0.85 else 0
        score += 5  if pfl > 0.75 else 2 if pfl > 0.60 else 0
        return min(round(score, 2), 20)
    except:
        return 0

def breakout_setup_score(hist):
    """Consolidation + breakout detection. Max: 15"""
    try:
        if len(hist) < 30:
            return 0
        closes = hist['Close'].tail(30).values
        score  = 0
        recent_range = (closes[-10:].max() - closes[-10:].min()) / closes[-1]
        if recent_range < 0.05:
            score += 8
        if closes[-1] > closes[-5:-1].max():
            score += 7
        return min(round(score, 2), 15)
    except:
        return 0

def volume_trend_score(hist):
    """Volume trend — increasing volume = accumulation. Max: 15"""
    try:
        if len(hist) < 20:
            return 0
        h = hist.copy()
        avg20 = h['Volume'].tail(20).mean()
        avg60 = h['Volume'].tail(60).mean() if len(h) >= 60 else avg20
        score  = 8 if h['Volume'].iloc[-1] > avg20 * 1.3 else 4 if h['Volume'].iloc[-1] > avg20 else 0
        score += 7 if avg20 > avg60 else 3 if avg20 > avg60 * 0.9 else 0
        return min(round(score, 2), 15)
    except:
        return 0

# ════════════════════════════════════════════════════════════════════════════════
# DATA FETCH WITH RETRY + CACHE
# ════════════════════════════════════════════════════════════════════════════════

def fetch_stock_data(symbol, cache, nifty_hist):
    if is_cache_fresh(cache, symbol):
        cached = cache[symbol]["data"]
        try:
            hist = yf.Ticker(symbol + ".NS").history(period="1y")
            if not hist.empty and len(hist) >= 20:
                cached.update({
                    "order_flow":     order_flow_score(hist),
                    "structure":      structure_score(hist),
                    "vcp":            vcp_score(hist),
                    "rs_vs_index":    rs_vs_index_score(hist, nifty_hist),
                    "proximity":      proximity_score(hist),
                    "breakout_setup": breakout_setup_score(hist),
                    "volume_trend":   volume_trend_score(hist),
                })
        except:
            pass
        return cached

    for attempt in range(3):
        try:
            ticker = yf.Ticker(symbol + ".NS")
            info   = ticker.info
            hist   = ticker.history(period="1y")

            if hist.empty or len(hist) < 50:
                return None
            if not info.get("regularMarketPrice") and not info.get("currentPrice"):
                return None

            close  = hist['Close']
            high   = hist['High']
            low    = hist['Low']
            volume = hist['Volume']

            rsi     = ta.rsi(close, length=14)
            macd_df = ta.macd(close)
            sma50   = ta.sma(close, length=50)
            sma200  = ta.sma(close, length=200)
            adx_df  = ta.adx(high, low, close, length=14)
            bb      = ta.bbands(close, length=20, std=2)
            atr     = ta.atr(high, low, close, length=14)

            last_price  = info.get("regularMarketPrice") or info.get("currentPrice")
            last_sma200 = sma200.iloc[-1]
            avg_vol_20  = volume.tail(20).mean()

            pct_above_sma200 = (
                ((last_price - last_sma200) / last_sma200) * 100
                if pd.notna(last_sma200) and last_sma200 > 0 else None
            )

            def safe(val):
                return round(float(val), 4) if val is not None and pd.notna(val) else None

            result = {
                "symbol":           symbol,
                "lastPrice":        round(last_price, 2),
                "peRatio":          round(info.get("trailingPE") or info.get("forwardPE") or 0, 2),
                "eps":              round(info.get("trailingEps") or 0, 2),
                "bookValue":        round(info.get("bookValue") or 0, 2),
                "marketCap":        info.get("marketCap") or 0,
                "roe":              round((info.get("returnOnEquity") or 0) * 100, 2),
                "debtToEquity":     round(info.get("debtToEquity") or 0, 2),
                "revenueGrowth":    round((info.get("revenueGrowth") or 0) * 100, 2),
                "earningsGrowth":   round((info.get("earningsQuarterlyGrowth") or 0) * 100, 2),
                "sector":           info.get("sector", "Unknown"),
                "industry":         info.get("industry", "Unknown"),
                "RSI":              safe(rsi.iloc[-1]),
                "MACD":             safe(macd_df['MACD_12_26_9'].iloc[-1]) if macd_df is not None else None,
                "MACD_Signal":      safe(macd_df['MACDs_12_26_9'].iloc[-1]) if macd_df is not None else None,
                "MACD_Hist":        safe(macd_df['MACDh_12_26_9'].iloc[-1]) if macd_df is not None else None,
                "SMA50":            safe(sma50.iloc[-1]),
                "SMA200":           safe(last_sma200),
                "Pct_Above_SMA200": safe(pct_above_sma200),
                "ADX":              safe(adx_df['ADX_14'].iloc[-1]) if adx_df is not None else None,
                "BB_Width":         safe((bb['BBU_20_2.0_2.0'] - bb['BBL_20_2.0_2.0']).iloc[-1] / bb['BBM_20_2.0_2.0'].iloc[-1]),
                "ATR":              safe(atr.iloc[-1]),
                "Volume":           int(volume.iloc[-1]),
                "AvgVolume20":      int(avg_vol_20),
                # ── Advanced Strategy Scores ──────────────────────────────────
                "order_flow":       order_flow_score(hist),
                "structure":        structure_score(hist),
                "vcp":              vcp_score(hist),
                "rs_vs_index":      rs_vs_index_score(hist, nifty_hist),
                "proximity":        proximity_score(hist),
                "breakout_setup":   breakout_setup_score(hist),
                "volume_trend":     volume_trend_score(hist),
            }

            cache[symbol] = {"ts": datetime.now().isoformat(), "data": result}
            save_cache(cache)
            return result

        except Exception as e:
            wait = 2 ** attempt
            if attempt < 2:
                print(f"      {symbol}: attempt {attempt+1} failed ({str(e)[:50]}) — retry in {wait}s")
                time.sleep(wait)
            else:
                print(f"      {symbol}: failed after 3 attempts")
                return None

    return None

# ════════════════════════════════════════════════════════════════════════════════
# SCORING ENGINE  (raw max ~150, normalized to 100)
# ════════════════════════════════════════════════════════════════════════════════

def score_stock_advanced(row):
    score   = 0
    reasons = []

    # ── FUNDAMENTALS (20 pts) ────────────────────────────────────────────────────
    pe = row.get("peRatio")
    if pe and pe > 0:
        if   5  < pe < 20: score += 10; reasons.append(f"PE={pe:.1f} (deep value)")
        elif 20 <= pe < 30: score +=  7; reasons.append(f"PE={pe:.1f} (fair value)")
        elif 30 <= pe < 50: score +=  4; reasons.append(f"PE={pe:.1f} (growth premium)")

    roe = row.get("roe")
    if roe and roe > 0:
        if   roe > 20: score += 10; reasons.append(f"ROE={roe:.1f}% (excellent)")
        elif roe > 15: score +=  7; reasons.append(f"ROE={roe:.1f}% (good)")
        elif roe > 10: score +=  4; reasons.append(f"ROE={roe:.1f}% (decent)")

    # ── TRADITIONAL TECHNICALS (25 pts) ──────────────────────────────────────────
    rsi = row.get("RSI")
    if rsi:
        if   50 < rsi < 70: score += 10; reasons.append(f"RSI={rsi:.1f} (momentum)")
        elif 40 < rsi <= 50: score +=  6; reasons.append(f"RSI={rsi:.1f} (healthy)")
        elif 30 < rsi <= 40: score +=  4; reasons.append(f"RSI={rsi:.1f} (recovering)")

    macd     = row.get("MACD")
    macd_sig = row.get("MACD_Signal")
    if macd and macd_sig:
        if   macd > macd_sig and macd > 0: score += 10; reasons.append("MACD bullish+positive")
        elif macd > macd_sig:              score +=  6; reasons.append("MACD bullish crossover")

    adx = row.get("ADX")
    if adx:
        if   adx > 30: score += 5; reasons.append(f"ADX={adx:.1f} (strong trend)")
        elif adx > 25: score += 3; reasons.append(f"ADX={adx:.1f} (trend forming)")

    # ── ADVANCED STRATEGIES (85 pts) ─────────────────────────────────────────────
    of  = row.get("order_flow",     0)
    st  = row.get("structure",      0)
    vcp = row.get("vcp",            0)
    rs  = row.get("rs_vs_index",    0)
    prx = row.get("proximity",      0)
    bs  = row.get("breakout_setup", 0)
    vt  = row.get("volume_trend",   0)

    score += of + st + vcp + rs + prx + bs + vt

    if of  > 10: reasons.append(f"OFI={of:.0f} (institutional buying)")
    if st  > 12: reasons.append(f"Structure={st:.0f} (HH/HL uptrend)")
    if vcp > 10: reasons.append(f"VCP={vcp:.0f} (coiling/breakout)")
    if rs  > 10: reasons.append(f"RS={rs:.0f} (beating NIFTY)")
    if prx > 10: reasons.append(f"Proximity={prx:.0f} (near 52wHi)")
    if bs  >  8: reasons.append(f"Breakout={bs:.0f} (pattern setup)")
    if vt  >  6: reasons.append(f"VolTrend={vt:.0f} (accumulation)")

    row["score"]   = min(round(score / 1.5, 1), 100)
    row["reasons"] = " | ".join(reasons) if reasons else "No strong signals"
    return row

# ════════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════════

def main():
    symbols = list(dict.fromkeys([
        "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR", "ITC",
        "SBIN", "BHARTIARTL", "KOTAKBANK", "BAJFINANCE", "LT", "WIPRO", "AXISBANK",
        "TITAN", "ASIANPAINT", "MARUTI", "SUNPHARMA", "HCLTECH", "NTPC",
        "ONGC", "POWERGRID", "ULTRACEMCO", "BAJAJFINSV", "ADANIPORTS",
        "TATAMOTORS", "NESTLEIND", "JSWSTEEL", "TATASTEEL", "TECHM",
        "HDFCLIFE", "SBILIFE", "COALINDIA", "BRITANNIA", "GRASIM",
        "EICHERMOT", "DIVISLAB", "DRREDDY", "CIPLA", "BPCL",
        "HINDALCO", "IOC", "HEROMOTOCO", "BAJAJ-AUTO", "UPL",
        "SHREECEM", "TATACONSUM", "ADANIGREEN", "ADANIENT", "TRENT",
        "DABUR", "PIDILITIND", "MARICO", "HAVELLS", "ICICIPRULI",
        "ICICIGI", "HDFCAMC", "INDUSINDBK", "BANDHANBNK", "NAUKRI",
        "DLF", "GODREJCP", "COLPAL", "AMBUJACEM", "SIEMENS",
        "BIOCON", "PAGEIND", "MRF", "BOSCHLTD", "HAL",
        "BEL", "BHEL", "GAIL", "PETRONET", "LICHSGFIN",
        "CHOLAFIN", "VOLTAS", "INDIGO", "JUBLFOOD", "BATAINDIA",
        "APOLLOHOSP", "APOLLOTYRE", "ASHOKLEY", "BANKBARODA", "PNB",
        "IDFCFIRSTB", "FEDERALBNK", "IRCTC", "TVSMOTOR", "VEDL",
        "SAIL", "TATAPOWER", "LUPIN", "AUROPHARMA", "ALKEM",
        "TORNTPHARM", "PFIZER", "SANOFI", "ABBOTINDIA", "GLAXO",
        "NOVARTISIND", "M&MFIN", "HINDZINC", "NATIONALUM",
        "JINDALSTEL", "ADANITRANS", "CESC", "NHPC", "LTI",
        "MINDTREE", "CADILAHC", "CUMMINSIND", "ABB",
        "EXIDEIND", "CANBK", "UNIONBANK", "RBLBANK",
        "AUBANK", "YESBANK", "IEX", "PFC", "RECLTD",
        "CONCOR", "ADANIPOWER", "ABCAPITAL",
        "TORNTPOWER", "GLENMARK", "M&M", "AVENUE", "SRTRANSFIN",
    ]))

    cache = load_cache()
    final_data = []

    print("\n" + "="*90)
    print("  ** NIFTY 200 ADVANCED SCREENING ENGINE v4.0")
    print("     Strategies: OFI | HH/HL Structure | VCP | RS vs NIFTY | 52wHi Proximity")
    print("="*90)
    print(f"  Scanning {len(symbols)} stocks | 5 parallel workers | cache={CACHE_EXPIRY_HOURS}h\n")

    print("  Fetching NIFTY 50 for relative strength...")
    try:
        nifty_hist = yf.Ticker("^NSEI").history(period="1y")
        if nifty_hist.empty:
            print("  !! NIFTY 50 unavailable -- RS scores will be 0")
            nifty_hist = pd.DataFrame()
    except Exception as e:
        print(f"  !! NIFTY fetch failed: {e}")
        nifty_hist = pd.DataFrame()

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_stock_data, sym, cache, nifty_hist): sym for sym in symbols}
        for i, future in enumerate(as_completed(futures), 1):
            sym    = futures[future]
            result = future.result()
            if result is None:
                print(f"  [{i:3d}/{len(symbols)}] {sym:15s} x SKIPPED")
                continue
            result = score_stock_advanced(result)
            final_data.append(result)
            print(
                f"  [{i:3d}/{len(symbols)}] {sym:15s} | "
                f"Score:{result['score']:6.1f}/100 | "
                f"Rs.{result['lastPrice']:8.2f} | "
                f"OFI:{result.get('order_flow',0):5.1f} "
                f"Str:{result.get('structure',0):5.1f} "
                f"VCP:{result.get('vcp',0):5.1f} "
                f"RS:{result.get('rs_vs_index',0):5.1f} "
                f"Prx:{result.get('proximity',0):5.1f}"
            )

    if not final_data:
        print("\n!! No data collected. Check internet connection.")
        return

    df = pd.DataFrame(final_data).sort_values("score", ascending=False)
    df.to_csv("nifty200_advanced_strategy_screen.csv", index=False)

    print("\n" + "="*90)
    print(f"  TOP 20 STOCKS\n")
    for rank, (_, row) in enumerate(df.head(20).iterrows(), 1):
        print(f"  {rank:2d}. {row['symbol']:12s} Rs.{row['lastPrice']:8.2f} | "
              f"Score:{row['score']:6.1f} | PE:{row['peRatio']:6.1f} | ROE:{row['roe']:5.1f}%")
        print(f"      {row['reasons'][:110]}")
        print()

    print("="*90)
    print(f"  Saved: nifty200_advanced_strategy_screen.csv  ({len(df)} stocks)\n")
    print(f"  Score Distribution:")
    print(f"    Elite    80-100 : {len(df[df['score'] >= 80]):3d} stocks")
    print(f"    Strong   65-79  : {len(df[(df['score'] >= 65) & (df['score'] < 80)]):3d} stocks")
    print(f"    Moderate 50-64  : {len(df[(df['score'] >= 50) & (df['score'] < 65)]):3d} stocks")
    print(f"    Weak     35-49  : {len(df[(df['score'] >= 35) & (df['score'] < 50)]):3d} stocks")
    print(f"    No setup  <35   : {len(df[df['score'] < 35]):3d} stocks\n")
    print(f"  Best combo: RS >= 10 + VCP >= 10 + Proximity >= 10 = Minervini SEPA setup")
    print(f"     Entry trigger: breakout candle on volume > 1.5x average")
    print("="*90 + "\n")


if __name__ == "__main__":
    main()
