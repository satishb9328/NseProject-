import pandas as pd
import yfinance as yf
import pandas_ta as ta
import time

# Step 1: Define a list of major Nifty 200 stocks (with .NS suffix for Yahoo Finance)
# Using a comprehensive list of Nifty 200 stocks
symbols = [
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR", "ITC",
    "SBIN", "BHARTIARTL", "KOTAKBANK", "BAJFINANCE", "LT", "WIPRO", "AXISBANK",
    "TITAN", "ASIANPAINT", "MARUTI", "SUNPHARMA", "HCLTECH", "NTPC",
    "ONGC", "POWERGRID", "ULTRACEMCO", "BAJAJFINSV", "ADANIPORTS",
    "M&M", "TATAMOTORS", "NESTLEIND", "JSWSTEEL", "TATASTEEL",
    "TECHM", "HDFCLIFE", "SBILIFE", "COALINDIA", "BRITANNIA",
    "GRASIM", "EICHERMOT", "DIVISLAB", "DRREDDY", "CIPLA",
    "BPCL", "HINDALCO", "IOC", "HEROMOTOCO", "BAJAJ-AUTO",
    "UPL", "SHREECEM", "TATACONSUM", "ADANIGREEN", "ADANIENT",
    "TRENT", "AVENUE", "DABUR", "PIDILITIND", "MARICO",
    "HAVELLS", "ICICIPRULI", "ICICIGI", "HDFCAMC", "SRTRANSFIN",
    "BERGEPAINT", "TORNTPHARM", "MUTHOOTFIN", "INDUSINDBK", "BANDHANBNK",
    "NAUKRI", "DLF", "GODREJCP", "COLPAL", "AMBUJACEM",
    "SIEMENS", "LTI", "MINDTREE", "BIOCON", "CADILAHC",
    "PAGEIND", "MRF", "BOSCHLTD", "CUMMINSIND", "ABB",
    "HAL", "BEL", "BHEL", "GAIL", "PETRONET",
    "LICHSGFIN", "CHOLAFIN", "MCDOWELL-N", "PEL", "VOLTAS",
    "INDIGO", "INTERGLBE", "JUBLFOOD", "BATAINDIA", "TATACOMM",
    "MOTHERSUMI", "EXIDEIND", "APOLLOHOSP", "APOLLOTYRE", "ASHOKLEY",
    "BANKBARODA", "CANBK", "PNB", "UNIONBANK", "IDFCFIRSTB",
    "FEDERALBNK", "RBLBANK", "AUBANK", "YESBANK", "IEX",
    "PFC", "RECLTD", "IRCTC", "CONCOR", "ADANIPOWER",
    "TVSMOTOR", "EICHERMOT", "ABCAPITAL", "M&MFIN", "HINDZINC",
    "VEDL", "NATIONALUM", "SAIL", "JINDALSTEL", "ADANITRANS",
    "TORNTPOWER", "CESC", "TATAPOWER", "NHPC", "SIEMENS",
    "LUPIN", "AUROPHARMA", "GLENMARK", "ALKEM", "TORNTPHARM",
    "PFIZER", "SANOFI", "ABBOTINDIA", "GLAXO", "NOVARTISIND"
]

# Step 2: Fetch fundamentals using yfinance
def fetch_fundamentals(symbol):
    try:
        stock = yf.Ticker(symbol + ".NS")
        info = stock.info
        if not info or info.get('regularMarketPrice') is None:
            return None
        return {
            "symbol": symbol,
            "lastPrice": info.get('regularMarketPrice', info.get('currentPrice', None)),
            "peRatio": info.get('trailingPE', info.get('forwardPE', None)),
            "eps": info.get('trailingEps', None),
            "bookValue": info.get('bookValue', None),
            "marketCap": info.get('marketCap', None),
            "sector": info.get('sector', None),
            "industry": info.get('industry', None)
        }
    except Exception as e:
        print(f"  Error fetching fundamentals for {symbol}: {e}")
        return None

# Step 3: Fetch historical data + add technical indicators
def fetch_technicals(symbol):
    try:
        stock = yf.Ticker(symbol + ".NS")
        hist = stock.history(period="6mo")
        if hist.empty or len(hist) < 20:
            return None
        hist['RSI'] = ta.rsi(hist['Close'], length=14)
        macd_result = ta.macd(hist['Close'])
        if macd_result is not None and 'MACD_12_26_9' in macd_result.columns:
            hist['MACD'] = macd_result['MACD_12_26_9']
        else:
            hist['MACD'] = None
        hist['SMA50'] = ta.sma(hist['Close'], length=50)
        hist['SMA200'] = ta.sma(hist['Close'], length=200)
        latest = hist.tail(1).iloc[0]
        return {
            "RSI": latest.get('RSI', None),
            "MACD": latest.get('MACD', None),
            "SMA50": latest.get('SMA50', None),
            "SMA200": latest.get('SMA200', None)
        }
    except Exception as e:
        print(f"  Error fetching technicals for {symbol}: {e}")
        return None

# Step 4: Scoring function
def score_stock(row):
    score = 0
    # Fundamentals
    if row['peRatio'] is not None and row['peRatio'] < 25: score += 10
    if row['eps'] is not None and row['eps'] > 10: score += 10
    if row['marketCap'] is not None and row['marketCap'] > 10000: score += 20
    # Technicals
    if row['RSI'] is not None and 40 < row['RSI'] < 70: score += 15
    if row['MACD'] is not None and row['MACD'] > 0: score += 15
    if row['SMA50'] is not None and row['SMA200'] is not None and row['SMA50'] > row['SMA200']: score += 10
    # Sector bonus
    if row['sector'] in ['Technology', 'Financial Services', 'Banking']: score += 10
    return score

# Step 5: Combine everything
final_data = []
limit = min(20, len(symbols))
print(f"Processing first {limit} stocks from Nifty 200...\n")
for i, symbol in enumerate(symbols[:limit]):
    print(f"[{i+1}/{limit}] Processing {symbol}...")
    fundamentals = fetch_fundamentals(symbol)
    if fundamentals is None:
        print(f"  Skipping {symbol} (no fundamentals data)")
        continue
    technicals = fetch_technicals(symbol)
    if technicals is None:
        print(f"  Skipping {symbol} (no technical data)")
        continue
    combined = {**fundamentals, **technicals}
    combined['score'] = score_stock(combined)
    final_data.append(combined)
    print(f"  Price: {combined['lastPrice']}, PE: {combined['peRatio']}, Score: {combined['score']}")
    time.sleep(0.5)  # Rate limiting for Yahoo Finance

if not final_data:
    print("\nNo data collected. Check internet connectivity.")
else:
    final_df = pd.DataFrame(final_data)
    ranked_df = final_df.sort_values(by='score', ascending=False)

    # Step 6: Save results
    output_path = "nifty200_screening.csv"
    ranked_df.to_csv(output_path, index=False)
    print(f"\n{'='*60}")
    print(f"Results saved to {output_path}")
    print(f"{'='*60}")
    print("\nTop 10 stocks by score:")
    print(ranked_df[['symbol', 'lastPrice', 'peRatio', 'RSI', 'MACD', 'score']].head(10).to_string(index=False))
    print(f"\nFull results ({len(final_data)} stocks) saved to CSV.")
