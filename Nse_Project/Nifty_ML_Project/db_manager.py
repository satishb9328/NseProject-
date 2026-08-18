# db_manager.py
import psycopg2
import pandas as pd
import config

def get_fyers_token():
    """Reads the Fyers token from the Supabase config table."""
    print("🔑 Fetching Fyers token from Supabase...")
    conn = psycopg2.connect(config.DB_CONNECTION_STRING)
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM config WHERE key = 'fyers_access_token'")
        row = cur.fetchone()
    conn.close()
    return row[0] if row else None

def fetch_raw_ticks():
    """Downloads all TBT Futures data from Supabase."""
    print("🔌 Connecting to Supabase to download TBT data...")
    conn = psycopg2.connect(config.DB_CONNECTION_STRING)
    query = """
        SELECT timestamp, ltp, bid_price, ask_price, total_buy_qty, total_sell_qty, volume 
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