import psycopg2
import json

CONN = "postgresql://postgres.jryrijkauzlmuzcehlof:dZVAOHh1fe9RcauJ@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres"

conn = psycopg2.connect(CONN, connect_timeout=25)
cur = conn.cursor()

cur.execute("""
    SELECT instrument, MIN(timestamp), MAX(timestamp), COUNT(*)
    FROM tbt_ticks
    WHERE instrument LIKE 'NSE:NIFTY%FUT%'
    GROUP BY instrument
    ORDER BY MIN(timestamp) DESC
    LIMIT 20
""")
print("=== distinct FUT instruments (latest first) ===")
for r in cur.fetchall():
    print(r)

cur.execute("""
    SELECT MIN(timestamp), MAX(timestamp), COUNT(*)
    FROM tbt_ticks
    WHERE instrument LIKE 'NSE:NIFTY%FUT%'
      AND timestamp >= '2026-10-25 18:30:00+00'
      AND timestamp <  '2026-10-28 18:30:00+00'
""")
print("=== ticks in Oct 26-27 window (UTC) ===")
print(cur.fetchall())

# columns of tbt_ticks
cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'tbt_ticks' ORDER BY ordinal_position
""")
print("=== tbt_ticks columns ===")
print([r[0] for r in cur.fetchall()])

conn.close()
print("DONE")
