import pandas as pd

# 1. URLs డిఫైన్ చేయడం
eq_url = "https://public.fyers.in/sym_details/NSE_CM.csv"
fo_url = "https://public.fyers.in/sym_details/NSE_FO.csv"

print("Downloading Fyers Master Files...")

# 2. Pandas ద్వారా నేరుగా CSV ఫైల్స్‌ను రీడ్ చేయడం (header=None పెట్టుకోవాలి)
df_equity = pd.read_csv(eq_url, header=None)
df_fo = pd.read_csv(fo_url, header=None)

# 3. డేటా సరిగ్గా లోడ్ అయిందో లేదో చెక్ చేయడానికి మొదటి 5 రోస్ (rows) ప్రింట్ చేయడం
print("\n--- Equity Master Data Preview ---")
print(df_equity.head())

print("\n--- F&O Master Data Preview ---")
print(df_fo.head())

# (ఆప్షనల్) భవిష్యత్తులో ఆఫ్‌లైన్‌లో వాడటానికి మీ లోకల్ సిస్టమ్‌లో సేవ్ చేసుకోవచ్చు:
df_equity.to_csv("NSE_CM.csv", index=False)
df_fo.to_csv("NSE_FO.csv", index=False)
print("\nFiles successfully downloaded and saved locally!")