import requests
import json
import gzip
import pandas as pd

# Upstox అధికారిక NSE ఇన్‌స్ట్రుమెంట్స్ లింక్ (JSON.gz ఫార్మాట్)
url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

print("Upstox నుండి ఇన్‌స్ట్రుమెంట్స్ డేటాను డౌన్‌లోడ్ చేస్తోంది...")
response = requests.get(url)

if response.status_code == 200:
    # Gzip ఫైల్‌ని డీకంప్రెస్ చేసి JSON డేటాగా మార్చడం
    decompressed_data = gzip.decompress(response.content)
    instruments = json.loads(decompressed_data.decode('utf-8'))
    
    # Pandas డేటాఫ్రేమ్‌లోకి మార్చడం
    df = pd.DataFrame(instruments)
    
    # కేవలం ఈక్విటీ (NSE_EQ) స్టాక్స్ మాత్రమే ఫిల్టర్ చేయడం
    df_eq = df[df['segment'] == 'NSE_EQ']
    
    print(f"మొత్తం ఈక్విటీ స్టాక్స్ లభించాయి: {len(df_eq)}")
    
    # ఉదాహరణకు కొన్ని స్టాక్స్ చూద్దాం
    print(df_eq[['trading_symbol', 'instrument_key', 'isin']].head())
    
    # మీరు దీన్ని CSV లేదా JSON రూపంలో కూడా సేవ్ చేసుకోవచ్చు
    # df_eq.to_csv('nse_equity_instruments.csv', index=False)
else:
    print("డేటా డౌన్‌లోడ్ చేయడంలో విఫలమైంది. ఎర్రర్ కోడ్:", response.status_code)