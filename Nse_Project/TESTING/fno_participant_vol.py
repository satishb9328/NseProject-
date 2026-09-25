import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta

# గత 3 సంవత్సరాల తేదీలను సెట్ చేయడం
end_date = datetime.today()
start_date = end_date - timedelta(days=3*365)

# కేవలం వర్కింగ్ డేస్ (Business days) మాత్రమే క్రియేట్ చేయడం
dates = pd.bdate_range(start=start_date, end=end_date)

# మీరు ఇచ్చిన కొత్త డైరెక్టరీ పాత్
folder_name = r'D:\FNO DATA\Participant data\Volume'

# ఒకవేళ ఆ ఫోల్డర్స్ ముందుగా లేకపోతే, పైథాన్ వాటిని ఆటోమేటిక్‌గా క్రియేట్ చేస్తుంది
os.makedirs(folder_name, exist_ok=True)

# NSE మనల్ని బ్లాక్ చేయకుండా బ్రౌజర్ లాగా యాక్ట్ చేయడానికి Headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive'
}

# సెషన్ స్టార్ట్ చేయడం
session = requests.Session()
session.headers.update(headers)

print("Initializing session with NSE...")
try:
    session.get("https://www.nseindia.com", timeout=10)
    time.sleep(2)
except Exception as e:
    print(f"Session initialization failed: {e}")

# ఒక్కో రోజుకి సంబంధించిన ఫైల్ డౌన్‌లోడ్ ప్రాసెస్
for d in dates:
    date_str = d.strftime('%d%m%Y')
    url = f"https://nsearchives.nseindia.com/content/nsccl/fao_participant_vol_{date_str}.csv"
    
    try:
        response = session.get(url, timeout=10)
        
        if response.status_code == 200:
            # os.path.join ఉపయోగించి ఫోల్డర్ పాత్ మరియు ఫైల్ పేరును కలుపుతున్నాం
            file_path = os.path.join(folder_name, f"fao_participant_vol_{date_str}.csv")
            
            with open(file_path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded: {date_str}")
        else:
            print(f"No Data for {date_str} (Status: {response.status_code})")
            
    except Exception as e:
        print(f"Error downloading data for {date_str}: {e}")
        
    time.sleep(1.5)
    
print(f"All possible files have been successfully downloaded to: {folder_name}")