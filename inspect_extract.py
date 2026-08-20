import json
from pathlib import Path
from Nse_Project.Live_dashboard import upstox_live_feed as feed

path = Path('live_options_data.json')
text = path.read_text(encoding='utf-8')
data = json.loads(text)
for k, v in list(data.get('instrument_data', {}).items())[:5]:
    raw = v.get('raw_sample')
    values = feed.extract_live_values(raw, instrument_key=k)
    print('KEY', k)
    print('EXTRACT', values)
    print('---')
