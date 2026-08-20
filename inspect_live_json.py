import json
from pathlib import Path
path = Path('live_options_data.json')
text = path.read_text(encoding='utf-8')
print('length', len(text))
print('endswith braces', text.strip().endswith('}'))
print('last 5 lines:')
lines = text.splitlines()
for line in lines[-10:]:
    print(line)
try:
    data = json.loads(text)
    print('PARSED OK')
    print('updated_at', data.get('updated_at'))
    print('expiry_date', data.get('expiry_date'))
    print('instrument_data keys', list(data.get('instrument_data', {}).keys())[:10])
except Exception as e:
    print('PARSE ERROR', repr(e))
    idx = getattr(e, 'pos', None)
    if idx is not None:
        print('error pos', idx)
        window = 50
        start = max(0, idx-window)
        end = min(len(text), idx+window)
        print('context:', text[start:end])
