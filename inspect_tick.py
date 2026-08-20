import json
from pathlib import Path

path = Path('live_options_data.json')
text = path.read_text(encoding='utf-8')
data = json.loads(text)
count = 0
for k, v in list(data.get('instrument_data', {}).items())[:5]:
    print('KEY', k)
    raw = v.get('raw_sample', {})
    print('FIELDS', sorted(v.keys()))
    print('raw_sample type', type(raw))
    def recurse(o, prefix=''):
        if isinstance(o, dict):
            for kk, vv in o.items():
                new_prefix = f'{prefix}{kk}' if prefix else kk
                if isinstance(vv, (dict, list)):
                    recurse(vv, new_prefix + '.')
                else:
                    if any(tok in new_prefix.lower() for tok in ['oi', 'coi', 'strike', 'ltp', 'volume', 'vtt']):
                        print('  ', new_prefix, '->', vv)
        elif isinstance(o, list):
            for i, item in enumerate(o):
                recurse(item, f'{prefix}{i}.')
    recurse(raw)
    print('---')
    count += 1
if count == 0:
    print('NO ITEMS')
