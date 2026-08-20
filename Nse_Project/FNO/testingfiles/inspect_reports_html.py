import requests

url = 'https://www.nseindia.com/all-reports-derivatives'
headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'text/html'}
html = requests.get(url, headers=headers, timeout=20).text

import re

for term in ['content/nsccl', 'content/fo', 'archives', 'download', 'csv', 'participant']:
    idx = html.lower().find(term)
    print(term, 'found', idx != -1)
    if idx != -1:
        start = max(0, idx-200)
        end = idx+400
        snippet = html[start:end]
        print('--- snippet ---')
        print(snippet)
        print('--- end snippet ---\n')
