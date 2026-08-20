import requests
from bs4 import BeautifulSoup

url = 'https://www.nseindia.com/all-reports-derivatives'
headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'text/html'}
r = requests.get(url, headers=headers, timeout=20)
soup = BeautifulSoup(r.text, 'html.parser')

items = soup.select('div.reportsDownload')
print('total items', len(items))

for item in items:
    label = item.select_one('.chk_container')
    if not label:
        continue
    text = label.get_text(strip=True)
    if 'Participant wise Open Interest' in text:
        print('FOUND item')
        print(item.prettify())
        break
else:
    print('not found')
