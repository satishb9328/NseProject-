import requests
from bs4 import BeautifulSoup

url = 'https://www.nseindia.com/all-reports-derivatives'
headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'text/html'}

r = requests.get(url, headers=headers, timeout=20)
print('status', r.status_code)

soup = BeautifulSoup(r.text, 'html.parser')
links = []
for a in soup.find_all('a', href=True):
    href = a['href']
    text = a.get_text(strip=True)
    if 'csv' in text.lower() or href.lower().endswith('.csv'):
        links.append((text, href))

for text, href in links:
    print(text, href)

print('total', len(links))
