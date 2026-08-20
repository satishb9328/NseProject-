import requests, datetime

d = datetime.date(2026,3,17)
url = f'https://archives.nseindia.com/content/nsccl/fao_participant_oi_{d.strftime("%d%m%Y")}.csv'
print(url)
try:
    r = requests.head(url, headers={'User-Agent':'Mozilla/5.0'}, timeout=20)
    print('status', r.status_code)
    if r.status_code == 200:
        r2 = requests.get(url, headers={'User-Agent':'Mozilla/5.0'}, timeout=20)
        print('first line', r2.text.splitlines()[0])
except Exception as e:
    print('error', e)
