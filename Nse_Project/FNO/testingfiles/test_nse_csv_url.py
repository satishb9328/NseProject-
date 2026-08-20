import requests
import datetime

for d in [datetime.date(2026,3,17), datetime.date(2026,3,16), datetime.date(2026,3,15)]:
    url = f'https://www.nseindia.com/content/nsccl/fao_participant_oi_{d.strftime("%d%m%Y")}.csv'
    try:
        r = requests.get(url, headers={'User-Agent':'Mozilla/5.0'}, timeout=20)
        print(d, r.status_code, len(r.text))
        if r.status_code == 200:
            print(r.text.splitlines()[0:3])
            break
    except Exception as e:
        print('err', e)
