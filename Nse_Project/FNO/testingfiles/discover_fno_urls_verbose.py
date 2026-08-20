import requests, datetime

templates = [
    'https://www.nseindia.com/content/nsccl/fao_participant_oi_{date}.csv',
    'https://www.nseindia.com/content/nsccl/participant_oi_{date}.csv',
    'https://www.nseindia.com/reports/archives/derivatives/participant_oi_{date}.csv',
    'https://www.nseindia.com/reports/archives/derivatives/fao_participant_oi_{date}.csv',
    'https://www.nseindia.com/reports/archives/derivatives/participant_wise_oi_{date}.csv',
    'https://www.nseindia.com/reports/archives/derivatives/fo_participant_oi_{date}.csv',
    'https://www.nseindia.com/reports/archives/derivatives/fo_participant_oi_{date}_csv',
    'https://www.nseindia.com/reports/archives/derivatives/participantwiseoi_{date}.csv',
    'https://www.nseindia.com/content/fo/participant_oi_{date}.csv',
    'https://www.nseindia.com/archives/fo_participant_oi_{date}.csv',
]

headers = {'User-Agent': 'Mozilla/5.0', 'Accept': '*/*'}
date = datetime.date.today() - datetime.timedelta(days=1)
datestr = date.strftime('%d%m%Y')
print('Testing date', datestr)
for tpl in templates:
    url = tpl.format(date=datestr)
    try:
        r = requests.head(url, headers=headers, timeout=10, allow_redirects=True)
        print(r.status_code, url)
    except Exception as e:
        print('ERR', url, e)
