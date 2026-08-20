import requests, datetime

# Candidate URL templates (placeholders {date})
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

start = datetime.date.today() - datetime.timedelta(days=10)
for i in range(10):
    d = start + datetime.timedelta(days=i)
    datestr = d.strftime('%d%m%Y')
    for tpl in templates:
        url = tpl.format(date=datestr)
        try:
            r = requests.head(url, headers=headers, timeout=10)
        except Exception as e:
            continue
        if r.status_code == 200:
            print('FOUND', datestr, url)
            raise SystemExit
        # occasionally servers respond 302, 403 etc - print first few
        if r.status_code in (200,301,302,403,404):
            pass
    
print('Done scanning')
