import requests

base = 'https://www.nseindia.com'
files = [
    '/assets/js/nse-min.js?v=14032026',
    '/dist/js/blinkui/src/blinkui.js?v=14032026',
    '/dist/js/sections/all-reports.js?v=14032026',
    '/assets/js/common.js?v=14032026',
    '/assets/js/multilingual/library.js',
    '/assets/js/multilingual/P9_Language.js?v=14032026',
    '/assets/js/realtime/header-nifty.js',
    '/DDwOWm-TsesHY/Ma7QC4fta7Mut/U/5Yw1VtDX0kLJhD/UQF7/DjJoQSR5/NFQB',
]

for path in files:
    url = base + path
    try:
        r = requests.get(url, headers={'User-Agent':'Mozilla/5.0'}, timeout=20)
        txt = r.text
        found = 'SingledownloadReports' in txt
        print(path, 'status', r.status_code, 'found', found)
        if found:
            idx = txt.find('SingledownloadReports')
            print(txt[idx-200:idx+400])
            break
    except Exception as e:
        print(path, 'error', e)
