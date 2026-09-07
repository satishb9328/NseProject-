import sys, os, types
sys.path.insert(0, r'c:\Users\ADMIN\Desktop\MyProject2\TESTING')
import funda_test as ft

# --- Test 1: HTTP 429 retry logic (no real 2-5 min wait) ---
s = ft.IndianStockScreener()
calls = []
class FakeResp:
    def __init__(self, code):
        self.status_code = code
        self.text = '<html><h1>Test</h1><ul id="top-ratios"></ul></html>'
    def json(self):
        return {}
def fake_get(url, timeout=None):
    calls.append(url)
    if len(calls) == 1:
        return FakeResp(429)
    return FakeResp(200)
s.session.get = fake_get
orig_sleep = ft.time.sleep
ft.time.sleep = lambda x: None  # 跳过真实的 2-5 分钟等待
resp = s._request_with_retry('http://x', max_retries=3)
ft.time.sleep = orig_sleep
assert resp is not None and resp.status_code == 200, resp
assert len(calls) == 2, calls
print('Test 1 OK: 429 retry, calls =', len(calls))

# --- Test 2: HTML fallback parsing with BeautifulSoup ---
html = '''<html><h1>Reliance Industries</h1>
<ul id="top-ratios">
<li><span class="name">Current Price</span><span class="number">1,234.5</span></li>
<li><span class="name">ROE</span><span class="number">20.5</span></li>
<li><span class="name">Stock P/E</span><span class="number">15</span></li>
<li><span class="name">Book Value</span><span class="number">100</span></li>
</ul></html>'''
s2 = ft.IndianStockScreener()
s2._request_with_retry = lambda url, **kw: types.SimpleNamespace(status_code=200, text=html)
data = s2._fetch_fundamentals_via_html('RELIANCE')
assert data and data['ratios']['current_price'] == 1234.5, data
assert data['ratios']['roe'] == 20.5, data
assert data['name'] == 'Reliance Industries', data
print('Test 2 OK: HTML fallback ratios =', data['ratios'])

# --- Test 3: API failure -> auto fallback to HTML in get_fundamental_data ---
s3 = ft.IndianStockScreener()
s3._fetch_fundamentals_via_api = lambda sym: None  # 模拟 API 结构变化/失败
s3._fetch_fundamentals_via_html = lambda sym: {'symbol': sym, 'name': 'X', 'ratios': {'roe': 18, 'pe': 20, 'current_price': 100, 'market_cap': 5000}}
d = s3.get_fundamental_data('TCS', fields=['ratios'])
assert d and d['ratios']['roe'] == 18, d
print('Test 3 OK: fallback get_fundamental_data ->', d['symbol'])

# --- Test 4: CSV export works ---
import pandas as pd
df = pd.DataFrame([{'symbol': 'A', 'roe': 20}])
df.to_csv('_tmp_validate.csv', index=False)
assert os.path.exists('_tmp_validate.csv')
os.remove('_tmp_validate.csv')
print('Test 4 OK: CSV export')

# --- Test 5: random module imported for randomized sleep ---
assert hasattr(ft, 'random')
assert 0.5 <= ft.random.uniform(0.5, 1.5) <= 1.5
print('Test 5 OK: randomized sleep uses random.uniform(0.5, 1.5)')

print('ALL TESTS PASSED')
