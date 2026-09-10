
import requests
import json
import time
import random
import pandas as pd
from bs4 import BeautifulSoup
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, asdict
import sys
import re

# 配置常量
SCREENER_BASE_URL = "https://www.screener.in/company"
SCREENER_API_URL = "https://www.screener.in/api/company"
SEARCH_URL = "https://www.screener.in/api/search/"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json',
}

@dataclass
class StockProfile:
    symbol: str
    name: str
    current_price: float = 0.0
    market_cap: float = 0.0
    pe_ratio: float = 0.0
    pb_ratio: float = 0.0
    roe: float = 0.0
    roce: float = 0.0
    debt_to_equity: float = 0.0
    dividend_yield: float = 0.0
    eps_ttm: float = 0.0
    book_value: float = 0.0
    face_value: float = 0.0
    industry_pe: float = 0.0

class IndianStockScreener:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.cache = {}
        self.cache_ttl = 300  # 5分钟缓存

    def _request_with_retry(self, url: str, timeout: int = 15, max_retries: int = 3) -> Optional[requests.Response]:
        """
        带重试逻辑的 GET 请求。

        针对 HTTP 429 (Too Many Requests) 做特殊处理：
        随机等待 2-5 分钟后再自动重试，而不是直接崩溃。
        """
        for attempt in range(1, max_retries + 1):
            try:
                response = self.session.get(url, timeout=timeout)
            except requests.RequestException as e:
                print(f"Request error for {url}: {e}")
                return None

            if response.status_code == 429:
                wait_seconds = random.uniform(120, 300)  # 2 - 5 分钟
                print(
                    f"⚠️ HTTP 429 Too Many Requests for {url}. "
                    f"Waiting {wait_seconds / 60:.1f} minutes before retry "
                    f"({attempt}/{max_retries})..."
                )
                time.sleep(wait_seconds)
                continue

            return response

        print(f"❌ Exhausted {max_retries} retries for {url} due to HTTP 429.")
        return None

    def search_company(self, query: str) -> List[Dict[str, Any]]:
        """
        按名称或代码搜索公司
        """
        try:
            response = self._request_with_retry(f"{SEARCH_URL}?q={query}", timeout=10)
            if response is not None and response.status_code == 200:
                data = response.json()
                return data.get('results', [])
            return []
        except Exception as e:
            print(f"Search error for {query}: {e}")
            return []

    def _fetch_fundamentals_via_api(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        主路径：尝试通过 Screener.in 的 JSON API 获取基本面数据。

        如果 API 结构发生变化或请求失败，返回 None，
        由 get_fundamental_data 回退到 BeautifulSoup 解析 HTML 页面。
        """
        url = f"{SCREENER_API_URL}/{symbol}/"
        response = self._request_with_retry(url)
        if response is None or response.status_code != 200:
            return None

        try:
            payload = response.json()
        except (ValueError, json.JSONDecodeError):
            return None

        if not isinstance(payload, dict):
            return None

        # 防御性解析：Screener.in 的 API 返回结构可能随时间变化
        ratios = payload.get("ratios")
        if not ratios:
            company = payload.get("company") or payload.get("data") or {}
            ratios = company.get("ratios") if isinstance(company, dict) else None

        if not ratios:
            # 结构不匹配 -> 触发 HTML 兜底
            return None

        return {
            "symbol": symbol,
            "name": payload.get("name") or symbol,
            "ratios": ratios,
        }

    def _fetch_fundamentals_via_html(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        回退路径：使用 BeautifulSoup 解析 Screener.in 的 HTML 页面。
        当 JSON API 结构变化或请求失败时，仍然可以拿到基本面数据。
        """
        response = self._request_with_retry(
            f"{SCREENER_BASE_URL}/{symbol}/consolidated/"
        )
        if response is None or response.status_code != 200:
            print(
                f"Failed to fetch data for {symbol}. "
                f"Status: {response.status_code if response else 'N/A'}"
            )
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        ratios = {}
        ratio_fields = {
            "Current Price": "current_price",
            "Market Cap": "market_cap",
            "Stock P/E": "pe",
            "Book Value": "book_value",
            "Dividend Yield": "dividend_yield",
            "ROCE": "roce",
            "ROE": "roe",
            "Face Value": "face_value",
        }
        for item in soup.select("#top-ratios li"):
            name = item.select_one(".name")
            value = item.select_one(".number")
            if name and value and name.get_text(" ", strip=True) in ratio_fields:
                number = re.search(
                    r"-?[\d,]+(?:\.\d+)?", value.get_text(" ", strip=True)
                )
                if number:
                    ratios[ratio_fields[name.get_text(" ", strip=True)]] = float(
                        number.group().replace(",", "")
                    )

        balance_sheet = soup.select_one("#balance-sheet table")
        if balance_sheet:
            balance_values = {}
            for row in balance_sheet.select("tbody tr"):
                cells = row.find_all(["th", "td"])
                if len(cells) < 2:
                    continue
                label = cells[0].get_text(" ", strip=True).rstrip("+").strip()
                number = re.search(
                    r"-?[\d,]+(?:\.\d+)?", cells[-1].get_text(" ", strip=True)
                )
                if number:
                    balance_values[label] = float(number.group().replace(",", ""))

            equity = balance_values.get("Equity Capital", 0) + balance_values.get("Reserves", 0)
            if equity:
                ratios["debt_to_equity"] = balance_values.get("Borrowings", 0) / equity

        if ratios.get("book_value"):
            ratios["pb"] = ratios.get("current_price", 0) / ratios["book_value"]
        if ratios.get("pe"):
            ratios["eps_ttm"] = ratios.get("current_price", 0) / ratios["pe"]

        title = soup.select_one("h1")
        return {
            "symbol": symbol,
            "name": title.get_text(" ", strip=True) if title else symbol,
            "ratios": ratios,
        }

    def get_fundamental_data(self, symbol: str, fields: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """
        获取公司详细基本面数据。

        优先使用 JSON API，若 API 结构变化或请求失败，
        自动回退到 BeautifulSoup 解析 HTML 页面。
        fields: 可选字段列表 ['ratios', 'quarterly_results', 'profit_loss', 'balance_sheet', 'cash_flow', 'shareholding', 'peers']
        """
        # 检查缓存
        cache_key = f"{symbol}_{','.join(fields) if fields else 'all'}"
        if cache_key in self.cache:
            timestamp, data = self.cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                return data

        try:
            # 主路径：JSON API
            data = self._fetch_fundamentals_via_api(symbol)
            if data:
                self.cache[cache_key] = (time.time(), data)
                return data

            # 回退路径：BeautifulSoup 解析 HTML 页面
            data = self._fetch_fundamentals_via_html(symbol)
            if data:
                self.cache[cache_key] = (time.time(), data)
                return data

            return None
        except Exception as e:
            print(f"Error fetching fundamentals for {symbol}: {e}")
            return None

    def parse_ratios(self, data: Dict) -> StockProfile:
        """
        从原始数据中解析关键比率
        """
        profile = StockProfile(symbol=data.get('symbol', ''), name=data.get('name', ''))
        
        ratios = data.get('ratios', {})
        if ratios:
            profile.current_price = ratios.get('current_price', 0)
            profile.market_cap = ratios.get('market_cap', 0)
            profile.pe_ratio = ratios.get('pe', 0)
            profile.pb_ratio = ratios.get('pb', 0)
            profile.roe = ratios.get('roe', 0)
            profile.roce = ratios.get('roce', 0)
            profile.debt_to_equity = ratios.get('debt_to_equity', 0)
            profile.dividend_yield = ratios.get('dividend_yield', 0)
            profile.eps_ttm = ratios.get('eps_ttm', 0)
            profile.book_value = ratios.get('book_value', 0)
            profile.face_value = ratios.get('face_value', 0)
            profile.industry_pe = ratios.get('industry_pe', 0)
            
        return profile

    def screen_stocks(self, symbols: List[str], min_roe: float = 15, max_pe: float = 30, min_market_cap: float = 1000) -> pd.DataFrame:
        """
        根据基本面条件筛选股票
        """
        results = []
        print(f"Starting screening for {len(symbols)} stocks...")
        
        for i, symbol in enumerate(symbols):
            print(f"Processing {i+1}/{len(symbols)}: {symbol}")
            data = self.get_fundamental_data(symbol, fields=['ratios'])
            
            if not data:
                continue
                
            profile = self.parse_ratios(data)
            
            # 应用筛选条件
            if profile.roe >= min_roe and \
               profile.pe_ratio <= max_pe and \
               profile.pe_ratio > 0 and \
               profile.market_cap >= min_market_cap:
                results.append(asdict(profile))
                
            # 避免请求过快被限制，随机延迟模拟人类行为
            time.sleep(random.uniform(0.5, 1.5))
            
        if results:
            df = pd.DataFrame(results)
            return df.sort_values(by='roe', ascending=False)
        else:
            return pd.DataFrame()

    def get_peer_comparison(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        获取同行比较数据
        """
        data = self.get_fundamental_data(symbol, fields=['peers'])
        if not data or 'peers' not in data:
            return None
            
        peers = data['peers']
        if not peers:
            return None
            
        # 构建同行数据框
        peer_data = []
        for peer in peers:
            peer_symbol = peer.get('symbol', '')
            # 获取每个同行的基本比率
            peer_full_data = self.get_fundamental_data(peer_symbol, fields=['ratios'])
            if peer_full_data:
                p_profile = self.parse_ratios(peer_full_data)
                peer_data.append(asdict(p_profile))
                
        if peer_data:
            return pd.DataFrame(peer_data)
        return None

def main():
    screener = IndianStockScreener()
    
    # మీరు ఇచ్చిన లోకల్ ఫైల్ పాత్ (Your local CSV file path)
    csv_path = r"C:\Users\ADMIN\Desktop\MyProject2\Nse_Project\ind_nifty500list.csv"
    
    print(f"📊 Reading local Nifty 500 list from: {csv_path}")
    try:
        # ఇంటర్నెట్ లేకుండా డైరెక్ట్‌గా మీ కంప్యూటర్ నుండి చదువుతుంది
        nifty_df = pd.read_csv(csv_path)
        
        # 'Symbol' అనే కాలమ్ నుండి స్టాక్స్ పేర్లను తీసుకుంటుంది
        nifty500_stocks = nifty_df['Symbol'].tolist()
        print(f"✅ Successfully loaded {len(nifty500_stocks)} stocks from local file.\n")
        
    except Exception as e:
        print(f"❌ Failed to read the local CSV file: {e}")
        return
    
    # Run the swing screener on all 500 stocks
    swing_df = screener.screen_swing_candidates(nifty500_stocks)
    
    if not swing_df.empty:
        print("\n🚀 SUCCESS! SWING TRADING CANDIDATES FOUND:")
        print("-----------------------------------------------------------------------------------------")
        display_cols = ['symbol', 'current_price', 'qtr_sales_growth', 'qtr_profit_growth', 'opm', 'roe', 'debt_to_equity', 'peg_ratio']
        print(swing_df[display_cols].to_string(index=False))
        
        swing_df.to_csv('nifty500_swing_candidates.csv', index=False)
        print(f"\n📁 Results saved to nifty500_swing_candidates.csv ({len(swing_df)} stocks matched)")
    else:
        print("\n❌ No stocks matched the aggressive swing trading criteria today.")

if __name__ == "__main__":
    main()