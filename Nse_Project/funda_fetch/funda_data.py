import requests
import json
import time
import random
import pandas as pd
from bs4 import BeautifulSoup
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, asdict
import re

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

SCREENER_BASE_URL = "https://www.screener.in/company"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
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
    eps_ttm: float = 0.0
    book_value: float = 0.0
    qtr_sales_growth: float = 0.0
    qtr_profit_growth: float = 0.0
    opm: float = 0.0
    peg_ratio: float = 0.0

class IndianStockScreener:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.cache = {}
        self.cache_ttl = 300

    def _request_with_retry(self, url: str, timeout: int = 15, max_retries: int = 3) -> Optional[requests.Response]:
        for attempt in range(1, max_retries + 1):
            try:
                response = self.session.get(url, timeout=timeout)
            except requests.RequestException as e:
                print(f"Request error for {url}: {e}")
                return None

            if response.status_code == 429:
                # FIX: Reduced penalty wait time to 45-90 seconds
                wait_seconds = random.uniform(45, 90)
                print(f"⚠️ Rate limit hit. Cooling down for {wait_seconds:.0f} seconds (Attempt {attempt}/{max_retries})...")
                time.sleep(wait_seconds)
                continue
            return response
        return None

    def _fetch_fundamentals_via_html(self, symbol: str) -> Optional[Dict[str, Any]]:
        response = self._request_with_retry(f"{SCREENER_BASE_URL}/{symbol}/consolidated/")
        if response is None or response.status_code != 200:
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        ratios = {}
        
        ratio_fields = {
            "Current Price": "current_price",
            "Market Cap": "market_cap",
            "Stock P/E": "pe",
            "Book Value": "book_value",
            "ROCE": "roce",
            "ROE": "roe",
        }
        for item in soup.select("#top-ratios li"):
            name = item.select_one(".name")
            value = item.select_one(".number")
            if name and value and name.get_text(" ", strip=True) in ratio_fields:
                number = re.search(r"-?[\d,]+(?:\.\d+)?", value.get_text(" ", strip=True))
                if number:
                    ratios[ratio_fields[name.get_text(" ", strip=True)]] = float(number.group().replace(",", ""))

        balance_sheet = soup.select_one("#balance-sheet table")
        if balance_sheet:
            balance_values = {}
            for row in balance_sheet.select("tbody tr"):
                cells = row.find_all(["th", "td"])
                if len(cells) < 2: continue
                label = cells[0].get_text(" ", strip=True).rstrip("+").strip()
                number = re.search(r"-?[\d,]+(?:\.\d+)?", cells[-1].get_text(" ", strip=True))
                if number:
                    balance_values[label] = float(number.group().replace(",", ""))
            
            equity = balance_values.get("Equity Capital", 0) + balance_values.get("Reserves", 0)
            if equity:
                ratios["debt_to_equity"] = round(balance_values.get("Borrowings", 0) / equity, 2)

        quarters_table = soup.select_one("#quarters table")
        if quarters_table:
            try:
                headers = [th.get_text(strip=True) for th in quarters_table.select("thead th")]
                if len(headers) >= 6:
                    latest_idx = -1
                    prev_yr_idx = -5
                    for row in quarters_table.select("tbody tr"):
                        cells = row.find_all(["th", "td"])
                        if len(cells) > abs(prev_yr_idx):
                            label = cells[0].get_text(strip=True).rstrip("+").strip()
                            try:
                                latest_val = float(cells[latest_idx].get_text(strip=True).replace(",", ""))
                                prev_yr_val = float(cells[prev_yr_idx].get_text(strip=True).replace(",", ""))
                                if label == "Sales" and prev_yr_val > 0:
                                    ratios["qtr_sales_growth"] = round(((latest_val - prev_yr_val) / prev_yr_val) * 100, 2)
                                elif label == "Net Profit" and prev_yr_val > 0:
                                    ratios["qtr_profit_growth"] = round(((latest_val - prev_yr_val) / prev_yr_val) * 100, 2)
                                elif label == "OPM %":
                                    ratios["opm"] = latest_val
                            except ValueError:
                                continue
            except Exception:
                pass

        if ratios.get("book_value"):
            ratios["pb"] = round(ratios.get("current_price", 0) / ratios["book_value"], 2)
        if ratios.get("pe"):
            ratios["eps_ttm"] = round(ratios.get("current_price", 0) / ratios["pe"], 2)
            if ratios.get("qtr_profit_growth", 0) > 0:
                ratios["peg_ratio"] = round(ratios["pe"] / ratios["qtr_profit_growth"], 2)

        title = soup.select_one("h1")
        return {"symbol": symbol, "name": title.get_text(" ", strip=True) if title else symbol, "ratios": ratios}

    def get_fundamental_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        cache_key = f"{symbol}_all"
        if cache_key in self.cache:
            timestamp, data = self.cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                return data

        try:
            # FIX: Skip the broken API entirely and go straight to HTML. 
            # This cuts our total requests in half!
            data = self._fetch_fundamentals_via_html(symbol)
            if data:
                self.cache[cache_key] = (time.time(), data)
                return data
            return None
        except Exception:
            return None

    def parse_ratios(self, data: Dict) -> StockProfile:
        profile = StockProfile(symbol=data.get('symbol', ''), name=data.get('name', ''))
        ratios = data.get('ratios', {})
        if ratios:
            for key, value in ratios.items():
                if hasattr(profile, key):
                    setattr(profile, key, value)
                elif key == 'pe': profile.pe_ratio = value
                elif key == 'pb': profile.pb_ratio = value
        return profile

    def screen_swing_candidates(self, symbols: List[str]) -> pd.DataFrame:
        results = []
        print(f"\n🔍 Scanning {len(symbols)} stocks for Swing Trading Breakouts...")
        
        for i, symbol in enumerate(symbols):
            print(f"[{i+1}/{len(symbols)}] Fetching {symbol}...")
            data = self.get_fundamental_data(symbol)
            if not data: 
                continue
                
            p = self.parse_ratios(data)
            
            if (p.qtr_sales_growth >= 15 and 
                p.qtr_profit_growth >= 20 and 
                p.roe >= 15 and 
                p.debt_to_equity < 1.0):
                
                results.append(asdict(p))
                
            # FIX: Slightly higher sleep to avoid triggering the 429 ban entirely
            time.sleep(random.uniform(1.5, 3.0))
            
        if results:
            df = pd.DataFrame(results)
            return df.sort_values(by='qtr_profit_growth', ascending=False)
        return pd.DataFrame()

def main():
    screener = IndianStockScreener()
    csv_path = "ind_nifty500list.csv"
    
    print(f"📊 Reading local Nifty 500 list from current folder: {csv_path}")
    try:
        nifty_df = pd.read_csv(csv_path)
        nifty500_stocks = nifty_df['Symbol'].tolist()
        print(f"✅ Successfully loaded {len(nifty500_stocks)} stocks from local file.\n")
    except Exception as e:
        print(f"❌ Failed to read the local CSV file: {e}")
        return
    
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