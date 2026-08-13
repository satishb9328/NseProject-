"""
    * NSE HISTORICAL DATA DOWNLOAD UTILITY *

    Description: This utility is Python Library to get publicly available historic candlestick data
     of stocks, index, its derivatives from the new NSE india website

    Timeframes supported are : 1m, 3m, 5m, 10m, 15m, 30m, 1h, 1d, 1w, 1M.

    Disclaimer : This utility is meant for educational purposes only. Downloading data from NSE
    website requires explicit approval from the exchange. Hence, the usage of this utility is for
    limited purposes only under proper/explicit approvals.

    Requirements : Following packages are to be installed (using pip) prior to using this utility
    - pandas
    - python 3.8 and above

"""

import pandas as pd
import time
import json
import requests
import os
from datetime import datetime, timedelta, date
import re

# ── Default local CSV paths (place these files next to NSE Historical.py) ──────
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_NSE_CM_CSV = os.path.join(_THIS_DIR, "NSE_CM.csv")
_DEFAULT_NSE_FO_CSV = os.path.join(_THIS_DIR, "NSE_FO.csv")

class NSEMasterData:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1',
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            'Content-Type': 'application/json',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-Mode': 'navigate'
        })
        # Primary charting URLs (may return 404 — handled gracefully below)
        self.nse_url = "https://charting.nseindia.com/Charts/GetEQMasters"
        self.nfo_url = "https://charting.nseindia.com/Charts/GetFOMasters"
        self.historical_url = "https://charting.nseindia.com//Charts/symbolhistoricaldata/"
        self.nse_data = None
        self.nfo_data = None
        # True only when at least one master DataFrame is populated
        self.has_data = False

    def search(self, symbol, exchange, match=False):
        """Search for symbols in the specified exchange.

        Args:
            symbol (str): The symbol or part of the symbol to search for.
            exchange (str): The exchange to search in ('NSE' or 'NFO').
            match (bool): If True, performs an exact match. If False, searches for symbols containing the input.

        Returns:
            pandas.DataFrame: A DataFrame containing all matching symbols.
        """
        exchange = exchange.upper()
        if exchange == 'NSE':
            df = self.nse_data
        elif exchange == 'NFO':
            df = self.nfo_data
        else:
            print(f"Invalid exchange '{exchange}'. Please choose 'NSE' or 'NFO'.")
            return pd.DataFrame()

        if df is None:
            print(f"Data for {exchange} not downloaded. Please run download() first.")
            return pd.DataFrame()

        if match:
            result = df[df['Symbol'].str.upper() == symbol.upper()]
        else:
            result = df[df['Symbol'].str.contains(symbol, case=False, na=False)]

        if result.empty:
            print(f"No matching result found for symbol '{symbol}' in {exchange}.")
            return pd.DataFrame()

        return result.reset_index(drop=True)

    def get_nse_symbol_master(self, url):
        """
        Fetch pipe-delimited master data from NSE charting URL.
        Returns empty DataFrame silently if the URL is unavailable (e.g., 404).
        """
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.text.splitlines()
            if not data:
                return pd.DataFrame()
            columns = ['ScripCode', 'Symbol', 'Name', 'Type']
            rows = [line.split('|') for line in data if line.strip()]
            # Only keep rows with exactly 4 fields
            rows = [r for r in rows if len(r) == 4]
            if not rows:
                return pd.DataFrame()
            return pd.DataFrame(rows, columns=columns)
        except Exception:
            # Silently ignore 404 / connection errors — caller will try CSV fallback
            return pd.DataFrame()

    @staticmethod
    def _load_csv_master(csv_path: str) -> pd.DataFrame:
        """
        Load a pipe-delimited master CSV saved on disk.
        Expected format (header optional):  ScripCode|Symbol|Name|Type
        Also accepts comma-separated NSE equity-list CSVs (EQUITY_L.csv style)
        by mapping available columns to the required schema.
        """
        if not os.path.exists(csv_path):
            return pd.DataFrame()
        try:
            # Try pipe-delimited first
            df = pd.read_csv(csv_path, sep='|', header=None, dtype=str)
            if df.shape[1] == 4:
                df.columns = ['ScripCode', 'Symbol', 'Name', 'Type']
                return df.dropna(subset=['ScripCode', 'Symbol'])
            # Try comma-delimited (NSE EQUITY_L style)
            df = pd.read_csv(csv_path, dtype=str)
            col_map = {}
            for c in df.columns:
                cl = c.strip().upper()
                if cl in ('SYMBOL',):
                    col_map[c] = 'Symbol'
                elif cl in ('NAME OF COMPANY', 'NAME', 'COMPANY NAME'):
                    col_map[c] = 'Name'
            if 'Symbol' not in col_map.values():
                return pd.DataFrame()
            df = df.rename(columns=col_map)
            if 'ScripCode' not in df.columns:
                df['ScripCode'] = '0'
            if 'Type' not in df.columns:
                df['Type'] = 'EQ'
            return df[['ScripCode', 'Symbol', 'Name', 'Type']].dropna(subset=['Symbol'])
        except Exception:
            return pd.DataFrame()

    def download_symbol_master(self,
                               cm_csv: str = _DEFAULT_NSE_CM_CSV,
                               fo_csv: str = _DEFAULT_NSE_FO_CSV):
        """
        Download NSE CM and FO master data.

        Priority order for each segment:
          1. Live charting URL (https://charting.nseindia.com/...)
          2. Local CSV file (cm_csv / fo_csv)

        Parameters
        ----------
        cm_csv : str
            Path to a local NSE CM master CSV (pipe or comma delimited).
            Defaults to  NseUtility/NSE_CM.csv
        fo_csv : str
            Path to a local NSE FO master CSV.
            Defaults to  NseUtility/NSE_FO.csv
        """
        # ── NSE CM (Equity) ──────────────────────────────────────────────────
        self.nse_data = self.get_nse_symbol_master(self.nse_url)
        if self.nse_data.empty:
            self.nse_data = self._load_csv_master(cm_csv)

        # ── NSE FO (Derivatives) ─────────────────────────────────────────────
        self.nfo_data = self.get_nse_symbol_master(self.nfo_url)
        if self.nfo_data.empty:
            self.nfo_data = self._load_csv_master(fo_csv)

        self.has_data = (
            (self.nse_data is not None and not self.nse_data.empty) or
            (self.nfo_data is not None and not self.nfo_data.empty)
        )

    def search_symbol(self, symbol, exchange):
        """Search for a symbol in the specified exchange and return the first match."""
        df = self.nse_data if exchange.upper() == 'NSE' else self.nfo_data
        if df is None:
            print(f"Data for {exchange} not downloaded. Please run download() first.")
            return None
        result = df[df['Symbol'].str.contains(symbol, case=False, na=False)]
        if result.empty:
            print(f"No matching result found for symbol '{symbol}' in {exchange}.")
            return None
        return result.iloc[0]

    def get_history(self, symbol="Nifty 50", exchange="NSE", start=None, end=None, interval='1d'):
        """Get historical data for a symbol."""

        def adjust_timestamp(ts):
            if interval in ['30m', '1h']:
                num = 15
            elif interval in ['10m']:
                num = 5
            else:
                num = int(re.match(r'\d+', interval).group())
            if num == 0:
                return (ts - timedelta(minutes=num)).round('min')
            else:
                return (ts - timedelta(minutes=num)).round((str(num) + 'min'))


        symbol_info = self.search_symbol(symbol, exchange)
        if symbol_info is None:
            return pd.DataFrame()

        interval_xref = {
            '1m': ('1', 'I'), '3m': ('3', 'I'), '5m': ('5', 'I'), '10m': ('5', 'I'),
            '15m': ('15', 'I'), '30m': ('15', 'I'), '1h': ('15', 'I'),
            '1d': ('1', 'D'), '1w': ('1', 'W'), '1M': ('1', 'M')
        }

        time_interval, chart_period = interval_xref.get(interval, ('1', 'D'))

        payload = {
            "exch": "N" if exchange.upper() == "NSE" else "D",
            "instrType": "C" if exchange.upper() == "NSE" else "D",
            "ScripCode": int(symbol_info['ScripCode']),
            "ulScripCode": int(symbol_info['ScripCode']),
            "fromDate": int(start.timestamp()) if start else 0,
            "toDate": int(end.timestamp()) if end else int(time.time()),
            "timeInterval": time_interval,
            "chartPeriod": chart_period,
            "chartStart": 0
        }

        try:
            # Set Cookies
            self.session.get("https://www.nseindia.com", timeout=5)
            response = self.session.post(self.historical_url, data=json.dumps(payload), timeout=10)
            response.raise_for_status()
            data = response.json()

            if not data:
                print("No data received from the Source - NSE.")
                return pd.DataFrame()

            df = pd.DataFrame(data)
            df.columns = ['Status', 'TS', 'Open', 'High', 'Low', 'Close', 'Volume']
            df['TS'] = pd.to_datetime(df['TS'], unit='s', utc=True)
            df['TS'] = df['TS'].dt.tz_localize(None)
            df = df[['TS', 'Open', 'High', 'Low', 'Close', 'Volume']]

            # Apply cutoff time only for intraday intervals
            intraday_intervals = ['1m', '3m', '5m', '15m']
            intraday_consolidate_intervals = ['10m','30m', '1h']
            if interval in intraday_intervals:
                cutoff_time = pd.Timestamp('15:30:00').time()
                df = df[df['TS'].dt.time <= cutoff_time]
                df['Timestamp'] = df['TS'].apply(adjust_timestamp)
                df.drop(columns=['TS'], inplace=True)
                df.set_index('Timestamp', inplace=True, drop=True)
                return df
            if interval in intraday_consolidate_intervals:
                cutoff_time = pd.Timestamp('15:30:00').time()
                df = df[df['TS'].dt.time <= cutoff_time]
                df['Timestamp'] = df['TS'].apply(adjust_timestamp)
                df.drop(columns=['TS'], inplace=True)
                df.set_index('Timestamp', inplace=True, drop=True)
                agg_parm = ''
                if interval == '30m':
                    agg_parm = '30min'
                elif interval == '10m':
                    agg_parm = '10min'
                else:
                    agg_parm = '60min'
                # Get the first timestamp to use as custom origin
                first_ts = df.index.min()
                offset_td = pd.to_timedelta(first_ts.time().strftime('%H:%M:%S'))
                df_aggregated = df.resample(agg_parm, origin='start_day', offset=offset_td).agg({
                    'Open': 'first',
                    'High': 'max',
                    'Low': 'min',
                    'Close': 'last',
                    'Volume': 'sum'
                })
                df_aggregated.dropna(inplace=True)
                return df_aggregated

            df.rename(columns={'TS': 'Timestamp'}, inplace=True)
            df.set_index('Timestamp', inplace=True, drop=True)
            return df

        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching historical data: {e}")
            return pd.DataFrame()

if __name__ == "__main__":

    pd.set_option("display.max_rows", None, "display.max_columns", None)

    # Initiate NSEMasterData instance
    nse = NSEMasterData()
    # Download the full symbol master data for both NSE and NFO from NSE Website
    nse.download_symbol_master()

    # Select Timeframe for historic data download - Static Start and End Dates
    # start_date = datetime(2025, 4, 1)
    # end_date = datetime(2025, 4, 11)

    # Select Timeframe for historic data download - Last n days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)


    # Symbol Search - NSE
    print("********************  Symbol Search Utility  **********************")
    symbols = nse.search('MidSmallcap 400', exchange='NSE', match=False)
    print(symbols)

    # Symbol Search - NFO
    symbols = nse.search('BANKNIFTY', exchange='NFO', match=False)
    print("********************  Symbol Search Utility  **********************")
    print(symbols)


    # Download Index EOD Data
    data = nse.get_history(
        symbol='NIFTY 50',
        exchange='NSE',
        start=start_date,
        end=end_date,
        interval='1d'
    )
    print("********************  Index EOD Data  **********************")
    print("Symbol : NIFTY 50")
    print(data.head(2))


    # Download Index Intraday Data
    data = nse.get_history(
        symbol='NIFTY BANK',
        exchange='NSE',
        start=start_date,
        end=end_date,
        interval='1m'
    )
    print("********************  Index Intraday Data  **********************")
    print("Symbol : BANKNIFTY - 1 Minute data")
    print(data.head(2))


    # Download Stock (Underlying) Data
    data = nse.get_history(
        symbol='TCS',
        exchange='NSE',
        start=start_date,
        end=end_date,
        interval='10m'
    )
    print("********************  Stock Intraday Data  **********************")
    print("Symbol : TCS - 10 Minute data")
    print(data.head(2))


    # Download Index Futures Data
    data = nse.get_history(
        symbol='NIFTY25DECFUT',
        exchange='NFO',
        start=start_date,
        end=end_date,
        interval='1h'
    )
    print("********************  Index Futures Data  **********************")
    print("Symbol : Nifty December Futures - 1 Hour data")
    print(data.head(2))

    # Download Stock Futures Data
    data = nse.get_history(
        symbol='RELIANCE25DECFUT',
        exchange='NFO',
        start=start_date,
        end=end_date,
        interval='1h'
    )
    print("********************  Stock Futures Data  **********************")
    print("Symbol : Reliance December Futures - 1 Hour data")
    print(data.head(2))

    # Download Index Options Data
    data = nse.get_history(
        symbol='BANKNIFTY25DEC50000PE',
        exchange='NFO',
        start=start_date,
        end=end_date,
        interval='5m'
    )
    print("********************  Index Options Data  **********************")
    print("Symbol : Banknifty PE Options - 5 Minute data")
    print(data.head(2))

    # Download Stock Options Data
    data = nse.get_history(
        symbol='TCS25DEC3000CE',
        exchange='NFO',
        start=start_date,
        end=end_date,
        interval='5m'
    )
    print("********************  Stock Options Data  **********************")
    print("Symbol : TCS CE Option - 5 Minute data")
    print(data.head(2))