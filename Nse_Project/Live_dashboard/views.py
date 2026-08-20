from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render
from pathlib import Path
import json
import os
import pandas as pd
import requests
from Nse_Project.Swing_ML.views import get_swing_ml_context

LIVE_OPTIONS_FILE = Path(__file__).resolve().parent.parent / "live_options_data.json"
OPTION_INSTRUMENT_METADATA: dict[str, dict] = {}
MASTER_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"

# 1. డేటాను తీసుకొచ్చే ఫంక్షన్
def most_active_index_contracts():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.nseindia.com/'
    }
    session = requests.Session()
    
    try:
        ref_url = 'https://www.nseindia.com/market-data/most-active-contracts'
        ref = session.get(ref_url, headers=headers)
        cookies = ref.cookies.get_dict()

        calls_url = 'https://www.nseindia.com/api/snapshot-derivatives-equity?index=calls-index-vol'
        puts_url = 'https://www.nseindia.com/api/snapshot-derivatives-equity?index=puts-index-vol'

        res_calls = session.get(calls_url, headers=headers, cookies=cookies)
        res_puts = session.get(puts_url, headers=headers, cookies=cookies)

        calls_list = res_calls.json().get('OPTIDX', {}).get('data', [])
        puts_list = res_puts.json().get('OPTIDX', {}).get('data', [])

        df_calls = pd.DataFrame(calls_list)
        df_puts = pd.DataFrame(puts_list)

        if df_calls.empty and df_puts.empty:
            return None

        if not df_calls.empty:
            df_calls['expiryDate'] = pd.to_datetime(df_calls['expiryDate'], errors='coerce')
            df_calls = df_calls.rename(columns={
                'openInterest': 'oi',
                'numberOfContractsTraded': 'volume',
                'lastPrice': 'ltp'
            })

        if not df_puts.empty:
            df_puts['expiryDate'] = pd.to_datetime(df_puts['expiryDate'], errors='coerce')
            df_puts = df_puts.rename(columns={
                'openInterest': 'oi',
                'numberOfContractsTraded': 'volume',
                'lastPrice': 'ltp'
            })

        expiry_candidates = []
        if not df_calls.empty:
            expiry_candidates.append(df_calls['expiryDate'].min())
        if not df_puts.empty:
            expiry_candidates.append(df_puts['expiryDate'].min())

        if not expiry_candidates:
            return None

        current_week_expiry = min(expiry_candidates)
        current_week_expiry = pd.to_datetime(current_week_expiry, errors='coerce')
        if pd.isna(current_week_expiry):
            return None

        if not df_calls.empty:
            df_calls = df_calls[df_calls['expiryDate'] == current_week_expiry]
        if not df_puts.empty:
            df_puts = df_puts[df_puts['expiryDate'] == current_week_expiry]

        if df_calls.empty and df_puts.empty:
            return None

        if not df_calls.empty:
            df_calls = df_calls.add_prefix('call_')
            df_calls['strikePrice'] = df_calls['call_strikePrice']
            df_calls['underlying'] = df_calls['call_underlying']
            for col in ['oi%', 'coi', 'coi%']:
                df_calls[f'call_{col}'] = '-'

        if not df_puts.empty:
            df_puts = df_puts.add_prefix('put_')
            df_puts['strikePrice'] = df_puts['put_strikePrice']
            df_puts['underlying'] = df_puts['put_underlying']
            for col in ['oi%', 'coi', 'coi%']:
                df_puts[f'put_{col}'] = '-'

        if not df_calls.empty and not df_puts.empty:
            merged_df = pd.merge(df_calls, df_puts, on=['underlying', 'strikePrice'], how='outer')
        elif not df_calls.empty:
            merged_df = df_calls
        else:
            merged_df = df_puts

        if 'underlying' in merged_df.columns:
            merged_df = merged_df.drop(columns=['underlying'])

        ordered_cols = [
            'call_oi%', 'call_oi', 'call_volume', 'call_ltp', 'call_coi', 'call_coi%',
            'strikePrice',
            'put_coi%', 'put_coi', 'put_ltp', 'put_volume', 'put_oi', 'put_oi%'
        ]
        final_cols = [col for col in ordered_cols if col in merged_df.columns]
        merged_df = merged_df[final_cols].sort_values(by=['strikePrice']).reset_index(drop=True)

        top_headers = []
        bottom_headers = []
        for col in merged_df.columns:
            if col == 'strikePrice':
                top_headers.append('')
                bottom_headers.append('Strike Price')
            elif col.startswith('call_'):
                top_headers.append('CALL')
                bottom_headers.append(col.replace('call_', '').upper())
            elif col.startswith('put_'):
                top_headers.append('PUT')
                bottom_headers.append(col.replace('put_', '').upper())
            else:
                top_headers.append('')
                bottom_headers.append(col.upper())

        merged_df.columns = pd.MultiIndex.from_arrays([top_headers, bottom_headers])

        return merged_df, current_week_expiry.strftime('%d-%b-%Y')

    except Exception:
        return None

def dashboard_view(request):
    result = most_active_index_contracts()

    if result is not None:
        df, current_expiry = result
    else:
        df, current_expiry = None, None

    context = {'current_expiry': current_expiry}
    context.update(get_swing_ml_context())
    return render(request, "dashboard.html", context)


def read_live_options_data() -> dict:
    json_path = os.path.join(settings.BASE_DIR, 'live_options_data.json')
    try:
        print(f"API fetched, file exists: {os.path.exists(json_path)}")
        if not os.path.exists(json_path):
            return {}
        with open(json_path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as exc:
        print(f"API fetch error reading JSON: {exc}")
        return {}


def get_nested(raw, path):
    if not isinstance(raw, dict):
        return None
    current = raw
    for key in path:
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list) and isinstance(key, int) and 0 <= key < len(current):
            current = current[key]
        else:
            return None
    return current


def _to_number(value):
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.replace(',', '').strip())
        except ValueError:
            return None
    return None


def _normalized_symbol_value(value) -> str | None:
    if not isinstance(value, str):
        return None
    symbol = value.strip().upper().replace(" ", "")
    return symbol if symbol else None


def _is_strict_target_symbol(value, target_symbol: str) -> bool:
    normalized = _normalized_symbol_value(value)
    if not normalized:
        return False

    target = target_symbol.upper().replace(" ", "")
    if not normalized.startswith(target):
        return False

    if target == "NIFTY":
        exclusions = ("BANKNIFTY", "FINNIFTY", "SENSEX", "MIDCPNIFTY")
        if any(exclusion in normalized for exclusion in exclusions):
            return False

    return True


def _collect_symbol_candidates(item: dict) -> list[str]:
    candidates: list[str] = []

    for field in ("tradingsymbol", "symbol", "instrument_key", "underlying", "underlying_name"):
        value = item.get(field)
        if isinstance(value, str):
            candidates.append(value)

    raw_sample = item.get("raw_sample")
    if isinstance(raw_sample, dict):
        def recurse(value):
            if isinstance(value, dict):
                for key, nested in value.items():
                    if isinstance(nested, str) and any(token in key.lower() for token in ("symbol", "tradingsymbol", "underlying")):
                        candidates.append(nested)
                    else:
                        recurse(nested)
            elif isinstance(value, list):
                for nested in value:
                    recurse(nested)

        recurse(raw_sample)

    return candidates


def _belongs_to_symbol(item: dict, target_symbol: str) -> bool:
    """Require a strict target-symbol marker from saved metadata or the raw feed payload."""
    if not isinstance(item, dict):
        return False

    for candidate in _collect_symbol_candidates(item):
        if _is_strict_target_symbol(candidate, target_symbol):
            return True

    return False


def build_option_chain_rows(instrument_data: dict, target_symbol: str = "NIFTY") -> list:
    rows = {}
    excluded_symbols = ("BANKNIFTY", "FINNIFTY", "SENSEX", "MIDCPNIFTY")
    target_prefix = target_symbol.upper().replace(" ", "")

    for instrument_key, item in instrument_data.items():
        if not isinstance(item, dict):
            continue

        # Lock 1: Value-based symbol filter (inspect item values, not the instrument_token key)
        symbol = item.get("tradingsymbol") or item.get("underlying_name")
        normalized_symbol = symbol.strip().upper().replace(" ", "") if isinstance(symbol, str) else ""
        if not normalized_symbol or not normalized_symbol.startswith(target_prefix):
            continue
        if any(excluded in normalized_symbol for excluded in excluded_symbols):
            continue

        # Lock 2: Hard strike boundary (strictly 20000 <= strike <= 30000)
        strike = item.get("strike_price", item.get("strike"))
        if not isinstance(strike, (int, float)) or not (20000 <= strike <= 30000):
            continue

        side = (item.get('option_type') or '').upper()
        raw = item.get('raw_sample', {})
        if side not in {"CE", "PE"}:
            continue

        strike_key = str(int(strike) if isinstance(strike, (int, float)) and float(strike).is_integer() else strike)
        row = rows.setdefault(strike_key, {
            'strike': strike_key,
            'call_oi_pct': '-',
            'call_oi': '-',
            'call_volume': '-',
            'call_ltp': '-',
            'call_coi': '-',
            'call_coi_pct': '-',
            'put_coi_pct': '-',
            'put_coi': '-',
            'put_ltp': '-',
            'put_volume': '-',
            'put_oi': '-',
            'put_oi_pct': '-',
        })

        target = 'call' if side == 'CE' else 'put'
        ltp = item.get('ltp')
        if ltp is None:
            ltp = get_nested(raw, ['fullFeed', 'marketFF', 'ltpc', 'ltp'])
        oi = item.get('oi')
        if oi is None:
            oi = get_nested(raw, ['fullFeed', 'marketFF', 'oi'])
        volume = item.get('volume')
        if volume is None:
            volume = get_nested(raw, ['fullFeed', 'marketFF', 'vtt'])
            if volume is None:
                volume = get_nested(raw, ['fullFeed', 'marketFF', 'marketOHLC', 'ohlc', 0, 'vol'])

        coi = item.get('coi')
        coi_pct = item.get('coi_pct')
        if coi is None and isinstance(oi, (int, float)):
            prev_oi = item.get('previous_oi')
            if isinstance(prev_oi, (int, float)):
                coi = oi - prev_oi
                coi_pct = round((coi / prev_oi) * 100, 2) if prev_oi != 0 else None

        row[f'{target}_ltp'] = ltp if ltp is not None else '-'
        row[f'{target}_oi'] = oi if oi is not None else '-'
        row[f'{target}_volume'] = volume if volume is not None else '-'
        row[f'{target}_coi'] = coi if coi is not None else '-'
        row[f'{target}_coi_pct'] = coi_pct if coi_pct is not None else '-'

        call_oi = _to_number(row.get('call_oi'))
        put_oi = _to_number(row.get('put_oi'))
        call_coi = _to_number(row.get('call_coi'))
        put_coi = _to_number(row.get('put_coi'))

        if call_oi is not None and put_oi is not None:
            oi_total = call_oi + put_oi
            row['call_oi_pct'] = round((call_oi / oi_total) * 100, 1) if oi_total != 0 else 0
            row['put_oi_pct'] = round((put_oi / oi_total) * 100, 1) if oi_total != 0 else 0

        if call_coi is not None and put_coi is not None:
            coi_total = call_coi + put_coi
            row['call_coi_pct'] = round((call_coi / coi_total) * 100, 1) if coi_total != 0 else 0
            row['put_coi_pct'] = round((put_coi / coi_total) * 100, 1) if coi_total != 0 else 0

    def has_activity(value):
        return isinstance(value, (int, float)) and value != 0

    filtered = [
        row for row in rows.values()
        if has_activity(row.get('call_oi')) or has_activity(row.get('call_volume')) or
           has_activity(row.get('put_oi')) or has_activity(row.get('put_volume'))
    ]
    filtered.sort(key=lambda row: float(row['strike']) if row['strike'] != '-' else float('inf'))
    return filtered


def live_option_chain_api(request):
    data = read_live_options_data()
    chain = build_option_chain_rows(data.get("instrument_data", {}), target_symbol="NIFTY")
    return JsonResponse({
        "updated_at": data.get("updated_at"),
        "expiry_date": data.get("expiry_date"),
        "option_chain": chain,
    })
