from django.http import JsonResponse
from django.shortcuts import render
from django.core.cache import cache
from pathlib import Path
import json
import pandas as pd
import requests

LIVE_OPTIONS_FILE = Path(__file__).resolve().parent / "live_options_data.json"
SWING_ML_CSV_FILE = Path(__file__).resolve().parent.parent / "Swing_stocks" / "nifty200_smc_precision_screen.csv"
SWING_ML_CACHE_KEY = 'swing_ml_signals'

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

# 2. వెబ్ పేజీలో చూపించడానికి Django View
def get_swing_ml_context():
    """Read cached Swing ML scanner results so the AI Signal Queue table renders."""
    cache_data = cache.get(SWING_ML_CACHE_KEY)
    if not cache_data:
        return {
            'tier_1_trades': [],
            'tier_2_trades': [],
            'scanner_warming_up': True,
            'last_updated': None,
        }
    return {
        'tier_1_trades': cache_data.get('tier_1', []),
        'tier_2_trades': cache_data.get('tier_2', []),
        'scanner_warming_up': False,
        'last_updated': cache_data.get('last_updated'),
    }


def dashboard_view(request):
    result = most_active_index_contracts()

    if result is not None:
        df, current_expiry = result
    else:
        df, current_expiry = None, None

    context = {'current_expiry': current_expiry}
    context.update(get_swing_ml_context())
    return render(request, "dashboard.html", context)


def swing_ml_dashboard(request):
    top_20 = []
    smc_setups = []

    if SWING_ML_CSV_FILE.exists():
        try:
            df = pd.read_csv(SWING_ML_CSV_FILE)
            df = df.where(pd.notnull(df), None)

            if 'score' in df.columns:
                top_20_df = df.sort_values(by='score', ascending=False).head(20)
            else:
                top_20_df = df.head(20)

            if 'smc_signal' in df.columns:
                smc_setups_df = df[df['smc_signal'] != 'NO_SETUP']
            else:
                smc_setups_df = df.iloc[0:0]

            top_20 = top_20_df.to_dict(orient='records')
            smc_setups = smc_setups_df.to_dict(orient='records')
        except Exception:
            top_20 = []
            smc_setups = []

    return render(request, "swing_dashboard.html", {
        'top_20': top_20,
        'smc_setups': smc_setups,
    })


def read_live_options_data() -> dict:
    try:
        if not LIVE_OPTIONS_FILE.exists():
            return {}
        with LIVE_OPTIONS_FILE.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


def live_option_chain_api(request):
    data = read_live_options_data()
    response_data = {
        "updated_at": data.get("updated_at"),
        "expiry_date": data.get("expiry_date"),
        "instrument_data": {},
    }

    for instrument, item in (data.get("instrument_data") or {}).items():
        oi = item.get("oi")
        previous_oi = item.get("previous_oi")
        coi = None
        oi_pct = None
        if oi is not None and previous_oi is not None:
            try:
                coi = oi - previous_oi
                oi_pct = round((coi / previous_oi) * 100, 2) if previous_oi else None
            except Exception:
                coi = None
                oi_pct = None

        response_data["instrument_data"][instrument] = {
            "ltp": item.get("ltp"),
            "volume": item.get("volume"),
            "oi": oi,
            "coi": coi,
            "oi_pct": oi_pct,
            "latest_tick_at": item.get("latest_tick_at"),
        }

    return JsonResponse(response_data)
