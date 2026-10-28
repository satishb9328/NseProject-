from django.core.cache import cache
from django.http import JsonResponse
from django.shortcuts import render
from pathlib import Path
import json
import pandas as pd

SWING_ML_CACHE_KEY = 'swing_ml_signals'
SWING_ML_CSV_FILE = Path(__file__).resolve().parent.parent / 'Swing_stocks' / 'nifty200_smc_precision_screen.csv'
LIVE_ALERTS_FILE = Path(__file__).resolve().parent / 'live_alerts.json'


def load_live_alerts():
    if not LIVE_ALERTS_FILE.exists():
        return []
    try:
        with LIVE_ALERTS_FILE.open('r', encoding='utf-8') as f:
            payload = json.load(f)
        return payload if isinstance(payload, list) else []
    except Exception:
        return []


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

    return render(request, 'swing_dashboard.html', {
        'top_20': top_20,
        'smc_setups': smc_setups,
    })
