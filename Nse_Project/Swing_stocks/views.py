import json
import os
from pathlib import Path

import pandas as pd
from django.http import JsonResponse
from django.shortcuts import render

from .combo_filter import filter_combo_setups

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SWING_STOCKS_CSV_FILE = Path(BASE_DIR) / "nifty200_smc_precision_screen.csv"
LIVE_ALERTS_FILE = Path(__file__).resolve().parent.parent / 'Swing_ML' / 'live_alerts.json'


def load_live_alerts():
    if not LIVE_ALERTS_FILE.exists():
        return []
    try:
        with LIVE_ALERTS_FILE.open('r', encoding='utf-8') as f:
            payload = json.load(f)
        return payload if isinstance(payload, list) else []
    except Exception:
        return []


def swing_dashboard(request):
    top_20 = []
    smc_setups = []
    categories = {
        "PERFECT_COMBO": [],
        "STRONG_SEPA": [],
        "SMC_ACTIVE": [],
    }

    if SWING_STOCKS_CSV_FILE.exists():
        try:
            df = pd.read_csv(SWING_STOCKS_CSV_FILE)
            df = df.where(pd.notnull(df), None)

            if "score" in df.columns:
                top_20_df = df.sort_values(by="score", ascending=False).head(20)
            else:
                top_20_df = df.head(20)

            if "smc_signal" in df.columns:
                smc_setups_df = df[df["smc_signal"] != "NO_SETUP"]
            else:
                smc_setups_df = df.iloc[0:0]

            categories = filter_combo_setups(df)
            top_20 = top_20_df.to_dict(orient="records")
            smc_setups = smc_setups_df.to_dict(orient="records")
        except Exception:
            top_20 = []
            smc_setups = []
            categories = {
                "PERFECT_COMBO": [],
                "STRONG_SEPA": [],
                "SMC_ACTIVE": [],
            }

    context = {
        "top_20": top_20,
        "smc_setups": smc_setups,
        "perfect_combo": categories.get("PERFECT_COMBO", []),
        "strong_sepa": categories.get("STRONG_SEPA", []),
        "smc_active": categories.get("SMC_ACTIVE", []),
    }

    print("Swing_stocks dashboard context:")
    print(f"  PERFECT COMBO: {len(context['perfect_combo'])}")
    print(f"  STRONG SEPA : {len(context['strong_sepa'])}")
    print(f"  SMC ACTIVE  : {len(context['smc_active'])}")

    return render(request, "swing_dashboard.html", context)


def swing_stocks_view(request):
    context = {
        "perfect_combo": [],
        "strong_sepa": [],
        "smc_active": [],
    }
    if SWING_STOCKS_CSV_FILE.exists():
        try:
            df = pd.read_csv(SWING_STOCKS_CSV_FILE).where(lambda value: pd.notnull(value), None)
            categories = filter_combo_setups(df)
            context.update({
                "perfect_combo": categories.get("PERFECT_COMBO", []),
                "strong_sepa": categories.get("STRONG_SEPA", []),
                "smc_active": categories.get("SMC_ACTIVE", []),
            })
        except Exception:
            pass
    return render(request, "swing_stocks.html", context)


def get_live_alerts(request):
    alerts = load_live_alerts()
    return JsonResponse({'alerts': alerts})
