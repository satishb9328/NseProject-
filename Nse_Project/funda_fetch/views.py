"""
funda_fetch.views — "Master Trend & Setup Scanner" page.

Reads ``master_trend_setups.csv`` (produced by ``trend_ready.py`` in this same
folder) with pandas, groups it by ``Setup_Category``, and renders a
Tailwind-styled dashboard of the eight swing setups.
"""
from pathlib import Path

import pandas as pd
from django.shortcuts import render

BASE_DIR = Path(__file__).resolve().parent
SETUPS_CSV = BASE_DIR / "master_trend_setups.csv"

CATEGORY_ORDER = [
    "Bottom Fishing",
    "Oversold BB20 Reclaim",
    "Mega Reversal (Weekly BB20)",
    "BB Lower Band Snap (Weekly)",
    "BB Lower Band Snap (Daily)",
    "Mid-Range Bounce",
    "VCP Breakout",
    "Consolidation / Flag",
]

# category -> (badge classes, dot color, count color, description)
CATEGORY_META = {
    "Bottom Fishing": (
        "bg-neon-green/10 text-neon-green border-neon-green/30",
        "bg-neon-green",
        "text-neon-green",
        "Within 10% of the 52-week low with a reversal signature.",
    ),
    "Oversold BB20 Reclaim": (
        "bg-neon-mint/10 text-neon-mint border-neon-mint/30",
        "bg-neon-mint",
        "text-neon-mint",
        "Recovering from oversold (RSI < 35) and reclaiming the BB20 midline with dual-volume dry-up.",
    ),
    "Mega Reversal (Weekly BB20)": (
        "bg-neon-rose/10 text-neon-rose border-neon-rose/30",
        "bg-neon-rose",
        "text-neon-rose",
        "Macro reversal: weekly RSI dipped below 35 in 15 weeks, daily reclaiming the weekly BB20 midline.",
    ),
    "BB Lower Band Snap (Weekly)": (
        "bg-orange-400/10 text-orange-300 border-orange-400/30",
        "bg-orange-400",
        "text-orange-300",
        "Weekly low pierced the weekly BB20 lower band, then the weekly close reclaimed it with RSI < 40.",
    ),
    "BB Lower Band Snap (Daily)": (
        "bg-yellow-400/10 text-yellow-300 border-yellow-400/30",
        "bg-yellow-400",
        "text-yellow-300",
        "Daily low pierced the daily BB20 lower band, then a bullish close reclaimed it with RSI < 40.",
    ),
    "Mid-Range Bounce": (
        "bg-neon-blue/10 text-neon-blue border-neon-blue/30",
        "bg-neon-blue",
        "text-neon-blue",
        "40-60% of the 52-week range, pulling back to the 20/50 EMA.",
    ),
    "VCP Breakout": (
        "bg-neon-violet/10 text-neon-violet border-neon-violet/30",
        "bg-neon-violet",
        "text-neon-violet",
        "Weekly 20-EMA cross/bounce with a volatility contraction or inside bar.",
    ),
    "Consolidation / Flag": (
        "bg-neon-amber/10 text-neon-amber border-neon-amber/30",
        "bg-neon-amber",
        "text-neon-amber",
        "Established uptrend (50 EMA > 200 EMA) coiling sideways.",
    ),
}


def _to_float(value):
    """Coerce a CSV cell to float; None for missing/NaN values."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(f) else f


def _to_str(value):
    """Coerce a CSV cell to a stripped string; '' for missing/NaN values."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _to_bool(value):
    """Coerce a CSV boolean (True/False/1/0/Yes/No) to a Python bool."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "y")
    return bool(value)


def _fmt(value, nd=2):
    """Format a float with thousands separators; em-dash for None."""
    if value is None:
        return "—"
    try:
        return f"{float(value):,.{nd}f}"
    except (TypeError, ValueError):
        return "—"


def _build_row(raw):
    signal = _to_str(raw.get("weekly_ema20_signal"))
    vol = _to_float(raw.get("Volume_Dry_Ratio"))
    wvol = _to_float(raw.get("Weekly_Vol_Ratio"))
    momentum = _to_float(raw.get("Momentum_Score"))
    pos = _to_float(raw.get("pct_above_52w_low"))

    if signal == "Cross":
        badge = "bg-neon-green/10 text-neon-green border-neon-green/30"
    elif signal == "Touch":
        badge = "bg-neon-blue/10 text-neon-blue border-neon-blue/30"
    else:
        badge = "bg-ink-700/40 text-slate-500 border-ink-600"

    if vol is None:
        vol_cls = "text-slate-500"
    elif vol < 0.75:
        vol_cls = "text-neon-green font-bold"   # strong volume contraction
    elif vol > 1.2:
        vol_cls = "text-neon-red"               # volume expansion / climax
    else:
        vol_cls = "text-slate-300"

    if wvol is None:
        wvol_cls = "text-slate-500"
    elif wvol < 0.80:
        wvol_cls = "text-neon-green font-bold"  # weekly volume contraction
    elif wvol >= 1.2:
        wvol_cls = "text-neon-red"              # weekly volume expansion
    else:
        wvol_cls = "text-slate-300"

    return {
        "symbol": _to_str(raw.get("symbol")),
        "close": _fmt(_to_float(raw.get("last_close")), 2),
        "pos_52w": (_fmt(pos, 1) + "%") if pos is not None else "—",
        "signal": signal,
        "signal_badge": badge,
        "momentum": _fmt(momentum, 1),
        "momentum_sort": momentum if momentum is not None else -1.0,
        "vol_display": _fmt(vol, 2),
        "vol_class": vol_cls,
        "wvol_display": _fmt(wvol, 2),
        "wvol_class": wvol_cls,
        "uptrend": _to_bool(raw.get("uptrend_50_200")),
    }


def trend_setups_view(request):
    """Render the grouped Master Trend & Setup Scanner dashboard."""
    df = pd.DataFrame()
    if SETUPS_CSV.exists():
        try:
            df = pd.read_csv(SETUPS_CSV)
        except Exception:
            df = pd.DataFrame()

    cat_col = next((c for c in df.columns if c.strip().lower() == "setup_category"), None)
    total = len(df) if (not df.empty and cat_col) else 0

    sections, cards = [], []
    for cat in CATEGORY_ORDER:
        badge, dot, count_color, desc = CATEGORY_META[cat]
        rows = []
        if cat_col and not df.empty:
            sub = df[df[cat_col].astype(str).str.strip() == cat]
            rows = [_build_row(r) for _, r in sub.iterrows()]
            rows.sort(key=lambda r: r["momentum_sort"], reverse=True)
        cards.append({"category": cat, "count": len(rows), "dot": dot,
                      "count_color": count_color, "description": desc})
        sections.append({"category": cat, "count": len(rows), "badge": badge,
                         "description": desc, "rows": rows})

    return render(request, "trend_setups.html", {
        "total": total,
        "summary_cards": cards,
        "sections": sections,
    })
