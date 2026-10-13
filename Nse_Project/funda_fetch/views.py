"""
funda_fetch.views — "Master Trend & Setup Scanner" page.

Reads pre-computed rows from the ``TrendSetup`` database table (populated in the
background by the ``update_trend_setups`` management command), groups them by
``Setup_Category``, and renders the Tailwind-styled dashboard.

The request path is a pure DB read plus cheap formatting — no pandas, no parquet,
no Upstox API calls, and no candlestick/chart-pattern scanning happens here.
"""
from django.shortcuts import render
from django.http import JsonResponse
from django.utils import timezone

from .models import LiveSignal, TrendSetup


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


def _fmt(value, nd=2):
    """Format a float with thousands separators; em-dash for None."""
    if value is None:
        return "—"
    try:
        return f"{float(value):,.{nd}f}"
    except (TypeError, ValueError):
        return "—"


def _build_row(obj):
    """Convert a TrendSetup row into the dict shape the template expects."""
    signal = obj.weekly_ema_signal or ""
    vol = obj.volume_dry_ratio
    wvol = obj.weekly_vol_dry
    momentum = obj.momentum
    pos = obj.position_52w

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

    signals = list(obj.signals or [])
    if obj.live_sniper_pattern:
        signals.append(obj.live_sniper_pattern)

    return {
        "symbol": obj.symbol,
        "close": _fmt(obj.close_price, 2),
        "pos_52w": (_fmt(pos, 1) + "%") if pos is not None else "—",
        "signal": signal,
        "signal_badge": badge,
        "momentum": _fmt(momentum, 1),
        "momentum_sort": momentum if momentum is not None else -1.0,
        "vol_display": _fmt(vol, 2),
        "vol_class": vol_cls,
        "wvol_display": _fmt(wvol, 2),
        "wvol_class": wvol_cls,
        "uptrend": bool(obj.trend_status),
        "is_hot": bool(obj.is_hot),
        "signals": signals,
    }


def trend_setups_view(request):
    """Render the grouped Master Trend & Setup Scanner dashboard from the DB."""
    setups = TrendSetup.objects.all()

    grouped = {cat: [] for cat in CATEGORY_ORDER}
    for obj in setups:
        if obj.setup_category in grouped:
            grouped[obj.setup_category].append(obj)

    total = sum(len(rows) for rows in grouped.values())

    sections, cards = [], []
    for cat in CATEGORY_ORDER:
        badge, dot, count_color, desc = CATEGORY_META[cat]
        rows = [_build_row(obj) for obj in grouped[cat]]
        # Group 🔥 HOT candidates at the very top; within each group, sort by
        # momentum descending so the fastest-moving hot stocks lead the list.
        rows.sort(key=lambda r: (not r["is_hot"], -r["momentum_sort"]))
        cards.append({"category": cat, "count": len(rows), "dot": dot,
                      "count_color": count_color, "description": desc})
        sections.append({"category": cat, "count": len(rows), "badge": badge,
                         "description": desc, "rows": rows})

    return render(request, "trend_setups.html", {
        "total": total,
        "summary_cards": cards,
        "sections": sections,
    })


def live_signals_today(request):
    """Return today's persisted live scanner signals as JSON."""
    rows = LiveSignal.objects.filter(date=timezone.localdate()).order_by("-created_at")
    return JsonResponse({
        "signals": [
            {
                "stock_symbol": row.stock_symbol,
                "signal_type": row.signal_type,
                "trigger_price": str(row.trigger_price),
                "weekly_midline": str(row.weekly_midline),
                "created_at": row.created_at.isoformat(),
            }
            for row in rows
        ]
    })
