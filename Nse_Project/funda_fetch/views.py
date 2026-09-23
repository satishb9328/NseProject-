"""
funda_fetch.views — "Master Trend & Setup Scanner" page.

Reads ``master_trend_setups.csv`` (produced by ``trend_ready.py`` in this same
folder) with pandas, groups it by ``Setup_Category``, and renders a
Tailwind-styled dashboard of the eight swing setups.
"""
from pathlib import Path

import pandas as pd
from django.core.cache import cache
from django.shortcuts import render

# MTF signal integration (data_engine + candlestick_scanner + chart_patterns all
# live at the project root). Imported defensively so a missing dependency never
# crashes the page — signal detection simply becomes a no-op.
try:
    from candlestick_scanner import scan_mtf_patterns
    from data_engine import build_mtf_dataframe
    from chart_patterns import detect_double_bottom, detect_trendline_breakout
except Exception:  # noqa: BLE001
    scan_mtf_patterns = None
    build_mtf_dataframe = None
    detect_double_bottom = None
    detect_trendline_breakout = None

# Live 2H "Sniper" bridge (live_ml_scanner.py in this same app). Imported
# defensively so the dashboard never crashes if the scanner is missing.
try:
    from .live_ml_scanner import main as run_live_scanner
except Exception:  # noqa: BLE001
    run_live_scanner = None

BASE_DIR = Path(__file__).resolve().parent
SETUPS_CSV = BASE_DIR / "master_trend_setups.csv"
UNIVERSE_CSV = BASE_DIR.parent / "ind_nifty500list.csv"

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
    """Coerce a CSV cell to float; None for missing/NaN values.

    Tolerates percentage / formatted strings (``"75.0%"``, ``"1,234.5"``) so
    thresholds like the 52W-position screen stay robust across CSV exports.
    """
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip().replace(",", "").replace("%", "")
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


# 🔥 HOT breakout-candidate thresholds — all three must hold for the badge.
HOT_52W_POSITION_MIN = 75.0   # "52W Position" column (pct_above_52w_low) > 75%
HOT_MOMENTUM_MIN = 50.0       # Momentum_Score > 50
HOT_VOL_DRY_MAX = 0.75        # Volume_Dry_Ratio < 0.75


# Frontend string mapping for the latest MTF candlestick signals.
SIGNAL_LABELS = {
    "hammer": "🔨 D-Hammer",
    "bullish_engulfing": "📈 D-Engulf",
    "fresh_golden_cross": "✨ Golden Cross",
    "weekly_hammer": "🔨 W-Hammer",
    "weekly_bullish_engulfing": "📈 W-Engulf",
    "weekly_bearish_engulfing": "📉 W-Bear Engulf",
    "double_bottom": "✌️ W-Pattern",
    "daily_tl": "🚀 D-TL Breakout",
    "weekly_tl": "🚀 W-TL Breakout",
}

# "Recent" lookback windows (trading days) for signal extraction.
STRUCTURAL_LOOKBACK = 3  # Golden Cross, Double Bottom, Trendline breakouts
CANDLE_LOOKBACK = 2      # Hammer / Engulfing candle shapes

# Signal keys grouped by their lookback window.
STRUCTURAL_SIGNAL_KEYS = {"fresh_golden_cross", "double_bottom", "daily_tl", "weekly_tl"}
CANDLE_SIGNAL_KEYS = {
    "hammer",
    "bullish_engulfing",
    "weekly_hammer",
    "weekly_bullish_engulfing",
    "weekly_bearish_engulfing",
}


def _load_isin_map():
    """Build {SYMBOL_UPPER: ISIN} from the Nifty 500 universe CSV."""
    mapping = {}
    if not UNIVERSE_CSV.exists():
        return mapping
    try:
        uni = pd.read_csv(UNIVERSE_CSV)
        uni.columns = [str(c).strip() for c in uni.columns]
        sym_col = next((c for c in uni.columns if c.lower() == "symbol"), None)
        isin_col = next((c for c in uni.columns if "isin" in c.lower()), None)
        if sym_col is None or isin_col is None:
            return mapping
        for sym, isin in zip(uni[sym_col], uni[isin_col]):
            key = str(sym).strip().upper()
            if key and pd.notna(isin):
                mapping[key] = str(isin).strip()
    except Exception:
        pass
    return mapping


ISIN_MAP = _load_isin_map()

# EOD data — signals are stable intraday, so cache the badge list for 12 hours.
SIGNAL_CACHE_TIMEOUT = 43200  # seconds


def _detect_signals(symbol: str, isin) -> list:
    """Return the cached MTF signal badges for one stock.

    The first request runs the parquet + scanner pipeline (candlestick patterns
    plus the double-bottom and trendline-breakout chart patterns) and stores the
    badge list in Django's cache for 12 hours; every subsequent request returns
    it instantly. Failures (missing parquet, unreadable file, scanner
    unavailable) are never cached — they simply render as "no signals" and are
    retried next request.

    Signals use a "recent" lookback rather than the strict latest bar:
    structural patterns (Golden Cross, Double Bottom, trendline breakouts) are
    flagged if True on any of the last 3 bars; candle shapes (hammer / engulfing)
    are flagged if True on any of the last 2 bars. ``iloc[-n:]`` clamps safely
    when a series is shorter than its window.
    """
    if (
        build_mtf_dataframe is None
        or scan_mtf_patterns is None
        or detect_double_bottom is None
        or detect_trendline_breakout is None
    ):
        return []

    cache_key = f"signals_{symbol.upper()}"

    try:
        cached = cache.get(cache_key)
    except Exception:  # noqa: BLE001 — a cache outage must never break the page
        cached = None
    if cached is not None:
        return cached

    try:
        df = build_mtf_dataframe(symbol, isin=isin)
        signals_df = scan_mtf_patterns(df)
        w_pattern = detect_double_bottom(df)
        d_tl = detect_trendline_breakout(df, high_col="High", close_col="Close")
        w_tl = detect_trendline_breakout(
            df, high_col="Weekly_High", close_col="Weekly_Close"
        )
    except Exception:
        return []  # transient failure — don't cache, so it retries next time

    # Collect every signal series keyed by its SIGNAL_LABELS key.
    series_map: dict = {}
    if signals_df is not None and not signals_df.empty:
        for col in signals_df.columns:
            if col in SIGNAL_LABELS:
                series_map[col] = signals_df[col]
    if w_pattern is not None and len(w_pattern):
        series_map["double_bottom"] = w_pattern
    if d_tl is not None and len(d_tl):
        series_map["daily_tl"] = d_tl
    if w_tl is not None and len(w_tl):
        series_map["weekly_tl"] = w_tl

    # Apply the per-category "recent" lookback window.
    signals = []
    for key, label in SIGNAL_LABELS.items():
        series = series_map.get(key)
        if series is None:
            continue
        window = CANDLE_LOOKBACK if key in CANDLE_SIGNAL_KEYS else STRUCTURAL_LOOKBACK
        if bool(series.iloc[-window:].any()):
            signals.append(label)

    try:
        cache.set(cache_key, signals, SIGNAL_CACHE_TIMEOUT)
    except Exception:  # noqa: BLE001
        pass

    return signals


def _build_row(raw, live_alerts=None):
    symbol = _to_str(raw.get("symbol"))
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

    # 🔥 HOT breakout candidate: top-quartile 52W position, still-bullish
    # momentum, and dried-up volume (all three must hold).
    is_hot = (
        pos is not None and pos > HOT_52W_POSITION_MIN
        and momentum is not None and momentum > HOT_MOMENTUM_MIN
        and vol is not None and vol < HOT_VOL_DRY_MAX
    )

    # Latest MTF candlestick signals from the data_engine + scanner pipeline.
    signals = _detect_signals(symbol, ISIN_MAP.get(symbol.upper()))

    # Live 2H sniper pattern (from the cached live scanner bridge), if any.
    if live_alerts:
        live_pattern = live_alerts.get(symbol.upper())
        if live_pattern:
            signals.append(live_pattern)

    return {
        "symbol": symbol,
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
        "is_hot": is_hot,
        "signals": signals,
    }


LIVE_SNIPER_CACHE_KEY = "live_sniper_alerts"
LIVE_SNIPER_CACHE_TIMEOUT = 300  # 5 minutes


def get_live_sniper_alerts() -> dict:
    """Return a cached ``{SYMBOL: pattern_label}`` map from the 2H live sniper bot.

    ``run_live_scanner()`` is expensive (per-symbol Upstox fetches), so its result
    is cached for 5 minutes (300 seconds) and served to every dashboard render.
    """
    if run_live_scanner is None:
        return {}

    try:
        cached = cache.get(LIVE_SNIPER_CACHE_KEY)
    except Exception:  # noqa: BLE001
        cached = None
    if cached is not None:
        return cached

    try:
        result = run_live_scanner()
        signals = result.get("signals", []) if isinstance(result, dict) else []
        mapping = {
            str(a.get("symbol", "")).strip().upper(): a.get("pattern")
            for a in signals
            if a.get("symbol") and a.get("pattern")
        }
    except Exception:  # noqa: BLE001
        mapping = {}

    try:
        cache.set(LIVE_SNIPER_CACHE_KEY, mapping, LIVE_SNIPER_CACHE_TIMEOUT)
    except Exception:  # noqa: BLE001
        pass

    return mapping


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

    # Live 2H sniper alerts (symbol -> pattern label), cached for 5 minutes.
    live_alerts = get_live_sniper_alerts()

    sections, cards = [], []
    for cat in CATEGORY_ORDER:
        badge, dot, count_color, desc = CATEGORY_META[cat]
        rows = []
        if cat_col and not df.empty:
            sub = df[df[cat_col].astype(str).str.strip() == cat]
            rows = [_build_row(r, live_alerts) for _, r in sub.iterrows()]
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
