"""
Views for the 52-Week-Low SMC "Queue Ticks" dashboard.

Reads the two CSVs written by the 52-week-low SMC scanner and exposes them to
``queue_ticks.html``:

    * ``nifty500_pure_smc_trade_setups.csv``  ->  ``trade_setups``
    * ``near_miss_watchlist.csv``             ->  ``near_misses``

The scanner has evolved over time, so the actual column names do not always
match the original specification (e.g. ``R:R`` vs ``RRR`` and ``Status`` vs
``Reason``). The loaders below therefore normalise every row into a stable,
template-friendly shape instead of trusting the raw header names.
"""

import logging
import math
from datetime import datetime
from pathlib import Path

import pandas as pd
from django.shortcuts import render

logger = logging.getLogger(__name__)

# The scanner runs against the NIFTY 500 universe.
TOTAL_SCANNED = 500

BASE_DIR = Path(__file__).resolve().parent

TRADE_SETUPS_CSV = BASE_DIR / "nifty500_pure_smc_trade_setups.csv"
NEAR_MISS_CSV = BASE_DIR / "near_miss_watchlist.csv"


def _coerce(value):
    """Return a JSON/template-friendly Python scalar.

    Converts numpy/pandas scalars to their native Python equivalents and turns
    blank strings and NaN/NaT/``pd.NA`` into ``None`` so the template never has
    to render ``nan`` or stray whitespace.
    """
    if value is None:
        return None

    if isinstance(value, str):
        value = value.strip()
        return value or None

    item = getattr(value, "item", None)
    if callable(item):
        try:
            value = item()
        except (AttributeError, ValueError):
            pass

    # NaN / NaT / pd.NA are "missing" for our purposes and must not leak into
    # the template (or a future JSON payload).
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, float) and math.isnan(value):
        return None

    return value


def _read_csv_records(csv_path: Path):
    """Return ``DataFrame.to_dict('records')`` for a CSV, or ``[]`` on error."""
    if not csv_path.exists():
        logger.warning("CSV not found: %s", csv_path)
        return []

    try:
        df = pd.read_csv(csv_path)
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError, ValueError) as exc:
        logger.exception("Failed to read CSV %s: %s", csv_path, exc)
        return []

    # Replace NaN/NaT with None so the template and any future JSON
    # serialisation never have to deal with float('nan').
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")


def _first(row: dict, *keys: str):
    """Return the first present, non-blank value among ``keys``."""
    for key in keys:
        if key not in row:
            continue
        value = _coerce(row.get(key))
        if value is None:
            continue
        return value
    return None


def _normalise_trade_setups(records):
    """Map raw setup rows onto the canonical keys used by ``queue_ticks.html``."""
    setups = []
    for row in records:
        setups.append({
            "symbol": _first(row, "Symbol", "symbol", "Ticker"),
            "price": _first(row, "Price", "price", "CMP", "LTP"),
            "entry": _first(row, "Entry", "entry"),
            "sl": _first(row, "SL", "sl", "StopLoss", "stop_loss", "Stop Loss"),
            "target": _first(row, "Target", "target"),
            "rrr": _first(row, "RRR", "R:R", "rrr", "RR", "Risk Reward"),
            "reasons": _first(row, "Reasons", "Reason", "Status", "Diag", "reasons"),
        })
    return setups


def _normalise_near_misses(records):
    """Map raw near-miss rows onto the canonical keys used by ``queue_ticks.html``."""
    rows = []
    for row in records:
        rows.append({
            "symbol": _first(row, "Symbol", "symbol", "Ticker"),
            "price": _first(row, "Price", "price", "CMP", "LTP"),
            "reason": _first(row, "Reason", "Reasons", "Status", "reason", "Diag"),
            "entry": _first(row, "Entry", "entry"),
            "sl": _first(row, "SL", "sl", "StopLoss", "stop_loss", "Stop Loss"),
            "target": _first(row, "Target", "target"),
            "rrr": _first(row, "RRR", "R:R", "rrr", "RR", "Risk Reward"),
        })
    return rows


def _latest_updated():
    """Return the newest scanner file mtime as a human-readable string."""
    existing = [p for p in (TRADE_SETUPS_CSV, NEAR_MISS_CSV) if p.exists()]
    if not existing:
        return None
    latest_ts = max(p.stat().st_mtime for p in existing)
    return datetime.fromtimestamp(latest_ts).strftime("%d %b %Y, %H:%M:%S")


def queue_ticks_smc_view(request):
    """Render the SMC 52-week-low queue-ticks dashboard."""
    trade_setups = _normalise_trade_setups(_read_csv_records(TRADE_SETUPS_CSV))
    near_misses = _normalise_near_misses(_read_csv_records(NEAR_MISS_CSV))

    context = {
        "trade_setups": trade_setups,
        "near_misses": near_misses,
        "ready_count": len(trade_setups),
        "near_miss_count": len(near_misses),
        "total_scanned": TOTAL_SCANNED,
        "last_updated": _latest_updated(),
    }
    return render(request, "queue_ticks.html", context)
