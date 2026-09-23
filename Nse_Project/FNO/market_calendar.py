"""
═══════════════════════════════════════════════════════════════════════
  NSE MARKET HOLIDAY CALENDAR
═══════════════════════════════════════════════════════════════════════
  Single, maintainable source of truth for NSE trading holidays.

  Holiday data is resolved in priority order:

    1. ``holidays.json``  — drop a JSON file next to this module to
       override / extend the calendar *without touching code*. This is the
       intended target for a future API sync job: fetch NSE's published
       calendar, dump it to ``holidays.json``, then call
       ``refresh_holidays()`` (or restart the process).

         Expected JSON shape (either is accepted):
             ["2026-01-26", "2026-03-08", ...]
             {"holidays": ["2026-01-26", ...]}

    2. ``_DEFAULT_HOLIDAYS`` — hardcoded fallback used when no JSON file
       is present, so the engine always has a working baseline even
       offline. Update this set annually as NSE publishes new dates.

  Usage from the engine::

      import market_calendar
      market_calendar.is_trading_day(some_date)
═══════════════════════════════════════════════════════════════════════
"""

import datetime
import json
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_HOLIDAYS_JSON = os.path.join(_HERE, 'holidays.json')

# ── Built-in fallback (UPDATE ANNUALLY as NSE publishes the calendar) ──
_DEFAULT_HOLIDAYS = {
    # 2024
    datetime.date(2024, 1, 22), datetime.date(2024, 3, 8),
    datetime.date(2024, 3, 25), datetime.date(2024, 3, 29),
    datetime.date(2024, 4, 11), datetime.date(2024, 4, 17),
    datetime.date(2024, 5, 1),  datetime.date(2024, 5, 20),
    datetime.date(2024, 6, 17), datetime.date(2024, 7, 17),
    datetime.date(2024, 8, 15), datetime.date(2024, 10, 2),
    datetime.date(2024, 11, 1), datetime.date(2024, 11, 15),
    datetime.date(2024, 11, 20), datetime.date(2024, 12, 25),
    # 2025
    datetime.date(2025, 2, 26), datetime.date(2025, 3, 14),
    datetime.date(2025, 3, 31), datetime.date(2025, 4, 10),
    datetime.date(2025, 4, 14), datetime.date(2025, 4, 18),
    datetime.date(2025, 5, 1),  datetime.date(2025, 8, 15),
    datetime.date(2025, 8, 27), datetime.date(2025, 10, 2),
    datetime.date(2025, 10, 21), datetime.date(2025, 10, 22),
    datetime.date(2025, 11, 5), datetime.date(2025, 12, 25),
    # 2026 — add when NSE publishes
}


def load_market_holidays(json_path: str = None) -> set:
    """Return the resolved ``set[datetime.date]`` of market holidays.

    Merges ``_DEFAULT_HOLIDAYS`` with any dates found in ``json_path``
    (defaulting to ``holidays.json`` beside this module). The built-in set
    is always included, so a missing/corrupt JSON file never breaks the
    engine — it simply falls back to the hardcoded baseline.
    """
    holidays = set(_DEFAULT_HOLIDAYS)
    path = json_path or _HOLIDAYS_JSON

    if not os.path.exists(path):
        return holidays

    try:
        with open(path, 'r', encoding='utf-8') as fh:
            payload = json.load(fh)

        # Accept either a bare list of ISO strings or {"holidays": [...]}.
        raw = payload if isinstance(payload, list) else payload.get('holidays', [])
        for item in raw:
            if isinstance(item, str):
                holidays.add(datetime.date.fromisoformat(item))
            elif isinstance(item, dict):
                value = item.get('date') or item.get('holiday')
                if value:
                    holidays.add(datetime.date.fromisoformat(str(value)))
    except (ValueError, TypeError, OSError, json.JSONDecodeError):
        # Corrupt / malformed config → silently keep the built-in baseline.
        pass

    return holidays


def refresh_holidays(json_path: str = None) -> set:
    """Re-read the calendar (e.g. after an API sync rewrites holidays.json).

    Updates the module-level ``MARKET_HOLIDAYS`` set in place so that any
    caller using ``is_trading_day`` / ``is_holiday`` immediately sees the
    refreshed dates.
    """
    global MARKET_HOLIDAYS
    MARKET_HOLIDAYS = load_market_holidays(json_path)
    return MARKET_HOLIDAYS


def is_holiday(dt: datetime.date) -> bool:
    """True if ``dt`` is an NSE market holiday."""
    return dt in MARKET_HOLIDAYS


def is_trading_day(dt: datetime.date) -> bool:
    """True if ``dt`` is an NSE trading day (weekday, not a holiday)."""
    return dt.weekday() < 5 and not is_holiday(dt)


# ── Module-level resolved calendar (importable by the engine) ──────────
MARKET_HOLIDAYS = load_market_holidays()
