"""Expiry-aware option-chain keying helpers shared by every live-options consumer.

Why this module exists
----------------------
``upstox_live_feed.py`` publishes ONE flat ``instrument_data`` map keyed by the Upstox
``instrument_key``. Every consumer (the Django dashboard API, the Gamma engine) folds
that map into option-chain ROWS. While those rows were keyed by the bare
``strike_price``, any payload that happened to carry two expiries — a second feed
instance publishing to the same Redis key, a snapshot restored across a rollover, a
half-completed subscription swap — silently merged them: the CE leg of one expiry and
the PE leg of another landed in the SAME row and the last writer won. That is invisible
in the UI and catastrophic in the maths, because COI/GEX of two unrelated chains get
added together.

Rows are therefore keyed by ``(expiry_date, strike_price)`` everywhere, and the expiry
is resolved through the helpers below so the producer and all consumers agree on the
answer for a given instrument.

Expiry resolution order
-----------------------
1. An explicit ``expiry`` / ``expiry_date`` field on the instrument entry (the feed
   writes this from the instrument master file, so it is authoritative).
2. The expiry code embedded in the Upstox tradingsymbol, which is the only marker
   legacy snapshots carry:
     * weekly  ``NIFTY`` + ``YY`` + ``M`` + ``DD``  -> ``NIFTY2690125750PE`` = 2026-09-01
       (month code ``1``-``9`` = Jan-Sep, ``O`` = Oct, ``N`` = Nov, ``D`` = Dec)
     * monthly ``NIFTY`` + ``YY`` + ``MMM``         -> ``NIFTY26AUG24300CE`` = 2026-08-*
       (the day is not encoded, so this resolves to MONTH scope only)
3. Unknown — never treated as a conflict, so an unrecognised symbol is grouped with the
   active expiry instead of being thrown away.

This module deliberately has no third-party imports so the feed, Django and the Gamma
engine can all use it.
"""
from __future__ import annotations

import re
from datetime import date, datetime

__all__ = [
    "MONTH_CODES",
    "MONTH_NAMES",
    "normalize_expiry",
    "parse_symbol_expiry",
    "resolve_row_expiry",
    "expiries_conflict",
    "expiry_group_key",
    "belongs_to_expiry",
    "strike_row_key",
]

# Weekly expiry codes: one character per month (Oct/Nov/Dec become O/N/D so the code
# stays a single character and the strike can follow immediately).
MONTH_CODES = {
    1: "1", 2: "2", 3: "3", 4: "4", 5: "5", 6: "6",
    7: "7", 8: "8", 9: "9", 10: "O", 11: "N", 12: "D",
}
_CODE_TO_MONTH = {code: month for month, code in MONTH_CODES.items()}

MONTH_NAMES = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# NIFTY2690125750PE -> ('26', '9', '01'); a letter can never start the expiry code, so
# sibling indices (NIFTYNXT50..., MIDCPNIFTY...) simply fail to match.
_WEEKLY_SYMBOL_RE = re.compile(r"^[A-Z]+(\d{2})([1-9OND])(\d{2})\d+(?:CE|PE)$")
# NIFTY26AUG24300CE -> ('26', 'AUG')
_MONTHLY_SYMBOL_RE = re.compile(
    r"^[A-Z]+(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d+(?:CE|PE)$"
)

_FULL_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")
_MONTH_ONLY_RE = re.compile(r"^(\d{4})-(\d{2})$")

_EXPIRY_FIELDS = ("expiry", "expiry_date", "expirydate")
_SYMBOL_FIELDS = ("tradingsymbol", "trading_symbol", "symbol")

# String spellings of "no value" that reach us after a JSON round-trip or a str() cast of
# a pandas/numpy sentinel. "NaT" is the important one: pandas' NaT is a datetime SUBCLASS,
# so a naive isinstance(value, datetime) branch happily calls .isoformat() on it and gets
# the string "NaT" back - a truthy, plausible-looking token that then compares as a REAL
# expiry and silently drops every instrument carrying it.
_MISSING_TOKENS = {"", "-", "nan", "nat", "none", "null", "na", "n/a", "nonetype"}


def _is_missing(value) -> bool:
    """True for None, NaN, pandas ``NaT``, ``numpy.datetime64('NaT')`` and their spellings.

    Deliberately duck-typed instead of importing pandas/numpy: this module is shared by the
    Django dashboard, the Gamma engine and the feed, and must stay dependency-free. Every
    NaN-like sentinel (float('nan'), np.nan, pd.NaT, np.datetime64('NaT')) has the defining
    property that it is NOT equal to itself, which is what the identity test below exploits.
    """
    if value is None:
        return True
    try:
        if value != value:  # NaN / NaT are never equal to themselves
            return True
    except Exception:
        # Exotic objects with a broken __ne__ are treated as present, then rejected below
        # by the format checks if they cannot produce a usable date.
        pass
    if isinstance(value, str):
        return value.strip().lower() in _MISSING_TOKENS
    return False


def normalize_expiry(value) -> str | None:
    """Return ``YYYY-MM-DD`` (day scope) or ``YYYY-MM`` (month scope), else None.

    Accepts ``date``/``datetime`` objects (the feed holds real dates in
    ``INSTRUMENT_METADATA``) and the ISO strings that survive a JSON round-trip.

    Anything missing - None, NaN, pandas ``NaT``, the strings "nan"/"NaT"/"" - returns None,
    i.e. "unknown". That distinction is critical: an unknown expiry NEVER conflicts, so a
    row with a missing expiry is grouped with the active chain, whereas a bogus token like
    "NaT" would have compared as a different expiry and silently dropped the row.
    """
    if _is_missing(value) or isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        # NaT is filtered out above; a real datetime is safe to project onto its date.
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    match = _FULL_DATE_RE.match(text)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    match = _MONTH_ONLY_RE.match(text)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    return None


def parse_symbol_expiry(symbol) -> str | None:
    """Return the expiry encoded in an Upstox option tradingsymbol, or None.

    Weekly codes resolve to a full ``YYYY-MM-DD``; monthly codes only encode the month,
    so they resolve to ``YYYY-MM`` (month scope) and are compared accordingly.
    """
    if _is_missing(symbol) or not isinstance(symbol, str):
        return None
    text = symbol.strip().upper().replace(" ", "")
    if not text:
        return None

    match = _WEEKLY_SYMBOL_RE.match(text)
    if match:
        year = 2000 + int(match.group(1))
        month = _CODE_TO_MONTH.get(match.group(2))
        day = int(match.group(3))
        if month and 1 <= day <= 31:
            return f"{year:04d}-{month:02d}-{day:02d}"
        return None

    match = _MONTHLY_SYMBOL_RE.match(text)
    if match:
        year = 2000 + int(match.group(1))
        month = MONTH_NAMES[match.group(2)]
        return f"{year:04d}-{month:02d}"

    return None


def resolve_row_expiry(item, default=None) -> str | None:
    """Return the normalized expiry for one ``instrument_data`` entry.

    ``default`` is returned only when the entry carries no usable marker at all, so a
    caller can decide whether "unknown" should inherit the active expiry or stay None.
    """
    if isinstance(item, dict):
        for field in _EXPIRY_FIELDS:
            normalized = normalize_expiry(item.get(field))
            if normalized:
                return normalized
        for field in _SYMBOL_FIELDS:
            parsed = parse_symbol_expiry(item.get(field))
            if parsed:
                return parsed
    return normalize_expiry(default)


def expiries_conflict(left, right) -> bool:
    """True only when both expiries are known AND provably different.

    Month-scope values (from monthly tradingsymbols) match any day inside that month, so
    ``2026-09`` never conflicts with ``2026-09-01`` while ``2026-08`` does. An unknown
    expiry never conflicts: this function must only return True when we can PROVE the two
    values belong to different chains.
    """
    first = normalize_expiry(left)
    second = normalize_expiry(right)
    if not first or not second or first == second:
        return False
    shorter, longer = sorted((first, second), key=len)
    return not longer.startswith(shorter)


def expiry_group_key(item, active_expiry=None) -> str:
    """Return the expiry half of the ``(expiry_date, strike_price)`` row key.

    Everything that does not provably belong to another chain is folded into the ACTIVE
    expiry bucket, so both legs of a strike always merge into one row even when only one
    of them carries an expiry marker. A genuinely foreign expiry keeps its own bucket and
    can therefore never overwrite an active-expiry row.
    """
    active = normalize_expiry(active_expiry)
    resolved = resolve_row_expiry(item)
    if resolved is None:
        return active or ""
    if not expiries_conflict(resolved, active):
        return active or resolved
    return resolved


def belongs_to_expiry(item, active_expiry) -> bool:
    """True unless ``item`` provably belongs to an expiry other than ``active_expiry``."""
    active = normalize_expiry(active_expiry)
    if not active:
        return True
    return not expiries_conflict(resolve_row_expiry(item), active)


def strike_row_key(item, strike, active_expiry=None) -> tuple:
    """Return the composite ``(expiry_date, strike_price)`` row key."""
    return (expiry_group_key(item, active_expiry), strike)
