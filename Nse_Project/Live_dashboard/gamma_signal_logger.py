"""Event-Driven Signal Logger for the "NIFTY DAY GAMMA" signal.

Completely standalone (Python standard library ONLY) so it can be imported by
both the live feed worker (``upstox_live_feed.py``) and the Gamma engine
(``gamma_blast_engine.py``) without pulling in redis / django / pandas.

What it does
------------
``track_and_log_signal(live_state)`` remembers the last logged signal *in
memory* and appends one row to ``gamma_signals_history.csv`` ONLY when the
signal actually changes state.  No per-tick spam — only transitions.

CSV columns
-----------
    Timestamp, Signal_Name, ATM_Strike, ATM_CE_LTP, ATM_PE_LTP

Plug-and-play guarantee
-----------------------
* No import-time side effects (no file is created until the first change).
* ``track_and_log_signal`` never raises — a logging failure can never take
  down the feed or the engine.
* Remove the single import line from the caller and the system behaves exactly
  as before.

ATM CE / PE parsing
-------------------
It resolves the strike nearest to the live spot and reads the CE and PE
premiums at that strike, mirroring ``upstox_live_feed._resolve_atm_option_keys``.
Two payload shapes are accepted:

  * Feed shape   : ``state["instrument_data"] = {key: {strike, option_type, ltp}}``
  * Engine shape : ``state["data"] = [{strike_price, call_ltp, put_ltp}]``

The signal can be supplied explicitly (recommended) or auto-discovered from a
small set of well-known keys (``day_gamma.signal``, ``day_gamma_signal``, ...).
"""

from __future__ import annotations

import csv
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

IST = timezone(timedelta(hours=5, minutes=30))

DEFAULT_SIGNAL_NAME = "NIFTY DAY GAMMA"
CSV_HEADER = ["Timestamp", "Signal_Name", "ATM_Strike", "ATM_CE_LTP", "ATM_PE_LTP"]

# Ordered candidate keys used to auto-discover the signal string from the
# incoming dict. The explicit ``signal=`` argument always wins over these.
SIGNAL_LOOKUP_KEYS = (
    "day_gamma_signal",
    "gamma_day_signal",
    "nifty_day_gamma_signal",
    "day_signal",
)

# Ordered candidate keys used to locate the live index spot.
SPOT_LOOKUP_KEYS = (
    "spot_price",
    "underlying_ltp",
    "underlying_spot",
    "index_ltp",
    "nifty_ltp",
    "spot",
    "underlying",
)


def _csv_path() -> str:
    """Resolve the CSV path (env override, else next to this module)."""
    return os.environ.get(
        "GAMMA_SIGNAL_LOG_PATH",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "gamma_signals_history.csv"),
    )


def _to_positive_number(value: Any) -> Optional[float]:
    """Coerce ``value`` to a positive float, or None. Bools are rejected."""
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _fmt_number(value: Any) -> str:
    """Render a number cleanly for CSV (no trailing '.0', no sci-notation)."""
    number = _to_positive_number(value)
    if number is None:
        return ""
    if number == int(number):
        return str(int(number))
    return f"{number:.4f}".rstrip("0").rstrip(".")


def _resolve_spot(state: Dict[str, Any]) -> Optional[float]:
    """Return the first positive spot-like value from ``state``."""
    if not isinstance(state, dict):
        return None
    for key in SPOT_LOOKUP_KEYS:
        number = _to_positive_number(state.get(key))
        if number is not None:
            return number
    return None


def _resolve_signal(state: Dict[str, Any], explicit_signal: Any = None) -> Optional[str]:
    """Resolve the signal string, giving ``explicit_signal`` top priority."""
    if explicit_signal is not None:
        text = str(explicit_signal).strip()
        return text or None

    if not isinstance(state, dict):
        return None

    # Gamma-engine ``last_profile`` shape: {"day_gamma": {"signal": "..."}}.
    day_gamma = state.get("day_gamma")
    if isinstance(day_gamma, dict):
        text = str(day_gamma.get("signal") or "").strip()
        if text:
            return text

    for key in SIGNAL_LOOKUP_KEYS:
        candidate = state.get(key)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()

    return None


def _resolve_atm_from_instrument_data(
    spot: float, instrument_data: Dict[str, Any]
) -> Optional[Tuple[int, Optional[float], Optional[float]]]:
    """Resolve (strike, ce_ltp, pe_ltp) from the feed-shape instrument map."""
    if not isinstance(instrument_data, dict):
        return None

    best_strike: Optional[float] = None
    best_dist: Optional[float] = None
    for entry in instrument_data.values():
        if not isinstance(entry, dict):
            continue
        strike = _to_positive_number(entry.get("strike"))
        if strike is None:
            continue
        dist = abs(strike - spot)
        if best_dist is None or dist < best_dist:
            best_strike = strike
            best_dist = dist

    if best_strike is None:
        return None

    ce_ltp = pe_ltp = None
    for entry in instrument_data.values():
        if not isinstance(entry, dict):
            continue
        strike = _to_positive_number(entry.get("strike"))
        if strike != best_strike:
            continue
        side = str(entry.get("option_type") or "").strip().upper()
        ltp = _to_positive_number(entry.get("ltp"))
        if side == "CE" and ce_ltp is None:
            ce_ltp = ltp
        elif side == "PE" and pe_ltp is None:
            pe_ltp = ltp

    return int(best_strike), ce_ltp, pe_ltp


def _resolve_atm_from_rows(
    spot: float, rows: Any
) -> Optional[Tuple[int, Optional[float], Optional[float]]]:
    """Resolve (strike, ce_ltp, pe_ltp) from the engine-normalised rows shape."""
    if not isinstance(rows, (list, tuple)):
        return None

    best_strike: Optional[float] = None
    best_dist: Optional[float] = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        strike = _to_positive_number(row.get("strike_price") or row.get("strike"))
        if strike is None:
            continue
        dist = abs(strike - spot)
        if best_dist is None or dist < best_dist:
            best_strike = strike
            best_dist = dist

    if best_strike is None:
        return None

    for row in rows:
        if not isinstance(row, dict):
            continue
        strike = _to_positive_number(row.get("strike_price") or row.get("strike"))
        if strike != best_strike:
            continue
        ce_ltp = _to_positive_number(row.get("call_ltp") or row.get("ce_ltp"))
        pe_ltp = _to_positive_number(row.get("put_ltp") or row.get("pe_ltp"))
        return int(best_strike), ce_ltp, pe_ltp

    return int(best_strike), None, None


def _resolve_atm(state: Dict[str, Any]) -> Optional[Tuple[int, Optional[float], Optional[float]]]:
    """Resolve (strike, ce_ltp, pe_ltp) for the strike nearest the live spot."""
    if not isinstance(state, dict):
        return None
    spot = _resolve_spot(state)
    if spot is None:
        return None

    instrument_data = state.get("instrument_data")
    if isinstance(instrument_data, dict):
        result = _resolve_atm_from_instrument_data(spot, instrument_data)
        if result is not None:
            return result

    rows = state.get("data")
    if isinstance(rows, (list, tuple)):
        result = _resolve_atm_from_rows(spot, rows)
        if result is not None:
            return result

    return None


class GammaSignalLogger:
    """Stateful CSV logger that writes only on signal transitions."""

    def __init__(self, csv_path: Optional[str] = None, signal_name: str = DEFAULT_SIGNAL_NAME):
        self._csv_path = csv_path or _csv_path()
        self._signal_name = signal_name
        self._lock = threading.Lock()
        # None == "nothing logged yet"; the first observed signal is a
        # transition (from nothing) and is therefore logged as a baseline.
        self._last_logged_signal: Optional[str] = None

    @property
    def last_logged_signal(self) -> Optional[str]:
        with self._lock:
            return self._last_logged_signal

    def reset(self) -> None:
        """Forget the last logged signal (useful for tests / re-baselining)."""
        with self._lock:
            self._last_logged_signal = None

    def track(self, state: Dict[str, Any], signal: Any = None) -> bool:
        """Log one row iff the signal changed. Returns True when a row was written."""
        resolved_signal = _resolve_signal(state, signal)
        if not resolved_signal:
            return False

        atm = _resolve_atm(state)
        strike, ce_ltp, pe_ltp = atm if atm is not None else (None, None, None)

        with self._lock:
            if resolved_signal == self._last_logged_signal:
                return False
            self._last_logged_signal = resolved_signal
            self._write_row(resolved_signal, strike, ce_ltp, pe_ltp)
            return True

    def _write_row(
        self,
        signal: str,
        strike: Optional[int],
        ce_ltp: Optional[float],
        pe_ltp: Optional[float],
    ) -> None:
        timestamp = datetime.now(IST).isoformat(timespec="seconds")
        row = [
            timestamp,
            self._signal_name,
            _fmt_number(strike),
            _fmt_number(ce_ltp),
            _fmt_number(pe_ltp),
        ]
        try:
            file_exists = os.path.exists(self._csv_path) and os.path.getsize(self._csv_path) > 0
            with open(self._csv_path, "a", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh)
                if not file_exists:
                    writer.writerow(CSV_HEADER)
                writer.writerow(row)
        except OSError:
            # Logging must never break the feed / engine.
            pass


# Eager singleton: no file I/O happens here, only an in-memory object.
_default_logger = GammaSignalLogger()


def track_and_log_signal(live_state: Dict[str, Any], signal: Any = None) -> bool:
    """Stateful entry point — call this from your live loop.

    Parameters
    ----------
    live_state : dict
        The feed's ``live_state`` (or the Gamma engine's raw Redis payload),
        used to parse the ATM strike + CE/PE LTPs.
    signal : optional
        The "NIFTY DAY GAMMA" signal string. When omitted, the module tries to
        auto-discover it from ``live_state`` (``day_gamma.signal`` etc.).

    Returns
    -------
    bool
        True if a transition was logged, False otherwise.
    """
    return _default_logger.track(live_state, signal=signal)


def reset_signal_logger() -> None:
    """Reset the in-memory last-logged signal (mainly for testing)."""
    _default_logger.reset()


__all__ = [
    "track_and_log_signal",
    "reset_signal_logger",
    "GammaSignalLogger",
    "DEFAULT_SIGNAL_NAME",
    "CSV_HEADER",
]



if __name__ == "__main__":  # pragma: no cover - manual smoke test
    # Synthetic feed-shape live_state (mirrors live_options_data.json).
    fake_state = {
        "spot_price": 25044.65,
        "instrument_data": {
            "CE_24950": {"strike": 24950, "option_type": "CE", "ltp": 380.15},
            "PE_24950": {"strike": 24950, "option_type": "PE", "ltp": 265.30},
            "CE_25000": {"strike": 25000, "option_type": "CE", "ltp": 340.05},
            "PE_25000": {"strike": 25000, "option_type": "PE", "ltp": 275.80},
            "CE_25050": {"strike": 25050, "option_type": "CE", "ltp": 301.25},
            "PE_25050": {"strike": 25050, "option_type": "PE", "ltp": 287.45},
        },
    }

    logger = GammaSignalLogger()

    # 1) First observation -> baseline row (a transition from nothing).
    wrote_1 = logger.track(fake_state, signal="🟡 Range-bound: Choppy / Transition Zone")
    # 2) Same signal -> no write.
    wrote_2 = logger.track(fake_state, signal="🟡 Range-bound: Choppy / Transition Zone")
    # 3) Signal changed -> new row.
    wrote_3 = logger.track(fake_state, signal="🟢 Strong Bullish Day Gamma / Buy Dips")

    print(f"CSV written to : {logger._csv_path}")
    print(f"First call     -> wrote: {wrote_1}")
    print(f"Same signal    -> wrote: {wrote_2}")
    print(f"Changed signal -> wrote: {wrote_3}")
    print(f"last_logged    -> {logger.last_logged_signal}")

    with open(logger._csv_path, "r", encoding="utf-8") as fh:
        print("\n--- file contents ---")
        print(fh.read())
