"""Normalized snapshot extraction (offline, pure).

Converts the two live-platform dicts — the ``live_options_data`` snapshot and
the ``market_state:*`` metrics dict — into the normalized raw capture row
defined in ``docs/SCHEMA.md``. No Redis, no I/O, no imports outside this
package.

Field mapping (verified against ``views.py`` / ``upstox_live_feed.py``):

* spot          <- snapshot["spot_price"] (with fallback key probing)
* active_expiry <- snapshot["expiry_date"]
* updated_at    <- snapshot["updated_at"] (verbatim)
* signal fields <- market_state["bias" / "momentum" / "score" /
                  "gamma_boost_val" / "iv_skew_est"]
* atm_strike    <- active-expiry chain strike nearest spot
* atm_ce/pe_ltp <- that strike's ``ltp`` (fallback raw_sample fullFeed LTP)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from . import IST, isoformat_ist, now_ist, parse_timestamp

# Top-level keys probed for the index spot, in precedence order (mirrors views.py).
_SPOT_KEYS = ("spot_price", "underlying_spot", "underlying_ltp", "index_ltp", "nifty_ltp")

# instrument_data item keys probed for a strike.
_STRIKE_KEYS = ("strike_price", "strike")

# Fallback LTP path inside an instrument_data item (Upstox V3 raw sample).
_LTP_FALLBACK_PATH = ("raw_sample", "fullFeed", "marketFF", "ltpc", "ltp")


def _to_float(value: Any) -> Optional[float]:
    """Coerce to float, rejecting bools, NaN and infinities (returns None)."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        f = float(value)
        return f if f == f and f not in (float("inf"), float("-inf")) else None
    if isinstance(value, str):
        try:
            f = float(value.replace(",", "").strip())
        except ValueError:
            return None
        return f if f == f and f not in (float("inf"), float("-inf")) else None
    return None


def _float_or(value: Any, default: float) -> float:
    f = _to_float(value)
    return default if f is None else f


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "on")
    if isinstance(value, (int, float)):
        return value != 0
    return False


def _nested_get(data: Any, path: tuple) -> Any:
    for key in path:
        if isinstance(data, dict):
            data = data.get(key)
        else:
            return None
    return data


class Extractor:
    """Builds normalized raw capture rows from live-platform payload dicts."""

    def __init__(
        self,
        min_spot: float = 0.0,
        drop_if_spot_missing: bool = True,
        atm_selection: str = "nearest_strike",
    ) -> None:
        self.min_spot = float(min_spot)
        self.drop_if_spot_missing = bool(drop_if_spot_missing)
        self.atm_selection = atm_selection

    def extract(
        self,
        snapshot: dict,
        market_state: dict,
        seq: int,
        captured_at: Optional[datetime] = None,
    ) -> Optional[dict]:
        """Return the normalized raw capture row, or ``None`` if unusable.

        A row is dropped only when spot is missing/invalid (and
        ``drop_if_spot_missing``) or when spot does not exceed ``min_spot``.
        A missing ATM strike does not drop the row: spot validation is still
        possible, and the enricher marks option lookups unavailable.
        """
        spot = self._spot(snapshot)
        if spot is None:
            if self.drop_if_spot_missing:
                return None
            spot = 0.0
        if spot <= self.min_spot:
            return None

        active_expiry = self._expiry(snapshot)
        updated_at_raw = snapshot.get("updated_at")
        captured_dt = captured_at or now_ist()
        ts_dt = parse_timestamp(updated_at_raw) or captured_dt

        chain = self.build_chain(snapshot)
        atm_strike = self._nearest_strike(chain, spot)
        atm = chain.get(atm_strike) if atm_strike is not None else {}

        return {
            "row_type": "capture",
            "ts": isoformat_ist(ts_dt),
            "updated_at": self._updated_at(updated_at_raw),
            "captured_at": isoformat_ist(captured_dt),
            "seq": int(seq),
            "session_date": ts_dt.astimezone(IST).date().isoformat(),
            "spot": spot,
            "active_expiry": active_expiry,
            "atm_strike": atm_strike,
            "bias": _float_or(market_state.get("bias"), 0.0),
            "momentum": _float_or(market_state.get("momentum"), 0.0),
            "gamma_boost": _float_or(market_state.get("gamma_boost_val"), 0.0),
            "score": _float_or(market_state.get("score"), 50.0),
            "iv_skew": _float_or(market_state.get("iv_skew_est"), 0.0),
            "atm_ce_ltp": atm.get("ce"),
            "atm_pe_ltp": atm.get("pe"),
            "data_source": snapshot.get("data_source"),
            "feed_connected": _to_bool(snapshot.get("feed_connected")),
            "stale": _to_bool(snapshot.get("stale")),
            "seconds_since_last_tick": _to_float(snapshot.get("seconds_since_last_tick")),
        }

    # -- helpers ------------------------------------------------------------
    @staticmethod
    def _updated_at(value: Any) -> Optional[str]:
        return None if value is None else str(value)

    def _spot(self, snapshot: dict) -> Optional[float]:
        for key in _SPOT_KEYS:
            value = _to_float(snapshot.get(key))
            if value is not None and value > 0:
                return value
        return None

    @staticmethod
    def _expiry(snapshot: dict) -> Optional[str]:
        value = snapshot.get("expiry_date") or snapshot.get("active_expiry")
        return str(value) if value else None

    def _iter_instruments(self, snapshot: dict):
        data = snapshot.get("instrument_data")
        if not isinstance(data, dict):
            return
        for item in data.values():
            if isinstance(item, dict):
                yield item

    def build_chain(self, snapshot: dict) -> dict:
        """Return ``{strike: {"ce": ltp|None, "pe": ltp|None}}`` for the active chain.

        Kept in-memory (never written to disk) so ``RingBuffer.forward_option``
        can resolve a pinned strike even after the live ATM strike has shifted.
        """
        chain: dict = {}
        for item in self._iter_instruments(snapshot):
            strike = _to_float(item.get(_STRIKE_KEYS[0], item.get(_STRIKE_KEYS[1])))
            side = (item.get("option_type") or "").upper()
            if strike is None or side not in ("CE", "PE"):
                continue
            entry = chain.setdefault(strike, {"ce": None, "pe": None})
            ltp = _to_float(item.get("ltp"))
            if ltp is None:
                ltp = _to_float(_nested_get(item, _LTP_FALLBACK_PATH))
            entry["ce" if side == "CE" else "pe"] = ltp
        return chain

    @staticmethod
    def _nearest_strike(chain: dict, spot: float) -> Optional[float]:
        if not chain:
            return None
        return min(chain.keys(), key=lambda strike: abs(strike - spot))

