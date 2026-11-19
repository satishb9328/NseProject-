"""Forward-value enrichment (offline, pure).

Given a matured raw row and the ring buffer it lives in, populate the 12
forward price fields (``spot`` / ``ce`` / ``pe`` × 15/30/60/120 seconds) and
the completeness flags defined in ``docs/SCHEMA.md``. Option forwards are
pinned to the captured ``atm_strike`` / ``active_expiry``; spot forwards are
always eligible.
"""

from __future__ import annotations

from typing import Optional, Tuple

from .state import RingBuffer

_DEFAULT_HORIZONS: Tuple[int, ...] = (15, 30, 60, 120)


class Enricher:
    """Populates forward fields on matured rows using RingBuffer lookups."""

    def __init__(self, horizons: Tuple[int, ...] = _DEFAULT_HORIZONS, warmup_seconds: float = 120.0) -> None:
        self.horizons: Tuple[int, ...] = tuple(sorted(int(h) for h in horizons))
        self.warmup_seconds = float(warmup_seconds)

    def enrich(self, row: dict, capture_mono: float, buffer: RingBuffer) -> dict:
        """Return the enriched row (raw fields + forward fields + completeness)."""
        enriched = dict(row)
        enriched["row_type"] = "enriched"

        pinned_strike = row.get("atm_strike")
        pinned_expiry = row.get("active_expiry")
        base_ce = row.get("atm_ce_ltp")
        base_pe = row.get("atm_pe_ltp")

        missing_horizons = []
        reasons = []

        for horizon in self.horizons:
            spot_val, spot_reason = buffer.forward_spot(capture_mono, horizon)
            ce_val, ce_reason = self._option_forward(
                buffer, capture_mono, horizon, "ce", base_ce, pinned_strike, pinned_expiry
            )
            pe_val, pe_reason = self._option_forward(
                buffer, capture_mono, horizon, "pe", base_pe, pinned_strike, pinned_expiry
            )

            enriched[f"spot_{horizon}s"] = spot_val
            enriched[f"ce_{horizon}s"] = ce_val
            enriched[f"pe_{horizon}s"] = pe_val

            if spot_val is None or ce_val is None or pe_val is None:
                missing_horizons.append(str(horizon))
                for reason in (spot_reason, ce_reason, pe_reason):
                    if reason:
                        reasons.append(reason)

        enriched["missing_horizons"] = missing_horizons or None

        if not missing_horizons:
            enriched["complete"] = True
            enriched["reason"] = None
        else:
            enriched["complete"] = False
            enriched["reason"] = self._dominant_reason(reasons, capture_mono, buffer)

        return enriched

    @staticmethod
    def _option_forward(buffer, capture_mono, horizon, side, base_value, pinned_strike, pinned_expiry):
        """Option forward is unavailable when the base LTP is missing."""
        if base_value is None:
            return None, "strike_unavailable"
        return buffer.forward_option(capture_mono, horizon, side, pinned_strike, pinned_expiry)

    def _dominant_reason(self, reasons, capture_mono: float, buffer: RingBuffer) -> str:
        end_mono = capture_mono + max(self.horizons)
        if "expiry_rollover" in reasons:
            return "expiry_rollover"
        if "strike_unavailable" in reasons:
            return "strike_unavailable"
        if "no_sample" in reasons:
            if buffer.has_gap(capture_mono, end_mono):
                return "feed_gap"
            oldest = buffer.oldest_mono
            if oldest is not None and (capture_mono + min(self.horizons)) < oldest:
                return "warmup"
            return "restart"
        return "unknown"
