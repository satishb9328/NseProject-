from __future__ import annotations

import math
from typing import Any, Dict

# All prices/levels are expressed in points with 2-decimal precision (matching
# PreopenSummary's DecimalField(decimal_places=2)). Rounding to this precision keeps
# the payload JSON-clean (no binary float noise such as 100.07000000000062) while
# remaining numerically identical to the previous Decimal math for the 2-decimal
# values NSE actually produces.
_ROUND_DIGITS = 2


def _coerce_price(value: Any, name: str) -> float:
    """Coerce a single OHLC value to a finite float, failing with a clear error.

    This is the only point where values enter the analyzer, so None, booleans,
    non-numeric strings, NaN and infinity are all rejected here — the analyzer can
    never crash the pipeline with an obscure ``decimal.InvalidOperation``.
    """
    if value is None:
        raise ValueError(f"Missing required OHLC value: {name}")
    if isinstance(value, bool):
        raise ValueError(f"Invalid {name}: boolean is not a price (got {value!r})")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {name}: expected a number, got {value!r}") from exc
    if not math.isfinite(result):
        raise ValueError(f"Invalid {name}: value must be finite (got {result!r})")
    return result


def preopen_master_analyzer(
    open_price: Any,
    high_price: Any,
    low_price: Any,
    close_price: Any,
) -> Dict[str, Dict[str, Any]]:
    """
    Analyze pre-open OHLC and return an advanced, JSON-serializable signal payload.

    Returned keys:
      - PORB_Levels
      - Auction_Sentiment
      - Volatility_Prediction
      - Magnet_Level

    Every numeric value is a native float, so the dict round-trips straight into
    ``json.dumps`` for the Redis publish step. Raises ``ValueError`` only on
    genuinely invalid input (missing/non-numeric values, or high < low).
    """
    # Coerce + round each value exactly once (no redundant arithmetic).
    o = round(_coerce_price(open_price, "open_price"), _ROUND_DIGITS)
    h = round(_coerce_price(high_price, "high_price"), _ROUND_DIGITS)
    l = round(_coerce_price(low_price, "low_price"), _ROUND_DIGITS)
    c = round(_coerce_price(close_price, "close_price"), _ROUND_DIGITS)

    if h < l:
        raise ValueError(
            f"Invalid pre-open OHLC: high_price ({h}) must be >= low_price ({l})"
        )

    signal = "NEUTRAL"
    if c < o:
        signal = "BEARISH_PRESSURE"
    elif c > o:
        signal = "BULLISH_PRESSURE"

    # High == Low (a flat pre-open) is valid: the range is simply zero. No division
    # is performed anywhere in this function, so there is no ZeroDivisionError
    # surface to guard.
    preopen_range_points = round(h - l, _ROUND_DIGITS)

    return {
        "PORB_Levels": {
            "bullish_breakout_level": h,
            "bearish_breakdown_level": l,
        },
        "Auction_Sentiment": {
            "signal": signal,
            "open_price": o,
            "close_price": c,
        },
        "Volatility_Prediction": {
            "preopen_range_points": preopen_range_points,
        },
        "Magnet_Level": {
            "mean_reversion_target": c,
        },
    }
