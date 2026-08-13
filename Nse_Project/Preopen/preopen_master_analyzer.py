from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict


def _to_decimal(value: Any) -> Decimal:
    """Fast, safe conversion to Decimal while avoiding binary float drift."""
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def preopen_master_analyzer(
    open_price: Any,
    high_price: Any,
    low_price: Any,
    close_price: Any,
) -> Dict[str, Dict[str, Any]]:
    """
    Analyze pre-open OHLC and return advanced signal payload.

    Returned keys:
      - PORB_Levels
      - Auction_Sentiment
      - Volatility_Prediction
      - Magnet_Level
    """
    o = _to_decimal(open_price)
    h = _to_decimal(high_price)
    l = _to_decimal(low_price)
    c = _to_decimal(close_price)

    if h < l:
        raise ValueError('Invalid pre-open OHLC: high_price must be >= low_price')

    signal = 'NEUTRAL'
    if c < o:
        signal = 'BEARISH_PRESSURE'
    elif c > o:
        signal = 'BULLISH_PRESSURE'

    preopen_range_points = h - l

    return {
        'PORB_Levels': {
            'bullish_breakout_level': h,
            'bearish_breakdown_level': l,
        },
        'Auction_Sentiment': {
            'signal': signal,
            'open_price': o,
            'close_price': c,
        },
        'Volatility_Prediction': {
            'preopen_range_points': preopen_range_points,
        },
        'Magnet_Level': {
            'mean_reversion_target': c,
        },
    }
