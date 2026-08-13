"""Compatibility wrapper for the legacy `Nse Project/Brokers/fyers_socket.py` module."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_LEGACY_MODULE_PATH = (
    Path(__file__).resolve().parents[2] / "Nse Project" / "Brokers" / "fyers_socket.py"
)

_spec = importlib.util.spec_from_file_location(
    "_legacy_nse_project_brokers_fyers_socket",
    _LEGACY_MODULE_PATH,
)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Unable to load legacy module from {_LEGACY_MODULE_PATH}")

_legacy_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_legacy_module)

Tick = _legacy_module.Tick
Candle = _legacy_module.Candle
FyersSocketManager = _legacy_module.FyersSocketManager
nse_eq_symbol = _legacy_module.nse_eq_symbol
nse_fut_symbol = _legacy_module.nse_fut_symbol
nifty_50_eq_symbols = _legacy_module.nifty_50_eq_symbols
nifty_bank_eq_symbols = _legacy_module.nifty_bank_eq_symbols

__all__ = [
    "Tick",
    "Candle",
    "FyersSocketManager",
    "nse_eq_symbol",
    "nse_fut_symbol",
    "nifty_50_eq_symbols",
    "nifty_bank_eq_symbols",
]

