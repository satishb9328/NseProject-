#!/usr/bin/env python3
"""Fetch live NIFTY spot and option chain from Upstox v2 and send payload to processor."""

import argparse
import importlib
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests
from GAMMA.gamma_blast_engine import GammaProcessor, SignalEngine

API_BASE_URL = "https://api.upstox.com/v2"
INDEX_INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
POLL_INTERVAL_SECONDS = 5
MAX_ROLLING_WINDOW = 120

COLOR_YELLOW = "\033[93m"
COLOR_RED = "\033[91m"
COLOR_GREEN = "\033[92m"
COLOR_RESET = "\033[0m"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("UpstoxNiftyFetcher")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def get_next_weekly_expiry(now: Optional[datetime] = None) -> str:
    """Return next weekly NIFTY expiry date in YYYY-MM-DD format."""
    now = now or datetime.now()
    expiry_weekday = 1  # Tuesday
    market_close_time = now.replace(hour=15, minute=30, second=0, microsecond=0)

    days_to_expiry = (expiry_weekday - now.weekday()) % 7
    if days_to_expiry == 0:
        if now < market_close_time:
            return now.strftime("%Y-%m-%d")
        days_to_expiry = 7

    expiry_date = now + timedelta(days=days_to_expiry)
    return expiry_date.strftime("%Y-%m-%d")


class UpstoxClient:
    def __init__(self, api_key: str, api_secret: str, access_token: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.access_token = access_token
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Api-Version": "2.0",
            "Authorization": f"Bearer {self.access_token}",
        })

    def validate_token(self) -> bool:
        url = f"{API_BASE_URL}/user/profile"
        try:
            response = self.session.get(url, timeout=10)
            if response.status_code == 200:
                return True
            logger.warning("Token validation failed: %s %s", response.status_code, response.text)
            return False
        except requests.RequestException as exc:
            logger.warning("Token validation request failed: %s", exc)
            return False

    def fetch_spot_price(self) -> Optional[float]:
        url = f"{API_BASE_URL}/market/quotes/ltp"
        params = {"instrument_key": INDEX_INSTRUMENT_KEY}
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            payload = response.json()
            data = payload.get("data", {}) if isinstance(payload, dict) else {}
            spot_item = data.get(INDEX_INSTRUMENT_KEY) if isinstance(data, dict) else None
            if spot_item is None:
                logger.error("Spot response missing expected key: %s", INDEX_INSTRUMENT_KEY)
                return None
            # Upstox /market/quotes/ltp returns the live index price under
            # ``last_price`` (not ``ltp``). Fall back to ``ltp``/``close`` for safety.
            spot = _safe_float(spot_item.get("last_price", spot_item.get("ltp", spot_item.get("close"))))
            if spot <= 0:
                logger.error("Spot response contained an invalid price: %s", spot_item)
                return None
            return spot
        except requests.RequestException as exc:
            logger.error("Failed to fetch spot price: %s", exc)
            return None
        except ValueError as exc:
            logger.error("Unexpected spot price payload: %s", exc)
            return None

    def fetch_option_chain(self, expiry_date: str) -> Optional[List[Dict[str, Any]]]:
        url = f"{API_BASE_URL}/option/chain"
        params = {
            "instrument_key": INDEX_INSTRUMENT_KEY,
            "expiry_date": expiry_date,
        }
        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                logger.error("Invalid option chain payload type: %s", type(payload))
                return None
            if payload.get("status") not in (None, "success", "ok"):
                logger.error("Option chain API returned error status: %s", payload.get("status"))
                return None
            raw_data = payload.get("data")
            if not isinstance(raw_data, list):
                logger.error("Option chain response missing data list")
                return None
            return raw_data
        except requests.RequestException as exc:
            logger.error("Failed to fetch option chain: %s", exc)
            return None
        except ValueError as exc:
            logger.error("Unexpected option chain payload: %s", exc)
            return None


def build_payload(spot_price: float, expiry_date: str, raw_chain: List[Dict[str, Any]]) -> Dict[str, Any]:
    mapped_data: List[Dict[str, Any]] = []

    for row in raw_chain:
        strike_price = _safe_float(row.get("strike_price") or row.get("strike") or 0)
        if strike_price <= 0:
            continue

        call_options = row.get("call_options") or {}
        call_market = call_options.get("market_data") or {}
        call_greeks = call_options.get("option_greeks") or {}

        put_options = row.get("put_options") or {}
        put_market = put_options.get("market_data") or {}
        put_greeks = put_options.get("option_greeks") or {}

        mapped_data.append({
            "strike_price": strike_price,
            "call_oi": _safe_int(call_market.get("oi")),
            "call_iv": _safe_float(call_greeks.get("iv")),
            "call_ltp": _safe_float(call_market.get("ltp")),
            "call_volume": _safe_int(call_market.get("volume")),
            "put_oi": _safe_int(put_market.get("oi")),
            "put_iv": _safe_float(put_greeks.get("iv")),
            "put_ltp": _safe_float(put_market.get("ltp")),
            "put_volume": _safe_int(put_market.get("volume")),
        })

    return {
        "spot_price": float(spot_price),
        "expiry_date": expiry_date,
        "data": mapped_data,
    }


def load_processor(module_path: Optional[str]) -> Any:
    if module_path:
        module_name, attr_name = module_path.rsplit(".", 1)
        module = importlib.import_module(module_name)
        processor_obj = getattr(module, attr_name)
        if callable(processor_obj):
            return processor_obj()
        return processor_obj

    class DefaultProcessor:
        def inject_and_process(self, payload: Dict[str, Any]) -> None:
            logger.info("processor.inject_and_process called with payload expiry=%s, spot=%s, rows=%d",
                        payload["expiry_date"], payload["spot_price"], len(payload["data"]))
            return None

    return DefaultProcessor()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch live NIFTY option chain from Upstox API v2.")
    parser.add_argument("--api-key", default=os.getenv("UPSTOX_API_KEY"), help="Upstox API key")
    parser.add_argument("--api-secret", default=os.getenv("UPSTOX_API_SECRET"), help="Upstox API secret")
    parser.add_argument("--access-token", default=os.getenv("UPSTOX_ACCESS_TOKEN"), help="Upstox access token")
    parser.add_argument("--processor", default=None,
                        help="Optional import path for a processor object, e.g. module.ProcessorClass or module.processor")
    parser.add_argument("--interval", type=int, default=POLL_INTERVAL_SECONDS, help="Polling interval in seconds")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.api_key or not args.api_secret or not args.access_token:
        logger.error("Missing required credentials. Set --api-key, --api-secret and --access-token or the corresponding env vars.")
        return 1

    client = UpstoxClient(api_key=args.api_key, api_secret=args.api_secret, access_token=args.access_token)
    if not client.validate_token():
        logger.error("Upstox access token is invalid or expired.")
        return 1

    processor = GammaProcessor("NIFTY")
    signal_engine = SignalEngine()
    closes: List[float] = []
    volumes: List[float] = []
    prev_spot = 0.0
    expiry_date = get_next_weekly_expiry()
    logger.info("Starting live NIFTY fetch loop for expiry %s", expiry_date)

    while True:
        try:
            spot_price = client.fetch_spot_price()
            if not spot_price or spot_price <= 0:
                logger.warning("Skipping fetch because spot price could not be retrieved.")
            else:
                raw_chain = client.fetch_option_chain(expiry_date=expiry_date)
                if raw_chain is None:
                    logger.warning("Skipping payload build because option chain could not be retrieved.")
                else:
                    payload = build_payload(spot_price=spot_price, expiry_date=expiry_date, raw_chain=raw_chain)
                    profile = processor.process_payload(payload)
                    if profile is None:
                        logger.warning("GammaProcessor returned no profile for payload.")
                    else:
                        total_volume = sum(
                            int(row.get("call_volume", 0)) + int(row.get("put_volume", 0))
                            for row in payload["data"]
                        )
                        closes.append(spot_price)
                        volumes.append(float(total_volume))
                        if len(closes) > MAX_ROLLING_WINDOW:
                            closes.pop(0)
                        if len(volumes) > MAX_ROLLING_WINDOW:
                            volumes.pop(0)

                        if prev_spot <= 0.0:
                            prev_spot = spot_price

                        signal = signal_engine.evaluate_entry(profile, prev_spot, closes, volumes)
                        if signal is not None:
                            print(f"{COLOR_RED}SIGNAL ALERT: {signal.direction} @ {signal.entry_price} | SL={signal.initial_sl} | TGT={signal.target} | SCORE={signal.score}{COLOR_RESET}")
                            logger.info("Signal generated: %s", signal)
                        else:
                            logger.debug("No signal generated this tick.")

                        prev_spot = spot_price

        except Exception as exc:  # pragma: no cover
            logger.exception("Unhandled error during fetch loop: %s", exc)

        logger.info("Sleeping for %s seconds before next fetch...", args.interval)
        time.sleep(args.interval)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
