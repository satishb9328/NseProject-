import asyncio
import gzip
import io
import json
import logging
import math
import os
import sys
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from upstox_client import ApiClient, Configuration
from upstox_client.feeder.market_data_streamer_v3 import MarketDataStreamerV3

try:
    from Nse_Project.Swing_stocks.combo_filter import filter_combo_setups
except Exception:  # pragma: no cover
    filter_combo_setups = None

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
SWING_STOCKS_DIR = PROJECT_ROOT / 'Swing_stocks'
COMBO_CONTEXT_FILE = SWING_STOCKS_DIR / 'combo_filter_context.json'
TOKEN_DIR = PROJECT_ROOT / 'Brokers' / 'upstox'
TOKEN_FILE_PATHS = [
    TOKEN_DIR / 'upstox' / 'upstox_token.json',
    TOKEN_DIR / 'upstox_v3_token.json',
]
LOG_FORMAT = '%(asctime)s | %(levelname)-8s | %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('SwingMLStockDataCollector')

AUTO_TRADE_ENABLED = False
RISK_PER_TRADE = 10000
VOLUME_HISTORY_LENGTH = 20
VOLUME_SPIKE_MULTIPLIER = 1.5
OFI_HISTORY_LENGTH = 100
ALERTS_FILE = Path(__file__).resolve().parent / 'live_alerts.json'
MAX_ALERTS = 50
SWING_SEPA_GROUP = "swing_sepa_updates"
STRONG_SEPA_CSV = SWING_STOCKS_DIR / "nifty200_smc_precision_screen.csv"
STRONG_SEPA_FIELDS = ("symbol", "rs_vs_index", "vcp", "proximity", "smc_signal", "score")


def save_combo_context(categories: dict) -> None:
    try:
        COMBO_CONTEXT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with COMBO_CONTEXT_FILE.open('w', encoding='utf-8') as fh:
            json.dump(categories, fh, indent=2)
        logger.info(f"Saved combo filter context to {COMBO_CONTEXT_FILE}")
    except Exception as exc:
        logger.warning(f"Failed to save combo filter context: {exc}")


def load_strong_sepa_symbols() -> list[str]:
    if not COMBO_CONTEXT_FILE.exists():
        logger.warning(f"Combo filter file not found: {COMBO_CONTEXT_FILE}")
        if STRONG_SEPA_CSV.exists() and filter_combo_setups is not None:
            try:
                df = pd.read_csv(STRONG_SEPA_CSV)
                df = df.where(pd.notnull(df), None)
                categories = filter_combo_setups(df)
                symbols = [
                    str(row.get('symbol')).upper().strip()
                    for row in categories.get('STRONG_SEPA', [])
                    if isinstance(row, dict) and row.get('symbol')
                ]
                if symbols:
                    logger.info(f"Derived {len(symbols)} strong_sepa symbols from {STRONG_SEPA_CSV}")
                    try:
                        save_combo_context(categories)
                        logger.info(f"Wrote missing combo filter context to {COMBO_CONTEXT_FILE}")
                    except Exception as exc:
                        logger.warning(f"Could not write combo context file: {exc}")
                    return sorted(set(symbols))
            except Exception as exc:
                logger.warning(f"Failed to derive strong_sepa symbols from CSV: {exc}")
        return []

    try:
        with COMBO_CONTEXT_FILE.open('r', encoding='utf-8') as fh:
            payload = json.load(fh)
        symbols = [row.get('symbol') for row in payload.get('strong_sepa', []) if isinstance(row, dict) and row.get('symbol')]
        symbols = [str(symbol).upper().strip() for symbol in symbols if symbol]
        logger.info(f"Loaded {len(symbols)} strong_sepa symbols from combo filter context.")
        return sorted(set(symbols))
    except Exception as exc:
        logger.error(f"Failed to read strong_sepa symbols: {exc}")
        return []


def log_health_check() -> None:
    token_path_info = []
    loaded_token_path = None
    for path in TOKEN_FILE_PATHS:
        exists = path.exists()
        token_path_info.append(f"{path} (exists={exists})")
        if exists and loaded_token_path is None:
            try:
                with path.open('r', encoding='utf-8') as fh:
                    data = json.load(fh)
                if data.get('access_token'):
                    loaded_token_path = path
            except Exception:
                pass

    combo_context_exists = COMBO_CONTEXT_FILE.exists()
    csv_exists = STRONG_SEPA_CSV.exists()
    logger.info('--- Swing ML Collector Health Check ---')
    logger.info('Token paths: %s', ' | '.join(token_path_info))
    logger.info('Loaded token path: %s', loaded_token_path or 'None')
    logger.info('Combo context file exists: %s', combo_context_exists)
    logger.info('Strong SEPA CSV exists: %s', csv_exists)
    if not combo_context_exists and csv_exists:
        logger.info('Combo context missing, but strong SEPA CSV exists. Collector will attempt CSV fallback.')
    logger.info('--- End health check ---\n')


def load_access_token() -> str | None:
    for path in TOKEN_FILE_PATHS:
        if path.exists():
            try:
                with path.open('r', encoding='utf-8') as fh:
                    data = json.load(fh)
                token = data.get('access_token')
                if token:
                    logger.info(f"Loaded Upstox token from {path}")
                    return token
                logger.warning(f"Token file exists but access_token missing: {path}")
            except Exception as exc:
                logger.warning(f"Failed to read token file {path}: {exc}")
    logger.error('No valid Upstox token found.')
    return None


def load_alerts() -> list[dict]:
    if not ALERTS_FILE.exists():
        return []
    try:
        with ALERTS_FILE.open('r', encoding='utf-8') as fh:
            payload = json.load(fh)
        return payload if isinstance(payload, list) else []
    except Exception:
        return []


def write_alerts(alerts: list[dict]) -> None:
    try:
        temp_file = ALERTS_FILE.with_suffix('.json.tmp')
        with temp_file.open('w', encoding='utf-8') as fh:
            json.dump(alerts, fh, indent=2, default=str)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temp_file, ALERTS_FILE)
    except Exception as exc:
        logger.warning(f'Failed to write live alerts file: {exc}')


def place_auto_buy_order(symbol: str, qty: int, ltp: float) -> dict:
    # This is a placeholder skeleton for Upstox order placement.
    # Replace with the actual SDK call when live execution is enabled.
    logger.info(f'📌 place_auto_buy_order called for {qty} shares of {symbol} at LTP {ltp}')
    return {
        'symbol': symbol,
        'qty': qty,
        'price': ltp,
        'status': 'SIMULATED' if not AUTO_TRADE_ENABLED else 'SUBMITTED',
    }


def append_alert(symbol: str, message: str, ofi_ratio: float | None = None, ofi_status: str | None = None) -> None:
    alert = {
        'symbol': symbol,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'message': message,
        'ofi_ratio': round(ofi_ratio, 3) if ofi_ratio is not None else None,
        'ofi_status': ofi_status,
    }
    alerts = load_alerts()
    alerts.insert(0, alert)
    write_alerts(alerts[:MAX_ALERTS])

    # Push the latest STRONG SEPA snapshot to connected dashboards.
    broadcast_sepa_update()


def _json_safe(value):
    if value is None:
        return None
    if isinstance(value, float):
        try:
            if math.isnan(value) or math.isinf(value):
                return None
        except (TypeError, ValueError):
            pass
    return value


def build_strong_sepa_snapshot() -> list:
    """Build the STRONG SEPA rows the swing dashboard table renders."""
    if filter_combo_setups is None or not STRONG_SEPA_CSV.exists():
        return []
    try:
        df = pd.read_csv(STRONG_SEPA_CSV)
        df = df.where(pd.notnull(df), None)
        rows = filter_combo_setups(df).get("STRONG_SEPA", [])
        return [
            {key: _json_safe(row.get(key)) for key in STRONG_SEPA_FIELDS}
            for row in rows
        ]
    except Exception as exc:
        logger.warning(f"Failed to build strong SEPA snapshot: {exc}")
        return []


def broadcast_sepa_update() -> None:
    """Push the latest STRONG SEPA snapshot to the swing_sepa_updates group."""
    try:
        channel_layer = get_channel_layer()
        if channel_layer is None:
            return
        strong_sepa = build_strong_sepa_snapshot()
        async_to_sync(channel_layer.group_send)(
            SWING_SEPA_GROUP,
            {
                "type": "swing_sepa_update",
                "strong_sepa": strong_sepa,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
    except Exception as exc:
        logger.warning(f"Failed to broadcast SEPA update: {exc}")


def get_upstox_instrument_map() -> dict[str, str]:
    url = 'https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz'
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        with gzip.open(io.BytesIO(response.content), mode='rt', encoding='utf-8') as fh:
            df = pd.read_csv(fh)
    except Exception as exc:
        logger.error(f"Failed to load Upstox instrument master file: {exc}")
        return {}

    if 'tradingsymbol' not in df.columns or 'instrument_key' not in df.columns:
        logger.error('Master file missing expected columns: tradingsymbol or instrument_key')
        return {}

    df['tradingsymbol'] = df['tradingsymbol'].astype(str).str.upper().str.strip()
    instrument_dict = pd.Series(df['instrument_key'].astype(str).values, index=df['tradingsymbol']).to_dict()
    logger.info(f"Loaded {len(instrument_dict)} Upstox instrument mappings.")
    return instrument_dict


def flatten_payload(payload, prefix=''):
    flattened = {}
    if isinstance(payload, dict):
        for key, value in payload.items():
            full_key = f"{prefix}.{key}" if prefix else str(key)
            flattened.update(flatten_payload(value, full_key))
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            full_key = f"{prefix}[{index}]" if prefix else str(index)
            flattened.update(flatten_payload(value, full_key))
    else:
        flattened[prefix] = payload
    return flattened


def extract_symbol_and_volume(message: object) -> tuple[str | None, float | None, float | None, float | None, float | None, float | None]:
    if message is None:
        return None, None, None, None, None, None

    if isinstance(message, str):
        try:
            message = json.loads(message)
        except Exception:
            return None, None, None, None, None, None

    flattened = flatten_payload(message)
    symbol = None
    volume = None
    ltp = None
    ltq = None
    bid = None
    ask = None

    for key, value in flattened.items():
        key_lower = str(key).lower()
        if symbol is None and any(token in key_lower for token in ['tradingsymbol', 'symbol', 'instrument', 'instrumentkey']):
            if isinstance(value, str) and value.strip():
                symbol = value.strip().upper()
        if volume is None and ('volume' in key_lower or 'tradedvolume' in key_lower or key_lower.endswith('qty')):
            if isinstance(value, (int, float)):
                volume = float(value)
        if ltp is None and any(token in key_lower for token in ['lasttradedprice', 'ltp', 'lastprice', 'price']):
            if isinstance(value, (int, float)):
                ltp = float(value)
        if ltq is None and any(token in key_lower for token in ['lasttradedqty', 'ltq', 'quantity', 'qty']):
            if isinstance(value, (int, float)):
                ltq = float(value)
        if bid is None and ('bestbid' in key_lower or 'bidprice' in key_lower or key_lower == 'bid'):
            if isinstance(value, (int, float)):
                bid = float(value)
        if ask is None and ('bestask' in key_lower or 'askprice' in key_lower or key_lower == 'ask'):
            if isinstance(value, (int, float)):
                ask = float(value)

    return symbol, volume, ltp, ltq, bid, ask


class SwingMLStockDataCollector:
    def __init__(self):
        self.strong_sepa_symbols = load_strong_sepa_symbols()
        self.instrument_map = get_upstox_instrument_map()
        self.volume_history: dict[str, deque[float]] = {}
        self.ofi_history: dict[str, deque[dict]] = {}
        self.ofi_sums: dict[str, dict[str, float]] = {}

    def get_subscribe_keys(self) -> list[str]:
        if not self.strong_sepa_symbols:
            logger.warning('No SEPA symbols available to subscribe to.')
            return []

        result = []
        missing = []
        for symbol in self.strong_sepa_symbols:
            key = self.instrument_map.get(symbol)
            if key:
                result.append(str(key))
            else:
                missing.append(symbol)

        if missing:
            logger.warning(f"Missing instrument keys for strong_sepa symbols: {missing}")
        logger.info(f"Subscribing to {len(result)} strong_sepa instruments.")
        return result

    def detect_volume_spike(self, symbol: str, volume: float) -> bool:
        history = self.volume_history.setdefault(symbol, deque(maxlen=VOLUME_HISTORY_LENGTH))
        if not history:
            history.append(volume)
            return False

        avg_volume = sum(history) / len(history)
        history.append(volume)
        if avg_volume <= 0:
            return False
        return volume > avg_volume * VOLUME_SPIKE_MULTIPLIER

    def update_ofi(self, symbol: str, ltp: float | None, ltq: float | None, bid: float | None, ask: float | None) -> float | None:
        if ltq is None or ltp is None:
            return None

        buy_volume = 0.0
        sell_volume = 0.0
        if ask is not None and ltp >= ask:
            buy_volume = ltq
        elif bid is not None and ltp <= bid:
            sell_volume = ltq

        history = self.ofi_history.setdefault(symbol, deque(maxlen=OFI_HISTORY_LENGTH))
        sums = self.ofi_sums.setdefault(symbol, {'buy': 0.0, 'sell': 0.0, 'total': 0.0})

        if len(history) == history.maxlen:
            old = history[0]
            sums['buy'] -= old['buy']
            sums['sell'] -= old['sell']
            sums['total'] -= old['total']

        entry = {'buy': buy_volume, 'sell': sell_volume, 'total': ltq}
        history.append(entry)
        sums['buy'] += buy_volume
        sums['sell'] += sell_volume
        sums['total'] += ltq

        return self.compute_ofi_ratio(symbol)

    def compute_ofi_ratio(self, symbol: str) -> float | None:
        sums = self.ofi_sums.get(symbol)
        if not sums or sums['total'] + 1 <= 0:
            return None
        return (sums['buy'] - sums['sell']) / (sums['total'] + 1)

    def build_streamer(self, token: str, instrument_keys: list[str], loop: asyncio.AbstractEventLoop) -> MarketDataStreamerV3:
        config = Configuration()
        config.access_token = token
        api_client = ApiClient(configuration=config)

        streamer = MarketDataStreamerV3(
            api_client=api_client,
            instrumentKeys=instrument_keys,
            mode=MarketDataStreamerV3.Mode['FULL'],
        )

        def on_open(*args):
            logger.info('✅ Upstox feed socket opened.')
            if not instrument_keys:
                logger.error('❌ No instrument keys available to subscribe.')
                return
            try:
                streamer.subscribe(instrument_keys, mode=MarketDataStreamerV3.Mode['FULL'])
                logger.info(f'📡 Subscribed to {len(instrument_keys)} strong_sepa instruments.')
            except Exception as exc:
                logger.error(f'❌ Failed to subscribe on open: {exc}')

        def on_message(*args):
            message = args[-1] if args else None
            symbol, volume, ltp, ltq, bid, ask = extract_symbol_and_volume(message)
            if symbol and volume is not None:
                ofi_ratio = self.update_ofi(symbol, ltp, ltq, bid, ask)
                if self.detect_volume_spike(symbol, volume):
                    alert_suffix = ''
                    ofi_status = None
                    if ofi_ratio is not None:
                        if ofi_ratio > 0.3:
                            alert_suffix = ' 🟢 REAL BREAKOUT: Volume Spike + Massive Buying'
                            ofi_status = 'REAL_BREAKOUT'
                        elif ofi_ratio < -0.2:
                            alert_suffix = ' 🔴 FAKEOUT ALERT: Volume Spike but Sellers Dominate'
                            ofi_status = 'FAKEOUT'
                        else:
                            alert_suffix = ' ⚪ Volume Spike with mixed order flow'
                            ofi_status = 'NEUTRAL'

                    alert_msg = f"🚀 LIVE VOLUME SPIKE DETECTED in {symbol} - Possible PERFECT COMBO Entry Triggered!{alert_suffix}"
                    logger.warning(alert_msg)

                    if ofi_status == 'REAL_BREAKOUT' and ltp is not None and ltp > 0:
                        qty = int(RISK_PER_TRADE / ltp)
                        if qty <= 0:
                            logger.warning(f'⚠️ Computed zero quantity for {symbol} at LTP {ltp}')
                        if AUTO_TRADE_ENABLED:
                            order = place_auto_buy_order(symbol, qty, ltp)
                            exec_msg = f"🟢 [REAL TRADE] Placed Market BUY for {qty} shares of {symbol}"
                            logger.info(exec_msg)
                            append_alert(symbol, exec_msg, ofi_ratio=ofi_ratio, ofi_status='REAL_TRADE')
                        else:
                            exec_msg = f"🟢 [PAPER TRADE] Would have bought {qty} shares of {symbol} at {ltp}"
                            logger.info(exec_msg)
                            append_alert(symbol, exec_msg, ofi_ratio=ofi_ratio, ofi_status='PAPER_TRADE')

                    append_alert(symbol, alert_msg, ofi_ratio=ofi_ratio, ofi_status=ofi_status)
            else:
                logger.debug('Received message missing symbol or volume.')

        def on_error(*args):
            error = args[-1] if args else None
            logger.error(f'❌ Upstox feed error: {error}')

        def on_close(*args):
            code = args[0] if len(args) > 0 else None
            msg = args[1] if len(args) > 1 else None
            logger.warning(f'🔌 Upstox feed closed: {code} {msg}')

        streamer.on(streamer.Event['OPEN'], on_open)
        streamer.on(streamer.Event['MESSAGE'], on_message)
        streamer.on(streamer.Event['ERROR'], on_error)
        streamer.on(streamer.Event['CLOSE'], on_close)
        streamer.auto_reconnect(True, interval=5, retry_count=5)
        return streamer

    async def run(self) -> None:
        token = load_access_token()
        if not token:
            return

        instrument_keys = self.get_subscribe_keys()
        if not instrument_keys:
            return

        loop = asyncio.get_running_loop()
        streamer = self.build_streamer(token, instrument_keys, loop)
        logger.info('🔌 Connecting to Upstox strong_sepa live feed...')
        streamer.connect()

        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            if hasattr(streamer, 'disconnect'):
                try:
                    streamer.disconnect()
                except Exception:
                    pass
            elif hasattr(streamer, 'close'):
                try:
                    streamer.close()
                except Exception:
                    pass


async def main() -> None:
    log_health_check()
    collector = SwingMLStockDataCollector()
    await collector.run()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('🛑 SEPA live collector stopped by user.')
