"""Redis-driven Gamma Blast strategy worker.

The WebSocket producer stores its latest JSON payload in ``live_options_data`` and
publishes a notification or the payload itself to ``gamma:live_options_updated``.
No market-data REST calls or local files are used by this module.
"""

from __future__ import annotations

import json
import math
import os
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Deque, Dict, Iterable, List, Optional

try:
    import redis
except ImportError:  # Keeps Django imports available until redis-py is installed.
    redis = None


CONFIG = {
    "symbol": os.getenv("GAMMA_SYMBOL", "NIFTY"),
    "lot_size": int(os.getenv("GAMMA_LOT_SIZE", "65")),
    "volume_surge_mult": 1.8,
    "atr_sl_mult": 1.2,
    "atr_target_mult": 2.5,
    "risk_per_trade": 0.02,
    "capital": 100000,
    "max_positions": 1,
    "history_size": 120,
}

REDIS_URL = os.getenv("GAMMA_REDIS_URL", "redis://127.0.0.1:6379/0")
REDIS_DATA_KEY = os.getenv("GAMMA_REDIS_DATA_KEY", "live_options_data")
REDIS_CHANNEL = os.getenv("GAMMA_REDIS_CHANNEL", "gamma:live_options_updated")


class GammaBotError(RuntimeError):
    """Raised when the Redis-driven worker cannot be started."""


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return default if value in (None, "") else float(value)
    except (TypeError, ValueError):
        return default


def bs_gamma(spot: float, strike: float, dte: float, rate: float, volatility: float) -> float:
    if spot <= 0 or strike <= 0 or dte <= 0 or volatility <= 0:
        return 0.0
    d1 = (math.log(spot / strike) + (rate + 0.5 * volatility**2) * dte) / (volatility * math.sqrt(dte))
    normal_pdf = math.exp(-0.5 * d1**2) / math.sqrt(2 * math.pi)
    return normal_pdf / (spot * volatility * math.sqrt(dte))


@dataclass
class OptionStrike:
    strike: float
    call_oi: float = 0.0
    call_iv: float = 0.0
    call_ltp: float = 0.0
    call_vol: float = 0.0
    put_oi: float = 0.0
    put_iv: float = 0.0
    put_ltp: float = 0.0
    put_vol: float = 0.0
    net_gex: float = 0.0


@dataclass
class GammaProfile:
    spot: float
    expiry: str
    dte: float
    strikes: List[OptionStrike] = field(default_factory=list)
    net_gex: float = 0.0
    gamma_flip_level: float = 0.0
    call_wall: float = 0.0
    put_wall: float = 0.0
    pcr: float = 0.0
    skew: float = 0.0


@dataclass
class EntrySignal:
    timestamp: str
    symbol: str
    spot: float
    direction: str
    entry_type: str
    entry_price: float
    initial_sl: float
    target: float
    rr: float
    score: int
    position_size: int


class GammaProcessor:
    """Transforms an already-received live option-chain snapshot into gamma metrics."""

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.risk_free_rate = 0.065

    def process_payload(self, payload: Dict[str, Any]) -> Optional[GammaProfile]:
        spot = _number(payload.get("spot_price"))
        expiry = str(payload.get("expiry_date") or "")
        rows = payload.get("data")
        if spot <= 0 or not expiry or not isinstance(rows, list):
            return None

        strikes = [self._to_strike(row) for row in rows if isinstance(row, dict)]
        strikes = [strike for strike in strikes if strike is not None]
        if not strikes:
            return None

        dte = self._fractional_dte(expiry)
        self._repair_iv_surface(strikes)
        for strike in strikes:
            call_gamma = bs_gamma(spot, strike.strike, dte, self.risk_free_rate, strike.call_iv / 100)
            put_gamma = bs_gamma(spot, strike.strike, dte, self.risk_free_rate, strike.put_iv / 100)
            strike.net_gex = (strike.call_oi * call_gamma - strike.put_oi * put_gamma) * CONFIG["lot_size"] * spot

        profile = GammaProfile(spot=spot, expiry=expiry, dte=dte, strikes=strikes)
        self._build_metrics(profile)
        return profile

    def _fractional_dte(self, expiry: str) -> float:
        try:
            expiry_close = datetime.strptime(expiry, "%Y-%m-%d").replace(hour=15, minute=30)
        except ValueError:
            return 1e-6
        return max((expiry_close - datetime.now()).total_seconds() / (365 * 24 * 60 * 60), 1e-6)

    @staticmethod
    def _to_strike(row: Dict[str, Any]) -> Optional[OptionStrike]:
        strike = _number(row.get("strike_price", row.get("strike")))
        if strike <= 0:
            return None
        return OptionStrike(
            strike=strike,
            call_oi=_number(row.get("call_oi")), call_iv=_number(row.get("call_iv")),
            call_ltp=_number(row.get("call_ltp")), call_vol=_number(row.get("call_volume")),
            put_oi=_number(row.get("put_oi")), put_iv=_number(row.get("put_iv")),
            put_ltp=_number(row.get("put_ltp")), put_vol=_number(row.get("put_volume")),
        )

    @staticmethod
    def _repair_iv_surface(strikes: List[OptionStrike]) -> None:
        valid_call_ivs = [strike.call_iv for strike in strikes if strike.call_iv > 0]
        valid_put_ivs = [strike.put_iv for strike in strikes if strike.put_iv > 0]
        default_call_iv = sum(valid_call_ivs) / len(valid_call_ivs) if valid_call_ivs else 15.0
        default_put_iv = sum(valid_put_ivs) / len(valid_put_ivs) if valid_put_ivs else 15.0
        for strike in strikes:
            strike.call_iv = strike.call_iv or default_call_iv
            strike.put_iv = strike.put_iv or default_put_iv

    @staticmethod
    def _build_metrics(profile: GammaProfile) -> None:
        profile.net_gex = sum(strike.net_gex for strike in profile.strikes)
        profile.pcr = round(sum(strike.put_oi for strike in profile.strikes) / max(sum(strike.call_oi for strike in profile.strikes), 1), 3)
        profile.call_wall = max(profile.strikes, key=lambda strike: strike.call_oi).strike
        profile.put_wall = max(profile.strikes, key=lambda strike: strike.put_oi).strike
        atm = min(profile.strikes, key=lambda strike: abs(strike.strike - profile.spot))
        profile.skew = atm.put_iv - atm.call_iv

        cumulative_gex = 0.0
        profile.gamma_flip_level = profile.spot
        for strike in sorted(profile.strikes, key=lambda item: item.strike):
            previous_gex = cumulative_gex
            cumulative_gex += strike.net_gex
            if (previous_gex < 0 <= cumulative_gex) or (previous_gex >= 0 > cumulative_gex):
                profile.gamma_flip_level = strike.strike
                break


class SignalEngine:
    def __init__(self) -> None:
        self.active_trade: Optional[Dict[str, Any]] = None

    @staticmethod
    def _compute_atr(closes: Iterable[float]) -> float:
        values = list(closes)
        if len(values) < 15:
            return 0.0
        return sum(abs(values[index] - values[index - 1]) for index in range(-14, 0)) / 14

    @staticmethod
    def _volume_surge(volumes: Iterable[float]) -> float:
        values = list(volumes)
        if len(values) < 10:
            return 0.0
        baseline = sum(values[-10:-1]) / 9
        return values[-1] / max(baseline, 1.0)

    def evaluate_entry(self, profile: GammaProfile, previous_spot: float, closes: Iterable[float], volumes: Iterable[float]) -> Optional[EntrySignal]:
        atr = self._compute_atr(closes)
        if atr <= 0:
            return None
        crossed_up = previous_spot < profile.gamma_flip_level <= profile.spot
        crossed_down = previous_spot > profile.gamma_flip_level >= profile.spot
        if not (crossed_up or crossed_down):
            return None

        volume_surge = self._volume_surge(volumes)
        flip_distance = abs(profile.spot - profile.gamma_flip_level) / profile.spot * 100
        score = min(30, int(max(0, volume_surge - 1.5) * 20)) if volume_surge >= CONFIG["volume_surge_mult"] else 0
        score += min(20, int(flip_distance * 4)) + min(20, int(abs(profile.skew) * 2))
        score += 15 if profile.pcr < 0.6 or profile.pcr > 1.4 else 8 if profile.pcr < 0.8 or profile.pcr > 1.2 else 0
        if score < 50:
            return None

        direction = "CE" if crossed_up else "PE"
        sl = profile.spot - atr * CONFIG["atr_sl_mult"] if direction == "CE" else profile.spot + atr * CONFIG["atr_sl_mult"]
        target = profile.spot + atr * CONFIG["atr_target_mult"] if direction == "CE" else profile.spot - atr * CONFIG["atr_target_mult"]
        risk_amount = CONFIG["capital"] * CONFIG["risk_per_trade"]
        size = max(1, int(risk_amount / (abs(profile.spot - sl) * CONFIG["lot_size"])))
        signal = EntrySignal(datetime.now().isoformat(timespec="seconds"), CONFIG["symbol"], profile.spot, direction, "FLIP_REVERSAL", profile.spot, round(sl, 1), round(target, 1), round(CONFIG["atr_target_mult"] / CONFIG["atr_sl_mult"], 2), score, size)
        self.active_trade = {"entry": profile.spot, "direction": direction, "sl": signal.initial_sl, "target": signal.target}
        return signal

    def manage_trade(self, spot: float, atr: float) -> Optional[str]:
        if not self.active_trade:
            return None
        trade = self.active_trade
        if trade["direction"] == "CE" and spot >= trade["entry"] + atr:
            trade["sl"] = max(trade["sl"], round(spot - atr * 0.8, 1))
        elif trade["direction"] == "PE" and spot <= trade["entry"] - atr:
            trade["sl"] = min(trade["sl"], round(spot + atr * 0.8, 1))
        hit_stop = spot <= trade["sl"] if trade["direction"] == "CE" else spot >= trade["sl"]
        hit_target = spot >= trade["target"] if trade["direction"] == "CE" else spot <= trade["target"]
        if hit_stop or hit_target:
            reason = "SL_HIT" if hit_stop else "TARGET_HIT"
            self.active_trade = None
            return reason
        return None


def normalize_live_payload(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize direct snapshots and Live Dashboard ``instrument_data`` maps."""
    if isinstance(payload.get("data"), list):
        normalized = dict(payload)
        normalized["spot_price"] = _find_spot_price(payload) or _estimate_spot_from_rows(normalized["data"])
        return normalized
    instrument_data = payload.get("instrument_data")
    if not isinstance(instrument_data, dict):
        return None
    rows: Dict[float, Dict[str, Any]] = {}
    for item in instrument_data.values():
        if not isinstance(item, dict):
            continue
        strike = _number(item.get("strike"))
        side = str(item.get("option_type", "")).upper()
        if strike <= 0 or side not in {"CE", "PE"}:
            continue
        row = rows.setdefault(strike, {"strike_price": strike})
        prefix = "call" if side == "CE" else "put"
        row[f"{prefix}_oi"] = _number(item.get("oi"))
        row[f"{prefix}_iv"] = _number(item.get("iv"))
        row[f"{prefix}_ltp"] = _number(item.get("ltp"))
        row[f"{prefix}_volume"] = _number(item.get("volume"))
    option_rows = list(rows.values())
    return {
        "spot_price": _find_spot_price(payload) or _estimate_spot_from_rows(option_rows),
        "expiry_date": payload.get("expiry_date"),
        "data": option_rows,
    }


def _find_spot_price(payload: Any) -> float:
    """Find an explicitly supplied index spot without mistaking option LTP for it."""
    spot_keys = {"spot_price", "underlying_spot", "underlying_ltp", "index_ltp", "nifty_ltp"}
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key.lower() in spot_keys:
                spot = _number(value)
                if spot > 0:
                    return spot
        for value in payload.values():
            spot = _find_spot_price(value)
            if spot > 0:
                return spot
    elif isinstance(payload, list):
        for value in payload:
            spot = _find_spot_price(value)
            if spot > 0:
                return spot
    return 0.0


def _estimate_spot_from_rows(rows: Iterable[Dict[str, Any]]) -> float:
    """Infer index spot from put-call parity, then use the median active strike."""
    parity_estimates = []
    active_strikes = []
    for row in rows:
        strike = _number(row.get("strike_price", row.get("strike")))
        call_ltp = _number(row.get("call_ltp"))
        put_ltp = _number(row.get("put_ltp"))
        if strike <= 0:
            continue
        if call_ltp > 0 and put_ltp > 0:
            parity_estimates.append(strike + call_ltp - put_ltp)
        if call_ltp > 0 or put_ltp > 0 or _number(row.get("call_oi")) > 0 or _number(row.get("put_oi")) > 0:
            active_strikes.append(strike)
    estimates = sorted(parity_estimates or active_strikes)
    if not estimates:
        return 0.0
    middle = len(estimates) // 2
    return estimates[middle] if len(estimates) % 2 else (estimates[middle - 1] + estimates[middle]) / 2


class RedisGammaBot:
    """Owns the pub/sub thread and hands each Redis update directly to the strategy."""

    def __init__(self, redis_url: str = REDIS_URL, data_key: str = REDIS_DATA_KEY, channel: str = REDIS_CHANNEL) -> None:
        self.redis_url = redis_url
        self.data_key = data_key
        self.channel = channel
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._redis_client: Any = None
        self.processor = GammaProcessor(CONFIG["symbol"])
        self.signal_engine = SignalEngine()
        self.closes: Deque[float] = deque(maxlen=CONFIG["history_size"])
        self.volumes: Deque[float] = deque(maxlen=CONFIG["history_size"])
        self.previous_spot = 0.0
        self.last_signal: Optional[Dict[str, Any]] = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "symbol": CONFIG["symbol"],
            "message": "🟢 Gamma Engine Active: Monitoring for volatility breakouts...",
            "heartbeat": True,
        }
        self.last_profile: Optional[Dict[str, Any]] = None
        self.last_error: Optional[str] = None
        self.processed_updates = 0

    def start(self) -> bool:
        with self._lock:
            if self.is_running:
                return False
            if redis is None:
                raise GammaBotError("redis-py is not installed; install the 'redis' package in the active environment.")
            self._redis_client = redis.Redis.from_url(self.redis_url, decode_responses=True)
            try:
                self._redis_client.ping()
            except Exception as exc:
                raise GammaBotError(f"Redis is unavailable at {self.redis_url}: {exc}") from exc
            self._stop_event.clear()
            self._prime_from_redis()
            self._thread = threading.Thread(target=self._listen, name="gamma-redis-listener", daemon=True)
            self._thread.start()
            return True

    def stop(self) -> bool:
        with self._lock:
            if not self.is_running:
                return False
            self._stop_event.set()
            return True

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def status(self) -> Dict[str, Any]:
        return {
            "running": self.is_running,
            "redis_url": self.redis_url,
            "data_key": self.data_key,
            "channel": self.channel,
            "processed_updates": self.processed_updates,
            "last_signal": self.last_signal,
            "last_profile": self.last_profile,
            "last_error": self.last_error,
            "active_trade": self.signal_engine.active_trade,
        }

    def _prime_from_redis(self) -> None:
        """Process any already-published snapshot so /gamma/status/ has data immediately."""
        if self._redis_client is None:
            return
        try:
            raw = self._redis_client.get(self.data_key)
            if raw:
                self._process_message(raw)
        except Exception as exc:
            self.last_error = str(exc)

    def _listen(self) -> None:
        try:
            with self._redis_client.pubsub(ignore_subscribe_messages=True) as pubsub:
                pubsub.subscribe(self.channel)
                while not self._stop_event.is_set():
                    message = pubsub.get_message(timeout=1.0)
                    if message and message.get("type") == "message":
                        self._process_message(message.get("data"))
        except Exception as exc:
            self.last_error = str(exc)

    def _process_message(self, message_data: Any) -> None:
        try:
            raw_payload = json.loads(message_data) if isinstance(message_data, str) and message_data.strip().startswith("{") else self._redis_client.get(self.data_key)
            if isinstance(raw_payload, str):
                raw_payload = json.loads(raw_payload)
            if not isinstance(raw_payload, dict):
                raise ValueError("Redis update did not contain a JSON object")
            payload = normalize_live_payload(raw_payload)
            if not payload:
                raise ValueError("Redis payload is missing option-chain data")
            profile = self.processor.process_payload(payload)
            if not profile:
                raise ValueError("Redis payload is missing valid spot, expiry, or strike data")

            total_volume = sum(strike.call_vol + strike.put_vol for strike in profile.strikes)
            self.closes.append(profile.spot)
            self.volumes.append(total_volume)
            if self.previous_spot > 0:
                signal = self.signal_engine.evaluate_entry(profile, self.previous_spot, self.closes, self.volumes)
                if signal:
                    self.last_signal = asdict(signal)
            atr = self.signal_engine._compute_atr(self.closes)
            self.signal_engine.manage_trade(profile.spot, atr)
            self.previous_spot = profile.spot
            self.last_profile = {"spot": profile.spot, "expiry": profile.expiry, "gamma_flip_level": profile.gamma_flip_level, "net_gex": profile.net_gex, "pcr": profile.pcr, "skew": profile.skew}
            self.processed_updates += 1
            self.last_error = None
        except Exception as exc:
            self.last_error = str(exc)


_default_bot: Optional[RedisGammaBot] = None
_default_bot_lock = threading.Lock()


def get_gamma_bot() -> RedisGammaBot:
    """Return the process-wide singleton Gamma bot shared by views and apps."""
    global _default_bot
    with _default_bot_lock:
        if _default_bot is None:
            _default_bot = RedisGammaBot()
        return _default_bot


def main() -> None:
    """CLI entry point: keep the Redis pub/sub engine running continuously."""
    print("🚀 Starting Gamma Blast Engine...")

    bot = RedisGammaBot()
    try:
        bot.start()
    except GammaBotError as e:
        print(f"❌ Error: {e}")
        return

    print(f"✅ Connected to Redis at {bot.redis_url}")
    print(f"📡 Listening on channel '{bot.channel}' (data key: {bot.data_key})")
    print("🔄 Waiting for live option-chain updates. Press Ctrl+C to exit.")

    try:
        while True:
            if not bot.is_running:
                print("⚠️ Listener thread stopped unexpectedly — restarting...")
                try:
                    bot.start()
                except GammaBotError as e:
                    print(f"❌ Error: {e}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 KeyboardInterrupt received — shutting down gracefully...")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        bot.stop()
        print("👋 Gamma Blast Engine stopped.")


if __name__ == "__main__":
    main()