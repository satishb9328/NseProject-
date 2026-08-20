"""
╔══════════════════════════════════════════════════════════════════════════════╗
║         EXPIRY GAMMA BLAST — PURE QUANT ENGINE v3.0 (Hardened)              ║
║         Target: NIFTY / BANKNIFTY  |  Injected Live TBT Data Mode            ║
║                                                                              ║
║  Fixes over v2.5:                                                            ║
║   • DTE now computed from the real EXPIRY date (was: today's close)          ║
║   • True Wilder ATR from H/L/C (was: constant 10.0)                          ║
║   • Proper $-GEX: Γ·S²·0.01·OI·lot (was missing S·0.01 factor)              ║
║   • Gamma flip linearly interpolated between strikes                         ║
║   • IV surface repaired from per-leg anchors + sanity clamped                ║
║   • Dynamic confluence score (PCR / momentum / wall / volume)                ║
║   • Trade-state guard + exit checks (SL/TARGET) + trailing SL               ║
║   • Max-pain, PCR, CSV logging of every signal                               ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import csv
import json
import math
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, List
from scipy.stats import norm

CONFIG = {
    "symbol":              "NIFTY",
    "lot_size":            65,
    "strike_step":         50,
    "volume_surge_mult":   1.8,
    "atr_sl_mult":         1.2,
    "atr_target_mult":     2.5,
    "r_free":              0.065,    # risk-free rate (India context)
    "iv_floor":            8.0,      # %  – min believable IV
    "iv_cap":              120.0,    # %  – max believable IV
    "default_iv":          15.0,     # %  – fallback when no IV at all
    "atr_window":          14,
    "log_file":            "gamma_blast_signals.csv",
}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("GammaBlastEngine")


# ───────────────────────── helpers ─────────────────────────
def _safe_float(x, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except (TypeError, ValueError):
        return default


def bs_gamma(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black–Scholes gamma (∂²V/∂S²)."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    try:
        d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        return norm.pdf(d1) / (S * sigma * math.sqrt(T))
    except (ValueError, ZeroDivisionError):
        return 0.0


def clamp_iv(iv: float) -> float:
    if iv <= 0 or not math.isfinite(iv):
        return CONFIG["default_iv"]
    return min(max(iv, CONFIG["iv_floor"]), CONFIG["iv_cap"])


def repair_iv_surface(strikes: List["OptionStrike"]) -> List["OptionStrike"]:
    """Interpolate missing IVs from per-leg anchors, then clamp to a sane band."""
    c_strikes = [s.strike for s in strikes if s.call_iv > 0]
    c_ivs     = [s.call_iv  for s in strikes if s.call_iv > 0]
    p_strikes = [s.strike for s in strikes if s.put_iv > 0]
    p_ivs     = [s.put_iv  for s in strikes if s.put_iv > 0]

    for s in strikes:
        if s.call_iv <= 0:
            s.call_iv = float(np.interp(s.strike, c_strikes, c_ivs)) if len(c_ivs) >= 2 \
                        else CONFIG["default_iv"]
        if s.put_iv <= 0:
            # use put smile first, fall back to call IV (not call-anchored interp)
            s.put_iv = float(np.interp(s.strike, p_strikes, p_ivs)) if len(p_ivs) >= 2 \
                       else s.call_iv
        s.call_iv = clamp_iv(s.call_iv)
        s.put_iv  = clamp_iv(s.put_iv)
    return strikes


def true_atr(highs: List[float], lows: List[float],
             closes: List[float], window: int = CONFIG["atr_window"]) -> float:
    """Wilder-style ATR from H/L/C. Returns 0.0 if not enough data."""
    n = min(len(highs), len(lows), len(closes))
    if n < window + 1:
        return 0.0
    trs = []
    for i in range(1, window + 1):
        h, l = highs[-i], lows[-i]
        pc   = closes[-(i + 1)]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return float(np.mean(trs)) if trs else 0.0


# ───────────────────────── dataclasses ─────────────────────────
@dataclass
class OptionStrike:
    strike:    float
    expiry:    str
    call_oi:   int   = 0
    call_iv:   float = 0.0
    call_ltp:  float = 0.0
    call_vol:  int   = 0
    put_oi:    int   = 0
    put_iv:    float = 0.0
    put_ltp:   float = 0.0
    put_vol:   int   = 0
    call_gamma: float = 0.0
    put_gamma:  float = 0.0
    net_gex:   float = 0.0


@dataclass
class GammaProfile:
    spot:               float
    expiry:             str
    dte:                float
    strikes:            List[OptionStrike] = field(default_factory=list)
    gex_by_strike:      Dict[float, float] = field(default_factory=dict)
    net_gex:            float = 0.0
    gamma_flip_level:   float = 0.0
    call_wall:          float = 0.0
    put_wall:           float = 0.0
    pcr:                float = 0.0
    max_pain:           float = 0.0


@dataclass
class EntrySignal:
    timestamp:   str
    symbol:      str
    spot:        float
    direction:   str           # "CE" | "PE"
    entry_type:  str
    entry_price: float
    initial_sl:  float
    target:      float
    rr:          float
    score:       int


# ───────────────────────── data processor ─────────────────────────
class UpstoxDataProcessor:
    def __init__(self, symbol: str):
        self.symbol = symbol

    def _fractional_dte(self, expiry_date: str) -> float:
        """Years to expiry, evaluated to 15:30 on expiry day."""
        now = datetime.now()
        try:
            exp = datetime.strptime(expiry_date, "%Y-%m-%d").replace(
                hour=15, minute=30, second=0, microsecond=0)
        except (ValueError, TypeError):
            exp = now.replace(hour=15, minute=30, second=0, microsecond=0)
        years = (exp - now).total_seconds() / (365.0 * 24.0 * 3600.0)
        return max(years, 1e-6)

    def inject_and_process(self, external_tbt_dict: dict) -> Optional[GammaProfile]:
        try:
            spot = _safe_float(external_tbt_dict.get("spot_price"))
            if spot <= 0:
                raise ValueError("Invalid spot_price")
            expiry_date = external_tbt_dict["expiry_date"]
            dte = self._fractional_dte(expiry_date)

            strikes: List[OptionStrike] = []
            for row in external_tbt_dict.get("data", []):
                K = _safe_float(row.get("strike_price"))
                if K <= 0:
                    continue
                s = OptionStrike(strike=K, expiry=expiry_date)
                s.call_oi  = int(_safe_float(row.get("call_oi", 0)))
                s.call_iv  = _safe_float(row.get("call_iv", 0.0))
                s.call_ltp = _safe_float(row.get("call_ltp", 0.0))
                s.call_vol = int(_safe_float(row.get("call_volume", 0)))
                s.put_oi   = int(_safe_float(row.get("put_oi", 0)))
                s.put_iv   = _safe_float(row.get("put_iv", 0.0))
                s.put_ltp  = _safe_float(row.get("put_ltp", 0.0))
                s.put_vol  = int(_safe_float(row.get("put_volume", 0)))
                strikes.append(s)

            if not strikes:
                return None
            strikes = repair_iv_surface(strikes)

            lot, r_f = CONFIG["lot_size"], CONFIG["r_free"]
            for s in strikes:
                s.call_gamma = bs_gamma(spot, s.strike, dte, r_f, s.call_iv / 100.0)
                s.put_gamma  = bs_gamma(spot, s.strike, dte, r_f, s.put_iv / 100.0)
                # Dealer convention: long calls (+γ), short puts (−γ)
                # $ gamma per 1% spot move = Γ · S² · 0.01 · OI · lot
                s.net_gex = ((s.call_oi * s.call_gamma - s.put_oi * s.put_gamma)
                             * lot * spot * spot * 0.01)

            profile = GammaProfile(spot=spot, expiry=expiry_date, dte=dte, strikes=strikes)
            self._build_metrics(profile)
            return profile

        except Exception as e:
            log.exception(f"Injection Engine Error: {e}")
            return None

    def _build_metrics(self, p: GammaProfile) -> None:
        df = pd.DataFrame([{"strike": s.strike, "call_oi": s.call_oi,
                            "put_oi": s.put_oi, "net_gex": s.net_gex}
                           for s in p.strikes])
        tot_call = float(df["call_oi"].sum())
        tot_put  = float(df["put_oi"].sum())
        p.pcr     = round(tot_put / tot_call, 3) if tot_call > 0 else 0.0
        p.net_gex = float(df["net_gex"].sum())
        p.gex_by_strike = {s.strike: s.net_gex for s in p.strikes}

        # Interpolated gamma flip: linear interp of the strike where cumGEX = 0
        sorted_s = sorted(p.strikes, key=lambda x: x.strike)
        p.gamma_flip_level = p.spot
        prev_cum, prev_strike = 0.0, None
        for s in sorted_s:
            new_cum = prev_cum + s.net_gex
            if prev_strike is not None and (
                    (prev_cum < 0 <= new_cum) or (prev_cum > 0 >= new_cum)):
                denom = new_cum - prev_cum
                frac = (-prev_cum / denom) if denom != 0 else 0.0
                p.gamma_flip_level = prev_strike + frac * (s.strike - prev_strike)
                break
            prev_cum, prev_strike = new_cum, s.strike

        p.call_wall = float(df.nlargest(1, "call_oi")["strike"].values[0])
        p.put_wall  = float(df.nlargest(1, "put_oi")["strike"].values[0])
        p.max_pain  = self._max_pain(sorted_s)

    @staticmethod
    def _max_pain(sorted_strikes: List[OptionStrike]) -> float:
        """Strike that minimises total in-the-money buyer payout at expiry."""
        all_k = [s.strike for s in sorted_strikes]
        best_strike, best_pain = None, None
        for exp_k in all_k:
            pain = 0.0
            for s in sorted_strikes:
                if s.strike < exp_k:                              # ITM calls
                    pain += (exp_k - s.strike) * s.call_oi
                elif s.strike > exp_k:                            # ITM puts
                    pain += (s.strike - exp_k) * s.put_oi
            if best_pain is None or pain < best_pain:
                best_pain, best_strike = pain, exp_k
        return float(best_strike) if best_strike else 0.0


# ───────────────────────── signal engine ─────────────────────────
class HardenedSignalEngine:
    def __init__(self):
        self.active_trade: Optional[Dict] = None

    # ---- state ----
    def in_trade(self) -> bool:
        return self.active_trade is not None

    def check_exit(self, current_spot: float) -> Optional[str]:
        """Return 'SL' / 'TARGET' / None for the active trade."""
        if not self.active_trade:
            return None
        t = self.active_trade
        if t["direction"] == "CE":
            if current_spot <= t["sl"]:     return "SL"
            if current_spot >= t["target"]: return "TARGET"
        else:
            if current_spot >= t["sl"]:     return "SL"
            if current_spot <= t["target"]: return "TARGET"
        return None

    def close_trade(self, reason: str) -> None:
        if self.active_trade:
            log.info(f"Trade closed ({reason}): {self.active_trade}")
            self.active_trade = None

    def manage_trailing_sl(self, current_spot: float, atr: float) -> Optional[float]:
        if not self.active_trade or atr <= 0:
            return None
        t = self.active_trade
        if t["direction"] == "CE" and current_spot >= t["entry"] + atr:
            new_sl = current_spot - atr * 0.8
            if new_sl > t["sl"]:
                t["sl"] = round(new_sl, 1)
        elif t["direction"] == "PE" and current_spot <= t["entry"] - atr:
            new_sl = current_spot + atr * 0.8
            if new_sl < t["sl"]:
                t["sl"] = round(new_sl, 1)
        return t["sl"]

    # ---- entries ----
    def evaluate_entry(self, p: GammaProfile, prev_spot: float,
                       highs: List[float], lows: List[float],
                       closes: List[float], volumes: List[float]
                       ) -> Optional[EntrySignal]:
        if self.in_trade():
            return None                                  # one position at a time
        if len(closes) < CONFIG["atr_window"] + 1:
            return None

        atr = true_atr(highs, lows, closes)
        if atr <= 0:
            return None
        avg_vol   = max(float(np.mean(volumes[-10:])), 1.0)
        vol_surge = volumes[-1] / avg_vol

        flip = p.gamma_flip_level
        flip_crossed_up   = prev_spot < flip <= p.spot
        flip_crossed_down = prev_spot > flip >= p.spot
        if not (flip_crossed_up or flip_crossed_down):
            return None
        if vol_surge < CONFIG["volume_surge_mult"]:
            return None

        direction = "CE" if flip_crossed_up else "PE"
        sl_mult, tgt_mult = CONFIG["atr_sl_mult"], CONFIG["atr_target_mult"]
        if direction == "CE":
            sl, target = p.spot - atr * sl_mult, p.spot + atr * tgt_mult
            risk, reward = p.spot - sl, target - p.spot
        else:
            sl, target = p.spot + atr * sl_mult, p.spot - atr * tgt_mult
            risk, reward = sl - p.spot, p.spot - target
        rr = round(reward / risk, 2) if risk > 0 else 0.0

        score = self._score_confluence(p, direction, vol_surge, atr, closes)
        sig = EntrySignal(
            timestamp=datetime.now().strftime("%H:%M:%S"),
            symbol=CONFIG["symbol"], spot=p.spot,
            direction=direction, entry_type="FLIP_REVERSAL",
            entry_price=p.spot, initial_sl=round(sl, 1),
            target=round(target, 1), rr=rr, score=score,
        )
        self.active_trade = {"entry": p.spot, "direction": direction,
                             "sl": sig.initial_sl, "target": sig.target}
        self._log_signal(sig)
        return sig

    @staticmethod
    def _score_confluence(p: GammaProfile, direction: str,
                          vol_surge: float, atr: float, closes: List[float]) -> int:
        score = 50
        if direction == "CE" and p.pcr > 1.2:  score += 10       # put-heavy → bullish tint
        if direction == "PE" and p.pcr < 0.8:  score += 10
        score += min(int((vol_surge - 1) * 20), 20)              # volume conviction
        mom = closes[-1] - closes[-5]
        if direction == "CE" and mom > 0:      score += 10
        if direction == "PE" and mom < 0:      score += 10
        if direction == "CE" and abs(p.spot - p.put_wall)  < atr: score += 10  # wall bounce
        if direction == "PE" and abs(p.spot - p.call_wall) < atr: score += 10
        return max(0, min(score, 100))

    @staticmethod
    def _log_signal(sig: EntrySignal) -> None:
        try:
            with open(CONFIG["log_file"], "a", newline="") as f:
                w = csv.writer(f)
                if f.tell() == 0:
                    w.writerow(["timestamp", "symbol", "direction", "entry_type",
                                "spot", "sl", "target", "rr", "score"])
                w.writerow([sig.timestamp, sig.symbol, sig.direction, sig.entry_type,
                            sig.spot, sig.initial_sl, sig.target, sig.rr, sig.score])
        except OSError as e:
            log.warning(f"Could not write signal log: {e}")


# ───────────────────────── live entry point ─────────────────────────
if __name__ == "__main__":
    import argparse
    import json
    import sys

    parser = argparse.ArgumentParser(
        description="Gamma Blast Engine v3 - process live market TBT payload JSON"
    )
    parser.add_argument(
        "--payload-file", "-f",
        default="default_payload.json",
        help="Path to JSON file containing live external TBT payload (default: default_payload.json)"
    )
    args = parser.parse_args()

    payload_path = Path(args.payload_file)
    if args.payload_file == "default_payload.json":
        print("Using fallback payload file: default_payload.json")
    else:
        print(f"Using payload file: {args.payload_file}")

    if not payload_path.exists():
        sample_payload = {
            "spot_price": 22100,
            "expiry_date": datetime.now().strftime("%Y-%m-%d"),
            "data": [],
        }
        try:
            payload_path.write_text(json.dumps(sample_payload, indent=2), encoding="utf-8")
            print(f"Created fallback payload file: {payload_path}")
        except Exception as exc:
            log.error("Failed to create default payload file: %s", exc)
            sys.exit(1)

    try:
        with payload_path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception as exc:
        log.error("Failed to load live payload file: %s", exc)
        sys.exit(1)

    processor = UpstoxDataProcessor(symbol="NIFTY")
    profile = processor.inject_and_process(payload)

    if profile is None:
        log.error("Gamma engine failed to build profile from live payload.")
        sys.exit(1)

    print("=== Gamma Profile ===")
    print(f"spot={profile.spot}  dte={profile.dte:.5f}y  netGEX={profile.net_gex:,.0f}")
    print(f"flip={profile.gamma_flip_level:.2f}  callWall={profile.call_wall}  "
          f"putWall={profile.put_wall}  maxPain={profile.max_pain}  pcr={profile.pcr}")
