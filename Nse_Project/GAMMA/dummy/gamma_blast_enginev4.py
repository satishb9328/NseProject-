"""
╔══════════════════════════════════════════════════════════════════════════════╗
║         EXPIRY GAMMA BLAST — PURE QUANT ENGINE v3.1 (FIXED)                 ║
║         Target: NIFTY / BANKNIFTY  |  Injected Live TBT Data Mode            ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import csv
import json
import math
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from scipy.stats import norm

# ──────────────────────────── CONFIG ────────────────────────────
CONFIG = {
    "symbol":              "NIFTY",
    "lot_size":            65,
    "strike_step":         50,
    "volume_surge_mult":   1.8,
    "atr_sl_mult":         1.2,
    "atr_target_mult":     2.5,
    "r_free":              0.065,
    "iv_floor":            8.0,
    "iv_cap":              120.0,
    "default_iv":          15.0,
    "atr_window":          14,
    "ema_short":           20,
    "ema_long":            50,
    "rsi_period":          14,
    "min_rr":              1.5,
    "score_threshold":     70,
    "log_file":            "logs/gamma_blast_signals.csv",
    "max_position_lots":   5,
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler('logs/gamma_engine.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("GammaBlastEngine")

# ──────────────────────────── HELPERS ────────────────────────────
def _safe_float(x, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except (TypeError, ValueError):
        return default

def clamp_iv(iv: float) -> float:
    if iv <= 0 or not math.isfinite(iv):
        return CONFIG["default_iv"]
    return min(max(iv, CONFIG["iv_floor"]), CONFIG["iv_cap"])

def bs_delta(S: float, K: float, T: float, r: float, sigma: float, option_type: str = "call") -> float:
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    try:
        d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        delta = norm.cdf(d1)
        return delta if option_type == "call" else delta - 1.0
    except (ValueError, ZeroDivisionError):
        return 0.0

def bs_gamma(S: float, K: float, T: float, r: float, sigma: float) -> float:
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    try:
        d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        return norm.pdf(d1) / (S * sigma * math.sqrt(T))
    except (ValueError, ZeroDivisionError):
        return 0.0

def true_atr(highs: List[float], lows: List[float], closes: List[float], window: int = CONFIG["atr_window"]) -> float:
    n = min(len(highs), len(lows), len(closes))
    if n < window + 1:
        return 0.0
    
    trs = []
    for i in range(1, n):
        h, l = highs[-(i)], lows[-(i)]
        pc = closes[-(i + 1)]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    
    if len(trs) < window:
        return float(np.mean(trs)) if trs else 0.0
    
    atr = float(np.mean(trs[:window]))
    for i in range(window, len(trs)):
        atr = (atr * (window - 1) + trs[i]) / window
    
    return atr

def calculate_ema(closes: List[float], period: int) -> float:
    if len(closes) < period:
        return closes[-1] if closes else 0.0
    series = pd.Series(closes)
    return float(series.ewm(span=period, adjust=False).iloc[-1])

def calculate_rsi(closes: List[float], period: int = CONFIG["rsi_period"]) -> float:
    if len(closes) < period + 1:
        return 50.0
    series = pd.Series(closes)
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    if loss.iloc[-1] == 0:
        return 100.0
    rs = gain.iloc[-1] / loss.iloc[-1]
    return 100 - (100 / (1 + rs))

def repair_iv_surface(strikes: List["OptionStrike"], spot: float) -> List["OptionStrike"]:
    if not strikes:
        return strikes
    
    c_strikes = [s.strike for s in strikes if s.call_iv > 0]
    c_ivs = [s.call_iv for s in strikes if s.call_iv > 0]
    p_strikes = [s.strike for s in strikes if s.put_iv > 0]
    p_ivs = [s.put_iv for s in strikes if s.put_iv > 0]
    
    if not c_ivs and not p_ivs:
        default_iv = CONFIG["default_iv"]
        for s in strikes:
            moneyness = s.strike / spot
            smile_adjust = 0.0
            if moneyness < 0.93:
                smile_adjust = 0.08
            elif moneyness < 0.97:
                smile_adjust = 0.04
            elif moneyness > 1.07:
                smile_adjust = 0.06
            elif moneyness > 1.03:
                smile_adjust = 0.03
            
            s.call_iv = clamp_iv(default_iv + smile_adjust * 100)
            s.put_iv = clamp_iv(default_iv + (0.05 + smile_adjust * 0.5) * 100)
        return strikes
    
    for s in strikes:
        if s.call_iv <= 0:
            if len(c_ivs) >= 2:
                s.call_iv = float(np.interp(s.strike, c_strikes, c_ivs, left=CONFIG["default_iv"], right=CONFIG["default_iv"]))
            elif len(c_ivs) == 1:
                s.call_iv = c_ivs[0]
            else:
                s.call_iv = clamp_iv(s.put_iv * 0.92 if s.put_iv > 0 else CONFIG["default_iv"])
        
        if s.put_iv <= 0:
            if len(p_ivs) >= 2:
                s.put_iv = float(np.interp(s.strike, p_strikes, p_ivs, left=CONFIG["default_iv"], right=CONFIG["default_iv"]))
            elif len(p_ivs) == 1:
                s.put_iv = p_ivs[0]
            else:
                s.put_iv = clamp_iv(s.call_iv * 1.08 if s.call_iv > 0 else CONFIG["default_iv"])
        
        s.call_iv = clamp_iv(s.call_iv)
        s.put_iv = clamp_iv(s.put_iv)
    
    return strikes

# ──────────────────────────── DATACLASSES ────────────────────────────
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
    call_delta: float = 0.0
    put_delta:  float = 0.0

@dataclass
class GammaProfile:
    spot:               float
    expiry:             str
    dte:                float
    strikes:            List[OptionStrike] = field(default_factory=list)
    gex_by_strike:      Dict[float, float] = field(default_factory=dict)
    net_gex:            float = 0.0
    gamma_flip_level:   float = 0.0
    gamma_flip_clusters: List[float] = field(default_factory=list)
    call_wall:          float = 0.0
    put_wall:           float = 0.0
    pcr:                float = 0.0
    max_pain:           float = 0.0
    net_delta:          float = 0.0
    vol_skew:           float = 0.0
    gex_clusters:       List[Tuple[float, float]] = field(default_factory=list)

@dataclass
class EntrySignal:
    timestamp:   str
    symbol:      str
    spot:        float
    direction:   str
    entry_type:  str
    entry_price: float
    initial_sl:  float
    target:      float
    rr:          float
    score:       int
    position_size: float = 1.0
    confidence:  str = "MEDIUM"

# ──────────────────────────── DATA PROCESSOR ────────────────────────────
class UpstoxDataProcessor:
    def __init__(self, symbol: str):
        self.symbol = symbol

    def _fractional_dte(self, expiry_date: str) -> float:
        now = datetime.now()
        try:
            exp = datetime.strptime(expiry_date, "%Y-%m-%d").replace(
                hour=15, minute=30, second=0, microsecond=0)
            
            if exp < now:
                exp = exp + timedelta(days=7)
                while exp < now:
                    exp = exp + timedelta(days=7)
                    
        except (ValueError, TypeError):
            days_until_thursday = (3 - now.weekday()) % 7
            if days_until_thursday == 0:
                days_until_thursday = 7
            exp = now.replace(hour=15, minute=30) + timedelta(days=days_until_thursday)
        
        years = (exp - now).total_seconds() / (365.0 * 24.0 * 3600.0)
        return max(years, 1e-6)

    def inject_and_process(self, external_tbt_dict: dict) -> Optional[GammaProfile]:
        try:
            spot = _safe_float(external_tbt_dict.get("spot_price"))
            if spot <= 0:
                raise ValueError("Invalid spot_price")
            
            expiry_date = external_tbt_dict.get("expiry_date", "")
            if not expiry_date:
                expiry_date = datetime.now().strftime("%Y-%m-%d")
            
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
                log.warning("No strikes found in payload")
                return None
            
            strikes = repair_iv_surface(strikes, spot)
            
            lot, r_f = CONFIG["lot_size"], CONFIG["r_free"]
            for s in strikes:
                s.call_gamma = bs_gamma(spot, s.strike, dte, r_f, s.call_iv / 100.0)
                s.put_gamma  = bs_gamma(spot, s.strike, dte, r_f, s.put_iv / 100.0)
                s.call_delta = bs_delta(spot, s.strike, dte, r_f, s.call_iv / 100.0, "call")
                s.put_delta  = bs_delta(spot, s.strike, dte, r_f, s.put_iv / 100.0, "put")
                
                s.net_gex = ((s.call_oi * s.call_gamma - s.put_oi * s.put_gamma)
                             * lot * spot * spot * 0.01)
            
            profile = GammaProfile(spot=spot, expiry=expiry_date, dte=dte, strikes=strikes)
            self._build_metrics(profile)
            return profile
            
        except Exception as e:
            log.exception(f"Injection Engine Error: {e}")
            return None

    def _build_metrics(self, p: GammaProfile) -> None:
        df = pd.DataFrame([
            {
                "strike": s.strike,
                "call_oi": s.call_oi,
                "put_oi": s.put_oi,
                "net_gex": s.net_gex,
                "call_delta": s.call_delta,
                "put_delta": s.put_delta,
            }
            for s in p.strikes
        ])
        
        tot_call = float(df["call_oi"].sum())
        tot_put = float(df["put_oi"].sum())
        p.pcr = round(tot_put / tot_call, 3) if tot_call > 0 else 0.0
        
        p.net_gex = float(df["net_gex"].sum())
        p.gex_by_strike = {s.strike: s.net_gex for s in p.strikes}
        p.net_delta = float((df["call_oi"] * df["call_delta"] + df["put_oi"] * df["put_delta"]).sum())
        
        self._find_gamma_flips(p)
        
        p.call_wall = float(df.nlargest(1, "call_oi")["strike"].values[0]) if len(df) > 0 else 0.0
        p.put_wall = float(df.nlargest(1, "put_oi")["strike"].values[0]) if len(df) > 0 else 0.0
        p.max_pain = self._max_pain(p.strikes)
        p.gex_clusters = self._find_gex_clusters(p)
        p.vol_skew = self._calculate_vol_skew(p)

    def _find_gamma_flips(self, p: GammaProfile) -> None:
        sorted_s = sorted(p.strikes, key=lambda x: x.strike)
        crossings = []
        cum_gex = 0.0
        prev_cum = 0.0
        prev_strike = None
        
        for s in sorted_s:
            cum_gex += s.net_gex
            
            if prev_strike is not None:
                if (prev_cum < 0 <= cum_gex) or (prev_cum > 0 >= cum_gex):
                    denom = cum_gex - prev_cum
                    if denom != 0:
                        frac = -prev_cum / denom
                        flip = prev_strike + frac * (s.strike - prev_strike)
                        crossings.append(flip)
            
            prev_cum = cum_gex
            prev_strike = s.strike
        
        p.gamma_flip_clusters = crossings
        if crossings:
            p.gamma_flip_level = min(crossings, key=lambda x: abs(x - p.spot))
        else:
            p.gamma_flip_level = p.spot

    @staticmethod
    def _max_pain(strikes: List[OptionStrike]) -> float:
        if not strikes:
            return 0.0
        
        all_k = sorted([s.strike for s in strikes])
        best_strike, best_pain = None, None
        
        for exp_k in all_k:
            pain = 0.0
            for s in strikes:
                if s.strike < exp_k:
                    pain += (exp_k - s.strike) * s.call_oi
                elif s.strike > exp_k:
                    pain += (s.strike - exp_k) * s.put_oi
            
            if best_pain is None or pain < best_pain:
                best_pain = pain
                best_strike = exp_k
        
        return float(best_strike) if best_strike else 0.0

    @staticmethod
    def _find_gex_clusters(p: GammaProfile, threshold: float = 1e9) -> List[Tuple[float, float]]:
        clusters = []
        sorted_s = sorted(p.strikes, key=lambda x: x.strike)
        
        i = 0
        while i < len(sorted_s):
            s = sorted_s[i]
            if abs(s.net_gex) > threshold:
                cluster_strikes = [s.strike]
                total_gex = s.net_gex
                j = i + 1
                
                while j < len(sorted_s) and (sorted_s[j].strike - s.strike) < 100:
                    if abs(sorted_s[j].net_gex) > threshold * 0.5:
                        cluster_strikes.append(sorted_s[j].strike)
                        total_gex += sorted_s[j].net_gex
                    j += 1
                
                if len(cluster_strikes) > 1 or abs(total_gex) > threshold * 1.5:
                    avg_strike = sum(cluster_strikes) / len(cluster_strikes)
                    clusters.append((avg_strike, total_gex))
                
                i = j
            else:
                i += 1
        
        return clusters

    def _calculate_vol_skew(self, p: GammaProfile) -> float:
        if not p.strikes:
            return 0.0
        
        atm_strike = min(p.strikes, key=lambda x: abs(x.strike - p.spot))
        atm_iv = (atm_strike.call_iv + atm_strike.put_iv) / 2
        
        put_25_delta_strikes = [s for s in p.strikes if s.put_delta > -0.30 and s.put_delta < -0.20]
        if put_25_delta_strikes:
            put_25 = min(put_25_delta_strikes, key=lambda x: abs(x.put_delta + 0.25))
            put_25_iv = put_25.put_iv
        else:
            put_25_iv = atm_iv * 1.10
        
        call_25_delta_strikes = [s for s in p.strikes if s.call_delta > 0.20 and s.call_delta < 0.30]
        if call_25_delta_strikes:
            call_25 = min(call_25_delta_strikes, key=lambda x: abs(x.call_delta - 0.25))
            call_25_iv = call_25.call_iv
        else:
            call_25_iv = atm_iv * 0.98
        
        return put_25_iv - call_25_iv

# ──────────────────────────── SIGNAL ENGINE ────────────────────────────
class HardenedSignalEngine:
    def __init__(self):
        self.active_trade: Optional[Dict] = None
        self.position_multiplier: float = 1.0
        self.historical_data: Dict[str, List[float]] = {
            "highs": [],
            "lows": [],
            "closes": [],
            "volumes": [],
        }

    def in_trade(self) -> bool:
        return self.active_trade is not None

    def check_exit(self, current_spot: float) -> Optional[str]:
        if not self.active_trade:
            return None
        
        t = self.active_trade
        if t["direction"] == "CE":
            if current_spot <= t["sl"]:
                return "SL"
            if current_spot >= t["target"]:
                return "TARGET"
        else:
            if current_spot >= t["sl"]:
                return "SL"
            if current_spot <= t["target"]:
                return "TARGET"
        return None

    def close_trade(self, reason: str) -> None:
        if self.active_trade:
            log.info(f"Trade closed ({reason}): {self.active_trade}")
            self.active_trade = None
            self.position_multiplier = 1.0

    def manage_trailing_sl(self, current_spot: float, atr: float) -> Optional[float]:
        if not self.active_trade or atr <= 0:
            return None
        
        t = self.active_trade
        if t["direction"] == "CE":
            if current_spot >= t["entry"] + atr * 0.5:
                new_sl = current_spot - atr * 0.6
                if new_sl > t["sl"]:
                    t["sl"] = round(new_sl, 1)
                    log.info(f"Trailing SL updated to {t['sl']}")
        else:
            if current_spot <= t["entry"] - atr * 0.5:
                new_sl = current_spot + atr * 0.6
                if new_sl < t["sl"]:
                    t["sl"] = round(new_sl, 1)
                    log.info(f"Trailing SL updated to {t['sl']}")
        
        return t["sl"]

    def evaluate_entry(self, p: GammaProfile, prev_spot: float,
                       highs: List[float], lows: List[float],
                       closes: List[float], volumes: List[float]) -> Optional[EntrySignal]:
        if self.in_trade():
            return None
        
        if len(closes) < CONFIG["atr_window"] + 1:
            return None

        atr = true_atr(highs, lows, closes)
        if atr <= 0:
            return None
        
        avg_vol = max(float(np.mean(volumes[-10:])), 1.0)
        vol_surge = volumes[-1] / avg_vol
        if vol_surge < CONFIG["volume_surge_mult"]:
            return None
        
        flip = p.gamma_flip_level
        flip_crossed_up = prev_spot < flip <= p.spot
        flip_crossed_down = prev_spot > flip >= p.spot
        
        if not (flip_crossed_up or flip_crossed_down):
            return None
        
        direction = "CE" if flip_crossed_up else "PE"
        
        nearest_strike = min(p.strikes, key=lambda x: abs(x.strike - flip))
        
        if direction == "CE":
            if nearest_strike.call_oi <= nearest_strike.put_oi * 0.8:
                return None
        else:
            if nearest_strike.put_oi <= nearest_strike.call_oi * 0.8:
                return None
        
        if len(closes) >= CONFIG["ema_long"]:
            ema_short = calculate_ema(closes, CONFIG["ema_short"])
            ema_long = calculate_ema(closes, CONFIG["ema_long"])
            
            if direction == "CE" and ema_short < ema_long:
                return None
            if direction == "PE" and ema_short > ema_long:
                return None
        
        if len(closes) >= CONFIG["rsi_period"] + 1:
            rsi = calculate_rsi(closes)
            if direction == "CE" and rsi > 70:
                return None
            if direction == "PE" and rsi < 30:
                return None
        
        score = self._score_confluence(p, direction, vol_surge, atr, closes)
        position_size = self._calculate_position_size(p, score, atr)
        
        if position_size < 0.5:
            return None
        
        sl_mult = CONFIG["atr_sl_mult"] * (0.8 if position_size < 0.75 else 1.0)
        tgt_mult = CONFIG["atr_target_mult"] * (0.9 if position_size < 0.75 else 1.0)
        
        if direction == "CE":
            sl, target = p.spot - atr * sl_mult, p.spot + atr * tgt_mult
            risk, reward = p.spot - sl, target - p.spot
        else:
            sl, target = p.spot + atr * sl_mult, p.spot - atr * tgt_mult
            risk, reward = sl - p.spot, p.spot - target
        
        rr = round(reward / risk, 2) if risk > 0 else 0.0
        if rr < CONFIG["min_rr"]:
            return None
        
        if score < CONFIG["score_threshold"]:
            return None
        
        confidence = self._determine_confidence(score, position_size, rr)
        
        sig = EntrySignal(
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            symbol=CONFIG["symbol"],
            spot=p.spot,
            direction=direction,
            entry_type="FLIP_REVERSAL",
            entry_price=p.spot,
            initial_sl=round(sl, 1),
            target=round(target, 1),
            rr=rr,
            score=score,
            position_size=position_size,
            confidence=confidence,
        )
        
        self.active_trade = {
            "entry": p.spot,
            "direction": direction,
            "sl": sig.initial_sl,
            "target": sig.target,
            "position_size": position_size,
            "entry_time": sig.timestamp,
        }
        
        self._log_signal(sig)
        log.info(f"📈 SIGNAL GENERATED: {direction} at {p.spot:.2f} | SL: {sig.initial_sl} | TGT: {sig.target} | RR: {rr} | Score: {score}")
        return sig

    @staticmethod
    def _score_confluence(p: GammaProfile, direction: str,
                          vol_surge: float, atr: float,
                          closes: List[float]) -> int:
        score = 50
        
        if direction == "CE" and p.pcr > 1.2:
            score += 12
        elif direction == "CE" and p.pcr > 1.0:
            score += 6
        elif direction == "PE" and p.pcr < 0.8:
            score += 12
        elif direction == "PE" and p.pcr < 1.0:
            score += 6
        
        score += min(int((vol_surge - 1) * 20), 20)
        
        if len(closes) >= 5:
            mom = closes[-1] - closes[-5]
            if direction == "CE" and mom > 0:
                score += 10
            elif direction == "PE" and mom < 0:
                score += 10
        
        if direction == "CE" and abs(p.spot - p.put_wall) < atr:
            score += 10
        elif direction == "PE" and abs(p.spot - p.call_wall) < atr:
            score += 10
        
        return max(0, min(score, 100))

    @staticmethod
    def _calculate_position_size(p: GammaProfile, score: int, atr: float) -> float:
        base = 1.0
        
        if atr / p.spot > 0.02:
            base *= 0.75
        
        if score >= 80:
            base *= 1.25
        elif score < 60:
            base *= 0.5
        
        return max(0.5, min(base, CONFIG["max_position_lots"]))

    @staticmethod
    def _determine_confidence(score: int, position_size: float, rr: float) -> str:
        if score >= 80 and position_size >= 1.0 and rr >= 2.0:
            return "HIGH"
        elif score >= 70 and position_size >= 0.75 and rr >= 1.5:
            return "MEDIUM"
        else:
            return "LOW"

    @staticmethod
    def _log_signal(sig: EntrySignal) -> None:
        try:
            log_file = Path(CONFIG["log_file"])
            log_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(log_file, "a", newline="") as f:
                w = csv.writer(f)
                if f.tell() == 0:
                    w.writerow(["timestamp", "symbol", "direction", "entry_type",
                                "spot", "sl", "target", "rr", "score", 
                                "position_size", "confidence"])
                w.writerow([sig.timestamp, sig.symbol, sig.direction, sig.entry_type,
                            sig.spot, sig.initial_sl, sig.target, sig.rr, sig.score,
                            sig.position_size, sig.confidence])
        except OSError as e:
            log.warning(f"Could not write signal log: {e}")

# ──────────────────────────── MAIN ────────────────────────────
if __name__ == "__main__":
    import argparse
    import json
    import sys
    
    parser = argparse.ArgumentParser(description="Gamma Blast Engine v3.1")
    parser.add_argument("--payload-file", "-f", default="data/default_payload.json",
                       help="Path to JSON file containing live market data")
    args = parser.parse_args()
    
    payload_path = Path(args.payload_file)
    
    if not payload_path.exists():
        sample_payload = {
            "spot_price": 22100,
            "expiry_date": datetime.now().strftime("%Y-%m-%d"),
            "data": [
                {
                    "strike_price": 22000,
                    "call_oi": 150000,
                    "call_iv": 16.5,
                    "call_ltp": 120.50,
                    "call_volume": 2500,
                    "put_oi": 180000,
                    "put_iv": 18.2,
                    "put_ltp": 85.30,
                    "put_volume": 3200
                },
                {
                    "strike_price": 22050,
                    "call_oi": 200000,
                    "call_iv": 17.1,
                    "call_ltp": 100.75,
                    "call_volume": 1800,
                    "put_oi": 160000,
                    "put_iv": 17.8,
                    "put_ltp": 90.50,
                    "put_volume": 2100
                } 
            ]
        },