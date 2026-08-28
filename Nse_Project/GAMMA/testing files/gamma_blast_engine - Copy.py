"""
╔══════════════════════════════════════════════════════════════════════════════╗
║         EXPIRY GAMMA BLAST — PURE QUANT ENGINE v2.5                          ║
║         Target: NIFTY / BANKNIFTY  |  Injected Live TBT Data Mode            ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import math
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
from scipy.stats import norm

CONFIG = {
    "symbol":           "NIFTY",          
    "lot_size":         50,               
    "strike_step":      50,               
    "volume_surge_mult":   1.8,           
    "atr_sl_mult":         1.2,           
    "atr_target_mult":     2.5,           
    "log_file": "gamma_blast_signals.csv",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("GammaBlastEngine")

@dataclass
class OptionStrike:
    strike:       float
    expiry:       str
    call_oi:      int   = 0
    call_iv:      float = 0.0
    call_ltp:     float = 0.0
    call_vol:      int   = 0
    put_oi:       int   = 0
    put_iv:       float = 0.0
    put_ltp:      float = 0.0
    put_vol:      int   = 0
    call_gamma:   float = 0.0
    put_gamma:    float = 0.0
    net_gex:      float = 0.0   

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

@dataclass
class EntrySignal:
    timestamp:    str
    symbol:       str
    spot:         float
    direction:    str    
    entry_type:   str    
    entry_price:  float
    initial_sl:   float
    target:       float
    rr:           float
    score:        int    


def bs_gamma(S: float, K: float, T: float, r: float, sigma: float) -> float:
    if T <= 0 or sigma <= 0: return 0.0
    try:
        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        return norm.pdf(d1) / (S * sigma * math.sqrt(T))
    except Exception: return 0.0


def repair_iv_surface(strikes: List[OptionStrike]) -> List[OptionStrike]:
    valid_strikes = [s.strike for s in strikes if s.call_iv > 0]
    valid_ivs = [s.call_iv for s in strikes if s.call_iv > 0]
    if len(valid_ivs) < 3:
        for s in strikes:
            if s.call_iv == 0: s.call_iv = 15.0
            if s.put_iv == 0: s.put_iv = 15.0
        return strikes
    for s in strikes:
        if s.call_iv == 0: s.call_iv = float(np.interp(s.strike, valid_strikes, valid_ivs))
        if s.put_iv == 0: s.put_iv = float(np.interp(s.strike, valid_strikes, valid_ivs))
    return strikes


class UpstoxDataProcessor:
   
    def __init__(self, symbol: str):
        self.symbol = symbol

    def _calculate_fractional_dte(self) -> float:
        now = datetime.now()
        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
        time_left_mins = (market_close - now).total_seconds() / 60.0
        if time_left_mins <= 0: return 1e-6  
        return time_left_mins / (365.0 * 24.0 * 60.0)

    def inject_and_process(self, external_tbt_dict: dict) -> Optional[GammaProfile]:
        
        try:
            spot = float(external_tbt_dict["spot_price"])
            expiry_date = external_tbt_dict["expiry_date"]
            dte = self._calculate_fractional_dte()
            
            strikes: List[OptionStrike] = []
            for row in external_tbt_dict["data"]:
                K = float(row["strike_price"])
                s = OptionStrike(strike=K, expiry=expiry_date)
                s.call_oi, s.call_iv, s.call_ltp, s.call_vol = int(row.get("call_oi", 0)), float(row.get("call_iv", 0.0)), float(row.get("call_ltp", 0.0)), int(row.get("call_volume", 0))
                s.put_oi, s.put_iv, s.put_ltp, s.put_vol = int(row.get("put_oi", 0)), float(row.get("put_iv", 0.0)), float(row.get("put_ltp", 0.0)), int(row.get("put_volume", 0))
                strikes.append(s)
                
            if not strikes: return None
            strikes = repair_iv_surface(strikes)
            
            lot, r_f = CONFIG["lot_size"], 0.065
            for s in strikes:
                s.call_gamma = bs_gamma(spot, s.strike, dte, r_f, s.call_iv / 100.0)
                s.put_gamma  = bs_gamma(spot, s.strike, dte, r_f, s.put_iv / 100.0)
                s.net_gex    = (s.call_oi * s.call_gamma - s.put_oi * s.put_gamma) * lot * spot
                
            profile = GammaProfile(spot=spot, expiry=expiry_date, dte=dte, strikes=strikes)
            self._build_metrics(profile)
            return profile
        except Exception as e:
            log.error(f"Injection Engine Error: {e}")
            return None

    def _build_metrics(self, p: GammaProfile):
        df = pd.DataFrame([{"strike": s.strike, "call_oi": s.call_oi, "put_oi": s.put_oi, "net_gex": s.net_gex} for s in p.strikes])
        p.pcr = round(df["put_oi"].sum() / max(df["call_oi"].sum(), 1), 3)
        p.net_gex = df["net_gex"].sum()
        p.gex_by_strike = {s.strike: s.net_gex for s in p.strikes}
        
        sorted_strikes = sorted(p.strikes, key=lambda x: x.strike)
        cum_gex = 0.0
        p.gamma_flip_level = p.spot
        for s in sorted_strikes:
            prev = cum_gex
            cum_gex += s.net_gex
            if prev < 0 <= cum_gex or prev >= 0 > cum_gex:
                p.gamma_flip_level = s.strike
                break
        p.call_wall = float(df.nlargest(1, "call_oi")["strike"].values[0])
        p.put_wall  = float(df.nlargest(1, "put_oi")["strike"].values[0])

class HardenedSignalEngine:
    def __init__(self):
        self.active_trade: Optional[Dict] = None

    def manage_trailing_sl(self, current_spot: float, atr: float) -> Optional[float]:
        if not self.active_trade: return None
        trade = self.active_trade
        if trade["direction"] == "CE" and current_spot >= trade["entry"] + atr:
            new_sl = current_spot - (atr * 0.8)
            if new_sl > trade["sl"]: trade["sl"] = round(new_sl, 1)
        elif trade["direction"] == "PE" and current_spot <= trade["entry"] - atr:
            new_sl = current_spot + (atr * 0.8)
            if new_sl < trade["sl"]: trade["sl"] = round(new_sl, 1)
        return trade["sl"]

    def evaluate_entry(self, p: GammaProfile, prev_spot: float, closes: List[float], volumes: List[float]) -> Optional[EntrySignal]:
        if len(closes) < 15: return None
        atr = float(np.mean([h - l for h, l in zip([c+5 for c in closes[-14:]], [c-5 for c in closes[-14:]])]))
        vol_surge = volumes[-1] / max(np.mean(volumes[-10:]), 1)
        
        flip_crossed_up   = prev_spot < p.gamma_flip_level <= p.spot
        flip_crossed_down = prev_spot > p.gamma_flip_level >= p.spot
        
        if (flip_crossed_up or flip_crossed_down) and vol_surge >= CONFIG["volume_surge_mult"]:
            direction = "CE" if flip_crossed_up else "PE"
            sl = p.spot - (atr * CONFIG["atr_sl_mult"]) if direction == "CE" else p.spot + (atr * CONFIG["atr_sl_mult"])
            target = p.spot + (atr * CONFIG["atr_target_mult"]) if direction == "CE" else p.spot - (atr * CONFIG["atr_target_mult"])
            
            sig = EntrySignal(
                timestamp=datetime.now().strftime("%H:%M:%S"), symbol=CONFIG["symbol"], spot=p.spot,
                direction=direction, entry_type="FLIP_REVERSAL", entry_price=p.spot,
                initial_sl=round(sl, 1), target=round(target, 1), rr=round((atr*2.5)/(atr*1.2), 2), score=85
            )
            self.active_trade = {"entry": p.spot, "direction": direction, "sl": sig.initial_sl, "target": sig.target}
            return sig
        return None


if __name__ == "__main__":
    sample_payload = {
        "spot_price": 22100,
        "expiry_date": "2026-05-30",
        "data": [
            {"strike_price": 22100, "call_oi": 1000, "call_iv": 15, "call_ltp": 50, "call_volume": 200, "put_oi": 800, "put_iv": 16, "put_ltp": 45, "put_volume": 180},
            {"strike_price": 22150, "call_oi": 900, "call_iv": 14, "call_ltp": 40, "call_volume": 150, "put_oi": 950, "put_iv": 15, "put_ltp": 42, "put_volume": 160},
            {"strike_price": 22050, "call_oi": 1100, "call_iv": 16, "call_ltp": 52, "call_volume": 220, "put_oi": 700, "put_iv": 15.5, "put_ltp": 47, "put_volume": 170},
        ]
    }

    processor = UpstoxDataProcessor(symbol="NIFTY")
    profile = processor.inject_and_process(sample_payload)
    print("=== Gamma Profile ===")
    print(profile)

    if profile is not None:
        engine = HardenedSignalEngine()
        signal = engine.evaluate_entry(
            profile,
            prev_spot=22080.0,
            closes=[22090.0, 22110.0, 22085.0, 22120.0, 22100.0, 22105.0, 22115.0, 22125.0, 22110.0, 22130.0, 22120.0, 22135.0, 22125.0, 22140.0, 22150.0],
            volumes=[1200, 1100, 1150, 1300, 1250, 1400, 1500, 1550, 1600, 1700, 1650, 1800, 1750, 1900, 2000]
        )
        print("=== Entry Signal ===")
        print(signal)

