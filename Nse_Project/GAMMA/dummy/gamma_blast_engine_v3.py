from typing import List, Tuple
from collections import defaultdict
import numpy as np
from dataclasses import dataclass

# 1. Define the missing SwingStartSignal data structure
@dataclass
class SwingStartSignal:
    is_setup_forming: bool
    setup_score: int
    phase: str
    confidence: float
    entry_trigger: str
    reasons: List[str]

# 2. Declare the class properly
class SwingSetupDetector:
    """
    KEY CONCEPT: Swing doesn't start with breakout. It starts with ACCUMULATION → COMPRESSION → EXPANSION pattern.
    """

    def __init__(self):
        self.min_bars = 20

    def detect(self, bars: List) -> SwingStartSignal:
        """Main detection method."""
        if len(bars) < self.min_bars:
            return SwingStartSignal(False, 0, "UNKNOWN", 0.0, "Insufficient data", [])
        
        reasons = []
        score = 0
        
        # LAYER 1: Order Flow Imbalance (Buy/Sell Pressure)
        buy_pressure = self._detect_buy_pressure(bars)
        if buy_pressure > 0.6:
            score += 2
            reasons.append(f"Buy pressure detected: {buy_pressure:.2f}")
        
        # LAYER 2: Support Zone Identification (Institutional Support)
        support_zone, zone_strength = self._identify_support_zone(bars)
        if zone_strength > 0.5:
            score += 2
            reasons.append(f"Support zone at {support_zone:.2f} (strength: {zone_strength:.2f})")
        
        # LAYER 3: Volatility Regime Change (Compression Detection)
        vol_regime, vol_change = self._detect_volatility_compression(bars)
        if vol_regime == "COMPRESSED":
            score += 2
            reasons.append(f"Volatility compressed {vol_change:.1f}%")
        
        # LAYER 4: Volume Profile Mode (Smart Money Accumulation)
        vol_mode, accumulation_score = self._analyze_volume_profile_mode(bars)
        if vol_mode == "ACCUMULATION":
            score += 2
            reasons.append(f"Volume mode: {vol_mode} (score: {accumulation_score:.2f})")
        
        # LAYER 5: Higher Lows Pattern (Demand Building)
        has_higher_lows, low_count = self._detect_higher_lows_pattern(bars)
        if has_higher_lows:
            score += 1
            reasons.append(f"Higher lows forming ({low_count} bars)")
        
        # LAYER 6: Range Contraction (Price Compression)
        is_contracting, range_pct = self._detect_range_contraction(bars)
        if is_contracting:
            score += 1
            reasons.append(f"Range contracted {range_pct:.2f}%")
        
        # Determine phase based on score
        phase = self._determine_phase(score, bars)
        
        # Determine entry trigger
        entry_trigger = self._get_entry_trigger(phase, bars)
        
        # Calculate confidence
        confidence = min(score / 10.0, 1.0)
        
        is_setup = score >= 4
        
        return SwingStartSignal(
            is_setup_forming=is_setup,
            setup_score=min(score, 10),
            phase=phase,
            confidence=confidence,
            entry_trigger=entry_trigger,
            reasons=reasons
        )

    # ========== LAYER 1: Order Flow Imbalance ==========
    def _detect_buy_pressure(self, bars: List) -> float:
        """
        Detect persistent buying pressure (Order Flow Imbalance).
        
        Uses: Close location in range (CLR), up/down bar count
        High buy pressure = price consistently closing in upper half of bars
        """
        if len(bars) < 5:
            return 0.0
        recent = bars[-10:]
        up_bars = 0
        clr_sum = 0.0  # Close Location in Range
        
        for bar in recent:
            range_size = bar.h - bar.l
            if range_size > 0:
                clr = (bar.c - bar.l) / range_size  # 0=bottom, 1=top
                clr_sum += clr
                if bar.c > bar.o:  # Green bar
                    up_bars += 1
        
        avg_clr = clr_sum / len(recent)
        up_ratio = up_bars / len(recent)
        
        # High buy pressure if:
        # - Average CLR > 0.6 (consistently closing near top)
        # - AND more than 50% up bars
        buy_pressure = (avg_clr * 0.6 + up_ratio * 0.4)
        return buy_pressure

    # ========== LAYER 2: Support Zone Identification ==========
    def _identify_support_zone(self, bars: List) -> Tuple[float, float]:
        """
        Identify support zones using PRICE CLUSTER ANALYSIS.
        
        Support = price level where volume accumulated + retested multiple times
        Uses: Low prices that recur within 1-2% band
        """
        if len(bars) < 15:
            return 0.0, 0.0
        
        lows = [bar.l for bar in bars[-30:]]
        vols = [bar.v for bar in bars[-30:]]
        
        # Cluster lows into zones (1.5% tolerance)
        zones = defaultdict(list)
        for i, low in enumerate(lows):
            # Find which zone this low belongs to
            placed = False
            for zone_mid in list(zones.keys()):
                if abs(low - zone_mid) / zone_mid < 0.015:  # 1.5% tolerance
                    zones[zone_mid].append((low, vols[i]))
                    placed = True
                    break
            if not placed:
                zones[low].append((low, vols[i]))
        
        # Find strongest support zone (most retests + high volume)
        best_zone = 0.0
        best_strength = 0.0
        
        for zone_mid, touches in zones.items():
            retest_count = len(touches)
            avg_vol = np.mean([v for _, v in touches])
            
            # Strength = retests + volume
            strength = (retest_count / 5.0) * 0.6 + (avg_vol / np.mean(vols)) * 0.4
            
            if strength > best_strength:
                best_strength = strength
                best_zone = zone_mid
        
        return best_zone, min(best_strength, 1.0)

    # ========== LAYER 3: Volatility Regime Change ==========
    def _detect_volatility_compression(self, bars: List) -> Tuple[str, float]:
        """
        Detect Bollinger Band SQUEEZE (volatility compression).
        
        Uses: Range contraction + ATR decline
        Squeeze = calm before the storm (precedes 70% of moves)
        """
        if len(bars) < 20:
            return "UNKNOWN", 0.0
        
        recent = bars[-20:]
        ranges = [bar.h - bar.l for bar in recent]
        
        avg_range_old = np.mean(ranges[:10])
        avg_range_new = np.mean(ranges[10:])
        
        compression_pct = (avg_range_old - avg_range_new) / avg_range_old * 100
        
        if compression_pct > 20:  # >20% compression = squeeze
            return "COMPRESSED", compression_pct
        elif compression_pct < -10:
            return "EXPANDING", abs(compression_pct)
        else:
            return "STABLE", abs(compression_pct)

    # ========== LAYER 4: Volume Profile Mode ==========
    def _analyze_volume_profile_mode(self, bars: List) -> Tuple[str, float]:
        """
        Detect ACCUMULATION vs DISTRIBUTION using Volume Profile.
        
        Accumulation = flat price, high volume (smart money buying)
        Distribution = rising price, declining volume (profit taking)
        """
        if len(bars) < 10:
            return "UNKNOWN", 0.0
        
        recent = bars[-15:]
        
        # Calculate price range and volume trend
        price_range = (recent[-1].h - recent[0].l) / recent[0].l * 100
        vols = [b.v for b in recent]
        vol_trend = np.polyfit(range(len(vols)), vols, 1)[0]  # Slope
        
        # Accumulation logic:
        # - Small price range (< 3%)
        # - Volume increasing or high
        # = Money flowing in at similar prices
        
        # FIX: Syntax error '3and' changed to '3 and'
        if price_range < 3 and vol_trend > 0:
            return "ACCUMULATION", 0.8
        elif price_range < 3 and vol_trend < 0:
            return "ACCUMULATION", 0.5
        elif price_range > 5 and vol_trend < 0:
            return "DISTRIBUTION", 0.7
        else:
            return "NEUTRAL", 0.3

    # ========== LAYER 5: Higher Lows Pattern ==========
    def _detect_higher_lows_pattern(self, bars: List) -> Tuple[bool, int]:
        """
        Detect HIGHER LOWS formation (demand building).
        
        Pattern: Each low is higher than prior low. Meaning: Buyers stepping in at progressively higher prices
        """
        if len(bars) < 5:
            return False, 0
        
        recent_lows = [bar.l for bar in bars[-5:]]
        
        higher_lows = True
        for i in range(1, len(recent_lows)):
            if recent_lows[i] < recent_lows[i-1] * 0.998:  # Allow 0.2% noise
                higher_lows = False
                break
        
        return higher_lows, len(recent_lows) if higher_lows else 0

    # ========== LAYER 6: Range Contraction ==========
    def _detect_range_contraction(self, bars: List) -> Tuple[bool, float]:
        """
        Detect PRICE RANGE CONTRACTION (consolidation).
        
        Range = High - Low per bar
        Contraction = average range shrinking
        """
        if len(bars) < 15:
            return False, 0.0
        
        ranges = [bar.h - bar.l for bar in bars[-15:]]
        
        old_avg = np.mean(ranges[:7])
        new_avg = np.mean(ranges[7:])
        
        if old_avg > 0:
            contraction_pct = (old_avg - new_avg) / old_avg * 100
            if contraction_pct > 15:  # >15% contraction
                return True, contraction_pct
        
        return False, 0.0

    # ========== Phase Determination ==========
    def _determine_phase(self, score: int, bars: List) -> str:
        """Determine market phase based on signal score."""
        current_close = bars[-1].c
        prior_close = bars[-2].c if len(bars) > 1 else current_close
        
        if score >= 8:
            if current_close > prior_close:
                return "EXPANSION"
            else:
                return "COMPRESSION"
        elif score >= 5:
            return "ACCUMULATION"
        elif score >= 3:
            return "SETUP_FORMING"
        else:
            return "NO_SETUP"

    def _get_entry_trigger(self, phase: str, bars: List) -> str:
        """Get what to watch for as entry trigger."""
        triggers = {
            "EXPANSION": "Wait for close above prior day high on volume",
            "ACCUMULATION": "Watch for breakout above consolidation range",
            "COMPRESSION": "Wait for ATR expansion + volume surge",
            "SETUP_FORMING": "Monitor for support formation + volume profile shift",
            "NO_SETUP": "Wait for price compression + higher lows pattern"
        }
        return triggers.get(phase, "Watch for pattern continuation")