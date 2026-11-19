"""Monotonic-clock ring buffer with dedup, pinning, and invalidation (offline).

Implements the in-memory state the enricher depends on:

* a bounded deque of captured rows keyed by a monotonic clock;
* ``updated_at``-based deduplication plus a monotonic ``seq`` counter;
* ATM-strike / expiry pinning for CE/PE forward lookups;
* gap and expiry-rollover invalidation for matured rows.

No Redis, no I/O, no imports outside this package. The clock is injectable so
tests can supply a deterministic fake.
"""

from __future__ import annotations

import time
from collections import deque
from typing import Callable, Optional, Tuple

MonotonicClock = Callable[[], float]


class RingBuffer:
    """Bounded, monotonic-clock sample buffer used for forward-value lookups."""

    def __init__(
        self,
        horizons: Tuple[int, ...] = (15, 30, 60, 120),
        max_seconds: Optional[float] = None,
        max_gap_seconds: float = 10.0,
        lookup_tolerance: float = 1.5,
        clock: MonotonicClock = time.monotonic,
    ) -> None:
        self.horizons: Tuple[int, ...] = tuple(sorted(int(h) for h in horizons))
        self.max_seconds = float(max_seconds if max_seconds is not None else max(self.horizons))
        self.max_gap_seconds = float(max_gap_seconds)
        self.lookup_tolerance = float(lookup_tolerance)
        self.clock = clock

        self._samples: deque = deque()  # each: {"mono": float, "row": dict}
        self._gaps: deque = deque()     # resume-mono times of detected gaps
        self._last_updated_at: Optional[str] = None
        self._last_mono: Optional[float] = None
        self._seq = 0

    # -- sequencing & dedup --------------------------------------------------
    def next_seq(self) -> int:
        """Return the next monotonic per-process capture sequence number."""
        self._seq += 1
        return self._seq

    @property
    def seq(self) -> int:
        return self._seq

    def is_duplicate(self, updated_at) -> bool:
        """True when this snapshot's ``updated_at`` matches the last captured one.

        ``updated_at`` is the feed's snapshot timestamp; the 1-second publish
        throttle plus the 5-second quiet-loop can re-serve the same value, so
        dedup is on ``updated_at`` alone. ``seq`` is per-process and therefore
        disambiguates only ordering, not cross-process repeats.
        """
        if updated_at is None or self._last_updated_at is None:
            return False
        return str(updated_at) == self._last_updated_at

    # -- ingestion -----------------------------------------------------------
    def add(self, row: dict, mono: Optional[float] = None, chain: Optional[dict] = None) -> float:
        """Record a captured row; returns the monotonic timestamp used.

        Detects and records a gap when the elapsed monotonic time since the
        previous sample exceeds ``max_gap_seconds``.
        """
        mono = float(self.clock()) if mono is None else float(mono)
        if self._last_mono is not None and (mono - self._last_mono) > self.max_gap_seconds:
            self._gaps.append(mono)
        self._samples.append({"mono": mono, "row": row, "chain": chain or {}})
        self._last_mono = mono
        updated_at = row.get("updated_at")
        if updated_at is not None:
            self._last_updated_at = str(updated_at)
        self._prune(mono)
        return mono

    def _prune(self, now_mono: float) -> None:
        cutoff = now_mono - self.max_seconds
        while self._samples and self._samples[0]["mono"] < cutoff:
            self._samples.popleft()
        while self._gaps and self._gaps[0] < cutoff:
            self._gaps.popleft()

    # -- introspection -------------------------------------------------------
    @property
    def oldest_mono(self) -> Optional[float]:
        return self._samples[0]["mono"] if self._samples else None

    @property
    def newest_mono(self) -> Optional[float]:
        return self._samples[-1]["mono"] if self._samples else None

    def __len__(self) -> int:
        return len(self._samples)

    def __bool__(self) -> bool:
        return bool(self._samples)

    # -- gap query -----------------------------------------------------------
    def has_gap(self, start_mono: float, end_mono: float) -> bool:
        """True if a feed gap was detected strictly inside (start, end]."""
        for gap_mono in self._gaps:
            if start_mono < gap_mono <= end_mono:
                return True
        return False


    # -- forward lookups -----------------------------------------------------
    def _sample_near(self, target_mono: float) -> Optional[dict]:
        best = None
        best_dist = None
        for sample in self._samples:
            dist = abs(sample["mono"] - target_mono)
            if dist <= self.lookup_tolerance and (best_dist is None or dist < best_dist):
                best = sample
                best_dist = dist
        return best

    def forward_spot(self, capture_mono: float, horizon: float) -> Tuple[Optional[float], Optional[str]]:
        """Return ``(spot_at_t+h, reason)``; reason is ``None`` or ``"no_sample"``."""
        sample = self._sample_near(capture_mono + horizon)
        if sample is None:
            return None, "no_sample"
        return sample["row"].get("spot"), None

    def forward_option(
        self,
        capture_mono: float,
        horizon: float,
        side: str,
        pinned_strike: Optional[float],
        pinned_expiry: Optional[str],
    ) -> Tuple[Optional[float], Optional[str]]:
        """Return the pinned-strike CE/PE LTP at ``capture_mono + horizon``.

        Resolves the pinned strike from the future snapshot's full chain
        (``sample["chain"]``), so a live ATM shift (e.g. 24800 -> 24900) still
        yields the 24800 leg's LTP instead of failing. ``side`` is ``"ce"`` or
        ``"pe"``. Reason is one of ``None``, ``"no_sample"``,
        ``"strike_unavailable"``, ``"expiry_rollover"``.
        """
        if pinned_strike is None:
            return None, "strike_unavailable"
        sample = self._sample_near(capture_mono + horizon)
        if sample is None:
            return None, "no_sample"
        row = sample["row"]
        if pinned_expiry is not None and row.get("active_expiry") not in (None, pinned_expiry):
            return None, "expiry_rollover"
        chain = sample.get("chain") or {}
        strike_data = chain.get(pinned_strike)
        if not strike_data:
            return None, "strike_unavailable"
        return strike_data.get(side), None

    # -- reset ---------------------------------------------------------------
    def clear(self) -> None:
        """Drop all buffered state and counters (e.g. session/expiry reset)."""
        self._samples.clear()
        self._gaps.clear()
        self._last_updated_at = None
        self._last_mono = None
        self._seq = 0

