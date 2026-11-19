"""Thread-safe metrics, rotating file logger, and atomic health writer (Phase 3)."""

from __future__ import annotations

import io
import json
import logging
import os
import sys
import threading
import time
from collections import defaultdict
from logging.handlers import RotatingFileHandler
from pathlib import Path


class MetricsTracker:
    """Thread-safe counters and gauges for the logger lifecycle."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict = defaultdict(int)
        self._gauges: dict = {}
        self._started_epoch = time.time()

    def increment(self, name: str, n: int = 1) -> None:
        with self._lock:
            self._counters[name] += int(n)

    def set_gauge(self, name: str, value) -> None:
        with self._lock:
            self._gauges[name] = value

    def set_state(self, state: str) -> None:
        self.set_gauge("state", state)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "started_at_epoch": self._started_epoch,
                "uptime_seconds": round(time.time() - self._started_epoch, 3),
                "state": self._gauges.get("state"),
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
            }


def _console_stream():
    """Return a UTF-8, error-tolerant console stream (Windows-safe)."""
    try:
        return io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        return sys.stderr


def setup_logger(name: str, log_dir, level: str = "INFO", console: bool = True) -> logging.Logger:
    """Create a logger with a rotating UTF-8 file handler (10 MB x 10 backups)."""
    log_dir = Path(log_dir)
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(level.upper())
    logger.propagate = False
    if logger.handlers:
        return logger  # already configured (idempotent)

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")

    file_handler = RotatingFileHandler(
        log_dir / "run_logger.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=10,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if console:
        console_handler = logging.StreamHandler(_console_stream())
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


class HealthJsonWriter:
    """Atomically writes a JSON health snapshot (temp file + ``os.replace``)."""

    def __init__(self, path) -> None:
        self.path = Path(path)

    def write(self, data: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)
