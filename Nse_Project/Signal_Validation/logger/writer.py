"""Thread-safe append-only JSONL writer (offline, no Redis).

Writes normalized raw and enriched rows to day-scoped files under the storage
tree. Features: batched flush (5s / 100 rows), IST date rotation, startup
partial-line truncation, and a 90-day retention sweep. Missing directories are
created automatically via ``os.makedirs(..., exist_ok=True)``.
"""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, TextIO

from . import IST

DEFAULT_DATA_DIR = "D:/Signal_Validation_Data"
DEFAULT_RAW_SUBDIR = "raw"
DEFAULT_PROCESSED_SUBDIR = "processed"


def _json_safe(obj):
    """Recursively convert non-finite floats to None (JSON cannot hold NaN/Inf)."""
    if isinstance(obj, float):
        if obj != obj or obj in (float("inf"), float("-inf")):
            return None
        return obj
    if isinstance(obj, dict):
        return {key: _json_safe(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(value) for value in obj]
    return obj


class JsonlWriter:
    """Append-only JSONL writer for the ``raw`` and ``processed`` streams."""

    def __init__(
        self,
        data_dir: Optional[str] = None,
        raw_dir: Optional[str] = None,
        processed_dir: Optional[str] = None,
        *,
        raw_filename_pattern: str = "signal_{date}.jsonl",
        processed_filename_pattern: str = "signal_{date}.enriched.jsonl",
        encoding: str = "utf-8",
        flush_interval_seconds: float = 5.0,
        flush_max_rows: int = 100,
        retention_days: int = 90,
        truncate_trailing_partial_line_on_start: bool = True,
        clock=time.monotonic,
        wall_clock=time.time,
    ) -> None:
        if data_dir is None:
            data_dir = os.environ.get("SIGVAL_DATA_DIR") or DEFAULT_DATA_DIR
        self.raw_dir = Path(raw_dir) if raw_dir else Path(data_dir) / DEFAULT_RAW_SUBDIR
        self.processed_dir = Path(processed_dir) if processed_dir else Path(data_dir) / DEFAULT_PROCESSED_SUBDIR
        self.raw_pattern = raw_filename_pattern
        self.proc_pattern = processed_filename_pattern
        self.encoding = encoding
        self.flush_interval_seconds = float(flush_interval_seconds)
        self.flush_max_rows = int(flush_max_rows)
        self.retention_days = int(retention_days)
        self.truncate_partial = bool(truncate_trailing_partial_line_on_start)
        self.clock = clock
        self.wall_clock = wall_clock

        self._lock = threading.RLock()
        self._raw_buf: list = []
        self._proc_buf: list = []
        self._raw_handle: Optional[TextIO] = None
        self._proc_handle: Optional[TextIO] = None
        self._raw_date: Optional[str] = None
        self._proc_date: Optional[str] = None
        self._last_flush_mono = self.clock()
        self._closed = False

    # -- public --------------------------------------------------------------
    def write_raw(self, row: dict) -> None:
        if not isinstance(row, dict):
            raise TypeError("write_raw expects a dict row")
        with self._lock:
            self._check_open()
            date = row.get("session_date") or self._default_date()
            self._ensure_stream("raw", date)
            self._raw_buf.append(self._encode(row))
            self._maybe_flush()

    def write_processed(self, row: dict) -> None:
        if not isinstance(row, dict):
            raise TypeError("write_processed expects a dict row")
        with self._lock:
            self._check_open()
            date = row.get("session_date") or self._default_date()
            self._ensure_stream("proc", date)
            self._proc_buf.append(self._encode(row))
            self._maybe_flush()

    def flush(self) -> None:
        with self._lock:
            self._flush()

    def sweep_retention(self, now_epoch: Optional[float] = None) -> None:
        with self._lock:
            self._sweep(now_epoch)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._flush()
            self._close_handle("raw")
            self._close_handle("proc")
            self._closed = True

    def __enter__(self) -> "JsonlWriter":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- internals -----------------------------------------------------------
    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("JsonlWriter is closed")

    @staticmethod
    def _default_date() -> str:
        return datetime.now(IST).date().isoformat()

    @staticmethod
    def _encode(row: dict) -> str:
        return json.dumps(_json_safe(row), ensure_ascii=False, separators=(",", ":"))

    def _ensure_stream(self, which: str, date: str) -> None:
        if which == "raw":
            handle, current = self._raw_handle, self._raw_date
        else:
            handle, current = self._proc_handle, self._proc_date
        if date == current and handle is not None:
            return
        if handle is not None:
            try:
                handle.close()
            except OSError:
                pass
        directory = self.raw_dir if which == "raw" else self.processed_dir
        pattern = self.raw_pattern if which == "raw" else self.proc_pattern
        new_handle = self._open(directory / pattern.format(date=date))
        if which == "raw":
            self._raw_handle, self._raw_date = new_handle, date
        else:
            self._proc_handle, self._proc_date = new_handle, date
        if date != current:
            self._sweep()


    def _open(self, path: Path) -> TextIO:
        os.makedirs(path.parent, exist_ok=True)
        if self.truncate_partial and path.exists() and path.stat().st_size > 0:
            self._truncate_trailing_partial(path)
        return open(path, "a", encoding=self.encoding, newline="\n")

    def _truncate_trailing_partial(self, path: Path) -> None:
        """Remove a trailing partial line left by a previous crash."""
        try:
            with open(path, "r+b") as fh:
                fh.seek(0, os.SEEK_END)
                size = fh.tell()
                if size == 0:
                    return
                fh.seek(size - 1)
                if fh.read(1) == b"\n":
                    return  # already ends cleanly
                pos = size - 1
                found = None
                while pos >= 0:
                    fh.seek(pos)
                    if fh.read(1) == b"\n":
                        found = pos
                        break
                    pos -= 1
                fh.truncate(found + 1 if found is not None else 0)
        except OSError:
            pass

    def _maybe_flush(self) -> None:
        now = self.clock()
        total = len(self._raw_buf) + len(self._proc_buf)
        if total >= self.flush_max_rows or (now - self._last_flush_mono) >= self.flush_interval_seconds:
            self._flush()

    def _flush(self) -> None:
        if self._raw_buf:
            if self._raw_handle is not None:
                self._raw_handle.write("\n".join(self._raw_buf) + "\n")
                self._raw_handle.flush()
            self._raw_buf.clear()
        if self._proc_buf:
            if self._proc_handle is not None:
                self._proc_handle.write("\n".join(self._proc_buf) + "\n")
                self._proc_handle.flush()
            self._proc_buf.clear()
        self._last_flush_mono = self.clock()

    def _close_handle(self, which: str) -> None:
        handle = self._raw_handle if which == "raw" else self._proc_handle
        if handle is not None:
            try:
                handle.close()
            except OSError:
                pass
        if which == "raw":
            self._raw_handle = None
        else:
            self._proc_handle = None

    def _sweep(self, now_epoch: Optional[float] = None) -> None:
        now = self.wall_clock() if now_epoch is None else now_epoch
        cutoff = now - self.retention_days * 86400
        for directory in (self.raw_dir, self.processed_dir):
            if not directory.exists():
                continue
            for path in directory.glob("signal_*.jsonl"):
                try:
                    if path.stat().st_mtime < cutoff:
                        path.unlink()
                except OSError:
                    continue

