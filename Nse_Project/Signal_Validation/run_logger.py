"""run_logger.py — process entrypoint for the Signal Validation logger (Phase 3).

Wires ``collector`` + ``extractor`` + ``state`` + ``enricher`` + ``writer``
into a single long-lived process.

Lifecycle: STARTING -> WARMING_UP -> CAPTURING -> RECONNECTING -> DRAINING -> STOPPED.

* single-instance lockfile (stale-PID recovery)
* graceful SIGINT/SIGTERM/SIGBREAK drain & flush
* startup + daily retention sweep (writer.sweep_retention)
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import threading
import time
from collections import deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from logger import __version__, load_logger_config, resolve_storage_paths  # noqa: E402
from logger.collector import Collector  # noqa: E402
from logger.enricher import Enricher  # noqa: E402
from logger.extractor import Extractor  # noqa: E402
from logger.health import HealthJsonWriter, MetricsTracker, setup_logger  # noqa: E402
from logger.state import RingBuffer  # noqa: E402
from logger.writer import JsonlWriter  # noqa: E402

_STATES = ("STARTING", "WARMING_UP", "CAPTURING", "RECONNECTING", "DRAINING", "STOPPED")


class SingleInstanceLock:
    """OS-level exclusive lock, auto-released when the process exits.

    Uses ``msvcrt.locking`` on Windows and ``fcntl.flock`` on POSIX over an open
    handle, so a crash or kill can never leave a stale lock behind. The PID is
    written to the file for operator visibility only.
    """

    def __init__(self, path) -> None:
        self.path = Path(path)
        self._fh = None

    def acquire(self) -> bool:
        if self._fh is not None:
            return False  # already held by this object
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._fh = open(self.path, "a+b")
        except OSError:
            return False
        try:
            self._fh.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self._fh.close()
            self._fh = None
            return False
        self._fh.seek(0)
        self._fh.truncate(0)
        self._fh.write(str(os.getpid()).encode("ascii"))
        self._fh.flush()
        return True

    def release(self) -> None:
        if self._fh is not None:
            try:
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            self._fh.close()
            self._fh = None
        try:
            self.path.unlink()
        except OSError:
            pass


class LoggerApp:
    """Owns the pipeline and the lifecycle state machine."""

    def __init__(self, config: dict, once: bool = False) -> None:
        self.config = config
        self.once = bool(once)
        self.state = "STARTING"
        self.stop_event = threading.Event()
        self.metrics = MetricsTracker()
        self._pending: deque = deque()
        self._ever_enriched = False

    # -- setup --------------------------------------------------------------
    def setup(self) -> None:
        cfg = self.config
        paths = resolve_storage_paths(cfg)
        self.logs_dir = paths["logs"]
        self.data_dir = paths["raw"].parent  # storage root
        health_cfg = cfg.get("health", {})

        self.logger = setup_logger("sigval", self.logs_dir, level=cfg["logging"].get("level", "INFO"))
        self.health_path = self.logs_dir / health_cfg.get("health_file", "health.json")
        self.heartbeat = float(health_cfg.get("heartbeat_interval_seconds", 5.0))

        quality = cfg.get("quality", {})
        writer_cfg = cfg.get("writer", {})
        enrichment = cfg.get("enrichment", {})
        redis_cfg = cfg.get("redis", {})

        self.extractor = Extractor(
            min_spot=quality.get("min_spot", 0.0),
            drop_if_spot_missing=quality.get("drop_if_spot_missing", True),
            atm_selection=quality.get("atm_selection", "nearest_strike"),
        )
        self.buffer = RingBuffer(
            horizons=tuple(cfg["horizons"]),
            max_seconds=enrichment.get("ring_buffer_max_seconds"),
            max_gap_seconds=enrichment.get("max_gap_seconds", 10.0),
        )
        self.enricher = Enricher(
            horizons=tuple(cfg["horizons"]),
            warmup_seconds=enrichment.get("warmup_seconds", 120.0),
        )
        self.writer = JsonlWriter(
            data_dir=cfg["storage"].get("data_dir"),
            raw_filename_pattern=writer_cfg.get("raw_filename_pattern", "signal_{date}.jsonl"),
            processed_filename_pattern=writer_cfg.get("processed_filename_pattern", "signal_{date}.enriched.jsonl"),
            encoding=writer_cfg.get("encoding", "utf-8"),
            flush_interval_seconds=writer_cfg.get("flush_interval_seconds", 5.0),
            flush_max_rows=writer_cfg.get("flush_max_rows", 100),
            retention_days=cfg["storage"].get("retention_days", 90),
            truncate_trailing_partial_line_on_start=writer_cfg.get("truncate_trailing_partial_line_on_start", True),
        )
        self.collector = Collector(
            url=redis_cfg.get("url", "redis://127.0.0.1:6379/0"),
            channel=redis_cfg.get("channel", "gamma:live_options_updated"),
            data_key=redis_cfg.get("data_key", "live_options_data"),
            market_state_key_prefix=redis_cfg.get("market_state_key_prefix", "market_state"),
        )
        self.health_writer = HealthJsonWriter(self.health_path)
        self.logger.info("Signal Validation logger v%s initialized (data_dir=%s)", __version__, self.data_dir)


    # -- lifecycle helpers ----------------------------------------------------
    def _set_state(self, state: str) -> None:
        if state not in _STATES:
            raise ValueError(f"unknown state: {state}")
        if state != self.state:
            self.state = state
            self.metrics.set_state(state)
            self.logger.info("state -> %s", state)
            self._write_health()

    def _write_health(self) -> None:
        payload = self.metrics.snapshot()
        payload.update({
            "version": __version__,
            "data_dir": str(self.data_dir),
            "health_file": str(self.health_path),
        })
        try:
            self.health_writer.write(payload)
        except OSError as exc:
            self.logger.warning("Failed to write health.json: %s", exc)

    # -- per-publish processing ----------------------------------------------
    def handle_update(self, snapshot: dict, market_state: dict) -> None:
        updated_at = snapshot.get("updated_at")
        if self.buffer.is_duplicate(updated_at):
            self.metrics.increment("duplicates")
            return

        seq = self.buffer.next_seq()
        row = self.extractor.extract(snapshot, market_state, seq)
        if row is None:
            self.metrics.increment("dropped")
            return

        chain = self.extractor.build_chain(snapshot)
        capture_mono = self.buffer.add(row, chain=chain)
        self.writer.write_raw(row)
        self.metrics.increment("captured")
        self.metrics.set_gauge("last_spot", row["spot"])
        self.metrics.set_gauge("last_snapshot_ts", row["ts"])

        self._pending.append((capture_mono, row))
        self._mature_pending()

        if self._ever_enriched:
            self._set_state("CAPTURING")
        else:
            self._set_state("WARMING_UP")

        if self.once:
            self.stop_event.set()

    def _mature_pending(self) -> None:
        newest = self.buffer.newest_mono
        if newest is None:
            return
        max_horizon = max(self.buffer.horizons)
        while self._pending and self._pending[0][0] + max_horizon <= newest:
            capture_mono, row = self._pending.popleft()
            enriched = self.enricher.enrich(row, capture_mono, self.buffer)
            self.writer.write_processed(enriched)
            self.metrics.increment("enriched" if enriched["complete"] else "incomplete")
            self._ever_enriched = True
            self._set_state("CAPTURING")

    def on_status(self, status: str) -> None:
        if status == "connected":
            self._set_state("CAPTURING" if self._ever_enriched else "WARMING_UP")
        elif status == "reconnecting":
            self.metrics.increment("reconnects")
            self._set_state("RECONNECTING")

    # -- run / drain ---------------------------------------------------------
    def run(self) -> int:
        self.setup()
        self.writer.sweep_retention()  # startup retention sweep
        self._set_state("WARMING_UP")

        listener = threading.Thread(
            target=self.collector.run_listener,
            args=(self.handle_update, self.stop_event, self.on_status),
            name="redis-listener",
            daemon=True,
        )
        listener.start()

        try:
            while not self.stop_event.wait(timeout=self.heartbeat):
                self._write_health()
        finally:
            self._drain()
            listener.join(timeout=5.0)
        self._set_state("STOPPED")
        self._write_health()
        return 0

    def _drain(self) -> None:
        self._set_state("DRAINING")
        try:
            self.writer.flush()
        finally:
            self.writer.close()
        try:
            self.collector.close()
        except Exception:
            pass


# -- signal handling ---------------------------------------------------------
def _install_signal_handlers(app: LoggerApp) -> None:
    def handler(signum, frame) -> None:
        app.stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, handler)
        except (ValueError, OSError):
            pass
    if hasattr(signal, "SIGBREAK"):
        try:
            signal.signal(signal.SIGBREAK, handler)
        except (ValueError, OSError):
            pass


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Signal Validation logger (read-only)")
    parser.add_argument("--config", default=None, help="path to logger.yaml (default: bundled)")
    parser.add_argument("--once", action="store_true", help="exit after the first captured snapshot (smoke test)")
    args = parser.parse_args(argv)

    config = load_logger_config(args.config)
    paths = resolve_storage_paths(config)
    lock = SingleInstanceLock(paths["logs"] / "run_logger.lock")

    if not lock.acquire():
        print("ERROR: another run_logger instance is already running.", file=sys.stderr)
        return 1

    try:
        app = LoggerApp(config, once=args.once)
        _install_signal_handlers(app)
        return app.run()
    finally:
        lock.release()


if __name__ == "__main__":
    raise SystemExit(main())

