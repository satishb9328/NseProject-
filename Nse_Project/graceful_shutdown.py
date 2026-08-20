import logging
import os
import signal
import threading

logger = logging.getLogger("graceful_shutdown")
shutdown_event = threading.Event()
_shutdown_requested = False


def _signal_handler(signum, frame) -> None:
    global _shutdown_requested
    signal_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else str(signum)
    if _shutdown_requested:
        logger.warning("Second shutdown signal (%s) received. Exiting immediately.", signal_name)
        os._exit(1)

    _shutdown_requested = True
    logger.info("Shutdown signal received: %s. Requesting graceful shutdown.", signal_name)
    shutdown_event.set()


def setup_signal_handlers() -> None:
    if threading.current_thread() is not threading.main_thread():
        return

    for sig_name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        if hasattr(signal, sig_name):
            try:
                signal.signal(getattr(signal, sig_name), _signal_handler)
            except (ValueError, OSError) as exc:
                logger.debug("Unable to install handler for %s: %s", sig_name, exc)


def is_shutdown_requested() -> bool:
    return shutdown_event.is_set()


def wait_for_shutdown(timeout: float | None = None) -> bool:
    return shutdown_event.wait(timeout)
