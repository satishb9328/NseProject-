import asyncio
import os
import threading

from django.apps import AppConfig
from django.conf import settings


class SwingMlConfig(AppConfig):
    default_auto_field = 'django.db.models.AutoField'
    name = 'Nse_Project.Swing_ML'
    verbose_name = 'Swing ML'

    def ready(self):
        # Only start in the child process created by Django autoreload.
        if settings.DEBUG and os.environ.get('RUN_MAIN') != 'true':
            return

        if getattr(self, 'scheduler', None) is not None:
            return

        from .scheduler import get_scheduler

        scheduler = get_scheduler()
        scheduler.start()
        self.scheduler = scheduler
        print('[ACTIVE] Swing_ML scheduler started')

        self._start_sepa_rest_collector()

    def _start_sepa_rest_collector(self):
        if getattr(self, 'collector_thread', None) is not None:
            return

        try:
            from .ml_stockdata_collector import main as collector_main
        except Exception as exc:
            print(f'[ERROR] Failed to import SEPA REST snapshot collector: {exc}')
            return

        def run_collector() -> None:
            try:
                asyncio.run(collector_main())
            except Exception as exc:
                print(f'[ERROR] SEPA REST snapshot collector failed: {exc}')

        thread = threading.Thread(target=run_collector, name='SEPARestSnapshotCollector', daemon=True)
        thread.start()
        self.collector_thread = thread
        print('[ACTIVE] Upstox REST snapshot collector started for SEPA stocks (5-minute interval)')
