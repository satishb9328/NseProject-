import os
import threading
import time

from django.apps import AppConfig
from django.conf import settings


class GammaConfig(AppConfig):
    default_auto_field = 'django.db.models.AutoField'
    name = 'Nse_Project.GAMMA'
    verbose_name = 'Gamma Blast Engine'

    def ready(self):
        # Only start in the child process created by Django autoreload,
        # mirroring the ml_stockdata_collector startup in Swing_ML/apps.py.
        if settings.DEBUG and os.environ.get('RUN_MAIN') != 'true':
            return

        if getattr(self, 'gamma_thread', None) is not None:
            return

        from .gamma_blast_engine import GammaBotError, get_gamma_bot

        def run_gamma_bot() -> None:
            bot = get_gamma_bot()
            for attempt in range(1, 6):
                try:
                    if bot.is_running:
                        print('[ACTIVE] GAMMA Redis listener already running')
                        return
                    bot.start()
                    print('[ACTIVE] GAMMA Redis listener started (daemon thread)')
                    return
                except GammaBotError as exc:
                    if attempt == 5:
                        print(f'[ERROR] GAMMA engine failed to start after 5 attempts: {exc}')
                        return
                    time.sleep(2)
                except Exception as exc:
                    print(f'[ERROR] GAMMA engine failed: {exc}')
                    return

        thread = threading.Thread(target=run_gamma_bot, name='GammaRedisListener', daemon=True)
        thread.start()
        self.gamma_thread = thread
        print('[ACTIVE] GAMMA engine start scheduled')