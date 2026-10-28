import os
import sys
import threading
import time

from django.apps import AppConfig
from django.conf import settings


class GammaConfig(AppConfig):
    default_auto_field = 'django.db.models.AutoField'
    name = 'Nse_Project.GAMMA'
    verbose_name = 'Gamma Blast Engine'

    def ready(self):        
        # Only start in the actual server process — not in management commands
        # like `manage.py shell`, `migrate`, etc. This guard prevents the bot
        # from starting during unrelated CLI invocations.
        #
        # Detection is multi-layered so it works for ALL server types:
        #   - `python manage.py runserver` → 'runserver' in sys.argv
        #   - `daphne Nse_Project.asgi:application` → 'daphne' in sys.argv
        #   - `uvicorn Nse_Project.asgi:application` → 'uvicorn' in sys.argv[0]
        #   - `gunicorn Nse_Project.wsgi:application` → 'gunicorn' in sys.argv[0]
        argv_str = ' '.join(sys.argv).lower()
        is_server_cmd = any(
            cmd in argv_str
            for cmd in ('runserver', 'daphne', 'uvicorn', 'gunicorn')
        )
        # Also check the process basename (uvicorn/daphne run as executables)
        proc_name = os.path.basename(sys.argv[0]).lower() if sys.argv else ''
        is_asgi_worker = proc_name in ('uvicorn', 'daphne', 'gunicorn')
        if not is_server_cmd and not is_asgi_worker:
            return

        # Startup process detection — handles BOTH modes correctly:
        #
        #   1. Default `runserver` (autoreload ON):
        #        Django spawns a parent (reloader) and a child (RUN_MAIN=true).
        #        We want to start the bot ONLY in the child to avoid duplicates.
        #
        #   2. `runserver --noreload` / `uvicorn` / `daphne` / `gunicorn`:
        #        Single process, RUN_MAIN is NEVER set. The old code would
        #        silently return here and the bot would never start. When
        #        `--noreload` is absent AND we're not in an ASGI worker, we
        #        keep the RUN_MAIN='true' check for runserver autoreload.
        is_noreload = '--noreload' in sys.argv
        is_asgi_proc = proc_name in ('uvicorn', 'daphne', 'gunicorn') or 'uvicorn' in argv_str or 'daphne' in argv_str
        if settings.DEBUG and not is_noreload and not is_asgi_proc and os.environ.get('RUN_MAIN') != 'true':
            return

        # Idempotency: don't start a second listener if one is already running.
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