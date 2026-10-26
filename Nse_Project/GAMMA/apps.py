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
        # from starting during unrelated CLI invocations (which would try to
        # connect to Redis and slow down every management command).
        if not any(
            cmd in sys.argv
            for cmd in ('runserver', 'daphne', 'uvicorn')
        ):
            return

        # Startup process detection — handles BOTH modes correctly:
        #
        #   1. Default `runserver` (autoreload ON):
        #        Django spawns a parent (reloader) and a child (RUN_MAIN=true).
        #        We want to start the bot ONLY in the child to avoid duplicates.
        #        The `--noreload` flag is NOT in sys.argv, so we keep the
        #        RUN_MAIN='true' check and skip the parent.
        #
        #   2. `runserver --noreload` (single process):
        #        Django runs ONE process and NEVER sets RUN_MAIN. The old code
        #        would silently return here and the bot would never start —
        #        which is exactly the bug we're fixing. When `--noreload` is
        #        present, we drop the RUN_MAIN requirement and start the bot
        #        in this single process.
        #
        #   3. Production (DEBUG=False): the entire `if settings.DEBUG` block
        #        is skipped, so the bot starts unconditionally in the server
        #        process (Daphne/Uvicorn/gunicorn). Production servers don't
        #        use autoreload, so RUN_MAIN is never set there either.
        is_noreload = '--noreload' in sys.argv
        if settings.DEBUG and not is_noreload and os.environ.get('RUN_MAIN') != 'true':
            return

        # Idempotency: don't start a second listener if one is already running
        # (e.g. ready() firing twice in the same process).
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