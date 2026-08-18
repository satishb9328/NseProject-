import os

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
        print('🟢 Swing_ML scheduler started')
