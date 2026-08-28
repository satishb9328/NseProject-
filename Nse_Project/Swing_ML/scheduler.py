import logging

from apscheduler.schedulers.background import BackgroundScheduler
from django.conf import settings
from django.core.cache import cache
from django.utils import timezone

logger = logging.getLogger(__name__)
SWING_ML_CACHE_KEY = 'swing_ml_signals'
CACHE_TTL_SECONDS = 60 * 5


def get_scheduler():
    scheduler = BackgroundScheduler(
        timezone=settings.TIME_ZONE or 'UTC',
        job_defaults={'coalesce': True, 'max_instances': 1},
    )

    scheduler.add_job(
        func=run_live_scanner,
        trigger='interval',
        minutes=5,
        next_run_time=timezone.now(),
        id='swing_ml_live_scanner',
        replace_existing=True,
        misfire_grace_time=120,
    )

    return scheduler


def run_live_scanner():
    print('[ACTIVE] Swing_ML scheduler job is running')
    try:
        from .live_ml_scanner import main as get_live_signals

        result = get_live_signals()
        if not isinstance(result, dict):
            raise ValueError('Swing_ML scanner must return a dict.')

        cache.set(
            SWING_ML_CACHE_KEY,
            {
                **result,
                'last_updated': timezone.now().isoformat(),
            },
            timeout=CACHE_TTL_SECONDS,
        )

        print(
            '[OK] Swing_ML scan completed:',
            'tier_1=%d' % len(result.get('tier_1', [])),
            'tier_2=%d' % len(result.get('tier_2', [])),
        )
    except Exception as exc:
        logger.exception('Swing_ML scheduler job failed: %s', exc)
        print('[ERROR] Swing_ML scheduler job failed:', exc)
