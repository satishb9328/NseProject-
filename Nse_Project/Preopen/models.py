from django.db import models
import pytz

# IST timezone — single source of truth for the whole module
IST = pytz.timezone('Asia/Kolkata')


class PreopenSummary(models.Model):
    # ── Core fields ──────────────────────────────────────────────────────────
    date  = models.DateField(unique=True)
    open  = models.DecimalField(max_digits=10, decimal_places=2)
    high  = models.DecimalField(max_digits=10, decimal_places=2)
    low   = models.DecimalField(max_digits=10, decimal_places=2)
    close = models.DecimalField(max_digits=10, decimal_places=2)

    class Meta:
        ordering = ['-date']
        verbose_name = 'Pre-Open Summary'
        verbose_name_plural = 'Pre-Open Summaries'

    def __str__(self):
        return f"PreopenSummary {self.date}"

    # ── Dynamic property (never stored in DB) ────────────────────────────────
    @property
    def pre_open_range(self):
        """Pre-Open Range = High - Low, calculated on the fly."""
        if self.high is not None and self.low is not None:
            return self.high - self.low
        return None

    # ── IST-safe factory classmethod ─────────────────────────────────────────
    @classmethod
    def save_from_ist_timestamp(cls, naive_ist_dt, open_val, high_val, low_val, close_val):
        """
        Create or update a PreopenSummary from a *naive* datetime that
        represents IST time (as returned by NSE's pre-open API).

        Problem this solves
        -------------------
        If you pass `datetime.now()` (naive, local-machine IST) to Django and
        Django's USE_TZ=True is active, it treats the value as UTC, which
        shifts it to 14:30 IST — a 5h30m error.  This method explicitly
        localises the naive datetime to Asia/Kolkata *before* extracting the
        date, so the record always lands on the correct IST calendar day.

        Usage
        -----
            from datetime import datetime
            naive_now = datetime(2026, 5, 3, 9, 0, 0)   # 09:00 AM from NSE
            obj, created = PreopenSummary.save_from_ist_timestamp(
                naive_now, open_val=24500.00, high_val=24520.00,
                low_val=24480.00, close_val=24510.00,
            )
        """
        # Attach IST timezone info without shifting the clock value
        aware_ist = IST.localize(naive_ist_dt)
        record_date = aware_ist.date()   # safe IST calendar date

        obj, created = cls.objects.update_or_create(
            date=record_date,
            defaults={
                'open':  open_val,
                'high':  high_val,
                'low':   low_val,
                'close': close_val,
            },
        )
        return obj, created

    # ── Range helpers ────────────────────────────────────────────────────────
    @classmethod
    def get_30d_price_range(cls, trading_days_only=False):
        """Return (low, high) across all open/high/low/close values for last 30 days.
        If trading_days_only=True, uses last 30 trading days (excluding weekends & official holidays).
        Trading days are limited to dates that actually exist in the DB (skips missing records)."""
        from datetime import date, timedelta as _td  # Only import what is used
        from django.utils import timezone
        end = timezone.now().date()
        # Wider lookback window to gather at least 30 trading days in case of holidays/weekends
        lookback_days = 90 if trading_days_only else 30
        start = end - _td(days=lookback_days)
        # Base queryset over wider window
        base_qs = cls.objects.filter(date__gte=start, date__lte=end)
        if not trading_days_only:
            data_iter = base_qs.values_list('open','high','low','close')
        else:
            # Build holiday set (graceful fallback)
            try:
                try:
                    from NseUtility import NseUtils
                    holiday_list = set(NseUtils().trading_holidays(list_only=True))
                except ImportError:
                    holiday_list = set()
            except Exception:
                holiday_list = set()
            trading_dates = []
            d = end
            # Pre-cache available dates for faster membership checks
            available_dates = set(base_qs.values_list('date', flat=True))
            while d >= start and len(trading_dates) < 30:
                if (d.weekday() < 5) and (d.strftime('%d-%b-%Y') not in holiday_list) and (d in available_dates):
                    trading_dates.append(d)
                d -= _td(days=1)
            if not trading_dates:
                return None, None
            data_iter = cls.objects.filter(date__in=trading_dates).values_list('open','high','low','close')
        vals = []
        for o,h,l,c in data_iter:
            for v in (o,h,l,c):
                if v is not None:
                    try:
                        vals.append(float(v))
                    except (TypeError, ValueError):
                        continue
        if not vals:
            return None, None
        return min(vals), max(vals)

    @classmethod
    def get_price_range(cls, start_date, end_date):
        """Return (low, high) as min of 'low' column and max of 'high' column between start_date and end_date (inclusive)."""
        from django.db.models import Min, Max
        # Query for min of low and max of high in the date range
        result = cls.objects.filter(date__gte=start_date, date__lte=end_date).aggregate(
            min_low=Min('low'),
            max_high=Max('high')
        )
        min_low = result.get('min_low')
        max_high = result.get('max_high')
        if min_low is None or max_high is None:
            return None, None
        return min_low, max_high

class SwingSignal(models.Model):
    symbol = models.CharField(max_length=50)
    signal_type = models.CharField(max_length=20) # 'BULLISH' లేదా 'BEARISH'
    change_pct = models.FloatField()
    volume = models.BigIntegerField()
    timestamp = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.symbol} - {self.signal_type} at {self.timestamp}"