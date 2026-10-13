"""TrendSetup — pre-computed rows for the Master Trend & Setup Scanner.

Populated by the ``update_trend_setups`` management command, which runs the heavy
MTF pipeline (parquet reads + candlestick/chart-pattern detection) once in the
background. The web view only reads this table, so the dashboard renders in
sub-seconds.
"""
from django.db import models
from django.utils import timezone


class TrendSetup(models.Model):
    """One fully-computed swing setup row (mirrors one row of master_trend_setups.csv)."""

    # Identity
    symbol = models.CharField(max_length=50, unique=True)
    name = models.CharField(max_length=255, blank=True, default="")
    industry = models.CharField(max_length=255, blank=True, default="")
    setup_category = models.CharField(max_length=64, db_index=True)

    # Already-coerced display values (raw values; presentation happens in the view).
    close_price = models.FloatField(null=True, blank=True)        # last_close
    position_52w = models.FloatField(null=True, blank=True)       # pct_above_52w_low
    weekly_ema_signal = models.CharField(max_length=32, blank=True, default="")
    momentum = models.FloatField(null=True, blank=True)           # Momentum_Score
    volume_dry_ratio = models.FloatField(null=True, blank=True)   # Volume_Dry_Ratio
    weekly_vol_dry = models.FloatField(null=True, blank=True)     # Weekly_Vol_Ratio
    trend_status = models.BooleanField(default=False)             # uptrend_50_200
    is_hot = models.BooleanField(default=False, db_index=True)

    # MTF signal badges computed by the background pipeline (list of label strings).
    signals = models.JSONField(default=list, blank=True)

    # Live 2H "Sniper" pattern (e.g. "🎯 2H Pullback (Engulfing)") — computed by
    # the background command, never fetched in the request path.
    live_sniper_pattern = models.CharField(max_length=255, null=True, blank=True)

    last_date = models.CharField(max_length=32, blank=True, default="")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["symbol"]
        verbose_name = "trend setup"
        verbose_name_plural = "trend setups"

    def __str__(self):
        return f"{self.symbol} — {self.setup_category}"


class LiveSignal(models.Model):
    stock_symbol = models.CharField(max_length=50)
    signal_type = models.CharField(max_length=64, default="Mega Reversal")
    trigger_price = models.DecimalField(max_digits=12, decimal_places=2)
    weekly_midline = models.DecimalField(max_digits=12, decimal_places=2)
    created_at = models.DateTimeField(auto_now_add=True)
    date = models.DateField(default=timezone.localdate, db_index=True)

    class Meta:
        unique_together = ("stock_symbol", "signal_type", "date")
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.stock_symbol} — {self.signal_type} — {self.date}"
