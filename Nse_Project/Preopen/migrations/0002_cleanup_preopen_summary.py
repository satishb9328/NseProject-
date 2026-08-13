"""
Migration: 0002_cleanup_preopen_summary
---------------------------------------
1. Removes all time-of-day columns (open_time, high_time, etc.)
2. Removes pre-calculated metric columns (average, high_above_avg, etc.)
3. Removes stored_date helper column
4. Converts open / high / low / close from FloatField → DecimalField(10, 2)
5. Adds unique=True constraint on date
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('Preopen', '0001_initial'),
    ]

    operations = [
        # ── Remove time-of-day columns ────────────────────────────────────────
        migrations.RemoveField(model_name='preopensummary', name='open_time'),
        migrations.RemoveField(model_name='preopensummary', name='high_time'),
        migrations.RemoveField(model_name='preopensummary', name='low_time'),
        migrations.RemoveField(model_name='preopensummary', name='close_time'),

        # ── Remove pre-calculated metric columns ──────────────────────────────
        migrations.RemoveField(model_name='preopensummary', name='average'),
        migrations.RemoveField(model_name='preopensummary', name='high_above_avg'),
        migrations.RemoveField(model_name='preopensummary', name='high_above_avg_time'),
        migrations.RemoveField(model_name='preopensummary', name='low_above_avg'),
        migrations.RemoveField(model_name='preopensummary', name='low_above_avg_time'),
        migrations.RemoveField(model_name='preopensummary', name='avg_above'),
        migrations.RemoveField(model_name='preopensummary', name='high_below_avg'),
        migrations.RemoveField(model_name='preopensummary', name='high_below_avg_time'),
        migrations.RemoveField(model_name='preopensummary', name='low_below_avg'),
        migrations.RemoveField(model_name='preopensummary', name='low_below_avg_time'),
        migrations.RemoveField(model_name='preopensummary', name='avg_below'),

        # ── Remove stored_date helper column ──────────────────────────────────
        migrations.RemoveField(model_name='preopensummary', name='stored_date'),

        # ── Upgrade OHLC: FloatField → DecimalField(10, 2) ───────────────────
        migrations.AlterField(
            model_name='preopensummary',
            name='open',
            field=models.DecimalField(max_digits=10, decimal_places=2),
        ),
        migrations.AlterField(
            model_name='preopensummary',
            name='high',
            field=models.DecimalField(max_digits=10, decimal_places=2),
        ),
        migrations.AlterField(
            model_name='preopensummary',
            name='low',
            field=models.DecimalField(max_digits=10, decimal_places=2),
        ),
        migrations.AlterField(
            model_name='preopensummary',
            name='close',
            field=models.DecimalField(max_digits=10, decimal_places=2),
        ),

        # ── Add unique constraint on date ─────────────────────────────────────
        migrations.AlterField(
            model_name='preopensummary',
            name='date',
            field=models.DateField(unique=True),
        ),
    ]
