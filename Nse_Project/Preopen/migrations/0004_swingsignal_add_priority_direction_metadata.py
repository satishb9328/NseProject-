# Generated migration for SwingSignal model changes
#
# This migration adds 3 new fields to the SwingSignal table:
#   - direction (CharField, default '')
#   - priority (FloatField, default 0.0)
#   - metadata (JSONField, default {})
#
# Also updates signal_type max_length from 20 → 50 to accommodate longer
# signal names like 'GEX_FLIP_WARNING' and 'CALL_OI_BUILDUP_RESISTANCE'.
#
# After deploying this file, run:
#   python manage.py migrate Preopen
#
# This will apply the schema changes to your SQLite database.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('Preopen', '0003_swingsignal_alter_preopensummary_options'),
    ]

    operations = [
        migrations.AlterField(
            model_name='swingsignal',
            name='signal_type',
            field=models.CharField(max_length=50),
        ),
        migrations.AddField(
            model_name='swingsignal',
            name='direction',
            field=models.CharField(default='', max_length=50),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='swingsignal',
            name='priority',
            field=models.FloatField(default=0.0),
            preserve_default=True,
        ),
        migrations.AddField(
            model_name='swingsignal',
            name='metadata',
            field=models.JSONField(blank=True, default=dict),
            preserve_default=True,
        ),
        migrations.AlterModelOptions(
            name='swingsignal',
            options={'ordering': ['-timestamp'], 'verbose_name': 'Swing Signal', 'verbose_name_plural': 'Swing Signals'},
        ),
    ]
