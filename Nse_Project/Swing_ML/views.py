from django.core.cache import cache
from django.shortcuts import render

SWING_ML_CACHE_KEY = 'swing_ml_signals'


def swing_dashboard_view(request):
    cache_data = cache.get(SWING_ML_CACHE_KEY)
    if not cache_data:
        context = {
            'tier_1_trades': [],
            'tier_2_trades': [],
            'scanner_warming_up': True,
            'last_updated': None,
        }
    else:
        context = {
            'tier_1_trades': cache_data.get('tier_1', []),
            'tier_2_trades': cache_data.get('tier_2', []),
            'scanner_warming_up': False,
            'last_updated': cache_data.get('last_updated'),
        }

    # ఈ డాటాని 'context' అనే డిక్షనరీ ద్వారా HTML కి పంపుతాం
    
    return render(request, 'dashboard.html', context)