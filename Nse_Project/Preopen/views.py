from django.shortcuts import render
from .models import PreopenSummary

def preopen_price_range(request):
    low, high = PreopenSummary.get_30d_price_range()
    context = {
        'low': low,
        'high': high,
    }
    return render(request, 'option_chain_index_legacy.html', context)


from django.shortcuts import render
from django.http import JsonResponse
from .models import SwingSignal

# 1. డాష్‌బోర్డ్ పేజీని చూపించే ఫంక్షన్
def swing_dashboard_view(request):
    return render(request, 'swing_dashboard.html')

# 2. బ్యాక్‌గ్రౌండ్ లో సిగ్నల్స్ తెచ్చే API ఫంక్షన్
def get_live_signals(request):
    # డేటాబేస్ నుండి లేటెస్ట్ 50 సిగ్నల్స్ తీసుకుంటుంది (కొత్తవి పైన వచ్చేలా)
    signals = SwingSignal.objects.all().order_by('-timestamp')[:50]
    
    data = []
    for s in signals:
        data.append({
            'symbol': s.symbol,
            'signal_type': s.signal_type,
            'change_pct': round(s.change_pct, 2),
            'volume': s.volume,
            'time': s.timestamp.strftime("%H:%M:%S") # టైమ్ ఫార్మాట్
        })
        
    return JsonResponse({'signals': data})
