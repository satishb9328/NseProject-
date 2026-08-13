from django.contrib import admin
from .models import PreopenSummary


@admin.register(PreopenSummary)
class PreopenSummaryAdmin(admin.ModelAdmin):
    list_display = [
        'date',
        'formatted_open', 'formatted_high', 'formatted_low', 'formatted_close',
        'pre_open_range_display',
    ]
    ordering = ['-date']
    search_fields = ['date']

    def formatted_open(self, obj):
        return f"{obj.open:.2f}" if obj.open is not None else "-"
    formatted_open.short_description = 'Open'
    formatted_open.admin_order_field = 'open'

    def formatted_high(self, obj):
        return f"{obj.high:.2f}" if obj.high is not None else "-"
    formatted_high.short_description = 'High'
    formatted_high.admin_order_field = 'high'

    def formatted_low(self, obj):
        return f"{obj.low:.2f}" if obj.low is not None else "-"
    formatted_low.short_description = 'Low'
    formatted_low.admin_order_field = 'low'

    def formatted_close(self, obj):
        return f"{obj.close:.2f}" if obj.close is not None else "-"
    formatted_close.short_description = 'Close'
    formatted_close.admin_order_field = 'close'

    def pre_open_range_display(self, obj):
        r = obj.pre_open_range
        return f"{r:.2f}" if r is not None else "-"
    pre_open_range_display.short_description = 'Range (H-L)'

