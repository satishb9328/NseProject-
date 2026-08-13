import os
import django
import sys
from datetime import date, timedelta

# Setup Django environment
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'myproject.settings')
django.setup()

from Preopen.models import PreopenSummary

# Calculate date range: today to 20 days ago
end_date = date.today()
start_date = end_date - timedelta(days=29)

qs = PreopenSummary.objects.filter(date__range=(start_date, end_date))

if not qs.exists():
    print(f"No records found from {start_date} to {end_date}.")
    sys.exit(0)

fields = ['open', 'high', 'low', 'close']

all_values = []
value_sources = []  # To track (value, field, date)
for obj in qs:
    for field in fields:
        val = getattr(obj, field, None)
        if val is not None:
            all_values.append(val)
            value_sources.append((val, field, obj.date))

if not all_values:
    print("No data found in any of the columns (open, high, low, close) in the last 20 days.")
    sys.exit(0)

min_val = min(all_values)
max_val = max(all_values)

min_entries = [(field, date) for v, field, date in value_sources if v == min_val]
max_entries = [(field, date) for v, field, date in value_sources if v == max_val]

print(f"Lowest value among all columns: {min_val}")
for field, date in min_entries:
    print(f"  Found in column '{field.upper()}' on {date}")

print(f"\nHighest value among all columns: {max_val}")
for field, date in max_entries:
    print(f"  Found in column '{field.upper()}' on {date}")
