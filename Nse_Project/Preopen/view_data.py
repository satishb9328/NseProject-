"""
Quick utility to view the latest preopen data
"""
import os
import json
from datetime import datetime

def main():
    data_file = os.path.join(os.path.dirname(__file__), 'data', 'preopen_data.json')

    if not os.path.exists(data_file):
        print("❌ No preopen data found!")
        print(f"   Expected file: {data_file}")
        print("\n   Run 'python fetch_preopen.py' to fetch data first.")
        return

    # Get file timestamp
    modified_time = datetime.fromtimestamp(os.path.getmtime(data_file))
    file_size = os.path.getsize(data_file)

    print("="*70)
    print("NSE PREOPEN DATA VIEWER")
    print("="*70)
    print(f"File: {data_file}")
    print(f"Last Updated: {modified_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"File Size: {file_size:,} bytes")
    print("="*70)
    print()

    # Load and parse JSON
    try:
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Show summary
        if isinstance(data, dict):
            print(f"Data Type: Dictionary with {len(data)} keys")
            print(f"Keys: {', '.join(list(data.keys())[:10])}")
            print()

            # Try to show some preopen data if available
            if 'data' in data:
                print(f"Records in 'data': {len(data.get('data', []))}")
                if data['data'] and len(data['data']) > 0:
                    print("\nFirst record sample:")
                    print(json.dumps(data['data'][0], indent=2, ensure_ascii=False))

            # Show metadata if available
            if 'metadata' in data:
                print("\nMetadata:")
                print(json.dumps(data['metadata'], indent=2, ensure_ascii=False))

        elif isinstance(data, list):
            print(f"Data Type: List with {len(data)} items")
            if data and len(data) > 0:
                print("\nFirst item:")
                print(json.dumps(data[0], indent=2, ensure_ascii=False))

        print()
        print("="*70)
        print("To view full JSON, run: python fetch_preopen.py --show")
        print("="*70)

    except json.JSONDecodeError as e:
        print(f"❌ Error parsing JSON: {e}")
        print("\nShowing raw content (first 500 chars):")
        with open(data_file, 'r', encoding='utf-8') as f:
            content = f.read()
            print(content[:500])
            if len(content) > 500:
                print(f"\n... ({len(content) - 500} more characters)")

    except Exception as e:
        print(f"❌ Error reading file: {e}")


if __name__ == '__main__':
    main()

