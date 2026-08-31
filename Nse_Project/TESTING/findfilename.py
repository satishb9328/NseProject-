import os

UPSTOX_DATA_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox_History"

print(f"Checking directory: {UPSTOX_DATA_DIR}\n")

if os.path.exists(UPSTOX_DATA_DIR):
    files = os.listdir(UPSTOX_DATA_DIR)
    parquet_files = [f for f in files if f.endswith('.parquet')]
    
    if parquet_files:
        print(f"Found {len(parquet_files)} parquet files. Here are the first 5:")
        for f in parquet_files[:5]:
            print(f"  -> {f}")
    else:
        print("Directory exists, but NO .parquet files were found inside it!")
        print("Here are the first 5 files in the directory:")
        all_files = [f for f in files if not f.startswith('.')]
        for f in all_files[:5]:
            print(f"  -> {f}")
else:
    print("ERROR: The directory does not exist! Check the path.")