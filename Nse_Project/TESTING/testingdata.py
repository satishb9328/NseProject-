import pandas as pd
import glob
import os

def analyze_parquet_dates():
    # Your folder path
    folder_path = r"D:\Api data\FyersHistory"
    
    # Find all .parquet files in the folder
    file_pattern = os.path.join(folder_path, "*.parquet")
    parquet_files = glob.glob(file_pattern)
    
    if not parquet_files:
        print(f"❌ No Parquet files found in {folder_path}")
        return

    print(f"🔍 Found {len(parquet_files)} Parquet files. Analyzing dates...\n")
    
    global_min_date = None
    global_max_date = None
    
    for file in parquet_files:
        try:
            # Read the parquet file
            df = pd.read_parquet(file)
            
            # Assuming your Fyers data has a 'Datetime' column. 
            # If it's named something else (like 'date'), change it below.
            date_column = 'Datetime' if 'Datetime' in df.columns else df.columns[0]
            
            # Ensure it is in datetime format
            df[date_column] = pd.to_datetime(df[date_column])
            
            # Get max and min dates for this specific file
            file_max = df[date_column].max()
            file_min = df[date_column].min()
            
            # Update global max and min
            if global_max_date is None or file_max > global_max_date:
                global_max_date = file_max
                
            if global_min_date is None or file_min < global_min_date:
                global_min_date = file_min
                
        except Exception as e:
            print(f"⚠️ Could not read {os.path.basename(file)}: {e}")

    print("=" * 50)
    print("📊 DATA RANGE ANALYSIS REPORT")
    print("=" * 50)
    print(f"Data Starts From : {global_min_date}")
    print(f"Data Available Upto: {global_max_date}")
    print("=" * 50)

if __name__ == "__main__":
    analyze_parquet_dates()