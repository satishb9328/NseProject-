import pandas as pd
import glob
import os

def check_timeframe():
    folder_path = r"D:\Api data\FyersHistory"
    
    # Parquet ఫైల్స్ వెతకడం
    parquet_files = glob.glob(os.path.join(folder_path, "*.parquet"))
    
    if not parquet_files:
        print("❌ No parquet files found in the folder.")
        return

    # టెస్టింగ్ కోసం మొదటి ఫైల్ ని శాంపిల్ గా తీసుకుంటున్నాం
    sample_file = parquet_files[0]
    file_name = os.path.basename(sample_file)
    print(f"🔍 Analyzing File: {file_name}...\n")
    
    try:
        # డేటాని చదవడం
        df = pd.read_parquet(sample_file)
        
        # టైమ్‌స్టాంప్ కాలమ్ పేరు (సాధారణంగా 'Datetime' ఉంటుంది)
        date_col = 'Datetime' if 'Datetime' in df.columns else df.columns[0]
        df[date_col] = pd.to_datetime(df[date_col])
        
        # డేటాని ఆర్డర్ లో పెట్టడం
        df = df.sort_values(by=date_col)
        
        # వరుస క్యాండిల్స్ మధ్య సమయ వ్యత్యాసాన్ని (Time Difference) కనుక్కోవడం
        time_diffs = df[date_col].diff().dropna()
        
        # మార్కెట్ హాలిడేస్, వీకెండ్స్ వల్ల వచ్చే గ్యాప్స్ ని పక్కనపెట్టి.. 
        # అత్యంత సాధారణంగా రిపీట్ అయిన టైమ్ గ్యాప్ (Mode) ని తీసుకోవడం
        most_common_diff = time_diffs.mode()[0]
        
        # దాన్ని నిమిషాల్లోకి మార్చడం
        minutes = int(most_common_diff.total_seconds() / 60)
        
        print("=" * 50)
        print(f"📊 TIMEFRAME RESULT")
        print("=" * 50)
        print(f"Time Gap between candles: {most_common_diff}")
        
        if minutes == 60:
            print(f"✅ Your data is in: 1-Hour (60 mins) Timeframe")
        elif minutes == 15:
            print(f"✅ Your data is in: 15-Minutes Timeframe")
        elif minutes == 5:
            print(f"✅ Your data is in: 5-Minutes Timeframe")
        elif minutes == 1440:
            print(f"✅ Your data is in: Daily (1-Day) Timeframe")
        else:
            print(f"✅ Your data is in: {minutes}-Minutes Timeframe")
        print("=" * 50)
        
    except Exception as e:
        print(f"❌ Error reading the file: {e}")

if __name__ == "__main__":
    check_timeframe()