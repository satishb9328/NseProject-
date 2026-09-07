import pandas as pd
df = pd.read_parquet(r"D:\Api data\ML_HISTO_DATA\Upstox Data\Day Data\NSE_EQ_INE848E01016.parquet")
print(f"Start Date: {df['Timestamp'].min()}")
print(f"End Date: {df['Timestamp'].max()}")
print(f"Total Rows: {len(df)}")