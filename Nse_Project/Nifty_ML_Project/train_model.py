# train_model.py
import glob
import os
import pandas as pd
import xgboost as xgb
from sklearn.metrics import accuracy_score, classification_report
import config

def main():
    print("--- STARTING ML MODEL TRAINING ---")
    
    # 1. Load all Parquet files from your D Drive
    parquet_files = glob.glob(os.path.join(config.SAVE_DIRECTORY, "*.parquet"))
    if not parquet_files:
        print(f"❌ No data files found in {config.SAVE_DIRECTORY}. Run build_features.py first!")
        return

    print(f"📂 Found {len(parquet_files)} data file(s). Loading...")
    df = pd.concat((pd.read_parquet(f) for f in parquet_files), ignore_index=True)
    print(f"✅ Loaded {len(df)} total ML-ready candles.")

    # 2. Define Features (X) and Target (y)
    feature_columns = ['candle_buy_vol', 'candle_sell_vol', 'oi_change', 'volume_delta', 
                       'ofi', 'ofi_imbalance', 'vwap_distance', 'prev_delta', 
                       'prev_oi_change', 'prev_ofi_imbalance']
    
    X = df[feature_columns]
    y = df['target']

    # 3. Time-Series Train/Test Split (80% Train, 20% Test)
    # We don't shuffle time-series data! We train on the past, test on the future.
    split_index = int(len(df) * 0.8)
    X_train, X_test = X.iloc[:split_index], X.iloc[split_index:]
    y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]
    
    print(f"🧠 Training Set: {len(X_train)} candles")
    print(f"🧪 Testing Set:  {len(X_test)} candles")

    # 4. Train the XGBoost Model
    print("\n🚀 Training XGBoost Classifier...")
    model = xgb.XGBClassifier(
        n_estimators=200,       # Number of trees
        learning_rate=0.05,     # How fast it learns
        max_depth=5,            # Complexity of trees
        subsample=0.8,          # Prevents overfitting
        colsample_bytree=0.8,   # Prevents overfitting
        objective='binary:logistic',
        eval_metric='logloss'
    )
    
    model.fit(X_train, y_train)

    # 5. Evaluate the Model
    print("\n📊 --- MODEL EVALUATION ---")
    predictions = model.predict(X_test)
    accuracy = accuracy_score(y_test, predictions)
    print(f"Overall Accuracy: {accuracy * 100:.2f}%")
    
    print("\nDetailed Classification Report:")
    print(classification_report(y_test, predictions, target_names=["DOWN/NEUTRAL (0)", "UP (1)"]))

    # 6. Save the Trained Model
    model_filename = "nifty_tbt_model.json"
    model.save_model(model_filename)
    print(f"\n💾 SUCCESS! Trained model saved as '{model_filename}'")
    print("You can now use this model for Live Signal Generation!")
    print("--- TRAINING COMPLETE ---")

if __name__ == "__main__":
    main()