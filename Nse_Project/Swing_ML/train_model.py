import os
import glob
import pandas as pd
import xgboost as xgb
import joblib
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import warnings
warnings.filterwarnings("ignore")

# =====================================================================
# 1. CONFIGURATION
# =====================================================================
INPUT_DIR = r"D:\Api data\ML_HISTO_DATA\ML_Master_Data"
MODEL_DIR = r"D:\Api data\ML_HISTO_DATA\Model_Weights"

os.makedirs(MODEL_DIR, exist_ok=True)

def main():
    print("🚀 Starting XGBoost Model Training...")
    print("=" * 60)
    
    # 1. Load All Data (అన్ని స్టాక్స్ డేటాని ఒకేచోట కలపడం)
    parquet_files = glob.glob(os.path.join(INPUT_DIR, "*_master.parquet"))
    if not parquet_files:
        print("❌ No master data files found!")
        return
        
    print(f"📥 Loading {len(parquet_files)} stock files. Please wait...")
    
    df_list = []
    for file in parquet_files:
        df = pd.read_parquet(file)
        df_list.append(df)
        
    # అన్నింటినీ ఒకే పెద్ద DataFrame గా మారుస్తున్నాం
    master_df = pd.concat(df_list, ignore_index=True)
    print(f"✅ Total Data Loaded: {len(master_df)} Rows")

    # 2. ఫీచర్స్ (Features) మరియు టార్గెట్ (Target) ని విడదీయడం
    # అనవసరమైన కాలమ్స్ (ప్రైస్, టైమ్ లాంటివి మోడల్ కి అనవసరం, కేవలం ఇండికేటర్స్ మాత్రమే కావాలి)
    drop_columns = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI', 'Next_3H_High', 'Target_Hit']
    
    # X = మన టెక్నికల్ ఇండికేటర్స్ (సమస్యలు)
    X = master_df.drop(columns=[col for col in drop_columns if col in master_df.columns])
    
    # y = భవిష్యత్తులో టార్గెట్ హిట్ అయిందా లేదా (జవాబులు)
    y = master_df['Target_Hit']

    print(f"📊 Features selected for training: {list(X.columns)}")
    
    # 3. Train-Test Split (80% ట్రైనింగ్ కి, 20% ఎగ్జామ్ కి)
    print("\n✂️ Splitting data into Training (80%) and Testing (20%)...")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=42)

    # 4. XGBoost మోడల్ సెట్టింగ్స్
    print("🧠 Training XGBoost AI Model. This might take 1-2 minutes...")
    model = xgb.XGBClassifier(
        n_estimators=200,      # 200 డెసిషన్ ట్రీస్
        max_depth=5,           # మరీ ఎక్కువగా ఆలోచించకుండా (Overfitting నివారణ)
        learning_rate=0.05,    # మెల్లగా నేర్చుకునేలా
        random_state=42,
        n_jobs=-1              # సిస్టమ్ లోని అన్ని కోర్స్ ని వాడమని కమాండ్
    )

    # మోడల్ చదవడం మొదలుపెడుతుంది
    model.fit(X_train, y_train)
    print("✅ Training Complete!")

    # 5. Model Evaluation (టెస్టింగ్)
    print("\n📈 Evaluating Model on 20% Unseen Data...")
    y_pred = model.predict(X_test)
    
    accuracy = accuracy_score(y_test, y_pred)
    print(f"🎯 Model Accuracy: {accuracy * 100:.2f}%\n")
    print("📋 Classification Report:")
    print(classification_report(y_test, y_pred))

    # ఏ ఇండికేటర్ బాగా పనిచేసిందో (Feature Importance) చూపించడం
    importance = model.feature_importances_
    feature_names = X.columns
    importance_df = pd.DataFrame({'Feature': feature_names, 'Importance': importance})
    importance_df = importance_df.sort_values(by='Importance', ascending=False).head(5)
    
    print("\n⭐ Top 5 Most Important Features for this Model:")
    for idx, row in importance_df.iterrows():
        print(f"   -> {row['Feature']}: {row['Importance']*100:.1f}%")

    # 6. మోడల్ ని సేవ్ చేయడం
    model_path = os.path.join(MODEL_DIR, "xgboost_swing_model.pkl")
    joblib.dump(model, model_path)
    print("=" * 60)
    print(f"🎉 AI Model Successfully Saved at: {model_path}")
    print("👉 Now we are ready for LIVE TRADING SIGNALS!")

if __name__ == "__main__":
    main()