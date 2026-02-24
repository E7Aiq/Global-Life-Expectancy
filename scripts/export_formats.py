import pandas as pd
import os

def export_data():
    print("======================================================================")
    print("💾 DATA EXPORT: Generating Alternative Formats (Parquet & JSON)")
    print("======================================================================")

    csv_path = "data/processed/master_life_expectancy.csv"
    parquet_path = "data/processed/master_life_expectancy.parquet"
    json_path = "data/processed/master_life_expectancy.json"

    # التأكد من وجود ملف الـ CSV الأساسي
    if not os.path.exists(csv_path):
        print(f"❌ Error: {csv_path} not found. Please run transform.py first.")
        return

    # تحميل البيانات
    print(f"📂 Loading master dataset from {csv_path}...")
    df = pd.read_csv(csv_path)

    # التصدير إلى Parquet
    print("📦 Exporting to Parquet format (Optimized for Big Data/BI)...")
    try:
        df.to_parquet(parquet_path, engine="pyarrow", index=False)
        print(f"   ✅ Saved: {parquet_path}")
    except ImportError:
        print("   ❌ Error: 'pyarrow' is missing. Please run: pip install pyarrow")
        return

    # التصدير إلى JSON
    print("📦 Exporting to JSON format (Optimized for Web APIs)...")
    df.to_json(json_path, orient="records", indent=2)
    print(f"   ✅ Saved: {json_path}")

    print("======================================================================")
    print("🚀 Export pipeline complete.")
    print("======================================================================")

if __name__ == "__main__":
    export_data()