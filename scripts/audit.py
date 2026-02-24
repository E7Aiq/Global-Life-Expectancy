import pandas as pd
import os

# --- Configuration ---
PROCESSED_DIR = "data/processed"
MASTER_FILE = os.path.join(PROCESSED_DIR, "master_life_expectancy.csv")

METRIC_COLS = [
    "life_exp_owid",
    "life_exp_wb",
    "hale_who",
    "life_exp_unicef",
    "life_exp_kaggle",
    "life_exp_us_cdc",
]


def audit_dataframe(df, name):
    """
    Prints a structured data quality report for a given DataFrame.
    Covers: row count, duplicate detection, and per-column missing value %.
    """
    print(f"\n{'=' * 70}")
    print(f"📋 DATA QUALITY AUDIT: {name}")
    print(f"{'=' * 70}")

    # --- Dimensions ---
    print(f"\n   📐 Shape: {df.shape[0]} rows × {df.shape[1]} columns")

    # --- Duplicates ---
    dup_count = df.duplicated().sum()
    dup_pct = dup_count / len(df) * 100 if len(df) > 0 else 0
    status = "✅ None" if dup_count == 0 else f"⚠️  {dup_count} ({dup_pct:.2f}%)"
    print(f"   🔁 Duplicate rows: {status}")

    # --- Missing Values ---
    print(f"\n   {'Column':<30} {'Missing':>10} {'of Total':>10} {'%':>10}")
    print(f"   {'─' * 30} {'─' * 10} {'─' * 10} {'─' * 10}")
    for col in df.columns:
        missing = df[col].isna().sum()
        total = len(df)
        pct = missing / total * 100 if total > 0 else 0
        flag = "" if pct == 0 else " ⚠️" if pct < 50 else " 🔴"
        print(f"   {col:<30} {missing:>10} {total:>10} {pct:>9.1f}%{flag}")


def audit_merge_integrity(df):
    """
    Calculates how many source metrics are present (non-null) per row.
    Shows the distribution: how many rows have 1 source, 2 sources, etc.
    """
    print(f"\n{'=' * 70}")
    print(f"🔗 MERGE INTEGRITY: Source Coverage per Row")
    print(f"{'=' * 70}")

    existing_metrics = [c for c in METRIC_COLS if c in df.columns]
    print(f"\n   Metric columns evaluated ({len(existing_metrics)}):")
    for col in existing_metrics:
        non_null = df[col].notna().sum()
        pct = non_null / len(df) * 100 if len(df) > 0 else 0
        print(f"      {col:<25} {non_null:>7} rows ({pct:.1f}%)")

    # Count non-null metrics per row
    df["_source_count"] = df[existing_metrics].notna().sum(axis=1)

    print(f"\n   {'Sources Present':<20} {'Rows':>10} {'%':>10}")
    print(f"   {'─' * 20} {'─' * 10} {'─' * 10}")

    total = len(df)
    for n_sources in range(0, len(existing_metrics) + 1):
        count = (df["_source_count"] == n_sources).sum()
        pct = count / total * 100 if total > 0 else 0
        if count > 0:
            bar = "█" * int(pct / 2)
            print(f"   {n_sources:<20} {count:>10} {pct:>9.1f}%  {bar}")

    # Summary stats
    avg_sources = df["_source_count"].mean()
    zero_source_rows = (df["_source_count"] == 0).sum()

    print(f"\n   📊 Average sources per row: {avg_sources:.2f} / {len(existing_metrics)}")
    if zero_source_rows > 0:
        print(f"   🔴 Rows with ZERO source data: {zero_source_rows}")
    else:
        print(f"   ✅ Every row has at least 1 source metric.")

    # Clean up temp column
    df.drop(columns=["_source_count"], inplace=True)


def main():
    print("=" * 70)
    print("🔍 ETL PIPELINE AUDIT: Global Life Expectancy & Health Outcomes")
    print("=" * 70)

    if not os.path.exists(MASTER_FILE):
        print(f"\n❌ Master file not found: {MASTER_FILE}")
        print("   Run scripts/transform.py first to generate the merged dataset.")
        return

    df = pd.read_csv(MASTER_FILE)
    print(f"\n💾 Loaded: {MASTER_FILE}")

    # --- Quality Audit ---
    audit_dataframe(df, "master_life_expectancy.csv")

    # --- Merge Integrity ---
    audit_merge_integrity(df)

    # --- Key Field Validation ---
    print(f"\n{'=' * 70}")
    print(f"🔑 KEY FIELD VALIDATION")
    print(f"{'=' * 70}")

    iso3_nulls = df["iso3"].isna().sum()
    year_nulls = df["year"].isna().sum()
    print(f"   iso3 nulls:         {iso3_nulls} {'✅' if iso3_nulls == 0 else '🔴'}")
    print(f"   year nulls:         {year_nulls} {'✅' if year_nulls == 0 else '🔴'}")
    print(f"   Unique countries:   {df['iso3'].nunique()}")
    print(f"   Year range:         {int(df['year'].min())} – {int(df['year'].max())}")

    # Check for duplicate (iso3, year) keys
    dup_keys = df.duplicated(subset=["iso3", "year"]).sum()
    print(f"   Duplicate (iso3, year) pairs: {dup_keys} {'✅' if dup_keys == 0 else '🔴'}")

    print(f"\n{'=' * 70}")
    print("✅ Audit complete.")


if __name__ == "__main__":
    main()