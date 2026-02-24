import pandas as pd
import os
from functools import reduce

# --- Configuration ---
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
OUTPUT_FILE = os.path.join(PROCESSED_DIR, "master_life_expectancy.csv")

MERGE_KEYS = ["iso3", "year"]

# --- Universal Country Name Corrections ---
# Handles: UN formal names (WHO), Kaggle typos, OWID naming conventions.
# Applied to both WHO and Kaggle before ISO3 mapping.
UNIVERSAL_CORRECTIONS = {
    "United States of America": "United States",
    "United Kingdom of Great Britain and Northern Ireland": "United Kingdom",
    "Russian Federation": "Russia",
    "Syrian Arab Republic": "Syria",
    "Türkiye": "Turkey",
    "United Republic of Tanzania": "Tanzania",
    "Venezuela (Bolivarian Republic of)": "Venezuela",
    "Venezuella (Bolivarian Republic of)": "Venezuela",
    "Viet Nam": "Vietnam",
    "Timor-Leste": "East Timor",
    "Republic of Moldova": "Moldova",
    "Micronesia": "Micronesia (country)",
    "Micronesia (Federated States of)": "Micronesia (country)",
    "Micronesia (Federatedd States of)": "Micronesia (country)",
    "Bolivia (Plurinational State of)": "Bolivia",
    "Iran (Islamic Republic of)": "Iran",
    "Democratic People's Republic of Korea": "North Korea",
    "Republic of Korea": "South Korea",
    "Lao People's Democratic Republic": "Laos",
    "Côte d'Ivoire": "Cote d'Ivoire",
    "Czechia": "Czech Republic",
    "Democratic Republic of the Congo": "Democratic Republic of Congo",
    "Cabo Verde": "Cape Verde",
    "Brunei Darussalam": "Brunei",
    "Swaziland": "Eswatini",
    "The former Yugoslav republic of Macedonia": "North Macedonia",
}


# =============================================================================
# STEP 1: Build ISO3 Mapping Dictionary from OWID (the cleanest source)
# =============================================================================
def build_iso3_mapping():
    """
    Builds a country_name -> iso3 lookup dictionary from OWID data.
    OWID has the broadest, cleanest coverage of (Entity, Code) pairs.
    Used to map Kaggle and WHO datasets that lack ISO3 codes.
    """
    filepath = os.path.join(RAW_DIR, "owid_historical_life_expectancy.csv")
    df = pd.read_csv(filepath, usecols=["Entity", "Code"])
    df = df.dropna(subset=["Code"])
    # Filter out OWID-specific aggregate codes (e.g., OWID_WRL, OWID_USS)
    df = df[~df["Code"].str.startswith("OWID_")]
    # Deduplicate: one canonical iso3 per entity name
    mapping = df.drop_duplicates(subset=["Entity"]).set_index("Entity")["Code"].to_dict()
    print(f"   🗺️  ISO3 mapping built: {len(mapping)} unique country names mapped.")
    return mapping


# =============================================================================
# STEP 2: Individual Dataset Cleaning Functions
# =============================================================================
def clean_owid():
    """
    OWID Historical Life Expectancy.
    Columns: Entity, Code, Year, Life expectancy
    Action: Rename, drop rows with null iso3, filter out OWID aggregates.
    """
    print("\n📦 Cleaning: owid_historical_life_expectancy.csv")
    filepath = os.path.join(RAW_DIR, "owid_historical_life_expectancy.csv")
    df = pd.read_csv(filepath)

    df = df.rename(columns={
        "Entity": "country_name",
        "Code": "iso3",
        "Year": "year",
        "Life expectancy": "life_exp_owid"
    })

    rows_before = len(df)
    df = df.dropna(subset=["iso3"])
    df = df[~df["iso3"].str.startswith("OWID_")]
    print(f"   Rows: {rows_before} → {len(df)} (dropped {rows_before - len(df)} rows with null/aggregate iso3)")

    # Keep only merge keys + metric (country_name used later for final merge enrichment)
    df = df[["iso3", "year", "country_name", "life_exp_owid"]]
    print(f"   ✅ Shape: {df.shape}")
    return df


def clean_worldbank():
    """
    World Bank Life Expectancy (extracted via API).
    Columns: iso3, country_name, year, life_exp_wb
    Action: Already clean from extract_wb.py. Minimal processing.
    """
    print("\n📦 Cleaning: worldbank_life_expectancy.csv")
    filepath = os.path.join(RAW_DIR, "worldbank_life_expectancy.csv")
    df = pd.read_csv(filepath)

    df = df.rename(columns={"life_exp_wb": "life_exp_wb"})  # explicit no-op for clarity

    rows_before = len(df)
    df = df.dropna(subset=["iso3"])
    print(f"   Rows: {rows_before} → {len(df)} (dropped {rows_before - len(df)} rows with null iso3)")

    # Drop country_name here — will use OWID's as canonical in final merge
    df = df[["iso3", "year", "life_exp_wb"]]
    print(f"   ✅ Shape: {df.shape}")
    return df


def clean_kaggle(iso3_mapping):
    """
    Kaggle Health Factors dataset.
    Has 'Country' (name) and 'Year' but NO iso3 code.
    Action: Map Country -> iso3 via OWID mapping dictionary.
    """
    print("\n📦 Cleaning: kaggle_health_factors.csv")
    filepath = os.path.join(RAW_DIR, "kaggle_health_factors.csv")

    # Try multiple encodings — Kaggle files can vary
    for encoding in ["utf-8", "utf-8-sig", "latin-1"]:
        try:
            df = pd.read_csv(filepath, encoding=encoding)
            break
        except UnicodeDecodeError:
            continue

    # Identify the life expectancy column (strip trailing whitespace from column names)
    df.columns = df.columns.str.strip()

    # --- Apply universal name corrections ---
    corrected_count = df["Country"].isin(UNIVERSAL_CORRECTIONS.keys()).sum()
    df["Country"] = df["Country"].replace(UNIVERSAL_CORRECTIONS)
    print(f"   🔧 Universal name corrections applied: {corrected_count} rows across {len(UNIVERSAL_CORRECTIONS)} rules")

    # Map Country to iso3
    df["iso3"] = df["Country"].map(iso3_mapping)

    unmapped = df[df["iso3"].isna()]["Country"].nunique()
    if unmapped > 0:
        unmapped_names = df[df["iso3"].isna()]["Country"].unique()[:10]
        print(f"   ⚠️  {unmapped} countries could not be mapped to iso3. Samples: {list(unmapped_names)}")

    rows_before = len(df)
    df = df.dropna(subset=["iso3"])
    print(f"   Rows: {rows_before} → {len(df)} (dropped {rows_before - len(df)} unmapped rows)")

    df = df.rename(columns={
        "Year": "year",
        "Life expectancy": "life_exp_kaggle"
    })

    df = df[["iso3", "year", "life_exp_kaggle"]]
    print(f"   ✅ Shape: {df.shape}")
    return df


def clean_unicef():
    """
    UNICEF Life Expectancy.
    Key columns: REF_AREA (iso3), TIME_PERIOD (year), OBS_VALUE (metric), SEX.
    Action: Filter for SEX == '_T' (Total) to avoid gender-level duplicates.
    """
    print("\n📦 Cleaning: unicef_life_expectancy.csv")
    filepath = os.path.join(RAW_DIR, "unicef_life_expectancy.csv")

    for encoding in ["utf-8", "utf-8-sig", "latin-1"]:
        try:
            df = pd.read_csv(filepath, encoding=encoding)
            break
        except UnicodeDecodeError:
            continue

    rows_before = len(df)

    # Filter for total (both sexes) only
    if "SEX" in df.columns:
        df = df[df["SEX"] == "_T"]
        print(f"   Filtered SEX == '_T': {rows_before} → {len(df)} rows")

    df = df.rename(columns={
        "REF_AREA": "iso3",
        "TIME_PERIOD": "year",
        "OBS_VALUE": "life_exp_unicef"
    })

    df["life_exp_unicef"] = pd.to_numeric(df["life_exp_unicef"], errors="coerce")
    df = df.dropna(subset=["iso3", "life_exp_unicef"])

    df = df[["iso3", "year", "life_exp_unicef"]]

    # Deduplicate — UNICEF may have multiple source entries per (iso3, year)
    df = df.groupby(["iso3", "year"], as_index=False)["life_exp_unicef"].mean()

    print(f"   ✅ Shape: {df.shape}")
    return df


def clean_who(iso3_mapping):
    """
    WHO Healthy Life Expectancy (HALE).
    The CSV has proper headers. Key columns:
      - DIM_GEO_CODE_TYPE: filter for 'COUNTRY' (exclude WORLDBANKINCOMEGROUP, WHOREGION)
      - DIM_SEX: filter for 'TOTAL' (Both Sexes)
      - GEO_NAME_SHORT: country name
      - DIM_TIME: year
      - AMOUNT_N: HALE value
    Action: Filter, rename, map country_name -> iso3 via OWID mapping.
    """
    print("\n📦 Cleaning: who_healthy_life_expectancy.csv")
    filepath = os.path.join(RAW_DIR, "who_healthy_life_expectancy.csv")

    for encoding in ["utf-8", "utf-8-sig", "latin-1"]:
        try:
            df = pd.read_csv(filepath, encoding=encoding)
            break
        except UnicodeDecodeError:
            continue

    print(f"   Raw shape: {df.shape}")
    print(f"   Columns: {list(df.columns)}")

    # --- Filter: COUNTRY-level only ---
    rows_before = len(df)
    if "DIM_GEO_CODE_TYPE" in df.columns:
        df = df[df["DIM_GEO_CODE_TYPE"] == "COUNTRY"]
        print(f"   Filtered DIM_GEO_CODE_TYPE == 'COUNTRY': {rows_before} → {len(df)} rows")
    else:
        print(f"   ⚠️  Column 'DIM_GEO_CODE_TYPE' not found. Skipping geo filter.")

    # --- Filter: Both Sexes only ---
    rows_before = len(df)
    if "DIM_SEX" in df.columns:
        sex_values = df["DIM_SEX"].unique()
        print(f"   Available sex dimensions: {sex_values}")
        if "TOTAL" in sex_values:
            df = df[df["DIM_SEX"] == "TOTAL"]
            print(f"   Filtered DIM_SEX == 'TOTAL': {rows_before} → {len(df)} rows")
        elif "BTSX" in sex_values:
            df = df[df["DIM_SEX"] == "BTSX"]
            print(f"   Filtered DIM_SEX == 'BTSX': {rows_before} → {len(df)} rows")
        elif "BOTHSEXES" in sex_values:
            df = df[df["DIM_SEX"] == "BOTHSEXES"]
            print(f"   Filtered DIM_SEX == 'BOTHSEXES': {rows_before} → {len(df)} rows")
        else:
            print(f"   ⚠️  No combined-sex indicator found. Averaging across sex dimensions.")
    else:
        print(f"   ⚠️  Column 'DIM_SEX' not found. Skipping sex filter.")

    # --- Rename ---
    df = df.rename(columns={
        "GEO_NAME_SHORT": "country_name",
        "DIM_TIME": "year",
        "AMOUNT_N": "hale_who"
    })

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["hale_who"] = pd.to_numeric(df["hale_who"], errors="coerce")

    # --- Apply universal name corrections before ISO3 mapping ---
    corrected_count = df["country_name"].isin(UNIVERSAL_CORRECTIONS.keys()).sum()
    df["country_name"] = df["country_name"].replace(UNIVERSAL_CORRECTIONS)
    print(f"   🔧 Universal name corrections applied: {corrected_count} WHO country names corrected")

    # --- Map country_name -> iso3 ---
    df["iso3"] = df["country_name"].map(iso3_mapping)

    unmapped = df[df["iso3"].isna()]["country_name"].nunique()
    if unmapped > 0:
        unmapped_names = df[df["iso3"].isna()]["country_name"].unique()[:10]
        print(f"   ⚠️  {unmapped} WHO country names could not be mapped. Samples: {list(unmapped_names)}")

    df = df.dropna(subset=["iso3", "hale_who"])

    df = df[["iso3", "year", "hale_who"]]
    df = df.drop_duplicates(subset=["iso3", "year"])

    print(f"   ✅ Shape: {df.shape}")
    return df


def clean_cdc():
    """
    CDC US Demographics (Excel).
    Complex file — US-only data with no standard headers.
    Action: Read with skiprows, extract year + life expectancy at birth
    for all races / both sexes. Hardcode iso3='USA'.
    """
    print("\n📦 Cleaning: cdc_us_demographics.xlsx")
    filepath = os.path.join(RAW_DIR, "cdc_us_demographics.xlsx")

    if not os.path.exists(filepath):
        print("   ⚠️  File not found. Skipping CDC dataset.")
        return pd.DataFrame(columns=["iso3", "year", "life_exp_us_cdc"])

    try:
        # Read raw to inspect structure first
        df_raw = pd.read_excel(filepath, engine="openpyxl", header=None, nrows=5)
        print(f"   Raw preview (first 5 rows):\n{df_raw.to_string()}")

        # Read full file — skip metadata rows at top
        df = pd.read_excel(filepath, engine="openpyxl", header=None, skiprows=2)

        # Assign column names for the first two relevant columns
        # Column 0 = Year, Column 1 = Life expectancy at birth (All races, Both sexes)
        df = df.rename(columns={
            df.columns[0]: "year",
            df.columns[1]: "life_exp_us_cdc"
        })

        df = df[["year", "life_exp_us_cdc"]]

        # Coerce to numeric — drops any footnote/text rows
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
        df["life_exp_us_cdc"] = pd.to_numeric(df["life_exp_us_cdc"], errors="coerce")

        # Filter valid 4-digit years
        df = df.dropna(subset=["year", "life_exp_us_cdc"])
        df = df[(df["year"] >= 1900) & (df["year"] <= 2100)]
        df["year"] = df["year"].astype(int)

        # Hardcode US identifiers
        df["iso3"] = "USA"

        df = df[["iso3", "year", "life_exp_us_cdc"]]
        df = df.drop_duplicates(subset=["iso3", "year"])

        print(f"   ✅ Shape: {df.shape}")
        return df

    except Exception as e:
        print(f"   ❌ Failed to process CDC file: {e}")
        return pd.DataFrame(columns=["iso3", "year", "life_exp_us_cdc"])


# =============================================================================
# STEP 3: Merge All Cleaned Datasets
# =============================================================================
def merge_all(dataframes):
    """
    Sequentially outer-merges a list of DataFrames on ['iso3', 'year'].
    Uses functools.reduce for clean, scalable merging.
    """
    print("\n" + "=" * 70)
    print("🔗 Merging all cleaned datasets...")
    print("=" * 70)

    for i, df in enumerate(dataframes):
        print(f"   DataFrame {i}: {df.shape} | Columns: {list(df.columns)}")

    merged = reduce(
        lambda left, right: pd.merge(left, right, on=MERGE_KEYS, how="outer", suffixes=("", "_dup")),
        dataframes
    )

    # Resolve country_name: prefer OWID's canonical name, fill gaps
    name_cols = [c for c in merged.columns if c.startswith("country_name")]
    if name_cols:
        merged["country_name"] = merged[name_cols[0]]
        for col in name_cols[1:]:
            merged["country_name"] = merged["country_name"].fillna(merged[col])
        # Drop duplicate name columns
        drop_cols = [c for c in merged.columns if c.startswith("country_name_")]
        merged = merged.drop(columns=drop_cols, errors="ignore")

    # Drop any other _dup columns from merge collisions
    dup_cols = [c for c in merged.columns if c.endswith("_dup")]
    merged = merged.drop(columns=dup_cols, errors="ignore")

    # Enforce final column order
    metric_cols = ["life_exp_owid", "life_exp_wb", "hale_who",
                   "life_exp_unicef", "life_exp_kaggle", "life_exp_us_cdc"]
    existing_metrics = [c for c in metric_cols if c in merged.columns]

    final_cols = ["iso3", "country_name", "year"] + existing_metrics
    merged = merged[[c for c in final_cols if c in merged.columns]]

    # Sort for deterministic output
    merged = merged.sort_values(["iso3", "year"]).reset_index(drop=True)

    return merged


# =============================================================================
# STEP 4: Main Pipeline Orchestrator
# =============================================================================
def main():
    print("=" * 70)
    print("🚀 TRANSFORM PIPELINE: Global Life Expectancy & Health Outcomes")
    print("=" * 70)

    # --- Build ISO3 mapping from OWID ---
    print("\n📌 Building ISO3 mapping dictionary from OWID...")
    iso3_mapping = build_iso3_mapping()

    # --- Clean each dataset ---
    df_owid = clean_owid()
    df_wb = clean_worldbank()
    df_kaggle = clean_kaggle(iso3_mapping)
    df_unicef = clean_unicef()
    df_who = clean_who(iso3_mapping)
    df_cdc = clean_cdc()

    # --- Filter: Keep only years 1950–2024 ---
    print("\n📅 Filtering all datasets to year range 1950–2024...")
    df_owid = df_owid[df_owid["year"].between(1950, 2024)]
    df_wb = df_wb[df_wb["year"].between(1950, 2024)]
    df_kaggle = df_kaggle[df_kaggle["year"].between(1950, 2024)]
    df_unicef = df_unicef[df_unicef["year"].between(1950, 2024)]
    df_who = df_who[df_who["year"].between(1950, 2024)]
    df_cdc = df_cdc[df_cdc["year"].between(1950, 2024)]

    # --- Merge ---
    all_dfs = [df_owid, df_wb, df_kaggle, df_unicef, df_who, df_cdc]
    # Filter out empty DataFrames (e.g., if CDC file was missing)
    all_dfs = [df for df in all_dfs if not df.empty]

    master = merge_all(all_dfs)

    # --- Fill missing country_name from OWID's canonical mapping ---
    print("\n📌 Filling missing country names from OWID canonical mapping...")
    iso3_to_name = (
        df_owid.dropna(subset=["iso3"])
        .drop_duplicates(subset=["iso3"])
        .set_index("iso3")["country_name"]
        .to_dict()
    )
    master["country_name"] = master["iso3"].map(iso3_to_name)

    # --- Drop rows with no country_name (World Bank regional aggregates) ---
    rows_before = len(master)
    master = master.dropna(subset=["country_name"])
    dropped = rows_before - len(master)
    if dropped > 0:
        print(f"   🧹 Dropped {dropped} rows with no country_name (regional aggregates like ARB, EAS, etc.)")
    else:
        print(f"   ✅ All rows have a valid country_name.")

    # --- Summary Statistics ---
    print("\n" + "=" * 70)
    print("📊 FINAL MASTER DATASET SUMMARY")
    print("=" * 70)
    print(f"   Shape:              {master.shape[0]} rows × {master.shape[1]} columns")
    print(f"   Unique countries:   {master['iso3'].nunique()}")
    print(f"   Year range:         {int(master['year'].min())} – {int(master['year'].max())}")
    print(f"   Columns:            {list(master.columns)}")
    print(f"\n   Null counts per metric:")
    metric_cols = [c for c in master.columns if c not in ["iso3", "country_name", "year"]]
    for col in metric_cols:
        non_null = master[col].notna().sum()
        total = len(master)
        coverage = non_null / total * 100
        print(f"      {col:<25} {non_null:>7} / {total} ({coverage:.1f}% coverage)")

    # --- Save ---
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    master.to_csv(OUTPUT_FILE, index=False)
    print(f"\n💾 Saved to: {OUTPUT_FILE}")
    print("✅ Transform pipeline complete.")


if __name__ == "__main__":
    main()