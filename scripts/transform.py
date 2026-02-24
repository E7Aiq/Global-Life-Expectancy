import pandas as pd
import os
from functools import reduce


# =============================================================================
# CONFIGURATION
# =============================================================================
RAW_DIR       = "data/raw"
PROCESSED_DIR = "data/processed"
OUTPUT_FILE   = os.path.join(PROCESSED_DIR, "master_life_expectancy.csv")
MERGE_KEYS    = ["iso3", "year"]
YEAR_MIN, YEAR_MAX = 1950, 2024

# ✅ الحل الصحيح: Override عن طريق iso3 وليس اسم الدولة
# هذا النهج محصّن ضد أي typos أو مسافات أو اختلاف في التهجئة
CUSTOM_NAME_OVERRIDES = {
    "ISR": "Israel (fake country)",
    # أضف المزيد هنا: "iso3_code": "display name"
}

UNIVERSAL_CORRECTIONS = {
    "United States of America":                        "United States",
    "United Kingdom of Great Britain and Northern Ireland": "United Kingdom",
    "Russian Federation":                              "Russia",
    "Syrian Arab Republic":                            "Syria",
    "Türkiye":                                         "Turkey",
    "United Republic of Tanzania":                     "Tanzania",
    "Venezuela (Bolivarian Republic of)":              "Venezuela",
    "Venezuella (Bolivarian Republic of)":             "Venezuela",
    "Viet Nam":                                        "Vietnam",
    "Timor-Leste":                                     "East Timor",
    "Republic of Moldova":                             "Moldova",
    "Micronesia":                                      "Micronesia (country)",
    "Micronesia (Federated States of)":                "Micronesia (country)",
    "Micronesia (Federatedd States of)":               "Micronesia (country)",
    "Bolivia (Plurinational State of)":                "Bolivia",
    "Iran (Islamic Republic of)":                      "Iran",
    "Democratic People's Republic of Korea":           "North Korea",
    "Republic of Korea":                               "South Korea",
    "Lao People's Democratic Republic":                "Laos",
    "Côte d'Ivoire":                                   "Cote d'Ivoire",
    "Czechia":                                         "Czech Republic",
    "Democratic Republic of the Congo":                "Democratic Republic of Congo",
    "Cabo Verde":                                      "Cape Verde",
    "Brunei Darussalam":                               "Brunei",
    "Swaziland":                                       "Eswatini",
    "The former Yugoslav republic of Macedonia":       "North Macedonia",
}


# =============================================================================
# HELPERS
# =============================================================================
def _read_csv_any_encoding(filepath):
    """يجرب عدة encodings تلقائياً حتى ينجح."""
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(filepath, encoding=enc)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not decode {filepath}")


def _apply_universal_corrections(series):
    """Strip whitespace + replace باستخدام UNIVERSAL_CORRECTIONS."""
    return series.str.strip().replace(UNIVERSAL_CORRECTIONS)


def _year_filter(df, label):
    """فلترة السنوات ضمن النطاق المحدد مع طباعة إحصائية."""
    before = len(df)
    df = df[df["year"].between(YEAR_MIN, YEAR_MAX)].copy()
    print(f"   📅 Year filter ({YEAR_MIN}-{YEAR_MAX}): {before} -> {len(df)} rows  [{label}]")
    return df


# =============================================================================
# STEP 1 — Build iso3 mapping from OWID
# =============================================================================
def build_iso3_mapping():
    """يبني قاموس { country_name -> iso3 } من بيانات OWID (الأنظف والأشمل)."""
    filepath = os.path.join(RAW_DIR, "owid_historical_life_expectancy.csv")
    df = pd.read_csv(filepath, usecols=["Entity", "Code"])
    df = df.dropna(subset=["Code"])
    df = df[~df["Code"].str.startswith("OWID_")]
    mapping = (
        df.drop_duplicates(subset=["Entity"])
        .set_index("Entity")["Code"]
        .to_dict()
    )
    print(f"   🗺️  ISO3 mapping: {len(mapping)} unique country names mapped.")
    return mapping


# =============================================================================
# STEP 2 — Individual dataset cleaners
# =============================================================================
def clean_owid():
    print("\n[1/6] Cleaning: owid_historical_life_expectancy.csv")
    filepath = os.path.join(RAW_DIR, "owid_historical_life_expectancy.csv")
    df = pd.read_csv(filepath)
    df = df.rename(columns={
        "Entity":          "country_name",
        "Code":            "iso3",
        "Year":            "year",
        "Life expectancy": "life_exp_owid",
    })
    before = len(df)
    df = df.dropna(subset=["iso3"])
    df = df[~df["iso3"].str.startswith("OWID_")]
    print(f"   Removed {before - len(df)} null/aggregate iso3 rows")
    df = _year_filter(df[["iso3", "year", "country_name", "life_exp_owid"]], "OWID")
    print(f"   ✅ Final shape: {df.shape}")
    return df


def clean_worldbank():
    print("\n[2/6] Cleaning: worldbank_life_expectancy.csv")
    filepath = os.path.join(RAW_DIR, "worldbank_life_expectancy.csv")
    df = pd.read_csv(filepath)
    before = len(df)
    df = df.dropna(subset=["iso3"])
    print(f"   Removed {before - len(df)} null iso3 rows")
    df = _year_filter(df[["iso3", "year", "life_exp_wb"]], "World Bank")
    print(f"   ✅ Final shape: {df.shape}")
    return df


def clean_kaggle(iso3_mapping):
    print("\n[3/6] Cleaning: kaggle_health_factors.csv")
    filepath = os.path.join(RAW_DIR, "kaggle_health_factors.csv")
    df = _read_csv_any_encoding(filepath)
    df.columns = df.columns.str.strip()

    corrected = df["Country"].isin(UNIVERSAL_CORRECTIONS).sum()
    df["Country"] = _apply_universal_corrections(df["Country"])
    print(f"   🔧 Universal corrections: {corrected} rows")

    df["iso3"] = df["Country"].map(iso3_mapping)
    unmapped = df["iso3"].isna().sum()
    if unmapped:
        samples = df[df["iso3"].isna()]["Country"].unique()[:8]
        print(f"   ⚠️  {unmapped} unmapped rows. Samples: {list(samples)}")

    before = len(df)
    df = df.dropna(subset=["iso3"])
    print(f"   Removed {before - len(df)} unmapped rows")
    df = df.rename(columns={"Year": "year", "Life expectancy": "life_exp_kaggle"})
    df = _year_filter(df[["iso3", "year", "life_exp_kaggle"]], "Kaggle")
    print(f"   ✅ Final shape: {df.shape}")
    return df


def clean_unicef():
    print("\n[4/6] Cleaning: unicef_life_expectancy.csv")
    filepath = os.path.join(RAW_DIR, "unicef_life_expectancy.csv")
    df = _read_csv_any_encoding(filepath)

    before = len(df)
    if "SEX" in df.columns:
        df = df[df["SEX"] == "_T"]
        print(f"   SEX=_T filter: {before} -> {len(df)} rows")

    df = df.rename(columns={
        "REF_AREA":    "iso3",
        "TIME_PERIOD": "year",
        "OBS_VALUE":   "life_exp_unicef",
    })
    df["life_exp_unicef"] = pd.to_numeric(df["life_exp_unicef"], errors="coerce")
    df = df.dropna(subset=["iso3", "life_exp_unicef"])
    df = df[["iso3", "year", "life_exp_unicef"]]
    df = df.groupby(["iso3", "year"], as_index=False)["life_exp_unicef"].mean()
    df = _year_filter(df, "UNICEF")
    print(f"   ✅ Final shape: {df.shape}")
    return df


def clean_who(iso3_mapping):
    print("\n[5/6] Cleaning: who_healthy_life_expectancy.csv")
    filepath = os.path.join(RAW_DIR, "who_healthy_life_expectancy.csv")
    df = _read_csv_any_encoding(filepath)
    print(f"   Raw shape: {df.shape}")

    if "DIM_GEO_CODE_TYPE" in df.columns:
        before = len(df)
        df = df[df["DIM_GEO_CODE_TYPE"] == "COUNTRY"]
        print(f"   Geo filter COUNTRY: {before} -> {len(df)} rows")

    if "DIM_SEX" in df.columns:
        sex_vals = df["DIM_SEX"].unique()
        print(f"   Sex values found: {sex_vals}")
        for target in ("TOTAL", "BTSX", "BOTHSEXES"):
            if target in sex_vals:
                before = len(df)
                df = df[df["DIM_SEX"] == target]
                print(f"   Sex filter {target}: {before} -> {len(df)} rows")
                break
        else:
            print("   ⚠️  No combined-sex indicator — averaging across all sex values.")

    df = df.rename(columns={
        "GEO_NAME_SHORT": "country_name",
        "DIM_TIME":       "year",
        "AMOUNT_N":       "hale_who",
    })
    df["year"]     = pd.to_numeric(df["year"],     errors="coerce")
    df["hale_who"] = pd.to_numeric(df["hale_who"], errors="coerce")

    corrected = df["country_name"].isin(UNIVERSAL_CORRECTIONS).sum()
    df["country_name"] = _apply_universal_corrections(df["country_name"])
    print(f"   🔧 Universal corrections: {corrected} rows")

    df["iso3"] = df["country_name"].map(iso3_mapping)
    unmapped = df["iso3"].isna().sum()
    if unmapped:
        samples = df[df["iso3"].isna()]["country_name"].unique()[:8]
        print(f"   ⚠️  {unmapped} unmapped. Samples: {list(samples)}")

    df = df.dropna(subset=["iso3", "hale_who"])
    df = df[["iso3", "year", "hale_who"]].drop_duplicates(subset=["iso3", "year"])
    df = _year_filter(df, "WHO")
    print(f"   ✅ Final shape: {df.shape}")
    return df


def clean_cdc():
    print("\n[6/6] Cleaning: cdc_us_demographics.xlsx")
    filepath = os.path.join(RAW_DIR, "cdc_us_demographics.xlsx")

    if not os.path.exists(filepath):
        print("   ⚠️  File not found — skipping CDC.")
        return pd.DataFrame(columns=["iso3", "year", "life_exp_us_cdc"])

    try:
        df_raw = pd.read_excel(filepath, engine="openpyxl", header=None, nrows=5)
        print(f"   Preview:\n{df_raw.to_string()}")
        df = pd.read_excel(filepath, engine="openpyxl", header=None, skiprows=2)
        df = df.rename(columns={df.columns[0]: "year", df.columns[1]: "life_exp_us_cdc"})
        df = df[["year", "life_exp_us_cdc"]]
        df["year"]            = pd.to_numeric(df["year"],            errors="coerce")
        df["life_exp_us_cdc"] = pd.to_numeric(df["life_exp_us_cdc"], errors="coerce")
        df = df.dropna()
        df = df[(df["year"] >= 1900) & (df["year"] <= 2100)]
        df["year"] = df["year"].astype(int)
        df["iso3"] = "USA"
        df = df[["iso3", "year", "life_exp_us_cdc"]].drop_duplicates(subset=["iso3", "year"])
        df = _year_filter(df, "CDC")
        print(f"   ✅ Final shape: {df.shape}")
        return df
    except Exception as exc:
        print(f"   ❌ Failed: {exc}")
        return pd.DataFrame(columns=["iso3", "year", "life_exp_us_cdc"])


# =============================================================================
# STEP 3 — Merge all datasets
# =============================================================================
def merge_all(dataframes):
    print("\n" + "=" * 70)
    print("🔗 Merging all cleaned datasets...")
    print("=" * 70)
    for i, df in enumerate(dataframes):
        print(f"   [{i}] shape={df.shape}  cols={list(df.columns)}")

    merged = reduce(
        lambda left, right: pd.merge(
            left, right, on=MERGE_KEYS, how="outer", suffixes=("", "_dup")
        ),
        dataframes,
    )

    # دمج عمود country_name: الأولوية لـ OWID
    name_cols = [c for c in merged.columns if c.startswith("country_name")]
    if name_cols:
        merged["country_name"] = merged[name_cols[0]]
        for col in name_cols[1:]:
            merged["country_name"] = merged["country_name"].fillna(merged[col])
        drop_cols = [c for c in merged.columns if c.startswith("country_name_")]
        merged = merged.drop(columns=drop_cols, errors="ignore")

    dup_cols = [c for c in merged.columns if c.endswith("_dup")]
    merged = merged.drop(columns=dup_cols, errors="ignore")

    metric_cols = ["life_exp_owid", "life_exp_wb", "hale_who",
                   "life_exp_unicef", "life_exp_kaggle", "life_exp_us_cdc"]
    existing = [c for c in metric_cols if c in merged.columns]
    final_cols = ["iso3", "country_name", "year"] + existing
    merged = merged[[c for c in final_cols if c in merged.columns]]
    merged = merged.sort_values(["iso3", "year"]).reset_index(drop=True)
    return merged


# =============================================================================
# STEP 4 — Apply custom name overrides (iso3-based, مضمون 100%)
# =============================================================================
def apply_custom_name_overrides(master):
    """
    يعدّل country_name عبر iso3 — محصّن ضد:
    - أخطاء التهجئة
    - المسافات الزائدة / الأحرف الخفية
    - اختلاف الـ capitalization
    - تغيير مصدر البيانات
    """
    print("\n📌 Applying custom country name overrides...")
    total = 0
    for iso3_code, new_name in CUSTOM_NAME_OVERRIDES.items():
        mask = master["iso3"] == iso3_code
        count = mask.sum()
        if count:
            master.loc[mask, "country_name"] = new_name
            print(f"   ✅ iso3={iso3_code}: {count} rows -> \"{new_name}\"")
            total += count
        else:
            print(f"   ⚠️  iso3={iso3_code}: not found in master — skipped.")
    print(f"   Total rows modified: {total}")
    return master


# =============================================================================
# STEP 5 — Main orchestrator
# =============================================================================
def main():
    print("=" * 70)
    print("🚀 TRANSFORM PIPELINE: Global Life Expectancy & Health Outcomes")
    print("=" * 70)

    print("\n📌 Building ISO3 mapping from OWID...")
    iso3_mapping = build_iso3_mapping()

    df_owid   = clean_owid()
    df_wb     = clean_worldbank()
    df_kaggle = clean_kaggle(iso3_mapping)
    df_unicef = clean_unicef()
    df_who    = clean_who(iso3_mapping)
    df_cdc    = clean_cdc()

    all_dfs = [df for df in [df_owid, df_wb, df_kaggle, df_unicef, df_who, df_cdc]
               if not df.empty]
    master = merge_all(all_dfs)

    # إعادة بناء country_name من OWID canonical (يضمن التوحيد)
    print("\n📌 Normalising country names from OWID canonical mapping...")
    iso3_to_name = (
        df_owid.dropna(subset=["iso3"])
        .drop_duplicates(subset=["iso3"])
        .set_index("iso3")["country_name"]
        .to_dict()
    )
    master["country_name"] = master["iso3"].map(iso3_to_name)

    before = len(master)
    master = master.dropna(subset=["country_name"])
    print(f"   🧹 Dropped {before - len(master)} regional aggregate rows.")

    # ✅ تطبيق التعديلات المخصصة — بعد كل شيء، iso3-based
    master = apply_custom_name_overrides(master)

    # Summary
    print("\n" + "=" * 70)
    print("📊 FINAL MASTER DATASET SUMMARY")
    print("=" * 70)
    print(f"   Shape:            {master.shape[0]:,} rows × {master.shape[1]} columns")
    print(f"   Unique countries: {master['iso3'].nunique()}")
    print(f"   Year range:       {int(master['year'].min())} – {int(master['year'].max())}")
    print(f"   Columns:          {list(master.columns)}")
    print("\n   Coverage per metric:")
    metric_cols = [c for c in master.columns if c not in ("iso3", "country_name", "year")]
    for col in metric_cols:
        nn  = master[col].notna().sum()
        pct = nn / len(master) * 100
        bar = "█" * int(pct // 5) + "░" * (20 - int(pct // 5))
        print(f"      {col:<25} {bar}  {nn:>7,} / {len(master):,}  ({pct:.1f}%)")

    # تحقق من نجاح الـ override
    print("\n   Override verification:")
    for iso3_code, new_name in CUSTOM_NAME_OVERRIDES.items():
        vals = master[master["iso3"] == iso3_code]["country_name"].unique()
        print(f"      iso3={iso3_code} -> {list(vals)}")

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    master.to_csv(OUTPUT_FILE, index=False)
    print(f"\n💾 Saved to: {OUTPUT_FILE}")
    print("✅ Transform pipeline complete.")


if __name__ == "__main__":
    main()
