import pandas as pd
import os

# --- Configuration ---
RAW_DATA_DIR = "data/raw"
SAMPLE_ROWS = 1000

# Soft-match keywords for identifying merge keys
COUNTRY_KEYWORDS = ['country', 'entity', 'area', 'iso3', 'iso_code', 'countryiso3code', 'ref_area', 'location']
YEAR_KEYWORDS = ['year', 'time', 'date', 'time_period', 'period']

def _detect_merge_keys(columns):
    """Classifies columns into potential country-keys and year-keys using soft matching."""
    cols_lower = {str(c).lower(): c for c in columns}
    
    country_matches = [cols_lower[cl] for cl in cols_lower if any(kw in cl for kw in COUNTRY_KEYWORDS)]
    year_matches = [cols_lower[cl] for cl in cols_lower if any(kw in cl for kw in YEAR_KEYWORDS)]
    
    return country_matches, year_matches

def _read_sample(filepath, filename):
    """Reads a sample of rows with encoding fallback for CSVs and explicit engine for Excel."""
    if filename.endswith('.csv'):
        for encoding in ['utf-8', 'utf-8-sig', 'latin-1']:
            try:
                return pd.read_csv(filepath, nrows=SAMPLE_ROWS, encoding=encoding)
            except UnicodeDecodeError:
                continue
        raise ValueError(f"Could not decode {filename} with any supported encoding.")
    elif filename.endswith('.xlsx'):
        return pd.read_excel(filepath, nrows=SAMPLE_ROWS, engine='openpyxl')
    else:
        raise ValueError(f"Unsupported file type: {filename}")

def profile_datasets(data_dir=RAW_DATA_DIR):
    """
    Profiles all raw data files in the specified directory.
    Extracts schema, dimensions, data types, null percentages, 
    and identifies potential merge keys (Country/Year).
    """
    print("🔍 Data Profiling Report")
    print("=" * 70)

    if not os.path.exists(data_dir):
        print(f"❌ Error: Directory '{data_dir}' not found.")
        return

    data_files = [f for f in sorted(os.listdir(data_dir)) if f.endswith(('.csv', '.xlsx'))]

    if not data_files:
        print(f"⚠️ No data files (.csv, .xlsx) found in '{data_dir}'.")
        return

    print(f"   Found {len(data_files)} data file(s) to profile.\n")

    for filename in data_files:
        filepath = os.path.join(data_dir, filename)
        print(f"{'─' * 70}")
        print(f"📄 File: {filename}")

        try:
            df = _read_sample(filepath, filename)

            if df.empty:
                print("   ⚠️ File is empty or contains only headers. Skipping.")
                continue

            # --- Schema & Dimensions ---
            print(f"   📐 Sample Shape: {df.shape[0]} rows × {df.shape[1]} columns")
            
            # --- Column Detail Table ---
            print(f"   {'Column':<35} {'Dtype':<15} {'Non-Null%':<12} {'Sample Value'}")
            print(f"   {'─'*35} {'─'*15} {'─'*12} {'─'*25}")
            for col in df.columns:
                dtype = str(df[col].dtype)
                non_null_pct = f"{df[col].notna().mean() * 100:.1f}%"
                sample_val = str(df[col].dropna().iloc[0]) if df[col].notna().any() else "N/A"
                # Truncate long sample values
                sample_val = (sample_val[:25] + '...') if len(sample_val) > 28 else sample_val
                print(f"   {str(col):<35} {dtype:<15} {non_null_pct:<12} {sample_val}")

            # --- Merge Key Detection ---
            country_keys, year_keys = _detect_merge_keys(df.columns)
            print(f"\n   🔑 Merge Key Detection:")
            print(f"      Country/Region column(s): {country_keys if country_keys else '❌ None found'}")
            print(f"      Year/Time column(s):      {year_keys if year_keys else '❌ None found'}")

            # --- Flag: ISO3 code presence ---
            has_iso3 = any('iso' in str(c).lower() for c in df.columns)
            if not has_iso3:
                print(f"      ⚠️ No ISO code column detected — will need country-name-to-ISO3 mapping in Transform phase.")

        except Exception as e:
            print(f"   ❌ Failed to read file: {e}")

    print(f"\n{'=' * 70}")
    print("✅ Profiling complete.")

if __name__ == "__main__":
    profile_datasets()