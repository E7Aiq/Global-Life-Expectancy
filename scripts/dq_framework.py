import pandas as pd
import numpy as np
import os

# --- Configuration ---
MASTER_FILE = "data/processed/master_life_expectancy.csv"
YEAR_RANGE = (1950, 2024)
LIFE_EXP_BOUNDS = (13, 95)

METRIC_COLS = [
    "life_exp_owid",
    "life_exp_wb",
    "hale_who",
    "life_exp_unicef",
    "life_exp_kaggle",
    "life_exp_us_cdc",
]

METRIC_LABELS = {
    "life_exp_owid":   "OWID",
    "life_exp_wb":     "World Bank",
    "hale_who":        "WHO (HALE)",
    "life_exp_unicef": "UNICEF",
    "life_exp_kaggle": "Kaggle",
    "life_exp_us_cdc": "CDC (US)",
}


# =============================================================================
#  DQ DIMENSION 1: COMPLETENESS
# =============================================================================
def check_completeness(df):
    """Measures null/missing rates per column and overall dataset fill rate."""
    print("\n1️⃣  COMPLETENESS (Missing Data Analysis)")
    print("-" * 60)

    total = len(df)
    existing = [c for c in METRIC_COLS if c in df.columns]

    print(f"\n   {'Column':<28} {'Present':>9} {'Missing':>9} {'Fill %':>9}")
    print(f"   {'─' * 28} {'─' * 9} {'─' * 9} {'─' * 9}")

    fill_rates = {}
    for col in ["iso3", "country_name", "year"] + existing:
        present = df[col].notna().sum()
        missing = total - present
        pct = present / total * 100
        fill_rates[col] = pct
        flag = "✅" if pct > 90 else "⚠️" if pct > 50 else "🔴"
        print(f"   {col:<28} {present:>9} {missing:>9} {pct:>8.1f}% {flag}")

    # Overall fill rate across metric columns only
    metric_cells = df[existing].size
    filled_cells = df[existing].notna().sum().sum()
    overall = filled_cells / metric_cells * 100 if metric_cells > 0 else 0
    print(f"\n   📊 Overall metric fill rate: {filled_cells:,} / {metric_cells:,} cells ({overall:.1f}%)")

    return fill_rates


# =============================================================================
#  DQ DIMENSION 2: UNIQUENESS & JOIN INTEGRITY
# =============================================================================
def check_uniqueness(df):
    """Validates primary key uniqueness and detects hidden duplicates."""
    print("\n2️⃣  UNIQUENESS & JOIN INTEGRITY")
    print("-" * 60)

    total_rows = len(df)
    unique_countries = df["iso3"].nunique()
    years_count = df["year"].nunique()
    expected_grid = unique_countries * years_count

    # Check composite key uniqueness
    group_sizes = df.groupby(["iso3", "year"]).size()
    max_per_group = group_sizes.max()
    dup_keys = (group_sizes > 1).sum()

    print(f"   Unique countries:          {unique_countries}")
    print(f"   Unique years:              {years_count}")
    print(f"   Max rows per (iso3, year): {max_per_group} (expected: 1)")
    print(f"   Actual rows:               {total_rows:,}")
    print(f"   Theoretical grid:          {expected_grid:,}")
    print(f"   Grid sparsity:             {total_rows / expected_grid * 100:.1f}%")

    if max_per_group == 1:
        print("   ✅ PASS — No duplicate composite keys")
    else:
        print(f"   ❌ FAIL — {dup_keys} duplicate (iso3, year) pairs detected")

    # Full-row duplicate check
    full_dups = df.duplicated().sum()
    if full_dups == 0:
        print("   ✅ PASS — No exact duplicate rows")
    else:
        print(f"   ⚠️  {full_dups} exact duplicate rows found")

    return max_per_group == 1 and full_dups == 0


# =============================================================================
#  DQ DIMENSION 3: VALIDITY (Range & Type Checks)
# =============================================================================
def check_validity(df):
    """Validates that metric values fall within logical bounds."""
    print("\n3️⃣  VALIDITY (Range & Type Checks)")
    print("-" * 60)

    lo, hi = LIFE_EXP_BOUNDS
    existing = [c for c in METRIC_COLS if c in df.columns]
    all_pass = True

    print(f"\n   Logical bounds: {lo} ≤ life expectancy ≤ {hi}")
    print(f"\n   {'Metric':<28} {'< {}'.format(lo):>8} {'> {}'.format(hi):>8} {'Total OOB':>10} {'Status':>8}")
    print(f"   {'─' * 28} {'─' * 8} {'─' * 8} {'─' * 10} {'─' * 8}")

    for col in existing:
        series = df[col].dropna()
        below = (series < lo).sum()
        above = (series > hi).sum()
        oob = below + above
        status = "✅" if oob == 0 else "❌"
        if oob > 0:
            all_pass = False
        print(f"   {col:<28} {below:>8} {above:>8} {oob:>10} {status:>8}")

    # Year range validation
    year_oob = df[~df["year"].between(*YEAR_RANGE)]
    if len(year_oob) > 0:
        all_pass = False
        print(f"\n   ❌ {len(year_oob)} rows with year outside {YEAR_RANGE}")
    else:
        print(f"\n   ✅ All years within {YEAR_RANGE[0]}–{YEAR_RANGE[1]}")

    # Data type check
    print(f"\n   Data types:")
    for col in existing:
        dtype = df[col].dtype
        is_numeric = pd.api.types.is_numeric_dtype(df[col])
        status = "✅" if is_numeric else "❌ NOT NUMERIC"
        print(f"      {col:<28} {str(dtype):<12} {status}")

    return all_pass


# =============================================================================
#  DQ DIMENSION 4: ACCURACY (Cross-Source Validation)
# =============================================================================
def check_accuracy(df):
    """Cross-validates overlapping sources to detect systematic discrepancies."""
    print("\n4️⃣  ACCURACY (Cross-Source Conflict Detection)")
    print("-" * 60)

    # Define source pairs that SHOULD agree (both measure life expectancy at birth)
    comparisons = [
        ("life_exp_wb", "life_exp_owid",   "World Bank vs OWID"),
        ("life_exp_wb", "life_exp_unicef", "World Bank vs UNICEF"),
        ("life_exp_wb", "life_exp_kaggle", "World Bank vs Kaggle"),
    ]

    print(f"\n   {'Comparison':<30} {'Pairs':>7} {'Mean Δ':>9} {'Max Δ':>9} {'Corr':>8} {'Status':>8}")
    print(f"   {'─' * 30} {'─' * 7} {'─' * 9} {'─' * 9} {'─' * 8} {'─' * 8}")

    results = {}
    for col_a, col_b, label in comparisons:
        if col_a not in df.columns or col_b not in df.columns:
            continue

        valid = df.dropna(subset=[col_a, col_b]).copy()
        n_pairs = len(valid)
        if n_pairs == 0:
            print(f"   {label:<30} {'N/A':>7}")
            continue

        diff = (valid[col_a] - valid[col_b]).abs()
        mean_d = diff.mean()
        max_d = diff.max()
        corr = valid[col_a].corr(valid[col_b])

        # Threshold: mean discrepancy > 3.5 years is a concern
        status = "✅" if mean_d < 3.5 else "⚠️" if mean_d < 5 else "❌"
        print(f"   {label:<30} {n_pairs:>7} {mean_d:>8.3f} {max_d:>8.3f} {corr:>8.4f} {status}")

        results[label] = {"pairs": n_pairs, "mean_diff": mean_d, "max_diff": max_d, "corr": corr}

    # Worst-case outlier report
    if "life_exp_wb" in df.columns and "life_exp_owid" in df.columns:
        valid = df.dropna(subset=["life_exp_wb", "life_exp_owid"]).copy()
        if len(valid) > 0:
            valid["_diff"] = (valid["life_exp_wb"] - valid["life_exp_owid"]).abs()
            top5 = valid.nlargest(5, "_diff")[["iso3", "year", "life_exp_wb", "life_exp_owid", "_diff"]]
            print(f"\n   Top 5 largest WB vs OWID discrepancies:")
            print(f"   {'iso3':<8} {'Year':>6} {'WB':>10} {'OWID':>10} {'|Δ|':>8}")
            print(f"   {'─' * 8} {'─' * 6} {'─' * 10} {'─' * 10} {'─' * 8}")
            for _, row in top5.iterrows():
                print(f"   {row['iso3']:<8} {int(row['year']):>6} {row['life_exp_wb']:>10.2f} {row['life_exp_owid']:>10.2f} {row['_diff']:>8.2f}")

    return results


# =============================================================================
#  DQ DIMENSION 5: CONSISTENCY (Business Logic — Health Gap)
# =============================================================================
def check_consistency(df):
    """Validates business logic: HALE should always be ≤ life expectancy."""
    print("\n5️⃣  CONSISTENCY (Business Logic Validation)")
    print("-" * 60)

    if "life_exp_wb" not in df.columns or "hale_who" not in df.columns:
        print("   ⚠️  Required columns not present. Skipping.")
        return True

    valid = df.dropna(subset=["life_exp_wb", "hale_who"]).copy()
    n_pairs = len(valid)
    print(f"\n   Overlapping (WB + WHO HALE) rows: {n_pairs:,}")

    if n_pairs == 0:
        print("   ⚠️  No overlapping data to validate.")
        return True

    # HALE should always be ≤ Life Expectancy
    valid["health_gap"] = valid["life_exp_wb"] - valid["hale_who"]
    violations = valid[valid["health_gap"] < 0]

    print(f"   Rows where HALE > Life Exp (logical violation): {len(violations)}")
    if len(violations) == 0:
        print("   ✅ PASS — HALE is always ≤ Life Expectancy")
    else:
        print(f"   ❌ FAIL — {len(violations)} rows violate HALE ≤ LE constraint")

    # Health gap statistics
    gap_mean = valid["health_gap"].mean()
    gap_median = valid["health_gap"].median()
    gap_std = valid["health_gap"].std()
    gap_min = valid["health_gap"].min()
    gap_max = valid["health_gap"].max()

    print(f"\n   📊 Health Gap Statistics (Life Exp − HALE):")
    print(f"      Mean:   {gap_mean:.2f} years")
    print(f"      Median: {gap_median:.2f} years")
    print(f"      Std:    {gap_std:.2f} years")
    print(f"      Range:  {gap_min:.2f} – {gap_max:.2f} years")

    # Per-region insight (top 10 highest gaps)
    top_gaps = (
        valid.groupby("iso3")["health_gap"]
        .mean()
        .nlargest(10)
    )
    print(f"\n   🏥 Top 10 Countries by Average Health Gap:")
    print(f"   {'iso3':<8} {'Avg Gap (yrs)':>14}")
    print(f"   {'─' * 8} {'─' * 14}")
    for iso3, gap in top_gaps.items():
        print(f"   {iso3:<8} {gap:>14.2f}")

    print(f"\n   💡 Insight: On average, people live {gap_mean:.1f} years in poor health")
    print(f"      globally — this is the core theme of this project.")

    return len(violations) == 0


# =============================================================================
#  SCORECARD
# =============================================================================
def print_scorecard(fill_rates, uniqueness_pass, validity_pass, accuracy_results, consistency_pass):
    """Prints a final pass/fail scorecard across all 5 dimensions."""
    print(f"\n{'=' * 60}")
    print(" 📊 DATA QUALITY SCORECARD")
    print(f"{'=' * 60}")

    existing = [c for c in METRIC_COLS if c in fill_rates]
    avg_fill = np.mean([fill_rates[c] for c in existing]) if existing else 0
    completeness_pass = avg_fill > 30  # lenient for multi-source sparse data

    # Accuracy: pass if mean diff < 3.5 years for all comparisons
    accuracy_pass = all(
        v["mean_diff"] < 3.5 for v in accuracy_results.values()
    ) if accuracy_results else True

    dimensions = [
        ("Completeness",  completeness_pass,  f"{avg_fill:.1f}% avg metric fill"),
        ("Uniqueness",    uniqueness_pass,     "Composite key integrity"),
        ("Validity",      validity_pass,       "Range & type checks"),
        ("Accuracy",      accuracy_pass,       "Cross-source < 3.5yr mean Δ"),
        ("Consistency",   consistency_pass,    "HALE ≤ Life Exp (dynamic check)"),
    ]

    passed = 0
    print(f"\n   {'Dimension':<18} {'Status':>8} {'Detail'}")
    print(f"   {'─' * 18} {'─' * 8} {'─' * 32}")
    for name, ok, detail in dimensions:
        status = "✅ PASS" if ok else "❌ FAIL"
        if ok:
            passed += 1
        print(f"   {name:<18} {status:>8}   {detail}")

    total = len(dimensions)
    score = passed / total * 100
    grade = "A" if score == 100 else "B" if score >= 80 else "C" if score >= 60 else "D"
    print(f"\n   🏆 Final Score: {passed}/{total} ({score:.0f}%) — Grade: {grade}")


# =============================================================================
#  MAIN
# =============================================================================
def run_dq_framework():
    print("=" * 60)
    print(" 🛡️  DATA QUALITY FRAMEWORK REPORT (5 Dimensions)")
    print("=" * 60)

    if not os.path.exists(MASTER_FILE):
        print(f"\n❌ File not found: {MASTER_FILE}")
        print("   Run scripts/transform.py first.")
        return

    df = pd.read_csv(MASTER_FILE)
    df = df[df["year"].between(*YEAR_RANGE)]

    total_rows = len(df)
    unique_countries = df["iso3"].nunique()
    years_count = df["year"].nunique()

    print(f"\n📁 Dataset: {MASTER_FILE}")
    print(f"   {total_rows:,} rows | {unique_countries} countries | {years_count} years")
    print(f"   Year range: {int(df['year'].min())} – {int(df['year'].max())}")

    # --- Run all 5 dimensions ---
    fill_rates = check_completeness(df)
    uniqueness_pass = check_uniqueness(df)
    validity_pass = check_validity(df)
    accuracy_results = check_accuracy(df)
    consistency_pass = check_consistency(df)

    # --- Final scorecard ---
    print_scorecard(fill_rates, uniqueness_pass, validity_pass, accuracy_results, consistency_pass)

    print(f"\n{'=' * 60}")
    print(" ✅ Data Quality Framework complete.")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    run_dq_framework()
