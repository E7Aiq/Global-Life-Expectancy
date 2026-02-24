import pandas as pd


def run_investigation():
    print("\n" + "=" * 60)
    print(" 🕵️  DATA ANOMALY INVESTIGATION")
    print("=" * 60)

    # Load data
    df = pd.read_csv("data/processed/master_life_expectancy.csv")
    df = df[df["year"].between(1950, 2024)]

    # ---------------------------------------------------------
    # CASE 1: Logical violations where HALE > Life Expectancy
    # ---------------------------------------------------------
    print("\n🔍 CASE 1: Logical Violations (HALE > World Bank Life Exp)")
    print("-" * 60)

    # Filter to rows that have BOTH metrics present before computing
    valid_hale = df.dropna(subset=["life_exp_wb", "hale_who"]).copy()
    valid_hale["health_gap"] = valid_hale["life_exp_wb"] - valid_hale["hale_who"]

    violations = valid_hale.loc[valid_hale["health_gap"] < 0].sort_values("health_gap")

    print(f"   Overlapping rows (WB + WHO): {len(valid_hale):,}")
    print(f"   Violations found:            {len(violations)}")

    if len(violations) > 0:
        print(f"\n   {'iso3':<8} {'Country':<28} {'Year':>6} {'WB LE':>10} {'HALE':>10} {'Gap':>8}")
        print(f"   {'─' * 8} {'─' * 28} {'─' * 6} {'─' * 10} {'─' * 10} {'─' * 8}")
        for _, row in violations.iterrows():
            country = str(row["country_name"])[:26] if pd.notna(row["country_name"]) else "N/A"
            print(
                f"   {row['iso3']:<8} {country:<28} {int(row['year']):>6} "
                f"{row['life_exp_wb']:>10.2f} {row['hale_who']:>10.2f} {row['health_gap']:>8.2f}"
            )

        # Root cause analysis
        print(f"\n   📌 Root Cause Analysis:")
        unique_countries = violations["iso3"].nunique()
        year_range = f"{int(violations['year'].min())}–{int(violations['year'].max())}"
        avg_gap = violations["health_gap"].mean()
        print(f"      Affected countries: {unique_countries}")
        print(f"      Year range:         {year_range}")
        print(f"      Mean violation:     {avg_gap:.2f} years")
        print(f"      Explanation:        WHO HALE and World Bank LE use different")
        print(f"                          methodologies. Small negative gaps (<2 yrs)")
        print(f"                          are expected due to estimation uncertainty.")
    else:
        print("   ✅ No violations found.")

    # ---------------------------------------------------------
    # CASE 2: Largest discrepancies between World Bank and UNICEF
    # ---------------------------------------------------------
    print("\n" + "=" * 60)
    print("🔍 CASE 2: Largest Discrepancies (World Bank vs UNICEF)")
    print("-" * 60)

    valid_unicef = df.dropna(subset=["life_exp_wb", "life_exp_unicef"]).copy()
    valid_unicef["diff"] = (valid_unicef["life_exp_wb"] - valid_unicef["life_exp_unicef"]).abs()

    print(f"   Overlapping rows (WB + UNICEF): {len(valid_unicef):,}")

    if len(valid_unicef) > 0:
        # Summary statistics
        mean_diff = valid_unicef["diff"].mean()
        median_diff = valid_unicef["diff"].median()
        max_diff = valid_unicef["diff"].max()
        pct_under_1 = (valid_unicef["diff"] < 1).mean() * 100

        print(f"   Mean |Δ|:                       {mean_diff:.3f} years")
        print(f"   Median |Δ|:                     {median_diff:.3f} years")
        print(f"   Max |Δ|:                        {max_diff:.3f} years")
        print(f"   Rows with |Δ| < 1 year:         {pct_under_1:.1f}%")

        # Top 10 worst discrepancies
        top_diffs = valid_unicef.nlargest(10, "diff")

        print(f"\n   Top 10 largest gaps:")
        print(f"   {'iso3':<8} {'Country':<28} {'Year':>6} {'WB':>10} {'UNICEF':>10} {'|Δ|':>8}")
        print(f"   {'─' * 8} {'─' * 28} {'─' * 6} {'─' * 10} {'─' * 10} {'─' * 8}")
        for _, row in top_diffs.iterrows():
            country = str(row["country_name"])[:26] if pd.notna(row["country_name"]) else "N/A"
            print(
                f"   {row['iso3']:<8} {country:<28} {int(row['year']):>6} "
                f"{row['life_exp_wb']:>10.2f} {row['life_exp_unicef']:>10.2f} {row['diff']:>8.2f}"
            )

        # Check if extreme outliers share a pattern
        extreme = valid_unicef[valid_unicef["diff"] > 10]
        if len(extreme) > 0:
            print(f"\n   ⚠️  {len(extreme)} rows with |Δ| > 10 years:")
            extreme_countries = (
                extreme.groupby("iso3")["diff"]
                .agg(["count", "mean"])
                .sort_values("mean", ascending=False)
            )
            print(f"   {'iso3':<8} {'Count':>7} {'Mean |Δ|':>10}")
            print(f"   {'─' * 8} {'─' * 7} {'─' * 10}")
            for iso3, row in extreme_countries.iterrows():
                print(f"   {iso3:<8} {int(row['count']):>7} {row['mean']:>10.2f}")
            print(f"\n   📌 These likely reflect methodological differences or")
            print(f"      data vintage gaps between World Bank and UNICEF sources.")
        else:
            print(f"\n   ✅ No extreme outliers (|Δ| > 10 years) found.")
    else:
        print("   ⚠️  No overlapping data between World Bank and UNICEF.")

    print(f"\n{'=' * 60}")
    print(" ✅ Anomaly investigation complete.")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    run_investigation()