"""
dq_framework.py — Data Quality: Methodology-Aware Conflict Detection
=====================================================================
Measures inter-source divergence for global life-expectancy estimates,
accounting for the fact that different providers use different models.

Two checks:
  1. Apples-to-Apples  — max–min spread across Total LE sources
  2. Apples-to-Oranges — HALE (WHO) must be ≤ Total LE (World Bank)
"""

import pandas as pd
import numpy as np
import os
from dataclasses import dataclass, field
from pathlib import Path

# ── constants ────────────────────────────────────────────────────────
LE_SOURCES: list[str] = [
    "life_exp_owid",
    "life_exp_wb",
    "life_exp_unicef",
    "life_exp_kaggle",
]
HALE_COL: str = "hale_who"
TOTAL_LE_COL: str = "life_exp_wb"          # reference source for logical check
DEFAULT_TOLERANCE: float = 2.5             # years
MIN_SOURCES_FOR_COMPARISON: int = 2        # need ≥2 sources for a meaningful spread


# ── result containers ────────────────────────────────────────────────
@dataclass
class ConflictResults:
    """Immutable container returned by the analysis functions."""
    total_rows: int = 0
    comparable_rows: int = 0               # rows with ≥ 2 LE sources
    within_tolerance: int = 0
    severe_conflicts: int = 0
    top_conflicts: pd.DataFrame = field(default_factory=pd.DataFrame)

@dataclass
class HALEResults:
    """Immutable container for the HALE-vs-LE logical check."""
    overlapping_rows: int = 0
    consistent_rows: int = 0
    violations: int = 0
    sample_violations: pd.DataFrame = field(default_factory=pd.DataFrame)


# ── core analysis ────────────────────────────────────────────────────
def load_master_data(file_path: str | Path) -> pd.DataFrame:
    """Load and validate the master CSV, ensuring required columns exist."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Master file not found: {path}")

    df = pd.read_csv(path)

    required_cols = {*LE_SOURCES, HALE_COL, TOTAL_LE_COL, "country_name", "year"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing expected columns: {sorted(missing)}")

    return df


def measure_le_conflicts(
    df: pd.DataFrame,
    tolerance: float = DEFAULT_TOLERANCE,
) -> ConflictResults:
    """
    Apples-to-Apples comparison across Total LE sources.

    Only rows with ≥ MIN_SOURCES_FOR_COMPARISON non-null values are
    evaluated, because a single source cannot produce a meaningful spread.
    """
    le_data = df[LE_SOURCES]

    source_count = le_data.notna().sum(axis=1)
    comparable_mask = source_count >= MIN_SOURCES_FOR_COMPARISON

    # vectorised max – min (NaN-safe by default)
    divergence = le_data.max(axis=1) - le_data.min(axis=1)

    # apply both filters: comparable AND exceeds tolerance
    severe_mask = comparable_mask & (divergence > tolerance)

    # build top-N frame without mutating the original df
    severe_df = (
        df.loc[severe_mask]
        .assign(le_divergence=divergence[severe_mask])
        .sort_values("le_divergence", ascending=False)
    )

    comparable_count = comparable_mask.sum()
    severe_count = severe_mask.sum()

    return ConflictResults(
        total_rows=len(df),
        comparable_rows=int(comparable_count),
        within_tolerance=int(comparable_count - severe_count),
        severe_conflicts=int(severe_count),
        top_conflicts=severe_df.head(5),
    )


def measure_hale_consistency(df: pd.DataFrame) -> HALEResults:
    """
    Apples-to-Oranges: HALE (WHO) must be ≤ Total LE (World Bank).

    Only rows where both values are present are evaluated.
    """
    mask_both = df[HALE_COL].notna() & df[TOTAL_LE_COL].notna()
    sub = df.loc[mask_both]

    violation_mask = sub[HALE_COL] > sub[TOTAL_LE_COL]
    violations_df = (
        sub.loc[violation_mask]
        .assign(hale_excess=lambda d: d[HALE_COL] - d[TOTAL_LE_COL])
    )

    overlapping = len(sub)
    violation_count = int(violation_mask.sum())

    return HALEResults(
        overlapping_rows=overlapping,
        consistent_rows=overlapping - violation_count,
        violations=violation_count,
        sample_violations=violations_df.head(5),
    )


# ── reporting ────────────────────────────────────────────────────────
_SEP = "=" * 70


def _print_le_report(r: ConflictResults, tolerance: float) -> None:
    print(f"\n📊 1. Standard Life Expectancy Conflicts (Tolerance: {tolerance} yrs)")
    print(f"   Total rows in dataset:          {r.total_rows:,}")
    print(f"   Rows with ≥ 2 sources (testable): {r.comparable_rows:,}")
    print(f"   Within tolerance:                 {r.within_tolerance:,}")
    print(f"   SEVERE conflicts:                 {r.severe_conflicts:,}")

    if not r.top_conflicts.empty:
        print("\n   ⚠️  Top Conflicts:")
        for row in r.top_conflicts.itertuples():
            print(
                f"      • {row.country_name} ({row.year}) "
                f"→ Divergence: {row.le_divergence:.2f} yrs"
            )


def _print_hale_report(r: HALEResults) -> None:
    print(f"\n📊 2. Logical Consistency — HALE vs Total LE")
    print(f"   Overlapping rows checked:  {r.overlapping_rows:,}")
    print(f"   Logically consistent:      {r.consistent_rows:,}")
    print(f"   Logic violations:          {r.violations:,}")

    if not r.sample_violations.empty:
        print("\n   ⚠️  Sample Violations:")
        for row in r.sample_violations.itertuples():
            print(
                f"      • {row.country_name} ({row.year}) "
                f"→ HALE: {row.hale_who:.2f} | WB: {row.life_exp_wb:.2f} "
                f"(exceeds by {row.hale_excess:.2f} yrs)"
            )


# ── orchestrator ─────────────────────────────────────────────────────
def run_conflict_analysis(
    file_path: str | Path = "data/processed/master_life_expectancy.csv",
    tolerance: float = DEFAULT_TOLERANCE,
) -> tuple[ConflictResults, HALEResults]:
    """
    Full pipeline: load → analyse → report.

    Returns both result objects so callers (tests, notebooks, downstream
    tasks) can inspect them programmatically.
    """
    print(_SEP)
    print("🔍 MEASURING DATA CONFLICTS (METHODOLOGY-AWARE)")
    print(_SEP)

    df = load_master_data(file_path)

    le_results = measure_le_conflicts(df, tolerance=tolerance)
    hale_results = measure_hale_consistency(df)

    _print_le_report(le_results, tolerance)
    _print_hale_report(hale_results)

    print(f"\n{_SEP}")
    print("🚀 Conflict analysis complete.")
    print(_SEP)

    return le_results, hale_results


# ── entry point ──────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        run_conflict_analysis()
    except (FileNotFoundError, ValueError) as exc:
        print(f"❌ {exc}")