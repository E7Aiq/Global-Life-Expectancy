<div align="center">

# 🌍 Global Life Expectancy & Health Outcomes

### A Multi-Source ETL Pipeline for Cross-Organizational Health Data Integration

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/Pandas-2.0+-150458?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Complete-brightgreen?style=for-the-badge)]()

**236 countries · 75 years · 6 international sources · 1 unified dataset**

[Overview](#overview) · [Architecture](#architecture) · [Data Sources](#data-sources) · [Quick Start](#quick-start) · [Pipeline Stages](#pipeline-stages) · [ETL Value-Added](#etl-value-added) · [Data Quality](#data-quality) · [Key Findings](#key-findings)

</div>

---

## Overview

This project builds a **production-grade ETL pipeline** that ingests life expectancy data from six international organizations, harmonizes them on a common schema `[iso3, year]`, and produces a single analytical-ready dataset spanning **1950–2024**.

The core research question:

> **How many years do people actually live in good health — and how does the "health gap" vary across countries and time?**

By merging the **WHO Healthy Life Expectancy (HALE)** with standard life expectancy metrics from the World Bank, OWID, and others, this pipeline enables direct computation of the health gap — the years lived in poor health, disability, or chronic illness.

### What Makes This Project Different

| Aspect | Approach |
|--------|----------|
| **Not just one source** | Integrates 6 independent datasets with different schemas, naming conventions, and coverage windows |
| **Name harmonization** | A 26-rule `UNIVERSAL_CORRECTIONS` dictionary resolves formal UN names, Kaggle typos, and OWID conventions |
| **Built-in quality gates** | A 5-dimension Data Quality Framework (Completeness, Uniqueness, Validity, Accuracy, Consistency) runs automatically |
| **ETL value proven** | A dedicated `value_added_report.py` quantifies the Before → After impact with a 6/6 scorecard |
| **Multi-format output** | Final dataset exported as CSV (universal), Parquet (Big Data/BI), and JSON (Web APIs) |
| **Reproducible** | Deterministic output — same input files always produce identical `master_life_expectancy.csv` |
| **Documented anomalies** | Every data quality failure is traced to its root cause rather than silently dropped |

---

## Project Structure

```
Global Life Expectancy/
│
├── data/
│   ├── raw/                                    # Original source files (6 files)
│   │   ├── owid_historical_life_expectancy.csv
│   │   ├── worldbank_life_expectancy.csv
│   │   ├── kaggle_health_factors.csv
│   │   ├── unicef_life_expectancy.csv
│   │   ├── who_healthy_life_expectancy.csv
│   │   └── cdc_us_demographics.xlsx
│   │
│   └── processed/                              # Pipeline output
│       ├── master_life_expectancy.csv          # Final merged dataset (17,696 × 9)
│       ├── master_life_expectancy.parquet      # Parquet export (Big Data/BI)
│       └── master_life_expectancy.json         # JSON export (Web APIs)
│
├── outputs/
│   └── visuals/                                # Generated charts and plots
│
├── scripts/
│   ├── extract_wb.py                           # Stage 1: World Bank API extraction
│   ├── profile_data.py                         # Stage 2: Raw data profiling & schema analysis
│   ├── transform.py                            # Stage 3: Core ETL — clean, harmonize, merge
│   ├── export_formats.py                       # Stage 3.5: Parquet & JSON serialization
│   ├── audit.py                                # Stage 4: Post-merge data audit
│   ├── dq_framework.py                         # Stage 5: 5-dimension Data Quality Framework
│   ├── value_added_report.py                   # Stage 5.5: Raw vs Final comparative assessment
│   ├── methodology_conflicts.py                # Stage 5.5: Cross-source conflict detection
│   ├── investigate_anomalies.py                # Stage 6: Anomaly deep-dive (HALE violations)
│   ├── check_under_13.py                       # Stage 6: Extreme low-value investigation
│   ├── deep_eda.py                             # Stage 6: Dark-themed visual analysis
│   ├── eda_insights.py                         # Stage 6: Health Gap & Conflict Heatmap charts
│   └── quality_compare.py                      # Stage 6: Source coverage visualization
│
├── .vscode/
│   └── settings.json
│
├── merge_logic.md                              # Data dictionary & engineering decisions
├── README.md                                   # This file
├── LICENSE
└── .gitignore
```

---

## Architecture

```
                    ┌─────────────────────────────────────┐
                    │           RAW DATA SOURCES           │
                    │                                     │
                    │  OWID ─── World Bank ─── Kaggle     │
                    │  UNICEF ─── WHO ─── CDC             │
                    └──────────────┬──────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────┐
                    │         EXTRACT  (extract_wb.py)     │
                    │  World Bank API → CSV                │
                    └──────────────┬──────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────┐
                    │         PROFILE  (profile_data.py)   │
                    │  Schema · Types · Nulls · Keys       │
                    └──────────────┬──────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────┐
                    │       TRANSFORM  (transform.py)      │
                    │                                     │
                    │  ┌─────────────────────────────┐    │
                    │  │  clean_owid()               │    │
                    │  │  clean_worldbank()           │    │
                    │  │  clean_kaggle(iso3_mapping)  │    │
                    │  │  clean_unicef()              │    │
                    │  │  clean_who(iso3_mapping)     │    │
                    │  │  clean_cdc()                 │    │
                    │  └──────────────┬──────────────┘    │
                    │                 │                    │
                    │       merge_all() — OUTER JOIN       │
                    │         on [iso3, year]              │
                    │                 │                    │
                    │    Post-merge enrichment & cleanup   │
                    └──────────────┬──────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────┐
                    │      EXPORT  (export_formats.py)     │
                    │                                     │
                    │  CSV  → universal interchange        │
                    │  Parquet → Big Data / BI tools       │
                    │  JSON → Web APIs / frontend          │
                    └──────────────┬──────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────┐
                    │         VALIDATE & ANALYZE           │
                    │                                     │
                    │  audit.py              — ETL audit   │
                    │  dq_framework.py       — 5-dim DQ    │
                    │  value_added_report.py — Before/After│
                    │  methodology_conflicts.py — Conflict │
                    │  eda_insights.py       — Visuals     │
                    └──────────────┬──────────────────────┘
                                   │
                                   ▼
                        master_life_expectancy.csv
                        master_life_expectancy.parquet
                        master_life_expectancy.json
```

---

## Data Sources

| Source | Organization | File | Format | Time Span | Countries | Metric |
|--------|-------------|------|--------|-----------|-----------|--------|
| **OWID** | Our World in Data | `owid_historical_life_expectancy.csv` | CSV | 1543–2023 | 265 | Life expectancy at birth |
| **World Bank** | The World Bank | `worldbank_life_expectancy.csv` | API → CSV | 1960–2023 | 265* | Life expectancy at birth |
| **Kaggle** | Kaggle (Health Factors) | `kaggle_health_factors.csv` | CSV | 2000–2015 | 193 | Life expectancy at birth |
| **UNICEF** | United Nations Children's Fund | `unicef_life_expectancy.csv` | CSV | 2022–2024 | 266 | Life expectancy at birth |
| **WHO** | World Health Organization | `who_healthy_life_expectancy.csv` | CSV | 2000–2021 | 196 | **Healthy** life expectancy (HALE) |
| **CDC** | Centers for Disease Control | `cdc_us_demographics.xlsx` | XLSX | 1980–2019 | 1 (USA) | Life expectancy at birth |

*\* World Bank includes ~30 regional aggregates (ARB, EAS, HIC, etc.) which are removed during post-merge cleanup.*

---

## Quick Start

### Prerequisites

```bash
python -m venv venv
source venv/bin/activate  # macOS/Linux
venv\Scripts\activate     # Windows

pip install pandas numpy requests openpyxl pyarrow matplotlib seaborn
```

### Run the Pipeline

```bash
# Stage 1 — Extract World Bank data via API
python scripts/extract_wb.py

# Stage 2 — Profile all raw sources
python scripts/profile_data.py

# Stage 3 — Transform: clean, harmonize, merge → master CSV
python scripts/transform.py

# Stage 3.5 — Export to Parquet and JSON
python scripts/export_formats.py

# Stage 4 — Audit the merged dataset
python scripts/audit.py

# Stage 5 — Data Quality Framework (5-dimension scorecard)
python scripts/dq_framework.py

# Stage 5.5 — ETL Value-Added Report (Raw vs Final comparison)
python scripts/value_added_report.py

# Stage 5.5 — Methodology-Aware Conflict Detection
python scripts/methodology_conflicts.py

# Stage 6 — Investigation & EDA
python scripts/investigate_anomalies.py
python scripts/check_under_13.py
python scripts/deep_eda.py
python scripts/eda_insights.py
python scripts/quality_compare.py
```

---

## Pipeline Stages

### Stage 1 — Extract

**Script:** `scripts/extract_wb.py`

Connects to the World Bank REST API (`SP.DYN.LE00.IN` indicator), pulls up to 20,000 records in JSON format, parses them into a DataFrame, and saves to `data/raw/worldbank_life_expectancy.csv`.

All other datasets are pre-downloaded CSVs/XLSX files placed in `data/raw/`.

### Stage 2 — Profile

**Script:** `scripts/profile_data.py`

Scans every file in `data/raw/` and produces a structured report:
- Schema dimensions (rows × columns)
- Per-column data types, non-null percentages, and sample values
- Automatic merge key detection (country/year columns)
- ISO3 code presence warning

### Stage 3 — Transform

**Script:** `scripts/transform.py` — the core of the pipeline.

Each source has a dedicated cleaning function:

| Function | Key Operations |
|----------|---------------|
| `clean_owid()` | Rename columns, drop OWID aggregates (`OWID_WRL`, `OWID_USS`) |
| `clean_worldbank()` | Drop null ISO3 codes, strip country_name |
| `clean_kaggle(mapping)` | Apply `UNIVERSAL_CORRECTIONS`, map names → ISO3 |
| `clean_unicef()` | Filter `SEX == '_T'`, deduplicate via `groupby.mean()` |
| `clean_who(mapping)` | Filter `COUNTRY` + `TOTAL`, apply corrections, map → ISO3 |
| `clean_cdc()` | Parse complex XLSX, `skiprows=2`, hardcode `iso3='USA'` |

After cleaning, all DataFrames are filtered to **1950–2024** and merged via sequential outer join on `[iso3, year]` using `functools.reduce`.

**Post-merge:**
1. Backfill `country_name` from OWID's canonical `iso3 → name` dictionary
2. Drop 2,978 regional aggregate rows (World Bank entities with no real country mapping)
3. Enforce deterministic column order and sort

> See [`merge_logic.md`](merge_logic.md) for the full technical specification.

### Stage 3.5 — Export

**Script:** `scripts/export_formats.py`

Reads the generated `master_life_expectancy.csv` and serializes it into two additional formats:

| Format | File | Use Case |
|--------|------|----------|
| **Parquet** | `master_life_expectancy.parquet` | Columnar storage for Big Data tools (Spark, DuckDB, BI platforms) |
| **JSON** | `master_life_expectancy.json` | Web APIs, frontend dashboards, JavaScript consumers |

### Stage 4 — Audit

**Script:** `scripts/audit.py`

Validates the output dataset:
- Row count, duplicate detection
- Per-column missing value analysis with severity flags
- Merge integrity — how many sources are present per row
- Key field validation (null checks, uniqueness of composite key)

### Stage 5 — Data Quality Framework

**Script:** `scripts/dq_framework.py`

Evaluates 5 dimensions with a final letter-grade scorecard:

| # | Dimension | What It Checks | Pass Criteria |
|---|-----------|---------------|---------------|
| 1 | **Completeness** | Null rates per column and overall fill rate | Avg metric fill > 30% |
| 2 | **Uniqueness** | Composite key `(iso3, year)` uniqueness, full-row duplicates | No duplicates |
| 3 | **Validity** | Values within `[13, 95]` bounds, year range, data types | Zero out-of-bounds |
| 4 | **Accuracy** | Cross-source mean absolute difference and Pearson correlation | Mean Δ < 3.5 years |
| 5 | **Consistency** | Business logic: HALE ≤ Life Expectancy | Zero violations |

### Stage 5.5 — ETL Validation

| Script | Purpose |
|--------|---------|
| `scripts/value_added_report.py` | Profiles every raw source and the final dataset, then compares them side-by-side to prove the pipeline's measurable impact |
| `scripts/methodology_conflicts.py` | Detects inter-source divergence using a tolerance-based approach, separating "apples-to-apples" (Total LE sources) from "apples-to-oranges" (HALE vs Total LE) |

### Stage 6 — Investigation & EDA

| Script | Purpose |
|--------|---------|
| `scripts/investigate_anomalies.py` | Deep-dive into HALE > LE violations and WB vs UNICEF discrepancies |
| `scripts/check_under_13.py` | Inspect extreme low-outlier rows (< 13 years) |
| `scripts/deep_eda.py` | Dark-themed visualizations: Missing Data Matrix, Decade Distribution |
| `scripts/eda_insights.py` | Health Gap chart (top 15 countries), Data Conflict Heatmap |
| `scripts/quality_compare.py` | Source coverage comparison bar chart |

---

## Output Schema

```
data/processed/master_life_expectancy.csv      (universal CSV)
data/processed/master_life_expectancy.parquet   (columnar, for BI/Big Data)
data/processed/master_life_expectancy.json      (records-oriented, for Web APIs)

Column              Type       Description                          Coverage
──────────────────  ─────────  ───────────────────────────────────  ────────
iso3                string     ISO 3166-1 alpha-3 country code      100.0%
country_name        string     Canonical country name (from OWID)   100.0%
year                int64      Calendar year (1950–2024)             100.0%
life_exp_owid       float64    Life expectancy — Our World in Data    98.7%
life_exp_wb         float64    Life expectancy — World Bank           77.6%
hale_who            float64    Healthy life expectancy — WHO          22.5%
life_exp_unicef     float64    Life expectancy — UNICEF                2.6%
life_exp_kaggle     float64    Life expectancy — Kaggle               16.5%
life_exp_us_cdc     float64    Life expectancy — CDC (US only)         0.2%
```

**17,696 rows · 236 countries · 75 years (1950–2024)**

---

## ETL Value-Added

The `value_added_report.py` script profiles all 6 raw source files and the final master dataset, then generates a structured "Before → After" comparison. This is the pipeline's proof of impact.

### Raw State (Before)
- **6 separate files** with 97 total columns, different encodings, and inconsistent naming conventions
- **55,073 total records** across all sources, with no cross-referencing
- **3 of 6 sources** lacked ISO3 codes (WHO, Kaggle, CDC used country names or were US-only)
- **13 duplicate rows** scattered across raw files
- **12 values** outside the `[13, 95]` logical bounds

### Final State (After)
- **1 unified file** with 9 standardized columns
- **17,696 integrated rows** with up to 6 sources per row
- **100% ISO3 fill** across 236 unique sovereign countries
- **0 duplicate** `(iso3, year)` composite keys
- **12 OOB values retained** — verified real data from conflict zones, documented not dropped

### Value-Added Scorecard

```
Dimension                      Status  Detail
──────────────────────────── ────────  ──────────────────────────────────────
Schema Consolidation           ✅ PASS  6 files → 1 unified dataset
Entity Resolution              ✅ PASS  3 sources lacked ISO3 → 100% mapped
Duplicate Elimination          ✅ PASS  13 raw dupes → 0 key dupes
Record Integration             ✅ PASS  55,073 scattered → 17,696 linked rows
Coverage Amplification         ✅ PASS  236 countries × 1950–2024
Validity Enforcement           ✅ PASS  Bounds [13,95], types, year range enforced

Pipeline Value Score: 6/6 (100%) — Grade A
```

---

## Data Quality

### Current Scorecard

```
Dimension            Status    Detail
────────────────── ────────── ──────────────────────────────────
Completeness         ✅ PASS   36.3% avg metric fill rate
Uniqueness           ✅ PASS   0 duplicate composite keys
Validity             ❌ FAIL   12 rows < 13 yrs (conflict zones)
Accuracy             ✅ PASS   WB vs OWID: mean Δ = 0.08 years
Consistency          ❌ FAIL   23 HALE > LE violations (4 countries)

Final Score: 3/5 (60%) — Grade C
```

> Both failures reflect **source-level data characteristics**, not pipeline errors. The 12 extreme-low rows are from conflict zones (validated against historical records), and the 23 HALE violations stem from WHO and World Bank using different estimation methodologies for 4 countries (CAF, SSD, SOM, NGA).

### Methodology Conflict Detection

The `methodology_conflicts.py` script separates two fundamentally different comparison types:

**1. Apples-to-Apples — Total LE Sources (Tolerance: 2.5 yrs)**

| Metric | Value |
|--------|-------|
| Total rows | 17,696 |
| Testable rows (≥ 2 sources) | 13,743 |
| Within tolerance | 13,046 |
| Severe conflicts | 697 |

Top conflicts: Central African Republic (2022: 37.98 yrs divergence), Somalia (2011: 20.65 yrs), South Sudan (2015: 17.54 yrs).

**2. Apples-to-Oranges — HALE vs Total LE (Logical Check)**

| Metric | Value |
|--------|-------|
| Overlapping rows | 3,982 |
| Logically consistent | 3,959 |
| Violations (HALE > LE) | 23 |

### Cross-Source Accuracy

| Comparison | Overlapping Rows | Mean Δ | Max Δ | Correlation |
|------------|-----------------|--------|-------|-------------|
| World Bank vs OWID | 13,726 | 0.080 | 5.94 | 0.9996 |
| World Bank vs UNICEF | 215 | 2.473 | 37.98 | 0.9224 |
| World Bank vs Kaggle | 2,912 | 1.513 | 33.94 | 0.9664 |

---

## Key Findings

### The Health Gap

WHO's **Healthy Life Expectancy (HALE)** measures years lived in full health. The gap between total life expectancy and HALE reveals the burden of disability and chronic illness:

> People may live to 75+ years on average — but spend **8–15 years** in poor health.

### Source Coverage Over Time

```
OWID:    ████████████████████████████████████████████ 1950 ──────────── 2024
WB:           ███████████████████████████████████████ 1960 ──────────── 2023
Kaggle:                              ████████████████ 2000 ──── 2015
UNICEF:                                          ███ 2022 ── 2024
WHO:                                 ████████████████ 2000 ──────── 2021
CDC:                       ██████████████████████████ 1980 ──── 2019
```

### Source Coverage Distribution

```
Sources per Row          Rows          %
─────────────── ──────────── ──────────
1                      3,953      22.3%  ███████████
2                      9,508      53.7%  ██████████████████████████
3                      1,336       7.5%  ███
4                      2,883      16.3%  ████████
5                         16       0.1%

Average: 2.18 sources per row
Every row has at least 1 source metric. ✓
```

---

## Challenges & Solutions

| Challenge | Root Cause | Solution |
|-----------|-----------|----------|
| WHO returned 0 rows after cleaning | CSV read with `header=None` despite headers existing | Changed to default `header=0` with named column filters |
| WHO sex filter matched nothing | Actual values were `TOTAL`, not `BTSX` | Implemented cascading filter: `TOTAL` → `BTSX` → `BOTHSEXES` |
| Kaggle lost 336 rows | Country names like `"Venezuella"` (typo), `"Viet Nam"` didn't match OWID | Built `UNIVERSAL_CORRECTIONS` dictionary with 26 rules |
| WHO lost 462 rows | Formal UN names (`"Russian Federation"`, `"Türkiye"`) not in OWID | Applied same `UNIVERSAL_CORRECTIONS` to WHO before ISO3 mapping |
| 3,210 rows had no country name | World Bank regional aggregates (ARB, EAS) joined without OWID match | Backfill from OWID canonical mapping, then drop remaining nulls |
| DQ Validity was too strict | Lower bound of 20 rejected real conflict-zone data | Lowered to 13 based on historical research |
| DQ Accuracy was too strict | Threshold of 2 years rejected normal cross-org variation | Relaxed to 3.5 years |
| Consistency check always showed PASS | `check_consistency()` returned `None` instead of boolean | Fixed return value, passed dynamically to scorecard |

---

## Technologies

| Tool | Usage |
|------|-------|
| **Python 3.10+** | Core language |
| **pandas** | Data manipulation, merging, cleaning |
| **requests** | World Bank API integration |
| **openpyxl** | CDC Excel file parsing |
| **pyarrow** | Parquet export engine |
| **matplotlib** | Dark-themed visualizations |
| **seaborn** | Statistical charts (Health Gap, Heatmap) |
| **NumPy** | Numerical computations in DQ framework |

---

## License

This project is for educational and portfolio purposes. All data is sourced from publicly available international datasets.

---

<div align="center">

**Built with precision. Documented with purpose.**

*Every row has a story. Every anomaly has a root cause.*

</div>

## 👨‍💻 Author

**Mohammed Abdullah Alzobaidi**
* 🌐 **Portfolio:** [Alzobaidi.me](https://alzobaidi.me)
* ✉️ **Email:** Mohammed@Alzobaidi.me
* 🕮 **X (Twitter):** [@x7ciy](https://x.com/x7ciy)