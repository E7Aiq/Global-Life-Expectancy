# Merge Logic — Global Life Expectancy & Health Outcomes

> **Pipeline Version:** 1.0  
> **Last Updated:** February 2026  
> **Output:** `data/processed/master_life_expectancy.csv`, `.parquet`, `.json`  
> **Final Shape:** 17,696 rows × 9 columns — 236 countries — 75 years (1950–2024)

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Data Sources & Ingestion](#2-data-sources--ingestion)
3. [Harmonization Strategy](#3-harmonization-strategy)
4. [Country Name Resolution](#4-country-name-resolution)
5. [Per-Source Cleaning Logic](#5-per-source-cleaning-logic)
6. [Merge Execution](#6-merge-execution)
7. [Post-Merge Enrichment](#7-post-merge-enrichment)
8. [Final Schema](#8-final-schema)
9. [Data Quality Summary](#9-data-quality-summary)
10. [Known Limitations](#10-known-limitations)

---

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                         EXTRACT PHASE                            │
│                                                                  │
│   OWID (CSV)   World Bank (API)   Kaggle (CSV)   UNICEF (CSV)   │
│   WHO (CSV)    CDC (XLSX)                                        │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                       TRANSFORM PHASE                            │
│                                                                  │
│   1. Build ISO3 mapping from OWID (236 countries)                │
│   2. Clean each source independently                             │
│   3. Apply UNIVERSAL_CORRECTIONS to name-only sources            │
│   4. Map country names → ISO3 codes                              │
│   5. Filter year range: 1950–2024                                │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                         MERGE PHASE                              │
│                                                                  │
│   Sequential OUTER JOIN on composite key [iso3, year]            │
│   via functools.reduce → single wide-format DataFrame            │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                      POST-MERGE PHASE                            │
│                                                                  │
│   1. Backfill country_name from OWID canonical mapping           │
│   2. Drop regional aggregates (ARB, EAS, etc.)                   │
│   3. Enforce column order and sort by [iso3, year]               │
│   4. Export → master_life_expectancy.csv                         │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                       EXPORT PHASE                               │
│                                                                  │
│   Script: export_formats.py                                      │
│                                                                  │
│   CSV (transform.py)  →  canonical interchange format            │
│   Parquet (pyarrow)   →  columnar, for Spark / DuckDB / BI      │
│   JSON (records)      →  Web APIs / frontend dashboards          │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. Data Sources & Ingestion

| # | Source | Raw File | Format | Coverage | Metric Column |
|---|--------|----------|--------|----------|---------------|
| 1 | Our World in Data (OWID) | `owid_historical_life_expectancy.csv` | CSV | 236 countries, 1543–2024 | `life_exp_owid` |
| 2 | World Bank | `worldbank_life_expectancy.csv` | CSV (via API) | 266 entities, 1960–2023 | `life_exp_wb` |
| 3 | Kaggle (Health Factors) | `kaggle_health_factors.csv` | CSV | 193 countries, 2000–2015 | `life_exp_kaggle` |
| 4 | UNICEF | `unicef_life_expectancy.csv` | CSV | ~195 countries, 2022 | `life_exp_unicef` |
| 5 | WHO (Healthy Life Expectancy) | `who_healthy_life_expectancy.csv` | CSV | 183 countries, 2000–2021 | `hale_who` |
| 6 | CDC (US Demographics) | `cdc_us_demographics.xlsx` | XLSX | USA only, 1980–2019 | `life_exp_us_cdc` |

### Why These Sources?

- **OWID** provides the longest historical baseline and the cleanest `(Entity, Code)` pairs — it serves as the **canonical reference** for ISO3 mapping and country naming.
- **World Bank** is the most widely cited institutional source with broad modern coverage.
- **WHO HALE** measures *healthy* life expectancy — a fundamentally different metric that enables the project's core analysis: the **health gap** (years lived in poor health).
- **Kaggle, UNICEF, CDC** add cross-validation depth and niche coverage (US sub-national from CDC, recent UNICEF estimates).

---

## 3. Harmonization Strategy

### 3.1 Composite Key

All datasets are merged on a **two-column composite key**:

```
Primary Key = [iso3, year]
```

- `iso3` — ISO 3166-1 alpha-3 country code (e.g., `USA`, `GBR`, `JPN`)
- `year` — Calendar year as integer (1950–2024)

### 3.2 The ISO3 Problem

Only 3 of 6 sources provide ISO3 codes natively:

| Source | Has ISO3? | Resolution |
|--------|-----------|------------|
| OWID | ✅ `Code` column | Direct rename |
| World Bank | ✅ `iso3` column | Direct use |
| UNICEF | ✅ `REF_AREA` column | Direct rename |
| Kaggle | ❌ `Country` name only | Map via OWID dictionary |
| WHO | ❌ `GEO_NAME_SHORT` only | Map via OWID dictionary |
| CDC | ❌ US-only, no code | Hardcoded `iso3 = 'USA'` |

### 3.3 ISO3 Mapping Dictionary

Built from OWID at runtime:

```python
# Source: owid_historical_life_expectancy.csv
# Columns used: Entity, Code
# Filters: dropna(Code), exclude OWID_ prefixes (OWID_WRL, OWID_USS, etc.)
# Result: dict with 236 entries → {"Afghanistan": "AFG", "Albania": "ALB", ...}
```

This dictionary is the **single source of truth** for all name-to-code resolution.

---

## 4. Country Name Resolution

### 4.1 The Problem

International organizations use different official names for the same country:

| Country | OWID | WHO | Kaggle |
|---------|------|-----|--------|
| USA | `United States` | `United States of America` | `United States of America` |
| Russia | `Russia` | `Russian Federation` | `Russian Federation` |
| Vietnam | `Vietnam` | `Viet Nam` | `Viet Nam` |
| Turkey | `Turkey` | `Türkiye` | `Turkey` |
| Venezuela | `Venezuela` | `Venezuela (Bolivarian Republic of)` | `Venezuella (Bolivarian Republic of)` |

### 4.2 UNIVERSAL_CORRECTIONS Dictionary

A centralized dictionary (26 rules) applied to **both** WHO and Kaggle before ISO3 mapping:

```python
UNIVERSAL_CORRECTIONS = {
    # UN formal → OWID standard
    "United States of America":                          "United States",
    "United Kingdom of Great Britain and Northern Ireland": "United Kingdom",
    "Russian Federation":                                "Russia",
    "Syrian Arab Republic":                              "Syria",
    "Türkiye":                                           "Turkey",
    "United Republic of Tanzania":                       "Tanzania",
    "Venezuela (Bolivarian Republic of)":                "Venezuela",
    "Viet Nam":                                          "Vietnam",
    "Timor-Leste":                                       "East Timor",
    "Republic of Moldova":                               "Moldova",
    "Bolivia (Plurinational State of)":                  "Bolivia",
    "Iran (Islamic Republic of)":                        "Iran",
    "Democratic People's Republic of Korea":             "North Korea",
    "Republic of Korea":                                 "South Korea",
    "Lao People's Democratic Republic":                  "Laos",
    "Côte d'Ivoire":                                     "Cote d'Ivoire",
    "Democratic Republic of the Congo":                  "Democratic Republic of Congo",
    "Cabo Verde":                                        "Cape Verde",
    "Brunei Darussalam":                                 "Brunei",
    "Swaziland":                                         "Eswatini",
    "The former Yugoslav republic of Macedonia":         "North Macedonia",
    "Micronesia (Federated States of)":                  "Micronesia (country)",
    # Kaggle-specific typos
    "Venezuella (Bolivarian Republic of)":               "Venezuela",
    "Micronesia":                                        "Micronesia (country)",
    "Micronesia (Federatedd States of)":                 "Micronesia (country)",
    "Czechia":                                           "Czech Republic",
}
```

### 4.3 Impact

| Dataset | Rows Corrected | Rows Recovered |
|---------|---------------|----------------|
| Kaggle | 352 rows across 26 rules | +32 rows (was losing 48, now loses 16) |
| WHO | 462 country names corrected | +418 rows recovered |

---

## 5. Per-Source Cleaning Logic

### 5.1 OWID — `clean_owid()`

```
Input:  21,565 rows
Output: 17,464 rows (4 columns)

Steps:
  1. Rename: Entity → country_name, Code → iso3, Year → year, Life expectancy → life_exp_owid
  2. Drop rows where iso3 is null
  3. Drop OWID aggregate codes (OWID_WRL, OWID_USS, etc.)
  4. Keep: [iso3, year, country_name, life_exp_owid]
```

> **Note:** OWID is the only source that carries `country_name` into the merge. All other sources drop their name column — the canonical name is backfilled from OWID post-merge.

### 5.2 World Bank — `clean_worldbank()`

```
Input:  16,926 rows
Output: 16,670 rows (3 columns)

Steps:
  1. Drop rows where iso3 is null (256 regional aggregates)
  2. Drop country_name (will use OWID's canonical version)
  3. Keep: [iso3, year, life_exp_wb]
```

> **Note:** World Bank data includes ~30 regional/income-group aggregates (ARB, EAS, HIC, etc.). These survive the merge but are removed in post-merge cleanup.

### 5.3 Kaggle — `clean_kaggle(iso3_mapping)`

```
Input:  2,938 rows
Output: 2,922 rows (3 columns)

Steps:
  1. Strip whitespace from column names
  2. Apply UNIVERSAL_CORRECTIONS to Country column (352 rows corrected)
  3. Map Country → iso3 via OWID dictionary
  4. Log unmapped countries (1 remaining: Czech Republic)
  5. Drop unmapped rows (16)
  6. Rename: Year → year, Life expectancy → life_exp_kaggle
  7. Keep: [iso3, year, life_exp_kaggle]
```

### 5.4 UNICEF — `clean_unicef()`

```
Input:  499 rows
Output: 464 rows (3 columns)

Steps:
  1. Filter SEX == '_T' (Total / both sexes)
  2. Rename: REF_AREA → iso3, TIME_PERIOD → year, OBS_VALUE → life_exp_unicef
  3. Coerce life_exp_unicef to numeric
  4. Drop nulls
  5. Deduplicate via groupby(['iso3', 'year']).mean()
  6. Keep: [iso3, year, life_exp_unicef]
```

### 5.5 WHO — `clean_who(iso3_mapping)`

```
Input:  12,936 rows
Output: 3,982 rows (3 columns)

Steps:
  1. Filter DIM_GEO_CODE_TYPE == 'COUNTRY' (drop WHOREGION, WORLDBANKINCOMEGROUP)
     → 12,936 → 12,078
  2. Filter DIM_SEX == 'TOTAL' (cascading: TOTAL → BTSX → BOTHSEXES)
     → 12,078 → 4,026
  3. Rename: GEO_NAME_SHORT → country_name, DIM_TIME → year, AMOUNT_N → hale_who
  4. Apply UNIVERSAL_CORRECTIONS (462 names corrected)
  5. Map country_name → iso3 via OWID dictionary
  6. Drop unmapped + null HALE values
  7. Deduplicate on [iso3, year]
  8. Keep: [iso3, year, hale_who]
```

### 5.6 CDC — `clean_cdc()`

```
Input:  Complex XLSX with metadata headers
Output: 41 rows (3 columns)

Steps:
  1. Read with engine='openpyxl', header=None
  2. Skip first 2 metadata rows (title + URL + footnotes)
  3. Extract column 0 → year, column 1 → life_exp_us_cdc (All races, Both sexes)
  4. Coerce to numeric, drop NaN
  5. Filter valid 4-digit years (1900–2100)
  6. Hardcode iso3 = 'USA'
  7. Deduplicate on [iso3, year]
  8. Keep: [iso3, year, life_exp_us_cdc]
```

---

## 6. Merge Execution

### 6.1 Strategy: Sequential Outer Join

```python
from functools import reduce

merged = reduce(
    lambda left, right: pd.merge(left, right, on=["iso3", "year"], how="outer"),
    [df_owid, df_wb, df_kaggle, df_unicef, df_who, df_cdc]
)
```

### 6.2 Why Outer Join?

Each source covers **different countries and time periods**:

```
OWID:    ████████████████████████████████████████████ 1950 ──────────── 2024
WB:           ███████████████████████████████████████ 1960 ──────────── 2023
Kaggle:                              ████████████████ 2000 ──── 2015
UNICEF:                                            █ 2022
WHO:                                 ████████████████ 2000 ──────── 2021
CDC:                       ██████████████████████████ 1980 ──── 2019
```

An **outer join** preserves every unique `(iso3, year)` pair from any source. The alternative (inner join) would reduce the dataset to only rows where ALL sources overlap — which would be nearly empty given the sparse coverage.

### 6.3 Merge Order

```
Step 1: OWID ⟕ WB         → base with country_name + 2 metrics
Step 2: result ⟕ Kaggle   → adds life_exp_kaggle
Step 3: result ⟕ UNICEF   → adds life_exp_unicef
Step 4: result ⟕ WHO      → adds hale_who
Step 5: result ⟕ CDC      → adds life_exp_us_cdc
```

OWID is merged first because it carries `country_name` — this ensures the name column propagates through the chain.

### 6.4 Collision Handling

```python
suffixes=("", "_dup")
```

If a non-key column (e.g., `country_name`) appears in multiple DataFrames, pandas appends `_dup` to the right-side version. After merging, all `_dup` columns are dropped.

---

## 7. Post-Merge Enrichment

### 7.1 Country Name Backfill

After the merge, some rows (from WB, Kaggle, WHO, or CDC) may lack `country_name` because they were outer-joined without matching an OWID row. The fix:

```python
iso3_to_name = df_owid.drop_duplicates("iso3").set_index("iso3")["country_name"].to_dict()
master["country_name"] = master["iso3"].map(iso3_to_name)
```

This replaces all `country_name` values with OWID's canonical version, ensuring consistency.

### 7.2 Aggregate Removal

World Bank data includes ~30 regional/income-group entities (e.g., `ARB` = Arab World, `EAS` = East Asia & Pacific). These have valid `iso3` codes but no corresponding entry in OWID's country list.

After the backfill, any row where `country_name` is still null is a regional aggregate:

```python
master = master.dropna(subset=["country_name"])
# Result: 2,978 aggregate rows dropped
```

### 7.3 Final Column Order

```python
["iso3", "country_name", "year",
 "life_exp_owid", "life_exp_wb", "hale_who",
 "life_exp_unicef", "life_exp_kaggle", "life_exp_us_cdc"]
```

### 7.4 Deterministic Sort

```python
master.sort_values(["iso3", "year"]).reset_index(drop=True)
```

Ensures identical output across runs for reproducibility.

### 7.5 Multi-Format Export

The master CSV is generated by `transform.py`. A separate script (`export_formats.py`) then reads the CSV and serializes it into two additional formats:

| Format | Engine | Use Case |
|--------|--------|----------|
| **CSV** | pandas (built-in) | Universal interchange — readable by any tool |
| **Parquet** | pyarrow | Columnar, compressed — optimal for Spark, DuckDB, BI platforms |
| **JSON** | pandas `to_json(orient="records")` | Records-oriented — ready for Web APIs and frontend consumption |

This separation keeps the main ETL (`transform.py`) memory-efficient and focused on data logic, while `export_formats.py` handles serialization as a distinct pipeline stage.

---

## 8. Final Schema

```
data/processed/
├── master_life_expectancy.csv          CSV    (universal interchange)
├── master_life_expectancy.parquet      Parquet (Big Data / BI tools)
└── master_life_expectancy.json         JSON   (Web APIs / frontend)

Column Definitions:
├── iso3              string     ISO 3166-1 alpha-3        100.0% fill
├── country_name      string     Canonical name (OWID)     100.0% fill
├── year              int64      Calendar year              100.0% fill
├── life_exp_owid     float64    Life expectancy (OWID)      98.7% fill
├── life_exp_wb       float64    Life expectancy (WB)        77.6% fill
├── hale_who          float64    Healthy life exp (WHO)      22.5% fill
├── life_exp_unicef   float64    Life expectancy (UNICEF)     2.6% fill
├── life_exp_kaggle   float64    Life expectancy (Kaggle)    16.5% fill
└── life_exp_us_cdc   float64    Life expectancy (CDC)        0.2% fill
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

## 9. Data Quality Summary

| Dimension | Result | Detail |
|-----------|--------|--------|
| **Completeness** | ✅ PASS | 36.3% avg metric fill (expected for multi-source sparse data) |
| **Uniqueness** | ✅ PASS | 0 duplicate `(iso3, year)` composite keys |
| **Validity** | ❌ FAIL | 12 rows with life expectancy < 13 years (conflict zones — real data) |
| **Accuracy** | ✅ PASS | WB vs OWID: mean Δ = 0.08 yrs, correlation = 0.9996 |
| **Consistency** | ❌ FAIL | 23 rows where HALE > Life Expectancy (methodological differences) |

**Score: 3/5 — Grade C**

> Both failures reflect **source-level data characteristics**, not pipeline errors. The 12 extreme-low rows are from conflict zones (validated against historical records), and the 23 HALE violations stem from WHO and World Bank using different estimation methodologies for 4 countries (CAF, SSD, SOM, NGA).

---

## 10. Known Limitations

| # | Issue | Impact | Severity |
|---|-------|--------|----------|
| 1 | `Czech Republic` unmapped in both Kaggle and WHO | ~60 rows missing | LOW |
| 2 | `Netherlands (Kingdom oof the)` — typo in WHO source data | ~22 rows missing | LOW |
| 3 | UNICEF covers only 2022 (single year) | Very sparse column | INFO |
| 4 | CDC is US-only (41 rows out of 17,696) | Minimal cross-validation value | INFO |
| 5 | WHO HALE ≠ Life Expectancy (different metric) | Cannot directly compare, only compute health gap | BY DESIGN |
| 6 | World Bank includes regional aggregates | Handled: dropped in post-merge phase | RESOLVED |

---
