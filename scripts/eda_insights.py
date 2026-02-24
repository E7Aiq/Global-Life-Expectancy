"""
eda_insights.py — Portfolio-Grade EDA Visualizations
=====================================================
Two key insights from the Global Life Expectancy master dataset:
  1. The Health Gap   — Total LE vs Healthy LE (illusion of longevity)
  2. The Conflict Map — World Bank vs OWID divergence heatmap
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import os
from pathlib import Path

# ── Global aesthetics ────────────────────────────────────────────────
PALETTE = {
    "bg":        "#FAFAFA",
    "text":      "#2D2D2D",
    "subtle":    "#8C8C8C",
    "total_le":  "#E8927C",   # warm coral for total LE bar
    "hale":      "#B83B5E",   # deep rose for healthy LE bar
    "gap_label": "#6C3483",   # purple accent for gap annotations
    "accent":    "#F08A5D",   # highlight accent
}

FONT_TITLE    = {"fontsize": 18, "fontweight": "bold", "color": PALETTE["text"],
                 "fontfamily": "serif"}
FONT_SUBTITLE = {"fontsize": 11, "color": PALETTE["subtle"], "fontfamily": "serif"}
FONT_AXIS     = {"fontsize": 11, "color": PALETTE["text"]}

OUTPUT_DIR = Path("outputs/visuals")

plt.rcParams.update({
    "figure.facecolor":  PALETTE["bg"],
    "axes.facecolor":    PALETTE["bg"],
    "savefig.facecolor": PALETTE["bg"],
    "font.family":       "serif",
    "axes.edgecolor":    "#DDDDDD",
    "axes.grid":         True,
    "grid.color":        "#ECECEC",
    "grid.linewidth":    0.6,
})


def _ensure_output_dir() -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR


def _strip_spines(ax, keep: str = "none"):
    """Remove chart borders for a cleaner look."""
    for spine in ax.spines.values():
        spine.set_visible(False)
    if "left" in keep:
        ax.spines["left"].set_visible(True)
    if "bottom" in keep:
        ax.spines["bottom"].set_visible(True)


# ── Data loader ──────────────────────────────────────────────────────
def load_data() -> pd.DataFrame:
    file_path = "data/processed/master_life_expectancy.csv"
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ File not found: {file_path}")
    return pd.read_csv(file_path)


# ── Insight 1 — The Health Gap ───────────────────────────────────────
def insight_1_the_health_gap(df: pd.DataFrame) -> None:
    """
    'The Illusion of Longevity'
    Among the top-15 longest-living countries, how many years are spent
    in poor health?  Overlaid horizontal bars with gap annotations.
    """
    print("🎨 Generating Insight 1: The Health Gap …")

    TARGET_YEAR = 2019  # most complete pre-COVID year
    required = ["life_exp_wb", "hale_who"]

    # ── dynamic year fallback ────────────────────────────────────────
    df_year = df[df["year"] == TARGET_YEAR].dropna(subset=required)
    if df_year.empty:
        overlap = df.dropna(subset=required)
        if overlap.empty:
            print("   ⚠️ No overlapping WB + HALE data at all — skipping.")
            return
        fallback = int(overlap["year"].max())
        print(f"   ⚠️ No data for {TARGET_YEAR}; falling back to {fallback}.")
        df_year = overlap[overlap["year"] == fallback]
        TARGET_YEAR = fallback

    top = (
        df_year
        .nlargest(15, "life_exp_wb")
        .assign(**{"Gap": lambda d: d["life_exp_wb"] - d["hale_who"]})
        .sort_values("Gap", ascending=True)
    )

    # ── palette ──────────────────────────────────────────────────────
    CLR_TOTAL = "#4e79a7"   # desaturated slate blue  — background bar
    CLR_HALE  = "#f28e2b"   # warm orange             — foreground bar
    CLR_GAP   = "#FFFFFF"   # white text in the gap zone

    # ── draw ─────────────────────────────────────────────────────────
    out = _ensure_output_dir()
    fig, ax = plt.subplots(figsize=(14, 9))

    bar_height = 0.68

    # total LE (background bar)
    ax.barh(
        top["country_name"], top["life_exp_wb"],
        color=CLR_TOTAL, edgecolor="white", linewidth=0.4,
        height=bar_height, label="Total Life Expectancy (World Bank)",
        zorder=2,
    )
    # healthy LE (foreground bar)
    ax.barh(
        top["country_name"], top["hale_who"],
        color=CLR_HALE, edgecolor="white", linewidth=0.4,
        height=bar_height, label="Healthy Life Expectancy (WHO)",
        zorder=3,
    )

    # ── data labels ──────────────────────────────────────────────────
    for _, row in top.iterrows():
        total = row["life_exp_wb"]
        hale  = row["hale_who"]
        gap   = row["Gap"]
        country = row["country_name"]

        # 1) gap value — centred in the blue "tail" between orange end & bar tip
        ax.text(
            hale + gap / 2, country,
            f"{gap:.1f}",
            ha="center", va="center",
            fontsize=9, fontweight="bold", color=CLR_GAP, zorder=4,
        )

        # 2) HALE value — just inside the orange bar tip
        ax.text(
            hale - 0.4, country,
            f"{hale:.1f}",
            ha="right", va="center",
            fontsize=8.5, fontweight="semibold", color="white", zorder=4,
        )

        # 3) total LE value — just outside the blue bar tip
        ax.text(
            total + 0.35, country,
            f"{total:.1f} Yrs",
            ha="left", va="center",
            fontsize=8.5, color=PALETTE["subtle"], zorder=4,
        )

    # ── titles ───────────────────────────────────────────────────────
    fig.suptitle(
        f"The Illusion of Longevity — Top 15 Countries ({TARGET_YEAR})",
        **FONT_TITLE, x=0.03, y=0.98, ha="left",
    )
    ax.set_title(
        "Years spent in poor health  ·  "
        "Gap shown in the blue zone between each pair of bars",
        **FONT_SUBTITLE, pad=20, loc="left",
    )

    ax.set_xlabel("Age  (years)", **FONT_AXIS, labelpad=10)
    ax.set_ylabel("")
    ax.tick_params(axis="y", labelsize=11, length=0, pad=4)
    ax.tick_params(axis="x", labelsize=10, colors=PALETTE["subtle"])
    ax.xaxis.set_major_locator(mticker.MultipleLocator(10))
    ax.set_xlim(0, top["life_exp_wb"].max() + 7)

    # minimalist: remove all spines then restore bottom only
    sns.despine(ax=ax, left=True, bottom=False, top=True, right=True)
    ax.spines["bottom"].set_color("#DDDDDD")

    # legend — horizontal bar above the chart, below the titles
    legend = ax.legend(
        loc="lower center", bbox_to_anchor=(0.5, 1.06), ncol=2,
        frameon=True, fancybox=True,
        framealpha=0.95, edgecolor="#DDDDDD",
        fontsize=10.5, borderpad=0.8, handlelength=1.8,
        columnspacing=2.5,
    )
    legend.get_frame().set_boxstyle("round,pad=0.4")

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    path = out / "insight_health_gap.png"
    fig.savefig(path, dpi=300, bbox_inches="tight")
    plt.show()
    print(f"   ✅ Saved → {path}")


# ── Insight 2 — The Conflict Heatmap ────────────────────────────────
def insight_2_the_conflict_heatmap(df: pd.DataFrame) -> None:
    """
    'The Statistical Conflict Map'
    Heatmap of the 20 countries with the largest divergence between
    World Bank and OWID life-expectancy estimates.
    """
    print("🎨 Generating Insight 2: The Conflict Heatmap …")

    core_sources = ["life_exp_wb", "life_exp_owid"]

    # ── dynamic source & year resolution ─────────────────────────────
    valid = df.dropna(subset=core_sources)
    if valid.empty:
        print("   ⚠️ No overlapping WB + OWID data — skipping.")
        return

    latest_year = int(valid["year"].max())
    df_year = valid[valid["year"] == latest_year].copy()

    if df_year.empty:
        print(f"   ⚠️ Latest year {latest_year} yielded 0 rows — skipping.")
        return

    print(f"   📌 Using overlap year: {latest_year}  ({len(df_year)} countries)")

    # optionally add Kaggle if available for this year
    extra_cols = []
    if "life_exp_kaggle" in df.columns:
        kaggle_avail = df_year["life_exp_kaggle"].notna().sum()
        if kaggle_avail >= 10:
            extra_cols.append("life_exp_kaggle")
            print(f"   📌 Kaggle data available for {kaggle_avail} countries — included.")

    # ── compute divergence ───────────────────────────────────────────
    df_year = df_year.assign(
        Divergence=lambda d: (d["life_exp_wb"] - d["life_exp_owid"]).abs()
    )

    top = (
        df_year
        .nlargest(20, "Divergence")
        .set_index("country_name")
    )

    display_cols = ["life_exp_wb", "life_exp_owid"] + extra_cols + ["Divergence"]
    heatmap_data = top[display_cols].rename(columns={
        "life_exp_wb":     "World Bank",
        "life_exp_owid":   "OWID",
        "life_exp_kaggle": "Kaggle",
        "Divergence":      "Divergence",
    })

    # ── draw ─────────────────────────────────────────────────────────
    # Color every cell in a row by its Divergence value so the viewer's
    # eye is drawn to high-conflict rows.  Annotations still show the
    # real WB / OWID / Divergence numbers.
    out = _ensure_output_dir()

    # Build a color-driver matrix: every column in a row = that row's
    # Divergence value.  sns.heatmap colors by this matrix while the
    # `annot` parameter displays the real numbers.
    color_data = pd.DataFrame(
        np.tile(heatmap_data["Divergence"].values[:, None], heatmap_data.shape[1]),
        index=heatmap_data.index,
        columns=heatmap_data.columns,
    )

    fig, ax = plt.subplots(figsize=(14, 12))

    sns.heatmap(
        color_data,                          # drives cell colour
        annot=heatmap_data,                  # drives cell text
        fmt=".1f",
        cmap="YlOrRd",
        linewidths=1.0, linecolor="white",
        cbar_kws={
            "label": "Divergence (years)",
            "shrink": 0.55,
            "pad": 0.025,
        },
        annot_kws={"size": 12, "va": "center", "weight": "bold"},
        ax=ax,
    )

    # Use dark text on light cells, white text on dark cells
    dmin = heatmap_data["Divergence"].min()
    dmax = heatmap_data["Divergence"].max()
    mid  = (dmin + dmax) / 2
    for text_obj, div_val in zip(ax.texts, np.tile(heatmap_data["Divergence"].values, heatmap_data.shape[1])):
        text_obj.set_color("white" if div_val > mid else PALETTE["text"])

    # ── titles: suptitle (main) + ax.set_title (subtitle) ───────────
    fig.suptitle(
        f"Statistical Conflict — World Bank vs OWID ({latest_year})",
        **FONT_TITLE, x=0.03, y=0.98, ha="left",
    )
    ax.set_title(
        "Top 20 countries ranked by absolute divergence  ·  "
        "Row colour intensity = degree of conflict",
        **FONT_SUBTITLE, pad=20, loc="left",
    )

    ax.set_ylabel("")
    ax.set_xlabel("")
    ax.tick_params(axis="y", labelsize=11, length=0, rotation=0, pad=6)
    ax.tick_params(axis="x", labelsize=11, length=0, rotation=0, pad=8)

    # protect top 5% for suptitle so it never clips
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    path = out / "insight_data_conflict.png"
    fig.savefig(path, dpi=300, bbox_inches="tight")
    plt.show()
    print(f"   ✅ Saved → {path}")


# ── Entry point ──────────────────────────────────────────────────────
if __name__ == "__main__":
    df = load_data()
    insight_1_the_health_gap(df)
    insight_2_the_conflict_heatmap(df)