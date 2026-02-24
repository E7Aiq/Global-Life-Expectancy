import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from matplotlib.colors import LinearSegmentedColormap

# ─────────────────────────────────────────────
#  DESIGN SYSTEM
# ─────────────────────────────────────────────
PALETTE = {
    "bg":       "#0D1117",
    "surface":  "#161B22",
    "border":   "#21262D",
    "accent1":  "#58A6FF",
    "accent2":  "#3FB950",
    "accent3":  "#F78166",
    "accent4":  "#D2A8FF",
    "accent5":  "#FFA657",
    "accent6":  "#7EE787", # أضفنا لوناً سادساً لملف CDC
    "text":     "#E6EDF3",
    "muted":    "#8B949E",
}

SOURCE_COLORS = {
    "life_exp_owid":   PALETTE["accent1"],
    "life_exp_wb":     PALETTE["accent2"],
    "hale_who":        PALETTE["accent4"],
    "life_exp_kaggle": PALETTE["accent5"],
    "life_exp_unicef": PALETTE["accent3"],
    "life_exp_us_cdc": PALETTE["accent6"], # تم إضافة CDC
}

SOURCE_LABELS = {
    "life_exp_owid":   "Our World in Data",
    "life_exp_wb":     "World Bank",
    "hale_who":        "WHO (HALE)",
    "life_exp_kaggle": "Kaggle",
    "life_exp_unicef": "UNICEF",
    "life_exp_us_cdc": "CDC (US Only)", # تم إضافة CDC
}

def apply_dark_style():
    plt.rcParams.update({
        "figure.facecolor":  PALETTE["bg"],
        "axes.facecolor":    PALETTE["surface"],
        "axes.edgecolor":    PALETTE["border"],
        "axes.labelcolor":   PALETTE["text"],
        "axes.titlecolor":   PALETTE["text"],
        "axes.spines.top":   False,
        "axes.spines.right": False,
        "axes.grid":         True,
        "grid.color":        PALETTE["border"],
        "grid.linewidth":    0.6,
        "grid.alpha":        0.8,
        "xtick.color":       PALETTE["muted"],
        "ytick.color":       PALETTE["muted"],
        "xtick.labelsize":   9,
        "ytick.labelsize":   9,
        "text.color":        PALETTE["text"],
        "font.family":       "monospace",
        "legend.facecolor":  PALETTE["surface"],
        "legend.edgecolor":  PALETTE["border"],
        "legend.labelcolor": PALETTE["text"],
        "legend.fontsize":   9,
    })

def load_data():
    df = pd.read_csv("data/processed/master_life_expectancy.csv")
    df = df[df["year"].between(1950, 2024)]
    return df

# ─────────────────────────────────────────────
#  CHART 1 — Missing Data Matrix
# ─────────────────────────────────────────────
def plot_missing_data_structure(df, save_path="data/processed/missing_data_structure.png"):
    print("→ Generating Missing Data Matrix...")

    cols = list(SOURCE_COLORS.keys())
    df_sorted = df.sort_values("year") # شلنا الفلترة من هنا عشان نحتفظ بكل الأعمدة

    cmap = LinearSegmentedColormap.from_list(
        "avail", [PALETTE["border"], PALETTE["accent1"]], N=2
    )

    fig, ax = plt.subplots(figsize=(13, 6))
    fig.patch.set_facecolor(PALETTE["bg"])
    ax.set_facecolor(PALETTE["surface"])

    # حطينا الفلترة [cols] هنا وقت استخراج المصفوفة فقط
    data_matrix = (~df_sorted[cols].isnull()).astype(int).values
    ax.imshow(data_matrix.T, aspect="auto", cmap=cmap,
              interpolation="nearest", vmin=0, vmax=1)

    ax.set_yticks(range(len(cols)))
    ax.set_yticklabels([SOURCE_LABELS[c] for c in cols],
                       fontsize=10, color=PALETTE["text"])

    total_rows = len(df_sorted)
    tick_positions = np.linspace(0, total_rows - 1, 6).astype(int)
    years_at_ticks = df_sorted["year"].reset_index(drop=True).iloc[tick_positions].values
    ax.set_xticks(tick_positions)
    ax.set_xticklabels([str(int(y)) for y in years_at_ticks], color=PALETTE["muted"])

    for i, col in enumerate(cols):
        coverage = df_sorted[col].notna().sum() / total_rows * 100
        color = (PALETTE["accent2"] if coverage > 50
                 else PALETTE["accent5"] if coverage > 20
                 else PALETTE["accent3"])
        ax.text(total_rows * 1.01, i, f"{coverage:.0f}%",
                va="center", ha="left", fontsize=9,
                color=color, fontweight="bold")

    ax.set_xlim(0, total_rows * 1.08)
    ax.set_xlabel("Timeline  (1950 → 2024)", fontsize=10,
                  color=PALETTE["muted"], labelpad=8)

    present = mpatches.Patch(color=PALETTE["accent1"], label="Data available")
    missing = mpatches.Patch(color=PALETTE["border"],  label="Missing / NaN")
    ax.legend(handles=[present, missing], loc="lower right", framealpha=0.9)

    fig.text(0.02, 0.97, "SOURCE COVERAGE MATRIX",
             fontsize=13, fontweight="bold", color=PALETTE["text"], va="top")
    fig.text(0.02, 0.91,
             "Data availability per source across the timeline — brighter = data present",
             fontsize=9, color=PALETTE["muted"], va="top")

    plt.tight_layout(rect=[0, 0, 1, 0.88])
    plt.savefig(save_path, dpi=300, bbox_inches="tight",
                facecolor=PALETTE["bg"])
    plt.show()
    print(f"   ✅ Saved → {save_path}")

# ─────────────────────────────────────────────
#  CHART 2 — Decade Distribution Boxplot
# ─────────────────────────────────────────────
def plot_global_distribution_by_decade(df, save_path="data/processed/distribution_by_decade.png"):
    print("→ Generating Decade Distribution Chart...")

    df_valid = df.dropna(subset=["life_exp_wb"]).copy()
    df_valid["decade"] = (df_valid["year"] // 10) * 10
    decades = sorted(df_valid["decade"].unique())

    fig, ax = plt.subplots(figsize=(14, 7))
    fig.patch.set_facecolor(PALETTE["bg"])
    ax.set_facecolor(PALETTE["surface"])

    box_data = [df_valid[df_valid["decade"] == d]["life_exp_wb"].values
                for d in decades]

    bp = ax.boxplot(
        box_data,
        positions=range(len(decades)),
        widths=0.55,
        patch_artist=True,
        showfliers=True,
        medianprops=dict(color=PALETTE["accent3"], linewidth=2.5),
        whiskerprops=dict(color=PALETTE["muted"], linewidth=1.2, linestyle="--"),
        capprops=dict(color=PALETTE["muted"], linewidth=1.5),
        flierprops=dict(marker="o", markersize=3, alpha=0.4,
                        markerfacecolor=PALETTE["accent1"],
                        markeredgecolor="none"),
        boxprops=dict(linewidth=1.2),
    )

    n = len(decades)
    for i, patch in enumerate(bp["boxes"]):
        t = i / max(n - 1, 1)
        r = int(88  + (248 - 88)  * t)
        g = int(166 + (63  - 166) * t)
        b = int(255 + (80  - 255) * t)
        patch.set_facecolor(f"#{r:02x}{g:02x}{b:02x}")
        patch.set_alpha(0.75)

    global_mean = df_valid["life_exp_wb"].mean()
    ax.axhline(global_mean, color=PALETTE["accent3"], linewidth=1.5,
               linestyle=":", alpha=0.8,
               label=f"Historical mean  {global_mean:.1f} yrs")

    for i, d in enumerate(decades):
        med = np.median(df_valid[df_valid["decade"] == d]["life_exp_wb"])
        ax.text(i, med + 0.8, f"{med:.0f}", ha="center", va="bottom",
                fontsize=7.5, color=PALETTE["accent3"], fontweight="bold")

    ax.set_xticks(range(len(decades)))
    ax.set_xticklabels([str(d) + "s" for d in decades], fontsize=10)
    ax.set_ylabel("Life Expectancy  (years)", fontsize=10,
                  color=PALETTE["muted"], labelpad=8)
    ax.set_ylim(20, 95)
    ax.legend(loc="upper left", framealpha=0.9)

    for x in np.arange(-0.5, len(decades) - 0.5, 1):
        ax.axvline(x, color=PALETTE["border"], linewidth=0.5, alpha=0.6)

    fig.text(0.02, 0.97, "LIFE EXPECTANCY DISTRIBUTION BY DECADE",
             fontsize=13, fontweight="bold", color=PALETTE["text"], va="top")
    fig.text(0.02, 0.91,
             "World Bank data  ·  Boxes show IQR  ·  Whiskers = 1.5×IQR  ·  Dots = outliers",
             fontsize=9, color=PALETTE["muted"], va="top")

    plt.tight_layout(rect=[0, 0, 1, 0.88])
    plt.savefig(save_path, dpi=300, bbox_inches="tight",
                facecolor=PALETTE["bg"])
    plt.show()
    print(f"   ✅ Saved → {save_path}")

# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    apply_dark_style()
    df = load_data()

    print("=" * 60)
    print("  GLOBAL LIFE EXPECTANCY  ·  Visual Analysis")
    print("=" * 60)

    plot_missing_data_structure(df)
    plot_global_distribution_by_decade(df)

    print("\n✅ All charts saved.")