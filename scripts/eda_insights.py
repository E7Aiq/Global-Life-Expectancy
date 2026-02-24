import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="whitegrid")

def load_data():
    return pd.read_csv('data/processed/master_life_expectancy.csv')

def insight_1_the_health_gap(df):
    """
    الرؤية الأولى: "وهم طول العمر"
    نقارن بين أطول الدول عمراً (حسب البنك الدولي) مقابل العمر الصحي الفعلي (حسب منظمة الصحة)
    """
    print("🎨 Generating Insight 1: The Health Gap...")
    # نأخذ بيانات عام 2019 (لأنها الأكثر اكتمالاً قبل كورونا)
    df_2019 = df[(df['year'] == 2019)].dropna(subset=['life_exp_wb', 'hale_who']).copy()
    
    # نختار أعلى 15 دولة في العمر المتوقع
    top_15_longest_living = df_2019.nlargest(15, 'life_exp_wb')
    
    # نحسب الفجوة (سنوات المرض/العجز)
    top_15_longest_living['Years in Poor Health'] = top_15_longest_living['life_exp_wb'] - top_15_longest_living['hale_who']
    
    # نرتبهم حسب الفجوة لنرى من يخدعنا بأرقامه
    top_15_longest_living = top_15_longest_living.sort_values(by='Years in Poor Health', ascending=True)
    
    plt.figure(figsize=(14, 8))
    
    # رسم شريطي مزدوج (متداخل)
    sns.barplot(x='life_exp_wb', y='country_name', data=top_15_longest_living, color='lightcoral', label='Total Life Expectancy (World Bank)')
    sns.barplot(x='hale_who', y='country_name', data=top_15_longest_living, color='darkred', label='Healthy Life Expectancy (WHO)')
    
    plt.title('The Illusion of Health: Top 15 Longest-Living Countries (2019)', fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('Age (Years)', fontsize=12)
    plt.ylabel('Country', fontsize=12)
    plt.legend(loc='lower right', frameon=True, shadow=True)
    
    plt.tight_layout()
    plt.savefig('insight_health_gap.png', dpi=300)
    plt.show()

def insight_2_the_conflict_heatmap(df):
    """
    الرؤية الثانية: "خريطة النزاع الإحصائي"
    كيف تختلف أرقام المنظمات الثلاث (البنك الدولي، OWID، وكاجل) لنفس الدول!
    """
    print("🎨 Generating Insight 2: The Conflict Heatmap...")
    df_recent = df[df['year'] == 2020].dropna(subset=['life_exp_wb', 'life_exp_owid', 'life_exp_kaggle']).copy()
    
    # حساب الفروقات المطلقة بين المنظمات لمعرفة أين يكمن التضارب
    df_recent['Diff (WB vs OWID)'] = abs(df_recent['life_exp_wb'] - df_recent['life_exp_owid'])
    df_recent['Diff (WB vs Kaggle)'] = abs(df_recent['life_exp_wb'] - df_recent['life_exp_kaggle'])
    
    # اختيار أكثر 20 دولة عليها اختلاف بين المنظمات
    df_recent['Total_Conflict'] = df_recent['Diff (WB vs OWID)'] + df_recent['Diff (WB vs Kaggle)']
    top_conflicts = df_recent.nlargest(20, 'Total_Conflict').set_index('country_name')
    
    heatmap_data = top_conflicts[['Diff (WB vs OWID)', 'Diff (WB vs Kaggle)']]
    
    plt.figure(figsize=(10, 8))
    sns.heatmap(heatmap_data, annot=True, cmap='YlOrRd', fmt=".1f", linewidths=.5)
    
    plt.title('Data Conflict: Top 20 Countries with Disputed Life Expectancy (2020)', fontsize=15, fontweight='bold', pad=15)
    plt.ylabel('Country', fontsize=12)
    plt.xlabel('Difference in Years Between Sources', fontsize=12)
    
    plt.tight_layout()
    plt.savefig('insight_data_conflict.png', dpi=300)
    plt.show()

if __name__ == "__main__":
    df = load_data()
    insight_1_the_health_gap(df)
    insight_2_the_conflict_heatmap(df)