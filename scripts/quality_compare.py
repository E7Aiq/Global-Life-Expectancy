import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# إعداد المظهر الاحترافي
sns.set_theme(style="whitegrid", context="talk")

def plot_data_quality():
    # تحميل الملف النهائي
    df = pd.read_csv('data/processed/master_life_expectancy.csv')
    
    # الأعمدة التي تمثل المصادر
    sources = {
        'OWID (Historical)': 'life_exp_owid',
        'World Bank': 'life_exp_wb',
        'Kaggle (Health Factors)': 'life_exp_kaggle',
        'WHO (HALE)': 'hale_who',
        'UNICEF': 'life_exp_unicef',
        'CDC (US Only)': 'life_exp_us_cdc'
    }
    
    # حساب نسبة البيانات غير المفقودة (النظيفة) لكل مصدر
    quality_metrics = []
    total_rows = len(df)
    
    for name, col in sources.items():
        valid_data_count = df[col].notnull().sum()
        coverage_percent = (valid_data_count / total_rows) * 100
        quality_metrics.append({'Source': name, 'Coverage (%)': coverage_percent})
        
    df_quality = pd.DataFrame(quality_metrics).sort_values(by='Coverage (%)', ascending=False)
    
    # الرسم البياني
    plt.figure(figsize=(12, 6))
    ax = sns.barplot(x='Coverage (%)', y='Source', data=df_quality, palette='viridis')
    
    plt.title('Data Source Quality: Global Coverage (1950-2024)', fontsize=16, fontweight='bold', pad=15)
    plt.xlabel('Percentage of Non-Null Records (%)', fontsize=12)
    plt.ylabel('Data Source', fontsize=12)
    plt.xlim(0, 100)
    
    # إضافة الأرقام على الأعمدة
    for p in ax.patches:
        width = p.get_width()
        plt.text(width + 1, p.get_y() + p.get_height()/2. + 0.1, '{:1.1f}%'.format(width), ha="left", fontsize=11)
        
    plt.tight_layout()
    plt.savefig('data_quality_comparison.png', dpi=300) # سيقوم بحفظ الصورة لاستخدامها في التقرير
    plt.show()

if __name__ == "__main__":
    plot_data_quality()