import pandas as pd

def check_extreme_lows():
    print("\n" + "="*60)
    print(" 🚨 INVESTIGATING EXTREME LOW LIFE EXPECTANCY (< 13 YEARS)")
    print("="*60)

    # تحميل البيانات
    df = pd.read_csv("data/processed/master_life_expectancy.csv")
    df = df[df["year"].between(1950, 2024)]

    # البحث عن الصفوف التي يقل فيها العمر عن 13 في البنك الدولي أو OWID
    extreme_lows = df[(df['life_exp_wb'] < 13) | (df['life_exp_owid'] < 13)].copy()

    columns_to_show = ['iso3', 'country_name', 'year', 'life_exp_wb', 'life_exp_owid']

    if len(extreme_lows) > 0:
        print(f"Found {len(extreme_lows)} rows. Here is the list:\n")
        # ترتيبها حسب الدولة ثم السنة عشان نقرأ القصة بوضوح
        print(extreme_lows[columns_to_show].sort_values(by=['iso3', 'year']).to_string(index=False))
    else:
        print("No rows found under 13 years.")
        
    print("\n" + "="*60)

if __name__ == "__main__":
    check_extreme_lows()