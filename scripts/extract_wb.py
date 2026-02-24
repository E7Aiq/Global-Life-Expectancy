import requests
import pandas as pd
import os

def fetch_world_bank_data():
    """
    دالة للاتصال بـ API البنك الدولي، سحب بيانات العمر المتوقع، 
    وتحويلها إلى ملف CSV محلي في مجلد البيانات الخام.
    """
    url = "https://api.worldbank.org/v2/country/all/indicator/SP.DYN.LE00.IN"
    params = {
        "format": "json",
        "per_page": 20000 
    }
    
    print("🔄 جاري الاتصال بـ API البنك الدولي لسحب البيانات...")
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        records = data[1]
        parsed_data = []
        
        for item in records:
            if item["value"] is not None:
                parsed_data.append({
                    "iso3": item["countryiso3code"],
                    "country_name": item["country"]["value"],
                    "year": int(item["date"]),
                    "life_exp_wb": item["value"]
                })
        
        df = pd.DataFrame(parsed_data)
        
        # التأكد من وجود مجلد الحفظ
        os.makedirs("data/raw", exist_ok=True)
        
        # مسار حفظ الملف
        output_filename = "data/raw/world_bank_life_expectancy.csv"
        df.to_csv(output_filename, index=False)
        
        print(f"✅ تمت العملية بنجاح! تم استخراج {len(df)} صف وحفظها في المسار: '{output_filename}'")
        
    except requests.exceptions.RequestException as e:
        print(f"❌ حدث خطأ أثناء الاتصال بالـ API: {e}")
    except KeyError as e:
        print(f"❌ حدث خطأ في هيكل البيانات المستلمة: {e}")

if __name__ == "__main__":
    fetch_world_bank_data()