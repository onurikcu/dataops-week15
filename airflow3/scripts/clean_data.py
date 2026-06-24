import pandas as pd
import re

def clean_store_data(file_path):
    # CSV'yi oku
    df = pd.read_csv(file_path)
    
    # 1. STORE_LOCATION sütunundaki özel karakterleri temizle
    # Regex ile sadece harf ve boşluk bırak, gerisini at
    df['STORE_LOCATION'] = df['STORE_LOCATION'].apply(lambda x: re.sub(r'[^a-zA-Z\s]', '', str(x)).strip())
    
    # 2. Ücret sütunlarından '$' işaretini kaldır ve sayıya çevir
    currency_cols = ['MRP', 'CP', 'DISCOUNT', 'SP']
    for col in currency_cols:
        df[col] = df[col].astype(str).str.replace('$', '', regex=False).astype(float)
    
    # 3. Temizlenmiş veriyi kaydet
    output_path = file_path.replace('.csv', '_cleaned.csv')
    df.to_csv(output_path, index=False)
    print(f"Veri temizlendi ve kaydedildi: {output_path}")

if __name__ == "__main__":
    clean_store_data('dirty_store_transactions.csv')