import pandas as pd
import re
from sqlalchemy import create_engine
import os

def clean_store_data(file_path):
    # 1. Dosyanın varlığını kontrol et
    if not os.path.exists(file_path):
        print(f"Hata: {file_path} dosyası bulunamadı!")
        return

    # CSV'yi oku
    df = pd.read_csv(file_path)
    
    # 2. STORE_LOCATION temizliği
    df['STORE_LOCATION'] = df['STORE_LOCATION'].apply(lambda x: re.sub(r'[^a-zA-Z\s]', '', str(x)).strip())
    
    # 3. Para birimi sütunları temizliği
    currency_cols = ['MRP', 'CP', 'DISCOUNT', 'SP']
    for col in currency_cols:
        df[col] = df[col].astype(str).str.replace('$', '', regex=False).astype(float)
    
    # 4. PostgreSQL'e Bağlan ve Yaz
    # 'postgres' burada docker-compose'daki service adımızdır
    db_url = 'postgresql+psycopg2://airflow:airflow@postgres/traindb'
    engine = create_engine(db_url)
    
    try:
        df.to_sql('clean_data_transactions', engine, if_exists='replace', index=False, schema='public')
        print("Veriler başarıyla PostgreSQL (traindb) veritabanına yazıldı.")
    except Exception as e:
        print(f"Veritabanı yazma hatası: {e}")

if __name__ == "__main__":
    # Dosya yolu artık Airflow'un dags klasöründe olduğu için tam yolu veriyoruz
    file_path = '/opt/airflow/dags/dirty_store_transactions.csv'
    clean_store_data(file_path)