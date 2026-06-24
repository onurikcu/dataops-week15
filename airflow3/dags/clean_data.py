import pandas as pd
import re
from sqlalchemy import create_engine
import os

def clean_store_data(file_path):
    # Spark_client konteynerinde dosya /dataops/ altında olmalı
    if not os.path.exists(file_path):
        print(f"Hata: {file_path} dosyası bulunamadı!")
        return

    df = pd.read_csv(file_path)
    
    # STORE_LOCATION temizliği
    df['STORE_LOCATION'] = df['STORE_LOCATION'].apply(lambda x: re.sub(r'[^a-zA-Z\s]', '', str(x)).strip())
    
    # Para birimi temizliği
    currency_cols = ['MRP', 'CP', 'DISCOUNT', 'SP']
    for col in currency_cols:
        # Hata payı bırakmak için regex ile temizle
        df[col] = df[col].replace(r'[\$,]', '', regex=True).astype(float)
    
    # PostgreSQL'e Bağlan
    # NOT: 'postgres' servisi airflow konteynerinden erişilebilir ama 
    # spark_client içinden postgres'e erişmek için docker ağ ismini kullanmalısın.
    # Eğer postgres container ismin 'airflow3-postgres-1' ise host kısmına onu yazmalısın.
    db_url = 'postgresql+psycopg2://airflow:airflow@airflow3-postgres-1:5432/traindb'
    engine = create_engine(db_url)
    
    try:
        df.to_sql('clean_data_transactions', engine, if_exists='replace', index=False, schema='public')
        print("Veriler başarıyla PostgreSQL'e yazıldı.")
    except Exception as e:
        print(f"Veritabanı yazma hatası: {e}")

if __name__ == "__main__":
    # Spark_client içinde dosya /dataops/ klasöründe olmalı
    file_path = '/dataops/dirty_store_transactions.csv'
    clean_store_data(file_path)