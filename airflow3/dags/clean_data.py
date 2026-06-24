import pandas as pd
import re
from sqlalchemy import create_engine
import boto3
import io

# Sabitler (Upload DAG ile aynı)
BUCKET_NAME = 'dataops-bronze'
FILE_KEY = 'raw/dirty_store_transactions.csv'
DB_URL = 'postgresql+psycopg2://airflow:airflow@airflow3-postgres-1:5432/traindb'

def clean_store_data():
    # 1. RustFS'ten veriyi oku
    s3 = boto3.client('s3', 
                      endpoint_url='http://rustfs:9000', 
                      aws_access_key_id='dataops', 
                      aws_secret_access_key='Ankara06')
    
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        print("Veri RustFS'ten başarıyla çekildi.")
    except Exception as e:
        print(f"RustFS okuma hatası: {e}")
        return

    # 2. Temizlik İşlemleri
    df['STORE_LOCATION'] = df['STORE_LOCATION'].apply(lambda x: re.sub(r'[^a-zA-Z\s]', '', str(x)).strip())
    
    currency_cols = ['MRP', 'CP', 'DISCOUNT', 'SP']
    for col in currency_cols:
        df[col] = df[col].replace(r'[\$,]', '', regex=True).astype(float)
    
    # 3. PostgreSQL'e Bağlan ve Yaz
    engine = create_engine(DB_URL)
    
    try:
        df.to_sql('clean_data_transactions', engine, if_exists='replace', index=False, schema='public')
        print("Başarılı: Veriler PostgreSQL'e yazıldı.")
    except Exception as e:
        print(f"Veritabanı yazma hatası: {e}")

if __name__ == "__main__":
    clean_store_data()