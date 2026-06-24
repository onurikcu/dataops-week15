import pandas as pd
import re
from sqlalchemy import create_engine
import os
import boto3
import io

def clean_store_data():
    # 1. RustFS'ten veriyi oku
    s3 = boto3.client('s3', 
                      endpoint_url='http://rustfs:9000', 
                      aws_access_key_id='dataops', 
                      aws_secret_access_key='Ankara06')
    
    try:
        obj = s3.get_object(Bucket='dataops-bronze', Key='raw/dirty_store_transactions.csv')
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
    
    # 3. PostgreSQL'e Bağlan
    # Host isminin 'airflow3-postgres-1' olduğundan eminsin
    db_url = 'postgresql+psycopg2://airflow:airflow@airflow3-postgres-1:5432/traindb'
    engine = create_engine(db_url)
    
    try:
        df.to_sql('clean_data_transactions', engine, if_exists='replace', index=False, schema='public')
        print("Başarılı: Veriler PostgreSQL'e yazıldı.")
    except Exception as e:
        print(f"Veritabanı yazma hatası: {e}")

if __name__ == "__main__":
    clean_store_data()