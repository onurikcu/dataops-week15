import pandas as pd
import sys
from sqlalchemy import create_engine

def clean_and_load(input_path, db_url):
    # 1. Veriyi Oku
    df = pd.read_csv(input_path)
    
    # 2. Temizlik (Missing values, duplicates)
    df = df.drop_duplicates()
    df = df.fillna(0) # Eksik değerleri 0 ile doldur
    
    # 3. Yükleme (Postgres)
    engine = create_engine(db_url)
    df.to_sql('clean_data_transactions', engine, if_exists='replace', index=False)
    print("İşlem başarıyla tamamlandı: 500 satır yüklendi.")

if __name__ == "__main__":
    # Parametreleri al
    path = sys.argv[1]
    db = "postgresql://airflow:airflow@postgres/airflow" # Host'u 'postgres' olarak bıraktık
    clean_and_load(path, db)