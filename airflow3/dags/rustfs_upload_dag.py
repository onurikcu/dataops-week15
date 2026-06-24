from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import boto3
import requests
import io

# Sabitler
URL = 'https://raw.githubusercontent.com/erkansirin78/datasets/refs/heads/master/dirty_store_transactions.csv'
BUCKET_NAME = 'dataops-bronze'
FILE_KEY = 'raw/dirty_store_transactions.csv'

def upload_from_url_to_rustfs():
    # 1. GitHub'dan veriyi indir
    response = requests.get(URL)
    response.raise_for_status()
    data = response.content
    
    # 2. RustFS'e yükle
    s3 = boto3.client('s3', 
                      endpoint_url='http://rustfs:9000', 
                      aws_access_key_id='dataops', 
                      aws_secret_access_key='Ankara06')
    
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
    except:
        pass

    s3.upload_fileobj(io.BytesIO(data), BUCKET_NAME, FILE_KEY)
    print("Veri GitHub'dan çekildi ve RustFS'e başarıyla yüklendi!")

with DAG(
    'rustfs_upload_dag',
    start_date=datetime(2026, 6, 24),
    schedule='@once',
    catchup=False
) as dag:

    upload_task = PythonOperator(
        task_id='upload_from_url',
        python_callable=upload_from_url_to_rustfs
    )

    trigger_cleaning = TriggerDagRunOperator(
        task_id='trigger_cleaning_pipeline',
        trigger_dag_id='data_pipeline_dag'
    )

    upload_task >> trigger_cleaning