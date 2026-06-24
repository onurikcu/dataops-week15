from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import boto3

# Sabitler
BUCKET_NAME = 'dataops-bronze'
FILE_KEY = 'raw/dirty_store_transactions.csv'
LOCAL_FILE_PATH = '/opt/airflow/dags/dirty_store_transactions.csv'

def upload_to_rustfs():
    s3 = boto3.client('s3', 
                      endpoint_url='http://rustfs:9000', 
                      aws_access_key_id='dataops', 
                      aws_secret_access_key='Ankara06')
    
    # Bucket oluştur
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
    except Exception as e:
        print(f"Bucket zaten var veya hata: {e}")

    # Dosyayı yükle
    s3.upload_file(LOCAL_FILE_PATH, BUCKET_NAME, FILE_KEY)
    print(f"Veri {BUCKET_NAME} bucket'ına {FILE_KEY} yoluyla yüklendi.")

with DAG(
    'rustfs_upload_dag',
    start_date=datetime(2026, 6, 24),
    schedule='@once',
    catchup=False
) as dag:

    upload_task = PythonOperator(
        task_id='upload_to_rustfs',
        python_callable=upload_to_rustfs
    )

    trigger_cleaning = TriggerDagRunOperator(
        task_id='trigger_cleaning_pipeline',
        trigger_dag_id='data_pipeline_dag'
    )

    upload_task >> trigger_cleaning