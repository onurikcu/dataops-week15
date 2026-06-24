from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Sevde',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_pipeline_dag',
    default_args=default_args,
    start_date=datetime(2026, 6, 24),
    schedule='@daily', # 'schedule_interval' yerine 'schedule' kullan
    catchup=False,
    tags=['dataops', 'assignment']
) as dag:

    # SSHOperator, spark_client içindeki /dataops/clean_script.py dosyasını çalıştırır
    run_cleaning_job = SSHOperator(
        task_id='run_spark_cleaning',
        ssh_conn_id='ssh_default',
        # Dosya ismini terminalde gördüğün isimle eşle:
        command='python3 /dataops/clean_data.py', 
        get_pty=True
    )

    run_cleaning_job