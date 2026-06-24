from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

with DAG(
    'data_pipeline_dag',
    start_date=datetime(2026, 6, 24),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # SSHOperator ile spark_client konteynerinde temizleme scriptini tetikleme
    clean_data = SSHOperator(
        task_id='run_spark_cleaning',
        ssh_conn_id='ssh_default',
        command='python3 /dataops/clean_script.py', # Spark_client içinde bu yolun var olduğundan emin ol
        get_pty=True
    )

    clean_data