from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

with DAG('data_pipeline_assignment', start_date=datetime(2026, 6, 24), schedule_interval=None, catchup=False) as dag:
    
    clean_task = SSHOperator(
        task_id='run_spark_script',
        ssh_conn_id='ssh_default', # Airflow Connections'ta bu ID ile tanımlayacağız
        command='python3 /opt/airflow/scripts/clean_data.py /dataops-bronze/raw/dirty_store_transactions.csv',
        dag=dag
    )