from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

# 'default_args' içinde 'start_date' sabit kalmalı, 
# dinamik olmaması (datetime.now() gibi) önerilir.
default_args = {
    'owner': 'Sevde',
    'depends_on_past': False, # Önceki run'lara bağımlılığı kaldırmak güvenlidir
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_pipeline_dag',
    default_args=default_args,
    description='Spark veri temizleme pipeline',
    start_date=datetime(2026, 6, 24),
    schedule='@daily',
    catchup=False,
    tags=['dataops', 'assignment'],
) as dag:

    run_cleaning_job = SSHOperator(
        task_id='run_spark_cleaning',
        ssh_conn_id='ssh_default',
        command='python3 /dataops/clean_data.py', 
        get_pty=True,
        # Timeout eklemek, script takılırsa DAG'ın sonsuza kadar çalışmasını engeller
        command_timeout=600 
    )