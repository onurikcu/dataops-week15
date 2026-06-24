from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_pipeline_assignment',
    default_args=default_args,
    start_date=datetime(2026, 1, 1), # Bu tarihi 2026'nın başına çektik
    schedule_interval=None,
    catchup=False
) as dag:

    t1 = BashOperator(
        task_id='test_task',
        bash_command='echo "DAG calisti!"'
    )