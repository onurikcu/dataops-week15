from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='data_pipeline_assignment',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    # Python scriptini çalıştıran görev
    clean_task = BashOperator(
        task_id='run_cleaning_script',
        bash_command='python3 /opt/airflow/scripts/clean_data.py'
    )