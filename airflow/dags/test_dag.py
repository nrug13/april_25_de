from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "nurgun",
    "retries": 0,
    "start_date": datetime(2026, 4, 25),
}

with DAG(
    dag_id="test_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["nurgun"],
) as dag:

    BashOperator(
        task_id="salam",
        bash_command="echo 'Salam, DAG işləyir!'",
    )
