from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "nurgun",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 3, 15),
}

with DAG(
    dag_id="pipeline",
    default_args=default_args,
    schedule_interval="30 23 * * *",
    catchup=False,
    tags=["nurgun"],
) as dag:

    trigger_github_to_minio = TriggerDagRunOperator(
        task_id="trigger_github_to_minio",
        trigger_dag_id="github_to_minio",
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_yesterdays_data = TriggerDagRunOperator(
        task_id="trigger_yesterdays_data",
        trigger_dag_id="postgres_check_v2",
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_github_to_minio >> trigger_yesterdays_data
