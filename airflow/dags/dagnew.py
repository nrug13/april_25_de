from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.sensors.sql import SqlSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule

postgres_conn_id = "pg_conn_id"
source_table = "public.transactions"
date_column= "transaction_date"

def check_row_count(**context):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    # load_date = context["ds"]
    load_date = (
        datetime.strptime(context["ds"], "%Y-%m-%d").date() - timedelta(days=1)
    )
    
    query = f"""
        SELECT COUNT(*)
        FROM {source_table}
        WHERE {date_column} = '{load_date}'
    """

    row_count = hook.get_first(query)[0]

    if row_count > 2:
        return "load_to_minio"
    return "fail_task"

def task_failure_callback(context):
    print("Task failed!")

def throw_exception():
    raise AirflowFailException("Yesterday's row count is not greater than 2.")


default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="practice_dag",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule_interval=None,
    catchup=False,
    tags=["practice"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_yesterday_data = SqlSensor(
        task_id="check_yesterday_data",
        conn_id=postgres_conn_id,
        sql=f"""
            SELECT 1
            FROM {source_table}
            WHERE {date_column} = '{{{{ macros.ds_add(ds, -1) }}}}'
            LIMIT 1
        """,
        mode="reschedule",
        poke_interval=10,
        timeout=300,
    )

    branch_on_row_count = BranchPythonOperator(
        task_id="branch_on_row_count",
        python_callable=check_row_count,
    )

    load_to_minio = BashOperator(
        task_id="load_to_minio",
        bash_command=f"""echo 'Hello'"""
    )

    fail_task = PythonOperator(
        task_id="fail_task",
        python_callable=throw_exception,
        on_failure_callback=task_failure_callback,
    )

    trigger_next_dag = TriggerDagRunOperator(
        task_id="end",
        trigger_dag_id="dag_etl",
        trigger_rule=TriggerRule.ALL_DONE
    )

    start >> check_yesterday_data >> branch_on_row_count
    branch_on_row_count >> [load_to_minio, fail_task] >> trigger_next_dag