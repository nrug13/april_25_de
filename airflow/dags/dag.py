from ast import If
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
from airflow.models.baseoperator import chain
REPO_URL = "https://github.com/ahajiyev628/airflow_dynamic_file_to_db_loader.git"
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException


def yesterday_check(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="pg_connection")

    logical_date = kwargs["logical_date"]
    yesterday = logical_date.date() - timedelta(days=1)

    sql_query = f"""
        SELECT COUNT(*)
        FROM transactions
        WHERE transaction_date = '{yesterday}'
    """

    rows = pg_hook.get_first(sql=sql_query)[0]

    kwargs["ti"].xcom_push(key="yesterday_rows", value=rows)


def branch_yesterday(**kwargs):

    rows = kwargs["ti"].xcom_pull(
        task_ids="yesterday_check",
        key="yesterday_rows"
    )

    if rows > 0:
        return "check_row_count_task"
    else:
        return "fail_no_data"


def check_row_count(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="pg_connection")

    sql_query = "SELECT COUNT(*) FROM transactions"
    row_count = pg_hook.get_first(sql=sql_query)[0]

    kwargs["ti"].xcom_push(key="row_count", value=row_count)


def branch_row_count(**kwargs):

    row_count = kwargs["ti"].xcom_pull(
        task_ids="check_row_count_task",
        key="row_count"
    )

    if row_count > 2:
        return "success"
    else:
        return "fail_low_rows"


def fail_no_data():
    raise AirflowFailException("DUNEN YOXDUR")


def fail_low_rows():
    raise AirflowFailException("COUNT <= 2")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="matrix_minio",
    default_args=default_args,
    start_date=datetime(2026, 3, 7),
    schedule=None,
    catchup=False,
    tags=["nurgun"],
) as dag:

    start = EmptyOperator(task_id="start")

    yesterday_check_task = PythonOperator(
        task_id="yesterday_check",
        python_callable=yesterday_check
    )

    branch_yesterday_task = BranchPythonOperator(
        task_id="branch_yesterday",
        python_callable=branch_yesterday
    )

    check_row_count_task = PythonOperator(
        task_id="check_row_count_task",
        python_callable=check_row_count
    )

    branch_row_count_task = BranchPythonOperator(
        task_id="branch_row_count",
        python_callable=branch_row_count
    )

    fail_no_data_task = PythonOperator(
        task_id="fail_no_data",
        python_callable=fail_no_data
    )

    fail_low_rows_task = PythonOperator(
        task_id="fail_low_rows",
        python_callable=fail_low_rows
    )

    success = EmptyOperator(task_id="success")

    end = EmptyOperator(task_id="end")

    start >> yesterday_check_task >> branch_yesterday_task

    branch_yesterday_task >> check_row_count_task
    branch_yesterday_task >> fail_no_data_task

    check_row_count_task >> branch_row_count_task

    branch_row_count_task >> success
    branch_row_count_task >> fail_low_rows_task

    [success, fail_no_data_task, fail_low_rows_task] >> end