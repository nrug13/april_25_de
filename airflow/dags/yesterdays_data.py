from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta

default_args = {
    "owner": "nurgun",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 3, 15),
}

def check_data_arrival(table_name, date_column, **context):
    yesterday = context["ds"]

    hook = PostgresHook(postgres_conn_id="postgres_mydb")
    sql = f"SELECT COUNT(*) FROM {table_name} WHERE {date_column} = '{yesterday}';"
    result = hook.get_first(sql)
    row_count = result[0] if result else 0

    if row_count == 0:
        raise AirflowException(f"{table_name}: {yesterday} tarixli data yoxdur")

    if row_count < 2:
        raise AirflowException(f"{table_name}: row count {row_count} < 2")

    print(f"OK: {table_name} cədvəlində {yesterday} üçün {row_count} sətir var")

with DAG(
    dag_id="postgres_check_v2",
    default_args=default_args,
    schedule_interval="30 23 * * *",
    catchup=False,
    tags=["nurgun"]
) as dag:

    check_task = PythonOperator(
        task_id="check_source",
        python_callable=check_data_arrival,
        op_kwargs={
            "table_name": "transactions",
            "date_column": "transaction_date",
        }
    )
