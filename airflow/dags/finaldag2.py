from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta


def on_failure_callback(context):
    print("dag error verdi. Context:", context)
def return_value():
    return "Hello, World!"
def check_weekend():
    today = datetime.now().weekday()
    if today >= 5:
        return False
    return True


default_args = {
    "owner": "nurgun",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2026, 1, 1),
    "email": ["nurgunganbarova@outlook.com"],
    "email_on_failure": True,
    "email_on_retry": True,
}
with DAG(
    dag_id="final_dag2",
    default_args=default_args,
    schedule_interval="00 02 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["etl", "daily"],
    on_failure_callback=on_failure_callback,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    start_pipeline = BashOperator(
        task_id="start_pipeline",
        bash_command="echo 'pipeline started'",
    )
    generate_value = PythonOperator(
        task_id="generate_value",
        python_callable=return_value,
    )
    branching_weekend = BranchPythonOperator(
        task_id="branching_weekend",
        python_callable=lambda: "data_processing" if check_weekend() else "end_print",
    )

    data_processing = BashOperator(
        task_id="data_processing",
        bash_command="echo 'data processing: {{ ds }}'",
        on_failure_callback=on_failure_callback,
    )

    print_date = BashOperator(
        task_id="print_date",
        bash_command="echo 'exec date: {{ ds }}'",
    )

    print_xcom_value = BashOperator(
        task_id="print_xcom_value",
        bash_command="echo 'deyer: {{ ti.xcom_pull(task_ids=\"generate_value\") }}'",
        on_failure_callback=on_failure_callback,
    )
    end_print = BashOperator(
        task_id="end_print",
        bash_command="echo 'pipeline finished!'",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> start_pipeline >> generate_value >> branching_weekend >> [data_processing, end_print]
    data_processing >> [print_date, print_xcom_value] >> end_print >> end
