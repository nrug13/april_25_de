from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import os

FILE_PATH = "/opt/airflow/files/data.csv"

default_args = {
    "owner": "nurgun",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 3, 15),
}

def check_file():
    if os.path.exists(FILE_PATH):
        return "file_found"
    return "file_missing"

with DAG(
    dag_id="file_sensor_example",
    default_args=default_args,
    schedule_interval="30 23 * * *",
    catchup=False,
    tags=["nurgun"],
) as dag:

    # Fayl gəlməsini gözlə (soft_fail=True — tapılmasa fail deyil, skipped olur)
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=FILE_PATH,
        poke_interval=10,
        timeout=60,
        soft_fail=True,
        mode="poke",
    )

    # Fayl varmı yoxsa yoxmu?
    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=check_file,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    file_found = EmptyOperator(task_id="file_found")

    file_missing = EmptyOperator(task_id="file_missing")

    # Fayl yoxdursa random CSV generate et
    generate_csv = BashOperator(
        task_id="generate_csv",
        bash_command=(
            f"python3 -c \""
            "import csv, random, os; "
            f"os.makedirs('/opt/airflow/files', exist_ok=True); "
            f"f=open('{FILE_PATH}','w',newline=''); "
            "w=csv.writer(f); "
            "w.writerow(['id','value','date']); "
            "[w.writerow([i, round(random.uniform(1,100),2), '2026-04-25']) for i in range(1,11)]; "
            "f.close(); "
            "print('CSV generated')"
            "\""
        ),
    )

    process_file = BashOperator(
        task_id="process_file",
        bash_command=f"echo 'Fayl emal edilir:' && cat {FILE_PATH}",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    done = EmptyOperator(task_id="done")

    wait_for_file >> branch
    branch >> file_found >> process_file
    branch >> file_missing >> generate_csv >> process_file
    process_file >> done
