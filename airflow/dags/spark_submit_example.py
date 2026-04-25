from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# ── Yollar ──────────────────────────────────────────────────────────────────
SPARK_JOB_PATH = "/opt/airflow/dags/custom_scripts/spark_job.py"
INPUT_CSV      = "/opt/airflow/files/data.csv"
OUTPUT_PATH    = "/opt/airflow/files/output/spark_result"

# ── Spark connection ─────────────────────────────────────────────────────────
# Airflow UI → Admin → Connections → spark_default
#   conn_type : Spark
#   host      : spark://spark   (docker-compose servis adı)
#   port      : 7077
SPARK_CONN_ID = "spark_default"

default_args = {
    "owner": "nurgun",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "start_date": datetime(2026, 4, 25),
}


def check_input_file():
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"Input fayl tapılmadı: {INPUT_CSV}")
    print(f"Input fayl mövcuddur: {INPUT_CSV}")


def print_output():
    import glob
    files = glob.glob(f"{OUTPUT_PATH}/*.csv")
    print(f"Output faylları: {files}")
    for f in files:
        with open(f) as fh:
            print(fh.read())


with DAG(
    dag_id="spark_submit_example",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["nurgun", "spark"],
    doc_md="""
## spark_submit_example

`data.csv` faylını Spark ilə emal edir:
- `id`, `value`, `date` sütunlarından statistika hesablayır
- Nəticəni `/opt/airflow/files/output/spark_result/` qovluğuna yazır

**Lazımi Airflow Connection:**
`Admin → Connections → spark_default`
- conn_type: `Spark`
- host: `spark://spark`
- port: `7077`
""",
) as dag:

    check_file = PythonOperator(
        task_id="check_input_file",
        python_callable=check_input_file,
    )

    spark_job = SparkSubmitOperator(
        task_id="spark_submit",
        conn_id=SPARK_CONN_ID,
        application=SPARK_JOB_PATH,
        application_args=[INPUT_CSV, OUTPUT_PATH],
        name="airflow_spark_example",
        conf={
            "spark.executor.memory": "512m",
            "spark.executor.cores": "1",
            "spark.driver.memory": "512m",
        },
        verbose=True,
    )

    show_result = PythonOperator(
        task_id="show_result",
        python_callable=print_output,
    )

    check_file >> spark_job >> show_result
