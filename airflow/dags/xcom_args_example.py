from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "nurgun",
    "retries": 0,
    "start_date": datetime(2026, 4, 25),
}

# ------- Python funksiyalar -------

def extract_data(source, **context):
    """op_kwargs nümunəsi: source parametri op_kwargs-dan gəlir"""
    print(f"Məlumat çəkilir: {source}")
    data = {"records": 42, "source": source, "date": context["ds"]}

    # XCom-a yazırıq — digər task-lar oxuya bilsin
    context["ti"].xcom_push(key="raw_data", value=data)
    return data  # return da avtomatik XCom-a yazır (key="return_value")


def transform_data(multiplier, **context):
    """op_args nümunəsi + XCom oxuma"""
    ti = context["ti"]

    # Əvvəlki task-ın XCom-undan oxuyuruq
    raw = ti.xcom_pull(task_ids="extract", key="raw_data")
    print(f"Gələn data: {raw}")

    transformed = {
        "records": raw["records"] * multiplier,
        "source": raw["source"],
        "date": raw["date"],
    }
    return transformed  # avtomatik xcom_push(key="return_value")


def load_data(**context):
    """XCom-dan son nəticəni oxuyub yükləyir"""
    ti = context["ti"]

    # return_value ilə saxlanılan XCom-u oxuyuruq
    result = ti.xcom_pull(task_ids="transform", key="return_value")
    print(f"Yüklənəcək data: {result}")
    print(f"Ümumi record sayı: {result['records']}")
    print("Yükləmə tamamlandı.")


def decide_branch(threshold, **context):
    """op_args + XCom + Şərt nümunəsi"""
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="transform", key="return_value")

    if result["records"] > threshold:
        print(f"Record sayı {result['records']} > {threshold} — yükləmə başlayır")
        return "load"
    else:
        print(f"Record sayı {result['records']} <= {threshold} — skip")
        return "skip"


def skip_task():
    print("Record sayı az olduğu üçün yükləmə skip edildi.")


# ------- DAG -------

with DAG(
    dag_id="xcom_args_example",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["nurgun"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
        op_kwargs={"source": "postgres"},   # funksiyaya keyword arg göndəririk
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        op_kwargs={"multiplier": 2},        # records * 2 edəcək
    )

    branch = PythonOperator(
        task_id="branch",
        python_callable=decide_branch,
        op_kwargs={"threshold": 50},        # 50-dən böyükdürsə load et
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_data,
    )

    skip = PythonOperator(
        task_id="skip",
        python_callable=skip_task,
    )

    extract >> transform >> branch >> [load, skip]
