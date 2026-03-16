# airflow_utils.py içində yazdığın importların hamısını buradan gətiririk
from airflow_utils import *
from helpers import *

# DAG üçün default parametrlər
default_args = {

    "owner": "nurgun",

    # Task fail olsa neçə dəfə retry etsin
    "retries": 0,

    # Retry olarsa nə qədər gözləsin
    "retry_delay": timedelta(minutes=5),

    # Scheduler hansı tarixdən başlasın
    "start_date": datetime(2026, 3, 15),
}
def read_var():
    js=Variable.get("TEST_JSON",deserialize_json=True)
    return js;

# DAG yaradılır
with DAG(

    dag_id="tasklar",

    default_args=default_args,

    # hər gün saat 23:30
    schedule_interval="30 23 * * *",

    # keçmiş tarixləri run etmə
    catchup=False,

    tags=["nurgun"]

) as dag:


    # Pipeline start
    start = EmptyOperator(
        task_id="start"
    )


    # Python task
    calculate = PythonOperator(
        task_id="calc",

        # funksiyanın adı verilir
        python_callable=toplama,

        # funksiyaya göndərilən arqumentlər
        ##op_args=[4,5]
        op_kwargs={"a":5,"b":20}
    )

    # Bash command task
    print_message = BashOperator(
        task_id="print_message",
        bash_command="echo Hello Nurgun"
    )
    print_variable=PythonOperator(
        task_id='json_read',
        python_callable=read_var
       
        )


    # Pipeline end
    end = EmptyOperator(
        task_id="end"
    )


    # Task flow
    start >> print_message >> calculate >>print_variable>> end