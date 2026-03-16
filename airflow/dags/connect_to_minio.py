from airflow_utils import *

def get_link():
    repo_links=Variable.get("LINKS",deserialize_json=True)
    return repo_links

default_args = {

    "owner": "nurgun",

    # Task fail olsa neçə dəfə retry etsin
    "retries": 0,

    # Retry olarsa nə qədər gözləsin
    "retry_delay": timedelta(minutes=5),

    # Scheduler hansı tarixdən başlasın
    "start_date": datetime(2026, 3, 15),
}
# DAG yaradılır
with DAG(

    dag_id="connect_minio",

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
    repo_links=get_link();
    
    minio_set_alias = BashOperator(
    task_id='connect_to_minio',
    bash_command=(
        "mc alias set myminio http://minio:9000 "
        "$MINIO_ROOT_USER $MINIO_ROOT_PASSWORD && "
        "echo 'success'"
    )
)
    
    # Pipeline end
    end = EmptyOperator(
        task_id="end"
    )


    # Task flow
    start >> minio_set_alias>> end