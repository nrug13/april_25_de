from airflow_utils import *

def get_link():
    repo_links=Variable.get("LINKS",deserialize_json=True)
    return repo_links
local_clone_path = "/opt/airflow/files/repos"



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

    dag_id="cloning",

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
    git_clone = BashOperator(
        task_id="git_clone",
        bash_command=f"mkdir -p {local_clone_path} && " +
                     " && ".join([f"git clone {repo} {local_clone_path}/{repo.split('/')[-1].replace('.git','')}" for repo in repo_links])
    )
    
    # Pipeline end
    end = EmptyOperator(
        task_id="end"
    )


    # Task flow
    start >> git_clone>> end