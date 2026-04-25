from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta

REPO_LINKS = Variable.get("repo", default_var=[], deserialize_json=True)

LOCAL_CLONE_PATH = "/opt/airflow/files/repos"
MINIO_URL = "http://minio:9000"
BUCKET = "bronze/repos"
ALIAS_NAME = "myminio"

default_args = {
    "owner": "nurgun",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 3, 15),
}

with DAG(
    dag_id="github_to_minio",
    default_args=default_args,
    schedule_interval="30 23 * * *",
    catchup=False,
    tags=["nurgun"]
) as dag:

    minio_alias = BashOperator(
        task_id="minio_set_alias",
        bash_command=f"mc alias set {ALIAS_NAME} {MINIO_URL} $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD",
    )

    git_clone = BashOperator(
        task_id="git_clone",
        bash_command=(
            f"mkdir -p {LOCAL_CLONE_PATH} && "
            + " && ".join([
                f"rm -rf {LOCAL_CLONE_PATH}/{repo.split('/')[-1].replace('.git','')} && "
                f"git clone {repo} {LOCAL_CLONE_PATH}/{repo.split('/')[-1].replace('.git','')}"
                for repo in REPO_LINKS
            ])
        ),
    )

    minio_upload = BashOperator(
        task_id="minio_upload",
        bash_command=" && ".join([
            f"mc mirror --overwrite {LOCAL_CLONE_PATH}/{repo.split('/')[-1].replace('.git','')} "
            f"{ALIAS_NAME}/{BUCKET}/{repo.split('/')[-1].replace('.git','')}"
            for repo in REPO_LINKS
        ]),
    )

    minio_alias >> git_clone >> minio_upload
