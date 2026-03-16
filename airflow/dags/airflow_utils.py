from ast import If
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.sensors.sql import SqlSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import os
##ENVIRONMENTDEKI VARIABLENI OXU
##json_var=Variable.get('TEST_JSON',deserialize_json=True)