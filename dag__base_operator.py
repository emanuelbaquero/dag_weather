import json
import requests
import datetime
import hashlib
import hmac
import base64
import os
from airflow.models import DAG
from datetime import timedelta
from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential
from random import randint
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from accenture_operators import ExecutePipeline, CheckPipelineStatus



default_args = {
    'owner': 'Accenture',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),

}


with DAG('dag__base_operator',
         default_args=default_args,
         schedule_interval=timedelta(minutes=45),
         catchup=False) as dag:

    start = DummyOperator(task_id='start')

    t_HelloOperator = ExecutePipeline(task_id="t_HelloOperator", name='Emanuel', pipeline='prueba_pipeline')

    actualizar_activity_runs = BashOperator(task_id='actualizar_activity_runs', bash_command='az login -u ebaquero@suppliers.tenaris.com -p Argentina123 && az config set extension.use_dynamic_install=yes_without_prompt && az datafactory pipeline-run query-by-factory --factory-name "dftdptdldev-core01" --last-updated-after "2021-01-16T00:36:44.3345758Z" --last-updated-before "2022-06-16T00:49:48.3686473Z" --resource-group "RG-TDP-TDL-DEV" > /opt/airflow/logs/activity_runs.json')

    t_CheckPipelineStatus = CheckPipelineStatus(task_id='t_CheckPipelineStatus', adf='dftdptdldev-core01', resource_group='RG-TDP-TDL-DEV')

    end = BashOperator(task_id='end',bash_command='echo prueba_bash')

    start >> t_HelloOperator >> actualizar_activity_runs >> t_CheckPipelineStatus >> end



