from datetime import datetime

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from airflow.providers.microsoft.azure.hooks.azure_data_factory import AzureDataFactoryHook
from airflow.hooks.base_hook import BaseHook

default_args = {
    'owner': 'Accenture',
    'start_date': days_ago(5)
}

def hello_world_loop():
    for palabra in ['hello', 'world']:
        print(palabra)

def chau_world_loop():
    for palabra in ['chau', 'world']:
        print(palabra)


def validity_pipeline_exists(**context):
    print('hola')



with DAG('dag_execute_adf_data_quality',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    start = DummyOperator(task_id='start')

    prueba_python_aux = PythonOperator(task_id='prueba_python',
                                   python_callable=hello_world_loop)

    prueba_python2 = PythonOperator(task_id='prueba_python2',
                                   python_callable=chau_world_loop)

    prueba_bash = BashOperator(task_id='prueba_bash',bash_command='az login -u ebaquero@suppliers.tenaris.com -p Argentina123 && az config set extension.use_dynamic_install=yes_without_prompt && az datafactory pipeline-run query-by-factory --factory-name "dftdptdldev-core01" --last-updated-after "2021-01-16T00:36:44.3345758Z" --last-updated-before "2022-06-16T00:49:48.3686473Z" --resource-group "RG-TDP-TDL-DEV" > /opt/airflow/logs/activity_runs.json')

start >> prueba_python_aux >> prueba_python2 >> prueba_bash