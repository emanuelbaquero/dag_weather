from airflow.models.baseoperator import BaseOperator
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from pendulum import yesterday
from airflow.providers.microsoft.azure.hooks.azure_data_factory import AzureDataFactoryHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
import subprocess
import pandas as pd
import json

class CheckPipelineStatus(BaseOperator):

    def __init__(
            self,
            adf: str,
            resource_group: str,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.adf = adf
        self.resource_group = resource_group



    def check_status(self, adf, resource_group):
        '''Check Status of Last Activity Runs from specific Pipeline

        subprocess.run(['az',
                        'login',
                        '-u',
                        'ebaquero@suppliers.tenaris.com',
                        '-p',
                        'Argentina123',
                        '&&',
                        'az',
                        'config',
                        'set',
                        'extension.use_dynamic_install=yes_without_prompt',
                        '&&',
                        'az',
                        'datafactory',
                        'pipeline-run',
                        'query-by-factory',
                        '--factory-name',
                        '"'+adf+'"',
                        '--last-updated-after',
                        '"2021-01-16T00:36:44.3345758Z"',
                        '--last-updated-before',
                        '"2022-06-16T00:49:48.3686473Z"',
                        '--resource-group',
                        '"'+resource_group+'"',
                        '>',
                        '/opt/airflow/logs/activity_runs.json'
                        ])'''

        print('Se ha ejecutado correctamente')


        df = pd.read_json('/opt/airflow/logs/activity_runs.json')
        print(df)

        l_runId = []
        l_runStart = []
        l_status = []

        for i in df.value:
            l_runId.append(i['runId'])
            l_runStart.append(i['runStart'])
            l_status.append(i['status'])

        df = pd.DataFrame({'runId': l_runId, 'runStart': l_runStart, 'status': l_status})

        status_str = df[df.runStart == df.runStart.max()].status.values[0]
        print(status_str=='Succeeded')

        return status_str=='Succeeded'



    def execute(self, context):
        status = self.check_status(self.adf, self.resource_group)
        return status