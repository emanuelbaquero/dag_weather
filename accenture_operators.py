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

class ExecutePipeline(BaseOperator):

    def __init__(
            self,
            name: str,
            pipeline: str,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.pipeline = pipeline



    def run_adf_pipeline(self, pipeline_name):
        '''Runs an Azure Data Factory pipeline using the AzureDataFactoryHook and passes in a date parameter
        '''



        # Create a dictionary with date parameter
        params = {}
        v_resource_group_name = Variable.get("resource_group_name")
        v_factory_name = Variable.get("factory_name")
        v_linked_service = Variable.get("linked_service")
        v_dataset_name = Variable.get("dataset_name")

        azure_data_factory_conn_id = 'azure_data_factory_conn'
        hook = AzureDataFactoryHook(azure_data_factory_conn_id)


        try:
            # Make connection to ADF, and run pipeline with parameter
            print('NOMBRE DEL PIPELINE: ', hook.get_factory(pipeline_name=pipeline_name,
                                                            resource_group_name=v_resource_group_name,
                                                            factory_name=v_factory_name))
            print('get_factory funciono Correctamente..')
        except:
            print('Fallo get_factory')

        try:
            if hook._factory_exists(v_resource_group_name, v_factory_name):
                print('Existe ADF')
            else:
                print('No Existe ADF')
        except:
            print('Fallo _factory_exists')

        try:
            hook.get_linked_service(v_linked_service, v_resource_group_name, v_factory_name)
            print('get_linked_service funciono Correctamente..')
        except:
            print('Fallo get_linked_service')

        try:
            if hook._linked_service_exists(v_resource_group_name, v_factory_name, v_linked_service):
                print('Existe Linked Service', v_linked_service)
            else:
                print('No Existe Linked Service', v_linked_service)
        except:
            print('Fallo _linked_service_exists')

        try:
            hook.get_dataset(v_dataset_name, v_resource_group_name, v_factory_name)
            print('get_dataset funciono Correctamente..')
        except:
            print('Fallo get_dataset')

        try:
            if hook._dataset_exists(v_resource_group_name, v_factory_name, v_dataset_name):
                print('Existe Dataset', v_dataset_name)
            else:
                print('No Existe Dataset', v_dataset_name)
        except:
            print('Fallo _dataset_exists')

        try:
            hook.get_pipeline(pipeline_name, v_resource_group_name, v_factory_name)
            print('get_pipeline funciono Correctamente..')
        except:
            print('Fallo get_dataset')

        try:
            if hook._pipeline_exists(v_resource_group_name, v_factory_name, pipeline_name):
                print('Existe Pipeline', pipeline_name)
            else:
                print('No Existe Pipeline', pipeline_name)
        except:
            print('Fallo _pipeline_exists')

        try:
            hook.run_pipeline(pipeline_name, v_resource_group_name, v_factory_name)
            print('run_pipeline funciono Correctamente..')
        except:
            print('Fallo run_pipeline')

    def execute(self, context):
        message = "Hello {}".format(self.name)
        self.run_adf_pipeline(self.pipeline)
        print(message)
        return True


