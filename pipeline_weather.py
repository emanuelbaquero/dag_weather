from airflow.models import DAG
from airflow.utils import dates
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.bash_operator  import BashOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from pymongo import MongoClient
import json
from os import listdir
from airflow.models import Variable
import boto3
import subprocess, os
import pandasql as ps
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator


PARENT_DAG_NAME = 'PipelineWeatherETL'
TASK_ID_SUBDAG_COMPLETO = 'subdag_en_el_dag_completo'
TASK_ID_SUBDAG_TEST_SIN_PROCESAR = 'subdag_en_el_dag_sin_procesar_test'
CHILD_DAG_NAME_COMPLETO = PARENT_DAG_NAME+'.'+TASK_ID_SUBDAG_COMPLETO
CHILD_DAG_NAME_TEST_SIN_PROCESAR = PARENT_DAG_NAME+'.'+TASK_ID_SUBDAG_TEST_SIN_PROCESAR


default_args = {
	'owner':'airfow',
	'start_date':dates.days_ago(1),
	'retries': 5,
	'retry_delay': timedelta(seconds=5),
	'schedule_interval':'@daily'
}

def get_dataframe_api(año, mes, dias):
	response_bs_as = requests.get("https://www.metaweather.com/api/location/search/?query=Buenos Aires")
	response_brasilia = requests.get("https://www.metaweather.com/api/location/search/?query=Brasília")
	response_santiago = requests.get("https://www.metaweather.com/api/location/search/?query=Santiago")

	id_BuenosAires = int(pd.DataFrame(response_bs_as.json()).woeid)
	id_Brasilia = int(pd.DataFrame(response_brasilia.json()).woeid)
	id_Santiago = int(pd.DataFrame(response_santiago.json()).woeid)

	for i in range(1, dias):
		try:
			STR_GET = "https://www.metaweather.com/api/location/{}/{}/{}/{}/".format(id_BuenosAires, año, mes, i)
			response = requests.get(STR_GET)
			#Pruebo los datos
			response.json()
		except:
			continue

		if i == 1:
			df_bsas = pd.DataFrame(response.json())
		else:
			df_bsas_aux = pd.DataFrame(response.json())
			df_bsas = pd.concat([df_bsas, df_bsas_aux])
	df_bsas['region'] = 'Buenos Aires'

	for i in range(1, dias):
		try:
			STR_GET = "https://www.metaweather.com/api/location/{}/{}/{}/{}/".format(id_Brasilia, año, mes, i)
			response = requests.get(STR_GET)
			response.json()
		except:
			continue
		if i == 1:
			df_brasilia = pd.DataFrame(response.json())
		else:
			df_brasilia_aux = pd.DataFrame(response.json())
			df_brasilia = pd.concat([df_brasilia, df_brasilia_aux])
	df_brasilia['region'] = 'Brasilia'

	for i in range(1, dias):
		try:
			STR_GET = "https://www.metaweather.com/api/location/{}/{}/{}/{}/".format(id_Santiago, año, mes, i)
			response = requests.get(STR_GET)
			response.json()
		except:
			continue

		if i == 1:
			df_santiago = pd.DataFrame(response.json())
		else:
			df_santiago_aux = pd.DataFrame(response.json())
			df_santiago = pd.concat([df_santiago, df_santiago_aux])

	df_santiago['region'] = 'Santiago'

	df = pd.concat([df_bsas, df_brasilia, df_santiago])

	return df



def get_api_weather(**kwargs):

	mes = str(kwargs['mes'])
	año = str(kwargs['año'])
	nombre = kwargs['nombre_archivo']
	print(nombre)

	df = get_dataframe_api(año, mes, 31)
	df.to_csv('/opt/airflow/logs/data_weather/'+nombre+'.csv',sep='|')
	print(df)

	return nombre



def guardar_datos_s3_aws(**kwargs):


	contador = 0
	for i in listdir('/opt/airflow/logs/data_weather/'):
		if ('csv' in i):

			if contador == 0:
				df = pd.read_csv('/opt/airflow/logs/data_weather/' + i, sep='|')
			else:
				df_temp = pd.read_csv('/opt/airflow/logs/data_weather/' + i, sep='|')
				df = pd.concat([df, df_temp], axis=0)

		contador = contador + 1

	df.to_csv('/opt/airflow/logs/data_weather/data_st_regiones.csv',sep='|')

	s3 = boto3.resource('s3',
						aws_access_key_id=Variable.get('aws_access_key_id'),
						aws_secret_access_key=Variable.get('aws_secret_access_key')
						)

	try:
		s3.Object('emas3bucket', 'csv/data_st_regiones.csv').delete()
	except:
		pass

	s3.meta.client.upload_file('/opt/airflow/logs/data_weather/data_st_regiones.csv', 'emas3bucket', 'csv/data_st_regiones.csv')

	return 'OK - Datos Cargados en DataLake Correctamente'



def getDataFrame():

	s3 = boto3.resource('s3',
						aws_access_key_id = Variable.get('aws_access_key_id'),
						aws_secret_access_key = Variable.get('aws_secret_access_key')
						)

	s3.meta.client.download_file('emas3bucket', 'csv/data_st_regiones.csv', 'data_st_regiones.csv')
	df = pd.read_csv('data_st_regiones.csv', sep='|')
	subprocess.run(["rm", "data_st_regiones.csv"])

	return df


def metrics_into_mongo():

#What_Is_The_Warmest_City_Region
	client = MongoClient(Variable.get('mongo_db_secret_access_key'))
	db = client.get_database('test')
	data = db.What_Is_The_Warmest_City_Region
	data.drop()
	print('Conectado a Mongo Correctamente')
	df = getDataFrame()


	df.the_temp = df.the_temp.astype(float)
	Query_What_Is_The_Warmest_City_Region = """


            SELECT region, avg(the_temp) AS the_temp_mean 
              FROM df 
          GROUP BY region  
          ORDER BY  the_temp_mean DESC


        """

	What_Is_The_Warmest_City_Region = ps.sqldf(Query_What_Is_The_Warmest_City_Region, locals())

	records = json.loads(What_Is_The_Warmest_City_Region.to_json()).values()
	data.insert(records)


#What_Are_The_Two_Drier_City_Region

	client = MongoClient(Variable.get('mongo_db_secret_access_key'))
	db = client.get_database('test')
	data = db.What_Are_The_Two_Drier_City_Region
	data.drop()


	Query_What_Are_The_Two_Drier_City_Region = """


            SELECT region, avg(humidity) AS humidity_mean 
              FROM df 
          GROUP BY region 
          ORDER BY humidity_mean


        """

	What_Are_The_Two_Drier_City_Region = ps.sqldf(Query_What_Are_The_Two_Drier_City_Region, locals())

	records = json.loads(What_Are_The_Two_Drier_City_Region.to_json()).values()
	data.insert(records)


#What_Is_The_Hottes_Day_For_Each_Month
	client = MongoClient(Variable.get('mongo_db_secret_access_key'))
	db = client.get_database('test')
	data = db.What_Is_The_Hottes_Day_For_Each_Month
	data.drop()

	df['año'] = df.created.str.split(pat='-').apply(lambda x: x[0]).astype(int)
	df['mes'] = df.created.str.split(pat='-').apply(lambda x: x[1]).astype(int)
	df['dia'] = df.created.str.split(pat='-').apply(lambda x: x[2]).apply(lambda x: x[0:2]).astype(int)

	dia_region_1 = []
	dia_region_2 = []
	dia_region_3 = []

	for i in [1,2,3]:

		query_buenos_aires = """  SELECT DISTINCT d2.region, d2.mes, d1.dia, d2.max_the_temp as the_temp 
										 FROM df d1 
										 JOIN  (SELECT region, mes, max(the_temp) as max_the_temp 
												  FROM df 
												 WHERE region = 'Buenos Aires' 
											  GROUP BY mes, region) d2 
										   ON d1.region = d2.region AND d1.mes=d2.mes AND d1.the_temp = d2.max_the_temp 
										WHERE d2.mes=""" + str(i)

		query_santiago = """      SELECT DISTINCT d2.region, d2.mes, d1.dia, d2.max_the_temp as the_temp 
										 FROM df d1 
										 JOIN  (SELECT region, mes, max(the_temp) as max_the_temp 
												  FROM df 
												 WHERE region = 'Santiago' 
											  GROUP BY mes, region) d2 
										   ON d1.region = d2.region AND d1.mes=d2.mes AND d1.the_temp = d2.max_the_temp 
										WHERE d2.mes=""" + str(i)

		query_brasilia = """      SELECT DISTINCT d2.region, d2.mes, d1.dia, d2.max_the_temp as the_temp 
										 FROM df d1 
										 JOIN  (SELECT region, mes, max(the_temp) as max_the_temp 
												  FROM df 
												 WHERE region = 'Brasilia' 
											  GROUP BY mes, region) d2 
										   ON d1.region = d2.region AND d1.mes=d2.mes AND d1.the_temp = d2.max_the_temp 
										WHERE d2.mes=""" + str(i)


		dia_region_1.append(ps.sqldf(query_buenos_aires, locals()))
		dia_region_2.append(ps.sqldf(query_santiago, locals()))
		dia_region_3.append(ps.sqldf(query_brasilia, locals()))


	df_regiones = pd.concat([dia_region_1[0], dia_region_1[1], dia_region_1[2], dia_region_2[0], dia_region_2[1], dia_region_2[2],dia_region_3[0], dia_region_3[1], dia_region_3[2]], axis=0)
	df_regiones = df_regiones.reset_index().iloc[:,1:]

	records = json.loads(df_regiones.to_json()).values()
	data.insert(records)


	return 'ok'


def test_no_repetir_carga_archivos(**context):

	print('entrar conodicion carpeta')
	if (os.path.isdir("/opt/airflow/logs/data_weather")):
		print('subdag_process_test_sin_procesar')
		return 'subdag_en_el_dag_sin_procesar_test'

	else:
		print('subdag_process_complete')
		return 'subdag_en_el_dag_completo'






def load_subdag_process_complete(param_PARENT_DAG_NAME, param_CHILD_DAG_NAME_COMPLETO, param_default_args):

	with DAG( param_CHILD_DAG_NAME_COMPLETO,
				default_args=param_default_args,
			  	catchup=True) as subdag:

		# TASK 1
		start = BashOperator(task_id='start', bash_command='echo start')

		proceso_ingesta = DummyOperator(task_id='proceso_ingesta', trigger_rule='all_done')
		proceso_ingesta_completado = DummyOperator(task_id='proceso_ingesta_completado', trigger_rule='all_done')

		crear_carpeta_temporal = BashOperator(task_id='crear_carpeta_temporal',
											  bash_command='mkdir /opt/airflow/logs/data_weather/')

		## 2010
		l_datos_2010 = []
		for mes in range(1, 13):
			datos_2010_mes = PythonOperator(task_id=f'datos_2010_{str(mes)}',
											python_callable=get_api_weather,
											op_kwargs={'año': 2010, 'mes': mes, 'nombre_archivo': '{{ task.task_id }}'},
											do_xcom_push=True,
											provide_context=True)
			l_datos_2010.append(datos_2010_mes)

		## 2011
		l_datos_2011 = []
		for mes in range(1, 13):
			datos_2011_mes = PythonOperator(task_id=f'datos_2011_{str(mes)}',
											python_callable=get_api_weather,
											op_kwargs={'año': 2011, 'mes': mes, 'nombre_archivo': '{{ task.task_id }}'},
											do_xcom_push=True,
											provide_context=True)
			l_datos_2011.append(datos_2011_mes)

		## 2012
		l_datos_2012 = []
		for mes in range(1, 13):
			datos_2012_mes = PythonOperator(task_id=f'datos_2012_{str(mes)}',
											python_callable=get_api_weather,
											op_kwargs={'año': 2012, 'mes': mes, 'nombre_archivo': '{{ task.task_id }}'},
											do_xcom_push=True,
											provide_context=True)
			l_datos_2012.append(datos_2012_mes)

		## 2013
		l_datos_2013 = []
		for mes in range(1, 13):
			datos_2013_mes = PythonOperator(task_id=f'datos_2013_{str(mes)}',
											python_callable=get_api_weather,
											op_kwargs={'año': 2013, 'mes': mes, 'nombre_archivo': '{{ task.task_id }}'},
											do_xcom_push=True,
											provide_context=True)
			l_datos_2013.append(datos_2013_mes)

		## 2014
		l_datos_2014 = []
		for mes in range(1, 13):
			datos_2014_mes = PythonOperator(task_id=f'datos_2014_{str(mes)}',
											python_callable=get_api_weather,
											op_kwargs={'año': 2014, 'mes': mes, 'nombre_archivo': '{{ task.task_id }}'},
											do_xcom_push=True,
											provide_context=True)
			l_datos_2014.append(datos_2014_mes)

		## 2015
		l_datos_2015 = []
		for mes in range(1, 13):
			datos_2015_mes = PythonOperator(task_id=f'datos_2015_{str(mes)}',
											python_callable=get_api_weather,
											op_kwargs={'año': 2015, 'mes': mes, 'nombre_archivo': '{{ task.task_id }}'},
											do_xcom_push=True,
											provide_context=True)
			l_datos_2015.append(datos_2015_mes)

		## 2016
		l_datos_2016 = []
		for mes in range(1, 13):
			datos_2016_mes = PythonOperator(task_id=f'datos_2016_{str(mes)}',
											python_callable=get_api_weather,
											op_kwargs={'año': 2016, 'mes': mes, 'nombre_archivo': '{{ task.task_id }}'},
											do_xcom_push=True,
											provide_context=True)
			l_datos_2016.append(datos_2016_mes)

		## 2017
		l_datos_2017 = []
		for mes in range(1, 13):
			datos_2017_mes = PythonOperator(task_id=f'datos_2017_{str(mes)}',
											python_callable=get_api_weather,
											op_kwargs={'año': 2017, 'mes': mes, 'nombre_archivo': '{{ task.task_id }}'},
											do_xcom_push=True,
											provide_context=True)
			l_datos_2017.append(datos_2017_mes)

		## 2018
		l_datos_2018 = []
		for mes in range(1, 13):
			datos_2018_mes = PythonOperator(task_id=f'datos_2018_{str(mes)}',
											python_callable=get_api_weather,
											op_kwargs={'año': 2018, 'mes': mes, 'nombre_archivo': '{{ task.task_id }}'},
											do_xcom_push=True,
											provide_context=True)
			l_datos_2018.append(datos_2018_mes)

		## 2019
		l_datos_2019 = []
		for mes in range(1, 13):
			datos_2019_mes = PythonOperator(task_id=f'datos_2019_{str(mes)}',
											python_callable=get_api_weather,
											op_kwargs={'año': 2019, 'mes': mes, 'nombre_archivo': '{{ task.task_id }}'},
											do_xcom_push=True,
											provide_context=True)
			l_datos_2019.append(datos_2019_mes)

		## 2020
		l_datos_2020 = []
		for mes in range(1, 13):
			datos_2020_mes = PythonOperator(task_id=f'datos_2020_{str(mes)}',
											python_callable=get_api_weather,
											op_kwargs={'año': 2020, 'mes': mes, 'nombre_archivo': '{{ task.task_id }}'},
											do_xcom_push=True,
											provide_context=True)
			l_datos_2020.append(datos_2020_mes)

		## 2021
		l_datos_2021 = []
		for mes in range(1, 13):
			datos_2021_mes = PythonOperator(task_id=f'datos_2021_{str(mes)}',
											python_callable=get_api_weather,
											op_kwargs={'año': 2021, 'mes': mes, 'nombre_archivo': '{{ task.task_id }}'},
											do_xcom_push=True,
											provide_context=True)
			l_datos_2021.append(datos_2021_mes)


		into_DataLake_s3 = PythonOperator(task_id='into_DataLake_s3',
										  python_callable=guardar_datos_s3_aws,
										  do_xcom_push=True,
										  provide_context=True)

		into_metrics_on_mongo = PythonOperator(	task_id='into_metrics_on_mongo',
													   python_callable=metrics_into_mongo,
													   do_xcom_push=True,
													   provide_context=True)

		eliminar_carpeta_temporal = BashOperator(	task_id='eliminar_carpeta_temporal',
													 bash_command='rm -r /opt/airflow/logs/data_weather')

		# TASK 2
		end = DummyOperator(task_id = 'end')



	l_datos = list(set().union(l_datos_2010, l_datos_2011, l_datos_2012,l_datos_2013,l_datos_2014,l_datos_2015,l_datos_2016,l_datos_2017,l_datos_2018,l_datos_2019,l_datos_2020,l_datos_2021))


	## SCHEDULER

	for i in range(len(l_datos ) -1):
		start >> crear_carpeta_temporal >> proceso_ingesta >> [l_datos[i], l_datos[ i +1]] >> proceso_ingesta_completado >> into_DataLake_s3 >> eliminar_carpeta_temporal >> into_metrics_on_mongo >> end


	return subdag




def load_subdag_process_test_sin_cargar_archivos(param_PARENT_DAG_NAME, CHILD_DAG_NAME_TEST_SIN_PROCESAR, param_default_args):

	with DAG( CHILD_DAG_NAME_TEST_SIN_PROCESAR, default_args=param_default_args) as subdag:

		start = DummyOperator(task_id='start')

		print('deep_start')

		into_DataLake_s3 = PythonOperator(task_id='into_DataLake_s3',
										  python_callable=guardar_datos_s3_aws,
										  do_xcom_push=True,
										  provide_context=True)

		into_metrics_on_mongo = PythonOperator(	task_id='into_metrics_on_mongo',
													   python_callable=metrics_into_mongo,
													   do_xcom_push=True,
													   provide_context=True)

		end = DummyOperator(task_id='end')



	start >> into_DataLake_s3 >> into_metrics_on_mongo >> end


	return subdag



# DAG
with DAG(dag_id=PARENT_DAG_NAME,
		 default_args=default_args,
		 start_date=dates.days_ago(1),
		 schedule_interval=timedelta(hours=6),
		 catchup=False) as dag:


	start = BashOperator(task_id='super_start', bash_command='echo super_start')


	task_test_no_repetir_carga_archivos = BranchPythonOperator(task_id='task_test_no_repetir_carga_archivos',
															   python_callable=test_no_repetir_carga_archivos,
															   provide_context=True
															   )


	subdag_process_complete = SubDagOperator(	task_id=TASK_ID_SUBDAG_COMPLETO,
												subdag=load_subdag_process_complete(PARENT_DAG_NAME, CHILD_DAG_NAME_COMPLETO, dag.default_args)
											)

	subdag_process_test_sin_procesar = SubDagOperator(	task_id=TASK_ID_SUBDAG_TEST_SIN_PROCESAR,
														subdag=load_subdag_process_test_sin_cargar_archivos(PARENT_DAG_NAME, CHILD_DAG_NAME_TEST_SIN_PROCESAR, dag.default_args)
													  )


	end = BashOperator(task_id='super_end', bash_command='echo super_end', trigger_rule='all_done')



	start >> task_test_no_repetir_carga_archivos >> [subdag_process_complete,subdag_process_test_sin_procesar] >> end




