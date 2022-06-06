from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from ingest import ingest_callable,format_to_csv,clean_directory
from pathlib import Path

PG_HOST = os.environ.get("PG_HOST") 
PG_USER =os.environ.get("PG_USER") 
PG_PASSWORD=os.environ.get("PG_PASSWORD") 
PG_PORT=os.environ.get("PG_PORT") 
PG_DATABASE=os.environ.get("PG_DATABASE") 
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# url="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"

def local_dag_tasks(dag,url_template,local_parquet_path,local_csv_path,table_name):
   with dag:
      # Now to create tasks under the DAG
      download_files = BashOperator(
         task_id="get_parquet_files",
         bash_command= f'curl -sSLf {url_template} > {local_parquet_path}',
         # bash_command= f'echo Successfully downloaded to yellow_taxi_{{execution_date.strftime(\'%Y-%m\')}}.parquet'
      ) #Create a bash operators/command line task to execute "bash command"
         
      parquet_to_csv_task = PythonOperator(
         task_id="parquet_to_csv",   
         python_callable=format_to_csv,
         op_kwargs=dict(
            src_file=local_parquet_path,
            output_csv=local_csv_path
         )
      )
      
      ingest_task = PythonOperator(
         task_id="ingest_to_postgres",
         python_callable= ingest_callable,
         op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST, 
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=table_name,
            csv_file=local_csv_path)
      )
      delete_temp_files= PythonOperator(
         task_id="delete_temp_files",   
         python_callable=clean_directory,
         op_kwargs=dict(
            parquet=local_parquet_path,
            csv=local_csv_path
         
         )
      )
      download_files>>parquet_to_csv_task>>ingest_task>>delete_temp_files

#=======================================================================================================================
# Yellow Trip Data
URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data/"
YELLOW_URL_TEMPLATE = URL_PREFIX+"yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet"
YELLOW_OUTPUT_FILE = AIRFLOW_HOME+'/yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet'
YELLOW_TABLE_NAME = "yellow_taxi_{{execution_date.strftime(\'%Y_%m\')}}"
YELLOW_OUTPUT_CSV = AIRFLOW_HOME+'/yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'

yellow_taxi_dag = DAG(
      dag_id="yellow_taxi_local_ingestion", #DAG name 
      schedule_interval="0 0 1 * *",#“At 00:00 on day-of-month 2.”
      start_date=datetime(2019, 1, 1), #DAG start date
      end_date=datetime(2020, 12, 1),#DAG end date
      catchup=True,
      max_active_runs=3, #max number of concurrent processes
      tags=["dtc-de"],   
)

local_dag_tasks(
   dag=yellow_taxi_dag,
   url_template=YELLOW_URL_TEMPLATE,
   local_parquet_path=YELLOW_OUTPUT_FILE,
   local_csv_path=YELLOW_OUTPUT_CSV,
   table_name=YELLOW_TABLE_NAME,
   )
#=======================================================================================================================
# For Hire Vehicle Data fhv_tripdata_2019-02.parquet
URL_PREFIX2="https://nyc-tlc.s3.amazonaws.com/trip+data/"
FHV_URL_TEMPLATE = URL_PREFIX2+"fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet"
FHV_OUTPUT_FILE = AIRFLOW_HOME+'/fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet'
FHV_TABLE_NAME = "fhv_tripdata_{{execution_date.strftime(\'%Y_%m\')}}"
FHV_OUTPUT_CSV = AIRFLOW_HOME+'/fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'

for_hire_vehicles_dag = DAG(
      dag_id="fhv_local_ingestion", #DAG name 
      schedule_interval="0 0 1 * *",#“At 00:00 on day-of-month 2.”
      start_date=datetime(2019, 1, 1), #DAG start date
      end_date=datetime(2020, 1, 1),#DAG end date
      catchup=True,
      max_active_runs=3, #max number of concurrent processes
      tags=["dtc-de"],   
)

local_dag_tasks(
   dag=for_hire_vehicles_dag,
   url_template=FHV_URL_TEMPLATE,
   local_parquet_path=FHV_OUTPUT_FILE,
   local_csv_path=FHV_OUTPUT_CSV,
   table_name=FHV_TABLE_NAME,
)

#=======================================================================================================================
#Zones
ZONES_URL_TEMPLATE = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
# ZONES_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/taxi_zone_lookup.parquet'
ZONES_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/taxi+_zone_lookup.csv'
ZONES_TABLE_NAME="zone_lookup"

zones_data_dag = DAG(
   dag_id="zones_data",
   schedule_interval="@once",
   start_date=days_ago(1),
   catchup=True,
   max_active_runs=3,
   tags=['dtc-de'],
)

local_dag_tasks(
   dag=zones_data_dag,
   url_template=ZONES_URL_TEMPLATE,
   local_parquet_path=ZONES_CSV_FILE_TEMPLATE,
   local_csv_path=ZONES_CSV_FILE_TEMPLATE,
   table_name=ZONES_TABLE_NAME
   )