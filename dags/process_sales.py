import sys
from pprint import pprint
import os
from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from src.export_raw import export_raw_job
from src.save_avro import save_avro_job

dag = DAG(
    dag_id = 'process_sales',
    start_date=datetime(2022, 8, 9),
    # end_date=datetime(2022, 8, 11),
    schedule_interval='0 1 * * *',
    max_active_runs=1,
    catchup=True
)

start = EmptyOperator(
    task_id='start',
    dag=dag
)

API_URL= os.environ["API_URL"]
AUTH_TOKEN = os.environ["AUTH_TOKEN"]
BASE_DIR = os.environ["BASE_DIR"]

extract_data_from_api = PythonOperator(
    task_id='extract_data_from_api',
    dag=dag,
    python_callable= export_raw_job,
    op_args=[BASE_DIR, API_URL, AUTH_TOKEN],
    provide_context=True
)

convert_to_avro = PythonOperator(
    task_id='convert_to_avro',
    dag=dag,
    python_callable=save_avro_job,
    op_args=[BASE_DIR],
    provide_context=True
)


start >> extract_data_from_api >> convert_to_avro



