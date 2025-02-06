from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import os

from datetime import datetime

local_workflow = DAG(
    "LocalIngestionDAG",
    schedule_interval="0 6 2 * *",
    start_date= datetime(2025,1,1)
)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
month = 01
URL_TEMPLATE = URL_PREFIX + F'yellow_tripdata_2021-{month}.parquet'

URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'
with local_workflow:

    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sSL {URL} > {AIRFLOW_HOME}/output.parquet'
    )


    ingest_task = BashOperator(
        task_id='ingest',
        bash_command=f'ls {AIRFLOW_HOME}'   
    )


    wget_task >> ingest_task