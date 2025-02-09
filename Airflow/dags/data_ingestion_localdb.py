from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

from ingest_script import ingest_callable
from datetime import datetime

local_workflow = DAG(
    "LocalIngestionDAG",
    schedule_interval="0 6 2 * *",
    start_date= datetime(2025,1,1)
)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST=os.getenv('PG_HOST')
PG_USER=os.getenv('PG_USER')
PG_PASSWORD=os.getenv('PG_PASSWORD')
PG_PORT=os.getenv('PG_PORT')
PG_DATABASE=os.getenv('PG_DATABASE')




URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ (execution_date - macros.timedelta(days=3*365)).strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ (execution_date - macros.timedelta(days=3*365)).strftime(\'%Y-%m\') }}.parquet'

with local_workflow:

    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
        )


    ingest_task = PythonOperator(
        task_id = "ingest",
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DATABASE,
            table_name='???',
            csv_file=OUTPUT_FILE_TEMPLATE
        )
    )


    wget_task >> ingest_task