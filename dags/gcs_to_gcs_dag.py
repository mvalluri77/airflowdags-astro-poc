import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

# Custom Python logic for derriving data value
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# Default arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definitions
with DAG(dag_id='GCS_to_GCS',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args
         ) as dag:

# Dummy strat task   
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    copy_files = GCSToGCSOperator(
            task_id='copy_files',
            source_bucket='poc21',
            source_object='raw/*.csv',
            destination_bucket='sample_bucket21',
            destination_object='copied_data/2017/',
            gcp_conn_id='google_cloud_conn_id'
        )

# Dummy end task   
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

start >> copy_files >> end