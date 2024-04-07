from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
# from airflow.utils.dates import days_ago
from datetime import datetime
# from main import fetch_crypto_prices
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_crypto_prices',
    default_args=default_args,
    description='DAG to automate real-time crypto prices ingestion pipeline',
    schedule_interval=timedelta(minutes=1), # run every minute
)

run_etl = DockerOperator(
    task_id = 'crypto_price_ingestion_pipeline',
    image = 'producer:airflow',
    network_mode='host',
    command = 'python3 main.py',
    api_version= 'auto',
    auto_remove = True,
    dag = dag
)