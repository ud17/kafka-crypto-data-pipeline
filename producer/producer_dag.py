from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from main import fetch_crypto_prices
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 31),
    'email': ['uditpandya.dev@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_crypto_prices',
    default_args=default_args,
    description='DAG to automate real-time crypto prices ingestion pipeline',
    schedule_interval=timedelta(days=1)
)

run_etl = PythonOperator(
    task_id = 'crypto_price_ingestion_pipeline',
    python_callable = fetch_crypto_prices,
    dag = dag
)

run_etl