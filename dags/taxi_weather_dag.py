from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.extract_taxi_data import extract_taxi_data
from scripts.fetch_weather_data import fetch_weather_data
from scripts.join_and_transform import join_and_transform
from scripts.load_to_postgres import load_to_postgres

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "taxi_weather_pipeline",
    default_args=default_args,
    description="Taxi + Weather Daily Pipeline",
    schedule_interval="@daily", 
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task_extract_taxi = PythonOperator(
        task_id="extract_taxi_data",
        python_callable=extract_taxi_data,
    )

    task_fetch_weather = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data,
    )

    task_join = PythonOperator(
        task_id="join_and_transform",
        python_callable=join_and_transform,
    )

    task_load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    task_extract_taxi >> task_fetch_weather >> task_join >> task_load
