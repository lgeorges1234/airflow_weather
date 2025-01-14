from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from weather_pipeline.tasks.data_collection import collect_weather_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


with DAG(
    'weather_prediction_pipeline',
    default_args = default_args,
    description='Weather data collection and prediction pipeline',
    schedule_interval='* * * * *', # Every minute
    star_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['weather', 'airflow_exam']
) as dag:
    
    # Task 1: Collect weather data
    collect_data = PythonOperator(
        task_id='collect_weather_data',
        python_callable=collect_weather_data,
        dag=dag,
    )