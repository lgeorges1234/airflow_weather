from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from docker.types import Mount
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Get the absolute path to the Airflow home directory
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')

# Common mount configuration
mounts = [
    Mount(
        source="/home/ubuntu/airflow/dags",
        target='/opt/airflow/dags',
        type='bind'
    ),
    Mount(
        source="/home/ubuntu/airflow/app",
        target='/opt/airflow/app',
        type='bind'
    )
]


def build_data_collection_group(dag: DAG):
    with TaskGroup(group_id='data_collection') as data_collection:
        collect_weather = DockerOperator(
            task_id='collect_weather_data',
            image='weather_collection:latest',  # Image personnalisée
            command='python -m weather_pipeline.tasks.data_collection',
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
            mounts=mounts,
            mount_tmp_dir=False,
            auto_remove=True,
            environment={
                'PYTHONPATH': '/opt/airflow/dags',
                'AIRFLOW_VAR_OPENWEATHER_API_KEY': '{{ var.value.openweather_api_key }}',
                'AIRFLOW_VAR_CITIES': '["paris", "london", "washington"]'
            }
        )
        return data_collection

def build_data_processing_group(dag: DAG):
    with TaskGroup(group_id='data_processing') as data_processing:
        transform_recent = DockerOperator(
            task_id='transform_recent_data',
            image='weather_processing:latest',  # Image personnalisée
            command='python -m weather_pipeline.tasks.data_transformation --mode recent',
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
            mounts=mounts,
            auto_remove=True
        )

        transform_full = DockerOperator(
            task_id='transform_full_data',
            image='weather_processing:latest',  # Image personnalisée
            command='python -m weather_pipeline.tasks.data_transformation --mode full',
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
            mounts=mounts,
            auto_remove=True
        )
        
        return data_processing

def build_model_training_group(dag: DAG):
    with TaskGroup(group_id='model_training') as model_training:
        train_lr = DockerOperator(
            task_id='train_linear_regression',
            image='weather_ml:latest',  # Image personnalisée
            command='python -m weather_pipeline.tasks.model_training --model linear',
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
            mounts=mounts,
            auto_remove=True
        )

        train_dt = DockerOperator(
            task_id='train_decision_tree',
            image='weather_ml:latest',
            command='python -m weather_pipeline.tasks.model_training --model decision',
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
            mounts=mounts,
            auto_remove=True
        )

        train_rf = DockerOperator(
            task_id='train_random_forest',
            image='weather_ml:latest',
            command='python -m weather_pipeline.tasks.model_training --model forest',
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
            mounts=mounts,
            auto_remove=True
        )

        select_model = DockerOperator(
            task_id='select_best_model',
            image='weather_ml:latest',
            command='python -m weather_pipeline.tasks.model_training --model select',
            docker_url='unix://var/run/docker.sock',
            network_mode='bridge',
            mounts=mounts,
            auto_remove=True
        )

        [train_lr, train_dt, train_rf] >> select_model
        
        return model_training

with DAG(
    'weather_prediction_pipeline',
    default_args=default_args,
    description='Weather data collection and prediction pipeline',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['weather', 'airflow_exam']
) as dag:
    
    # Build task groups
    data_collection = build_data_collection_group(dag)
    data_processing = build_data_processing_group(dag)
    model_training = build_model_training_group(dag)
    
    # Define main workflow
    data_collection >> data_processing >> model_training