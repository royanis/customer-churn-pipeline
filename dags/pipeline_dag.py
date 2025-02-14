print("pipeline_dag.py with conda env is being parsed!")
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os
import shlex
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'dmml_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'Data_management_pipeline',
    default_args=default_args,
    description='End-to-End Data Management Pipeline for Customer Churn Prediction',
    schedule_interval='@daily',  # Adjust as needed
    catchup=False
)

# Define the base path relative to the DAG location.
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../src"))
logging.info("BASE_PATH: %s", BASE_PATH)

# Using `conda run` to execute commands in the 'base' environment.
# For each task, we change directory to the task folder and then run the corresponding script.

# Task 1: Data Ingestion
ingestion_path = shlex.quote(os.path.join(BASE_PATH, "ingestion"))
ingestion_task = BashOperator(
    task_id='data_ingestion',
    bash_command=f"cd {ingestion_path} && conda run -n base python data_ingestion.py",
    dag=dag
)

# Task 2: Raw Data Storage
raw_storage_path = shlex.quote(os.path.join(BASE_PATH, "storage"))
raw_data_storage_task = BashOperator(
    task_id='raw_data_storage',
    bash_command=f"cd {raw_storage_path} && conda run -n base python raw_data_storage.py",
    dag=dag
)

# Task 3: Data Validation
validation_path = shlex.quote(os.path.join(BASE_PATH, "validation"))
validation_task = BashOperator(
    task_id='data_validation',
    bash_command=f"cd {validation_path} && conda run -n base python data_validation.py",
    dag=dag
)

# Task 4: Data Preparation
preparation_path = shlex.quote(os.path.join(BASE_PATH, "preparation"))
preparation_task = BashOperator(
    task_id='data_preparation',
    bash_command=f"cd {preparation_path} && conda run -n base python data_preparation.py",
    dag=dag
)

# Task 5: Data Transformation and Storage
transformation_path = shlex.quote(os.path.join(BASE_PATH, "transformation"))
transformation_task = BashOperator(
    task_id='data_transformation',
    bash_command=f"cd {transformation_path} && conda run -n base python data_transformation.py",
    dag=dag
)

# Task 6: Feature Store
feature_store_path = shlex.quote(os.path.join(BASE_PATH, "feature_store"))
feature_store_task = BashOperator(
    task_id='feature_store',
    bash_command=f"cd {feature_store_path} && conda run -n base python feature_store.py",
    dag=dag
)

# Task 7: Data Versioning
data_versioning_path = shlex.quote(os.path.join(BASE_PATH, "versioning"))
data_versioning_task = BashOperator(
    task_id='data_versioning',
    bash_command=f"cd {data_versioning_path} && conda run -n base python data_versioning.py",
    dag=dag
)

# Task 8: Model Building
model_path = shlex.quote(os.path.join(BASE_PATH, "model"))
model_building_task = BashOperator(
    task_id='model_building',
    bash_command=f"cd {model_path} && conda run -n base python model_building.py",
    dag=dag
)

# Define task dependencies in the order
ingestion_task >> raw_data_storage_task >> validation_task >> preparation_task >> transformation_task >> feature_store_task >> data_versioning_task >> model_building_task