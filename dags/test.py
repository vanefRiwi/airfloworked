from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": 30, 
}

def start_task():
    print("Pipeline started.")

def extract_data():
    print("Extracting data...")
    print("Data extracted successfully.")

def transform_data():
    print("Transforming Data")
    #raise Exception("Intentional failure for UI demo")
    print("Data transformed successfully.")

def load_data():
    print("Loading data...")
    print("Data loaded successfully.")

with DAG(
    dag_id="ui_demo_pipeline",
    description="DAG for Airflow UI walkthrough demo",
    default_args=default_args,
    start_date=datetime(2025, 12, 16),
    schedule="*/1 * * * *", 
    catchup=False,
    tags=["ui-demo", "training"],
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=start_task,
    )

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    extract >> transform >> load