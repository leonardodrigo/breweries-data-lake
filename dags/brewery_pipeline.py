from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.sensors.http import HttpSensor
from packages.extraction import extract_raw_data

default_args = {
    'owner': 'Leonardo Drigo',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1)
}

dag = DAG('brewery-pipeline',
        schedule_interval='@daily',
        default_args=default_args,
        catchup=False)


check_api_availability = HttpSensor(
    task_id=f'check_api_availability',
    http_conn_id='open_brewery_db_api',
    endpoint='/breweries',
    timeout=600,                           
    poke_interval=60,                                                        
    dag=dag 
)

extract_raw_data_from_api = PythonOperator(
    task_id=f"extract_raw_data_from_api",
    python_callable=extract_raw_data,
    op_kwargs={
        "container_name": "raw-open-brewerydb-datalake",
        "max_per_page": 200
        },
    dag=dag
)
                  

next_task = DummyOperator(
    task_id='next_task',
    dag=dag,
)


check_api_availability >> extract_raw_data_from_api >> next_task
