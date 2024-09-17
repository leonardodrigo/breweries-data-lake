from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.operators.dummy import DummyOperator # type: ignore
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from packages.extraction import extract_raw_data
import os


AZURE_STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
AZURE_TOKEN = os.environ["AZURE_TOKEN"]
EMAIL = os.environ["EMAIL"]

spark_config = {
    "spark.master": "spark://spark:7077",
    "spark.jars.packages": "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6",
    "spark.hadoop.fs.azure.account.key." + AZURE_STORAGE_ACCOUNT + ".blob.core.windows.net": AZURE_TOKEN,
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.repl.eagerEval.enabled": "true",
    "spark.sql.repl.eagerEval.maxNumRows": "10"
}

default_args = {
    "owner": "Leonardo Drigo",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
    "execution_timeout": timedelta(minutes=60),
    "tags": ["API", "Datalake", "Azure"]
}

dag = DAG(
    'brewery-pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)


check_api_availability = HttpSensor(
    task_id=f"check_api_availability",
    http_conn_id="open_brewery_db_api",
    endpoint="/breweries",
    timeout=600,                           
    poke_interval=60,                                                        
    dag=dag 
)

extract_raw_data_from_api = PythonOperator(
    task_id=f"extract_raw_data_from_api",
    python_callable=extract_raw_data,
    op_kwargs={
        "container_name": "bronze",
        "max_per_page": 200
    },
    dag=dag
)

breweries_to_silver = SparkSubmitOperator(
    task_id='breweries_to_silver',
    conn_id='spark_default',
    application='/opt/airflow/spark/breweries_to_silver.py',
    name='breweries_to_silver',
    verbose=True,
    conf=spark_config,
    dag=dag,
)

breweries_to_gold = SparkSubmitOperator(
    task_id='breweries_to_gold',
    conn_id='spark_default',
    application='/opt/airflow/spark/breweries_to_gold.py',
    name='breweries_to_gold',
    verbose=True,
    conf=spark_config,
    dag=dag,
)
                  
end = DummyOperator(
    task_id='end',
    dag=dag,
)


check_api_availability >> extract_raw_data_from_api >> breweries_to_silver >> breweries_to_gold >> end

