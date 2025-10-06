# exemplo_dag_taskflow_mista.py
from datetime import timedelta
import pendulum
import os

from airflow.decorators import dag, task
from airflow.operators.python import PythonVirtualenvOperator, PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow import AirflowException

from include.src.etl.bronze_ingest import bronze_ingest
from include.src.etl.pandas.bronze_to_silver import bronze_to_silver
from include.src.etl.pandas.silver_to_gold import silver_to_gold

# Default args comuns
default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

BASE_PATH = os.path.join(os.environ["AIRFLOW_HOME"], "include")

# DAG com decorator
@dag(
    schedule="30 1 * * *",  # minuto 30, hora 1, todo dia
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["ETL", "Pipeline"],
)
def nasa_asteroids_pipeline():

    bronze_ingest_task = PythonOperator(
        task_id="bronze_ingest_task",
        python_callable=bronze_ingest,
    )

    bronze_to_silver_task = PythonOperator(
        task_id="bronze_to_silver_task",
        python_callable=bronze_to_silver,
    )

    silver_to_gold_task = PythonOperator(
        task_id="silver_to_gold_task",
        python_callable=silver_to_gold,
    )

    bronze_ingest_task >> bronze_to_silver_task >> silver_to_gold_task

# instancia a DAG
nasa_asteroids_pipeline()
