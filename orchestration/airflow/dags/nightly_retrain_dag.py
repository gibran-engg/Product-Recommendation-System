from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def pipeline_placeholder():
    print("Nightly recommendation pipeline placeholder")


with DAG(
    dag_id="nightly_retrain",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["recommendation-system", "phase-1"],
) as dag:

    retrain_placeholder = PythonOperator(
        task_id="retrain_placeholder",
        python_callable=pipeline_placeholder,
    )