
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# Job order and launch command are shared with run.py (repo root)
from pipeline_jobs import DEFAULT_DRIVER_MEMORY, PIPELINE_JOBS, spark_submit_command


def spark_job(script: str) -> BashOperator:
    return BashOperator(
        task_id=script.removesuffix(".py"),
        bash_command=spark_submit_command(script, os.getenv("SPARK_DRIVER_MEMORY", DEFAULT_DRIVER_MEMORY)),
    )


with DAG(
    dag_id="nightly_retrain",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["recommendation-system", "phase-3"],
) as dag:

    tasks = [spark_job(script) for script in PIPELINE_JOBS]
    for upstream, downstream in zip(tasks, tasks[1:]):
        upstream >> downstream
