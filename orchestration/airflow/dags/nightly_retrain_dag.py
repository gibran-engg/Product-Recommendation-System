from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# processing/ and serving/ are mounted here by docker-compose.yml
SPARK_JOBS = "/opt/airflow/project/processing/spark_jobs"


def spark_job(script: str) -> BashOperator:
    return BashOperator(
        task_id=script.removesuffix(".py"),
        bash_command=f"python {SPARK_JOBS}/{script}",
    )


with DAG(
    dag_id="nightly_retrain",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["recommendation-system", "phase-2"],
) as dag:

    (
        spark_job("build_interaction_matrix.py")
        >> spark_job("train_als_model.py")
        >> spark_job("sanity_check.py")
        >> spark_job("push_recommendations_to_redis.py")
    )
