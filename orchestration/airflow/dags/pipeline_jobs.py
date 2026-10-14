"""
pipeline_jobs.py

Single source of truth for the batch pipeline: which Spark jobs run, in what
order, and the exact command that launches each one inside the airflow
container. Used by nightly_retrain_dag.py (Airflow) and by run.py at the repo
root (one-command local bring-up), so the two can never drift apart.

Plain module with no Airflow imports, so run.py can load it on the host.
"""

# processing/ and serving/ are mounted here by docker-compose.yml
PROJECT_DIR = "/opt/airflow/project"
SPARK_JOBS_DIR = f"{PROJECT_DIR}/processing/spark_jobs"

# Order matters: each job reads what the previous ones wrote to HDFS
PIPELINE_JOBS = [
    "build_interaction_matrix.py",      # raw events -> weighted, time-decayed (user, item) matrix + item features
    "train_als_model.py",               # implicit ALS -> model + top-N per user
    "sanity_check.py",                  # red-flag checks on ALS output
    "train_content_based.py",           # TF-IDF cold-start recommendations
    "cold_start_sanity_check.py",       # history vs recs for a few cold-start users
    "evaluate_model.py",                # Precision/Recall/hit-rate@K + coverage, ALS vs popularity vs TF-IDF
    "push_recommendations_to_redis.py", # ALS (warm) + TF-IDF (cold-start) top-N -> Redis
    "export_serving_features.py",       # catalog, user profiles, co-purchase, embeddings, metrics -> Redis
]


def spark_submit_command(script: str) -> str:
    # spark-submit starts the JVM before the job's Python runs, so the HDFS
    # identity set in spark_session.py comes too late — set it here instead
    return (
        f"HADOOP_USER_NAME=root "
        f"PYTHONPATH={PROJECT_DIR}/processing:{PROJECT_DIR}/serving "
        f"spark-submit "
        f"--conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 "
        f"--conf spark.hadoop.dfs.client.use.datanode.hostname=false "
        f"{SPARK_JOBS_DIR}/{script}"
    )
