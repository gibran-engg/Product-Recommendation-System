"""
spark_session.py

Shared SparkSession factory so every Spark job in this project connects
to HDFS the same way, instead of repeating boilerplate config in every file.
"""

import os

from pyspark.sql import SparkSession

HDFS_NAMENODE = "hdfs://namenode:8020"
# Same HDFS identity ingestion/hdfs_writer.py writes as; otherwise HDFS sees
# the OS login (e.g. "airflow" in its container) and denies writes under /data
HDFS_USER = "root"


def get_spark(app_name: str) -> SparkSession:
    os.environ.setdefault("HADOOP_USER_NAME", HDFS_USER)  # must be set before the JVM starts
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # standalone local mode — no cluster needed for this project size
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .config("spark.sql.shuffle.partitions", "8")  # keep shuffle partitions sane on a laptop
        # Reach DataNodes by hostname, not their Docker-internal IP, so jobs also work from the host
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        # Time decay uses calendar-day diffs; pin the zone so every machine builds the same matrix
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  # Spark's default INFO logging is very noisy
    return spark