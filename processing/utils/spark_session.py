"""
spark_session.py

Shared SparkSession factory so every Spark job in this project connects
to HDFS the same way, instead of repeating boilerplate config in every file.
"""

from pyspark.sql import SparkSession

HDFS_NAMENODE = "hdfs://namenode:8020"


def get_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # standalone local mode — no cluster needed for this project size
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .config("spark.sql.shuffle.partitions", "8")  # keep shuffle partitions sane on a laptop
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  # Spark's default INFO logging is very noisy
    return spark