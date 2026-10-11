"""
Shared pytest setup.

The pipeline scripts import each other through sys.path tweaks rather than
packages, so the tests put the same directories on the path the scripts
expect when they are launched directly.
"""

import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

for rel in (
    "processing/utils",
    "processing/spark_jobs",
    "serving",
    "experimentation",
    "ingestion",
):
    path = os.path.join(ROOT, rel)
    if path not in sys.path:
        sys.path.insert(0, path)


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession with the same timezone pin as processing/utils/spark_session.py."""
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .appName("recsys-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
