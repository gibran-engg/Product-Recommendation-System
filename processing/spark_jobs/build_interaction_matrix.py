"""
build_interaction_matrix.py

Reads raw events from HDFS, applies the finalized event weighting +
time-decay scheme, and aggregates into a user-item interaction matrix —
the direct input to ALS training.

Also emits a lightweight item_features table (popularity + conversion
rate) as a cheap by-product of the same aggregation pass — used later
for reranking, not required by ALS itself.

Usage:
    python build_interaction_matrix.py
"""

import logging
import os
import sys

# Make processing/utils importable regardless of the current working
# directory this script is launched from.
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "utils"))

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from spark_session import get_spark
from weighting import EVENT_WEIGHTS, TIME_DECAY_HALF_LIFE_DAYS

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

RAW_EVENTS_PATH = "hdfs://namenode:8020/data/raw/user_events/*/*/*"
INTERACTION_MATRIX_OUTPUT = "hdfs://namenode:8020/data/processed/interaction_matrix"
ITEM_FEATURES_OUTPUT = "hdfs://namenode:8020/data/processed/item_features"


def load_raw_events(spark):
    logger.info(f"Reading raw events from {RAW_EVENTS_PATH}")
    df = spark.read.parquet(RAW_EVENTS_PATH)
    logger.info(f"Loaded {df.count():,} raw events")
    return df


def apply_event_weights(df):
    """Map event_type -> base weight using the finalized scheme."""
    weight_expr = F.when(F.col("event_type") == "purchase", EVENT_WEIGHTS["purchase"]) \
        .when(F.col("event_type") == "cart", EVENT_WEIGHTS["cart"]) \
        .when(F.col("event_type") == "view", EVENT_WEIGHTS["view"]) \
        .otherwise(EVENT_WEIGHTS["remove_from_cart"])

    return df.withColumn("event_weight", weight_expr)


def apply_time_decay(df):
    """
    Applies exponential decay relative to the MOST RECENT timestamp in
    the dataset (not wall-clock now). final_weight = event_weight * decay
    """
    max_ts = df.agg(F.max("timestamp")).collect()[0][0]
    logger.info(f"Decaying relative to most recent event timestamp: {max_ts}")

    days_since = F.datediff(F.lit(max_ts), F.col("timestamp"))
    decay_factor = F.pow(F.lit(0.5), days_since / F.lit(TIME_DECAY_HALF_LIFE_DAYS))

    return df.withColumn("decay_factor", decay_factor) \
              .withColumn("weighted_score", F.col("event_weight") * F.col("decay_factor"))


def build_interaction_matrix(df):
    """Collapse to one row per (user_id, item_id) with a summed weighted score."""
    matrix = (
        df.groupBy("user_id", "item_id")
        .agg(F.sum("weighted_score").alias("interaction_score"))
        .withColumn("user_id", F.col("user_id").cast(IntegerType()))
        .withColumn("item_id", F.col("item_id").cast(IntegerType()))
    )
    logger.info(f"Interaction matrix: {matrix.count():,} unique (user, item) pairs")
    return matrix


def build_item_features(df):
    """
    Cheap by-product of the same pass: item popularity and conversion rate.
    Not consumed by ALS — used later for reranking/cold-start fallback.
    """
    item_stats = df.groupBy("item_id").agg(
        F.sum("event_weight").alias("popularity_score"),
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
        F.sum(F.when(F.col("event_type").isin("purchase", "cart"), 1).otherwise(0)).alias("convert_count"),
    )

    item_stats = item_stats.withColumn(
        "conversion_rate",
        F.when(F.col("view_count") > 0, F.col("convert_count") / F.col("view_count")).otherwise(0.0)
    )

    return item_stats.select("item_id", "popularity_score", "conversion_rate")


def main():
    spark = get_spark("build_interaction_matrix")

    df = load_raw_events(spark)
    df = apply_event_weights(df)
    df = apply_time_decay(df)

    matrix = build_interaction_matrix(df)
    matrix.write.mode("overwrite").parquet(INTERACTION_MATRIX_OUTPUT)
    logger.info(f"Wrote interaction matrix to {INTERACTION_MATRIX_OUTPUT}")

    item_features = build_item_features(df)
    item_features.write.mode("overwrite").parquet(ITEM_FEATURES_OUTPUT)
    logger.info(f"Wrote item features to {ITEM_FEATURES_OUTPUT}")

    matrix.show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()