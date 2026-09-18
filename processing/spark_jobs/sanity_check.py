"""
sanity_check.py

Manually spot-checks ALS recommendations against real users' actual
interaction history. A model that runs without errors isn't the same
as a model that's learned something sensible — this is that check.

Usage:
    python sanity_check.py
"""

import logging
import os
import sys

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "utils"))

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import functions as F

from spark_session import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

MODEL_PATH = "hdfs://namenode:8020/models/als_latest"
RAW_EVENTS_PATH = "hdfs://namenode:8020/data/raw/user_events/*/*/*"
NUM_SAMPLE_USERS = 5
MIN_INTERACTIONS_FOR_SAMPLE = 10  # pick users with real history, not near-cold-start users


def pick_sample_users(spark, raw_events):
    """Pick a handful of users with a decent interaction history to check against."""
    user_counts = raw_events.groupBy("user_id").count()
    candidates = user_counts.filter(F.col("count") >= MIN_INTERACTIONS_FOR_SAMPLE)
    sample = candidates.orderBy(F.rand(seed=42)).limit(NUM_SAMPLE_USERS)
    return [row["user_id"] for row in sample.collect()]


def get_user_history(raw_events, user_id):
    """What has this user actually interacted with — categories and brands."""
    history = (
        raw_events.filter(F.col("user_id") == user_id)
        .filter(F.col("category_code").isNotNull())
        .groupBy("category_code")
        .count()
        .orderBy(F.desc("count"))
        .limit(5)
    )
    return [row["category_code"] for row in history.collect()]


def get_recommendations(model, user_id):
    single_user_df = model.userFactors.filter(F.col("id") == user_id).select(F.col("id").alias("user_id"))
    if single_user_df.count() == 0:
        return None  # user not present in the trained model (e.g. filtered out or cold-start)
    recs = model.recommendForUserSubset(single_user_df, 10)
    return recs.collect()


def get_recommended_categories(raw_events, item_ids):
    """Look up what categories the recommended item_ids actually belong to."""
    items = raw_events.filter(F.col("item_id").isin(item_ids)) \
        .filter(F.col("category_code").isNotNull()) \
        .select("item_id", "category_code").distinct()
    return {row["item_id"]: row["category_code"] for row in items.collect()}


def run_sanity_check(spark):
    logger.info("Loading trained ALS model...")
    model = ALSModel.load(MODEL_PATH)

    logger.info("Loading raw events for history lookup...")
    raw_events = spark.read.parquet(RAW_EVENTS_PATH)

    sample_users = pick_sample_users(spark, raw_events)
    logger.info(f"Sampled users for sanity check: {sample_users}")

    all_recs_flat = []

    for user_id in sample_users:
        print(f"\n{'=' * 60}")
        print(f"USER {user_id}")
        print(f"{'=' * 60}")

        history_categories = get_user_history(raw_events, user_id)
        print(f"Top categories from actual history: {history_categories}")

        recs = get_recommendations(model, user_id)
        if recs is None:
            print("No recommendations — user not present in trained model.")
            continue

        rec_item_ids = [r["item_id"] for r in recs[0]["recommendations"]]
        rec_categories = get_recommended_categories(raw_events, rec_item_ids)

        all_recs_flat.append(tuple(sorted(rec_item_ids)))

        print(f"Recommended item_ids: {rec_item_ids}")
        print("Recommended item categories:")
        for item_id in rec_item_ids:
            print(f"  - {item_id}: {rec_categories.get(item_id, 'unknown')}")

        overlap = set(history_categories) & set(rec_categories.values())
        print(f"\nCategory overlap with history: {overlap if overlap else 'NONE — review this user manually'}")

    # Red flag: if every user gets the exact same recommendation list, personalization isn't happening
    if len(set(all_recs_flat)) == 1 and len(all_recs_flat) > 1:
        logger.warning(
            "RED FLAG: all sampled users received IDENTICAL recommendation lists. "
            "Model may not be personalizing — check rank, iterations, and input data."
        )
    else:
        logger.info("Recommendation lists differ across sampled users — good sign.")


def main():
    spark = get_spark("sanity_check")
    run_sanity_check(spark)
    spark.stop()


if __name__ == "__main__":
    main()