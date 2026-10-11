"""
cold_start_sanity_check.py

Human-readable spot check of the TF-IDF cold-start recommendations written by
train_content_based.py — the content-based counterpart of sanity_check.py.

For a few sampled cold-start users, prints the items they actually touched
(category + brand) next to what they were recommended, with the cosine score,
so you can eyeball whether the recommendations follow their history.

Usage:
    python cold_start_sanity_check.py
"""

import logging
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(HERE, "..", "utils"))
sys.path.append(HERE)

from pyspark.sql import functions as F

from spark_session import get_spark
from train_content_based import CONTENT_RECS_OUTPUT, RAW_EVENTS_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

NUM_SAMPLE_USERS = 5
RECS_TO_SHOW = 10


def item_label(row):
    return f"{row['category_code'] or 'cid:' + str(row['category_id'])} | {row['brand'] or '-'}"


def main():
    spark = get_spark("cold_start_sanity_check")

    raw_events = spark.read.parquet(RAW_EVENTS_PATH)
    items = raw_events.groupBy(F.col("item_id").cast("int").alias("item_id")).agg(
        F.max("category_code").alias("category_code"),
        F.max("category_id").alias("category_id"),
        F.max("brand").alias("brand"),
    ).cache()
    recs = spark.read.parquet(CONTENT_RECS_OUTPUT)

    sample = recs.orderBy(F.rand(seed=42)).limit(NUM_SAMPLE_USERS).collect()
    overlapping = 0

    for user in sample:
        user_id = user["user_id"]
        print(f"\n{'=' * 70}\nCOLD-START USER {user_id}\n{'=' * 70}")

        history = (
            raw_events.where(F.col("user_id") == user_id)
            .select(F.col("item_id").cast("int").alias("item_id"), "event_type")
            .join(items, "item_id")
            .collect()
        )
        print("History:")
        for row in history:
            print(f"  {row['event_type']:<17} {row['item_id']:<10} {item_label(row)}")

        top = user["recommendations"][:RECS_TO_SHOW]
        rec_items = {r["item_id"]: r for r in items.where(F.col("item_id").isin([t["item_id"] for t in top])).collect()}
        print(f"Top {len(top)} recommendations (cosine score):")
        for t in top:
            meta = rec_items.get(t["item_id"])
            print(f"  {t['rating']:.3f}  {t['item_id']:<10} {item_label(meta) if meta else 'unknown'}")

        history_categories = {row["category_code"] for row in history if row["category_code"]}
        rec_categories = {m["category_code"] for m in rec_items.values() if m["category_code"]}
        overlap = history_categories & rec_categories
        overlapping += bool(overlap)
        print(f"Category overlap with history: {overlap if overlap else 'NONE — popularity backfill or blank category_code'}")

    logger.info(f"{overlapping}/{len(sample)} sampled cold-start users got recommendations in a category from their history")
    spark.stop()


if __name__ == "__main__":
    main()
