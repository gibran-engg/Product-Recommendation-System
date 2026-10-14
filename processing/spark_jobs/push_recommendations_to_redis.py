"""
push_recommendations_to_redis.py

Loads the top-N recommendations from HDFS into Redis, where the FastAPI
/recommendations/{user_id} endpoint serves them from. Uses the serving
layer's own RedisClient so the key format and serialization are defined in
exactly one place.

Routing (features.md, "Content-Based Cold-Start"):
    - cold-start users (<= COLD_START_THRESHOLD events) get the TF-IDF list
      written by train_content_based.py
    - everyone else gets the ALS list written by train_als_model.py
Each recommendation carries a "source" field ("als" / "content") so the
serving layer and the demo UI can show which model produced it.

Usage:
    python push_recommendations_to_redis.py
"""

import logging
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(HERE, "..", "utils"))
sys.path.append(os.path.join(HERE, "..", "..", "serving"))

from pyspark.sql import functions as F

from app.services.redis_client import RedisClient

from spark_session import get_spark
from train_als_model import TOP_N_OUTPUT_PATH
from train_content_based import CONTENT_RECS_OUTPUT

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 5_000  # users per Redis pipeline round trip


def route_recommendations(als_top_n, content_top_n):
    """(user_id, recommendations, source): content-based wins for the users it covers."""
    content = content_top_n.withColumn("source", F.lit("content"))
    als = als_top_n.join(content_top_n.select("user_id"), "user_id", "left_anti").withColumn("source", F.lit("als"))
    return als.unionByName(content)


def push_recommendations(top_n, redis_client: RedisClient) -> dict[str, int]:
    batch, pushed = {}, {"als": 0, "content": 0}
    for row in top_n.toLocalIterator():
        batch[str(row["user_id"])] = [
            {"item_id": rec["item_id"], "score": round(float(rec["rating"]), 6), "source": row["source"]}
            for rec in row["recommendations"]
        ]
        pushed[row["source"]] += 1
        if len(batch) >= BATCH_SIZE:
            redis_client.set_many_recommendations(batch)
            batch = {}
    if batch:
        redis_client.set_many_recommendations(batch)
    return pushed


def main():
    spark = get_spark("push_recommendations_to_redis")
    redis_client = RedisClient()
    redis_client.ping()  # fail fast if Redis is unreachable, before reading HDFS

    logger.info(f"Reading ALS top-N from {TOP_N_OUTPUT_PATH} and content-based top-N from {CONTENT_RECS_OUTPUT}")
    top_n = route_recommendations(spark.read.parquet(TOP_N_OUTPUT_PATH), spark.read.parquet(CONTENT_RECS_OUTPUT))

    pushed = push_recommendations(top_n, redis_client)
    logger.info(f"Pushed recommendations to Redis: {pushed['als']:,} users from ALS, "
                f"{pushed['content']:,} cold-start users from TF-IDF")
    spark.stop()


if __name__ == "__main__":
    main()
