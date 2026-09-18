"""
push_recommendations_to_redis.py

Loads the top-N recommendations written by train_als_model.py from HDFS
into Redis, where the FastAPI /recommendations/{user_id} endpoint serves
them from. Uses the serving layer's own RedisClient so the key format and
serialization are defined in exactly one place.

Usage:
    python push_recommendations_to_redis.py
"""

import logging
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(HERE, "..", "utils"))
sys.path.append(os.path.join(HERE, "..", "..", "serving"))

from app.services.redis_client import RedisClient

from spark_session import get_spark
from train_als_model import TOP_N_OUTPUT_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 5_000  # users per Redis pipeline round trip


def push_recommendations(top_n, redis_client: RedisClient) -> int:
    batch, pushed = {}, 0
    for row in top_n.toLocalIterator():
        batch[str(row["user_id"])] = [
            {"item_id": rec["item_id"], "score": round(float(rec["rating"]), 6)}
            for rec in row["recommendations"]
        ]
        if len(batch) >= BATCH_SIZE:
            redis_client.set_many_recommendations(batch)
            pushed += len(batch)
            batch = {}
    if batch:
        redis_client.set_many_recommendations(batch)
        pushed += len(batch)
    return pushed


def main():
    spark = get_spark("push_recommendations_to_redis")
    redis_client = RedisClient()
    redis_client.ping()  # fail fast if Redis is unreachable, before reading HDFS

    logger.info(f"Reading top-N recommendations from {TOP_N_OUTPUT_PATH}")
    top_n = spark.read.parquet(TOP_N_OUTPUT_PATH)

    pushed = push_recommendations(top_n, redis_client)
    logger.info(f"Pushed recommendations for {pushed:,} users to Redis")
    spark.stop()


if __name__ == "__main__":
    main()
