"""
train_content_based.py

Content-based fallback for cold-start users (<= COLD_START_THRESHOLD events),
who ALS has too little history to personalise for.

Each item is described by tokens from its metadata, following the
category_code -> category_id -> unknown fallback from features.md:
    - every prefix of category_code ("cat:electronics", "cat:electronics.smartphone")
      so items sharing the full path score higher than items sharing only the root
    - "cid:<category_id>" (present even when category_code is blank)
    - "brand:<brand>"

Tokens are weighted with TF-IDF (binary TF, smoothed IDF) and L2-normalised,
so cosine similarity is just the sum of products over shared tokens — done
with joins on the sparse (item, token, weight) table, no dense vectors.

A cold-start user's profile is the interaction-weighted sum of the vectors of
the items they touched; candidates are the most popular items, ranked by
cosine similarity to the profile, ties and empty slots filled by popularity.

Output has the same schema as train_als_model.py's top-N, so
push_recommendations_to_redis.py can load it unchanged.

Usage:
    python train_content_based.py
"""

import logging
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(HERE, "..", "utils"))
sys.path.append(HERE)

from pyspark.sql import Window
from pyspark.sql import functions as F

from spark_session import get_spark
from train_als_model import INTERACTION_MATRIX_PATH, TOP_N
from weighting import COLD_START_THRESHOLD

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

RAW_EVENTS_PATH = "hdfs://namenode:8020/data/raw/user_events/*/*/*"
ITEM_FEATURES_PATH = "hdfs://namenode:8020/data/processed/item_features"
ITEM_TFIDF_OUTPUT = "hdfs://namenode:8020/data/processed/item_tfidf"
CONTENT_RECS_OUTPUT = "hdfs://namenode:8020/data/processed/content_based_recommendations"

# Only the most popular items are recommendation candidates — keeps the
# user x candidate join bounded and avoids surfacing obscure long-tail items
CANDIDATE_POOL_SIZE = 2000

# At most this many items of one brand before other brands get a turn
MAX_PER_BRAND = 3


def build_item_tfidf(raw_events):
    """(item_id, token, weight) with TF-IDF weights L2-normalised per item."""
    items = raw_events.groupBy(F.col("item_id").cast("int").alias("item_id")).agg(
        F.max("category_code").alias("category_code"),
        F.max("category_id").alias("category_id"),
        F.max("brand").alias("brand"),
    )

    category_prefixes = F.when(
        F.col("category_code").isNotNull(),
        F.expr(
            "transform(sequence(1, size(split(category_code, '[.]'))), "
            "i -> concat('cat:', array_join(slice(split(category_code, '[.]'), 1, i), '.')))"
        ),
    ).otherwise(F.array().cast("array<string>"))

    tokens = items.select(
        "item_id",
        F.explode(
            F.concat(
                category_prefixes,
                F.array(
                    F.when(F.col("category_id").isNotNull(), F.concat(F.lit("cid:"), F.col("category_id").cast("string"))),
                    F.when(F.col("brand").isNotNull(), F.concat(F.lit("brand:"), F.col("brand"))),
                ),
            )
        ).alias("token"),
    ).where(F.col("token").isNotNull()).distinct()

    n_items = items.count()
    idf = tokens.groupBy("token").agg(F.count("*").alias("doc_freq")).withColumn(
        "idf", F.log((F.lit(n_items) + 1) / (F.col("doc_freq") + 1)) + 1
    )

    weighted = tokens.join(idf, "token").select("item_id", "token", F.col("idf").alias("w"))
    norms = weighted.groupBy("item_id").agg(F.sqrt(F.sum(F.col("w") * F.col("w"))).alias("norm"))
    return weighted.join(norms, "item_id").select(
        "item_id", "token", (F.col("w") / F.col("norm")).alias("weight")
    )


def recommend_content_based(history, item_tfidf, item_popularity, k):
    """
    history:         (user_id, item_id, interaction_score) for the users to score
    item_tfidf:      output of build_item_tfidf
    item_popularity: (item_id, popularity_score) — top CANDIDATE_POOL_SIZE become candidates
    Returns (user_id, item_id, score, rank) for rank <= k, never an item the user already has.
    """
    candidates = item_popularity.orderBy(F.desc("popularity_score")).limit(CANDIDATE_POOL_SIZE)
    candidate_tfidf = item_tfidf.join(F.broadcast(candidates.select("item_id")), "item_id")

    raw_profiles = (
        history.join(item_tfidf, "item_id")
        .groupBy("user_id", "token")
        .agg(F.sum(F.col("interaction_score") * F.col("weight")).alias("profile_weight"))
    )
    # L2-normalise so the score is a true cosine similarity in [0, 1]
    profile_norms = raw_profiles.groupBy("user_id").agg(
        F.sqrt(F.sum(F.col("profile_weight") * F.col("profile_weight"))).alias("norm")
    ).where(F.col("norm") > 0)
    profiles = raw_profiles.join(profile_norms, "user_id").select(
        "user_id", "token", (F.col("profile_weight") / F.col("norm")).alias("profile_weight")
    )

    similarity = (
        profiles.join(candidate_tfidf, "token")
        .groupBy("user_id", "item_id")
        .agg(F.sum(F.col("profile_weight") * F.col("weight")).alias("score"))
    )

    # Popularity backfill (score 0) so every user gets k items even if no
    # candidate shares a token with their history
    backfill = history.select("user_id").distinct().crossJoin(
        F.broadcast(candidates.orderBy(F.desc("popularity_score")).limit(k + COLD_START_THRESHOLD).select("item_id"))
    ).withColumn("score", F.lit(0.0))

    scored = (
        similarity.unionByName(backfill)
        .groupBy("user_id", "item_id").agg(F.max("score").alias("score"))
        .join(history.select("user_id", "item_id"), ["user_id", "item_id"], "left_anti")
        .join(F.broadcast(candidates), "item_id")
    )

    # Brand diversity: same-category, same-brand items all tie at the top score
    # (TF-IDF can't tell two Samsung phones apart), so without a cap one popular
    # brand fills the whole list. Items past MAX_PER_BRAND are demoted below
    # the other brands, not dropped. Unbranded items are never capped.
    item_brand = candidate_tfidf.where(F.col("token").startswith("brand:")).select("item_id", F.col("token").alias("brand"))
    brand_window = Window.partitionBy("user_id", "brand_key").orderBy(F.desc("score"), F.desc("popularity_score"), "item_id")
    scored = (
        scored.join(F.broadcast(item_brand), "item_id", "left")
        .withColumn("brand_key", F.coalesce(F.col("brand"), F.col("item_id").cast("string")))
        .withColumn("over_brand_cap", F.row_number().over(brand_window) > MAX_PER_BRAND)
    )

    rank_window = Window.partitionBy("user_id").orderBy(
        "over_brand_cap", F.desc("score"), F.desc("popularity_score"), "item_id"
    )
    return (
        scored.withColumn("rank", F.row_number().over(rank_window))
        .where(F.col("rank") <= k)
        .select("user_id", "item_id", "score", "rank")
    )


def main():
    spark = get_spark("train_content_based")

    raw_events = spark.read.parquet(RAW_EVENTS_PATH)
    matrix = spark.read.parquet(INTERACTION_MATRIX_PATH)
    item_popularity = spark.read.parquet(ITEM_FEATURES_PATH).select(
        F.col("item_id").cast("int").alias("item_id"), "popularity_score"
    )

    item_tfidf = build_item_tfidf(raw_events).cache()
    item_tfidf.write.mode("overwrite").parquet(ITEM_TFIDF_OUTPUT)
    logger.info(
        f"Wrote TF-IDF vectors for {item_tfidf.select('item_id').distinct().count():,} items "
        f"({item_tfidf.select('token').distinct().count():,} tokens) to {ITEM_TFIDF_OUTPUT}"
    )

    cold_users = (
        raw_events.groupBy(F.col("user_id").cast("int").alias("user_id")).count()
        .where(F.col("count") <= COLD_START_THRESHOLD)
        .select("user_id")
    )
    history = matrix.join(cold_users, "user_id")
    logger.info(f"Cold-start users (<= {COLD_START_THRESHOLD} events): {cold_users.count():,}")

    recs = recommend_content_based(history, item_tfidf, item_popularity, TOP_N)

    # Same shape as ALS recommendForAllUsers, so push_recommendations_to_redis.py can read it
    top_n = recs.groupBy("user_id").agg(
        F.sort_array(F.collect_list(F.struct("rank", "item_id", F.col("score").cast("float").alias("rating"))))
        .alias("ranked")
    ).select("user_id", F.expr("transform(ranked, r -> named_struct('item_id', r.item_id, 'rating', r.rating))").alias("recommendations"))

    top_n.write.mode("overwrite").parquet(CONTENT_RECS_OUTPUT)
    logger.info(f"Wrote content-based top-{TOP_N} for {top_n.count():,} cold-start users to {CONTENT_RECS_OUTPUT}")

    top_n.show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
