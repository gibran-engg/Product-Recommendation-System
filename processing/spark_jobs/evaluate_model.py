"""
evaluate_model.py

Offline evaluation: Precision@K, Recall@K, hit-rate@K and catalog coverage
on a hold-out split of the interaction matrix.

Uses the same 80/20 split and ALS hyperparameters as train_als_model.py, but
trains its own model on the train half so the test half is genuinely unseen.
Items a user already has in train are never counted as recommendations.

Compared models:
    - ALS                       (all evaluable users)
    - Popularity baseline       (the A/B control — beat this or ALS isn't earning its keep)
    - Content-based TF-IDF      (cold-start segment only, which is what it is for)

Usage:
    python evaluate_model.py
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
from train_als_model import load_interaction_matrix, train_als, train_test_split
from train_content_based import RAW_EVENTS_PATH, build_item_tfidf, recommend_content_based
from weighting import COLD_START_THRESHOLD

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

METRICS_OUTPUT = "hdfs://namenode:8020/data/processed/evaluation_metrics"

K_VALUES = [10, 20]
MAX_K = max(K_VALUES)


def exclude_seen_and_rank(candidates, train_df, k):
    """candidates: (user_id, item_id, score) -> top-k per user, minus items already in train."""
    unseen = candidates.join(train_df.select("user_id", "item_id"), ["user_id", "item_id"], "left_anti")
    rank_window = Window.partitionBy("user_id").orderBy(F.desc("score"), "item_id")
    return unseen.withColumn("rank", F.row_number().over(rank_window)).where(F.col("rank") <= k)


def als_recommendations(model, users, train_df):
    # Over-fetch so there are still MAX_K left after dropping already-seen items
    max_seen = train_df.groupBy("user_id").count().agg(F.max("count")).collect()[0][0]
    raw = model.recommendForUserSubset(users, MAX_K + max_seen)
    exploded = raw.select("user_id", F.explode("recommendations").alias("r")).select(
        "user_id", F.col("r.item_id").alias("item_id"), F.col("r.rating").alias("score")
    )
    return exclude_seen_and_rank(exploded, train_df, MAX_K)


def popularity_recommendations(item_popularity, users, train_df):
    max_seen = train_df.groupBy("user_id").count().agg(F.max("count")).collect()[0][0]
    top_items = item_popularity.orderBy(F.desc("popularity_score")).limit(MAX_K + max_seen)
    candidates = users.crossJoin(
        F.broadcast(top_items.select("item_id", F.col("popularity_score").alias("score")))
    )
    return exclude_seen_and_rank(candidates, train_df, MAX_K)


def compute_metrics(recs, truth, users, n_catalog_items, model_name, segment):
    """recs: (user_id, item_id, rank); truth: (user_id, item_id). Users with no recs score 0."""
    truth_sizes = truth.groupBy("user_id").agg(F.count("*").alias("n_relevant"))
    rows = []
    for k in K_VALUES:
        top_k = recs.where(F.col("rank") <= k)
        hits = top_k.join(truth, ["user_id", "item_id"]).groupBy("user_id").agg(F.count("*").alias("hits"))
        per_user = (
            users.join(truth_sizes, "user_id")
            .join(hits, "user_id", "left")
            .fillna(0, subset=["hits"])
            .select(
                (F.col("hits") / F.lit(k)).alias("precision"),
                (F.col("hits") / F.col("n_relevant")).alias("recall"),
                (F.col("hits") > 0).cast("double").alias("hit"),
            )
        )
        agg = per_user.agg(
            F.count("*").alias("n_users"),
            F.avg("precision").alias("precision"),
            F.avg("recall").alias("recall"),
            F.avg("hit").alias("hit_rate"),
        ).collect()[0]
        coverage = top_k.select("item_id").distinct().count() / n_catalog_items
        rows.append((segment, model_name, k, int(agg["n_users"]),
                     float(agg["precision"] or 0), float(agg["recall"] or 0),
                     float(agg["hit_rate"] or 0), float(coverage)))
    return rows


def main():
    spark = get_spark("evaluate_model")

    matrix = load_interaction_matrix(spark)
    train_df, test_df = train_test_split(matrix)
    train_df, test_df = train_df.cache(), test_df.cache()

    # Only users with history in train can be ranked by ALS; their unseen-in-train
    # test items are the ground truth (test items absent from train stay in, honestly
    # lowering recall — no model could have recommended them)
    users = test_df.select("user_id").distinct().join(train_df.select("user_id").distinct(), "user_id").cache()
    truth = test_df.join(users, "user_id").select("user_id", "item_id")
    n_catalog_items = train_df.select("item_id").distinct().count()

    train_counts = train_df.groupBy("user_id").count()
    cold_users = users.join(train_counts.where(F.col("count") <= COLD_START_THRESHOLD), "user_id").select("user_id").cache()
    cold_truth = truth.join(cold_users, "user_id")
    logger.info(f"Evaluable users: {users.count():,} (cold-start <= {COLD_START_THRESHOLD} train items: "
                f"{cold_users.count():,}); catalog: {n_catalog_items:,} items")

    item_popularity = train_df.groupBy("item_id").agg(F.sum("interaction_score").alias("popularity_score"))

    model = train_als(train_df)
    als_recs = als_recommendations(model, users, train_df).cache()
    pop_recs = popularity_recommendations(item_popularity, users, train_df).cache()

    item_tfidf = build_item_tfidf(spark.read.parquet(RAW_EVENTS_PATH)).cache()
    content_recs = recommend_content_based(
        train_df.join(cold_users, "user_id"), item_tfidf, item_popularity, MAX_K
    ).cache()

    results = []
    results += compute_metrics(als_recs, truth, users, n_catalog_items, "als", "all")
    results += compute_metrics(pop_recs, truth, users, n_catalog_items, "popularity", "all")
    results += compute_metrics(als_recs.join(cold_users, "user_id"), cold_truth, cold_users, n_catalog_items, "als", "cold_start")
    results += compute_metrics(pop_recs.join(cold_users, "user_id"), cold_truth, cold_users, n_catalog_items, "popularity", "cold_start")
    results += compute_metrics(content_recs, cold_truth, cold_users, n_catalog_items, "content_tfidf", "cold_start")

    metrics = spark.createDataFrame(
        results,
        "segment string, model string, k int, n_users int, precision double, recall double, hit_rate double, coverage double",
    ).withColumn("evaluated_at", F.current_timestamp())
    metrics.write.mode("overwrite").parquet(METRICS_OUTPUT)
    logger.info(f"Wrote evaluation metrics to {METRICS_OUTPUT}")

    metrics.drop("evaluated_at").orderBy("segment", "k", "model").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
