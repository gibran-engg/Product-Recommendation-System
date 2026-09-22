"""
train_als_model.py

Trains the ALS collaborative filtering model on the interaction matrix
built by build_interaction_matrix.py, using implicit feedback mode.

Usage:
    python train_als_model.py
"""

import logging
import os
import sys

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "utils"))

from pyspark.ml.recommendation import ALS

from spark_session import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

INTERACTION_MATRIX_PATH = "hdfs://namenode:8020/data/processed/interaction_matrix"
MODEL_OUTPUT_PATH = "hdfs://namenode:8020/models/als_latest"
TOP_N_OUTPUT_PATH = "hdfs://namenode:8020/data/processed/top_n_recommendations"

TOP_N = 20


def load_interaction_matrix(spark):
    logger.info(f"Reading interaction matrix from {INTERACTION_MATRIX_PATH}")
    df = spark.read.parquet(INTERACTION_MATRIX_PATH)
    logger.info(f"Loaded {df.count():,} interaction rows")
    return df


def train_test_split(df, train_ratio: float = 0.8):
    train_df, test_df = df.randomSplit([train_ratio, 1 - train_ratio], seed=42)
    logger.info(f"Train: {train_df.count():,} rows | Test: {test_df.count():,} rows")
    return train_df, test_df


def train_als(train_df):
    logger.info("Training ALS model (implicit feedback)...")
    als = ALS(
        userCol="user_id",
        itemCol="item_id",
        ratingCol="interaction_score",
        implicitPrefs=True,       # critical: treats scores as confidence, not ratings
        rank=20,                  # number of latent factors — tune later if needed
        maxIter=15,
        regParam=0.1,
        coldStartStrategy="drop", # drop NaN predictions for unseen users/items in eval
        nonnegative=True,         # interaction scores are non-negative, keep factors consistent
    )
    model = als.fit(train_df)
    logger.info("ALS training complete.")
    return model


def sanity_check_predictions(model, test_df):
    """Quick numerical red-flag check before deeper manual sanity checking."""
    predictions = model.transform(test_df)
    predictions = predictions.na.drop(subset=["prediction"])

    stats = predictions.selectExpr(
        "min(prediction) as min_pred",
        "max(prediction) as max_pred",
        "avg(prediction) as avg_pred"
    ).collect()[0]

    logger.info(
        f"Prediction stats — min: {stats['min_pred']:.4f}, "
        f"max: {stats['max_pred']:.4f}, avg: {stats['avg_pred']:.4f}"
    )

    if stats["min_pred"] == stats["max_pred"]:
        logger.warning("RED FLAG: all predictions are identical — model may not have trained properly.")


def main():
    spark = get_spark("train_als_model")

    df = load_interaction_matrix(spark)
    train_df, test_df = train_test_split(df)

    model = train_als(train_df)
    model.write().overwrite().save(MODEL_OUTPUT_PATH)
    logger.info(f"Saved model to {MODEL_OUTPUT_PATH}")

    sanity_check_predictions(model, test_df)

    logger.info(f"Generating top-{TOP_N} recommendations for all users...")
    top_n = model.recommendForAllUsers(TOP_N)
    top_n.write.mode("overwrite").parquet(TOP_N_OUTPUT_PATH)
    logger.info(f"Wrote top-N recommendations to {TOP_N_OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()