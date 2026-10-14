"""
export_serving_features.py

Last step of the batch pipeline: turns the HDFS outputs of the earlier jobs
into the read-optimised features the serving layer's hybrid re-ranker and the
demo storefront need, and writes them to Redis.

Written per item  (item:{id})
    - catalog metadata: brand, category_code, category_id, average price
    - behaviour: views / carts / purchases, popularity, conversion rate
    - TF-IDF tokens (from train_content_based.py)
    - ALS item factors (from the saved model)
    - "similar": top items by cosine similarity of ALS item embeddings
    - "also":    top items co-occurring in the same sessions (weighted by the
                 weaker of the two interactions, cosine-normalised)

Written per user  (user:{id})
    - event counts, cold-start segment, average price, recent items
    - weighted category / brand shares, L2-normalised TF-IDF profile
    - ALS user factors, interacted items (excluded from recommendations)

Global
    - catalog:popular, catalog:category:{root} sorted sets, catalog:categories
    - catalog:complements   category -> categories its shoppers also engage with
    - demo:users     a handful of warm and cold-start users to try in the UI
    - metrics:pipeline, metrics:evaluation

Usage:
    python export_serving_features.py
"""

import logging
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(HERE, "..", "utils"))
sys.path.append(os.path.join(HERE, "..", "..", "serving"))
sys.path.append(HERE)

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import Window
from pyspark.sql import functions as F

from app.services.redis_client import RedisClient

from build_interaction_matrix import ITEM_FEATURES_OUTPUT, apply_event_weights
from evaluate_model import METRICS_OUTPUT
from spark_session import get_spark
from train_als_model import ALS_PARAMS, INTERACTION_MATRIX_PATH, MODEL_OUTPUT_PATH, TOP_N, TOP_N_OUTPUT_PATH
from train_content_based import CANDIDATE_POOL_SIZE, CONTENT_RECS_OUTPUT, ITEM_TFIDF_OUTPUT, RAW_EVENTS_PATH
from weighting import COLD_START_THRESHOLD, EVENT_WEIGHTS, TIME_DECAY_HALF_LIFE_DAYS

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

NEIGHBOURS_K = 12           # similar / co-occurring items kept per item
# An item seen by a single user gets an ALS vector that is just that user's
# direction, so all of that user's items look ~100% similar to each other.
# Embedding neighbours are only trusted between items with this many users.
MIN_EMBEDDING_USERS = 3
COMPLEMENTS_K = 8           # complementary categories kept per category
# A category pair needs this many shoppers in common: one user who viewed a
# phone and a cultivator is noise, 20 who viewed a phone and headphones aren't
MIN_COMPLEMENT_USERS = 2
# Cosine alone over-rewards tiny categories (3 shared users made "saw" the top
# complement of headphones); scaling by users / (users + k) discounts thin evidence
COMPLEMENT_SHRINKAGE = 5
MAX_SESSION_ITEMS = 50      # longer sessions are bots or crawls, and blow up the pair join
TOP_SHARES = 5              # categories / brands kept per user profile
PROFILE_TOKENS = 25         # TF-IDF profile tokens kept per user
RECENT_ITEMS = 10
SEEN_ITEMS_CAP = 200
DEMO_WARM_USERS = 12
DEMO_COLD_USERS = 8
BATCH_SIZE = 5_000


def load_events(spark):
    raw = spark.read.parquet(RAW_EVENTS_PATH)
    events = raw.select(
        F.col("user_id").cast("int").alias("user_id"),
        F.col("item_id").cast("int").alias("item_id"),
        "event_type",
        # decimal first: a float64 category_id would otherwise stringify as 2.05e18
        F.col("category_id").cast("decimal(20,0)").cast("string").alias("category_id"),
        F.col("category_code"),
        F.col("brand"),
        F.col("price").cast("double").alias("price"),
        F.to_timestamp("timestamp").alias("ts"),
        F.col("session_id"),
    )
    return apply_event_weights(events).withColumn(
        # category_code -> category_id fallback, as in features.md
        "category", F.coalesce(F.col("category_code"), F.concat(F.lit("cid:"), F.col("category_id")))
    )


def round_vector(col, digits=5):
    return F.transform(col, lambda x: F.round(x, digits))


def dot(a, b):
    return F.aggregate(F.zip_with(a, b, lambda x, y: x * y), F.lit(0.0).cast("double"), lambda acc, x: acc + x)


def top_k(df, partition_col, order_col, k):
    window = Window.partitionBy(partition_col).orderBy(F.desc(order_col), "other_id")
    return df.withColumn("rank", F.row_number().over(window)).where(F.col("rank") <= k)


def collect_neighbours(df, extra_fields=()):
    """(item_id, other_id, score, rank, ...) -> (item_id, [{item_id, score, ...}] by rank)."""
    fields = [F.col("other_id").alias("item_id"), F.round("score", 4).alias("score"), *[F.col(f) for f in extra_fields]]
    return df.groupBy("item_id").agg(
        F.sort_array(F.collect_list(F.struct("rank", F.struct(*fields).alias("n")))).alias("ranked")
    ).select("item_id", F.expr("transform(ranked, r -> r.n)").alias("neighbours"))


def als_similar_items(item_factors, item_popularity, matrix):
    """Cosine similarity of ALS item embeddings against the popular candidate pool."""
    supported = matrix.groupBy("item_id").agg(F.countDistinct("user_id").alias("n_users")).where(
        F.col("n_users") >= MIN_EMBEDDING_USERS
    )
    normed = item_factors.select(F.col("id").alias("item_id"), "features").join(supported.select("item_id"), "item_id").withColumn(
        "norm", F.sqrt(dot(F.col("features"), F.col("features")))
    ).where(F.col("norm") > 0)
    candidates = normed.join(
        F.broadcast(item_popularity.orderBy(F.desc("popularity_score")).limit(CANDIDATE_POOL_SIZE).select("item_id")),
        "item_id",
    ).select(F.col("item_id").alias("other_id"), F.col("features").alias("other_features"), F.col("norm").alias("other_norm"))
    pairs = normed.crossJoin(F.broadcast(candidates)).where(F.col("item_id") != F.col("other_id"))
    scored = pairs.select(
        "item_id", "other_id",
        (dot(F.col("features"), F.col("other_features")) / (F.col("norm") * F.col("other_norm"))).alias("score"),
    ).where(F.col("score") > 0)
    return collect_neighbours(top_k(scored, "item_id", "score", NEIGHBOURS_K)).withColumnRenamed("neighbours", "similar")


def session_cooccurrence(events):
    """Items engaged with in the same session. Pair weight = the weaker interaction (view 1, cart 3, purchase 5)."""
    session_items = (
        events.groupBy("session_id", "item_id").agg(F.max("event_weight").alias("w")).where(F.col("w") > 0)
    )
    sizes = session_items.groupBy("session_id").count().where(F.col("count").between(2, MAX_SESSION_ITEMS))
    session_items = session_items.join(sizes.select("session_id"), "session_id").cache()
    totals = session_items.groupBy("item_id").agg(F.sum("w").alias("total"))

    a, b = session_items.alias("a"), session_items.alias("b")
    pairs = a.join(b, (F.col("a.session_id") == F.col("b.session_id")) & (F.col("a.item_id") != F.col("b.item_id"))).select(
        F.col("a.item_id").alias("item_id"), F.col("b.item_id").alias("other_id"), F.least("a.w", "b.w").alias("w")
    )
    co = pairs.groupBy("item_id", "other_id").agg(F.sum("w").alias("co_w"), F.count("*").alias("sessions"))
    scored = (
        co.join(totals, "item_id")
        .join(totals.select(F.col("item_id").alias("other_id"), F.col("total").alias("other_total")), "other_id")
        # co_w <= min(total, other_total), so this cosine-style score is in (0, 1]
        .withColumn("score", F.col("co_w") / F.sqrt(F.col("total") * F.col("other_total")))
    )
    return collect_neighbours(top_k(scored, "item_id", "score", NEIGHBOURS_K), ["sessions"]).withColumnRenamed("neighbours", "also")


def category_complements(events):
    """
    Complementary categories: shoppers who engaged with category A also engaged
    with category B. User-level (not session-level) co-occurrence, because the
    same shopper often browses accessories in a later session. Scored like the
    item co-occurrence, shared users / sqrt(users_a * users_b), then shrunk by
    users / (users + COMPLEMENT_SHRINKAGE) so pairs backed by few shoppers rank lower.
    """
    user_categories = events.where(F.col("category_code").isNotNull() & (F.col("event_weight") > 0)).select(
        "user_id", "category_code"
    ).distinct().cache()
    sizes = user_categories.groupBy("category_code").agg(F.count("*").alias("n"))
    a, b = user_categories.alias("a"), user_categories.alias("b")
    pairs = (
        a.join(b, (F.col("a.user_id") == F.col("b.user_id")) & (F.col("a.category_code") != F.col("b.category_code")))
        .groupBy(F.col("a.category_code").alias("category"), F.col("b.category_code").alias("other"))
        .agg(F.count("*").alias("users"))
        .where(F.col("users") >= MIN_COMPLEMENT_USERS)
    )
    scored = (
        pairs.join(sizes.select(F.col("category_code").alias("category"), F.col("n").alias("n_a")), "category")
        .join(sizes.select(F.col("category_code").alias("other"), F.col("n").alias("n_b")), "other")
        .withColumn("score", F.col("users") / F.sqrt(F.col("n_a") * F.col("n_b"))
                    * F.col("users") / (F.col("users") + F.lit(COMPLEMENT_SHRINKAGE)))
    )
    window = Window.partitionBy("category").orderBy(F.desc("score"), "other")
    rows = scored.withColumn("rank", F.row_number().over(window)).where(F.col("rank") <= COMPLEMENTS_K).collect()

    complements: dict[str, list[dict]] = {}
    for r in sorted(rows, key=lambda r: (r["category"], r["rank"])):
        complements.setdefault(r["category"], []).append(
            {"category": r["other"], "score": round(r["score"], 4), "users": r["users"]}
        )
    return complements


def build_items(events, item_features, item_tfidf, item_factors):
    metadata = events.groupBy("item_id").agg(
        F.max("brand").alias("brand"),
        F.max("category_code").alias("category_code"),
        F.max("category_id").alias("category_id"),
        F.round(F.avg("price"), 2).alias("price"),
        F.sum((F.col("event_type") == "view").cast("int")).alias("views"),
        F.sum((F.col("event_type") == "cart").cast("int")).alias("carts"),
        F.sum((F.col("event_type") == "purchase").cast("int")).alias("purchases"),
        F.countDistinct("user_id").alias("unique_users"),
    )
    tokens = item_tfidf.groupBy("item_id").agg(
        F.map_from_entries(F.collect_list(F.struct("token", F.round("weight", 4)))).alias("tokens")
    )
    factors = item_factors.select(F.col("id").alias("item_id"), round_vector(F.col("features")).alias("factors"))
    return (
        metadata.join(item_features, "item_id", "left")
        .join(tokens, "item_id", "left")
        .join(factors, "item_id", "left")
    )


def share_list(events, user_col, key_col, out_name):
    """Per user: top keys by share of the user's weighted interactions."""
    weighted = events.where(F.col(key_col).isNotNull()).groupBy(user_col, key_col).agg(F.sum("event_weight").alias("w"))
    totals = weighted.groupBy(user_col).agg(F.sum("w").alias("total"))
    window = Window.partitionBy(user_col).orderBy(F.desc("w"), key_col)
    ranked = (
        weighted.join(totals, user_col).where(F.col("total") > 0)
        .withColumn("rank", F.row_number().over(window)).where(F.col("rank") <= TOP_SHARES)
    )
    return ranked.groupBy(user_col).agg(
        F.sort_array(F.collect_list(F.struct(
            "rank", F.struct(F.col(key_col).alias("key"), F.round(F.col("w") / F.col("total"), 4).alias("share")).alias("s")
        ))).alias("ranked")
    ).select(user_col, F.expr("transform(ranked, r -> r.s)").alias(out_name))


def build_users(events, matrix, item_tfidf, user_factors):
    stats = events.groupBy("user_id").agg(
        F.count("*").alias("n_events"),
        F.sum((F.col("event_type") == "view").cast("int")).alias("views"),
        F.sum((F.col("event_type") == "cart").cast("int")).alias("carts"),
        F.sum((F.col("event_type") == "purchase").cast("int")).alias("purchases"),
        F.countDistinct("item_id").alias("n_items"),
        F.countDistinct("session_id").alias("n_sessions"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.date_format(F.min("ts"), "yyyy-MM-dd HH:mm").alias("first_seen"),
        F.date_format(F.max("ts"), "yyyy-MM-dd HH:mm").alias("last_seen"),
    ).withColumn("segment", F.when(F.col("n_events") <= COLD_START_THRESHOLD, "cold_start").otherwise("warm"))

    recent_window = Window.partitionBy("user_id").orderBy(F.desc("ts"), "item_id")
    recent = (
        events.withColumn("rank", F.row_number().over(recent_window)).where(F.col("rank") <= RECENT_ITEMS)
        .groupBy("user_id").agg(F.sort_array(F.collect_list(F.struct(
            "rank", F.struct("item_id", "event_type", F.date_format("ts", "yyyy-MM-dd HH:mm").alias("at")).alias("e")
        ))).alias("ranked"))
        .select("user_id", F.expr("transform(ranked, r -> r.e)").alias("recent"))
    )

    seen = matrix.groupBy("user_id").agg(F.slice(F.collect_list("item_id"), 1, SEEN_ITEMS_CAP).alias("seen"))

    # Same profile construction as train_content_based.recommend_content_based
    raw_profile = matrix.join(item_tfidf, "item_id").groupBy("user_id", "token").agg(
        F.sum(F.col("interaction_score") * F.col("weight")).alias("w")
    )
    norms = raw_profile.groupBy("user_id").agg(F.sqrt(F.sum(F.col("w") * F.col("w"))).alias("norm")).where(F.col("norm") > 0)
    profile_window = Window.partitionBy("user_id").orderBy(F.desc("w"), "token")
    profile = (
        raw_profile.join(norms, "user_id").withColumn("w", F.col("w") / F.col("norm"))
        .withColumn("rank", F.row_number().over(profile_window)).where(F.col("rank") <= PROFILE_TOKENS)
        .groupBy("user_id").agg(F.map_from_entries(F.collect_list(F.struct("token", F.round("w", 4)))).alias("profile"))
    )

    factors = user_factors.select(F.col("id").alias("user_id"), round_vector(F.col("features")).alias("factors"))

    return (
        stats.join(recent, "user_id", "left")
        .join(share_list(events, "user_id", "category", "categories"), "user_id", "left")
        .join(share_list(events, "user_id", "brand", "brands"), "user_id", "left")
        .join(seen, "user_id", "left")
        .join(profile, "user_id", "left")
        .join(factors, "user_id", "left")
    )


def write_rows(df, key_prefix, key_col, redis_client):
    batch, written = {}, 0
    for row in df.toLocalIterator():
        value = row.asDict(recursive=True)
        batch[f"{key_prefix}:{value[key_col]}"] = value
        if len(batch) >= BATCH_SIZE:
            redis_client.set_many_json(batch)
            written += len(batch)
            batch = {}
    if batch:
        redis_client.set_many_json(batch)
        written += len(batch)
    return written


def write_catalog_indexes(items, redis_client):
    rows = items.select(
        "item_id", F.coalesce("popularity_score", F.lit(0.0)).alias("p"),
        F.coalesce(F.split("category_code", "[.]").getItem(0), F.lit("other")).alias("root"),
    ).collect()
    redis_client.replace_sorted_set("catalog:popular", {str(r["item_id"]): float(r["p"]) for r in rows})

    by_root: dict[str, dict[str, float]] = {}
    for r in rows:
        by_root.setdefault(r["root"], {})[str(r["item_id"])] = float(r["p"])
    for root, scores in by_root.items():
        redis_client.replace_sorted_set(f"catalog:category:{root}", scores)

    categories = sorted(
        ({"key": root, "n_items": len(scores), "popularity": round(sum(scores.values()), 2)} for root, scores in by_root.items()),
        key=lambda c: -c["popularity"],
    )
    redis_client.set_many_json({"catalog:categories": categories})
    return len(rows), len(categories)


def pick_demo_users(users, rec_sources):
    """The most active warm users with ALS recs + a spread of cold-start users with TF-IDF recs."""
    summary = users.join(rec_sources, "user_id").select(
        "user_id", "segment", "source", "n_events", "views", "carts", "purchases",
        F.col("categories").getItem(0).getField("key").alias("top_category"),
    )
    warm = summary.where((F.col("segment") == "warm") & (F.col("source") == "als")).orderBy(
        F.desc("purchases"), F.desc("carts"), F.desc("n_events"), "user_id"
    ).limit(DEMO_WARM_USERS)
    cold = summary.where(F.col("source") == "content").orderBy(F.desc("n_events"), F.desc("carts"), "user_id").limit(DEMO_COLD_USERS)
    return [r.asDict() for r in warm.collect() + cold.collect()]


def pipeline_metrics(events, matrix, items, rec_sources, top_n_items, n_users):
    counts = {r["event_type"]: r["count"] for r in events.groupBy("event_type").count().collect()}
    bounds = events.agg(F.min("ts").alias("start"), F.max("ts").alias("end")).collect()[0]
    per_hour = events.groupBy(F.date_format("ts", "yyyy-MM-dd HH:00").alias("hour")).count().orderBy("hour").collect()
    top_categories = (
        events.groupBy(F.coalesce(F.split("category_code", "[.]").getItem(0), F.lit("other")).alias("root"))
        .agg(F.count("*").alias("events"), F.sum((F.col("event_type") == "purchase").cast("int")).alias("purchases"))
        .orderBy(F.desc("events")).limit(10).collect()
    )
    n_items = items.count()
    n_pairs = matrix.count()
    sources = {r["source"]: r["count"] for r in rec_sources.groupBy("source").count().collect()}
    user_events = events.groupBy("user_id").count()
    cold_users = user_events.where(F.col("count") <= COLD_START_THRESHOLD).count()
    recommended_items = top_n_items.distinct().count()

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "events": sum(counts.values()),
        "event_types": counts,
        "date_range": [bounds["start"].isoformat(sep=" "), bounds["end"].isoformat(sep=" ")],
        "events_per_hour": [{"hour": r["hour"], "events": r["count"]} for r in per_hour],
        "users": n_users,
        "items": n_items,
        "interaction_pairs": n_pairs,
        "matrix_density": round(n_pairs / max(n_users * n_items, 1), 6),
        "sessions": events.select("session_id").distinct().count(),
        "cold_start_users": cold_users,
        "cold_start_threshold": COLD_START_THRESHOLD,
        "users_with_recs": sources,
        "recommended_catalog_coverage": round(recommended_items / max(n_items, 1), 4),
        "top_categories": [r.asDict() for r in top_categories],
        "config": {
            "event_weights": EVENT_WEIGHTS,
            "time_decay_half_life_days": TIME_DECAY_HALF_LIFE_DAYS,
            "als": ALS_PARAMS,
            "top_n": TOP_N,
            "content_candidate_pool": CANDIDATE_POOL_SIZE,
        },
    }


def evaluation_metrics(spark):
    try:
        rows = spark.read.parquet(METRICS_OUTPUT).collect()
    except Exception as exc:  # evaluate_model.py not run yet — the UI shows "not available"
        logger.warning(f"No evaluation metrics at {METRICS_OUTPUT}: {exc}")
        return []
    return [
        {**r.asDict(), "evaluated_at": r["evaluated_at"].isoformat(sep=" ", timespec="seconds")}
        for r in sorted(rows, key=lambda r: (r["segment"], r["k"], r["model"]))
    ]


def main():
    spark = get_spark("export_serving_features")
    redis_client = RedisClient()
    redis_client.ping()

    events = load_events(spark).cache()
    matrix = spark.read.parquet(INTERACTION_MATRIX_PATH).cache()
    item_features = spark.read.parquet(ITEM_FEATURES_OUTPUT).select(
        F.col("item_id").cast("int").alias("item_id"), F.round("popularity_score", 3).alias("popularity_score"),
        F.round("conversion_rate", 4).alias("conversion_rate"),
    )
    item_tfidf = spark.read.parquet(ITEM_TFIDF_OUTPUT).cache()
    model = ALSModel.load(MODEL_OUTPUT_PATH)

    als_top_n = spark.read.parquet(TOP_N_OUTPUT_PATH)
    content_top_n = spark.read.parquet(CONTENT_RECS_OUTPUT)
    rec_sources = (
        content_top_n.select("user_id", F.lit("content").alias("source"))
        .unionByName(als_top_n.join(content_top_n.select("user_id"), "user_id", "left_anti").select("user_id", F.lit("als").alias("source")))
    )
    top_n_items = als_top_n.unionByName(content_top_n).select(F.explode("recommendations").alias("r")).select("r.item_id")

    logger.info("Building item features (metadata, TF-IDF, ALS embeddings, neighbours)...")
    items = (
        build_items(events, item_features, item_tfidf, model.itemFactors)
        .join(als_similar_items(model.itemFactors, item_features, matrix), "item_id", "left")
        .join(session_cooccurrence(events), "item_id", "left")
        .cache()
    )
    n_items = write_rows(items, "item", "item_id", redis_client)
    n_indexed, n_roots = write_catalog_indexes(items, redis_client)
    logger.info(f"Wrote {n_items:,} items ({n_indexed:,} indexed across {n_roots} top-level categories)")

    logger.info("Building user profiles...")
    users = build_users(events, matrix, item_tfidf, model.userFactors).cache()
    n_users = write_rows(users, "user", "user_id", redis_client)
    logger.info(f"Wrote {n_users:,} user profiles")

    complements = category_complements(events)
    redis_client.set_many_json({"catalog:complements": complements})
    logger.info(f"Wrote complementary categories for {len(complements):,} categories")

    demo_users = pick_demo_users(users, rec_sources)
    metrics = pipeline_metrics(events, matrix, items, rec_sources, top_n_items, n_users)
    evaluation = evaluation_metrics(spark)
    redis_client.set_many_json({
        "demo:users": demo_users,
        "metrics:pipeline": metrics,
        "metrics:evaluation": evaluation,
    })
    logger.info(f"Wrote {len(demo_users)} demo users, pipeline metrics and {len(evaluation)} evaluation rows")
    spark.stop()


if __name__ == "__main__":
    main()
