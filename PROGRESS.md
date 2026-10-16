# Progress log

Dated build log: what was done, decisions, and measured results. Newest entries at the bottom.

## 13/09/2026 — 12:00 pm

Gibran has verified the required dataset, cleaned it with python script, written the json schema for the same, and set up docker

## 13/09/2026 — 03:30 pm

Gibran has established primary pipeline connection from kafka to hdfs using docker container,
Analysed the 'cleaned_events.csv' under data\dataset\cleaned using exploratory data anaylsis in 'eda.ipynb' under notebooks\eda.ipynb
Listed the findings in a markdown below the cells, need to collaborate and engineer additional features for the system.

## 13/09/2026 — 11:00 pm

Meet has finalized the Docker-based systems infrastructure for the recommendation system,
including PostgreSQL, Redis, Airflow, FastAPI, Prometheus, Grafana, Kafka, and HDFS services
using Docker Compose.

Meet has configured and verified the Airflow skeleton DAG with a nightly schedule for the
recommendation pipeline, establishing the orchestration structure for future ingestion,
feature engineering, model training, and cache refresh tasks.

Meet has implemented and verified the FastAPI serving layer with health and root endpoints,
along with a dummy `/recommendations/{user_id}` endpoint returning static recommendations
as required for the Phase 1 milestone.

## 14/09/2026 — 03:00 am

Meet has verified that the complete Docker Compose environment is running successfully and
that the FastAPI recommendation endpoint is accessible through the API service.

## 14/09/2026 — 01:30 pm

Implemented Redis integration with connection, recommendation storage, and retrieval methods,
and verified API ↔ Redis communication.
Updated FastAPI /recommendations/{user_id} to serve cached recommendations from Redis with
proper 404 handling for users without cached data.

## 17/09/2026 — 05:45 pm

Gibran has tested the PySpark feature engineering job ('build_interaction_matrix.py') end to
end, applying the finalized event weighting (purchase=5, cart=3, view=1) and 14-day-half-life
time decay to a 10,000-event sample from 'cleaned_events.csv', matching the volume
'kafka_producer.py' streams per test run. The job correctly collapsed events into a
user-item interaction matrix — 7,028 unique (user, item) pairs across 2,868 users and 4,145
items, interaction_score range 1.0-22.0 — plus the item_features by-product
(popularity_score, conversion_rate). Note: the live Docker/Kafka/HDFS stack on this machine
wasn't reachable from this session, so the job's actual transformation code was imported and
run unmodified against a local stand-in for the HDFS input (no hdfs://namenode paths or
architecture touched) — this validates the logic, not the live Kafka-to-HDFS wiring, which
still needs a run inside the real stack to fully close out.

Trained the ALS model ('train_als_model.py') on that matrix (implicit feedback, rank=20,
maxIter=15) — prediction stats came out non-degenerate (min 0.0, max 0.71, avg 0.05, not the
identical-everywhere red flag the script checks for). Ran 'sanity_check.py' against 5 sampled
users with real interaction history: recommendation lists differed across all 5 (no
"identical for everyone" red flag), and 3 of 5 showed direct category overlap with the
user's actual browsing history, the other 2 flagged for manual review as the script intends.

Self-check against the Phase 2 milestone ("ALS model produces real recommendations for a
test user"): met on the Data+ML side. Flagging two things for the team: (1) some
recommendation lists lean on a few popular categories (e.g. electronics.smartphone) more
than expected — likely a sample-size artifact of the 10k-row/2.5-hour test slice rather than
a bug, worth re-checking once run against a larger slice; (2) 'nightly_retrain_dag.py' is
still the Phase 1 placeholder (tagged "phase-1", no real task wired) — the other half of the
Phase 2 milestone ("Airflow can trigger the pipeline end-to-end") is still open on the
Systems/Serving side.

## 19/09/2026 — 02:00 am

Dataset re-cleaned with SQL -> data/dataset/cleaned/final_cleaned_dataset.csv (3,582,337 rows,
Oct 1-5 2019, no nulls). The old cleaned_events.csv and its results are invalid.

Removed duplicated/obsolete test code: processing/spark_jobs/local_test/, testing/,
data/raw/clean_raw_data.ipynb (superseded by the SQL cleaning), data/raw/csv_to_para.ipynb.

Feature engineering verified on the full new dataset against an independent pandas
implementation: 1,995,903 (user,item) pairs, exact match (max diff 3e-13); weights 1/3/5 and
14-day decay (0.820..1.0 across Oct 1-5) correct; popularity + conversion rate exact.
ALS on the full dataset: hold-out hit-rate@20 19.18% vs 12.58% popularity baseline (ALS better).
Known weaknesses for Phase 3: only 0.76% of the catalog is ever recommended, top item reaches
39% of users, 6.7% of recommended items were already interacted with (no recency filter).

Production path fixed and run end to end on the live Docker stack:
kafka_producer -> Kafka -> hdfs_writer -> HDFS -> build_interaction_matrix -> train_als_model
-> sanity_check -> push_recommendations_to_redis (new) -> Redis -> FastAPI (200 for a real
user, 404 for unknown). Fixes: producer pointed at the new CSV; hdfs_writer now flushes the
tail buffer when idle and commits Kafka offsets only after HDFS writes; spark_session uses
HDFS user root (writes were denied), datanode hostnames (host access) and UTC (decay was
timezone-dependent). Old-dataset events moved in HDFS to /archive/user_events_old_dataset_13092026.

OPEN (@Meet): nightly_retrain_dag.py now chains the 4 Spark jobs and docker-compose mounts
processing/ + serving/ into the airflow container, but the airflow image build
(Java 17 + pyspark 4.2.0) was not finished, so the DAG has NOT been run yet. To verify:
docker compose build airflow && docker compose up -d airflow, then trigger nightly_retrain.

## 11/10/2026

Phase 3 (Data+ML) built and run on the live stack, inside the airflow container:
- train_content_based.py: TF-IDF over item metadata (category_code prefixes, category_id,
  brand) for cold-start users (<= 3 events). Cosine similarity to the user's profile over
  the top 2,000 popular items, already-seen items excluded, max 3 items per brand
  (otherwise every smartphone viewer got 10 identical-score Samsungs), popularity backfill.
  Output: /data/processed/content_based_recommendations (same schema as ALS top-N),
  /data/processed/item_tfidf. 1,782 cold-start users covered.
- cold_start_sanity_check.py: prints history vs recommendations (category/brand) for 5
  cold-start users — 5/5 got recommendations in a category from their history.
- evaluate_model.py: Precision/Recall/hit-rate@K and coverage on an 80/20 hold-out
  (own ALS trained on the 80%). Output: /data/processed/evaluation_metrics.
  Results on the 10k-event slice (761 evaluable users, 495 cold-start), K=10:
      all users:   ALS hit 11.6% / recall 9.0% / coverage 8.1%
                   popularity hit 10.2% / recall 8.0% / coverage 0.5%
      cold-start:  TF-IDF hit 20.0% / recall 17.1% / coverage 30.5%
                   ALS hit 10.9%, popularity hit 10.1%
  TF-IDF ~2x popularity/ALS for cold-start. ALS vs popularity on all users is within noise
  on this tiny slice — re-run on a larger slice before quoting.

Pipeline fixes:
- nightly_retrain (@Meet, your file): DAG failed writing to HDFS as user "airflow" —
  spark-submit starts the JVM before the job's Python, so spark_session.py's
  HADOOP_USER_NAME comes too late. Added HADOOP_USER_NAME=root to the bash_command.
  DAG was also paused (runs stuck "queued"); unpaused. Full run now succeeds:
  10,000 events -> 7,011 pairs -> ALS -> 2,485 users in Redis -> API 200 (20 recs).
- HDFS held the same 10k events 3x (producer always re-sends the first SAMPLE_SIZE rows).
  Moved the 2 older copies to /archive/user_events_dup_11102026. Don't re-run the producer
  until it is made idempotent.

OPEN: push_recommendations_to_redis.py still pushes ALS only — route cold-start users to the
content-based output. New jobs are not in the DAG yet (@Meet).

## 14/10/2026

Closed the OPEN items from 11/10 and built the demo layer:
- push_recommendations_to_redis.py routes cold-start users to the TF-IDF output and everyone else to ALS.
- export_serving_features.py exports the catalog, user profiles, complements and pipeline metrics to Redis.
- The DAG and run.py share one job list (orchestration/airflow/dags/pipeline_jobs.py): 8 jobs, same order.
- FastAPI hybrid re-ranker (serving/app/services/ranker.py) blends ALS, co-purchase, content, session, category,
  price, brand and popularity factors, with a variety pass so one category can't fill the list.
- Demo storefront (serving/web/) with a debug panel showing each item's factor breakdown, and an Insights page.
- run.py brings the whole stack up in order; howtorun.md is the step-by-step runbook.

## 16/10/2026

Re-ingested a 150,000-event sample (Oct 3 16:59 → Oct 4 01:15) and ran the full pipeline:
25,046 users, 15,539 items, 93,235 (user, item) pairs, 53.7% cold-start, 24,859 users with recommendations in Redis.

Offline evaluation, K = 10:
- All users (n = 8,797): ALS hit rate 20.3% / recall 14.6% vs popularity 13.9% / 10.0%.
- Cold-start (n = 4,356): TF-IDF hit rate 28.3% / recall 24.1% vs ALS 20.4% and popularity 15.1%.

End-to-end test of the storefront (feeds, product page, cart, Insights, debug panel, edge cases): all working,
30–95 ms per recommendation request. Two bugs found and fixed:
- The cart was shared by every demo user (one localStorage key). It is now stored per user.
- The ALS factor was divided by the best candidate's score, so a cold-start user's weak 0.26 match showed as 1.0
  and dominated the ranking. Implicit ALS already predicts a 0..1 preference, so it is now clipped to 0..1 instead.

Known caveats, not fixed (both need a full pipeline re-run):
- The 14-day time-decay half-life has no effect on an 8-hour sample.
- evaluate_model.py uses a random 80/20 split, not a time-based one, so the offline metrics are likely optimistic.

Repo cleanup:
- Docs merged into three files: README.md (reference), howtorun.md (runbook), PROGRESS.md (this log, was progress.txt).
  Removed Recommendation_System_Project_Plan.md (outdated; its goal and team split are now in the README) and the
  empty bughdfs.txt. README results updated to the 150k run.
- Removed 15 empty placeholder files left over from the original folder plan (realtime/, empty tests, schemas.py,
  health.py, ab_testing.py, topics.py, empty requirements files, offline_eval_playground.ipynb).
- tests/explore.py moved to processing/explore_hdfs.py: it reads hdfs://namenode, so it only runs inside the stack.

Two config bugs fixed:
- .env was committed before .gitignore listed it, so git kept tracking it (it was empty, so no secrets leaked).
  Untracked it (the file stays on disk). .gitignore also ignored .env.example, the template run.py copies to create
  .env; it is now explicitly un-ignored.
- experimentation/logger.py imports psycopg (v3) but requirements.txt and serving/requirements.txt listed
  psycopg2-binary, so the logger would fail on import once wired in. Both now list psycopg[binary]>=3.1;
  verified it imports in the rebuilt API image (psycopg 3.3.6). Airflow's own psycopg2 connection is unchanged.
