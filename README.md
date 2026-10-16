# Product Recommendation System

An end-to-end recommendation system built on real e-commerce behaviour data (REES46, October 2019).
Events are streamed through **Kafka** into an **HDFS** data lake. **PySpark** builds weighted, time-decayed features and trains
an **ALS** collaborative-filtering model and a **TF-IDF** cold-start model, then evaluates both offline. The results are cached
in **Redis** and served by a **FastAPI** hybrid re-ranker that adapts to the live session. **Airflow** orchestrates the batch
pipeline, and a demo storefront with a debug panel shows why every item is recommended.

```bash
python run.py        # brings the whole system up, in order, and opens the storefront
```

**Docs:** this README is the full reference. [howtorun.md](howtorun.md) is the step-by-step runbook for a demo,
and [PROGRESS.md](PROGRESS.md) is the dated build log with decisions and measured results.

**Goal.** Recommend products from implicit signals (views, cart adds, purchases) rather than ratings, as a real system
(event stream → data lake → distributed training → cache → live API) that adapts within a session, not as a notebook.
Built by a team of two working in parallel tracks against one frozen event schema (`data/schema.json`):
**Data + ML** (ingestion, Spark feature engineering, ALS, TF-IDF, evaluation) and **Systems + Serving**
(Docker stack, Airflow, FastAPI, Redis, experimentation, monitoring).

---

## Contents

1. [Architecture](#1-architecture)
2. [Quick start](#2-quick-start)
3. [The demo storefront](#3-the-demo-storefront)
4. [Data](#4-data)
5. [Batch pipeline (Spark jobs)](#5-batch-pipeline-spark-jobs)
6. [Serving layer & hybrid re-ranker](#6-serving-layer--hybrid-re-ranker)
7. [Results](#7-results)
8. [Redis key reference](#8-redis-key-reference)
9. [API reference](#9-api-reference)
10. [Repository layout](#10-repository-layout)
11. [Testing](#11-testing)
12. [Troubleshooting](#12-troubleshooting)
13. [Known limitations & roadmap](#13-known-limitations--roadmap)

---

## 1. Architecture

```mermaid
flowchart LR
    CSV[(Cleaned dataset<br/>3.58M events)] --> P[Kafka producer]
    P --> K[Kafka<br/>topic user_events]
    K --> W[hdfs-writer]
    W --> H[(HDFS<br/>year/month/day parquet)]

    subgraph Batch["Batch layer: Spark, orchestrated by Airflow (nightly_retrain)"]
        H --> FE[build_interaction_matrix<br/>weights + 14-day decay]
        FE --> ALS[train_als_model<br/>implicit ALS]
        ALS --> SC[sanity_check]
        SC --> CB[train_content_based<br/>TF-IDF cold-start]
        CB --> CSC[cold_start_sanity_check]
        CSC --> EV[evaluate_model<br/>P/R/hit-rate@K, coverage]
        EV --> PR[push_recommendations_to_redis]
        PR --> EX[export_serving_features]
    end

    PR --> R[(Redis)]
    EX --> R
    R --> API[FastAPI<br/>hybrid re-ranker]
    UI[Storefront + debug panel] <-->|clicks = session events| API
```

**Lambda-style split.** The **batch layer** (Spark) does the heavy lifting once per run: features, models,
evaluation and precomputed neighbours. The **serving layer** (FastAPI) does cheap per-request work: it gathers
candidates, scores them against the user's profile, the product on screen and the live session, and explains the result.

| Layer | Technology | Where |
|---|---|---|
| Ingestion | Kafka (Confluent 7.4) + Python producer/consumer | `ingestion/` |
| Data lake | HDFS 3.2 (single NameNode + DataNode) | `docker-compose.yml` |
| Processing & ML | PySpark 4.2 (MLlib ALS, DataFrame TF-IDF) | `processing/` |
| Orchestration | Apache Airflow 2.10 (also the Spark runtime) | `orchestration/airflow/` |
| Serving cache | Redis 7 | `serving/app/services/redis_client.py` |
| API + storefront | FastAPI + plain HTML/CSS/JS (no build step) | `serving/` |
| Experiment logging | PostgreSQL 15 (schema ready, see roadmap) | `infra/postgres/init.sql`, `experimentation/` |
| Monitoring | Prometheus + Grafana (compose services, not yet wired) | `monitoring/` |

---

## 2. Quick start

For a guided first run with expected output at each step, follow [howtorun.md](howtorun.md).

### Requirements

- **Docker Desktop**, with about 8 GB RAM allocated
- **Python 3.10+** on the host. `run.py` uses only the standard library, so no `pip install` is needed.
- The cleaned dataset at `data/dataset/cleaned/final_cleaned_dataset.csv`. If only `final_cleaned_dataset.zip` is
  there, `run.py` extracts it. The `data/dataset/` folder is git-ignored because the file is about 500 MB.

### One command

```bash
python run.py
```

`run.py` runs these steps in order and stops at the first failure, printing how to resume:

| # | Step | What happens | Typical time |
|---|---|---|---|
| 1 | Preflight | Starts Docker Desktop if it isn't running, creates `.env` from `.env.example`, finds or extracts the dataset | seconds |
| 2 | Infrastructure | `docker compose up` for Zookeeper, Kafka, NameNode, DataNode, Redis, Postgres; waits for Redis PING, `pg_isready`, HDFS to leave safe mode, Kafka to list topics | 30–90 s |
| 3 | Ingestion | Skipped if HDFS already holds events. Otherwise starts `hdfs-writer`, runs the producer (10,000 events by default) and waits until the writer's logs confirm every event was flushed to HDFS | ~2–3 min |
| 4 | Airflow | Builds and starts the Airflow container (Java 17 + PySpark) and waits for `spark-submit` | 1–5 min (first build is slower) |
| 5 | Spark pipeline | The 8 jobs below, run one by one with live logs | ~30 min on a laptop |
| 6 | Serve | Builds and starts the API, waits for `/health`, prints URLs, opens the browser | ~20 s |

### All commands

| Command | Purpose |
|---|---|
| `python run.py` | Everything |
| `python run.py --skip-ingest` | Don't touch HDFS ingestion |
| `python run.py --reingest --sample-size 100000` | Archive the current HDFS events to `/archive/user_events_<timestamp>` and stream a fresh, larger sample |
| `python run.py --skip-pipeline` | Bring up infra + API only (Redis already filled) |
| `python run.py --no-browser` | Don't open the storefront |
| `python run.py pipeline` | Re-run all Spark jobs (stack already up) |
| `python run.py pipeline --from evaluate_model.py` | Resume the pipeline from a given job |
| `python run.py pipeline --only export_serving_features.py` | Run a single job |
| `python run.py serve` | Rebuild and restart only the API + storefront |
| `python run.py status` | Containers, HDFS event-file count, Redis key count, API health |
| `python run.py down` | Stop everything (Docker volumes, and so HDFS/Redis/Postgres data, are kept) |

### URLs once it's up

| What | URL |
|---|---|
| Storefront + debug panel | http://localhost:8000 |
| API docs (Swagger) | http://localhost:8000/docs |
| Airflow (DAG `nightly_retrain`) | http://localhost:8080 (`airflow standalone` prints the admin password in the container logs) |
| HDFS NameNode UI | http://localhost:9870 (Utilities → Browse the file system → `/data`) |

### Running the pipeline through Airflow instead

The DAG `nightly_retrain` (scheduled daily at 02:00) runs the same jobs in the same order. Both the DAG and `run.py` read
the job list from `orchestration/airflow/dags/pipeline_jobs.py`, so they can't drift apart. Trigger it from the Airflow UI,
or with:

```bash
docker exec airflow airflow dags trigger nightly_retrain
```

---

## 3. The demo storefront

Open http://localhost:8000. It looks like a normal shop, but every number comes from the pipeline or the live re-ranker.
Nothing is mocked in the browser.

### Pages

| Page | What it shows |
|---|---|
| **Home** | *Recommended for you* (home-feed re-ranking), *Trending now* (popularity), *Shop by category* |
| **Category / Search** | Products sorted by popularity. Search matches brand, category or item id (e.g. `samsung`, `headphone`) |
| **Product** | Real stats (views, carts, purchases, conversion, ALS embedding size) and *Customers also bought*: alternatives **and** complements (e.g. watches, headphones and tablets for a phone), re-ranked and diversified by category. Opening the page logs a `view` event; *Add to Cart* logs `cart`; *Buy Now* logs `purchase` |
| **Cart** | *Remove* logs `remove_from_cart` (weight 0); *Checkout* logs a `purchase` per item |
| **Insights** | Ingestion and matrix stats, the pipeline with its real config, offline evaluation charts (with a table view), event mix, top categories, events per hour, re-ranker weights |

### Account menu: switch who you are

| Persona | Path through the system |
|---|---|
| **Returning shopper (ALS)** | Users with more than 3 events. Precomputed ALS top-20 + ALS user vector + full history profile |
| **Cold-start shopper (TF-IDF)** | Users with ≤ 3 events (53% of users). Precomputed TF-IDF content-based top-20 |
| **New visitor** | Not in the data at all. No vector, no history: popularity + live session only, i.e. true cold start |
| **Any user_id** | Type any id from the dataset |

### Debug panel (the green **Debug** button)

- **Item Analysis**: the product on screen, then the full breakdown for the recommendation under your mouse (hover any card):
  each factor's value (0–1) and weight, the final score, any variety adjustment, the factors ranked by their share of the score,
  the product's complementary categories (with shared-shopper counts) and its top-5 ALS embedding neighbours. The footer shows how many candidates were scored, how many session events were used and the latency.
- **User Profile**: segment and strategy, batch history from HDFS (event counts, sessions, average price), weighted
  category and brand affinity, the TF-IDF profile tokens, recent history, and the **live session** stored in Redis (with a Reset button).
- **Similar Items**: the product's ALS embedding neighbours (cosine), items that appeared in the same sessions (with
  shared-session counts), its complementary categories, and its TF-IDF tokens.

Each event shows a toast such as *"re-ranked 76 candidates in 53 ms · 7 positions changed"*.

> **Product names and pictures are generated.** The REES46 dataset has no titles or photos, so a product is shown as
> `Brand + most specific category` (e.g. `brand=bosch`, `category_code=appliances.environment.vacuum` → **Bosch Vacuum**)
> with a category illustration. Prices are the item's average price across events, in USD. Star ratings, reviews and
> discounts are deliberately absent because the data has none.

---

## 4. Data

**Source:** [REES46 eCommerce behavior data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store), October 2019.

**Cleaning** (SQL, see `PROGRESS.md`): the result is `final_cleaned_dataset.csv` with 3,582,337 rows (Oct 1–5 2019), no nulls in required
fields, `price > 0`, and `event_type ∈ {view, cart, purchase, remove_from_cart}`. The contract is in `data/schema.json`:

| Field | Type | Notes |
|---|---|---|
| `timestamp` | ISO datetime | event time |
| `event_type` | string | view / cart / purchase / remove_from_cart |
| `item_id` | int | product |
| `category_id` | int | always present |
| `category_code` | string | dot path, e.g. `electronics.audio.headphone`; can be blank |
| `brand` | string | can be blank |
| `price` | float | > 0 |
| `user_id` | int | |
| `session_id` | UUID | groups one browsing session |

**Ingestion.** `ingestion/kafka_producer.py` streams the first `SAMPLE_SIZE` rows (default 10,000, overridable with
`--sample-size`) to Kafka topic `user_events`, keyed by `user_id` so one user's events stay in order. `ingestion/hdfs_writer.py`
consumes them and writes parquet partitioned as `/data/raw/user_events/year=YYYY/month=MM/day=DD/`. It flushes every 500 events or
after 30 s idle, and commits Kafka offsets only after the HDFS write succeeds, so a crash can't lose events.

> The producer always sends the *first* N rows, so running it twice duplicates events in HDFS. `run.py` therefore skips
> ingestion when HDFS already has data, and `--reingest` archives the old events first.

---

## 5. Batch pipeline (Spark jobs)

All jobs live in `processing/spark_jobs/` and share `processing/utils/spark_session.py` (HDFS as user `root`, UTC time zone,
DataNodes reached by hostname) and `processing/utils/weighting.py` (the constants below).

### Feature engineering design

| Feature | Rule | Why |
|---|---|---|
| **Weighted implicit feedback** | purchase = 5, cart = 3, view = 1, remove_from_cart = 0 | Views make up ~98% of events and would drown out stronger intent signals |
| **Time decay** | `score = weight × 0.5^(days_since / 14)`, measured from the **latest event in the data**, not today | Recent behaviour counts more. Decaying 2019 data against 2026 would collapse every weight to ~0 |
| **Behavioural features** | item popularity (Σ weights), conversion rate ((carts + purchases) / views), per-user counts, category/brand shares, average price | Feed re-ranking and cold start rather than ALS itself |
| **Session intent** | the last clicks in the current session, newest weighted most (×0.8 per step back) | Adapts recommendations within the visit |
| **Content-based cold start** | users with **≤ 3 events** (`COLD_START_THRESHOLD`) get TF-IDF over `category_code` prefixes → `category_id` → brand | EDA found 49.1% of users have ≤ 3 interactions; ALS can't personalise them |

**Design principle:** ALS handles collaborative filtering. Behavioural, session and content features are used for re-ranking and
cold start instead of being forced into the ALS model.

### The 8 jobs, in order

| # | Job | Reads | Writes (HDFS / Redis) | What it does |
|---|---|---|---|---|
| 1 | `build_interaction_matrix.py` | raw events | `/data/processed/interaction_matrix`, `/data/processed/item_features` | Applies weights and decay, collapses to one row per (user, item) with summed `interaction_score`, computes item popularity and conversion rate |
| 2 | `train_als_model.py` | interaction matrix | `/models/als_latest`, `/data/processed/top_n_recommendations` | Implicit-feedback ALS (`rank=20, maxIter=15, regParam=0.1, nonnegative`) trained on an 80% split, checks the predictions for red flags, writes the top-20 for every user |
| 3 | `sanity_check.py` | model, raw events | logs | For 5 users with ≥ 10 interactions, compares their actual categories with the recommended ones and flags lists that are identical for everyone |
| 4 | `train_content_based.py` | raw events, matrix, item features | `/data/processed/item_tfidf`, `/data/processed/content_based_recommendations` | Item tokens = every `category_code` prefix + `cid:<category_id>` + `brand:<brand>`. TF-IDF (binary TF, smoothed IDF), L2-normalised. A user's profile = interaction-weighted sum of item vectors. Candidates = 2,000 most popular items; cosine ranking excludes seen items, caps 3 items per brand, backfills with popularity |
| 5 | `cold_start_sanity_check.py` | TF-IDF output | logs | Prints history vs recommendations (category/brand) for 5 cold-start users |
| 6 | `evaluate_model.py` | matrix, raw events | `/data/processed/evaluation_metrics` | Same 80/20 split; a fresh ALS on the 80%; Precision/Recall/hit-rate@{10,20} and catalog coverage for ALS vs popularity on all users, and ALS vs popularity vs TF-IDF on cold-start users. Items seen in training never count as hits |
| 7 | `push_recommendations_to_redis.py` | ALS + TF-IDF top-N | Redis `recommendations:{user}` | **Routing:** cold-start users get the TF-IDF list, everyone else gets ALS. Each item carries `source: "als" \| "content"` |
| 8 | `export_serving_features.py` | everything above | Redis `item:*`, `user:*`, `catalog:*`, `demo:users`, `metrics:*` | Builds what the re-ranker and storefront need (details below) |

### What `export_serving_features.py` computes

- **Per item:** brand, category, average price; views/carts/purchases/unique users; popularity and conversion rate; TF-IDF tokens;
  the 20-dim ALS item vector, plus two neighbour lists:
  - **`similar`**: the top 12 items by **cosine similarity of ALS item vectors**, searched among the 2,000 most popular items.
    Only items with **≥ 3 distinct users** take part: an item seen by one user gets a vector pointing the same way as that
    user's vector, so all of that user's items would look ~100% similar.
  - **`also`**: the top 12 items **engaged with in the same sessions**. Each pair is weighted by the weaker of the two interactions
    (view 1 / cart 3 / purchase 5) and normalised `co_weight / sqrt(total_a × total_b)`, so the score is in (0, 1].
    Sessions with more than 50 items are ignored (bots or crawls).
- **Per user:** event counts, sessions, first/last seen, average price, segment (`warm` / `cold_start`), top-5 category and brand shares
  (weighted by event weight), the 10 most recent events, up to 200 interacted items (excluded from recommendations), the top-25 TF-IDF
  profile tokens, and the ALS user vector.
- **Complementary categories** (`catalog:complements`): categories whose shoppers overlap. These are the headphones and watches for a phone.
  User-level co-occurrence of `category_code`, scored `shared / sqrt(users_a × users_b) × shared / (shared + 5)`. The second
  factor is shrinkage: without it, 3 shared shoppers made "saw" the top complement of headphones. Pairs need **≥ 2 shared shoppers**;
  the top 8 are kept per category. In this slice, smartphone → watch (22 shoppers), headphone (21), telephone (7), TV (9), tablet (6).
- **Global:** popularity-sorted catalog indexes (overall and per top-level category), 20 demo users (12 most active ALS users + 8 cold-start
  users), pipeline metrics (counts, density, event mix, events per hour, top categories, config), and the evaluation rows.

---

## 6. Serving layer & hybrid re-ranker

`serving/app/` is a FastAPI app. It also serves the storefront from `serving/web/` (mounted as a volume, so UI edits show up on refresh).

### Request flow: `GET /api/recommendations/{user_id}?context_item={id}`

1. **Load** the user's profile, precomputed top-N and live session from Redis, plus the product on screen if there is one.
2. **Generate candidates.** When an item comes from several sources, the first one wins:

   | Order | Source | When |
   |---|---|---|
   | 1 | `copurchase`: the product's same-session neighbours | product page |
   | 2 | `similar`: the product's ALS embedding neighbours | product page |
   | 3 | `category`: the 20 most popular items in the same `category_code` | product page (covers long-tail items with no neighbours) |
   | 3b | `complement`: the 5 most popular items from each of the product's top 4 complementary categories | product page. If fewer than 3 categories were learned, the department's most popular other categories fill in, marked "dept" |
   | 4 | `als` / `content`: the user's precomputed top-20 | known users |
   | 5 | `session`: neighbours of the last 3 clicked items | after clicks |
   | 6 | `popular`: the 30 most popular items | always (fallback) |

3. **Exclude** the product on screen, everything clicked this session, and everything the user has already interacted with.
4. **Score** every candidate (`serving/app/services/ranker.py`, pure functions, unit-tested):

   | Factor | Formula | Range |
   |---|---|---|
   | Base ALS Score | `max(0, user_vector · item_vector)`, divided by the best candidate's value | 0–1 |
   | Item-Item Similarity | TF-IDF cosine between the product on screen and the candidate | 0–1 |
   | Complementary Category | the candidate category's complement score for the product on screen, scaled so the strongest = 1 (department fallback = 0.3) | 0–1 |
   | Item Co-purchase Factor | the `also` score from the product on screen or a recent session item (max) | 0–1 |
   | User-Item Similarity | TF-IDF cosine between the user's history profile and the candidate | 0–1 |
   | Session Intent Boost | TF-IDF cosine between a session profile (event weight × 0.8^position) and the candidate | 0–1 |
   | Category Affinity | user's share of the exact category + ½ × share of the same top-level category | 0–1 |
   | Price Range Fit | `exp(-|ln(price / reference)|)`: 1 at the same price, 0.5 at double or half. Reference = product on screen, else user's average, else session average | 0–1 |
   | Brand Affinity | user's share of interactions with this brand | 0–1 |
   | Popularity | `log(1 + popularity)`, divided by the best candidate's value | 0–1 |

   Users without batch history take category and brand shares from the live session instead.

5. **Combine.** `final = Σ weight × factor`. The weights sum to 1, so the final score is in [0, 1] and is shown as **Match %**.
   It's a blended relevance score, **not a calibrated probability**. Each factor's *contribution* = `weight × value / final`.
   There are two weight profiles (`serving/app/config.py`):

   | Factor | Home feed (about the shopper) | Product page (about the product) |
   |---|---|---|
   | Base ALS Score | **0.30** | 0.12 |
   | Item-Item Similarity | – | **0.24** |
   | Item Co-purchase Factor | 0.18 | 0.20 |
   | Complementary Category | – | 0.14 |
   | User-Item Similarity | 0.14 | 0.06 |
   | Session Intent Boost | 0.14 | 0.08 |
   | Category Affinity | 0.10 | 0.05 |
   | Price Range Fit | 0.06 | 0.04 (accessories are naturally cheaper) |
   | Brand Affinity | 0.05 | 0.04 |
   | Popularity | 0.03 | 0.03 |

   Why two profiles: with a single profile, a phone buyer who opened a vacuum was shown phones, because their ALS history
   dominated. On a product page, relevance to that product has to lead.

6. **Diversify.** Items are picked greedily by `score × decay^(items already picked from the same category)`, with decay **0.6**
   on product pages and **0.75** on the home feed. A phone page therefore shows a mix such as
   phone → watch → phone → tablet → TV → headphones → … rather than twelve phones. The Match % shown is still the raw relevance score.
   The debug panel shows the "Variety adjustment" row (e.g. `×0.6 → 0.259`), so a 43% item placed below a 32% item is explained.

**Live session.** `POST /api/events` pushes to the Redis list `session:{user}` (20 most recent events, 2-hour TTL). The next
request reads it, so recommendations change right after a click without retraining.

**Latency:** about 50–120 ms warm for 50–80 candidates on a laptop through Docker. The first request after a restart is slower
because it loads the 5,000-item search index into memory.

---

## 7. Results

The current run uses a 150,000-event sample (Oct 3 16:59 → Oct 4 01:15, 2019): 25,046 users, 15,539 items and 93,235
(user, item) pairs (density 0.024%). 53.7% of users are cold-start (≤ 3 events). The Insights page shows the live numbers
from the last run.

Offline evaluation, K = 10, 80/20 hold-out:

| Segment | Model | Hit rate | Recall | Precision | Coverage |
|---|---|---|---|---|---|
| All users (n = 8,797) | **ALS** | **20.3%** | **14.6%** | **2.3%** | **1.6%** |
| | Popularity | 13.9% | 10.0% | 1.5% | 0.1% |
| Cold-start (n = 4,356) | **TF-IDF** | **28.3%** | **24.1%** | **3.0%** | **7.7%** |
| | ALS | 20.4% | 17.6% | 2.2% | 1.4% |
| | Popularity | 15.1% | 12.7% | 1.6% | 0.1% |

How to read it:
- **ALS beats popularity by about 6 points of hit rate** on all users, and recommends 16× more distinct items.
- **TF-IDF is the best model for cold-start users** (+8 points over ALS, +13 over popularity), which is why they are routed to it.
- Precision is low by construction: users have only a few held-out items, so even a perfect top-10 has a low ceiling.
- The split is random rather than time-based, so treat these as optimistic (see [Known limitations](#13-known-limitations--roadmap)).

---

## 8. Redis key reference

| Key | Type | Written by | Content |
|---|---|---|---|
| `recommendations:{user_id}` | JSON list | push job | `[{item_id, score, source}]`, top-20 |
| `item:{item_id}` | JSON | export job | metadata, stats, `tokens`, `factors`, `similar`, `also` |
| `user:{user_id}` | JSON | export job | stats, `segment`, `categories`, `brands`, `recent`, `seen`, `profile`, `factors` |
| `catalog:popular` | sorted set | export job | item → popularity |
| `catalog:category:{root}` | sorted set | export job | per top-level category |
| `catalog:categories` | JSON | export job | `[{key, n_items, popularity}]` |
| `catalog:complements` | JSON | export job | `{category_code: [{category, score, users}]}` |
| `demo:users` | JSON | export job | personas for the account menu |
| `metrics:pipeline`, `metrics:evaluation` | JSON | export job | Insights page |
| `session:{user_id}` | list | API | live session events, 2-hour TTL |

Inspect them with `docker exec -it redis redis-cli` (for example `GET user:537216305`).

---

## 9. API reference

Interactive docs: http://localhost:8000/docs

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | `{status, redis, features_loaded}` |
| GET | `/recommendations/{user_id}` | Raw precomputed top-N (original Phase 2 endpoint; 404 if unknown) |
| GET | `/api/recommendations/{user_id}?context_item=&limit=` | Re-ranked recommendations with the per-factor breakdown |
| GET | `/api/items/{item_id}` | Item detail + `similar` + `also` neighbours + TF-IDF tokens |
| GET | `/api/users/demo` | Demo personas |
| GET | `/api/users/{user_id}` | Profile, strategy, live session |
| POST | `/api/events` | `{user_id, item_id, event_type}`, appended to the live session |
| DELETE | `/api/session/{user_id}` | Clear the live session |
| GET | `/api/catalog/categories` | Top-level categories |
| GET | `/api/catalog/items?category=&q=&offset=&limit=` | Popularity-sorted listing / search |
| GET | `/api/metrics` | Pipeline metrics, evaluation rows, re-ranker weight profiles |

Example:

```bash
curl "http://localhost:8000/api/recommendations/537216305?context_item=3701134&limit=5"
```

---

## 10. Repository layout

```text
run.py                         one-command runner (stdlib only)
docker-compose.yml             the whole stack
data/schema.json               event contract
ingestion/                     kafka_producer.py, hdfs_writer.py, Dockerfiles
processing/
  utils/                       spark_session.py, weighting.py
  spark_jobs/                  the 8 pipeline jobs
  explore_hdfs.py              quick HDFS stats (events, users, density); run inside the airflow container
orchestration/airflow/
  Dockerfile                   Airflow + Java 17 + PySpark
  dags/pipeline_jobs.py        job order + spark-submit command (shared with run.py)
  dags/nightly_retrain_dag.py  the DAG
serving/
  app/main.py                  FastAPI app, /health, static storefront
  app/config.py                re-ranker weight profiles, session settings
  app/routes/                  recommendations.py (raw), shop.py (/api/*)
  app/services/                redis_client, catalog, recommender (candidates), ranker (scoring)
  web/                         index.html, styles.css, app.js (storefront + debug panel)
experimentation/               A/B bucketing + Postgres event logger (not yet wired into the API)
infra/postgres/init.sql        experiment_assignments, recommendation_events tables
monitoring/                    Prometheus / Grafana config
notebooks/                     eda.ipynb
tests/                         pytest suite
howtorun.md                    step-by-step demo runbook
PROGRESS.md                    dated build log with decisions and measured results
```

---

## 11. Testing

```bash
pip install -r requirements.txt
python -m pytest tests -q
```

`tests/test_ranker.py` covers the re-ranker: weights sum to 1 in each profile; the final score equals the weighted sum and the
contributions add up to 100%; a session click promotes matching items; co-purchase uses the product on screen; price fit; ALS scaling;
the new-visitor path; the regression case (a phone buyer on a vacuum page gets vacuums first); the complement factor; and the variety
pass (six phones and one pair of headphones → the headphones make the top 3, and Match % stays the raw score). Spark-based tests use the local
SparkSession fixture in `tests/conftest.py` and are skipped if PySpark isn't installed.

---

## 12. Troubleshooting

| Symptom | Fix |
|---|---|
| `Docker engine is not running` | Start Docker Desktop (run.py tries to on Windows) |
| Debug panel shows **No data** | Redis is up but empty: `python run.py pipeline` |
| A Spark job fails | Fix it, then `python run.py pipeline --from <job>.py` |
| `HDFS out of safe mode` times out | `docker restart namenode datanode`, then re-run |
| Recommendations look duplicated / skewed | HDFS may hold duplicate events: `python run.py --reingest` |
| Storefront change not visible | Hard-refresh. Changes to `serving/app` need `python run.py serve` (rebuilds the image) |
| `netlib-blas ... libblas.so.3` warning | Harmless: Spark falls back to its Java BLAS |
| Want a fresh demo session | Debug → User Profile → **Reset**, or pick another persona |

---

## 13. Known limitations & roadmap

**Limitations**
- The demo sample covers only about 8 hours of traffic (150k events). Scale up with `--reingest --sample-size <n>`.
- Over 8 hours the 14-day time-decay half-life has almost no effect: every event keeps about 99% of its weight.
- `evaluate_model.py` uses a random 80/20 split, not a time-based one, so future events can leak into training and the
  offline metrics are likely optimistic.
- No product titles or images in the dataset; names and pictures are generated (see above).
- Complements only cover categories that exist in the slice. There are no phone cases in it, and a few weak pairs (e.g. smartphone → underwear,
  3 shoppers) survive the threshold but rank low. More data (`--sample-size`) sharpens them.
- The producer isn't idempotent; `run.py` guards against duplicates, the producer itself doesn't.
- Storefront clicks only live in the Redis session. They are deliberately **not** sent to Kafka: they're timestamped 2026, and time decay
  measured from the latest event would then wipe out the 2019 history.

**Roadmap**
1. A/B test: bucket users with `experimentation/bucketing.py` (treatment = hybrid re-ranker, control = popularity) and log
   impressions/clicks/purchases to Postgres with `experimentation/logger.py`, then compute CTR/conversion and significance.
2. Thompson-sampling bandit over the candidate pool, and a Kafka consumer that builds session vectors.
3. Prometheus metrics on the API (latency, Redis hit rate) and a Grafana dashboard.
4. Learn the re-ranker weights from logged clicks instead of hand-setting them.
5. Make ingestion idempotent (event ids or offset-based slicing) and evaluate on the full 3.58M events.
