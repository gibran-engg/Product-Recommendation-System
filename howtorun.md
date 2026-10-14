# RUNBOOK: start the Product Recommendation System from scratch

Run every command from the project folder in PowerShell:
D:\Product-Recommendation-System

---

## Step 0: Prepare the laptop (2 min)

1. Close apps you don't need: ChatGPT, extra VS Code windows, browser tabs. Spark needs the RAM.
2. Restart Docker Desktop: taskbar whale icon → Restart.
   If it hangs, run this, then open Docker Desktop again:

   wsl --shutdown

3. Wait until Docker Desktop shows "Engine running". Check:

   docker ps

   ✅ It prints a table (it may be empty) within a few seconds.
   ❌ It hangs or errors: Docker isn't ready. Wait or restart it again.

---

## Step 1: Choose your data size

HDFS keeps your data between runs. Pick ONE:

- **Option A: keep the 500k events already in HDFS (recommended).** Nothing to do; ingestion is skipped automatically.
- **Option B: start over with a smaller, fresh sample** (faster pipeline, less memory). This archives the old events in HDFS and
  streams new ones, which takes about 3 min per 15k events (150k ≈ 30 min):

  python run.py --reingest --sample-size 150000 --skip-pipeline --no-browser

  ✅ Wait for "✓ All events written to HDFS", then go to Step 3 (Step 2 has already been done for you).

---

## Step 2: Start the infrastructure + API, without the pipeline (~2–5 min)

python run.py --skip-pipeline --no-browser

✅ You should see, in order:
- ✓ Docker engine is up
- ✓ Dataset found
- ✓ Redis answers PING / ✓ Postgres accepts connections / ✓ HDFS out of safe mode / ✓ Kafka broker is reachable
- ✓ HDFS already holds N event files ... skipping
- ✓ API is healthy, then "System is up."

---

## Step 3: Free memory for Spark (10 s)

Kafka isn't needed for training. Stop it and the ingestion containers:

docker stop recommendation-hdfs-writer kafka zookeeper

(Ignore "No such container" for any that weren't running.)

---

## Step 4: Run the Spark pipeline (≈ 45–75 min for 500k, ≈ 25–40 min for 150k)

python run.py pipeline

8 jobs run in this order, each with 2 GB of memory. Every job ends with a ✓ line:

1. build_interaction_matrix.py      weights + time decay → (user, item) matrix
2. train_als_model.py               ALS model + top-20 per user
3. sanity_check.py                  prints 5 users: history vs recommendations
4. train_content_based.py           TF-IDF for cold-start users (SLOWEST, long silence is normal)
5. cold_start_sanity_check.py       prints 5 cold-start users
6. evaluate_model.py                precision / recall / hit-rate table (slow)
7. push_recommendations_to_redis.py "Pushed recommendations to Redis: X users from ALS, Y from TF-IDF"
8. export_serving_features.py       catalog, profiles, complements, metrics → Redis (slow)

Ignore these, they are harmless:
- WARN FileStreamSink ... FileNotFoundException ... user_events/*/*/*  (followed by a long Java stack trace)
- netlib-blas: JNI_OnLoad ... libblas.so.3

How to tell whether it's working: open Task Manager → "Vmmem".
- CPU 20%+ → working, leave it alone.
- CPU 0% for 5+ minutes → crashed or stuck (see Troubleshooting).

✅ Done when you see: "✓ export_serving_features.py finished" and "Done in X min".

If a job fails, fix the problem, then resume from that job (finished jobs are saved in HDFS):

python run.py pipeline --from <job_name>.py

---

## Step 5: Restart the API so it picks up the new data (20 s)

python run.py serve

✅ "✓ API is healthy" with no "!" warning, then the storefront opens.

---

## Step 6: Verify it works (2 min)

1. Open http://localhost:8000
2. The debug panel badge (top right) says **Live** (green), not "No data".
3. The account menu lists "returning shoppers" and "cold-start shoppers".
4. Open any product → "Customers also bought" shows a mix (e.g. phones + watches + headphones).
5. Click Add to Cart → a toast says "re-ranked N candidates in X ms".
6. Open Insights → events, users and evaluation charts are filled in.

Quick checks from the terminal:

python run.py status
curl http://localhost:8000/health

✅ health shows "redis": true and "features_loaded": true

---

## Step 7 (optional): Airflow UI login

1. Set a password (you'll be prompted for it):

   docker exec -it airflow airflow users reset-password -u admin

2. Open http://localhost:8080 and log in as admin with that password.
   The DAG is "nightly_retrain" (same 8 jobs, same order).

---

## Useful URLs

| What | URL |
|---|---|
| Storefront + debug panel | http://localhost:8000 |
| API docs | http://localhost:8000/docs |
| Airflow | http://localhost:8080 |
| HDFS browser | http://localhost:9870 (Utilities → Browse → /data) |

---

## Troubleshooting

| Problem | Fix |
|---|---|
| A command hangs at "Preflight" | Docker engine is down: restart Docker Desktop (Step 0) |
| Vmmem CPU 0% for minutes during Step 4 | Docker ran out of memory: Ctrl+C, restart Docker (Step 0), run Step 2 + Step 3 again, then python run.py pipeline --from <job that was running>.py |
| Crashes again on 500k | Use a smaller sample: Step 1 Option B with --sample-size 150000 |
| Debug badge says "No data" | The pipeline hasn't finished: re-run Step 4 (use --from to resume) |
| Storefront shows old data | python run.py serve |
| Start a clean demo session | Debug panel → User Profile → Reset |
| Stop everything (data is kept) | python run.py down |