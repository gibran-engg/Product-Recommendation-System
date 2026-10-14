"""
run.py — bring the whole recommendation system up with one command.

    python run.py                 # everything: infra -> ingest -> Spark pipeline -> API + storefront
    python run.py pipeline        # re-run only the Spark jobs (stack already up)
    python run.py pipeline --from evaluate_model.py
    python run.py serve           # (re)start only the API + storefront
    python run.py status          # what's running, what's in HDFS / Redis
    python run.py down            # stop the stack (data volumes are kept)

Order (the same one the Airflow DAG uses, see orchestration/airflow/dags/pipeline_jobs.py):
    1. Docker + dataset preflight
    2. Kafka, Zookeeper, HDFS, Redis, Postgres
    3. Ingestion: Kafka producer -> hdfs-writer -> HDFS   (skipped if HDFS already holds events)
    4. Airflow container (it has Java + Spark and runs the jobs)
    5. Spark jobs, one by one, stopping at the first failure
    6. FastAPI + storefront at http://localhost:8000

Standard library only — no pip install needed to run it.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
import webbrowser
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "orchestration" / "airflow" / "dags"))
from pipeline_jobs import PIPELINE_JOBS, spark_submit_command  # noqa: E402

DATASET = ROOT / "data" / "dataset" / "cleaned" / "final_cleaned_dataset.csv"
DATASET_ZIP = DATASET.with_suffix(".zip")
INFRA_SERVICES = ["zookeeper", "kafka", "namenode", "datanode", "redis", "postgres"]
HDFS_EVENTS_DIR = "/data/raw/user_events"
API_URL = "http://localhost:8000"

URLS = [
    ("Storefront + debug panel", f"{API_URL}"),
    ("API docs (Swagger)", f"{API_URL}/docs"),
    ("Airflow (DAG nightly_retrain)", "http://localhost:8080"),
    ("HDFS NameNode UI", "http://localhost:9870"),
]

if os.name == "nt":
    os.system("")  # enables ANSI colours in the Windows console

BOLD, DIM, GREEN, YELLOW, RED, RESET = "\033[1m", "\033[2m", "\033[32m", "\033[33m", "\033[31m", "\033[0m"


class StepFailed(Exception):
    pass


def step(title: str) -> None:
    print(f"\n{BOLD}==> {title}{RESET}", flush=True)


def info(msg: str) -> None:
    print(f"    {msg}", flush=True)


def ok(msg: str) -> None:
    print(f"    {GREEN}✓{RESET} {msg}", flush=True)


def warn(msg: str) -> None:
    print(f"    {YELLOW}!{RESET} {msg}", flush=True)


def sh(args: list[str], check=True, capture=False, env=None, stream=False) -> subprocess.CompletedProcess:
    """Run a command from the repo root. stream=True shows its output live (long Spark jobs)."""
    if stream:
        proc = subprocess.run(args, cwd=ROOT, env=env)
    else:
        proc = subprocess.run(args, cwd=ROOT, env=env, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if check and proc.returncode != 0:
        detail = "" if stream else (proc.stderr or proc.stdout or "").strip()[-2000:]
        raise StepFailed(f"`{' '.join(args)}` failed (exit {proc.returncode})\n{detail}")
    return proc


def compose(*args: str, **kwargs) -> subprocess.CompletedProcess:
    return sh(["docker", "compose", *args], **kwargs)


def wait_for(description: str, probe, timeout: int = 180, interval: float = 3) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if probe():
                ok(description)
                return
        except Exception:
            pass
        time.sleep(interval)
    raise StepFailed(f"Timed out after {timeout}s waiting for: {description}")


def exec_ok(container: str, *cmd: str) -> bool:
    return sh(["docker", "exec", container, *cmd], check=False, capture=True).returncode == 0


# --------------------------------------------------------------------------- steps

def preflight() -> None:
    step("Preflight")
    if not shutil.which("docker"):
        raise StepFailed("Docker is not installed or not on PATH — install Docker Desktop first.")
    if sh(["docker", "info"], check=False, capture=True).returncode != 0:
        desktop = Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "Docker" / "Docker" / "Docker Desktop.exe"
        if os.name == "nt" and desktop.exists():
            info("Docker engine is not running — starting Docker Desktop...")
            subprocess.Popen([str(desktop)])
            wait_for("Docker engine is up", lambda: sh(["docker", "info"], check=False, capture=True).returncode == 0, timeout=240, interval=5)
        else:
            raise StepFailed("Docker engine is not running — start Docker and re-run.")
    else:
        ok("Docker engine is up")

    if not (ROOT / ".env").exists():
        shutil.copy(ROOT / ".env.example", ROOT / ".env")
        ok("Created .env from .env.example")

    if DATASET.exists():
        ok(f"Dataset found ({DATASET.stat().st_size / 1e6:,.0f} MB)")
    elif DATASET_ZIP.exists():
        info(f"Extracting {DATASET_ZIP.name}...")
        with zipfile.ZipFile(DATASET_ZIP) as zf:
            member = next(n for n in zf.namelist() if n.endswith("final_cleaned_dataset.csv"))
            with zf.open(member) as src, open(DATASET, "wb") as dst:
                shutil.copyfileobj(src, dst)
        ok("Dataset extracted")
    else:
        warn(f"No dataset at {DATASET.relative_to(ROOT)} — ingestion will be skipped if HDFS already has events")


def start_infra() -> None:
    step("Starting Kafka, HDFS, Redis, Postgres")
    compose("up", "-d", *INFRA_SERVICES)
    wait_for("Redis answers PING", lambda: "PONG" in sh(["docker", "exec", "redis", "redis-cli", "ping"], capture=True).stdout)
    wait_for("Postgres accepts connections", lambda: exec_ok("postgres", "pg_isready", "-U", "admin"))
    wait_for("HDFS out of safe mode", lambda: exec_ok("namenode", "hdfs", "dfsadmin", "-safemode", "wait"), timeout=240)
    wait_for("Kafka broker is reachable",
             lambda: exec_ok("kafka", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"), timeout=240)


def hdfs_event_files() -> int:
    proc = sh(["docker", "exec", "namenode", "hdfs", "dfs", "-count", HDFS_EVENTS_DIR], check=False, capture=True)
    if proc.returncode != 0:
        return 0
    return int(proc.stdout.split()[1])  # DIR_COUNT FILE_COUNT CONTENT_SIZE PATH


def ingest(reingest: bool, sample_size: int) -> None:
    step("Ingestion: Kafka producer -> hdfs-writer -> HDFS")
    files = hdfs_event_files()
    if files and not reingest:
        ok(f"HDFS already holds {files} event files under {HDFS_EVENTS_DIR} — skipping "
           f"(the producer always re-sends the first rows, so re-running it would duplicate events; use --reingest)")
        return
    if files and reingest:
        archive = f"/archive/user_events_{datetime.now():%Y%m%d_%H%M%S}"
        sh(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/archive"])
        sh(["docker", "exec", "namenode", "hdfs", "dfs", "-mv", HDFS_EVENTS_DIR, archive])
        ok(f"Moved existing events to {archive}")
    if not DATASET.exists():
        raise StepFailed(f"HDFS has no events and the dataset is missing at {DATASET.relative_to(ROOT)}")

    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    compose("up", "-d", "--build", "hdfs-writer")
    ok("hdfs-writer running")
    info(f"Streaming {sample_size:,} events into Kafka (this takes ~{sample_size * 0.012:,.0f}s)...")
    env = {**os.environ, "SAMPLE_SIZE": str(sample_size)}
    compose("build", "producer", env=env)
    compose("run", "--rm", "producer", env=env, stream=True)

    def flushed() -> bool:
        logs = sh(["docker", "logs", "--since", started, "recommendation-hdfs-writer"], capture=True)
        total = sum(int(n.replace(",", "")) for n in re.findall(r"Flushed ([\d,]+) events", logs.stdout + logs.stderr))
        info(f"{DIM}hdfs-writer has flushed {total:,} / {sample_size:,} events{RESET}")
        return total >= sample_size

    wait_for("All events written to HDFS", flushed, timeout=300, interval=10)


def start_airflow() -> None:
    step("Starting Airflow (Java + Spark runtime for the jobs)")
    compose("up", "-d", "--build", "airflow")
    wait_for("spark-submit available in the airflow container",
             lambda: exec_ok("airflow", "bash", "-c", "command -v spark-submit"), timeout=300)


def run_pipeline(start_from: str | None = None, only: str | None = None) -> None:
    jobs = PIPELINE_JOBS
    for name in filter(None, [start_from, only]):
        if name not in jobs:
            raise StepFailed(f"Unknown job {name!r}. Jobs: {', '.join(jobs)}")
    if only:
        jobs = [only]
    elif start_from:
        jobs = jobs[jobs.index(start_from):]

    for i, job in enumerate(jobs, 1):
        step(f"Spark job {i}/{len(jobs)}: {job}")
        t0 = time.time()
        proc = sh(["docker", "exec", "airflow", "bash", "-c", spark_submit_command(job)], check=False, stream=True)
        if proc.returncode != 0:
            raise StepFailed(f"{job} failed (exit {proc.returncode}). Fix it, then resume with:\n"
                             f"    python run.py pipeline --from {job}")
        ok(f"{job} finished in {time.time() - t0:,.0f}s")


def api_health() -> dict | None:
    try:
        with urllib.request.urlopen(f"{API_URL}/health", timeout=3) as res:
            return json.loads(res.read())
    except Exception:
        return None


def serve(open_browser: bool) -> None:
    step("Starting API + storefront")
    compose("up", "-d", "--build", "api")
    wait_for("API is healthy", lambda: (api_health() or {}).get("redis"), timeout=120)
    if not (api_health() or {}).get("features_loaded"):
        warn("API is up but Redis has no serving features yet — run `python run.py pipeline`")
    print(f"\n{BOLD}{GREEN}System is up.{RESET}")
    for label, url in URLS:
        print(f"    {label:<32} {url}")
    if open_browser:
        webbrowser.open(API_URL)


def status() -> None:
    step("Containers")
    print(compose("ps", "--format", "table {{.Name}}\t{{.Status}}\t{{.Ports}}", check=False, capture=True).stdout)
    step("Data")
    info(f"HDFS event files under {HDFS_EVENTS_DIR}: {hdfs_event_files()}")
    keys = sh(["docker", "exec", "redis", "redis-cli", "dbsize"], check=False, capture=True).stdout.strip()
    info(f"Redis keys: {keys or 'redis not running'}")
    health = api_health()
    info(f"API health: {health if health else 'not reachable'}")


def main() -> None:
    parser = argparse.ArgumentParser(description="One-command runner for the recommendation system.")
    sub = parser.add_subparsers(dest="command")

    up = sub.add_parser("up", help="everything (default)")
    for p in (parser, up):
        p.add_argument("--skip-ingest", action="store_true", help="don't stream events into HDFS")
        p.add_argument("--reingest", action="store_true", help="archive HDFS events and stream a fresh sample")
        p.add_argument("--sample-size", type=int, default=10_000, help="events to stream when ingesting (default 10000)")
        p.add_argument("--skip-pipeline", action="store_true", help="don't run the Spark jobs")
        p.add_argument("--no-browser", action="store_true", help="don't open the storefront")

    pipe = sub.add_parser("pipeline", help="run the Spark jobs only")
    pipe.add_argument("--from", dest="start_from", metavar="JOB", help="resume from this job, e.g. evaluate_model.py")
    pipe.add_argument("--only", metavar="JOB", help="run a single job")

    srv = sub.add_parser("serve", help="(re)start the API + storefront only")
    srv.add_argument("--no-browser", action="store_true")
    sub.add_parser("status", help="show what's running")
    sub.add_parser("down", help="stop the stack (volumes are kept)")

    args = parser.parse_args()
    command = args.command or "up"
    started = time.time()
    try:
        if command == "up":
            preflight()
            start_infra()
            if not args.skip_ingest:
                ingest(args.reingest, args.sample_size)
            if not args.skip_pipeline:
                start_airflow()
                run_pipeline()
            serve(not args.no_browser)
        elif command == "pipeline":
            preflight()
            start_airflow()
            run_pipeline(args.start_from, args.only)
        elif command == "serve":
            serve(not args.no_browser)
        elif command == "status":
            status()
        elif command == "down":
            step("Stopping the stack")
            compose("down", stream=True)
    except StepFailed as exc:
        print(f"\n{RED}{BOLD}✗ {exc}{RESET}")
        sys.exit(1)
    except KeyboardInterrupt:
        print(f"\n{YELLOW}Interrupted.{RESET}")
        sys.exit(130)
    if command in ("up", "pipeline"):
        print(f"\n{DIM}Done in {(time.time() - started) / 60:,.1f} min{RESET}")


if __name__ == "__main__":
    main()
