"""
kafka_producer.py

Reads a sample slice of the cleaned dataset and publishes each row as an
event to the `user_events` Kafka topic — simulating a live stream of user
behaviour instead of a static batch file.

Usage:
    python kafka_producer.py
"""

import json
import logging
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# Config — edit these
# ---------------------------------------------------------------------------

INPUT_CSV = Path(__file__).resolve().parents[1] / "data" / "dataset" / "cleaned" / "final_cleaned_dataset.csv"

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"   # host-facing listener from docker-compose.yml
TOPIC_NAME = "user_events"

SAMPLE_SIZE = 10_000        # how many rows to stream for this test run
DELAY_SECONDS = 0.01        # small delay between sends to simulate real traffic

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# Fields exactly as finalized in data/schema.json
SCHEMA_FIELDS = [
    "timestamp", "event_type", "item_id", "category_id",
    "category_code", "brand", "price", "user_id", "session_id"
]


# ---------------------------------------------------------------------------
# Step 1: Load a sample slice of the cleaned dataset
# ---------------------------------------------------------------------------

def load_sample_data(path: str, sample_size: int) -> pd.DataFrame:
    logger.info(f"Loading sample from {path}")
    df = pd.read_csv(path, nrows=sample_size)
    logger.info(f"Loaded {len(df):,} rows for streaming")
    return df


# ---------------------------------------------------------------------------
# Step 2: Convert a DataFrame row into a schema-conformant event dict
# ---------------------------------------------------------------------------

def row_to_event(row: pd.Series) -> dict:
    event = {}
    for field in SCHEMA_FIELDS:
        value = row.get(field)
        # Replace NaN/NaT with None so JSON serialization doesn't choke
        event[field] = None if pd.isna(value) else value
    return event


# ---------------------------------------------------------------------------
# Step 3: Stream events to Kafka
# ---------------------------------------------------------------------------

def send_events(producer: KafkaProducer, df: pd.DataFrame) -> None:
    sent = 0
    failed = 0

    for _, row in df.iterrows():
        event = row_to_event(row)
        try:
            # Keying by user_id keeps all of one user's events on the same
            # partition, preserving per-user order
            key = str(event["user_id"]).encode("utf-8")
            producer.send(TOPIC_NAME, key=key, value=event)
            sent += 1

            if sent % 500 == 0:
                logger.info(f"Sent {sent:,} events so far...")

            time.sleep(DELAY_SECONDS)

        except Exception as e:
            failed += 1
            logger.error(f"Failed to send event: {e}")

    producer.flush()
    logger.info(f"Done. Sent {sent:,} events, {failed:,} failed.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k,
    )

    df = load_sample_data(INPUT_CSV, SAMPLE_SIZE)
    send_events(producer, df)

    producer.close()
    logger.info("Producer closed.")


if __name__ == "__main__":
    main()