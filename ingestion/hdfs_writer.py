"""
hdfs_writer.py

Consumes events from the `user_events` Kafka topic, buffers them, and
periodically flushes the buffer into HDFS as a Parquet file, partitioned
by event date (year/month/day) — matching the data lake layout planned
in the project architecture.

Flush triggers (whichever comes first):
    - Buffer reaches FLUSH_EVERY events
    - FLUSH_INTERVAL_SECONDS have elapsed since the last flush

Usage:
    python hdfs_writer.py
"""

import io
import json
import logging
import time
import uuid
from datetime import datetime
from collections import defaultdict

import pandas as pd
from kafka import KafkaConsumer
from hdfs import InsecureClient

# ---------------------------------------------------------------------------
# Config — edit these
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC_NAME = "user_events"
CONSUMER_GROUP_ID = "hdfs-writer-group"

HDFS_WEBHDFS_URL = "http://localhost:9870"   # NameNode web UI / WebHDFS port
HDFS_USER = "root"
HDFS_BASE_PATH = "/data/raw/user_events"

FLUSH_EVERY = 500            # events
FLUSH_INTERVAL_SECONDS = 30  # seconds

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Step 1: Determine the HDFS partition path for an event's date
# ---------------------------------------------------------------------------

def get_partition_path(timestamp_str: str) -> str:
    """
    Builds a year=/month=/day= partition path from an event's timestamp.
    Falls back to today's date if the timestamp can't be parsed.
    """
    try:
        dt = pd.to_datetime(timestamp_str)
    except Exception:
        dt = datetime.utcnow()

    return f"{HDFS_BASE_PATH}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"


# ---------------------------------------------------------------------------
# Step 2: Flush a buffer of events to HDFS as Parquet
# ---------------------------------------------------------------------------

def flush_buffer(client: InsecureClient, buffer: list) -> None:
    if not buffer:
        return

    df = pd.DataFrame(buffer)

    # Group by partition in case the buffer spans a date boundary
    grouped = defaultdict(list)
    for record in buffer:
        partition_path = get_partition_path(record.get("timestamp"))
        grouped[partition_path].append(record)

    for partition_path, records in grouped.items():
        part_df = pd.DataFrame(records)

        # Write Parquet to an in-memory buffer, then upload to HDFS
        parquet_buffer = io.BytesIO()
        part_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        file_name = f"part-{int(time.time())}-{uuid.uuid4().hex[:8]}.parquet"
        full_path = f"{partition_path}/{file_name}"

        client.write(full_path, data=parquet_buffer.read(), overwrite=True)
        logger.info(f"Flushed {len(records):,} events to {full_path}")

    logger.info(f"Flush complete. Total events written: {len(buffer):,}")


# ---------------------------------------------------------------------------
# Main consume loop
# ---------------------------------------------------------------------------

def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",   # catch messages sent before this started
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    hdfs_client = InsecureClient(HDFS_WEBHDFS_URL, user=HDFS_USER)

    buffer = []
    last_flush_time = time.time()

    logger.info(f"Listening on topic '{TOPIC_NAME}'... (Ctrl+C to stop)")

    try:
        for message in consumer:
            buffer.append(message.value)

            time_since_flush = time.time() - last_flush_time
            should_flush = (
                len(buffer) >= FLUSH_EVERY
                or time_since_flush >= FLUSH_INTERVAL_SECONDS
            )

            if should_flush:
                flush_buffer(hdfs_client, buffer)
                buffer = []
                last_flush_time = time.time()

    except KeyboardInterrupt:
        logger.info("Stopping consumer, flushing remaining buffer...")
        flush_buffer(hdfs_client, buffer)

    finally:
        consumer.close()
        logger.info("Consumer closed.")


if __name__ == "__main__":
    main()