"""
raw_consumer.py
────────────────
Reads raw play events from Kafka → lands to Snowflake RAW.PLAY_EVENTS.

Why keep a raw consumer alongside PySpark?
  - Durability: raw events in Snowflake before any transformation
  - Replayability: if Spark aggregation logic changes, re-derive from raw
  - Debugging: query individual events without going through Spark

Batching strategy:
  - Flush to Snowflake every BATCH_SIZE events OR FLUSH_INTERVAL seconds
  - Whichever comes first — balances latency vs insert efficiency

Run: python -m consumer.raw_consumer
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_PLAY       = "play-events"
CONSUMER_GROUP   = "raw-events-loader"

BATCH_SIZE       = 100    # insert every 100 events
FLUSH_INTERVAL   = 30     # or every 30 seconds


def _get_snowflake_conn():
    return snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database  = "MUSIC_STREAM",
        schema    = "RAW",
        role      = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def _insert_batch(cur, batch: list[dict]) -> int:
    """Bulk insert a batch of raw events as VARIANT JSON."""
    if not batch:
        return 0
    sql = """
        INSERT INTO MUSIC_STREAM.RAW.PLAY_EVENTS (raw_data, consumed_at)
        SELECT PARSE_JSON(%s), %s::TIMESTAMP_NTZ
    """
    data = [(json.dumps(e), datetime.now(timezone.utc).isoformat()) for e in batch]
    cur.executemany(sql, data)
    return len(batch)


def run():
    logger.info("Starting raw consumer — group=%s | topic=%s", CONSUMER_GROUP, TOPIC_PLAY)

    consumer = KafkaConsumer(
        TOPIC_PLAY,
        bootstrap_servers  = KAFKA_BOOTSTRAP,
        group_id           = CONSUMER_GROUP,
        auto_offset_reset  = "latest",          # start from newest messages
        enable_auto_commit = False,             # manual commit after Snowflake write
        value_deserializer = lambda v: json.loads(v.decode("utf-8")),
        key_deserializer   = lambda k: k.decode("utf-8") if k else None,
        max_poll_records   = BATCH_SIZE,
        session_timeout_ms = 30000,
    )

    conn  = _get_snowflake_conn()
    cur   = conn.cursor()
    batch = []
    last_flush = time.time()
    total = 0

    logger.info("Connected to Snowflake and Kafka. Waiting for events...\n")

    try:
        while True:
            # Poll with 1-second timeout so we check flush interval regularly
            records = consumer.poll(timeout_ms=1000)

            for tp, messages in records.items():
                for msg in messages:
                    batch.append(msg.value)

            # Flush condition: batch full OR time elapsed
            should_flush = (
                len(batch) >= BATCH_SIZE or
                (batch and time.time() - last_flush >= FLUSH_INTERVAL)
            )

            if should_flush:
                try:
                    inserted = _insert_batch(cur, batch)
                    conn.commit()
                    consumer.commit()        # only commit offset after successful write
                    total += inserted
                    logger.info(
                        "Flushed %d events to Snowflake (total: %d)",
                        inserted, total
                    )
                    batch = []
                    last_flush = time.time()
                except Exception as e:
                    logger.error("Snowflake insert failed: %s — retrying next cycle", e)
                    conn.rollback()
                    # Don't clear batch — retry on next flush

    except KeyboardInterrupt:
        logger.info("\nStopping consumer...")
        if batch:
            logger.info("Flushing remaining %d events...", len(batch))
            _insert_batch(cur, batch)
            conn.commit()
    finally:
        cur.close()
        conn.close()
        consumer.close()
        logger.info("Consumer closed. Total inserted: %d", total)


if __name__ == "__main__":
    run()