"""
kafka_producer.py
─────────────────
Publishes play events to Kafka topics.

Topics:
  play-events  → all play events (partitioned by user_id)
  skip-events  → only skipped events (subset, useful for skip-rate analysis)

Partitioning by user_id ensures all events for one user go to the same
partition — preserving ordering per user for session analytics.

Run: python -m producer.kafka_producer
     python -m producer.kafka_producer --rate 10  (10 events/sec)
     python -m producer.kafka_producer --batch 1000  (publish 1000 then stop)
"""

import os
import sys
import time
import logging
import argparse
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

from producer.event_generator import PlayEventGenerator
from producer.event_schema import PlayEvent

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_PLAY      = "play-events"
TOPIC_SKIP      = "skip-events"


def make_producer() -> KafkaProducer:
    """Create and return a configured KafkaProducer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        # Partition key = user_id encoded as bytes
        # Kafka uses this to assign the message to a consistent partition
        key_serializer   = lambda k: k.encode("utf-8"),
        value_serializer = lambda v: v.encode("utf-8"),
        # Batching: wait up to 10ms to batch messages → better throughput
        linger_ms        = 10,
        # Compression reduces network + storage overhead
        compression_type = "gzip",
        # Retry on transient failures
        retries          = 5,
        retry_backoff_ms = 200,
        # Wait for all in-sync replicas to ack (durability)
        acks             = "all",
    )


def on_send_success(record_metadata):
    logger.debug(
        "Delivered → topic=%s partition=%d offset=%d",
        record_metadata.topic, record_metadata.partition, record_metadata.offset
    )


def on_send_error(exc):
    logger.error("Failed to deliver message: %s", exc)


def publish_event(producer: KafkaProducer, event: PlayEvent) -> None:
    """Publish a single event to the appropriate Kafka topic(s)."""
    payload = event.to_json()

    # All events → play-events topic, keyed by user_id
    producer.send(
        TOPIC_PLAY,
        key=event.user_id,
        value=payload
    ).add_callback(on_send_success).add_errback(on_send_error)

    # Skipped events also go to skip-events topic for separate analysis
    if event.skipped:
        producer.send(
            TOPIC_SKIP,
            key=event.user_id,
            value=payload
        ).add_callback(on_send_success).add_errback(on_send_error)


def run(rate: float = 5.0, max_events: int = None):
    """
    Continuously publish play events.

    Args:
        rate:       events per second (default 5)
        max_events: stop after N events (None = run forever)
    """
    logger.info("Connecting to Kafka at %s", KAFKA_BOOTSTRAP)
    producer  = make_producer()
    generator = PlayEventGenerator()
    interval  = 1.0 / rate
    count     = 0

    logger.info(
        "Producer started — rate=%.1f events/sec | topics=[%s, %s]",
        rate, TOPIC_PLAY, TOPIC_SKIP
    )
    logger.info("Press Ctrl+C to stop\n")

    try:
        while True:
            event = generator.generate()
            publish_event(producer, event)
            count += 1

            # Log a summary every 100 events
            if count % 100 == 0:
                logger.info(
                    "Published %d events | last: %s - %s (%s)",
                    count, event.artist_name, event.track_name,
                    "SKIP" if event.skipped else "PLAY"
                )

            if max_events and count >= max_events:
                logger.info("Reached max_events=%d — stopping", max_events)
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("\nInterrupted — flushing remaining messages...")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed. Total events published: %d", count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Music play event Kafka producer")
    parser.add_argument("--rate",  type=float, default=5.0,
                        help="Events per second (default: 5)")
    parser.add_argument("--batch", type=int,   default=None,
                        help="Stop after N events (default: run forever)")
    args = parser.parse_args()
    run(rate=args.rate, max_events=args.batch)