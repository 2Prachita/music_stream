"""
spark_streaming.py
──────────────────
PySpark Structured Streaming job.

Reads play-events from Kafka → applies 5-minute tumbling windows →
writes aggregated results to Snowflake STREAMING schema.

Aggregations computed per window:
  - Plays per artist
  - Top tracks by play count
  - Skip rate per track
  - Device breakdown
  - Unique listener count per artist

Run: python -m consumer.spark_streaming

Requires: pip install pyspark and Java 8/11/17/19 installed
"""

import os
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SNOWFLAKE_OPTS  = {
    "sfURL":       f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
    "sfUser":      os.getenv("SNOWFLAKE_USER"),
    "sfPassword":  os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase":  "MUSIC_STREAM",
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "sfRole":      os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
}


def create_spark_session():
    """
    Create SparkSession with Kafka + Snowflake connectors.
    Packages are auto-downloaded on first run (requires internet).
    """
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .appName("MusicStreamPipeline")
        .config("spark.jars.packages",
                # Kafka connector + Snowflake connector for Spark
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.5,"
                "net.snowflake:snowflake-jdbc:3.14.4")
        # Reduce log verbosity
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def define_schema():
    """Schema for incoming JSON events from Kafka."""
    from pyspark.sql.types import (
        StructType, StructField, StringType,
        IntegerType, BooleanType, DoubleType
    )
    return StructType([
        StructField("event_id",    StringType(),  True),
        StructField("user_id",     StringType(),  True),
        StructField("track_id",    StringType(),  True),
        StructField("track_name",  StringType(),  True),
        StructField("artist_id",   StringType(),  True),
        StructField("artist_name", StringType(),  True),
        StructField("duration_ms", IntegerType(), True),
        StructField("played_ms",   IntegerType(), True),
        StructField("skipped",     BooleanType(), True),
        StructField("device",      StringType(),  True),
        StructField("country",     StringType(),  True),
        StructField("timestamp",   StringType(),  True),
        StructField("source",      StringType(),  True),
        StructField("completion_rate", DoubleType(), True),
    ])


def write_to_snowflake(df, table: str, schema: str = "STREAMING"):
    """Write a Spark DataFrame to Snowflake (batch write)."""
    opts = {**SNOWFLAKE_OPTS, "sfSchema": schema, "dbtable": table}
    (df.write
       .format("net.snowflake.spark.snowflake")
       .options(**opts)
       .mode("append")
       .save())


def run():
    from pyspark.sql import functions as F
    from pyspark.sql.types import TimestampType

    logging.basicConfig(level=logging.INFO)
    logger.info("Starting PySpark Structured Streaming job")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    schema = define_schema()

    # ── Read stream from Kafka ─────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "play-events")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 1000)  # process max 1000 msgs per micro-batch
        .load()
    )

    # Kafka value is bytes → parse JSON using our schema
    events = (
        raw_stream
        .select(
            F.from_json(
                F.col("value").cast("string"),
                schema
            ).alias("data")
        )
        .select("data.*")
        # Parse timestamp string → proper timestamp for windowing
        .withColumn("event_time",
                    F.to_timestamp(F.col("timestamp")).cast(TimestampType()))
        # Add watermark: tolerate up to 2 minutes of late data
        .withWatermark("event_time", "2 minutes")
    )

    # ── Aggregation 1: Plays per artist per 5-min window ──────────────
    artist_plays = (
        events
        .groupBy(
            F.window("event_time", "5 minutes"),   # tumbling window
            F.col("artist_id"),
            F.col("artist_name"),
        )
        .agg(
            F.count("*").alias("play_count"),
            F.countDistinct("user_id").alias("unique_listeners"),
            F.avg("completion_rate").alias("avg_completion_rate"),
            F.sum(F.when(F.col("skipped"), 1).otherwise(0)).alias("skip_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "artist_id", "artist_name",
            "play_count", "unique_listeners",
            F.round("avg_completion_rate", 3).alias("avg_completion_rate"),
            "skip_count",
            F.round(
                F.col("skip_count") / F.col("play_count"), 3
            ).alias("skip_rate"),
        )
    )

    # ── Aggregation 2: Top tracks per 5-min window ────────────────────
    track_plays = (
        events
        .groupBy(
            F.window("event_time", "5 minutes"),
            F.col("track_id"),
            F.col("track_name"),
            F.col("artist_name"),
        )
        .agg(
            F.count("*").alias("play_count"),
            F.countDistinct("user_id").alias("unique_listeners"),
            F.avg("completion_rate").alias("avg_completion_rate"),
            F.sum(F.when(F.col("skipped"), 1).otherwise(0)).alias("skip_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "track_id", "track_name", "artist_name",
            "play_count", "unique_listeners",
            F.round("avg_completion_rate", 3).alias("avg_completion_rate"),
            "skip_count",
        )
    )

    # ── Aggregation 3: Device breakdown per window ────────────────────
    device_stats = (
        events
        .groupBy(
            F.window("event_time", "5 minutes"),
            F.col("device"),
            F.col("country"),
        )
        .agg(
            F.count("*").alias("play_count"),
            F.countDistinct("user_id").alias("unique_users"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "device", "country", "play_count", "unique_users",
        )
    )

    # ── Write each aggregation to Snowflake via foreachBatch ──────────
    def write_artist_plays(batch_df, epoch_id):
        if batch_df.count() > 0:
            write_to_snowflake(batch_df, "AGG_ARTIST_PLAYS")
            logger.info("Wrote artist_plays batch epoch=%d rows=%d",
                        epoch_id, batch_df.count())

    def write_track_plays(batch_df, epoch_id):
        if batch_df.count() > 0:
            write_to_snowflake(batch_df, "AGG_TRACK_PLAYS")
            logger.info("Wrote track_plays batch epoch=%d rows=%d",
                        epoch_id, batch_df.count())

    def write_device_stats(batch_df, epoch_id):
        if batch_df.count() > 0:
            write_to_snowflake(batch_df, "AGG_DEVICE_STATS")
            logger.info("Wrote device_stats batch epoch=%d rows=%d",
                        epoch_id, batch_df.count())

    # Start streaming queries (micro-batch every 30 seconds)
    q1 = (artist_plays.writeStream
          .foreachBatch(write_artist_plays)
          .trigger(processingTime="30 seconds")
          .option("checkpointLocation", "/tmp/checkpoints/artist_plays")
          .start())

    q2 = (track_plays.writeStream
          .foreachBatch(write_track_plays)
          .trigger(processingTime="30 seconds")
          .option("checkpointLocation", "/tmp/checkpoints/track_plays")
          .start())

    q3 = (device_stats.writeStream
          .foreachBatch(write_device_stats)
          .trigger(processingTime="30 seconds")
          .option("checkpointLocation", "/tmp/checkpoints/device_stats")
          .start())

    logger.info("All 3 streaming queries running. Ctrl+C to stop.\n")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()