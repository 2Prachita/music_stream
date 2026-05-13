-- snowflake/setup.sql
-- ─────────────────────────────────────────────────────────────────────
-- Run this ONCE in your Snowflake worksheet before starting the pipeline.
-- Creates the database, schemas, and all tables.
-- Safe to re-run: uses IF NOT EXISTS everywhere.

-- ── Database ──────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS MUSIC_STREAM;

-- ── Schemas ───────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS MUSIC_STREAM.RAW;
CREATE SCHEMA IF NOT EXISTS MUSIC_STREAM.STREAMING;
CREATE SCHEMA IF NOT EXISTS MUSIC_STREAM.STAGING;
CREATE SCHEMA IF NOT EXISTS MUSIC_STREAM.MARTS;

-- ── RAW: landing zone ─────────────────────────────────────────────────
-- Raw events land here as VARIANT — schema-on-read, never lose data
CREATE TABLE IF NOT EXISTS MUSIC_STREAM.RAW.PLAY_EVENTS (
    raw_data      VARIANT                              COMMENT 'Full event JSON',
    consumed_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── STREAMING: Spark micro-batch output ───────────────────────────────
CREATE TABLE IF NOT EXISTS MUSIC_STREAM.STREAMING.AGG_ARTIST_PLAYS (
    window_start        TIMESTAMP_NTZ,
    window_end          TIMESTAMP_NTZ,
    artist_id           VARCHAR,
    artist_name         VARCHAR,
    play_count          INTEGER,
    unique_listeners    INTEGER,
    avg_completion_rate FLOAT,
    skip_count          INTEGER,
    skip_rate           FLOAT,
    loaded_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS MUSIC_STREAM.STREAMING.AGG_TRACK_PLAYS (
    window_start        TIMESTAMP_NTZ,
    window_end          TIMESTAMP_NTZ,
    track_id            VARCHAR,
    track_name          VARCHAR,
    artist_name         VARCHAR,
    play_count          INTEGER,
    unique_listeners    INTEGER,
    avg_completion_rate FLOAT,
    skip_count          INTEGER,
    loaded_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS MUSIC_STREAM.STREAMING.AGG_DEVICE_STATS (
    window_start    TIMESTAMP_NTZ,
    window_end      TIMESTAMP_NTZ,
    device          VARCHAR,
    country         VARCHAR,
    play_count      INTEGER,
    unique_users    INTEGER,
    loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── Verification queries (run after pipeline starts) ──────────────────
-- SELECT COUNT(*) FROM MUSIC_STREAM.RAW.PLAY_EVENTS;
-- SELECT * FROM MUSIC_STREAM.STREAMING.AGG_ARTIST_PLAYS ORDER BY window_start DESC LIMIT 10;
-- SELECT * FROM MUSIC_STREAM.STREAMING.AGG_TRACK_PLAYS  ORDER BY play_count DESC LIMIT 10;