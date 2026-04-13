# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Real-time public transport analytics pipeline for Melbourne's Yarra Trams using GTFS (General Transit Feed Specification) real-time data.

**Data flow:** GTFS API → Kafka → Spark Structured Streaming → Redis/S3 (Delta Lake) → dbt → Streamlit Dashboard

## Running the Project

```bash
# Start all services (Kafka, Redis, producer, Spark consumers, dashboard)
docker-compose up --build

# Start individual services
docker-compose up kafka producer
docker-compose up vehicle-positions-consumer
docker-compose up trip-updates-consumer
```

## Analytics (dbt + DuckDB)

```bash
cd analytics/analytics

# Run all models
dbt run

# Run a specific model
dbt run --select gold_route_hourly_stats

# Run tests
dbt test

# Generate docs
dbt docs generate && dbt docs serve
```

## Environment Variables

Copy `.env` and populate:
- `GTFS_API_KEY` — Victorian Transport API key
- `AWS_ACCESS_KEY` / `AWS_SECRET_KEY` — S3 credentials for Delta Lake
- `KAFKA_BOOTSTRAP_SERVERS` — defaults to `kafka:9092`
- `ENABLE_S3_SINK` / `ENABLE_REDIS_SINK` — feature flags for output sinks (boolean strings)

Runtime config is centralized in `streaming/config.py` as dataclasses.

## Architecture

### Ingestion (`ingestion/`)
- `producer/main.py` spawns 6 polling threads — 3 vehicle position feeds and 3 trip update feeds (tram/bus/metro)
- `producer/gtfs_parser.py` parses protobuf GTFS-RT messages into JSON
- Messages published to two Kafka topics: `vehicle_positions` and `trip_updates` (2 partitions, 7-day retention)
- `kafka-init/init-kafka.sh` creates topics on startup

### Streaming (`streaming/`)
Spark Structured Streaming jobs consuming from Kafka and writing to Redis and/or S3.

- `process_vehicle_positions.py` — enriches positions with static GTFS route/trip data from S3, writes to Redis (`vehicle:trip:{trip_id}`) with 120s TTL
- `process_trip_updates.py` — explodes stop_time_updates, joins with scheduled stop times, calculates `delay_seconds`, writes to Redis (`delay:trip:{trip_id}`) with 120s TTL
- Both consumers optionally write to S3 Delta Lake partitioned by year/month/day/hour (triggered every 1 minute)
- Static GTFS data is read from `s3a://ptv-gtfs-static/delta` (configured via `STATIC_DATA_PATH`)
- `utils.py` contains shared Spark session creation and Redis write helpers

### Analytics (`analytics/`)
dbt project using DuckDB adapter. Models read Delta Lake data from S3 using DuckDB's S3/Delta extensions.

- `intermediate/trip_updates_deduped.sql` — incremental model, deduped by `trip_id + route_id + stop_sequence`
- `mart/gold_route_hourly_stats.sql` — hourly KPIs per route joined with GTFS static routes/trips
- `mart/gold_stop_hourly_stats.sql` — hourly KPIs per stop
- Local dev uses `dev.duckdb` file

### Dashboard (`dashboard/`)
Streamlit app.

- `connector.py` — `DataConnector` class queries DuckDB for analytics and Redis for live data
- `app.py` — KPI cards, route leaderboard, stop popularity, live vehicle positions map

## Key Design Decisions

- Kafka topics use `replicas=1` — single-node dev setup, not production-grade
- Redis TTL is 120 seconds; streaming jobs must run continuously to keep cache populated
- `ENABLE_S3_SINK=false` in docker-compose by default — S3 writes are opt-in
- dbt incremental models use unique keys to handle duplicate Kafka message delivery
- Spark image is Apache Spark 3.5.7 with Scala 2.12, Java 11, and delta-spark + hadoop-aws JARs
