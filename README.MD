# Yarra Trams — Real-Time Transit Analytics Pipeline

A end-to-end streaming data pipeline that ingests live GTFS-RT feeds from Melbourne's public transport network, calculates on-time performance and delays in real time, and surfaces the results in an interactive dashboard.

![Dashboard screenshot placeholder](docs/dashboard.png)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Victorian Transport API                       │
│              (GTFS-RT feeds: tram / bus / metro)                    │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  protobuf over HTTP (poll every 5–60s)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Ingestion Layer                               │
│   Python producer — 6 polling threads (3 modes × 2 feed types)     │
│   Parses protobuf → JSON, publishes to Kafka                        │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
              ┌─────────────┴──────────────┐
              ▼                            ▼
     vehicle_positions              trip_updates
       (Kafka topic)                (Kafka topic)
              │                            │
              ▼                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       Streaming Layer (Spark)                        │
│  vehicle-positions-consumer    trip-updates-consumer                 │
│  • Join with GTFS static       • Explode stop_time_updates          │
│    routes + trips              • Join with scheduled stop times      │
│  • Resolve route name,         • Calculate delay_seconds            │
│    headsign                    • Handle post-midnight schedules      │
└──────────────┬────────────────────────────┬─────────────────────────┘
               │                            │
       ┌───────┴───────┐            ┌───────┴───────┐
       ▼               ▼            ▼               ▼
  Redis (live)     S3 Delta     Redis (live)    S3 Delta
  vehicle:trip:*   Lake         delay:trip:*    Lake
  TTL 120s         (silver)     TTL 120s        (silver)
       │                                            │
       │                                            ▼
       │                          ┌─────────────────────────────────┐
       │                          │       Analytics Layer (dbt)      │
       │                          │  • trip_updates_deduped          │
       │                          │    (incremental, dedup by        │
       │                          │     trip+stop+hour)              │
       │                          │  • gold_route_hourly_stats       │
       │                          │  • gold_stop_hourly_stats        │
       │                          │  Runs every 5 min → DuckDB       │
       │                          └────────────────┬────────────────┘
       │                                           │
       └──────────────────┬────────────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       Dashboard (Dash / Plotly)                      │
│  • Live vehicle map (colour-coded by mode)                          │
│  • On-time performance gauge                                         │
│  • Problem routes leaderboard (lowest OTP)                          │
│  • Most visited stops                                                │
│  • 24-hour delay trend                                               │
│  Refreshes every 5 seconds                                           │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, `gtfs-realtime-bindings` (protobuf), `kafka-python` |
| Message broker | Apache Kafka (KRaft, single-node) |
| Stream processing | Apache Spark 3.5 Structured Streaming, Delta Lake |
| Live cache | Redis 7 (TTL-based, 120s) |
| Historical storage | AWS S3 + Delta Lake (partitioned by year/month/day/hour) |
| Analytical layer | dbt-core, dbt-duckdb, DuckDB |
| Dashboard | Dash, Plotly, Dash Mantine Components |
| Orchestration | Docker Compose |

---

## Prerequisites

- Docker and Docker Compose
- A [Victorian Transport API key](https://www.ptv.vic.gov.au/footer/data-and-reporting/datasets/ptv-timetable-api/) (free)
- AWS credentials with read/write access to S3 (required for the historical analytics layer)

---

## Getting Started

**1. Configure environment variables**

```bash
cp .env.example .env
```

Edit `.env` and fill in:

```
GTFS_API_KEY=your_ptv_api_key
AWS_ACCESS_KEY=your_aws_access_key
AWS_SECRET_KEY=your_aws_secret_key
```

All runtime settings are controlled via environment variables:

| Variable | Default | Description |
|---|---|---|
| `GTFS_API_KEY` | — | Victorian Transport API key |
| `AWS_ACCESS_KEY` / `AWS_SECRET_KEY` | — | S3 credentials for Delta Lake |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `REDIS_HOST` | `redis` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `ENABLE_REDIS_SINK` | `false` | Write streaming output to Redis |
| `ENABLE_S3_SINK` | `false` | Write streaming output to S3 Delta Lake |
| `STATIC_DATA_PATH` | `s3a://ptv-gtfs-static/delta` | S3 path for static GTFS Delta tables |
| `APP_TIMEZONE` | `Australia/Melbourne` | Timezone for delay calculations |
| `DUCKDB_PATH` | `/duckdb/dev.duckdb` | DuckDB file path (shared Docker volume) |

> In `docker-compose.yml`, `ENABLE_REDIS_SINK=True` and `ENABLE_S3_SINK=False` by default. Enable S3 writes only if you want historical data persisted for the dbt analytics layer.

**2. Upload GTFS static reference data to S3** *(one-time setup)*

The streaming jobs enrich live vehicle data with route names, headsigns, and scheduled stop times from GTFS static files. Run the uploader once before starting the pipeline:

```bash
cd ingestion/static-uploader
pip install -r requirements.txt
python main.py
```

This downloads the PTV GTFS ZIP and writes `routes`, `trips`, `stops`, and `stop_times` tables to `s3://ptv-gtfs-static/delta/`.

**3. Start the full pipeline**

```bash
docker-compose up --build
```

This starts all services in order:

| Service | What it does |
|---|---|
| `kafka` | Message broker (KRaft, no Zookeeper) |
| `init-kafka` | Creates `vehicle_positions` and `trip_updates` topics |
| `producer` | Polls GTFS-RT API, publishes to Kafka |
| `redis` | Live vehicle and delay cache |
| `vehicle-positions-consumer` | Spark job: positions → Redis + S3 |
| `trip-updates-consumer` | Spark job: delays → Redis + S3 |
| `dbt-runner` | Refreshes DuckDB analytics every 5 minutes |
| `dashboard` | Dash app at **http://localhost:8050** |

Allow ~2 minutes for Kafka to become healthy and the first Spark micro-batches to land in Redis before the dashboard shows live data. The dbt analytics layer will populate after the first dbt-runner cycle (~5 minutes).

---

## Analytics (dbt + DuckDB)

The `analytics/` directory is a dbt project using the DuckDB adapter. Models read Delta Lake data from S3 using DuckDB's native S3 and Delta extensions.

```bash
cd analytics/analytics

# Run all models
dbt run

# Run a specific model
dbt run --select gold_route_hourly_stats

# Run data tests
dbt test

# Browse generated docs
dbt docs generate && dbt docs serve
```

**Model layers:**

| Model | Materialisation | Description |
|---|---|---|
| `trip_updates_deduped` | Incremental | Deduplicates Kafka messages by `trip_id + route_id + stop_sequence`, keeping the latest update per stop |
| `gold_route_hourly_stats` | Table | Hourly OTP and average delay per route, joined with static GTFS route/trip metadata |
| `gold_stop_hourly_stats` | Table | Hourly visit counts per stop, joined with static GTFS stop metadata |

---

## Dashboard

| Panel | Data source | Description |
|---|---|---|
| Active Vehicles | Redis (live) | Count of vehicles currently reporting positions |
| Data Freshness | Redis (live) | Seconds since the most recent position update |
| OTP Gauge | DuckDB | `on_time_stops / total_stops` — a stop is on-time if delay < 5 min |
| Problem Routes | DuckDB | Routes ranked by lowest on-time performance |
| Most Visited Stops | DuckDB | Stops ranked by total stop events |
| Live Map | Redis (live) | Vehicle positions, colour-coded by mode |
| 24h Delay Trend | DuckDB | Hourly average delay in minutes over the last 24 hours |

Use the **All / Bus / Tram / Train** filter to segment all panels by transport mode.

---

## Project Structure

```
├── ingestion/
│   ├── producer/          # Python GTFS-RT poller (6 threads)
│   ├── kafka-init/        # Topic creation script
│   └── static-uploader/   # One-time GTFS static → S3 loader
├── streaming/             # Spark Structured Streaming consumers
│   └── schemas/           # Kafka message schemas (JSON)
├── analytics/             # dbt project (DuckDB adapter)
│   └── analytics/
│       └── models/
│           ├── intermediate/  # trip_updates_deduped
│           └── mart/          # gold_route_hourly_stats, gold_stop_hourly_stats
├── dashboard/             # Dash app + DataConnector
├── docker-compose.yml
└── .env.example
```

---

## Key Design Decisions

**Why Redis for live data?** The Spark micro-batches land every few seconds. Redis provides sub-millisecond reads for the dashboard's 5-second refresh cycle without putting load on the analytical layer.

**Why Delta Lake on S3?** ACID guarantees on object storage allow the dbt incremental models to safely deduplicate Kafka's at-least-once delivery without a traditional database.

**Why DuckDB for analytics?** The analytical queries (OTP aggregations, stop rankings) run entirely on the dbt-materialised DuckDB file. This eliminates a running database process and keeps the stack self-contained.

**Why dbt incremental models?** Trip update messages are re-published whenever a vehicle reports a new position. The `trip_updates_deduped` model uses `ROW_NUMBER() OVER (PARTITION BY trip_id, stop_sequence ORDER BY source_timestamp DESC)` to keep only the latest update per stop, avoiding inflated delay averages.
