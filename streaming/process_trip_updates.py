import redis
import pandas as pd

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, to_date, to_unix_timestamp, to_utc_timestamp, concat_ws, split, lit, concat, date_add, lpad, when

from utils import read_from_kafka, load_schema, parse_kafka_value, write_to_redis, join_gtfs_with_schedule

def write_delay_batch(batch_df: DataFrame, batch_id: int) -> None:
    print(f"Writing delay batch {batch_id} to Redis")

    batch_df.foreachPartition(
        lambda rows: write_to_redis(
            rows,
            key_prefix="delay",
            ttl_seconds=120,
            field_mapper=lambda r: {
                "trip_id": r.trip_id,
                "last_stop_sequence": r.stop_sequence,
                "delay_seconds": r.delay_seconds,
            }
        )
    )

def keep_latest_stop(batch: pd.DataFrame, state: pd.DataFrame) -> pd.DataFrame:
    if state.empty:
        state = pd.DataFrame(columns=batch.columns)
    
    combined = pd.concat([state, batch], ignore_index=True)
    # Keep row with max stop_sequence per trip
    latest = combined.sort_values("stop_sequence", ascending=False).drop_duplicates(subset=["trip_id"])
    return latest


KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
REDIS_HOST = "redis"
REDIS_PORT = "6379"

TOPIC = "trip_updates"
SCHEMA_PATH = "schemas/trip_update.json"

STATIC_STOP_TIMES_PATHS = [
    "static/bus/stop_times",
    "static/metro/stop_times",
    "static/tram/stop_times"
]

spark = (
    SparkSession.builder
    .appName("TripUpdateKafkaConsumer")
    .getOrCreate()
)

raw_df = read_from_kafka(
    spark=spark,
    topic=TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
)

input_schema = load_schema(SCHEMA_PATH)
parsed_df = parse_kafka_value(raw_df, input_schema)

exploded_df = parsed_df.select(
    col("value.mode").alias("mode"),
    col("value.header.gtfs_realtime_version").alias("gtfs_realtime_version"),
    col("value.header.timestamp").cast("long").alias("source_timestamp"),
    col("value.entity_id").alias("entity_id"),
    col("value.trip_update.trip.trip_id").alias("trip_id"),
    col("value.trip_update.trip.route_id").alias("route_id"),
    col("value.trip_update.trip.start_time").alias("start_time"),
    col("value.trip_update.trip.start_date").alias("start_date"),
    col("value.trip_update.trip.schedule_relationship").alias("trip_schedule_relationship"),
    explode(col("value.trip_update.stop_time_update")).alias("stop_time_update")
)

df = exploded_df.select(
    col("mode"),
    col("gtfs_realtime_version"),
    col("source_timestamp"),
    col("entity_id"),
    col("trip_id"),
    col("route_id"),
    col("start_time"),
    col("start_date"),
    col("trip_schedule_relationship"),
    col("stop_time_update.stop_sequence").alias("stop_sequence"),
    col("stop_time_update.stop_id").alias("stop_id"),
    col("stop_time_update.arrival.time").alias("stop_arrival_time"),
    col("stop_time_update.departure.time").alias("stop_departure_time"),
    col("stop_time_update.schedule_relationship").alias("schedule_relationship")
).filter(col("trip_id").isNotNull())



df = join_gtfs_with_schedule(spark, df, STATIC_STOP_TIMES_PATHS, ["trip_id", "stop_sequence"])

enriched_df = df.withColumn(
    "start_date", to_date(col("start_date"), "yyyyMMdd")
)

enriched_df = enriched_df.withColumn(
    "hour", split("arrival_time", ":").getItem(0).cast("int")
).withColumn(
    "minute_second",
    concat(lit(":"), split("arrival_time", ":").getItem(1),
           lit(":"), split("arrival_time", ":").getItem(2))
).withColumn(
    "normalized_date", when(col("hour") >= 24, date_add(col("start_date"), 1)).otherwise(col("start_date"))
).withColumn(
    "normalized_hour", when(col("hour") >= 24, col("hour") - 24).otherwise(col("hour"))
).withColumn(
    "datetime_str",
    concat_ws(" ", col("normalized_date"), concat(lpad(col("normalized_hour").cast("string"), 2, "0"), col("minute_second")))
).withColumn(
    "arrival_time_posix",
    to_unix_timestamp(to_utc_timestamp(col("datetime_str"), "Australia/Melbourne"))
).drop("hour", "minute_second", "normalized_date", "normalized_hour", "datetime_str")


enriched_df = enriched_df.filter(
    col("arrival_time_posix").isNotNull() & col("stop_arrival_time").isNotNull()
)

enriched_df = enriched_df.withColumn(
    "delay_seconds",
    col("arrival_time_posix") - col("stop_arrival_time")
)

query = (
    enriched_df.writeStream
    .foreachBatch(write_delay_batch)
    .outputMode("update")
    .start()
)

query.awaitTermination()
