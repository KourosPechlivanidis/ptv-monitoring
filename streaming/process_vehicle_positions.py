import redis

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, coalesce, lit

from utils import read_from_kafka, load_schema, parse_kafka_value, write_to_redis, join_gtfs_with_schedule

def write_vehicle_batch(df: DataFrame, batch_id: int) -> None:
    print(f"Writing vehicle batch {batch_id} to Redis")

    df.foreachPartition(
        lambda rows: write_to_redis(
            rows,
            key_prefix="vehicle",
            ttl_seconds=120,
            field_mapper=lambda r: {
                "trip_id": r.trip_id,
                "mode": r.mode,
                "route_id": r.route_id,
                "route_name": r.resolved_route_name,
                "headsign": r.resolved_headsign,
                "latitude": r.latitude,
                "longitude": r.longitude,
                "label": r.label,
                "timestamp": r.position_timestamp,
            }
        )
    )

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "vehicle_positions"
SCHEMA_PATH = "schemas/vehicle_position.json"

STATIC_ROUTES_PATHS = [
    "static/bus/routes",
    "static/metro/routes",
    "static/tram/routes"
]

STATIC_TRIPS_PATHS = [
    "static/bus/trips",
    "static/metro/trips",
    "static/tram/trips"
]

spark = (
    SparkSession.builder
    .appName("VehiclePositionsKafkaConsumer")
    .getOrCreate()
)
raw_df = read_from_kafka(
    spark=spark,
    topic=TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
)

schema = load_schema(SCHEMA_PATH)
parsed_df = parse_kafka_value(raw_df, schema)

df = parsed_df.select(
    col("value.mode"),
    col("value.header.gtfs_realtime_version").alias("gtfs_realtime_version"),
    col("value.header.timestamp").cast("long").alias("source_timestamp"),
    col("value.entity_id"),
    col("value.vehicle.trip.trip_id").alias("trip_id"),
    col("value.vehicle.trip.route_id").alias("route_id"),
    col("value.vehicle.trip.start_time").alias("start_time"),
    col("value.vehicle.trip.start_date").alias("start_date"),
    col("value.vehicle.vehicle.vehicle_id").alias("vehicle_id"),
    col("value.vehicle.vehicle.label").alias("label"),
    col("value.vehicle.position.latitude").cast("double").alias("latitude"),
    col("value.vehicle.position.longitude").cast("double").alias("longitude"),
    col("value.vehicle.timestamp").cast("long").alias("position_timestamp"),
)


df = join_gtfs_with_schedule(spark, df, STATIC_ROUTES_PATHS, ["route_id"], ["route_id","route_short_name", "route_long_name"])
df = join_gtfs_with_schedule(spark, df, STATIC_TRIPS_PATHS, ["trip_id"], ["trip_id", "trip_headsign"])

df = df.withColumn(
    "resolved_route_name",
    coalesce(
        col("route_short_name"),
        col("route_long_name"),
        col("route_id"),
        lit("UNKNOWN_ROUTE")
    )
).withColumn(
    "resolved_headsign",
    coalesce(
        col("trip_headsign"),
        lit("UNKNOWN_DESTINATION")
    )
)

query = (
    df.writeStream
    .foreachBatch(write_vehicle_batch)
    .start()
)

query.awaitTermination()
