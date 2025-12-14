import redis

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from utils import read_from_kafka, load_schema, parse_kafka_value

def write_to_redis(batch_df: DataFrame, batch_id: int) -> None:
    print(f"Writing batch id: {batch_id} to Redis")

    for row in batch_df.collect():
        if row.trip_id is None:
            continue

        key = f"trip:{row.trip_id}"

        redis_client.hset(
            key,
            mapping={
                "mode": row.mode,
                "route_id": row.route_id,
                "latitude": row.latitude,
                "longitude": row.longitude,
                "label": row.label,
                "timestamp": row.position_timestamp,
            },
        )

        redis_client.expire(key, 120)

KAFK_BOOTSTRAP_SERVERS = "localhost:9094"
REDIS_HOST = "localhost"
REDIS_PORT = "6379"

TOPIC = "vehicle_positions"
SCHEMA_PATH = "schemas/vehicle_positions.json"

spark = (
    SparkSession.builder
    .appName("VehiclePositionsKafkaConsumer")
    .getOrCreate()
)
raw_df = read_from_kafka(
    spark=spark,
    topic=TOPIC,
    bootstrap_servers=KAFK_BOOTSTRAP_SERVERS,
)

schema = load_schema(SCHEMA_PATH)
parsed_df = parse_kafka_value(raw_df, schema)

canonical_df = parsed_df.select(
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

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

query = (
    canonical_df.writeStream
    .foreachBatch(write_to_redis)
    .start()
)

query.awaitTermination()
