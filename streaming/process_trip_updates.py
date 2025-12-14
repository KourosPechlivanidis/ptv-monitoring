import redis

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, to_date, to_unix_timestamp, to_utc_timestamp, concat_ws, split, lit, concat, date_add, lpad, when, desc, row_number
from pyspark.sql import Window

from utils import read_from_kafka, load_schema, parse_kafka_value

def write_to_redis(batch_df: DataFrame, batch_id: int) -> None:
    print(f"Writing batch id: {batch_id} to Redis")

    for row in batch_df.collect():
        if row.trip_id is None or row.delay_seconds is None:
            continue
        key = f"trip:{row.trip_id}"
        redis_client.hset(key, mapping={"current_delay": row.delay_seconds})

KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
REDIS_HOST = "localhost"
REDIS_PORT = "6379"

TOPIC = "trip_updates"
SCHEMA_PATH = "schemas/trip_updates.json"
STATIC_PATHS = [
    "static/bus/stop_times.parquet",
    "static/metro/stop_times.parquet",
    "static/tram/stop_times.parquet"
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

schema = load_schema(SCHEMA_PATH)
parsed_df = parse_kafka_value(raw_df, schema)


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

canonical_df = exploded_df.select(
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


dfs = []
for path in STATIC_PATHS:
    dfs.append(spark.read.parquet(path))

static_stop_times_df = dfs[0].unionByName(dfs[1]).unionByName(dfs[2])

enriched_df = canonical_df.join(
    static_stop_times_df,
    on=["trip_id", "stop_sequence"],
    how="left"
).withColumn(
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

enriched_df = enriched_df.withColumn(
    "delay_seconds", col("stop_arrival_time") - col("arrival_time_posix") - 3600
)

w = Window.partitionBy("trip_id").orderBy(desc("stop_sequence"))
latest_df = enriched_df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

query = (
    latest_df.writeStream
    .foreachBatch(write_to_redis)
    .start()
)

query.awaitTermination()
