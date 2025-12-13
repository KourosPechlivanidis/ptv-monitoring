from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_utc_timestamp, from_json, explode, to_date, to_unix_timestamp, concat_ws, desc, split, lit, when, concat, date_add, lpad, row_number
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, IntegerType
from pyspark.sql import Window
import redis


spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .getOrCreate()

# read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", "trip_updates") \
    .option("startingOffsets", "latest") \
    .load()

# Define schema
schema = StructType([
     StructField("header", StructType([
        StructField("gtfs_realtime_version", StringType(), True),
        StructField("incrementality", IntegerType(), True),
        StructField("timestamp", LongType(), True)
    ])),
    StructField("entity_id", StringType(), True),
    StructField("mode", StringType(), True),
    StructField("trip_update", StructType([
        StructField("trip", StructType([
            StructField("trip_id", StringType(), True),
            StructField("route_id", StringType(), True),
            StructField("start_time", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("schedule_relationship", IntegerType(), True)
        ])),
        StructField("stop_time_update", ArrayType(
            StructType([
                StructField("stop_sequence", IntegerType(), True),
                StructField("arrival", StructType([
                    StructField("time", LongType(), True)
                ]), True),
                StructField("departure", StructType([
                    StructField("time", LongType(), True)
                ]), True),
                StructField("stop_id", StringType(), True),
                StructField("schedule_relationship", IntegerType(), True)
            ])
        ), True)
    ]))
])


# Cast Kafka value and parse JSON
df = df.select(col("value").cast("string"))
df = df.select(from_json(col("value"), schema).alias("value"))

exploded_df = df.select(
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

canoncial_df = exploded_df.select(
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
)

canoncial_df = canoncial_df.filter(col("trip_id").isNotNull())

# Read static stop times
dfs = []
paths = ["static/bus/stop_times.parquet", "static/metro/stop_times.parquet", "static/tram/stop_times.parquet"]
for path in paths:
    df_static = spark.read.parquet(path)
    dfs.append(df_static)

static_stop_times_df = dfs[0].unionByName(dfs[1]).unionByName(dfs[2])

# Join canonical DF with static stop times
enriched_df = canoncial_df.join(
    static_stop_times_df,
    on=["trip_id", "stop_sequence"],
    how="left"
)

enriched_df = enriched_df.withColumn(
    "start_date",
    to_date(col("start_date"), "yyyyMMdd")
)

# Normalize extended-hour arrival times
enriched_df = enriched_df.withColumn(
    "hour", split("arrival_time", ":").getItem(0).cast("int")
).withColumn(
    "minute_second", concat(lit(":"), split("arrival_time", ":").getItem(1), lit(":"), split("arrival_time", ":").getItem(2))
).withColumn(
    "normalized_date",
    when(col("hour") >= 24, date_add(col("start_date"), 1)).otherwise(col("start_date"))
).withColumn(
    "normalized_hour",
    when(col("hour") >= 24, col("hour") - 24).otherwise(col("hour"))
).withColumn(
    "datetime_str",
    concat_ws(" ", col("normalized_date"), concat(lpad(col("normalized_hour").cast("string"), 2, "0"), col("minute_second")))
).withColumn(
    "arrival_time_posix",
    to_unix_timestamp(to_utc_timestamp(col("datetime_str"), "Australia/Melbourne"))  # <-- converts to integer POSIX timestamp
).drop("hour", "minute_second", "normalized_date", "normalized_hour", "datetime_str")

enriched_df = enriched_df.withColumn("delay_seconds", col("stop_arrival_time") - col("arrival_time_posix"))

enriched_df = enriched_df.withColumn(
    "delay_seconds",
    col("delay_seconds") - 3600
)

w = Window.partitionBy("trip_id").orderBy(desc("stop_sequence"))

latest_df = enriched_df.withColumn(
    "rn", row_number().over(w)
).filter(col("rn") == 1).drop("rn")

# Redis write function (commented)
r = redis.Redis(host='localhost', port=6379, db=0)
def write_to_redis(batch_df, batch_id):
    print(f"Writing batch id: {batch_id} to redis")
    for row in batch_df.collect():
        key = f"trip:{row.trip_id}"
        if r.exists(key):
            if row.delay_seconds is None:
                continue
            
            value = {
                "current_delay": row.delay_seconds,
            }
            r.hmset(key, value) 

query = enriched_df.writeStream \
    .foreachBatch(write_to_redis) \
    .start()

query.awaitTermination()


 
