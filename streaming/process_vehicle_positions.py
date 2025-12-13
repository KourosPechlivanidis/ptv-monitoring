from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
import redis


spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", "vehicle_positions") \
    .option("startingOffsets", "latest") \
    .load()


schema = StructType([
     StructField("mode", StringType(), True),
     StructField("header", StructType([
         StructField("gtfs_realtime_version", StringType(), True),
         StructField("incrementality", LongType(), True),
         StructField("timestamp", LongType(), True)
     ])),
     StructField("entity_id", StringType(), True),
     StructField("vehicle", StructType([
         StructField("trip", StructType([
             StructField("trip_id", StringType(), True),
             StructField("route_id", StringType(), True),
             StructField("start_time", StringType(), True),
             StructField("start_date", StringType(), True)
         ])),
         StructField("vehicle", StructType([
             StructField("vehicle_id", StringType(), True),
             StructField("label", StringType(), True)
         ])),
         StructField("position", StructType([
             StructField("latitude", DoubleType(), True),
             StructField("longitude", DoubleType(), True)
         ])),
         StructField("timestamp", LongType(), True)
     ]))
 ])

# Kafka value is binary, so cast it
df = df.select(col("value").cast("string"))
df = df.select(from_json(col("value"), schema).alias("value"))

canonical_df = df.select(
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
     col("value.vehicle.timestamp").cast("long").alias("position_timestamp")
)

r = redis.Redis(host='localhost', port=6379, db=0)

def write_to_redis(batch_df, batch_id):
    print(f"Writing batch id: {batch_id} to redis")
    for row in batch_df.collect():
        key = f"trip:{row.trip_id}"
        value = {
            "mode": row.mode,
            "trip_id": row.trip_id,
            "route_id": row.route_id,
            "latitude": row.latitude,
            "longitude": row.longitude,
            "label": row.label,
            "timestamp": row.position_timestamp
        }
        r.hmset(key, value) 
        r.expire("key", 120)

query = canonical_df.writeStream \
    .foreachBatch(write_to_redis) \
    .start()

query.awaitTermination()