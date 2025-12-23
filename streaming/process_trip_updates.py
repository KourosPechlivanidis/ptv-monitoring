import pandas as pd
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from utils import (read_from_kafka, 
                   load_schema, 
                   parse_kafka_value, 
                   write_to_redis, 
                   join_gtfs_with_schedule
)

from config import TripUpdateConfig

def write_delay_batch(df: DataFrame, batch_id: int, config: TripUpdateConfig) -> None:
    
    """
   
    Function that maps and writes fields to redis
    
    :param df: microbatch as a spark df that is written to redis
    :type batch_id: DataFrame
    
    """

    print(f"Writing delay batch {batch_id} to Redis")

    df.foreachPartition(
        lambda rows: write_to_redis(
            rows,
            key_prefix="delay",
            ttl_seconds=120,
            redis_host=config.redis_host,
            redis_port=config.redis_port,
            field_mapper=lambda r: {
                "trip_id": r.trip_id,
                "last_stop_sequence": r.stop_sequence,
                "delay_seconds": r.delay_seconds,
            }
        )
    )


config = TripUpdateConfig()

spark = (
    SparkSession.builder
    .appName("TripUpdateKafkaConsumer")
    .getOrCreate()
)

raw_df = read_from_kafka(
    spark=spark,
    topic=config.topic,
    bootstrap_servers=config.kafka_bootstrap_servers
)

input_schema = load_schema(config.schema_path)
parsed_df = parse_kafka_value(raw_df, input_schema)

exploded_df = parsed_df.select(
    F.col("value.mode").alias("mode"),
    F.col("value.header.gtfs_realtime_version").alias("gtfs_realtime_version"),
    F.col("value.header.timestamp").cast("long").alias("source_timestamp"),
    F.col("value.entity_id").alias("entity_id"),
    F.col("value.trip_update.trip.trip_id").alias("trip_id"),
    F.col("value.trip_update.trip.route_id").alias("route_id"),
    F.col("value.trip_update.trip.start_time").alias("start_time"),
    F.col("value.trip_update.trip.start_date").alias("start_date"),
    F.col("value.trip_update.trip.schedule_relationship").alias("trip_schedule_relationship"),
    F.explode(F.col("value.trip_update.stop_time_update")).alias("stop_time_update")
)

df = exploded_df.select(
    F.col("mode"),
    F.col("gtfs_realtime_version"),
    F.col("source_timestamp"),
    F.col("entity_id"),
    F.col("trip_id"),
    F.col("route_id"),
    F.col("start_time"),
    F.col("start_date"),
    F.col("trip_schedule_relationship"),
    F.col("stop_time_update.stop_sequence").alias("stop_sequence"),
    F.col("stop_time_update.stop_id").alias("stop_id"),
    F.col("stop_time_update.arrival.time").alias("stop_arrival_time"),
    F.col("stop_time_update.departure.time").alias("stop_departure_time"),
    F.col("stop_time_update.schedule_relationship").alias("schedule_relationship")
).filter(F.col("trip_id").isNotNull())


df = df.withColumn("effective_stop_time", 
                    F.coalesce(F.col("stop_arrival_time"), F.col("stop_departure_time")))

df = df.withColumn(
    "effective_stop_time_calculation_method",
    F.when(F.col("stop_arrival_time").isNotNull(), "arrival")
     .when(F.col("stop_departure_time").isNotNull(), "departure")
     .otherwise(None)
)

df = join_gtfs_with_schedule(spark, df, config.stop_times_paths, ["trip_id", "stop_sequence"])

enriched_df = df.withColumn(
    "start_date", F.to_date(F.col("start_date"), "yyyyMMdd")
)

enriched_df = enriched_df.withColumnRenamed("arrival_time", "scheduled_stop_time")

enriched_df = enriched_df.withColumn(
    "hour", F.split("scheduled_stop_time", ":").getItem(0).cast("int")
).withColumn(
    "minute_second",
    F.concat(F.lit(":"), F.split("scheduled_stop_time", ":").getItem(1),
           F.lit(":"), F.split("scheduled_stop_time", ":").getItem(2))
).withColumn(
    "normalized_date", F.when(F.col("hour") >= 24, F.date_add(F.col("start_date"), 1)).otherwise(F.col("start_date"))
).withColumn(
    "normalized_hour", F.when(F.col("hour") >= 24, F.col("hour") - 24).otherwise(F.col("hour"))
).withColumn(
    "datetime_str",
    F.concat_ws(" ", F.col("normalized_date"), F.concat(F.lpad(F.col("normalized_hour").cast("string"), 2, "0"), F.col("minute_second")))
).withColumn(
    "scheduled_stop_time",
    F.to_unix_timestamp(F.to_utc_timestamp(F.col("datetime_str"), config.timezone))
).drop("hour", "minute_second", "normalized_date", "normalized_hour", "datetime_str")


enriched_df = enriched_df.filter(
    F.col("effective_stop_time").isNotNull() & F.col("scheduled_stop_time").isNotNull()
)

enriched_df = enriched_df.withColumn(
    "delay_seconds",
    F.col("effective_stop_time") - F.col("scheduled_stop_time")
)

query = (
    enriched_df.writeStream
    .foreachBatch(write_delay_batch)
    .outputMode("update")
    .start()
)

query.awaitTermination()
