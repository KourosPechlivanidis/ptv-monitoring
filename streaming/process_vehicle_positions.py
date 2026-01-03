from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions  as F

from utils import (read_from_kafka, 
                   load_schema, 
                   parse_kafka_value, 
                   write_to_redis, 
                   join_gtfs_with_schedule
)
from config import VehicleConfig

def get_write_vehicle_batch(config: VehicleConfig):
    
    def write_vehicle_batch(df: DataFrame, batch_id: int) -> None:

        """
    
        Function that maps and writes fields to redis
        
        :param df: microbatch as a spark df that is written to redis
        :type batch_id: DataFrame
        
        """
            
        print(f"Writing vehicle batch {batch_id} to Redis")

        df.foreachPartition(
            lambda rows: write_to_redis(
                rows,
                key_prefix="vehicle",
                ttl_seconds=120,
                redis_host=config.redis_host,
                redis_port=config.redis_port,
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

    return write_vehicle_batch


config = VehicleConfig()

spark = (
    SparkSession.builder
    .appName("VehiclePositionsKafkaConsumer")
    # Delta lake extensions
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # S3 extensions
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key", config.aws_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", config.aws_secret_key)
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.path.style.access", "true") 
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    .getOrCreate()
)

raw_df = read_from_kafka(
    spark=spark,
    topic=config.topic,
    bootstrap_servers=config.kafka_bootstrap_servers,
)

schema = load_schema(config.schema_path)
parsed_df = parse_kafka_value(raw_df, schema)

df = parsed_df.select(
    F.col("value.mode"),
    F.col("value.header.gtfs_realtime_version").alias("gtfs_realtime_version"),
    F.col("value.header.timestamp").cast("long").alias("source_timestamp"),
    F.col("value.entity_id"),
    F.col("value.vehicle.trip.trip_id").alias("trip_id"),
    F.col("value.vehicle.trip.route_id").alias("route_id"),
    F.col("value.vehicle.trip.start_time").alias("start_time"),
    F.col("value.vehicle.trip.start_date").alias("start_date"),
    F.col("value.vehicle.vehicle.vehicle_id").alias("vehicle_id"),
    F.col("value.vehicle.vehicle.label").alias("label"),
    F.col("value.vehicle.position.latitude").cast("double").alias("latitude"),
    F.col("value.vehicle.position.longitude").cast("double").alias("longitude"),
    F.col("value.vehicle.timestamp").cast("long").alias("position_timestamp"),
)

df = df.withColumn(
    "event_ts",
    F.from_unixtime(F.col("source_timestamp")).cast("timestamp")
)

df = (
    df
    .withColumn("year",  F.year("event_ts"))
    .withColumn("month", F.month("event_ts"))
    .withColumn("day",   F.dayofmonth("event_ts"))
    .withColumn("hour",  F.hour("event_ts"))
)

df_enriched = join_gtfs_with_schedule(spark, df, config.route_paths, ["route_id"], ["route_id","route_short_name", "route_long_name"])
df_enriched = join_gtfs_with_schedule(spark, df_enriched, config.trip_paths, ["trip_id"], ["trip_id", "trip_headsign"])

df_enriched = df_enriched.withColumn(
    "resolved_route_name",
    F.coalesce(
        F.col("route_short_name"),
        F.col("route_long_name"),
        F.col("route_id"),
        F.lit("UNKNOWN_ROUTE")
    )
).withColumn(
    "resolved_headsign",
    F.coalesce(
        F.col("trip_headsign"),
        F.lit("UNKNOWN_DESTINATION")
    )
)



redis_query = (
    df_enriched.writeStream
    .queryName("RedisSink")
    .foreachBatch(get_write_vehicle_batch(config))
    .start()
)

delta_query = (
    df.writeStream
    .queryName("S3Sink")
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", config.s3_checkpoint_path)
    .partitionBy("year", "month", "day", "hour")
    .trigger(processingTime='1 minute') 
    .start(config.s3_delta_path)
)

spark.streams.awaitAnyTermination()
