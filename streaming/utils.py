from enum import Enum
from typing import List
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
import redis


_redis_pools: dict = {}


def _get_redis_pool(host: str, port: int) -> redis.ConnectionPool:
    key = (host, port)
    if key not in _redis_pools:
        _redis_pools[key] = redis.ConnectionPool(host=host, port=port, db=0)
    return _redis_pools[key]



class ReadMode(str, Enum):
    BATCH = "batch"
    STREAM = "stream"


def read_from_kafka(
    spark: SparkSession,
    topic: str,
    bootstrap_servers: str,
    mode: ReadMode = ReadMode.STREAM,
) -> DataFrame:
    
    """
    
    Read from kafka topic with options to read as batch for development purposes
    
    :param spark: Sparksession to be used for reading from kafka
    :param topic: Topic to read from
    :param bootstrap_servers: Kafka host to read from
    :param mode: Read as either batch or stream (default)
   
    :return df: Return read data as a spark dataframe
    
    """
    reader = spark.read if mode is ReadMode.BATCH else spark.readStream

    return (
        reader.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option(
            "startingOffsets",
            "earliest" if mode is ReadMode.BATCH else "latest",
        )
        .option("failOnDataLoss", "false") # Dont throw an error when checkpoint does not match latest offset, as this can happen because the program may be resumed anytime
        .load()
    )


def write_to_redis(
            rows,
            key_prefix: str,
            ttl_seconds: int,
            redis_host: str,
            redis_port: int,
            field_mapper
):
    
    """
   
    Generic function that writes a set of list of rows to redis
   
    
    :param rows: Description
    :param key_prefix: prefix to attach to redis, used to identify the source of the record in Redis
    :param ttl_seconds: ttl to apply to the record in redis
    :param field_mapper: determines which fields to put into reddis and how they are mapped
    
    """
    pool = _get_redis_pool(redis_host, redis_port)
    r = redis.Redis(connection_pool=pool)

    for row in rows:
        trip_id = getattr(row, "trip_id", None)
        if trip_id is None:
            continue

        key = f"{key_prefix}:trip:{trip_id}"
        mapping = field_mapper(row)

        if not mapping:
            continue

        r.hset(key, mapping=mapping)
        r.expire(key, ttl_seconds)


def parse_kafka_value(
    df: DataFrame,
    schema: StructType,
) -> DataFrame:
    
    """
    
    Parse a raw df read from kafka (only containing the value column),
    to a schema
    
    :param df: raw df as read in kafka
    :param schema: schema to cast the kafka value to
    :return df: dataframe with raw kafka records casted to a schema

    """
    return (
        df.select(F.col("value").cast("string"))
          .select(F.from_json(F.col("value"), schema).alias("value"))
    )

def load_schema(path: str) -> StructType:
    
    """
    
    Simple function to load a schema from a path
    
    :param path: path to read the schema from
    :return schema: Spark interpretable schema 
    
    """

    with open(path, "r") as f:
        return StructType.fromJson(json.load(f))
    

def join_gtfs_with_schedule(
    spark: SparkSession,
    df: DataFrame,
    static_path: str,
    join_keys: List[str],
    select_cols=None
):
    # 1. Load static data and filter to the latest schedule partition.
    #    The static uploader appends a full copy of the schedule on each run,
    #    partitioned by valid_from. Without this filter, every historical upload
    #    would be included in the join, producing duplicate rows per entity.
    static_df = spark.read.format("delta").load(static_path)
    max_valid_from = static_df.agg(F.max("valid_from")).collect()[0][0]
    static_df = static_df.filter(F.col("valid_from") == max_valid_from).cache()

    # 2. Alias both DataFrames to resolve ambiguity
    main_alias = "main"
    static_alias = "static"
    
    # 3. Join using the aliased DataFrames
    joined = df.alias(main_alias).join(
        static_df.alias(static_alias), 
        on=join_keys, 
        how="left"
    )

    if select_cols:
        # We explicitly pull existing columns from the 'main' alias 
        # to tell Spark exactly which 'stop_id' we want.
        existing_cols = [f"{main_alias}.{c}" for c in df.columns]
        
        # Pull new columns specifically from the 'static' alias
        new_cols = [f"{static_alias}.{c}" for c in select_cols if c not in df.columns]
        
        return joined.select(*existing_cols, *new_cols)

    return joined