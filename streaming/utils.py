from enum import Enum
from typing import List
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
import redis



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
            redis_port: str,
            field_mapper
):
    
    """
   
    Generic function that writes a set of list of rows to redis
   
    
    :param rows: Description
    :param key_prefix: prefix to attach to redis, used to identify the source of the record in Redis
    :param ttl_seconds: ttl to apply to the record in redis
    :param field_mapper: determines which fields to put into reddis and how they are mapped
    
    """
    r = redis.Redis(host=redis_host, port=redis_port, db=0)

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
        static_paths: List[str], 
        join_keys: List[str], 
        select_cols=None):
    
    """
    
    Function that joins the GTFS realtime feed with the schedule
    
    :param spark: sparksession used to perform computations
    :param df: Dataframe containing GTFS realtime records
    :param static_paths: Source for static data
    :param join_keys: Keys used to join GTFS realtime with static data
    :param select_cols: Optional to only select a subset of columns to optimise performance
    """
       
    
    # Read in static dfs
    static_dfs = []
    for p in static_paths:
        sdf = spark.read.parquet(p)
        if select_cols:
            sdf = sdf.select(*select_cols)
        static_dfs.append(sdf)

    
    # Union into one df
    static_df = static_dfs[0]
    for sdf in static_dfs[1:]:
        static_df = static_df.unionByName(sdf, allowMissingColumns=True)


    # Create aliases used for joining and resolving ambigious column references
    rt = df.alias("rt")
    st = static_df.alias("st")

    # Create the condition of join. The assumption is made that join col names are always the same in both sources
    join_condition = None
    for k in join_keys:
        cond = F.col(f"rt.{k}") == F.col(f"st.{k}")
        join_condition = cond if join_condition is None else join_condition & cond

    joined = rt.join(st, join_condition, how="left")

    # To prevent ambigious columns, prioritise keeping columns from realtime source
    rt_cols = [F.col(f"rt.{c}").alias(c) for c in rt.columns]

    st_cols = [
        F.col(f"st.{c}")
        for c in st.columns
        if c not in join_keys
    ]

    return joined.select(*rt_cols, *st_cols)