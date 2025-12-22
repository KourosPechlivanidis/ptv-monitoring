from enum import Enum
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
import redis

REDIS_HOST = "redis"
REDIS_PORT = "6379"

class ReadMode(str, Enum):
    BATCH = "batch"
    STREAM = "stream"


def read_from_kafka(
    spark: SparkSession,
    topic: str,
    bootstrap_servers: str,
    mode: ReadMode = ReadMode.STREAM,
) -> DataFrame:
    reader = spark.read if mode is ReadMode.BATCH else spark.readStream

    return (
        reader.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option(
            "startingOffsets",
            "earliest" if mode is ReadMode.BATCH else "latest",
        )
        .load()
    )


def write_to_redis(
            rows,
            key_prefix,
            ttl_seconds,
            field_mapper
):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

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
    return (
        df.select(F.col("value").cast("string"))
          .select(F.from_json(F.col("value"), schema).alias("value"))
    )

def load_schema(path: str) -> StructType:
    with open(path, "r") as f:
        return StructType.fromJson(json.load(f))
    

def join_gtfs_with_schedule(spark, df: DataFrame, static_paths, join_keys, select_cols=None):
       
    static_dfs = []
    for p in static_paths:
        sdf = spark.read.parquet(p)
        if select_cols:
            sdf = sdf.select(*select_cols)
        static_dfs.append(sdf)

    
    static_df = static_dfs[0]
    for sdf in static_dfs[1:]:
        static_df = static_df.unionByName(sdf, allowMissingColumns=True)

    rt = df.alias("rt")
    st = static_df.alias("st")

    join_condition = None
    for k in join_keys:
        cond = F.col(f"rt.{k}") == F.col(f"st.{k}")
        join_condition = cond if join_condition is None else join_condition & cond

    joined = rt.join(st, join_condition, how="left")
    rt_cols = [F.col(f"rt.{c}").alias(c) for c in rt.columns]

    st_cols = [
        F.col(f"st.{c}")
        for c in st.columns
        if c not in join_keys
    ]

    return joined.select(*rt_cols, *st_cols)