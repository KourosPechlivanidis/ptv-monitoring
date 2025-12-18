from enum import Enum
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F

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