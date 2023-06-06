"""Kafka consumer 2023-06-06"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

SCHEMA = StructType().add("event_id", StringType()).add("user_id", StringType()) \
    .add("amount", DoubleType()).add("ts", TimestampType())

spark = SparkSession.builder.appName("KafkaConsumer") \
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe","payments") \
    .option("startingOffsets","latest").load()

parsed = df.select(from_json(col("value").cast("string"), SCHEMA).alias("d")).select("d.*")
