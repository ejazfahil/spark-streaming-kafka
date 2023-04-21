"""DLQ handler 2023-04-21"""
from pyspark.sql.functions import col, current_timestamp

def route_to_dlq(df, error_col="parse_error"):
    """Send malformed records to dead-letter topic."""
    bad = df.filter(col(error_col).isNotNull()) \
           .withColumn("dlq_ts", current_timestamp())
    return bad.selectExpr(
        "CAST(event_id AS STRING) AS key",
        "to_json(struct(*)) AS value"
    ).writeStream.format("kafka") \
     .option("kafka.bootstrap.servers","localhost:9092") \
     .option("topic","payments_dlq").start()
