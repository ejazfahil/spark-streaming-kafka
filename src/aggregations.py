"""Windowed aggregations 2022-12-16"""
from pyspark.sql.functions import window, sum as _sum, count, avg

def hourly_totals(parsed_df):
    return parsed_df.withWatermark("ts","10 minutes") \
        .groupBy(window("ts","1 hour","30 minutes"),"user_id") \
        .agg(_sum("amount").alias("total_amount"),
             count("*").alias("n_events"),
             avg("amount").alias("avg_amount"))

def write_to_postgres(agg_df, checkpoint_dir):
    return agg_df.writeStream.outputMode("update") \
        .foreachBatch(lambda df,_: df.write.jdbc(
            "jdbc:postgresql://localhost/warehouse","hourly_totals","append",
            {"user":"analyst","password":"secret","driver":"org.postgresql.Driver"})) \
        .option("checkpointLocation", checkpoint_dir).start()
