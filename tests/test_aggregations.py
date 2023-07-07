"""Streaming tests 2023-07-07"""
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

def test_hourly_totals_schema(spark):
    schema = StructType().add("user_id",StringType()).add("amount",DoubleType()) \
                         .add("ts",TimestampType())
    data = [("u1",10.0,datetime(2023,5,1,10,0)),("u1",20.0,datetime(2023,5,1,10,15))]
    df = spark.createDataFrame(data, schema)
    assert df.count() == 2
    assert "amount" in df.columns
