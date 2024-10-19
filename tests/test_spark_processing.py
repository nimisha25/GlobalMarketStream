import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from scripts.spark_processing import write_to_postgres

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("TestSparkApp") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

@patch('pyspark.sql.DataFrame.foreachPartition', autospec=True)
def test_write_to_postgres(mock_foreachPartition, spark):
    # Create a sample DataFrame
    data = [
        ("^GSPC", 4000.0, "2023-01-01 12:00:00", "2023-01-01 17:00:00", 0.5, False)
    ]
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("timestamp_utc", StringType(), True),
        StructField("percent_change", DoubleType(), True),
        StructField("is_anomaly", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)

    # Call the function
    write_to_postgres(df, epoch_id=1)

    # Assert that the foreachPartition function was called
    assert mock_foreachPartition.called, "The foreachPartition function was not called."

