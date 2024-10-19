from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    explode,
    when,
    abs,
    current_timestamp,
    to_utc_timestamp,
    timestamp_seconds,
    avg,
    stddev,
    window,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    ArrayType,
)
import os
import psycopg2
import logging


def start_spark_streaming():
    # Configure logging
    logging.basicConfig(
        filename='/Users/nimishamalik/Desktop/Stream-pipeline/logs/spark_processing.log',
        level=logging.INFO,
        format='%(asctime)s %(levelname)s:%(message)s'
    )
    logging.info("Starting Spark Streaming Application")

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("StockDataStreaming") \
        .config("spark.jars", "/Users/nimishamalik/postgresql-42.7.4.jar") \
        .getOrCreate()

    # Define the schema for the incoming Kafka data
    schema = StructType([
        StructField("quoteResponse", StructType([
            StructField("result", ArrayType(StructType([
                StructField("symbol", StringType()),
                StructField("regularMarketPrice", DoubleType()),
                StructField("regularMarketTime", LongType()),
                StructField("exchangeTimezoneName", StringType()),
                StructField("regularMarketChangePercent", DoubleType()),
            ]))),
            StructField("error", StringType()),
        ]))
    ])

    # Read from Kafka topic
    stock_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "stock_data") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    stock_df = stock_stream.selectExpr("CAST(value AS STRING) as value")

    df_parsed = stock_df.select(from_json(col("value"), schema).alias("data"))
    df_exploded = df_parsed.select(explode("data.quoteResponse.result").alias("stock"))

    # Transform the data
    stock_df_transformed = df_exploded.select(
        col("stock.symbol").alias("symbol"),
        col("stock.regularMarketPrice").alias("price"),
        timestamp_seconds(col("stock.regularMarketTime")).alias("timestamp"),
        to_utc_timestamp(timestamp_seconds(col("stock.regularMarketTime")), col("stock.exchangeTimezoneName")).alias("timestamp_utc"),
        col("stock.regularMarketChangePercent").alias("percent_change")
    )
    # Convert timestamp to UTC
    stock_df_transformed = stock_df_transformed.withColumn(
        "timestamp", to_utc_timestamp(col("timestamp"), "UTC")
    )

    # Add watermark to stock_df_transformed
    stock_df_with_watermark = stock_df_transformed.withWatermark("timestamp_utc", "1 minute")

    # Define windowed aggregations
    window_duration = "5 minutes"
    slide_duration = "1 minute"

    windowed_df = stock_df_with_watermark \
        .groupBy(
            window(col("timestamp_utc"), window_duration, slide_duration),
            col("symbol")
        ) \
        .agg(
            avg("price").alias("avg_price"),
            stddev("price").alias("stddev_price")
        )

    # Prepare windowed_df for joining
    windowed_df = windowed_df.select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_price"),
        col("stddev_price")
    )


    # Join the windowed stats back to the original data using the timestamp and symbol
    joined_df = stock_df_with_watermark.alias("sdf").join(
        windowed_df.alias("wdf"),
        on=[
            col("sdf.symbol") == col("wdf.symbol"),
            col("sdf.timestamp_utc") >= col("wdf.window_start"),
            col("sdf.timestamp_utc") < col("wdf.window_end")
        ],
        how="inner"
    )

    # Calculate Z-Score and flag anomalies
    anomaly_threshold = 3  # You can adjust this threshold
    final_df = joined_df.withColumn(
        "z_score",
        (col("sdf.price") - col("wdf.avg_price")) / col("wdf.stddev_price")
    ).withColumn(
        "is_anomaly",
        when(
            (abs(col("z_score")) >= anomaly_threshold) & col("z_score").isNotNull(),
            True
        ).otherwise(False)
    ).select(
        col("sdf.symbol").alias("symbol"),
        col("sdf.price").alias("price"),
        col("sdf.timestamp").alias("timestamp"),
        col("sdf.timestamp_utc").alias("timestamp_utc"),
        col("sdf.percent_change").alias("percent_change"),
        col("is_anomaly")
    )

    # processing_time_df = stock_df_transformed.withColumn("processing_time", current_timestamp())

    # query_debug = stock_df_transformed.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .option("truncate", "false") \
    #     .start()

    # Write to console for debugging
    # query_debug = final_df.writeStream \
    #     .foreachBatch(logging_output) \
    #     .outputMode("append") \
    #     .start()
    
    # query_debug.awaitTermination()

    # Write the final DataFrame to PostgreSQL
    query = final_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", "/Users/nimishamalik/Desktop/Stream-pipeline/checkpoints/spark_job") \
        .start()

    query.awaitTermination()

def logging_output(df, epoch_id):
    rows = df.collect()
    logging.info(f"Batch {epoch_id}: {len(rows)} rows")
    
    for row in rows:
        logging.info(
            f"Symbol: {row['symbol']}, Price: {row['price']}, Timestamp: {row['timestamp']}, "
            f"Timestamp UTC: {row['timestamp_utc']}, Percent Change: {row['percent_change']}, "
            f"Is Anomaly: {row['is_anomaly']}"
        )

def write_to_postgres(df, epoch_id):
    if df.count() > 0:
        # Deduplicate data based on 'symbol' and 'timestamp'
        df_dedup = df.dropDuplicates(['symbol', 'timestamp'])
        
        def upsert_partition(partition):
            import psycopg2
            conn = psycopg2.connect(database="mydatabase", user="postgres", password="1234", host="localhost", port="5432")
            cursor = conn.cursor()
            query = """
            INSERT INTO stock_data (symbol, price, timestamp, timestamp_utc, percent_change, is_anomaly)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp) DO UPDATE SET
                price = EXCLUDED.price,
                timestamp_utc = EXCLUDED.timestamp_utc,
                percent_change = EXCLUDED.percent_change,
                is_anomaly = EXCLUDED.is_anomaly;
            """
            batch = []
            for row in partition:
                batch.append((
                    row['symbol'],
                    row['price'],
                    row['timestamp'],
                    row['timestamp_utc'],
                    row['percent_change'],
                    row['is_anomaly']
                ))
            if batch:
                cursor.executemany(query, batch)
                conn.commit()
            cursor.close()
            conn.close()
        
        # Apply upsert function to each partition
        df_dedup.foreachPartition(upsert_partition)
        
        logging.info(f"Batch {epoch_id} written to PostgreSQL.")
    else:
        logging.info(f"No data to write for batch {epoch_id}")


def send_alerts(anomalies_df):
    # Configure the anomaly logger
    anomaly_logger = logging.getLogger('anomaly_logger')
    if not anomaly_logger.handlers:
        anomaly_handler = logging.FileHandler('/Users/nimishamalik/Desktop/Stream-pipeline/logs/anomalies.log')
        anomaly_handler.setLevel(logging.INFO)
        anomaly_formatter = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')
        anomaly_handler.setFormatter(anomaly_formatter)
        anomaly_logger.addHandler(anomaly_handler)
        anomaly_logger.propagate = False  # Prevents duplicating logs

    anomalies = anomalies_df.collect()
    for anomaly in anomalies:
        alert_message = f"Anomaly detected: {anomaly.symbol} at {anomaly.timestamp} with price {anomaly.price}"
        anomaly_logger.info(alert_message)
        # Here you can add code to send email alerts or notifications

if __name__ == "__main__":
    start_spark_streaming()
