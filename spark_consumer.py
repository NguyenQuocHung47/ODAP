from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, FloatType
from pyspark.sql.functions import (
    from_json, col, udf, regexp_replace, to_date, concat_ws, date_format, count, sum as _sum
)
import time
import requests

schema = StructType() \
    .add("User", StringType()) \
    .add("Card", StringType()) \
    .add("Year", StringType()) \
    .add("Month", StringType()) \
    .add("Day", StringType()) \
    .add("Time", StringType()) \
    .add("Amount", StringType()) \
    .add("Use Chip", StringType()) \
    .add("Merchant Name", StringType()) \
    .add("Merchant City", StringType()) \
    .add("Merchant State", StringType()) \
    .add("Zip", StringType()) \
    .add("MCC", StringType()) \
    .add("Errors?", StringType()) \
    .add("Is Fraud?", StringType())

spark = SparkSession.builder \
    .appName("TransactionConsumer") \
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://locahost:9000") \
    .getOrCreate()

def convert_amount(amount_str):
    if amount_str is not None:
        return float(amount_str.replace('$', '').replace(',', '').strip())
    return None

convert_amount_udf = udf(convert_amount, FloatType())

def fetch_exchange_rate(): 
    url = "https://api.exchangerate-api.com/v4/latest/USD"
    response = requests.get(url)
    try:
        data = response.json()
        usd_to_vnd_rate = data['rates']['VND']
        return usd_to_vnd_rate
    except Exception as e:
        print(f"Error: {e}")

def process_with_spark():
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions") \
        .load()
    
    exchange_rate = fetch_exchange_rate()

    value_df = kafka_df.selectExpr("CAST(value AS STRING)")
    parsed_df = value_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
    #parsed_df = parsed_df.withColumn("Amount", convert_amount_udf(col("Amount")))

    if exchange_rate is not None:
        processed_df = parsed_df.filter("`Is Fraud?` = 'No' AND `Use Chip` != 'Online Transaction' AND `Errors?` = ''") \
            .withColumn("Amount", regexp_replace("Amount", "\\$", "").cast(FloatType())) \
            .filter(col("Amount").isNotNull() & (col("Amount") > 0)) \
            .withColumn("Amount_VND", (col("Amount") * exchange_rate).cast(FloatType()))

    processed_df = processed_df.withColumn("Transaction_Date", concat_ws("/", col("Day"), col("Month"), col("Year"))) \
        .withColumn("Transaction_Time", date_format("Time", "HH:mm:ss"))

    selected_columns = ["Card", "Transaction_Date", "Transaction_Time", "Merchant Name", "Merchant City", "Amount_VND"]
    clean_df_selected = processed_df.select(selected_columns)

    coalesced_transactions = clean_df_selected.coalesce(1)

    query = coalesced_transactions \
        .writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("path", "hdfs://localhost:9000/user/transactions") \
        .option("checkpointLocation", "hdfs://localhost:9000/user/transactions/checkpoints") \
        .option("header", "true") \
        .start()

    timeout = 10  
    no_data_count = 0

    try:
        while query.isActive:
            time.sleep(3) 
            if query.lastProgress and not query.lastProgress["numInputRows"]:
                no_data_count += 1
            else:
                no_data_count = 0

            if no_data_count * 5 >= timeout:
                print("No new messages received from Kafka. Stopping Spark Streaming...")
                query.stop()
                break
    finally:
        spark.stop()

process_with_spark()

