# import os
# import shutil
# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName("MergeCSVFiles") \
#     .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
#     .getOrCreate()

# input_path = "hdfs://localhost:9000/user/transactions/"
# temp_output_path = "hdfs://localhost:9000/user/transactions/temp_merged_transactions"
# final_output_file = "hdfs://localhost:9000/user/transactions/merged_transactions.csv"

# df = spark.read \
#     .format("csv") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .load(input_path)

# df.coalesce(1).write \
#     .format("csv") \
#     .option("header", "true") \
#     .mode("overwrite") \
#     .save(temp_output_path)


# local_temp_path = "/tmp/spark_temp_output"
# os.makedirs(local_temp_path, exist_ok=True)

# os.system(f"hdfs dfs -get {temp_output_path}/*.csv {local_temp_path}")

# csv_file = next(f for f in os.listdir(local_temp_path) if f.endswith(".csv"))
# local_merged_file = os.path.join(local_temp_path, "merged_transactions.csv")
# shutil.move(os.path.join(local_temp_path, csv_file), local_merged_file)

# os.system(f"hdfs dfs -put -f {local_merged_file} {final_output_file}")

# shutil.rmtree(local_temp_path)
# os.system(f"hdfs dfs -rm -r {temp_output_path}")

# print(f"All CSV files have been merged into: {final_output_file}")

from pyspark.sql import SparkSession

HDFS_INPUT_PATH = "hdfs://localhost:9000/user/transactions/"
HDFS_TEMP_OUTPUT_PATH = "hdfs://localhost:9000/user/transactions/temp_merged_transactions"

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("MergeCSVFiles") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()

    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(HDFS_INPUT_PATH)

    df.coalesce(1).write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(HDFS_TEMP_OUTPUT_PATH)

    spark.stop()

