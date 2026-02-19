from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import sys
import os

def run_spark_job(input_path, output_path):
    print(f"Starting Spark Job...")
    print(f"Reading from: {input_path}")
    print(f"Writing to: {output_path}")

    localstack_endpoint = os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "test")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")

    spark = (SparkSession.builder
        .appName("HelsinkiBikesSparkProcessor")
        .config("spark.jars", "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar")
        .config("spark.hadoop.fs.s3a.endpoint", localstack_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate())

    df = spark.read \
        .option("header", "True") \
        .option("inferSchema", "True") \
        .csv(input_path)

    dep_counts = df.groupBy("departure_name") \
        .agg(count("*").alias("departure_count")) \
        .withColumnRenamed("departure_name", "station_name")

    ret_counts = df.groupBy("return_name") \
        .agg(count("*").alias("return_count")) \
        .withColumnRenamed("return_name", "r_station_name")

    result_df = dep_counts.join(
        ret_counts, 
        dep_counts.station_name == ret_counts.r_station_name, 
        "full_outer"
    ).select(
        col("station_name"), 
        col("departure_count"), 
        col("return_count")
    ).fillna(0)

    result_df.write \
        .mode("overwrite") \
        .json(output_path)

    print("Spark Job Finished Successfully")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark_processor.py <input_s3_path> <output_s3_path>")
        sys.exit(1)
    
    run_spark_job(sys.argv[1], sys.argv[2])
