from pyspark.sql import SparkSession

def process_data(input_path, output_path):
    spark = SparkSession.builder \
        .appName("HelsinkiBikesMetrics") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
        .config("spark.hadoop.fs.s3a.access.key", "test") \
        .config("spark.hadoop.fs.s3a.secret.key", "test") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    dep_counts = df.groupBy("Departure station name").count().withColumnRenamed("count", "departure_count")

    ret_counts = df.groupBy("Return station name").count().withColumnRenamed("count", "return_count")

    metrics = dep_counts.join(
        ret_counts, 
        dep_counts["Departure station name"] == ret_counts["Return station name"], 
        "full_outer"
    )

    metrics.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    
    process_data(args.input, args.output)