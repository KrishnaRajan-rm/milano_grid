import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (col,avg,sum,max,stddev,lag,row_number,when)

os.environ["HADOOP_HOME"] = r"C:\GraddedAssignment\hadoop"
os.environ["PATH"] += r";C:\GraddedAssignment\hadoop\bin"

spark = (
    SparkSession.builder
    .appName("Telecom ML2 Feature Engineering")
    .master("local[*]")
    .getOrCreate()
)

INPUT_PATH = "../dataset/output/hourly_grid_summary"
OUTPUT_PATH = "../dataset/output/ml2_features"

df = spark.read.parquet(INPUT_PATH)

print("Input data loaded")

df = df.select(
    "grid_id",
    "timestamp",
    "internet",
    "total_activity"
)

df = df.withColumn(
    "timestamp",
    col("timestamp").cast("timestamp")
)

df = df.orderBy(
    "grid_id",
    "timestamp"
)

window_order = (
    Window
    .partitionBy("grid_id")
    .orderBy("timestamp")
)

df = df.withColumn(
    "row_number",
    row_number().over(window_order)
)

trailing_window = (
    Window
    .partitionBy("grid_id")
    .orderBy("timestamp")
    .rowsBetween(-23, 0)
)

df = df.withColumn(
    "avg_activity",
    avg("total_activity").over(trailing_window)
)

df = df.withColumn(
    "active_hours",
    sum(
        when(
            col("total_activity") > 0,
            1
        ).otherwise(0)
    ).over(trailing_window)
)

df = df.withColumn(
    "peak_activity",
    max("total_activity").over(trailing_window)
)

df = df.withColumn(
    "peak_ratio",
    when(
        col("avg_activity") > 0,
        col("peak_activity") / col("avg_activity")
    ).otherwise(0)
)

df = df.withColumn(
    "variability",
    stddev("total_activity").over(trailing_window)
)

df = df.withColumn(
    "recent_activity",
    sum("total_activity").over(trailing_window)
)

df = df.withColumn(
    "previous_activity",
    lag("recent_activity", 24).over(window_order)
)

df = df.withColumn(
    "activity_growth",
    when(
        col("previous_activity") > 0,
        (
            col("recent_activity")
            - col("previous_activity")
        ) / col("previous_activity")
    ).otherwise(0)
)

internet_total = (
    sum("internet")
    .over(trailing_window)
)

activity_total = (
    sum("total_activity")
    .over(trailing_window)
)


df = df.withColumn(
    "internet_share",
    when(
        activity_total > 0,
        internet_total / activity_total
    ).otherwise(0)
)

df = df.withColumn(
    "feature_timestamp",
    col("timestamp")
)

features = df.select(
    "grid_id",
    "feature_timestamp",
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
    "row_number"
)

features = features.filter(
    col("row_number") >= 48
)

features = features.drop(
    "row_number"
)

(
    features
    .write
    .mode("overwrite")
    .option("header", True)
    .parquet(OUTPUT_PATH)
)

print("ML2 feature generation completed")

print("Feature columns:")

features.printSchema()

print("Sample records:")

features.show(10, truncate=False)

spark.stop()