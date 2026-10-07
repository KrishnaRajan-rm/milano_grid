import os 
import json
os.environ["HADOOP_HOME"] = r"C:\GraddedAssignment\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\GraddedAssignment\hadoop\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,desc
from sp2 import run

spark=(SparkSession.builder.appName("Telecom sp6").master("local[2]").config("spark.driver.memory","4g").getOrCreate())

clean_parquet_path = "../dataset/output/clean_activity"
hourly_parquet_path = "../dataset/output/hourly_grid_summary"
dashboard_csv_path = "../dataset/output/dashboard_summary"
hourly_csv_path = "hourly_grid_summary.csv"
geojson_path = "../dataset/reference/milano-grid.geojson"

clean_df = run()

clean_df.write.mode("overwrite").partitionBy("date").parquet(clean_parquet_path)
print("Clean activity data written as partitioned Parquet.")

hourly_grid_summary = (spark.read.option("header",True).option('inferSchema',True).csv(hourly_csv_path))

hourly_grid_summary.show(5)

hourly_grid_summary.write.mode("overwrite").parquet(hourly_parquet_path)
print("Hourly grid summary written as Parquet.")

dashboard_summary = (hourly_grid_summary.groupby("timestamp","grid_id").agg(sum("total_activity").alias("total_activity")).orderBy(desc("total_activity")).limit(10))

dashboard_summary.write.mode("overwrite").option("header",True).csv(dashboard_csv_path)

read_clean_df = (spark.read.parquet(clean_parquet_path))

read_hourly_df = (spark.read.parquet(hourly_parquet_path))

print("\nCLEAN PARQUET SCHEMA:")
read_clean_df.printSchema()

print("CLEAN PARQUET COUNT:")
clean_count = read_clean_df.count()
print(clean_count)


print("\nHOURLY PARQUET SCHEMA:")
read_hourly_df.printSchema()

print("HOURLY PARQUET COUNT:")
hourly_count = read_hourly_df.count()
print(hourly_count)

original_clean_count = clean_df.count()

original_hourly_count = hourly_grid_summary.count()


print("\nCOUNT VALIDATION:")

print(
    "Clean count matches:",
    original_clean_count == clean_count
)

print(
    "Hourly count matches:",
    original_hourly_count == hourly_count
)

def get_directory_size(path):

    total_size = 0

    for root, directories, files in os.walk(path):

        for file in files:

            file_path = os.path.join(root, file)

            total_size += os.path.getsize(file_path)

    return total_size


clean_size = get_directory_size(clean_parquet_path)

hourly_size = get_directory_size(hourly_parquet_path)


print("\nFILE SIZE COMPARISON:")

print(
    f"Clean Parquet size: "
    f"{clean_size / (1024 * 1024):.2f} MB"
)

print(
    f"Hourly Parquet size: "
    f"{hourly_size / (1024 * 1024):.2f} MB"
)

spark.stop()
