import os
import time
import json

os.environ["HADOOP_HOME"] = r"C:\GraddedAssignment\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\GraddedAssignment\hadoop\bin"

from sp2 import run
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder
    .appName("Telecom SP5")
    .master("local[*]")
    .getOrCreate()
)

clean_df = run()

hotspot_df = (
    clean_df
    .groupBy("grid_id")
    .agg(
        sum("sms_in").alias("total_sms_in"),
        sum("sms_out").alias("total_sms_out"),
        sum("call_in").alias("total_call_in"),
        sum("call_out").alias("total_call_out"),
        sum("internet").alias("total_internet")
    )
    .withColumn(
        "total_activity",
        col("total_sms_in")
        + col("total_sms_out")
        + col("total_call_in")
        + col("total_call_out")
        + col("total_internet")
    )
    .orderBy(desc("total_activity"))
    .limit(10)
)

print("=" * 69)
print("ACTIVITY 1 — HOTSPOT PHYSICAL PLAN")
print("=" * 69)

hotspot_df.explain(mode="formatted")

print("=" * 69)
print("ACTIVITY 2 — CACHE PERFORMANCE")
print("=" * 69)

start = time.perf_counter()

clean_df.count()

first_time_count = time.perf_counter() - start


cache_df = clean_df.select(
    "grid_id",
    "date",
    "total_activity"
).cache()

start = time.perf_counter()

cache_df.count()

first_cache_time = time.perf_counter() - start


start = time.perf_counter()

cache_df.count()

second_cache_time = time.perf_counter() - start


print(f"Before cache: {first_time_count:.4f} seconds")
print(f"First cached action: {first_cache_time:.4f} seconds")
print(f"Second cached action: {second_cache_time:.4f} seconds")

print("=" * 69)
print("ACTIVITY 3 — REPARTITIONING")
print("=" * 69)

before_partitions = clean_df.rdd.getNumPartitions()

print(
    "Number of partitions before:",
    before_partitions
)


repartitioned_df = clean_df.repartition("date")

after_partitions = repartitioned_df.rdd.getNumPartitions()

print("Number of partitions after:",after_partitions)

print("=" * 69)
print("ACTIVITY 4 — COLUMN PRUNING")
print("=" * 69)

required_columns = [
    "grid_id",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet"
]

pruned_df = clean_df.select(*required_columns)

pruned_df = (
    pruned_df
    .groupBy("grid_id")
    .agg(
        sum("sms_in").alias("total_sms_in"),
        sum("sms_out").alias("total_sms_out"),
        sum("call_in").alias("total_call_in"),
        sum("call_out").alias("total_call_out"),
        sum("internet").alias("total_internet")
    )
)

print("Physical plan after column pruning:")

pruned_df.explain(mode="formatted")

print("=" * 69)
print("ACTIVITY 5 — BROADCAST JOIN")
print("=" * 69)

path = "../dataset/reference/milano-grid.geojson"

with open(path, "r", encoding="utf-8") as file:
    geo_json_data = json.load(file)


grid_lookup = []

for feature in geo_json_data["features"]:

    properties = feature["properties"]
    geometry = feature["geometry"]

    grid_lookup.append(
        (
            int(properties["cellId"]),
            json.dumps(geometry)
        )
    )


geo_json_df = spark.createDataFrame(
    grid_lookup,
    ["grid_id", "geometry"]
)

standard_join = clean_df.join(
    geo_json_df,
    on="grid_id",
    how="left"
)

print("STANDARD JOIN PLAN")

standard_join.explain(mode="formatted")

broadcast_join = clean_df.join(
    broadcast(geo_json_df),
    on="grid_id",
    how="left"
)

print("BROADCAST JOIN PLAN")

broadcast_join.explain(mode="formatted")

print("=" * 69)
print("ACTIVITY 6 — OVER-PARTITIONING")
print("=" * 69)

current_partitions = clean_df.rdd.getNumPartitions()

print(
    "Current partitions:",
    current_partitions
)

# over_partitioned_df = clean_df.repartition(100)
# over_partition_count = (
#     over_partitioned_df.rdd.getNumPartitions()
# )
# print(
#     "Over-partitioned partitions:",
#     over_partition_count
# )
print("=" * 69)
print("ACTIVITY 7 — PERFORMANCE OBSERVATIONS")
print("=" * 69)

print(f"""
OBSERVATION 1 — CACHE

Evidence:
Before cache       : {first_time_count:.4f} seconds
First cached action: {first_cache_time:.4f} seconds
Second cached action: {second_cache_time:.4f} seconds

OBSERVATION 2 — REPARTITIONING

Evidence:
Before repartition : {before_partitions} partitions
After repartition  : {after_partitions} partitions
""")

clean_df.unpersist()

spark.stop()

print("SP5 COMPLETED")