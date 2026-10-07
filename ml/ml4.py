# ============================================================
# ML4 — ANOMALY DETECTION
# ============================================================

import os

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    hour,
    percentile_approx,
    when,
    abs,
    concat,
    lit,
    round as spark_round
)


# ============================================================
# WINDOWS / HADOOP CONFIGURATION
# ============================================================

os.environ["HADOOP_HOME"] = r"C:\GraddedAssignment\hadoop"
os.environ["PATH"] += r";C:\GraddedAssignment\hadoop\bin"


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Telecom ML4 Anomaly Detection")
    .master("local[*]")
    .getOrCreate()
)


# ============================================================
# PATHS
# ============================================================

INPUT_PATH = "../dataset/output/hourly_grid_summary"

OUTPUT_PATH = "../dataset/output/ml4_anomalies"


# ============================================================
# ACTIVITY 1
# HISTORICAL MEDIAN BASELINE
#
# Baseline = historical median activity
# for each grid and hour-of-day.
# ============================================================

print("\n========================================")
print("ML4 ACTIVITY 1")
print("Historical baseline")
print("========================================")

df = spark.read.parquet(INPUT_PATH)

print("Input data loaded")

df = df.select(
    "grid_id",
    "timestamp",
    "total_activity"
)

df = df.withColumn(
    "timestamp",
    col("timestamp").cast("timestamp")
)

df = df.withColumn(
    "hour_of_day",
    hour("timestamp")
)


# ============================================================
# ACTIVITY 2
# REUSABLE BASELINE FUNCTION
# ============================================================

def calculate_baseline(
    dataframe,
    grouping_columns,
    activity_column="total_activity"
):

    return (
        dataframe
        .groupBy(*grouping_columns)
        .agg(
            percentile_approx(
                activity_column,
                0.5
            ).alias("baseline_activity")
        )
    )


baseline_df = calculate_baseline(
    df,
    ["grid_id", "hour_of_day"]
)

print("Historical baseline created")

baseline_df.orderBy(
    "grid_id",
    "hour_of_day"
).show(20, truncate=False)


# ============================================================
# ACTIVITY 3
# COMPUTE DEVIATION
#
# deviation = actual activity - baseline activity
# ============================================================

print("\n========================================")
print("ML4 ACTIVITY 3")
print("Deviation from baseline")
print("========================================")

anomaly_df = (
    df
    .join(
        baseline_df,
        on=[
            "grid_id",
            "hour_of_day"
        ],
        how="left"
    )
    .withColumn(
        "deviation",
        col("total_activity")
        - col("baseline_activity")
    )
)

anomaly_df.select(
    "grid_id",
    "timestamp",
    "total_activity",
    "baseline_activity",
    "deviation"
).show(
    20,
    truncate=False
)


# ============================================================
# ACTIVITY 4
# ANOMALY SCORE
#
# Percentage deviation:
#
# ((actual - baseline) / baseline) * 100
# ============================================================

print("\n========================================")
print("ML4 ACTIVITY 4")
print("Anomaly score")
print("========================================")

anomaly_df = anomaly_df.withColumn(
    "anomaly_score",
    when(
        col("baseline_activity") > 0,
        (
            (
                col("total_activity")
                - col("baseline_activity")
            )
            / col("baseline_activity")
        ) * 100
    ).otherwise(0.0)
)

anomaly_df.select(
    "grid_id",
    "timestamp",
    "total_activity",
    "baseline_activity",
    spark_round(
        "anomaly_score",
        2
    ).alias("anomaly_score")
).show(
    20,
    truncate=False
)


# ============================================================
# ACTIVITY 5
# HIGH / LOW ANOMALIES
#
# 50% is the chosen methodological threshold.
# ============================================================

print("\n========================================")
print("ML4 ACTIVITY 5")
print("Anomaly flag and direction")
print("========================================")

ANOMALY_THRESHOLD = 50.0

anomaly_df = anomaly_df.withColumn(
    "anomaly_flag",
    when(
        col("anomaly_score") >= ANOMALY_THRESHOLD,
        1
    )
    .when(
        col("anomaly_score") <= -ANOMALY_THRESHOLD,
        1
    )
    .otherwise(0)
)

anomaly_df = anomaly_df.withColumn(
    "direction",
    when(
        col("anomaly_score") >= ANOMALY_THRESHOLD,
        "HIGH"
    )
    .when(
        col("anomaly_score") <= -ANOMALY_THRESHOLD,
        "LOW"
    )
    .otherwise("NORMAL")
)

anomaly_df.select(
    "grid_id",
    "timestamp",
    "total_activity",
    "baseline_activity",
    "anomaly_score",
    "anomaly_flag",
    "direction"
).show(
    30,
    truncate=False
)

print("\n========================================")
print("ML4 ACTIVITY 6")
print("Compare ML3 and NP3")
print("========================================")


# ------------------------------------------------------------
# Start with anomaly data
# ------------------------------------------------------------

comparison_df = anomaly_df


# ------------------------------------------------------------
# ML3 comparison
#
# Expected ML3 columns:
# grid_id
# timestamp
# prediction
# ------------------------------------------------------------

ML3_PATH = "../dataset/output/ml3_predictions"

if os.path.exists(ML3_PATH):

    print("ML3 predictions found")

    ml3_df = spark.read.parquet(ML3_PATH)

    ml3_columns = ml3_df.columns

    print("ML3 columns:")
    print(ml3_columns)

    if (
        "grid_id" in ml3_columns
        and "timestamp" in ml3_columns
        and "prediction" in ml3_columns
    ):

        comparison_df = (
            comparison_df
            .join(
                ml3_df.select(
                    "grid_id",
                    "timestamp",
                    "prediction"
                ),
                on=[
                    "grid_id",
                    "timestamp"
                ],
                how="left"
            )
        )

        comparison_df = comparison_df.withColumn(
            "ml3_anomaly_disagreement",
            when(
                col("anomaly_flag")
                != col("prediction"),
                1
            ).otherwise(0)
        )

    else:

        print(
            "ML3 file exists, but required columns "
            "were not found."
        )

        comparison_df = comparison_df.withColumn(
            "prediction",
            lit(None).cast("double")
        )

        comparison_df = comparison_df.withColumn(
            "ml3_anomaly_disagreement",
            lit(None).cast("integer")
        )

else:

    print(
        "ML3 prediction file not found."
    )

    comparison_df = comparison_df.withColumn(
        "prediction",
        lit(None).cast("double")
    )

    comparison_df = comparison_df.withColumn(
        "ml3_anomaly_disagreement",
        lit(None).cast("integer")
    )


# ------------------------------------------------------------
# NP3 comparison
#
# Expected NP3 columns:
# grid_id
# timestamp
# alert_flag
# ------------------------------------------------------------

NP3_PATH = "../dataset/output/np3_alerts"

if os.path.exists(NP3_PATH):

    print("NP3 alerts found")

    np3_df = spark.read.parquet(NP3_PATH)

    np3_columns = np3_df.columns

    print("NP3 columns:")
    print(np3_columns)

    if (
        "grid_id" in np3_columns
        and "timestamp" in np3_columns
        and "alert_flag" in np3_columns
    ):

        comparison_df = (
            comparison_df
            .join(
                np3_df.select(
                    "grid_id",
                    "timestamp",
                    "alert_flag"
                ),
                on=[
                    "grid_id",
                    "timestamp"
                ],
                how="left"
            )
        )

        comparison_df = comparison_df.withColumn(
            "np3_anomaly_disagreement",
            when(
                col("anomaly_flag")
                != col("alert_flag"),
                1
            ).otherwise(0)
        )

    else:

        print(
            "NP3 file exists, but required columns "
            "were not found."
        )

        comparison_df = comparison_df.withColumn(
            "alert_flag",
            lit(None).cast("integer")
        )

        comparison_df = comparison_df.withColumn(
            "np3_anomaly_disagreement",
            lit(None).cast("integer")
        )

else:

    print(
        "NP3 alert file not found."
    )

    comparison_df = comparison_df.withColumn(
        "alert_flag",
        lit(None).cast("integer")
    )

    comparison_df = comparison_df.withColumn(
        "np3_anomaly_disagreement",
        lit(None).cast("integer")
    )


# ------------------------------------------------------------
# Show comparison
# ------------------------------------------------------------

comparison_df.select(
    "grid_id",
    "timestamp",
    "anomaly_flag",
    "direction",
    "prediction",
    "alert_flag"
).show(
    30,
    truncate=False
)


# ============================================================
# ACTIVITY 7
# HUMAN-READABLE REASON
# ============================================================

print("\n========================================")
print("ML4 ACTIVITY 7")
print("Human-readable reason")
print("========================================")

comparison_df = comparison_df.withColumn(
    "reason",
    when(
        col("direction") == "HIGH",
        concat(
            lit("Activity is "),
            spark_round(
                abs(col("anomaly_score")),
                2
            ),
            lit(
                "% above the historical baseline"
            )
        )
    )
    .when(
        col("direction") == "LOW",
        concat(
            lit("Activity is "),
            spark_round(
                abs(col("anomaly_score")),
                2
            ),
            lit(
                "% below the historical baseline"
            )
        )
    )
    .otherwise(
        lit(
            "Activity is within the "
            "historical baseline range"
        )
    )
)


comparison_df.select(
    "grid_id",
    "timestamp",
    "total_activity",
    "baseline_activity",
    "anomaly_score",
    "anomaly_flag",
    "direction",
    "reason"
).show(
    30,
    truncate=False
)

final_ml4_df = comparison_df.select(
    "grid_id",
    "timestamp",
    "total_activity",
    "baseline_activity",
    "deviation",
    "anomaly_score",
    "anomaly_flag",
    "direction",
    "reason",
    "prediction",
    "alert_flag",
    "ml3_anomaly_disagreement",
    "np3_anomaly_disagreement"
)

(
    final_ml4_df
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print("\n========================================")
print("ML4 COMPLETED SUCCESSFULLY")
print("========================================")

print("Output path:")
print(OUTPUT_PATH)

print("\nFinal schema:")
final_ml4_df.printSchema()

print("\nSample final records:")
final_ml4_df.show(
    20,
    truncate=False
)

spark.stop()