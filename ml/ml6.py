# ============================================================
# ML6 — BATCH RISK + ANOMALY SCORING
# ============================================================

import os
import joblib

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit,
    row_number,
    abs,
    desc
)
from pyspark.sql.window import Window


# ============================================================
# WINDOWS / HADOOP
# ============================================================

os.environ["HADOOP_HOME"] = r"C:\GraddedAssignment\hadoop"
os.environ["PATH"] += r";C:\GraddedAssignment\hadoop\bin"


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("Telecom ML6 Batch Scoring")
    .master("local[*]")
    .getOrCreate()
)


# ============================================================
# PATHS
# ============================================================

ML2_PATH = r"C:\GraddedAssignment\dataset\output\ml2_features"

ML4_PATH = r"C:\GraddedAssignment\dataset\output\ml4_anomalies"

MODEL_PATH = r"C:\GraddedAssignment\dataset\output\ml5_model\decision_tree.joblib"

OUTPUT_PATH = r"C:\GraddedAssignment\dataset\output\network_risk_scores"

REPORT_PATH = r"C:\GraddedAssignment\dataset\output\top20_operational_attention"


# ============================================================
# MODEL VERSION
# ============================================================

MODEL_VERSION = "decision-tree-v1"


# ============================================================
# ACTIVITY 1
# READ LATEST ML2 FEATURE TABLE
# ============================================================

print("\n========================================")
print("ML6 ACTIVITY 1")
print("Reading ML2 feature table")
print("========================================")

features = spark.read.parquet(ML2_PATH)

print("ML2 features loaded")

features.printSchema()

print("Sample:")
features.show(10, truncate=False)


# ============================================================
# VALIDATE REQUIRED ML2 COLUMNS
# ============================================================

required_features = [
    "grid_id",
    "feature_timestamp",
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share"
]

missing_features = [
    feature
    for feature in required_features
    if feature not in features.columns
]

if missing_features:

    raise ValueError(
        f"Missing ML2 features: {missing_features}"
    )


# ============================================================
# LOAD TRAINED MODEL
# ============================================================

print("\nLoading trained ML5 model...")

if not os.path.exists(MODEL_PATH):

    raise FileNotFoundError(
        f"Model not found: {MODEL_PATH}"
    )

model = joblib.load(MODEL_PATH)

print("ML5 Decision Tree loaded successfully")


# ============================================================
# ACTIVITY 2
# RUN RISK SCORING IN BATCH
# ============================================================

print("\n========================================")
print("ML6 ACTIVITY 2")
print("Batch risk scoring")
print("========================================")

# Convert Spark ML2 features to Pandas
# because the sklearn Decision Tree is a Python model.

feature_pdf = features.select(
    "grid_id",
    "feature_timestamp",
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share"
).toPandas()


# ------------------------------------------------------------
# Feature columns MUST be in the same order used during ML3
# ------------------------------------------------------------

X = feature_pdf[
    [
        "avg_activity",
        "activity_growth",
        "active_hours",
        "peak_ratio",
        "variability",
        "internet_share"
    ]
]


# ------------------------------------------------------------
# Risk probability
# ------------------------------------------------------------

if hasattr(model, "predict_proba"):

    probabilities = model.predict_proba(X)

    classes = list(model.classes_)

    if 1 in classes:

        risk_scores = probabilities[
            :,
            classes.index(1)
        ]

    else:

        risk_scores = [0.0] * len(feature_pdf)

else:

    predictions = model.predict(X)

    risk_scores = predictions


# ------------------------------------------------------------
# Add risk score
# ------------------------------------------------------------

feature_pdf["risk_score"] = risk_scores


# ------------------------------------------------------------
# Risk level
# ------------------------------------------------------------

def get_risk_level(score):

    if score >= 0.70:
        return "high"

    elif score >= 0.40:
        return "medium"

    else:
        return "low"


feature_pdf["risk_level"] = [
    get_risk_level(score)
    for score in feature_pdf["risk_score"]
]


feature_pdf["model_version"] = MODEL_VERSION


# ============================================================
# Convert back to Spark
# ============================================================

risk_df = spark.createDataFrame(feature_pdf)


print("Batch risk scoring completed")

risk_df.select(
    "grid_id",
    "feature_timestamp",
    "risk_score",
    "risk_level",
    "model_version"
).show(20, truncate=False)


# ============================================================
# ACTIVITY 2 — ANOMALY SCORING
# ============================================================

print("\n========================================")
print("ML6 — Anomaly scoring")
print("========================================")


# ------------------------------------------------------------
# Load ML4 anomaly results
# ------------------------------------------------------------

if os.path.exists(ML4_PATH):

    anomaly_df = spark.read.parquet(
        ML4_PATH
    )

    print("ML4 anomaly table loaded")

    print("ML4 columns:")
    print(anomaly_df.columns)

else:

    print(
        "ML4 output not found."
    )

    anomaly_df = None


# ------------------------------------------------------------
# Join ML4 anomaly information
# ------------------------------------------------------------

if anomaly_df is not None:

    anomaly_columns = [
        "grid_id",
        "timestamp",
        "anomaly_score",
        "anomaly_flag",
        "direction",
        "reason"
    ]

    available_columns = [
        c
        for c in anomaly_columns
        if c in anomaly_df.columns
    ]

    anomaly_df = anomaly_df.select(
        *available_columns
    )

    # ML2 timestamp is feature_timestamp.
    # ML4 timestamp is timestamp.

    anomaly_df = anomaly_df.withColumn(
        "feature_timestamp",
        col("timestamp")
    )

    anomaly_df = anomaly_df.drop(
        "timestamp"
    )

    risk_df = risk_df.join(
        anomaly_df,
        on=[
            "grid_id",
            "feature_timestamp"
        ],
        how="left"
    )

else:

    risk_df = (
        risk_df
        .withColumn(
            "anomaly_score",
            lit(None).cast("double")
        )
        .withColumn(
            "anomaly_flag",
            lit(None).cast("integer")
        )
        .withColumn(
            "direction",
            lit(None).cast("string")
        )
        .withColumn(
            "reason",
            lit(None).cast("string")
        )
    )


# ============================================================
# ACTIVITY 3
# FINAL NETWORK RISK SCORES TABLE
# ============================================================

print("\n========================================")
print("ML6 ACTIVITY 3")
print("Creating network_risk_scores")
print("========================================")


network_risk_scores = risk_df.select(
    "grid_id",
    col("feature_timestamp").alias("timestamp"),
    "risk_score",
    "risk_level",
    "model_version",
    "anomaly_score",
    "anomaly_flag",
    "direction",
    "reason"
)


# ------------------------------------------------------------
# Save
# ------------------------------------------------------------

(
    network_risk_scores
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)


print(
    "network_risk_scores saved successfully"
)

print(
    "Output:",
    OUTPUT_PATH
)


# ============================================================
# SHOW FINAL TABLE
# ============================================================

network_risk_scores.select(
    "grid_id",
    "timestamp",
    "risk_score",
    "risk_level",
    "model_version",
    "anomaly_score",
    "anomaly_flag",
    "direction"
).show(
    20,
    truncate=False
)


# ============================================================
# ACTIVITY 6
# TOP 20 OPERATIONAL ATTENTION REPORT
# ============================================================

print("\n========================================")
print("ML6 ACTIVITY 6")
print("Top 20 operational attention")
print("========================================")


# ------------------------------------------------------------
# Create combined operational score
#
# Priority:
# 1. High risk
# 2. Strong anomaly
#
# We use absolute anomaly score because both
# HIGH and LOW anomalies matter operationally.
# ------------------------------------------------------------

attention_df = network_risk_scores.withColumn(
    "anomaly_magnitude",
    when(
        col("anomaly_score").isNotNull(),
        abs(col("anomaly_score"))
    ).otherwise(0.0)
)


# ------------------------------------------------------------
# Rank by risk first, anomaly second
# ------------------------------------------------------------

attention_df = attention_df.orderBy(
    desc("risk_score"),
    desc("anomaly_magnitude")
)


# ------------------------------------------------------------
# Take top 20
# ------------------------------------------------------------

top20 = attention_df.limit(20)


# ------------------------------------------------------------
# Save report
# ------------------------------------------------------------

(
    top20
    .write
    .mode("overwrite")
    .option("header", True)
    .parquet(REPORT_PATH)
)


print(
    "Top-20 operational attention report created"
)

print(
    "Report:",
    REPORT_PATH
)


# ============================================================
# FINAL TOP 20
# ============================================================

print("\nTOP 20 OPERATIONAL ATTENTION:")

top20.select(
    "grid_id",
    "timestamp",
    "risk_score",
    "risk_level",
    "anomaly_score",
    "direction",
    "reason"
).show(
    20,
    truncate=False
)


# ============================================================
# COMPLETION
# ============================================================

print("\n========================================")
print("ML6 COMPLETED SUCCESSFULLY")
print("========================================")

print(
    "Risk scores:",
    OUTPUT_PATH
)

print(
    "Top-20 report:",
    REPORT_PATH
)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()