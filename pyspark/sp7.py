import os
import json
import glob
import argparse
import logging
from datetime import datetime
os.environ["HADOOP_HOME"] = r"C:\GraddedAssignment\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\GraddedAssignment\hadoop\bin"
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("telecom_pipeline")

def read_raw(spark, input_path):

    input_pattern = os.path.join(
        input_path,
        "sms-call-internet-mi-*.csv"
    )

    files = glob.glob(input_pattern)

    if not files:
        raise FileNotFoundError(
            f"No input files found: {input_pattern}"
        )

    logger.info("Input files found: %d", len(files))

    raw_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_pattern)
    )

    input_rows = raw_df.count()

    logger.info("Input rows: %d", input_rows)

    return raw_df

def clean(raw_df):

    columns_mapping = {
        "datetime": "timestamp",
        "CellID": "grid_id",
        "countrycode": "country_code",
        "smsin": "sms_in",
        "smsout": "sms_out",
        "callin": "call_in",
        "callout": "call_out",
        "internet": "internet"
    }

    clean_df = raw_df.toDF(
        *[
            columns_mapping.get(column, column)
            for column in raw_df.columns
        ]
    )

    activity_columns = [
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet"
    ]

    for column in activity_columns:

        clean_df = clean_df.withColumn(
            column,
            col(column).cast("double")
        )

    clean_df = clean_df.withColumn(
        "timestamp",
        col("timestamp").cast("timestamp")
    )

    quarantine_condition = (
        col("grid_id").isNull()|col("timestamp").isNull()|
        (col("sms_in").isNotNull() & (col("sms_in") < 0))|
        (col("sms_out").isNotNull() & (col("sms_out") < 0))|
        (col("call_in").isNotNull() & (col("call_in") < 0))|
        (col("call_out").isNotNull() & (col("call_out") < 0))|
        (col("internet").isNotNull() & (col("internet") < 0))
        )

    rejected_rows = clean_df.filter(quarantine_condition).count()

    activity_null_condition = (
        col("sms_in").isNull()|
        col("sms_out").isNull()|
        col("call_in").isNull()|
        col("call_out").isNull()|
        col("internet").isNull()
    )

    activity_nulls_handled = clean_df.filter(activity_null_condition).count()

    clean_df = clean_df.filter(
        ~quarantine_condition
    )

    for column in activity_columns:

        clean_df = clean_df.withColumn(
            column,
            when(
                col(column).isNull(),
                lit(0.0)
            ).otherwise(
                col(column)
            )
        )

    clean_df = (
        clean_df
        .withColumn(
            "total_sms",
            col("sms_in") + col("sms_out")
        )
        .withColumn(
            "total_calls",
            col("call_in") + col("call_out")
        )
        .withColumn(
            "total_activity",
            col("total_sms")
            + col("total_calls")
            + col("internet")
        )
        .withColumn(
            "date",
            to_date(col("timestamp"))
        )
        .withColumn(
            "hour",
            hour(col("timestamp"))
        )
        .withColumn(
            "day_of_week",
            date_format(col("timestamp"), "EEEE")
        )
    )


    output_rows = clean_df.count()

    logger.info("Rejected rows: %d", rejected_rows)

    logger.info(
        "Activity null rows handled: %d",
        activity_nulls_handled
    )

    logger.info("Clean output rows: %d", output_rows)

    return clean_df, rejected_rows, activity_nulls_handled

def aggregate(clean_df):

    hourly_grid_summary = (
        clean_df
        .groupBy(
            "timestamp",
            "grid_id"
        )
        .agg(
            sum("sms_in").alias("sms_in"),
            sum("sms_out").alias("sms_out"),
            sum("call_in").alias("call_in"),
            sum("call_out").alias("call_out"),
            sum("internet").alias("internet")
        )
        .withColumn(
            "total_sms",
            col("sms_in") + col("sms_out")
        )
        .withColumn(
            "total_calls",
            col("call_in") + col("call_out")
        )
        .withColumn(
            "total_activity",
            col("total_sms")
            + col("total_calls")
            + col("internet")
        )
    )

    output_rows = hourly_grid_summary.count()

    logger.info(
        "Aggregated hourly output rows: %d",
        output_rows
    )

    return hourly_grid_summary

def enrich(hourly_grid_summary, reference_path):

    geojson_path = os.path.join(
        reference_path,
        "milano-grid.geojson"
    )

    if not os.path.exists(geojson_path):
        raise FileNotFoundError(
            f"GeoJSON reference not found: {geojson_path}"
        )

    with open(
        geojson_path,
        "r",
        encoding="utf-8"
    ) as file:

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


    spark = hourly_grid_summary.sparkSession

    geo_json_df = spark.createDataFrame(
        grid_lookup,
        ["grid_id", "geometry"]
    )

    enriched_df = (
        hourly_grid_summary
        .join(
            broadcast(geo_json_df),
            on="grid_id",
            how="left"
        )
    )

    missing_geometry = (
        enriched_df
        .filter(col("geometry").isNull())
        .select("grid_id")
        .distinct()
        .count()
    )

    logger.info(
        "Grids with missing geometry: %d",
        missing_geometry
    )


    output_rows = enriched_df.count()

    logger.info(
        "Enriched output rows: %d",
        output_rows
    )

    return enriched_df

def write_outputs(
    clean_df,
    hourly_grid_summary,
    enriched_df,
    output_path
):

    os.makedirs(
        output_path,
        exist_ok=True
    )

    clean_path = os.path.join(
        output_path,
        "clean_activity"
    )

    clean_df.write \
        .mode("overwrite") \
        .partitionBy("date") \
        .parquet(clean_path)

    hourly_path = os.path.join(
        output_path,
        "hourly_grid_summary"
    )

    hourly_grid_summary.write \
        .mode("overwrite") \
        .parquet(hourly_path)

    dashboard_path = os.path.join(
        output_path,
        "dashboard_summary"
    )

    dashboard_summary = (
        hourly_grid_summary
        .groupBy("grid_id")
        .agg(
            sum("total_activity")
            .alias("total_activity")
        )
        .orderBy(
            desc("total_activity")
        )
        .limit(10)
    )

    dashboard_summary.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(dashboard_path)
    
    enriched_path = os.path.join(
        output_path,
        "enriched_activity"
    )

    enriched_df.write \
        .mode("overwrite") \
        .parquet(enriched_path)


    logger.info("Outputs written successfully")

def main():

    parser = argparse.ArgumentParser(
        description="Telecom Spark Pipeline"
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Input raw data folder"
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Output folder"
    )

    parser.add_argument(
        "--reference",
        required=True,
        help="Reference data folder"
    )


    args = parser.parse_args()


    start_time = datetime.now()

    logger.info("======================================")
    logger.info("TELECOM PIPELINE STARTED")
    logger.info("Start time: %s", start_time)
    logger.info("======================================")


    spark = (
        SparkSession.builder
        .appName("Telecom Pipeline")
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


    try:

        # 1. READ
        raw_df = read_raw(
            spark,
            args.input
        )


        # 2. CLEAN
        clean_df, rejected_rows, nulls_handled = clean(
            raw_df
        )


        # 3. AGGREGATE
        hourly_grid_summary = aggregate(
            clean_df
        )


        # 4. ENRICH
        enriched_df = enrich(
            hourly_grid_summary,
            args.reference
        )


        # 5. WRITE
        write_outputs(
            clean_df,
            hourly_grid_summary,
            enriched_df,
            args.output
        )


        end_time = datetime.now()

        logger.info("======================================")
        logger.info("PIPELINE STATUS: SUCCESS")
        logger.info("End time: %s", end_time)
        logger.info("======================================")


    except Exception as error:

        end_time = datetime.now()

        logger.exception(
            "PIPELINE STATUS: FAILED"
        )

        logger.error(
            "End time: %s",
            end_time
        )

        raise


    finally:

        spark.stop()

if __name__ == "__main__":
    main()