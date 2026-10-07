import os
os.environ["HADOOP_HOME"] = r"C:\GraddedAssignment\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\GraddedAssignment\hadoop\bin"


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    input_file_name,
    when,
    lit,
    hour,
    to_date,
    date_format
)

def run():
    spark = SparkSession.builder\
            .appName("Telecom sp2")\
            .master("local[*]")\
            .getOrCreate()
    
    input_file_path = "../dataset/raw/sms-call-internet-mi-*.csv"
    
    raw_df = (spark.read
            .option("header",True)
            .option("inferSchema",True)
            .csv(input_file_path))
    
    columns_mapping = {
        "datetime" : "timestamp",
        "CellID" : "grid_id",
        "countrycode" :"country_code",
        "smsin" :"sms_in",
        "smsout" : "sms_out",
        "callin" : "call_in",
        "callout" : "call_out",
        "internet" : "internet"
    }
    
    clean_df = raw_df.toDF(
        *[
            columns_mapping.get(column,column)
            for column in raw_df.columns
        ]
    )
    print("-----------------------------------",clean_df.count())
    
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
    
    file_count = clean_df.select(input_file_name()).distinct().count()
    timestamp_count = clean_df.select("timestamp").distinct().count()
    
    print(f"Cadence Validation : {file_count * 24 == timestamp_count}")
    
    quarantine_condition = (
        col("grid_id").isNull() |
        col("timestamp").isNull() |
        (col("sms_in").isNotNull() & (col("sms_in") < 0)) |
        (col("sms_out").isNotNull() & (col("sms_out") < 0)) |
        (col("call_in").isNotNull() & (col("call_in") < 0)) |
        (col("call_out").isNotNull() & (col("call_out") < 0)) |
        (col("internet").isNotNull() & (col("internet") < 0))
    )
    
    rejected_rows = clean_df.filter(
        quarantine_condition
    )
    
    activity_null_condition = (
        col("sms_in").isNull()|
        col("sms_out").isNull()|
        col("call_in").isNull()|
        col("call_out").isNull()|
        col("internet").isNull()
    )
    
    activity_null_rows = clean_df.filter(
        activity_null_condition
    )
    
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
    
    clean_df = clean_df.withColumn(
        "total_sms",
        col("sms_in") + col("sms_out")
    )
    
    clean_df = clean_df.withColumn(
        "total_calls",
        col("call_in") + col("call_out")
    )
    
    clean_df = clean_df.withColumn(
        "total_activity",
        col("total_sms") + col("total_calls") + col("internet")
    )
    
    clean_df = clean_df.withColumn(
        "date",
        to_date(col("timestamp"))
    )
    
    clean_df = clean_df.withColumn(
        "hour",
        hour(col("timestamp"))
    )
    
    clean_df = clean_df.withColumn(
        "day_of_week",
        date_format(col("timestamp"),"EEEE")
    )
    
    print(f"Row count Before cleaning :{raw_df.count()} ")
    print(f"Row count After cleaning :{clean_df.count()} ")
    print(f"Rejected Rows : {rejected_rows.count()}")
    print(f"Activity Columns Null Count : {activity_null_rows.count()}")
    return clean_df


if __name__ == "__main__":
    run()