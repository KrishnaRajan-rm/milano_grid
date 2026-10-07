from sp2 import run
 
from pyspark.sql.functions import (
    col,
    desc,
    sum,
    to_date
)
def hourly_run():
 
    clean_df = run()
 
    hourly_grid_summary = (clean_df
    .groupBy("timestamp","grid_id")
    .agg(
        sum("sms_in").alias("sms_in"),
        sum("sms_out").alias("sms_out"),
        sum("call_in").alias("call_in"),
        sum("call_out").alias("call_out"),
        sum("internet").alias("internet")
    ))
 
    hourly_grid_summary = (hourly_grid_summary
        .withColumn("total_sms",col("sms_in") + col("sms_out"))
        .withColumn("total_calls" , col("call_in") + col("call_out"))
        .withColumn("total_activity" , col("total_sms") + col("total_calls") + col("internet"))
        )
 
    daily_summary = (hourly_grid_summary
                    .withColumn("date",to_date(col("timestamp")))
                    .groupBy("date","grid_id")
        .agg(
            sum("total_calls").alias("daily_total_calls"),
            sum("total_sms").alias("daily_total_sms"),
            sum("total_activity").alias("daily_total_activity"),
            sum("internet").alias("daily_internet")
       
        ))
 
    hotspot_ranking = daily_summary.orderBy(desc("daily_total_activity")).limit(10)
    print("TOP-10 GRIDS WITH HIGH ACTIVITY")
    hotspot_ranking.show()
 
    peak_activity_hour = (hourly_grid_summary.groupBy("timestamp")
                .agg(
                        sum("total_activity").alias("total_activity")
                    )
                .orderBy(desc("total_activity"))
                .limit(1)
    )
 
    print("PEAK ACTIVITY HOUR")
    peak_activity_hour.show()
 
    share = (hourly_grid_summary.withColumn(
    "internet_share",
    col("internet") / col("total_activity")
    ))
 
    print("INTERNET SHARE OF TOTAL ACTIVITY")
    share.select("grid_id","timestamp","internet_share").show()

    
    hourly_grid_summary.toPandas().to_csv("hourly_grid_summary.csv",index=False)
 
    return hourly_grid_summary
 
 
if __name__ == "__main__":
    hourly_run()