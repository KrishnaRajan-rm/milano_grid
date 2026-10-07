import os
os.environ["HADOOP_HOME"] = r"C:\GraddedAssignment\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\GraddedAssignment\hadoop\bin"
 
from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name,hour,col

spark=(SparkSession.builder
    .appName("Telecome Data")
    .master("local[*]")
    .getOrCreate())

folder_path="../dataset/raw/sms-call-internet-mi-*.csv"

# manual_df=spark.read.csv(folder_path,header=True,inferSchema=True)
# manual_df.printSchema()

manual_schema=StructType([
    StructField("datetime",StringType(),True),
    StructField("CellID",IntegerType(),True),
    StructField("countrycode",IntegerType(),True),
    StructField("smsin",DoubleType(),True),
    StructField("smsout",DoubleType(),True),
    StructField("callin",DoubleType(),True),
    StructField("callout",DoubleType(),True),
    StructField("internet",DoubleType(),True)])

manual_df=(spark.read.schema(manual_schema).option("header",True).csv(folder_path))

infer_df=(spark.read.option("header",True).option("inferSchema",True).csv(folder_path))

print("Manual Schema:")
manual_df.printSchema()

print("Infer Schema:")
infer_df.printSchema()

print("Total records:",manual_df.count())

print("unique files:",manual_df.select(input_file_name()).distinct().count())

print("unique grids:", manual_df.select("CellID").distinct().count())

print("unique country codes:", manual_df.select("countrycode").distinct().count())

print("unique datetime:", manual_df.select(hour(col("datetime")).alias("hours")).distinct().count())

print("unique sms in:", manual_df.select("smsin").distinct().count())

print("unique sms out:", manual_df.select("smsout").distinct().count())

print("unique call in:", manual_df.select("callin").distinct().count())

print("unique call out:", manual_df.select("callout").distinct().count())

print("unique internet:", manual_df.select("internet").distinct().count())

spark.stop()

