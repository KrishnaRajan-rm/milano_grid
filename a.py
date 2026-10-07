# C:\Users\chandru.sivakumar\Downloads\mysql-connector-j-26.7.0.zip\mysql-connector-j-26.7.0\mysql-connector-j-26.7.0.jar
 
import os
os.environ["HADOOP_HOME"] = r"C:\GraddedAssignment\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\GraddedAssignment\hadoop\bin"
 
from pyspark.sql import SparkSession
 
spark = SparkSession.builder \
    .appName("MySQLJDBC") \
    .config("spark.jars", r"C:\GraddedAssignment\mysql-connector-j-26.7.0\mysql-connector-j-26.7.0.jar") \
    .getOrCreate()
 
jdbc_url = "jdbc:mysql://localhost:3306/krishna"
 
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "employees") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()
 
df.show()