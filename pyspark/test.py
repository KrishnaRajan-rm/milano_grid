import os
os.environ["HADOOP_HOME"] = r"C:\GraddedAssignment\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\GraddedAssignment\hadoop\bin"
 

from pyspark.sql import SparkSession

spark=(SparkSession.builder
    .appName("MyApp")
    .getOrCreate())

spark.stop