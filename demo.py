import os
os.environ["HADOOP_HOME"] = r"C:\GraddedAssignment\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\GraddedAssignment\hadoop\bin"
 

from pyspark.sql import SparkSession  
 
spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[1]") \
    .getOrCreate()
 
 # Data
students = [
    ("Ravi", 22),
    ("Priya", 23),
    ("Arun", 21)
]
 
# Column names
columns = ["name", "age"]
df = spark.createDataFrame(students, columns)
df.show()
df.printSchema()
df.select("name").show()
df.filter(df.age >30).show()
spark.stop()
