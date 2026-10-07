import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    desc,
    sum
)
 
spark = SparkSession.builder\
        .appName("Telecom sp4")\
        .master("local[*]")\
        .getOrCreate()
 
hourly_summary  = spark.read\
                  .option("header",True)\
                  .option("inferSchema",True)\
                  .csv("hourly_grid_summary.csv")
 
path = "../dataset/reference/milano-grid.geojson"
 
with open(path,'r',encoding="utf-8") as file:
    geo_json_data = json.load(file)
 
print("TOP LEVEL TYPE")
print(geo_json_data["type"])
 
print("NUMBER OF FEATURES")
print(geo_json_data["features"])
 
print("FIRST FEATURES")
print(geo_json_data["features"][0]["properties"])
 
print("FIRST GEOMETRY")
print(geo_json_data["features"][0]["geometry"])
 
 
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
    ["grid_id","geometry"]
)
 
print("GEO_JSON_DF ROWS COUNT")
print(geo_json_df.count())
 
geo_df = (
    hourly_summary.join(
        broadcast(geo_json_df),
        how = "left",
        on = "grid_id"
 
    )
)
 
geo_df_count = geo_df.count()
hourly_summary_count = hourly_summary.count()
 
print(geo_df_count == hourly_summary_count)
 
missing_grid_geometry = (geo_df.filter(col("geometry").isNull())
                         .select("grid_id")
                         .distinct().count()
                         )
 
assert missing_grid_geometry == 0,"Missing Geometry for Some Grids"
 
print("ENRICHMENT COVERAGE")
enrichment_geometry = (geo_df.filter(col("geometry").isNotNull())
                         .select("grid_id")
                         .distinct()
                         .count())
 
print(f"{(enrichment_geometry / hourly_summary_count):.2f}")
 
def derive_centroid(geometry_json):
 
    geometry = json.loads(geometry_json)
    coordinates = geometry["coordinates"]
 
    if geometry["type"] == "Polygon":
        points = coordinates[0]
 
    elif geometry["type"] == "MultiPolygon":
        points = coordinates[0][0]
    else:
        return (None,None)
 
    longitudes = [point[0] for point in points]
    latitudes = [point[1] for point in points]
 
    center_longitude = (min(longitudes) + max(longitudes))/2
    center_latitude = (min(latitudes) + max(latitudes))/2
 
    return (center_longitude,center_latitude)
 
 
 
girds = geo_df.filter(col("grid_id").isin(1,2)).collect()
 
centers = {}
 
for row in girds:
 
    longitude,latitude = derive_centroid(row["geometry"])
 
    centers[row["grid_id"]] = (longitude,latitude)
 
assert centers[1] != centers[2],"Grids have identical centers"
 
standard_join = hourly_summary.join(
    geo_json_df,
    on= "grid_id",
    how = "left"  
)
 
print("STANDARD JOIN")
standard_join.explain(
    mode = "formatted"
)
 
 
print("BROADCAST JOIN")
geo_df.explain(
    mode = "formatted"
)
 
geo_df = geo_df.select(
    "timestamp",
    "grid_id",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
     col("internet").alias("internet_activity"),
     "total_activity",
     "geometry"
)
 
print("ENRICHED DATASET")
geo_df.show(5,truncate = False)
 
print("TOP 10 HIGH_ACTIVITY GRIDS")
 
activity_grids = geo_df.groupBy("grid_id","geometry").agg(
    sum("total_activity").alias("Top_activity")
   
).orderBy(desc("Top_activity")).limit(10)
 
activity_grids.show()