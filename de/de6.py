import pymysql
import pandas as pd
import json
 
from shapely.geometry import shape
 
Connection = pymysql.connect(
    host = "localhost",
    user = "root",
    password = "root",
    database = "telecom_analytics",
    autocommit = True
)
 
cursor = Connection.cursor()
 
reference_path = "../dataset/reference/milano-grid.geojson"
 
with open(reference_path,"r",encoding = "utf-8") as file:
    geo_data = json.load(file)
 
grid_record = []
 
for feature in geo_data["features"]:
 
    properties = feature["properties"]
    geometry = feature["geometry"]
 
    grid_id = int(properties["cellId"])
 
    polygon = shape(geometry)
 
    centroid = polygon.centroid
 
    grid_record.append((
        grid_id,
        centroid.y,
        centroid.x,
        json.dumps(geometry)
    ))
 
dim_grid = pd.DataFrame(grid_record)
 
 
hourly_summary = pd.read_parquet("../dataset/output/hourly_grid_summary")
hourly_summary["timestamp"] = pd.to_datetime(hourly_summary["timestamp"],errors = "coerce")
 
hourly_summary["time_key"] = hourly_summary["timestamp"].astype("int64") // 10**9
 
print("DIMENSIONS")
print(["grid_id","timestamp"])
 
print("MEASURES")
print(["sms_in" , "sms_out" , "call_in" , "call_out" , "internet", "total_activity"])
 
cursor.execute("""
    create table if not exists dim_time(
      time_key bigint primary key,
      timestamp datetime not null,
      date date not null,
      hour int not null,
      day varchar(20)
    )
""")
 
cursor.execute("""
   create table if not exists dim_grid(
     grid_id int primary key,
     centroid_longitude double,
     centroid_latitude double,
     geometry json
   )
""")
 
cursor.execute("""
   create table if not exists fact_network_activity(
     grid_id int,
     time_key bigint,
     sms_in double,
     sms_out double,
     call_in double,
     call_out double,
     internet double,
     total_activity double,
 
     primary key(grid_id,time_key),
 
     foreign key (grid_id) references dim_grid(grid_id),
     foreign key (time_key) references dim_time(time_key)
    )
""")
 
 
dim_time = hourly_summary[["timestamp"]].drop_duplicates().copy()
 
dim_time["time_key"] = dim_time["timestamp"].astype("int64") // 10**9
 
dim_time["hour"] = dim_time["timestamp"].dt.hour
 
dim_time["date"] = dim_time["timestamp"].dt.date
 
dim_time["day"] = dim_time["timestamp"].dt.day_name()
 
time_rows = dim_time[
        [
            "time_key",
            "timestamp",
            "date",
            "hour",
            "day"
    ]
].itertuples(index = False,name = None)
 
cursor.executemany("""
        insert ignore into dim_time(
          time_key,
          timestamp,
          date,
          hour,
          day
        )
       
        values(%s,%s,%s,%s,%s)
    """, time_rows)
Connection.commit()
 
 
cursor.executemany("""
      insert ignore into dim_grid(
          grid_id,
          centroid_longitude,
          centroid_latitude,
          geometry
      )
      values(%s,%s,%s,%s)
      on duplicate key update
       centroid_longitude = values(centroid_longitude),
       centroid_latitude = values(centroid_latitude),
       geometry = values(geometry)
""",grid_record)
 
Connection.commit()
 
batch_size = 20000
batch = []
 
for ind , row in hourly_summary.iterrows():
 
    batch.append((
        int(row["grid_id"]),
        int(row["time_key"]),
        float(row["sms_in"]),
        float(row["sms_out"]),
        float(row["call_in"]),
        float(row["call_out"]),
        float(row["internet"]),
        float(row["total_activity"])
    ))
 
    if(len(batch) == batch_size):
 
        cursor.executemany("""
        insert ignore into fact_network_activity(
            grid_id,
            time_key,
            sms_in,
            sms_out,
            call_in,
            call_out,
            internet,
            total_activity
 
        )
        values(%s,%s,%s,%s,%s,%s,%s,%s)
 
        """, batch)
 
        Connection.commit()
        batch = []
 
if(len(batch) > 0):
    cursor.executemany("""
        insert into fact_network_activity(
            grid_id,
            time_key,
            sms_in,
            sms_out,
            call_in,
            call_out,
            internet,
            total_activity
 
        )
        values(%s,%s,%s,%s,%s,%s,%s,%s)
 
        """, batch)
   
    Connection.commit()
 
cursor.execute("""
  create index idx_fact_grid
  on fact_network_activity(grid_id)
""")
 
cursor.execute("""
  create index idx_fact_time
  on fact_network_activity(time_key)
""")
 