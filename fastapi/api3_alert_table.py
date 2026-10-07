import pymysql
import pandas as pd
 
connection = pymysql.connect(
    host = "localhost",
    user = "root",
    password = "root",
    autocommit = True,
    database = "telecom_analytics"
)
 
cursor = connection.cursor()
 
cursor.execute("""
    create table if not exists alerts(
     alert_id bigint auto_increment primary key,
     grid_id int not null,
     timestamp datetime not null,
     alert_type varchar(50) not null,
     current_activity double,
     baseline_activity double,
     reason varchar(200),
 
     index idx_alert_timestamp (timestamp),
     index idx_alert_id (alert_type),
     index idx_alert_grid_id (grid_id)
    )
""")
 
alert_df = pd.read_csv("../dataset/landing/alerts_df.csv")
alert_df["timestamp"] = pd.to_datetime(
    alert_df["timestamp"],
    errors = "coerce"
)
 
cursor.execute("truncate table alerts")
 
data = alert_df[
    [
        "grid_id",
        "timestamp",
        "alert_type",
        "current_activity",
        "baseline_activity",
        "reason"
    ]
].itertuples(
    index = False,
    name = None
)
 
cursor.executemany("""
    insert into alerts(
      grid_id,
      timestamp,
      alert_type,
      current_activity,
      baseline_activity,
      reason
    ) values(%s,%s,%s,%s,%s,%s)
""",data)
 
cursor.close()
connection.close()