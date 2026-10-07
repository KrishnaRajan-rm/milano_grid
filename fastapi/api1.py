from pydantic import BaseModel
import pymysql
from datetime import datetime
# from typing import Optional
from fastapi import FastAPI, HTTPException

MYSQL_HOST = "localhost"
MYSQL_PORT=3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_DATABASE = "telecom_analytics"

app=(FastAPI(title="Telecom Network Analytics API", version="1.0"))

class NetworkSummary(BaseModel):
    total_activity:float
    active_grids:int
    peak_hour: int
    top_grid:int
    as_of:datetime

def get_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        cursorclass=pymysql.cursors.DictCursor
    )

@app.get("/network/summary",response_model=NetworkSummary)
def network_summary(as_of : datetime = None):
    connection=None
    try:
        connection=get_connection()
        cursor=connection.cursor()

        if as_of is None:
            cursor.execute("""
                select max(timestamp) as max_timestamp from dim_time
                """)
            result=cursor.fetchone()
            effective_as_of = result["max_timestamp"]
        else:
            effective_as_of=as_of
       

        cursor.execute("""
            select coalesce(sum(f.total_activity),0) as total_activity from fact_network_activity f
            join dim_time t on f.time_key=t.time_key
            where t.timestamp<= %s
            """, (effective_as_of,) )

        total_activity=cursor.fetchone()["total_activity"]

        cursor.execute("""
            select count(distinct f.grid_id) as activity_grids from fact_network_activity f
            join dim_time t on f.time_key=t.time_key
            where t.timestamp <= %s and f.total_activity>0
            """, (effective_as_of,) )

        activity_grids=cursor.fetchone()["activity_grids"]


        cursor.execute("""
            select t.hour as peak_hour,sum(f.total_activity) as total_activity from fact_network_activity f
            join dim_time t on f.time_key=t.time_key
            where t.timestamp<= %s
            group by t.hour
            order by total_activity desc
            limit 1
            """, (effective_as_of,) )

        peak_result=cursor.fetchone()

        if peak_result is None:
            raise RuntimeError("No peak hour found for the given timestamp")

        peak_hour=peak_result["peak_hour"]

        cursor.execute("""
            select f.grid_id as top_grid,sum(f.total_activity) as total_activity from fact_network_activity f
            join dim_time t on f.time_key=t.time_key
            where t.timestamp<= %s group by f.grid_id
            order by total_activity desc limit 1
            """, (effective_as_of,) )

        top_grid_result=cursor.fetchone()

        if top_grid_result is None:
            raise RuntimeError("No top grid found for the given timestamp")

        top_grid=top_grid_result["top_grid"]

        return NetworkSummary(
            total_activity= total_activity,
            active_grids= activity_grids,
            peak_hour= peak_hour,
            top_grid= top_grid,
            as_of=effective_as_of
        )

    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"Analytics data source unavailable: {str(error)}"
        )

    finally:
        if connection is not None:
            connection.close()

