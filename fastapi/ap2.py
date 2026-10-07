from pydantic import BaseModel
import pymysql
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Query

MYSQL_HOST = "localhost"
MYSQL_PORT=3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_DATABASE = "telecom_analytics"

app=(FastAPI(title="Telecom Network Analytics API", version="1.0"))
 
class GridActivity(BaseModel):
    timestamp: datetime
    
    sms_in: float
    sms_out: float
    call_in: float
    call_out: float
    internet: float
    total_sms: float
    total_calls: float
    total_activity: float

class GridResponse(BaseModel):
    grid_id:int
    as_of:datetime
    data:list[GridActivity]

def get_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        cursorclass=pymysql.cursors.DictCursor
    )

@app.get("/network/grid/{grid_id}",response_model=GridResponse)
def get_grid_activity(grid_id:int,
                      date:str = Query(default=None),
                      hour:int = Query(default=None),
                      as_of:datetime = Query(default=None)) :

    connection=None
    try:
        if grid_id<1 or grid_id>10000:
            raise HTTPException(status_code=404,detail=f'invalid {grid_id}')
        connection=get_connection()
        cursor=connection.cursor()

        if date is not None:
            try:
                date=datetime.strptime(date,"%Y-%m-%d").date()
            except ValueError:
                raise HTTPException(status_code=400,detail="Invalid Hour Value")

        if hour is not None:
            if hour<0 or hour>23:
                raise HTTPException(status_code=400, detail="Invalid Hour Value")
        
        if as_of is None:
            cursor.execute("""select max(timestamp) as max_timestamp from dim_time""")
            result=cursor.fetchone()
            effective_as_of =result["max_timestamp"]
        else:
            effective_as_of=as_of
        starting_window=effective_as_of - timedelta(hours=23)

        query="""
                select t.timestamp, f.sms_in, f.sms_out,f.call_in,f.call_out,f.internet,f.total_activity 
                from fact_network_activity f join dim_time t 
                on f.time_key=t.time_key where f.grid_id=%s 
                and t.timestamp between %s and %s"""

        parameters= [grid_id,starting_window,effective_as_of]

        if date is not None:
            query+=" and t.date=%s"
            parameters.append(date)

        if hour is not None:
            query +=  " and t.hour=%s"
            parameters.append(hour)

        query+= " order by t.timestamp"

        cursor.execute(query,tuple(parameters))

        result = cursor.fetchall()
        data=[]

        for row in result:
            data.append(GridActivity(
                timestamp=row["timestamp"],
                sms_in = float(row["sms_in"]),
                sms_out = float(row["sms_out"]),
                call_in = float(row["call_in"]),
                call_out = float(row["call_out"]),
                internet = float(row["internet"]),
                total_sms = float(row["sms_in"]) + float(row["sms_out"]),
                total_calls = float(row["call_in"]) + float(row["call_out"]),
                total_activity = float(row["total_activity"])
        ))
    
        return GridResponse(
            grid_id = grid_id,
            as_of = effective_as_of,
            data = data
        )
     
       
 
    except HTTPException :
        raise
 
 
    except Exception as e:
        raise HTTPException(
            status_code = 500,
            detail = f"Error while fetching grid summary : {e}"
        )
 
    finally:
        if connection is not None :
            connection.close()
 