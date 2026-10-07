from pydantic import BaseModel,Field
import pymysql
from datetime import datetime, timedelta
from typing import Optional
from fastapi import FastAPI, HTTPException, Query

def get_connection():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="root",
        database="telecom_analytics",
        cursorclass=pymysql.cursors.DictCursor
    )

app=(FastAPI(title="Telecom Network Analytics API", version="1.0"))

class NetworkResult(BaseModel):
    grid_id:int
    timestamp:datetime
    sms_in:float
    sms_out:float
    call_in:float
    call_out:float
    internet:float
    total_activity:float

    status:Optional[str]=None
    severity:Optional[str]=None
    reason:Optional[str]=None
    risk_score:Optional[str]=None

class HotspotResponse(BaseModel):
    as_of:datetime
    results:list[NetworkResult]

class AlertResponse(BaseModel):
    as_of:datetime
    results:list[NetworkResult]

@app.get("/network/hotspot", response_model=HotspotResponse)
@app.get("/network/hotspots", response_model=HotspotResponse)
def get_hotspot(limit:int=Query(default=10,ge=1),
                as_of:Optional[datetime]=None):
    
    connection=None

    try:
        connection=get_connection()
        cursor=connection.cursor()

        if as_of is None:
            cursor.execute("""
                select max(timestamp) as max_timestamp
                from dim_time
            """)
            result=cursor.fetchone()
            effective_as_of=result["max_timestamp"]
            if effective_as_of is None:
                raise HTTPException(status_code=500, detail="No analytics data available")
        else:
            effective_as_of=as_of

        cursor.execute("""
            select f.grid_id,t.timestamp,f.call_in,f.call_out,f.sms_in,f.sms_out,f.internet,f.total_activity 
            from fact_network_activity f 
            join dim_time t on f.time_key=t.time_key
            where t.timestamp=%s
            order by f.total_activity desc,f.grid_id asc limit %s""",(effective_as_of, limit))

        rows=cursor.fetchall()
        result=[]
        for row in rows:
            result.append(NetworkResult(
                grid_id=row["grid_id"],
                timestamp=row["timestamp"],
                sms_in=row["sms_in"],
                sms_out=row["sms_out"],
                call_in=row["call_in"],
                call_out=row["call_out"],
                internet=row["internet"],
                total_activity=float(row["total_activity"]),
                status="High Activity"
            ))

        return HotspotResponse(
            as_of=effective_as_of,
            results=result
        )
    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=500,detail=f"Internal server error: {error}")
    finally:
        if connection is not None:
            connection.close()

@app.get("/network/alerts", response_model=AlertResponse)
def get_alert(limit:int=Query(default=10,ge=1),severity:Optional[str]=None,as_of:Optional[datetime]=None):

    connection = None

    try:

        connection = get_connection()
        cursor = connection.cursor()

        if as_of is None:

            cursor.execute("""
                SELECT MAX(timestamp) AS max_timestamp
                FROM alerts
            """)

            result = cursor.fetchone()

            if result["max_timestamp"] is None:
                raise HTTPException(
                    status_code=500,
                    detail="No alert data available"
                )

            effective_as_of = result["max_timestamp"]

        else:

            effective_as_of = as_of

        query = """
            SELECT
                grid_id,
                timestamp,
                current_activity,
                baseline_activity,
                alert_type,
                reason
            FROM alerts
            WHERE timestamp = %s
        """

        parameters = [effective_as_of]

        # The dashboard sends ALL for its unfiltered state.
        if severity is not None and severity.upper() != "ALL":

            query += """ AND alert_type = %s"""

            parameters.append(severity)

        query += """
            ORDER BY
                current_activity DESC,
                grid_id ASC,
                alert_type ASC
            LIMIT %s
        """

        parameters.append(limit)

        cursor.execute(
            query,
            tuple(parameters)
        )

        rows = cursor.fetchall()

        results = []

        for row in rows:

            results.append(
                NetworkResult(
                    grid_id=row["grid_id"],
                    timestamp=row["timestamp"],
                    sms_in=0.0,
                    sms_out=0.0,
                    call_in=0.0,
                    call_out=0.0,
                    internet=0.0,
                    total_activity=float(
                        row["current_activity"]
                    ),
                    status=row["alert_type"],
                    severity=row["alert_type"],
                    reason=row["reason"]
                )
            )

        return AlertResponse(
            as_of=effective_as_of,
            results=results
        )

    except HTTPException:
        raise

    except Exception as error:

        raise HTTPException(
            status_code=500,
            detail=f"Analytics data source unavailable: {error}"
        )

    finally:

        if connection is not None:
            connection.close()
