from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import pymysql


app = FastAPI( title="Telecom Network Analytics API",version="1.0")

def get_connection():

    return pymysql.connect(
        host="localhost",
        user="root",
        password="root",
        database="telecom_analytics",
        cursorclass=pymysql.cursors.DictCursor
    )

class GridFeatures(BaseModel):
    grid_id: int
    avg_activity: float
    activity_growth: float
    active_hours: int
    peak_ratio: float
    variability: float
    internet_share: float
    feature_timestamp: datetime
    data_quality: str
    feature_freshness: str

@app.get("/network/grid/{grid_id}/features",response_model=GridFeatures)
def get_grid_features(grid_id: int, timestamp: Optional[datetime] = None):
    connection = None
    try:
        if grid_id < 1 or grid_id > 10000:
            raise HTTPException(
                status_code=404,
                detail="Grid not found"
            )
        connection = get_connection()
        cursor = connection.cursor()
 
        timestamp_filter = ""
        parameters = [grid_id]
        if timestamp is not None:
            timestamp_filter = "AND feature_timestamp <= %s"
            parameters.append(timestamp.replace(tzinfo=None))

        cursor.execute(f"""
            SELECT
                grid_id,
                avg_activity,
                activity_growth,
                active_hours,
                peak_ratio,
                variability,
                internet_share,
                feature_timestamp
                FROM ml2_features
                WHERE grid_id = %s
                {timestamp_filter}
                ORDER BY feature_timestamp DESC
                LIMIT 1""", parameters)

        row = cursor.fetchone()
        if row is None:
            raise HTTPException(status_code=404,detail=f"Grid {grid_id} features not found")

        quality = "valid"

        feature_values = [
            row["avg_activity"],
            row["activity_growth"],
            row["active_hours"],
            row["peak_ratio"],
            row["variability"],
            row["internet_share"]
        ]

        if any(value is None for value in feature_values):
            quality = "invalid"

        feature_freshness = "current"

        return GridFeatures(
            grid_id=int(row["grid_id"]),
            avg_activity=float(row["avg_activity"]),
            activity_growth=float(row["activity_growth"]),
            active_hours=int(row["active_hours"]),
            peak_ratio=float(row["peak_ratio"]),
            variability=float(row["variability"]),
            internet_share=float(row["internet_share"]),
            feature_timestamp=row["feature_timestamp"],
            data_quality=quality,
            feature_freshness=feature_freshness
        )

    except HTTPException:
        raise

    except Exception as error:
        raise HTTPException(status_code=500,detail=f"Feature data source unavailable: {error}")


    finally:
        if connection is not None:
            connection.close()