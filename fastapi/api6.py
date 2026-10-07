"""API6: supporting operational-evidence endpoints for the CP assistant."""

import json
from datetime import datetime
from pathlib import Path

import pymysql
from fastapi import FastAPI, HTTPException, Query


app = FastAPI(title="Telecom Network Analytics API", version="1.0")
PROJECT_ROOT = Path(__file__).resolve().parents[1]
GEOJSON_PATH = PROJECT_ROOT / "dataset" / "reference" / "milano-grid.geojson"


def get_connection():
    return pymysql.connect(
        host="localhost", user="root", password="root", database="telecom_analytics",
        cursorclass=pymysql.cursors.DictCursor,
    )


def load_features():
    try:
        return json.loads(GEOJSON_PATH.read_text(encoding="utf-8"))["features"]
    except (OSError, KeyError, json.JSONDecodeError) as error:
        raise HTTPException(status_code=500, detail=f"Grid reference data unavailable: {error}") from error


def feature_for(grid_id: int):
    for feature in load_features():
        if int(feature.get("properties", {}).get("cellId", -1)) == grid_id:
            return feature
    raise HTTPException(status_code=404, detail=f"Grid {grid_id} location not found")


def centroid(feature):
    coordinates = feature["geometry"]["coordinates"]
    ring = coordinates[0] if feature["geometry"]["type"] == "Polygon" else coordinates[0][0]
    return {
        "longitude": sum(point[0] for point in ring) / len(ring),
        "latitude": sum(point[1] for point in ring) / len(ring),
    }


@app.get("/network/grid/{grid_id}/location")
def get_grid_location(grid_id: int):
    if grid_id < 1:
        raise HTTPException(status_code=422, detail="grid_id must be positive")
    feature = feature_for(grid_id)
    return {
        "grid_id": grid_id,
        "centroid": centroid(feature),
        "geometry": feature["geometry"],
        "source": "dataset/reference/milano-grid.geojson",
    }


@app.get("/network/grid/{grid_id}/anomaly")
def get_anomaly_score(grid_id: int, timestamp: datetime | None = None):
    """Expose the latest stored anomaly evidence derived by the alert pipeline."""
    connection = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        query = """
            SELECT grid_id, timestamp, current_activity, baseline_activity, alert_type, reason
            FROM alerts WHERE grid_id = %s
        """
        parameters = [grid_id]
        if timestamp is not None:
            query += " AND timestamp <= %s"
            parameters.append(timestamp.replace(tzinfo=None))
        query += " ORDER BY timestamp DESC LIMIT 1"
        cursor.execute(query, tuple(parameters))
        row = cursor.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail=f"No stored anomaly evidence for grid {grid_id}")
        baseline = float(row["baseline_activity"] or 0)
        current = float(row["current_activity"] or 0)
        anomaly_score = ((current - baseline) / baseline * 100) if baseline > 0 else None
        return {**row, "anomaly_score": anomaly_score, "source": "alerts table / ML4-style baseline evidence"}
    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(status_code=503, detail=f"Anomaly evidence unavailable: {error}") from error
    finally:
        if connection is not None:
            connection.close()


@app.get("/network/grid/{grid_id}/nearby-hotspots")
def get_nearby_hotspots(grid_id: int, limit: int = Query(default=5, ge=1, le=20)):
    origin = centroid(feature_for(grid_id))
    candidates = []
    for feature in load_features():
        candidate_id = int(feature.get("properties", {}).get("cellId", -1))
        if candidate_id == grid_id:
            continue
        point = centroid(feature)
        distance = ((origin["latitude"] - point["latitude"]) ** 2 + (origin["longitude"] - point["longitude"]) ** 2) ** 0.5
        candidates.append({"grid_id": candidate_id, "centroid": point, "distance_degrees": round(distance, 5)})
    return {"grid_id": grid_id, "nearby_grids": sorted(candidates, key=lambda item: item["distance_degrees"])[:limit], "source": "grid geometry only; activity must be retrieved separately"}


@app.get("/pipeline/status")
def get_pipeline_status():
    outputs = [
        PROJECT_ROOT / "dataset" / "output" / "hourly_grid_summary" / "_SUCCESS",
        PROJECT_ROOT / "dataset" / "output" / "ml2_features" / "_SUCCESS",
        PROJECT_ROOT / "dataset" / "output" / "ml4_anomalies" / "_SUCCESS",
        PROJECT_ROOT / "dataset" / "output" / "network_risk_scores" / "_SUCCESS",
    ]
    checks = [{"output": str(path.relative_to(PROJECT_ROOT)), "available": path.exists(), "last_modified": datetime.fromtimestamp(path.stat().st_mtime).isoformat() if path.exists() else None} for path in outputs]
    return {"status": "healthy" if all(item["available"] for item in checks) else "partial", "checks": checks}
