# ============================================================
# api5.py
# ML5 — Risk Prediction API
# ============================================================

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
import os
import joblib
import numpy as np


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    title="Telecom Network Analytics API",
    version="1.0"
)


# ============================================================
# MODEL CONFIGURATION
# ============================================================

MODEL_PATH = r"C:\GraddedAssignment\dataset\output\ml5_model\decision_tree.joblib"

MODEL_VERSION = "decision-tree-v1"


# ============================================================
# LOAD MODEL AT SERVICE STARTUP
# ============================================================

model = None


try:

    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(
            f"ML5 model artifact not found: {MODEL_PATH}"
        )

    model = joblib.load(MODEL_PATH)

    print("ML5 model loaded successfully")
    print("Model:", MODEL_PATH)

except Exception as error:

    print(
        f"ML5 STARTUP FAILURE: {error}"
    )

    raise RuntimeError(
        f"ML5 model could not be loaded: {error}"
    )


# ============================================================
# REQUEST MODEL
# ============================================================

class RiskPredictionRequest(BaseModel):

    grid_id: int = Field(
        ...,
        ge=1,
        le=10000,
        description="Grid ID"
    )

    avg_activity: float = Field(
        ...,
        ge=0
    )

    activity_growth: float

    active_hours: int = Field(
        ...,
        ge=0,
        le=24
    )

    peak_ratio: float = Field(
        ...,
        ge=0
    )

    variability: float = Field(
        ...,
        ge=0
    )

    internet_share: float = Field(
        ...,
        ge=0,
        le=1
    )

    feature_timestamp: datetime


# ============================================================
# RESPONSE MODEL
# ============================================================

class RiskPredictionResponse(BaseModel):

    grid_id: int

    risk_score: float

    risk_level: str

    model_version: str

    feature_timestamp: datetime

    explanation_note: str

    contributing_features: Optional[list[str]] = None


# ============================================================
# PREDICTION ENDPOINT
# ============================================================

@app.post(
    "/network/predict-risk",
    response_model=RiskPredictionResponse
)
def predict_risk(
    request: RiskPredictionRequest
):

    try:

        # ----------------------------------------------------
        # ML2 FEATURE ORDER
        # ----------------------------------------------------

        features = np.array([[
            request.avg_activity,
            request.activity_growth,
            request.active_hours,
            request.peak_ratio,
            request.variability,
            request.internet_share
        ]])

        # ----------------------------------------------------
        # MODEL PREDICTION
        # ----------------------------------------------------

        prediction = model.predict(features)[0]

        # ----------------------------------------------------
        # RISK SCORE
        #
        # Decision Tree normally provides probability
        # through predict_proba().
        # ----------------------------------------------------

        if hasattr(model, "predict_proba"):

            probabilities = model.predict_proba(features)[0]

            classes = list(model.classes_)

            if 1 in classes:

                risk_score = float(
                    probabilities[
                        classes.index(1)
                    ]
                )

            else:

                risk_score = 0.0

        else:

            risk_score = float(prediction)

        # ----------------------------------------------------
        # RISK LEVEL
        # ----------------------------------------------------

        if risk_score >= 0.70:

            risk_level = "high"

        elif risk_score >= 0.40:

            risk_level = "medium"

        else:

            risk_level = "low"

        # ----------------------------------------------------
        # CONTRIBUTING FEATURES
        #
        # Decision Tree feature importance is available.
        # This identifies the globally important features.
        # ----------------------------------------------------

        contributing_features = None

        if hasattr(model, "feature_importances_"):

            feature_names = [
                "avg_activity",
                "activity_growth",
                "active_hours",
                "peak_ratio",
                "variability",
                "internet_share"
            ]

            importances = model.feature_importances_

            ranked_features = sorted(
                zip(
                    feature_names,
                    importances
                ),
                key=lambda x: x[1],
                reverse=True
            )

            contributing_features = [
                name
                for name, importance in ranked_features
                if importance > 0
            ][:3]

        # ----------------------------------------------------
        # RESPONSE
        # ----------------------------------------------------

        return RiskPredictionResponse(

            grid_id=request.grid_id,

            risk_score=round(
                risk_score,
                4
            ),

            risk_level=risk_level,

            model_version=MODEL_VERSION,

            feature_timestamp=request.feature_timestamp,

            explanation_note=(
                "Risk prediction generated using "
                "the trained ML5 Decision Tree model."
            ),

            contributing_features=contributing_features
        )

    except Exception as error:

        raise HTTPException(
            status_code=500,
            detail=f"Risk prediction failed: {error}"
        )