# CP4 - Claude Code Repository Guide

This file is the project guide for Claude Code. It explains the repository structure, data flow, project rules, terminology constraints, and missing tests/documentation to propose before making changes.

## Repository Structure

- `dataset/raw/`: source Milano telecom activity CSV files.
- `dataset/reference/milano-grid.geojson`: canonical grid geography reference.
- `dataset/output/`: generated Spark/ML outputs used by the APIs and dashboard.
- `dataset/landing/`: intermediate CSV outputs and alert exports.
- `pyspark/`: Spark processing jobs. The main end-to-end Spark pipeline is `pyspark/sp7.py`.
- `airflow/dags/`: orchestration DAGs. `dag2.py` covers landing-to-raw movement and validation; `dag3.py` runs the Spark processing flow.
- `de/`: data engineering scripts for validation and warehouse loading, including `de2.py` and `de6.py`.
- `ml/`: machine-learning feature engineering, anomaly detection, model training and batch scoring. Important files include `ml2.py`, `ml4.py` and `ml6.py`.
- `fastapi/`: backend API services. `mainapi.py` mounts the individual API modules.
- `noc-dashboard/`: React/Vite network operations dashboard.
- `cp/`: Claude/CP phase assignment artifacts and demos.

## Data Flow

1. Raw Milano activity CSV files are read from `dataset/raw/`.
2. The Spark pipeline validates, cleans and standardizes activity data.
3. The canonical processing grain is one grid per hourly timestamp.
4. Grid geography is joined from `dataset/reference/milano-grid.geojson` using `properties.cellId`.
5. Spark writes clean activity, hourly summaries, enriched activity, dashboard summaries and operational attention outputs to `dataset/output/`.
6. ML scripts build features, anomaly outputs and risk scores from the processed activity.
7. Data engineering scripts load curated outputs into the MySQL warehouse.
8. FastAPI endpoints read from MySQL, GeoJSON and model artifacts.
9. The React dashboard calls FastAPI routes to show summary metrics, grid activity, hotspots, alerts and risk predictions.

## Important Files

- Spark pipeline: `pyspark/sp7.py`
- Airflow DAGs: `airflow/dags/dag2.py`, `airflow/dags/dag3.py`
- API route assembly: `fastapi/mainapi.py`
- API1 network summary: `fastapi/api1.py`
- API2 grid activity: `fastapi/ap2.py`
- API3 hotspots and alerts: `fastapi/api3.py`
- API4 grid features: `fastapi/api4.py`
- API5 risk prediction: `fastapi/api5.py`
- API6 operational evidence: `fastapi/api6.py`
- ML2 feature engineering: `ml/ml2.py`
- ML4 anomaly evidence: `ml/ml4.py`
- ML6 batch risk scoring: `ml/ml6.py`
- React dashboard: `noc-dashboard/src/App.jsx`
- Dashboard styling: `noc-dashboard/src/App.css`
- API base config: `noc-dashboard/src/config.js`

## Non-Negotiable Project Rules

- Do not equate high activity with confirmed congestion.
- Treat high activity, anomaly scores and model risk as investigation signals only.
- The canonical grain is one grid per hourly timestamp.
- Join geography on `properties.cellId`.
- Activity values are not counts, packets, users or MB.
- Do not invent root causes from activity data alone.
- Do not use stale prompt data when a live API/tool is available.
- Keep model explanations evidence-grounded and name the source of each figure.
- Report missing or failed evidence clearly instead of hiding it.
- Preserve existing code unless a requested activity truly requires a change.

## Terminology Constraints

Use these terms carefully:

- "activity": aggregated telecom usage signal from SMS, calls and internet fields.
- "grid": Milano spatial cell identified by `grid_id` / `cellId`.
- "hotspot": high-activity grid in the selected reporting window.
- "alert": operational signal created by comparison to baseline or threshold logic.
- "risk score": ML output estimating likelihood of future high-activity risk.
- "anomaly score": comparison signal against baseline behavior.
- "pipeline status": availability and freshness of generated data outputs.

Avoid these unsupported claims unless separate evidence exists:

- "confirmed congestion"
- "network outage"
- "customer impact"
- "capacity failure"
- "fault root cause"
- "traffic volume in MB"
- "number of users"

## Claude Code Working Rules

Before modifying files:

1. Inspect the relevant files first.
2. Explain whether the activity is documentation, code or both.
3. Keep documentation-only work in the requested artifact.
4. For code activities, prefer the existing FastAPI, Spark, ML and React structure.
5. Do not create isolated environments unless explicitly requested.
6. Verify with a focused syntax check or runnable command when possible.
7. If a change would alter the main application behavior substantially, ask for approval first.

## Missing Tests To Propose

- Spark pipeline test for the one-grid-per-hour grain.
- Spark validation test for required activity columns and invalid/null grid IDs.
- GeoJSON join test proving `properties.cellId` maps to `grid_id`.
- API1 test for `/network/summary` shape and empty-data behavior.
- API2 test for `/network/grid/{grid_id}` with valid, invalid and filtered requests.
- API3 test for `/network/hotspots` ordering and `/network/alerts` severity filtering.
- API4 test for feature quality and missing-feature fallback.
- API5 test for risk prediction request validation and stable risk-level thresholds.
- API6 test for grid location, anomaly fallback and pipeline status output.
- Dashboard test for API failure states and risk form behavior.
- Claude tool assistant test proving live tools are called before answering.
- Context-engineering test proving old evidence is summarized before prompt insertion.

## Missing Documentation To Propose

- One-command local run guide covering backend, dashboard and required data services.
- Data dictionary for activity fields and units/meaning.
- API contract document listing request/response examples for API1 through API6.
- ML model card for the decision-tree risk model.
- Anomaly/risk interpretation guide for NOC users.
- Pipeline output inventory with path, producer and consumer for each dataset.
- Troubleshooting guide for MySQL, model artifact, Spark output and API failures.
- Claude integration guide covering tool use, evidence references and uncertainty handling.

## Claude Prompt For Repository Explanation

Use this prompt when asking Claude Code to explain the repository without changing files:

"Read the repository and explain the structure and data flow. Locate the Spark pipeline, Airflow DAGs, API routes, ML scoring code and React pages. Do not modify any files. Respect these rules: high activity is not confirmed congestion; the canonical grain is one grid per hourly timestamp; geography joins use properties.cellId; activity values are not counts or MB. Propose missing tests and documentation after the explanation."
