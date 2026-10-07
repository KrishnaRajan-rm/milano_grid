# Milano Grid

Milano Grid is a data engineering and network operations project for processing Milan telecommunications activity, enriching it with grid geography, scoring operational risk, and serving results through a FastAPI API and React dashboard.

## Repository layout

- `de/`, `pyspark/`, `ml/`, `np/`: ingestion, Spark processing, machine-learning, and network-processing scripts
- `fastapi/`: API services and alert endpoints
- `airflow/dags/`: Airflow orchestration DAGs
- `noc-dashboard/`: React/Vite operations dashboard
- `dataset/reference/`: versioned geographic reference data
- `cp/`: supporting plugin and operations material

## Local data policy

Large raw inputs, landing tables, Spark outputs, logs, and model artifacts are intentionally excluded from Git because they are generated or too large for a practical GitHub repository. The pipeline scripts expect those directories to exist locally and can recreate them from the input data.

The tracked dashboard reference copy is under `noc-dashboard/public/reference/`. Dependencies are installed from `noc-dashboard/package.json`.

## Dashboard

```powershell
cd noc-dashboard
npm install
npm run dev
```

## API

Run the FastAPI entrypoint from the repository root using the project environment and the command appropriate to the local deployment setup.
