---
name: noc-ops-standards
description: Apply the telecom NOC assignment rules, terminology constraints, verification commands and approved MCP usage.
---

# NOC Ops Standards

Use this skill for the telecom Network Operations and Predictive Intelligence assignment.

## Non-Negotiable Rules

- Do not equate high activity with confirmed congestion.
- Treat high activity, anomaly scores and risk scores as investigation signals.
- The canonical grain is one grid per hourly timestamp.
- Join geography on `properties.cellId`.
- Activity values are not counts, packets, users or MB.
- Do not invent root causes from activity data alone.
- Use live API or MCP evidence when the task asks about current network state.
- Name the source tool or endpoint for every operational figure.
- Report missing data, failed APIs and unavailable model outputs as evidence gaps.

## Repository Landmarks

- Spark pipeline: `pyspark/sp7.py`
- Airflow DAGs: `airflow/dags/dag2.py`, `airflow/dags/dag3.py`
- API assembly: `fastapi/mainapi.py`
- Network summary API: `fastapi/api1.py`
- Grid activity API: `fastapi/ap2.py`
- Hotspots and alerts API: `fastapi/api3.py`
- Grid feature API: `fastapi/api4.py`
- Risk prediction API: `fastapi/api5.py`
- Operational evidence API: `fastapi/api6.py`
- ML scoring: `ml/ml6.py`
- Dashboard: `noc-dashboard/src/App.jsx`

## Standard Commands

Run focused checks after changing CP, API or dashboard code:

```powershell
python -m py_compile cp\cp12_mcp_server.py cp\cp13_verify_plugin.py
python cp\cp12_mcp_server.py --self-test
python cp\cp13_verify_plugin.py
python -m py_compile fastapi\mainapi.py fastapi\api6.py
```

For dashboard changes:

```powershell
cd noc-dashboard
npm run build
```

## Approved MCP

Use only the local MCP server configured in `.mcp.json`. It wraps the existing FastAPI APIs and must not directly query MySQL, Parquet or GeoJSON.

Approved tools include:

- `get_network_summary`
- `get_hotspots`
- `get_grid_metrics`
- `get_grid_features`
- `get_grid_location`
- `get_nearby_hotspots`
- `get_alerts`
- `get_anomaly_score`
- `get_highest_anomaly_scores`
- `get_pipeline_status`

## Ownership And Versioning

- Plugin owner: Telecom NOC Analytics Team.
- Current version: `0.1.0`.
- Increment patch version for documentation/rule updates.
- Increment minor version for new tools, commands or MCP resources.
- Increment major version only for breaking changes to tool names, project rules or expected workflow.
