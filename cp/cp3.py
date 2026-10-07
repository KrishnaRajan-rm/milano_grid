"""CP Activity 3: context engineering demo for grid explanations.

The script gathers live project evidence, builds a raw "dump everything"
context and a smaller curated context, then optionally sends both to Claude.
It is safe to run without a Claude key: it prints the collected context and
the setup gap instead of crashing.
"""

import json
import os
from statistics import mean
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


API_BASE_URL = os.getenv("NOC_API_BASE_URL", "http://127.0.0.1:8000")
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-5")
DEFAULT_GRID_ID = int(os.getenv("CP3_GRID_ID", "4821"))


def request_json(path, method="GET", payload=None):
    url = f"{API_BASE_URL}{path}"
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"content-type": "application/json"} if payload is not None else {}
    request = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=12) as response:
            return {"ok": True, "source": path, "data": json.loads(response.read().decode("utf-8"))}
    except HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        return {"ok": False, "source": path, "error": f"HTTP {error.code}: {body[:500]}"}
    except (URLError, TimeoutError) as error:
        return {"ok": False, "source": path, "error": f"API unavailable: {error}"}


def collect_evidence(grid_id):
    evidence = {
        "network_summary": request_json("/network/summary"),
        "grid_activity": request_json(f"/network/grid/{grid_id}"),
        "hotspots": request_json(f"/network/hotspots?{urlencode({'limit': 10})}"),
        "grid_features": request_json(f"/network/grid/{grid_id}/features"),
        "prior_alerts": request_json(f"/network/alerts?{urlencode({'limit': 25})}"),
        "anomaly_score": request_json(f"/network/grid/{grid_id}/anomaly"),
        "grid_location": request_json(f"/network/grid/{grid_id}/location"),
        "pipeline_status": request_json("/pipeline/status"),
        "nearby_hotspots": request_json(f"/network/grid/{grid_id}/nearby-hotspots"),
    }

    features = evidence["grid_features"]
    if features["ok"]:
        feature_data = features["data"]
        risk_payload = {
            "grid_id": grid_id,
            "avg_activity": feature_data["avg_activity"],
            "activity_growth": feature_data["activity_growth"],
            "active_hours": feature_data["active_hours"],
            "peak_ratio": feature_data["peak_ratio"],
            "variability": feature_data["variability"],
            "internet_share": feature_data["internet_share"],
            "feature_timestamp": feature_data["feature_timestamp"],
        }
        evidence["model_score"] = request_json("/network/predict-risk", method="POST", payload=risk_payload)
    else:
        evidence["model_score"] = {
            "ok": False,
            "source": "/network/predict-risk",
            "error": "Skipped because grid features were unavailable.",
        }

    return evidence


def latest_activity(activity_result):
    if not activity_result["ok"]:
        return None
    rows = activity_result["data"].get("data", [])
    return rows[-1] if rows else None


def summarize_older_evidence(grid_id, evidence):
    summary = {"grid_id": grid_id, "activity_history_summary": None, "alert_history_summary": None}

    activity = evidence["grid_activity"]
    if activity["ok"]:
        rows = activity["data"].get("data", [])
        older_rows = rows[:-1]
        if older_rows:
            values = [float(row["total_activity"]) for row in older_rows]
            summary["activity_history_summary"] = {
                "points_summarized": len(older_rows),
                "oldest_timestamp": older_rows[0]["timestamp"],
                "newest_historical_timestamp": older_rows[-1]["timestamp"],
                "min_total_activity": min(values),
                "max_total_activity": max(values),
                "avg_total_activity": round(mean(values), 2),
                "trend_hint": "rising" if values[-1] > values[0] else "falling_or_flat",
            }

    alerts = evidence["prior_alerts"]
    if alerts["ok"]:
        rows = alerts["data"].get("results", [])
        grid_alerts = [row for row in rows if int(row.get("grid_id", -1)) == grid_id]
        summary["alert_history_summary"] = {
            "alerts_checked": len(rows),
            "matching_grid_alerts": len(grid_alerts),
            "matching_alert_types": sorted({row.get("severity") or row.get("status") for row in grid_alerts if row.get("severity") or row.get("status")}),
            "note": "Endpoint returns recent alert slice, not the full lifetime history.",
        }

    return summary


def evidence_gaps(evidence):
    gaps = []
    for name, result in evidence.items():
        if not result["ok"]:
            gaps.append({"evidence": name, "source": result["source"], "gap": result["error"]})
    return gaps


def build_dump_context(grid_id, evidence):
    return {
        "instruction": "Raw dump context. This intentionally includes every collected item for comparison.",
        "grid_id": grid_id,
        "all_collected_evidence": evidence,
    }


def build_curated_context(grid_id, evidence):
    current_activity = latest_activity(evidence["grid_activity"])
    history_summary = summarize_older_evidence(grid_id, evidence)
    return {
        "instruction": "Curated context. Use this instead of dumping full raw history into the active prompt.",
        "grid_id": grid_id,
        "current_evidence": {
            "network_summary": evidence["network_summary"],
            "current_grid_activity": current_activity,
            "grid_features": evidence["grid_features"],
            "model_score": evidence["model_score"],
            "anomaly_score": evidence["anomaly_score"],
            "pipeline_quality_status": evidence["pipeline_status"],
            "grid_location": evidence["grid_location"],
        },
        "historical_evidence": history_summary,
        "uncertainty": evidence_gaps(evidence),
    }


def claude_analyze(context_package, label):
    key = os.getenv("ANTHROPIC_API_KEY")
    if not key:
        return "Claude call skipped: ANTHROPIC_API_KEY is not set."

    payload = {
        "model": MODEL,
        "max_tokens": 900,
        "system": (
            "You are a telecom NOC context-engineering evaluator. Use only supplied evidence. "
            "Separate the answer into CURRENT EVIDENCE, HISTORICAL EVIDENCE and UNCERTAINTY. "
            "Name the source object for each figure. If evidence is missing, say so directly."
        ),
        "messages": [
            {
                "role": "user",
                "content": f"Evaluate this {label} context package for Grid {context_package['grid_id']}:\n"
                f"{json.dumps(context_package, indent=2, default=str)}",
            }
        ],
    }
    request = Request(
        ANTHROPIC_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"content-type": "application/json", "x-api-key": key, "anthropic-version": "2023-06-01"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=45) as response:
            data = json.loads(response.read().decode("utf-8"))
            return "\n".join(block["text"] for block in data["content"] if block["type"] == "text")
    except HTTPError as error:
        return f"Claude call failed: HTTP {error.code}: {error.read().decode('utf-8', errors='replace')[:500]}"
    except (URLError, TimeoutError) as error:
        return f"Claude call failed: {error}"


def compare_contexts(dump_answer, curated_answer):
    return {
        "comparison_goal": "Check whether removing irrelevant raw context changes the answer.",
        "dump_everything_answer_available": not dump_answer.startswith("Claude call skipped"),
        "curated_answer_available": not curated_answer.startswith("Claude call skipped"),
        "expected_result": (
            "The curated answer should be shorter, clearer about current versus historical evidence, "
            "and more explicit about uncertainty. Any major change in severity should be investigated."
        ),
    }


def main():
    grid_id = DEFAULT_GRID_ID
    evidence = collect_evidence(grid_id)
    dump_context = build_dump_context(grid_id, evidence)
    curated_context = build_curated_context(grid_id, evidence)

    print("CP3 Context Engineering Demo")
    print(f"Grid: {grid_id}")
    print()
    print("Collected evidence status:")
    for name, result in evidence.items():
        print(f"- {name}: {'ok' if result['ok'] else 'gap'} ({result['source']})")

    print()
    print("Curated context package:")
    print(json.dumps(curated_context, indent=2, default=str))

    print()
    print("Claude comparison:")
    dump_answer = claude_analyze(dump_context, "dump everything")
    curated_answer = claude_analyze(curated_context, "curated")
    print("DUMP EVERYTHING ANSWER")
    print(dump_answer)
    print()
    print("CURATED CONTEXT ANSWER")
    print(curated_answer)
    print()
    print("CONTEXT REMOVAL EVALUATION")
    print(json.dumps(compare_contexts(dump_answer, curated_answer), indent=2))


if __name__ == "__main__":
    main()
