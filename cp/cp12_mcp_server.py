"""CP12 MCP server for the telecom NOC APIs.

This server wraps the existing FastAPI service. It does not read MySQL,
Parquet or GeoJSON directly, and it does not reimplement API1-API6.

Run a local smoke test:
    python cp\\cp12_mcp_server.py --self-test

Use as an MCP stdio server from Claude Code/assistant configuration:
    python cp\\cp12_mcp_server.py
"""

import argparse
import json
import os
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen


API_BASE_URL = os.getenv("NOC_API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
SERVER_NAME = "telecom-noc-mcp"
PROTOCOL_VERSION = "2024-11-05"


TOOLS = [
    {
        "name": "get_network_summary",
        "description": "Call API1 /network/summary for the current network summary.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "get_hotspots",
        "description": "Call API3 /network/hotspots for high-activity grids.",
        "inputSchema": {
            "type": "object",
            "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 50}},
            "additionalProperties": False,
        },
    },
    {
        "name": "get_grid_metrics",
        "description": "Call API2 /network/grid/{grid_id} for recent grid activity metrics.",
        "inputSchema": {
            "type": "object",
            "properties": {"grid_id": {"type": "integer", "minimum": 1, "maximum": 10000}},
            "required": ["grid_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_grid_features",
        "description": "Call API4 /network/grid/{grid_id}/features for ML feature metrics.",
        "inputSchema": {
            "type": "object",
            "properties": {"grid_id": {"type": "integer", "minimum": 1, "maximum": 10000}},
            "required": ["grid_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_grid_location",
        "description": "Call API6 /network/grid/{grid_id}/location for grid geometry and centroid.",
        "inputSchema": {
            "type": "object",
            "properties": {"grid_id": {"type": "integer", "minimum": 1, "maximum": 10000}},
            "required": ["grid_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_nearby_hotspots",
        "description": "Call API6 /network/grid/{grid_id}/nearby-hotspots.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "grid_id": {"type": "integer", "minimum": 1, "maximum": 10000},
                "limit": {"type": "integer", "minimum": 1, "maximum": 20},
            },
            "required": ["grid_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_alerts",
        "description": "Call API3 /network/alerts for prior/current operational alerts.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "minimum": 1, "maximum": 50},
                "severity": {"type": "string", "enum": ["High Activity", "ALL"]},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "get_anomaly_score",
        "description": "Call API6 /network/grid/{grid_id}/anomaly for one grid anomaly score.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "grid_id": {"type": "integer", "minimum": 1, "maximum": 10000},
                "timestamp": {"type": "string"},
            },
            "required": ["grid_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_highest_anomaly_scores",
        "description": "Call API3 alerts and rank returned grids by anomaly percentage from current/baseline evidence.",
        "inputSchema": {
            "type": "object",
            "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 50}},
            "additionalProperties": False,
        },
    },
    {
        "name": "get_pipeline_status",
        "description": "Call API6 /pipeline/status for pipeline completion/quality status.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
]


RESOURCE_TEMPLATES = [
    {
        "uriTemplate": "network://summary",
        "name": "Network Summary",
        "description": "Current API1 network summary.",
        "mimeType": "application/json",
    },
    {
        "uriTemplate": "network://hotspots",
        "name": "Hotspots",
        "description": "API3 high-activity grids.",
        "mimeType": "application/json",
    },
    {
        "uriTemplate": "network://alerts",
        "name": "Alerts",
        "description": "API3 operational alerts.",
        "mimeType": "application/json",
    },
    {
        "uriTemplate": "network://pipeline/status",
        "name": "Pipeline Status",
        "description": "API6 pipeline completion status.",
        "mimeType": "application/json",
    },
    {
        "uriTemplate": "network://grid/{grid_id}/metrics",
        "name": "Grid Metrics",
        "description": "API2 grid activity metrics.",
        "mimeType": "application/json",
    },
    {
        "uriTemplate": "network://grid/{grid_id}/location",
        "name": "Grid Location",
        "description": "API6 grid location.",
        "mimeType": "application/json",
    },
    {
        "uriTemplate": "network://grid/{grid_id}/nearby-hotspots",
        "name": "Nearby Hotspots",
        "description": "API6 nearby grid evidence.",
        "mimeType": "application/json",
    },
]


def check_base_url():
    parsed = urlparse(API_BASE_URL)
    allowed_hosts = {"127.0.0.1", "localhost"}
    if parsed.scheme != "http" or parsed.hostname not in allowed_hosts:
        raise ValueError("NOC_API_BASE_URL must be http://127.0.0.1:<port> or http://localhost:<port>.")


def validated_grid_id(arguments):
    grid_id = arguments.get("grid_id")
    if not isinstance(grid_id, int) or grid_id < 1 or grid_id > 10000:
        raise ValueError("grid_id must be an integer between 1 and 10000.")
    return grid_id


def validated_limit(arguments, default, maximum):
    limit = arguments.get("limit", default)
    if not isinstance(limit, int) or limit < 1 or limit > maximum:
        raise ValueError(f"limit must be an integer between 1 and {maximum}.")
    return limit


def api_json(path, method="GET", payload=None):
    check_base_url()
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"content-type": "application/json"} if payload is not None else {}
    request = Request(f"{API_BASE_URL}{path}", data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=12) as response:
            return {"ok": True, "api_path": path, "data": json.loads(response.read().decode("utf-8"))}
    except HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        return {"ok": False, "api_path": path, "error": f"HTTP {error.code}: {body[:500]}"}
    except (URLError, TimeoutError) as error:
        return {"ok": False, "api_path": path, "error": f"API unavailable: {error}"}


def ranked_anomaly_scores(limit):
    alerts = api_json(f"/network/alerts?{urlencode({'limit': limit})}")
    if not alerts["ok"]:
        return alerts

    ranked = []
    for row in alerts["data"].get("results", []):
        current = row.get("total_activity")
        reason = row.get("reason")
        anomaly_score = None
        if reason and "x times" in reason:
            marker = reason.split("x times", 1)[0].split()[-1]
            try:
                anomaly_score = round((float(marker) - 1.0) * 100.0, 4)
            except ValueError:
                anomaly_score = None
        ranked.append(
            {
                "grid_id": row.get("grid_id"),
                "timestamp": row.get("timestamp"),
                "current_activity": current,
                "severity": row.get("severity") or row.get("status"),
                "anomaly_score_percent": anomaly_score,
                "source": "API3 /network/alerts reason/current_activity fields",
                "interpretation_limit": "High anomaly/activity is an investigation signal, not confirmed congestion.",
            }
        )

    ranked.sort(key=lambda item: (-1 if item["anomaly_score_percent"] is None else item["anomaly_score_percent"]), reverse=True)
    return {"ok": True, "api_path": alerts["api_path"], "data": {"results": ranked[:limit]}}


def call_tool(name, arguments):
    arguments = arguments or {}
    if name == "get_network_summary":
        return api_json("/network/summary")
    if name == "get_hotspots":
        return api_json(f"/network/hotspots?{urlencode({'limit': validated_limit(arguments, 10, 50)})}")
    if name == "get_grid_metrics":
        return api_json(f"/network/grid/{validated_grid_id(arguments)}")
    if name == "get_grid_features":
        return api_json(f"/network/grid/{validated_grid_id(arguments)}/features")
    if name == "get_grid_location":
        return api_json(f"/network/grid/{validated_grid_id(arguments)}/location")
    if name == "get_nearby_hotspots":
        grid_id = validated_grid_id(arguments)
        limit = validated_limit(arguments, 5, 20)
        return api_json(f"/network/grid/{grid_id}/nearby-hotspots?{urlencode({'limit': limit})}")
    if name == "get_alerts":
        limit = validated_limit(arguments, 10, 50)
        query = {"limit": limit}
        severity = arguments.get("severity")
        if severity:
            query["severity"] = severity
        return api_json(f"/network/alerts?{urlencode(query)}")
    if name == "get_anomaly_score":
        grid_id = validated_grid_id(arguments)
        query = {}
        if arguments.get("timestamp"):
            query["timestamp"] = arguments["timestamp"]
        suffix = f"?{urlencode(query)}" if query else ""
        return api_json(f"/network/grid/{grid_id}/anomaly{suffix}")
    if name == "get_highest_anomaly_scores":
        return ranked_anomaly_scores(validated_limit(arguments, 10, 50))
    if name == "get_pipeline_status":
        return api_json("/pipeline/status")
    raise ValueError(f"Unknown tool: {name}")


def resource_to_tool(uri):
    if uri == "network://summary":
        return "get_network_summary", {}
    if uri == "network://hotspots":
        return "get_hotspots", {}
    if uri == "network://alerts":
        return "get_alerts", {}
    if uri == "network://pipeline/status":
        return "get_pipeline_status", {}

    parts = uri.split("/")
    if len(parts) >= 5 and parts[0] == "network:" and parts[2] == "grid":
        try:
            grid_id = int(parts[3])
        except ValueError as error:
            raise ValueError("Grid resource URI must include a numeric grid_id.") from error
        if parts[4] == "metrics":
            return "get_grid_metrics", {"grid_id": grid_id}
        if parts[4] == "location":
            return "get_grid_location", {"grid_id": grid_id}
        if parts[4] == "nearby-hotspots":
            return "get_nearby_hotspots", {"grid_id": grid_id}

    raise ValueError(f"Unknown resource URI: {uri}")


def mcp_result(result):
    return {"content": [{"type": "text", "text": json.dumps(result, indent=2, default=str)}], "isError": not result.get("ok", False)}


def handle_request(message):
    method = message.get("method")
    params = message.get("params") or {}

    if method == "initialize":
        return {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {"tools": {}, "resources": {}},
            "serverInfo": {"name": SERVER_NAME, "version": "1.0.0"},
        }
    if method == "tools/list":
        return {"tools": TOOLS}
    if method == "tools/call":
        return mcp_result(call_tool(params.get("name"), params.get("arguments") or {}))
    if method == "resources/list":
        return {
            "resources": [
                {"uri": "network://summary", "name": "Network Summary", "mimeType": "application/json"},
                {"uri": "network://hotspots", "name": "Hotspots", "mimeType": "application/json"},
                {"uri": "network://alerts", "name": "Alerts", "mimeType": "application/json"},
                {"uri": "network://pipeline/status", "name": "Pipeline Status", "mimeType": "application/json"},
            ]
        }
    if method == "resources/templates/list":
        return {"resourceTemplates": RESOURCE_TEMPLATES}
    if method == "resources/read":
        uri = params.get("uri")
        tool_name, arguments = resource_to_tool(uri)
        result = call_tool(tool_name, arguments)
        return {"contents": [{"uri": uri, "mimeType": "application/json", "text": json.dumps(result, indent=2, default=str)}]}

    raise ValueError(f"Unsupported MCP method: {method}")


def read_framed_message():
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        line = line.decode("ascii", errors="replace").strip()
        if line == "":
            break
        key, _, value = line.partition(":")
        headers[key.lower()] = value.strip()
    length = int(headers.get("content-length", "0"))
    if length <= 0:
        return None
    return json.loads(sys.stdin.buffer.read(length).decode("utf-8"))


def write_framed_message(message):
    body = json.dumps(message, separators=(",", ":"), default=str).encode("utf-8")
    sys.stdout.buffer.write(f"Content-Length: {len(body)}\r\n\r\n".encode("ascii"))
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()


def serve_stdio():
    while True:
        message = read_framed_message()
        if message is None:
            break
        if "id" not in message:
            continue
        try:
            result = handle_request(message)
            write_framed_message({"jsonrpc": "2.0", "id": message["id"], "result": result})
        except Exception as error:
            write_framed_message(
                {
                    "jsonrpc": "2.0",
                    "id": message["id"],
                    "error": {"code": -32000, "message": str(error)},
                }
            )


def self_test():
    checks = {
        "question_1_tool": call_tool("get_highest_anomaly_scores", {"limit": 5}),
        "question_2_tool": call_tool("get_pipeline_status", {}),
        "resources": handle_request({"method": "resources/templates/list"}),
        "security": "local API only; validated grid_id and limit; no arbitrary URL/file access; API errors are returned as gaps",
    }
    print(json.dumps(checks, indent=2, default=str))


def main():
    parser = argparse.ArgumentParser(description="CP12 MCP server for existing NOC FastAPI endpoints.")
    parser.add_argument("--self-test", action="store_true", help="Call the wrapped APIs once and print JSON.")
    args = parser.parse_args()
    if args.self_test:
        self_test()
    elif sys.stdin.isatty():
        parser.print_help()
    else:
        serve_stdio()


if __name__ == "__main__":
    main()
