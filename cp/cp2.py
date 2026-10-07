"""CP Activity 2: Claude tool-using NOC assistant demo.

This CP demo calls the existing FastAPI service as its live evidence source.
Set ANTHROPIC_API_KEY, start the FastAPI service, then run this script.
"""

import json
import os
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


API_BASE_URL = os.getenv("NOC_API_BASE_URL", "http://127.0.0.1:8000")
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-5")
MAX_TOOL_ROUNDS = 8


TOOLS = [
    {"name": "get_network_summary", "description": "Get the current network-wide summary from API1.", "input_schema": {"type": "object", "properties": {}}},
    {"name": "get_grid_activity", "description": "Get the recent activity history for one grid from API2.", "input_schema": {"type": "object", "properties": {"grid_id": {"type": "integer", "minimum": 1}}, "required": ["grid_id"]}},
    {"name": "get_hotspots", "description": "Get high-activity grids from API3.", "input_schema": {"type": "object", "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 25}}}},
    {"name": "get_grid_features", "description": "Get stored ML features for one grid from API4.", "input_schema": {"type": "object", "properties": {"grid_id": {"type": "integer", "minimum": 1}}, "required": ["grid_id"]}},
    {"name": "get_anomaly_score", "description": "Get a grid anomaly score from an ML4/ML6 API endpoint, when deployed.", "input_schema": {"type": "object", "properties": {"grid_id": {"type": "integer", "minimum": 1}, "timestamp": {"type": "string"}}, "required": ["grid_id"]}},
    {"name": "get_grid_location", "description": "Get grid location from API6, when deployed.", "input_schema": {"type": "object", "properties": {"grid_id": {"type": "integer", "minimum": 1}}, "required": ["grid_id"]}},
    {"name": "get_pipeline_status", "description": "Get pipeline status from API6, when deployed.", "input_schema": {"type": "object", "properties": {}}},
    {"name": "get_nearby_hotspots", "description": "Get nearby hotspots from API6, when deployed.", "input_schema": {"type": "object", "properties": {"grid_id": {"type": "integer", "minimum": 1}}, "required": ["grid_id"]}},
]


def read_json(url):
    """Call a live local API and return a transparent success/failure result."""
    try:
        with urlopen(url, timeout=12) as response:
            return {"ok": True, "data": json.loads(response.read().decode("utf-8"))}
    except HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        return {"ok": False, "error": f"HTTP {error.code}: {body[:500]}"}
    except (URLError, TimeoutError) as error:
        return {"ok": False, "error": f"API unavailable: {error}"}


def local_tool(tool_name, values, evidence_log):
    """Map Claude's tools directly to project endpoints; no cached data is used."""
    grid_id = values.get("grid_id")
    limit = values.get("limit", 10)
    routes = {
        "get_network_summary": "/network/summary",
        "get_grid_activity": f"/network/grid/{grid_id}",
        "get_hotspots": f"/network/hotspots?{urlencode({'limit': limit})}",
        "get_grid_features": f"/network/grid/{grid_id}/features",
        # If any endpoint is unavailable, that failure is passed to Claude as evidence.
        "get_anomaly_score": f"/network/grid/{grid_id}/anomaly?{urlencode({'timestamp': values.get('timestamp', '')})}",
        "get_grid_location": f"/network/grid/{grid_id}/location",
        "get_pipeline_status": "/pipeline/status",
        "get_nearby_hotspots": f"/network/grid/{grid_id}/nearby-hotspots",
    }
    result = read_json(f"{API_BASE_URL}{routes[tool_name]}")
    evidence_log.append({"tool": tool_name, "ok": result["ok"]})
    return result


def claude_request(messages, require_tool=False):
    key = os.getenv("ANTHROPIC_API_KEY")
    if not key:
        raise RuntimeError("ANTHROPIC_API_KEY is not set. The Claude API cannot be called.")
    payload = {
        "model": MODEL,
        "max_tokens": 900,
        "system": (
            "You are a telecom NOC assistant. You MUST call relevant tools before answering "
            "any question about current network state; never use assumed or stale data. Use only "
            "tool results as evidence. If a tool fails or data is unavailable, explicitly report the "
            "gap and do not infer its missing value. In the final answer, attach a source in square "
            "brackets to every factual figure or claim, for example [get_hotspots]. Do not diagnose "
            "a root cause from activity data alone."
        ),
        "tools": TOOLS,
        "messages": messages,
    }
    if require_tool:
        payload["tool_choice"] = {"type": "any"}
    request = Request(
        ANTHROPIC_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"content-type": "application/json", "x-api-key": key, "anthropic-version": "2023-06-01"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=45) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        raise RuntimeError(f"Claude API error {error.code}: {error.read().decode('utf-8', errors='replace')[:500]}") from error


class NocConversation:
    def __init__(self):
        self.messages = []
        self.evidence_log = []

    def ask(self, question):
        self.messages.append({"role": "user", "content": question})
        require_tool = True
        for _ in range(MAX_TOOL_ROUNDS):
            response = claude_request(self.messages, require_tool=require_tool)
            require_tool = False
            content = response["content"]
            tool_uses = [block for block in content if block["type"] == "tool_use"]
            self.messages.append({"role": "assistant", "content": content})
            if not tool_uses:
                text = "\n".join(block["text"] for block in content if block["type"] == "text")
                sources = ", ".join(f"{item['tool']} ({'ok' if item['ok'] else 'failed'})" for item in self.evidence_log)
                return f"{text}\n\nEvidence-tool audit: {sources or 'No tool was called.'}"
            tool_results = []
            for tool_use in tool_uses:
                result = local_tool(tool_use["name"], tool_use["input"], self.evidence_log)
                tool_results.append({"type": "tool_result", "tool_use_id": tool_use["id"], "content": json.dumps(result)})
            self.messages.append({"role": "user", "content": tool_results})
        return "Unable to complete the answer: the assistant exceeded the allowed tool-call rounds."


def answer(question):
    conversation = NocConversation()
    return conversation.ask(question)


if __name__ == "__main__":
    try:
        conversation = NocConversation()
        prompt = " ".join(sys.argv[1:])
        if prompt:
            print(conversation.ask(prompt))
        else:
            first_question = "Which areas need attention right now?"
            follow_up = "Explain Grid 4821."
            print(f"Engineer: {first_question}")
            print(conversation.ask(first_question))
            print()
            print(f"Engineer: {follow_up}")
            print(conversation.ask(follow_up))
    except RuntimeError as error:
        print(f"Setup required: {error}")
        print("Set ANTHROPIC_API_KEY in your shell, then run this script again.")
