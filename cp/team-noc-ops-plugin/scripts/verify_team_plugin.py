"""Verify the CP13 team plugin package."""

import json
from pathlib import Path


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
MANIFEST = PLUGIN_ROOT / ".codex-plugin" / "plugin.json"
SKILL = PLUGIN_ROOT / "skills" / "noc-ops-standards" / "SKILL.md"
MCP = PLUGIN_ROOT / ".mcp.json"


def require(path):
    if not path.exists():
        raise SystemExit(f"Missing required plugin asset: {path}")


def main():
    require(MANIFEST)
    require(SKILL)
    require(MCP)

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    mcp = json.loads(MCP.read_text(encoding="utf-8"))
    skill_text = SKILL.read_text(encoding="utf-8")

    expected_rules = [
        "Do not equate high activity with confirmed congestion.",
        "The canonical grain is one grid per hourly timestamp.",
        "Join geography on `properties.cellId`.",
        "Activity values are not counts, packets, users or MB.",
    ]

    for rule in expected_rules:
        if rule not in skill_text:
            raise SystemExit(f"Missing rule in skill: {rule}")

    if manifest["name"] != "team-noc-ops-plugin":
        raise SystemExit("Plugin manifest name is incorrect.")
    if "telecom-noc" not in mcp.get("mcpServers", {}):
        raise SystemExit("Approved telecom-noc MCP server is missing.")

    print("CP13 plugin verification passed.")
    print(f"Plugin: {manifest['name']} {manifest['version']}")
    print("Assets: rules, skill, commands, hook checklist, approved MCP configuration")


if __name__ == "__main__":
    main()
