"""CP13 clean-environment style verification entry point.

This verifies the packaged team plugin from the project root without creating
an isolated environment.
"""

from pathlib import Path
import runpy


PLUGIN_VERIFY = Path(__file__).resolve().parent / "team-noc-ops-plugin" / "scripts" / "verify_team_plugin.py"


if __name__ == "__main__":
    runpy.run_path(str(PLUGIN_VERIFY), run_name="__main__")
