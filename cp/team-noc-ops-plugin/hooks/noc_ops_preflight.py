"""Hook-style preflight checklist for the NOC project.

This file documents the safeguards a team hook should enforce before work:
- keep CP artifacts in cp/
- preserve existing code unless the activity needs integration
- require source citations for operational figures
- reject wording that treats high activity as confirmed congestion
"""


BLOCKED_PHRASES = [
    "confirmed congestion",
    "traffic volume in mb",
    "number of users",
    "root cause is",
]


def check_text(text):
    lowered = text.lower()
    return [phrase for phrase in BLOCKED_PHRASES if phrase in lowered]
