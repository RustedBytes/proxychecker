"""Render the measured Rust line coverage as a badge and CI progress bar."""

import json
import os
import sys
from pathlib import Path

report_path = Path(sys.argv[1])
report = json.loads(report_path.read_text())
lines = report["data"][0]["totals"]["lines"]
percent = lines["percent"]
covered, count = lines["covered"], lines["count"]
color = "#4c1" if percent >= 90 else "#e05d44"
label = f"{percent:.2f}%"
svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="150" height="20" role="img" aria-label="coverage: {label}">
<rect width="90" height="20" fill="#555"/><rect x="90" width="60" height="20" fill="{color}"/>
<g fill="white" font-family="Verdana, sans-serif" font-size="11" text-anchor="middle">
<text x="45" y="14">Rust coverage</text><text x="120" y="14">{label}</text></g></svg>'''
report_path.with_name("coverage.svg").write_text(svg)
filled = min(20, round(percent / 5))
bar = "▰" * filled + "▱" * (20 - filled)
summary = (
    f"### Rust line coverage: {label}\n\n"
    f"{bar} **{label}** — required: **90%**\n\n"
    f"{covered}/{count} production Rust lines covered by unit and Python API tests.\n"
    "All production modules are included; test code and dependencies are excluded.\n"
)
print(summary)
if summary_path := os.environ.get("GITHUB_STEP_SUMMARY"):
    with Path(summary_path).open("a") as output:
        output.write(summary)
