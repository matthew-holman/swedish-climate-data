#!/usr/bin/env python3
"""
run_pipeline.py — SMHI pipeline orchestrator.

Runs all four pipeline steps in order, stopping immediately if any step fails.

Steps:
    1. fetch_stations.py      — fetch qualifying SMHI weather stations
    2. fetch_observations.py  — download observations from SMHI API  (default)
       fetch_fake_observations.py — generate synthetic observations  (--fake)
    3. derive_normals.py      — derive per-station climate normals
    4. validate.py            — validate output before committing

Usage:
    python smhi/run_pipeline.py [--fake]

Arguments:
    --fake   Use synthetic observations (fetch_fake_observations.py) instead
             of fetching real data from the SMHI API. Use this while the SMHI
             corrected-archive endpoint is unavailable, or for development.
"""

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent

# ─── Args ─────────────────────────────────────────────────────────────────────

use_fake = "--fake" in sys.argv[1:]

obs_script = (
    Path("smhi/fetch_fake_observations.py")
    if use_fake
    else Path("smhi/fetch_observations.py")
)

STEPS = [
    ("Step 1 — Fetch stations",     Path("smhi/fetch_stations.py")),
    ("Step 2 — Fetch observations", obs_script),
    ("Step 3 — Derive normals",     Path("smhi/derive_normals.py")),
    ("Step 4 — Validate",           Path("smhi/validate.py")),
]

# ─── Runner ───────────────────────────────────────────────────────────────────

def run_step(label: str, script: Path) -> bool:
    print(f"\n{'━' * 50}")
    print(f"  {label}")
    print(f"  {script}")
    print(f"{'━' * 50}\n")

    env = {**os.environ, "PYTHONPATH": str(ROOT)}
    result = subprocess.run([sys.executable, str(script)], env=env)
    return result.returncode == 0


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    mode = "synthetic observations (--fake)" if use_fake else "real SMHI observations"
    print(f"SMHI pipeline — {mode}")

    for label, script in STEPS:
        if not run_step(label, script):
            print(f"\nPipeline aborted at: {label}", file=sys.stderr)
            print("Fix the error above and re-run, or run the step individually.", file=sys.stderr)
            sys.exit(1)

    print(f"\n{'━' * 50}")
    print("  Pipeline complete")
    print(f"{'━' * 50}")
    print("\nOutput files ready to commit:")
    print("  output/weather_stations.json")


if __name__ == "__main__":
    main()
