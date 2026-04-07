#!/usr/bin/env python3
"""
fetch_stations.py — Step 1 of 4: fetch qualifying SMHI weather stations.

Fetches the station list for both parameters used in the SMHI pipeline:
  - Parameter 2  : daily mean temperature
  - Parameter 26 : daily minimum temperature

Keeps only stations that appear in both parameter lists AND have records
covering the pipeline's climate window (current year − 15 through current
year − 1). Using a recent rolling window rather than a fixed WMO normal
period maximises station coverage and ensures data reflects present-day
climate. Stations missing either parameter cannot produce complete climate
normals and are excluded.

Output:
    data/stations.json — array of station objects, one per qualifying station

Each record contains:
    id          — SMHI station ID (integer)
    name        — station name (string)
    lat         — latitude (float, WGS84)
    lng         — longitude (float, WGS84)
    elevationM  — station elevation in metres (float)

This file is the input for Step 2 (fetch_observations.py).
data/ is gitignored — this file is never committed.

Usage:
    python smhi/fetch_stations.py
    make smhi-stations
"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ─── Constants ────────────────────────────────────────────────────────────────

BASE_URL = "https://opendata-download-metobs.smhi.se/api/version/1.0"

# Parameters we need — both must be present for a station to qualify
PARAM_MEAN = 2   # daily mean temperature
PARAM_MIN = 26   # daily minimum temperature

# Climate window: current year − 15 through current year − 1.
# Derived at runtime so the script stays current without manual updates.
# Using a recent window rather than the WMO 1991–2020 normal period
# maximises station coverage and reflects present-day climate more accurately.
_now          = datetime.now(tz=timezone.utc)
NORMAL_START  = datetime(_now.year - 15, 1,  1,  tzinfo=timezone.utc)
NORMAL_END    = datetime(_now.year - 1,  12, 31, tzinfo=timezone.utc)

OUTPUT_PATH = Path("data/stations.json")

# ─── API ──────────────────────────────────────────────────────────────────────

def fetch_stations(parameter: int) -> list[dict]:
    """Fetch the full station list for a given parameter from the SMHI API."""
    url = f"{BASE_URL}/parameter/{parameter}/station.json"
    print(f"  GET {url}")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    stations = data.get("station", [])
    print(f"  → {len(stations)} stations returned")
    return stations


# ─── Filtering ────────────────────────────────────────────────────────────────

def covers_normal_period(station: dict) -> bool:
    """
    Return True if the station's record range covers the full normal period.
    SMHI timestamps are Unix milliseconds — convert to seconds for datetime.
    """
    from_dt = datetime.fromtimestamp(station["from"] / 1000, tz=timezone.utc)
    to_dt   = datetime.fromtimestamp(station["to"]   / 1000, tz=timezone.utc)
    return from_dt <= NORMAL_START and to_dt >= NORMAL_END


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"Normal period : {NORMAL_START.date()} – {NORMAL_END.date()}")
    print()

    # Fetch both parameter lists
    print(f"Fetching stations for parameter {PARAM_MEAN} (daily mean temperature)...")
    mean_stations = fetch_stations(PARAM_MEAN)
    time.sleep(1)  # be a good citizen

    print(f"\nFetching stations for parameter {PARAM_MIN} (daily minimum temperature)...")
    min_stations = fetch_stations(PARAM_MIN)
    print()

    # Index each list by station id
    mean_by_id = {s["id"]: s for s in mean_stations}
    min_by_id  = {s["id"]: s for s in min_stations}

    # Intersect: stations present in both parameter lists
    both_ids = set(mean_by_id) & set(min_by_id)
    print(f"Stations with parameter {PARAM_MEAN}  : {len(mean_by_id)}")
    print(f"Stations with parameter {PARAM_MIN} : {len(min_by_id)}")
    print(f"Stations with both parameters        : {len(both_ids)}")

    # Filter to stations whose records cover the full normal period.
    # Use the mean-temp record for the date range check — the two parameter
    # records for the same station typically share the same from/to range,
    # and mean temp (param 2) is more widely available.
    qualifying = [
        mean_by_id[sid]
        for sid in sorted(both_ids)
        if covers_normal_period(mean_by_id[sid])
    ]
    print(f"Covering full normal period          : {len(qualifying)}")
    print()

    if not qualifying:
        print("No qualifying stations found. Exiting.", file=sys.stderr)
        sys.exit(1)

    # Build output records
    records = [
        {
            "id":         s["id"],
            "name":       s["name"],
            "lat":        s["latitude"],
            "lng":        s["longitude"],
            "elevationM": s["height"],
        }
        for s in qualifying
    ]

    # Write output
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"Written {len(records)} stations → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
