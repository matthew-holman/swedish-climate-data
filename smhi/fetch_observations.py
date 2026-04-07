#!/usr/bin/env python3
"""
fetch_observations.py — Step 2 of 4: download daily temperature observations.

Reads data/stations.json (produced by fetch_stations.py) and downloads
corrected-archive temperature observations from the SMHI open data API for
each station, for both parameters:
  - Parameter 2  : daily mean temperature (one reading per day)
  - Parameter 26 : daily minimum temperature (two readings per day at 06:00
                   and 18:00 UTC — the daily minimum is derived by taking the
                   lower of the two)

Only the corrected-archive period is fetched. This covers quality-controlled
historical data up to approximately 3 months before the current date, which
is sufficient to cover the full climate window (current year − 15 through
current year − 1).

Output:
    data/observations/{station_id}.csv — one file per station

Each output CSV has three columns:
    date       — YYYY-MM-DD
    mean_temp  — daily mean temperature in °C (float, may be empty if missing)
    min_temp   — daily minimum temperature in °C (float, may be empty if missing)

Only rows within the climate window are written. Stations where either
parameter fails to download are skipped with a warning and do not produce
an output file.

This file is the input for Step 3 (derive_normals.py).
data/ is gitignored — these files are never committed.

Usage:
    python smhi/fetch_observations.py
    make smhi-obs
"""

import csv
import io
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ─── Constants ────────────────────────────────────────────────────────────────

BASE_URL = "https://opendata-download-metobs.smhi.se/api/version/1.0"

PARAM_MEAN = 2   # daily mean temperature
PARAM_MIN  = 26  # daily minimum temperature (two readings per day)

# Climate window — derived at runtime to match fetch_stations.py
_now           = datetime.now(tz=timezone.utc)
WINDOW_START   = f"{_now.year - 15}-01-01"
WINDOW_END     = f"{_now.year - 1}-12-31"

STATIONS_PATH  = Path("data/stations.json")
OBS_DIR        = Path("data/observations")

REQUEST_DELAY  = 0.5  # seconds between API calls — be a good citizen

# ─── HTTP ─────────────────────────────────────────────────────────────────────

def fetch_csv(station_id: int, parameter: int) -> str | None:
    """
    Download the corrected-archive CSV for a station/parameter combination.
    Returns the response text, or None on failure.
    """
    url = (
        f"{BASE_URL}/parameter/{parameter}/station/{station_id}"
        f"/period/corrected-archive/data.csv"
    )
    try:
        response = requests.get(url, timeout=60)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.text
    except requests.RequestException as exc:
        print(f"    Warning: request failed for station {station_id} "
              f"param {parameter}: {exc}", file=sys.stderr)
        return None


# ─── CSV parsing ──────────────────────────────────────────────────────────────

def _find_data_start(lines: list[str], header_prefix: str) -> int | None:
    """
    Return the index of the first data row (the line after the column header).
    SMHI CSVs have a metadata block at the top — we skip it by looking for
    the column header line that starts with the given prefix.
    """
    for i, line in enumerate(lines):
        if line.lstrip("\ufeff").startswith(header_prefix):
            return i + 1
    return None


def parse_mean_csv(csv_text: str) -> dict[str, float]:
    """
    Parse a corrected-archive CSV for parameter 2 (daily mean temperature).

    Column header: Från Datum Tid (UTC);Till Datum Tid (UTC);Representativt dygn;Lufttemperatur;Kvalitet
    Data row:      2011-01-01 00:00:01;2011-01-02 00:00:00;2011-01-01;-3.4;G

    Returns {date_str: mean_temp} for all valid rows.
    """
    lines = csv_text.splitlines()
    start = _find_data_start(lines, "Från Datum")
    if start is None:
        return {}

    result: dict[str, float] = {}
    for line in lines[start:]:
        if not line.strip():
            continue
        cols = line.split(";")
        if len(cols) < 4:
            continue
        date = cols[2].strip()  # Representativt dygn (representative day)
        raw  = cols[3].strip().replace(",", ".")
        try:
            result[date] = float(raw)
        except ValueError:
            continue  # missing or non-numeric value — skip row

    return result


def parse_min_csv(csv_text: str) -> dict[str, float]:
    """
    Parse a corrected-archive CSV for parameter 26 (daily minimum temperature).

    Parameter 26 is recorded twice per day (06:00 and 18:00 UTC). The daily
    minimum is the lower of the two readings.

    Column header: Datum;Tid (UTC);Lufttemperatur;Kvalitet
    Data rows:     2011-01-01;06:00:00;-8.2;G
                   2011-01-01;18:00:00;-5.1;G

    Returns {date_str: min_temp} for all valid rows.
    """
    lines = csv_text.splitlines()
    start = _find_data_start(lines, "Datum;Tid")
    if start is None:
        return {}

    # Accumulate all readings per date, then take the minimum
    readings: dict[str, list[float]] = {}
    for line in lines[start:]:
        if not line.strip():
            continue
        cols = line.split(";")
        if len(cols) < 3:
            continue
        date = cols[0].strip()
        raw  = cols[2].strip().replace(",", ".")
        try:
            readings.setdefault(date, []).append(float(raw))
        except ValueError:
            continue

    return {date: min(vals) for date, vals in readings.items()}


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    if not STATIONS_PATH.exists():
        print(f"Stations file not found: {STATIONS_PATH}", file=sys.stderr)
        print("Run fetch_stations.py first.", file=sys.stderr)
        sys.exit(1)

    import json
    stations = json.loads(STATIONS_PATH.read_text(encoding="utf-8"))
    OBS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Climate window : {WINDOW_START} – {WINDOW_END}")
    print(f"Stations       : {len(stations)}")
    print(f"Output dir     : {OBS_DIR}")
    print()

    written  = 0
    skipped  = 0

    for i, station in enumerate(stations):
        sid  = station["id"]
        name = station["name"]
        prefix = f"[{i + 1}/{len(stations)}] {name} ({sid})"
        print(prefix)

        # --- Fetch parameter 2 (mean temp) ---
        raw_mean = fetch_csv(sid, PARAM_MEAN)
        time.sleep(REQUEST_DELAY)
        if raw_mean is None:
            print(f"  Skipping — no corrected-archive data for param {PARAM_MEAN}")
            skipped += 1
            continue

        # --- Fetch parameter 26 (min temp) ---
        raw_min = fetch_csv(sid, PARAM_MIN)
        time.sleep(REQUEST_DELAY)
        if raw_min is None:
            print(f"  Skipping — no corrected-archive data for param {PARAM_MIN}")
            skipped += 1
            continue

        # --- Parse both CSVs ---
        mean_by_date = parse_mean_csv(raw_mean)
        min_by_date  = parse_min_csv(raw_min)

        if not mean_by_date or not min_by_date:
            print(f"  Skipping — failed to parse CSV data")
            skipped += 1
            continue

        # --- Merge on date, filter to climate window ---
        all_dates = sorted(set(mean_by_date) | set(min_by_date))
        rows = [
            (date, mean_by_date.get(date), min_by_date.get(date))
            for date in all_dates
            if WINDOW_START <= date <= WINDOW_END
        ]

        if not rows:
            print(f"  Skipping — no data within climate window")
            skipped += 1
            continue

        # --- Write output CSV ---
        out_path = OBS_DIR / f"{sid}.csv"
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["date", "mean_temp", "min_temp"])
            for date, mean, min_ in rows:
                writer.writerow([
                    date,
                    "" if mean is None else mean,
                    "" if min_ is None else min_,
                ])

        print(f"  {len(rows)} days → {out_path.name}")
        written += 1

    print()
    print("─" * 45)
    print(f"Written  : {written}")
    print(f"Skipped  : {skipped}")
    print("─" * 45)


if __name__ == "__main__":
    main()
