#!/usr/bin/env python3
"""
fetch_fake_observations.py — Synthetic substitute for Step 2 of 4.

Generates plausible synthetic daily temperature observations for each station
in data/stations.json and writes them in the same format that
fetch_observations.py would produce. Use this while the SMHI corrected-archive
API endpoint is unavailable.

Output is deterministic: re-running always produces identical CSV files.
Replace with real data from fetch_observations.py once SMHI restore the
corrected-archive endpoint.

Temperature model
-----------------
Annual mean temperature (°C) — latitude and elevation:
    annual_mean = 5.0 − 0.7 × (lat − 60) − 0.0065 × elevationM

Seasonal amplitude — increases with latitude (harsher winters further north):
    amplitude = 10.0 + 0.35 × (lat − 55)

Base temperature by day-of-year — sinusoidal, peaks mid-July (doy ≈ 200):
    base(doy) = annual_mean + amplitude × sin(2π × (doy − 80) / 365)

Daily mean  = base(doy) + Gaussian noise (σ = 3°C)
Daily min   = daily_mean − diurnal_offset + Gaussian noise (σ = 1.5°C)
    diurnal_offset = 4.5 + 2.0 × cos(2π × (doy − 200) / 365)
    (≈ 6.5°C in winter, ≈ 2.5°C in summer — matches Swedish diurnal patterns)

Noise is seeded per station+date for reproducibility:
    seed = station_id × 10000 + date.toordinal()

Output:
    data/observations/{station_id}.csv — one file per station

Each output CSV has three columns:
    date       — YYYY-MM-DD
    mean_temp  — synthetic daily mean temperature in °C
    min_temp   — synthetic daily minimum temperature in °C

Covers the full climate window (current year − 15 through current year − 1).
This file is the input for Step 3 (derive_normals.py).
data/ is gitignored — these files are never committed.

Usage:
    python smhi/fetch_fake_observations.py
    make smhi-fake-obs
"""

import csv
import json
import math
import random
import sys
from datetime import date, timedelta
from datetime import datetime, timezone
from pathlib import Path

# ─── Constants ────────────────────────────────────────────────────────────────

_now         = datetime.now(tz=timezone.utc)
WINDOW_START = date(_now.year - 15, 1, 1)
WINDOW_END   = date(_now.year - 1, 12, 31)

STATIONS_PATH = Path("data/stations.json")
OBS_DIR       = Path("data/observations")

# ─── Temperature model ────────────────────────────────────────────────────────

def _annual_mean(lat: float, elevation_m: float) -> float:
    """Baseline annual mean temperature in °C for a given lat/elevation."""
    return 5.0 - 0.7 * (lat - 60) - 0.0065 * elevation_m


def _seasonal_amplitude(lat: float) -> float:
    """Seasonal swing in °C — larger further north."""
    return 10.0 + 0.35 * (lat - 55)


def _base_temp(doy: int, lat: float, elevation_m: float) -> float:
    """Smoothed daily temperature before noise, using a sinusoidal annual cycle."""
    mean = _annual_mean(lat, elevation_m)
    amp  = _seasonal_amplitude(lat)
    return mean + amp * math.sin(2 * math.pi * (doy - 80) / 365)


def _diurnal_offset(doy: int) -> float:
    """
    Typical gap between daily mean and daily minimum.
    Larger in winter (~6.5°C) when nights are long, smaller in summer (~2.5°C).
    """
    return 4.5 + 2.0 * math.cos(2 * math.pi * (doy - 200) / 365)


def generate_day(station_id: int, d: date, lat: float, elevation_m: float) -> tuple[float, float]:
    """
    Return (mean_temp, min_temp) for a single day.
    Seeded by station_id and date so results are deterministic.
    """
    rng = random.Random(station_id * 10000 + d.toordinal())
    doy = d.timetuple().tm_yday

    mean = _base_temp(doy, lat, elevation_m) + rng.gauss(0, 3.0)
    min_ = mean - _diurnal_offset(doy) + rng.gauss(0, 1.5)
    min_ = min(min_, mean - 0.3)  # min must always be below mean

    return round(mean, 1), round(min_, 1)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    if not STATIONS_PATH.exists():
        print(f"Stations file not found: {STATIONS_PATH}", file=sys.stderr)
        print("Run fetch_stations.py first.", file=sys.stderr)
        sys.exit(1)

    stations = json.loads(STATIONS_PATH.read_text(encoding="utf-8"))
    OBS_DIR.mkdir(parents=True, exist_ok=True)

    # Build the full date range once
    all_dates = []
    d = WINDOW_START
    while d <= WINDOW_END:
        all_dates.append(d)
        d += timedelta(days=1)

    print(f"Climate window : {WINDOW_START} – {WINDOW_END} ({len(all_dates):,} days)")
    print(f"Stations       : {len(stations)}")
    print(f"Output dir     : {OBS_DIR}")
    print()

    for i, station in enumerate(stations):
        sid        = station["id"]
        lat        = station["lat"]
        elevation  = station["elevationM"]
        out_path   = OBS_DIR / f"{sid}.csv"

        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["date", "mean_temp", "min_temp"])
            for d in all_dates:
                mean, min_ = generate_day(sid, d, lat, elevation)
                writer.writerow([d.isoformat(), mean, min_])

        if (i + 1) % 50 == 0 or (i + 1) == len(stations):
            print(f"\r  Progress: {i + 1}/{len(stations)}", end="", flush=True)

    print()
    print()
    print("─" * 45)
    print(f"Written  : {len(stations)} stations")
    print(f"Days/file: {len(all_dates):,}")
    print(f"Output   : {OBS_DIR}/")
    print("─" * 45)


if __name__ == "__main__":
    main()
