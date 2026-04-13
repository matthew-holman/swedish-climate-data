#!/usr/bin/env python3
"""
derive_normals.py — Step 3 of 4: derive per-station climate normals.

Reads the per-station observation CSVs produced by fetch_observations.py (or
fetch_fake_observations.py) and computes climate normals for each station
across the climate window (current year − 15 through current year − 1).

Derived fields per station:
    last_frost_doy      — median day-of-year of the last spring frost
                          (last day where daily min temp < 0°C)
    first_frost_doy     — median day-of-year of the first autumn frost after
                          July 1st (first day where daily min temp < 0°C)
    growing_days        — first_frost_doy − last_frost_doy
    gdd_annual          — mean annual growing degree days above 5°C
                          (sum of daily mean − 5°C for days where mean > 5°C)
    monthly_mean_temps  — array of 12 floats (Jan–Dec), mean daily mean
                          temperature per calendar month across all years

Output:
    output/weather_stations.json — array of station records

Stations with no observation file are skipped with a warning. Fields that
cannot be computed (e.g. no frost events ever recorded) are emitted as null.

This file is the input for Step 4 (validate.py).

Usage:
    python smhi/derive_normals.py
    make smhi-normals
"""

import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from smhi.types import Station, WeatherStation

# ─── Constants ────────────────────────────────────────────────────────────────

_now         = datetime.now(tz=timezone.utc)
WINDOW_START = _now.year - 15
WINDOW_END   = _now.year - 1

ROOT          = Path(__file__).parent.parent
STATIONS_PATH = ROOT / "data/stations.json"
OBS_DIR       = ROOT / "data/observations"
OUTPUT_PATH   = ROOT / "output/weather_stations.json"

# ─── Per-station derivation ───────────────────────────────────────────────────

def _nullable_int(value) -> int | None:
    """Convert a pandas scalar to int, or None if NaN/missing."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return int(round(value))


def _nullable_float(value, decimals: int = 1) -> float | None:
    """Convert a pandas scalar to a rounded float, or None if NaN/missing."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return round(float(value), decimals)


def derive_station_normals(station: Station) -> WeatherStation | None:
    """
    Compute climate normals for a single station.
    Returns a record dict, or None if the observation file is missing.
    """
    sid      = station["id"]
    obs_path = OBS_DIR / f"{sid}.csv"

    if not obs_path.exists():
        print(f"  Warning: no observation file for station {sid} ({station['name']}) — skipping",
              file=sys.stderr)
        return None

    # --- Load observations ---
    df = pd.read_csv(obs_path, parse_dates=["date"])
    df["year"]  = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["doy"]   = df["date"].dt.day_of_year

    # Filter to climate window
    df = df[(df["year"] >= WINDOW_START) & (df["year"] <= WINDOW_END)]

    df_mean = df.dropna(subset=["mean_temp"])
    df_min  = df.dropna(subset=["min_temp"])

    # --- Last spring frost ---
    # Per year: last day before July 1st (month < 7) where min_temp < 0°C.
    # Constraining to the first half of the year isolates spring frosts from
    # autumn/winter frosts, which are captured separately as first_frost_doy.
    # Years with no spring frost don't contribute to the median.
    spring_mask    = (df_min["min_temp"] < 0) & (df_min["month"] < 7)
    spring_frosts  = df_min[spring_mask].groupby("year")["doy"].max()
    last_frost_doy = _nullable_int(spring_frosts.median() if not spring_frosts.empty else None)

    # --- First autumn frost ---
    # Per year: first day after July 1st (month >= 7) where min_temp < 0°C.
    autumn_mask    = (df_min["min_temp"] < 0) & (df_min["month"] >= 7)
    autumn_frosts  = df_min[autumn_mask].groupby("year")["doy"].min()
    first_frost_doy = _nullable_int(autumn_frosts.median() if not autumn_frosts.empty else None)

    print("Autumn frost by year")
    print(autumn_frosts.sort_index().to_string())

    # --- Growing days ---
    if last_frost_doy is not None and first_frost_doy is not None:
        growing_days = first_frost_doy - last_frost_doy
    else:
        growing_days = None

    # --- Growing degree days (GDD base 5°C) ---
    # Per year: sum of (mean_temp − 5) for days where mean_temp > 5°C. Mean across years.
    gdd_mask    = df_mean["mean_temp"] > 5
    gdd_by_year = (
        df_mean[gdd_mask]
        .groupby("year")
        .apply(lambda g: (g["mean_temp"] - 5).sum(), include_groups=False)
    )
    gdd_annual = _nullable_float(gdd_by_year.mean() if not gdd_by_year.empty else None)

    print("GDD by year")
    print(gdd_by_year.sort_index().to_string())

    # --- Monthly mean temperatures ---
    # Mean of daily mean temp per calendar month across all years in the window.
    monthly = df_mean.groupby("month")["mean_temp"].mean().round(1)
    monthly_mean_temps = [
        _nullable_float(monthly.get(m))
        for m in range(1, 13)
    ]

    return WeatherStation(
        id=sid,
        name=station["name"],
        lat=station["lat"],
        lng=station["lng"],
        elevationM=station["elevationM"],
        last_frost_doy=last_frost_doy,
        first_frost_doy=first_frost_doy,
        growing_days=growing_days,
        gdd_annual=gdd_annual,
        monthly_mean_temps=monthly_mean_temps,
    )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    if not STATIONS_PATH.exists():
        print(f"Stations file not found: {STATIONS_PATH}", file=sys.stderr)
        print("Run fetch_stations.py first.", file=sys.stderr)
        sys.exit(1)

    stations: list[Station] = json.loads(STATIONS_PATH.read_text(encoding="utf-8"))

    print(f"Climate window : {WINDOW_START}–{WINDOW_END}")
    print(f"Stations       : {len(stations)}")
    print()

    results: list[WeatherStation] = []
    skipped = 0

    for i, station in enumerate(stations):
        print(f"Station {station["name"]}:{station["id"]}:")
        record = derive_station_normals(station)
        if record is None:
            skipped += 1
        else:
            results.append(record)

        if (i + 1) % 50 == 0 or (i + 1) == len(stations):
            print(f"\r  Progress: {i + 1}/{len(stations)}", end="", flush=True)

    print()
    print()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print("─" * 45)
    print(f"Written  : {len(results)} stations → {OUTPUT_PATH}")
    if skipped:
        print(f"Skipped  : {skipped} (no observation file)")
    print("─" * 45)


if __name__ == "__main__":
    main()
