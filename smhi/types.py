"""
Shared TypedDicts for the SMHI pipeline.

Station         — one entry in data/stations.json
                  produced by fetch_stations.py
                  consumed by fetch_observations.py, fetch_fake_observations.py,
                  derive_normals.py

WeatherStation  — one entry in output/weather_stations.json
                  produced by derive_normals.py
                  consumed by validate.py
"""

from typing import TypedDict


class Station(TypedDict):
    id:         int
    name:       str
    lat:        float
    lng:        float
    elevationM: float


class WeatherStation(TypedDict):
    id:                  int
    name:                str
    lat:                 float
    lng:                 float
    elevationM:          float
    last_frost_doy:      int | None
    last_frost_p90:      int | None
    first_frost_doy:     int | None
    first_frost_p10:     int | None
    growing_days:        int | None
    gdd_annual:          float | None
    gdd_p10:             float | None
    gdd_p90:             float | None
    gdd_cv:              float | None
    monthly_mean_temps:  list[float | None]
