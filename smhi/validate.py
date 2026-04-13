#!/usr/bin/env python3
"""
validate.py — Step 4 of 4: validate output/weather_stations.json.

Runs a series of checks against the derived climate normals before the file
is committed. Checks fall into four categories:

  1. Structure     — all required fields present, correct types, no nulls on
                     identity fields
  2. Coverage      — station count is reasonable, all stations from
                     data/stations.json appear in the output
  3. Climate ranges — values are within plausible bounds for Swedish climate
  4. Consistency   — internal relationships hold (e.g. first_frost > last_frost,
                     growing_days == first - last, seasonal curve shape)

NOTE: Reference-value checks against published SMHI normals are intentionally
omitted until the SMHI corrected-archive API is restored and real observation
data can be used. The checks here focus on structural and logical correctness.

Exits with code 0 if all checks pass, code 1 if any fail.

Usage:
    python smhi/validate.py
    make smhi-validate
"""

import json

from smhi.types import Station, WeatherStation
import sys
from pathlib import Path

# ─── Paths ────────────────────────────────────────────────────────────────────

ROOT          = Path(__file__).parent.parent
OUTPUT_PATH   = ROOT / "output/weather_stations.json"
STATIONS_PATH = ROOT / "data/stations.json"

# ─── Check runner ─────────────────────────────────────────────────────────────

_results: list[tuple[bool, str]] = []


def check(name: str, passed: bool, detail: str = "") -> bool:
    status = "PASS" if passed else "FAIL"
    msg = f"  [{status}] {name}"
    if detail:
        msg += f": {detail}"
    print(msg)
    _results.append((passed, name))
    return passed


# ─── Checks ───────────────────────────────────────────────────────────────────

REQUIRED_FIELDS = [
    "id", "name", "lat", "lng", "elevationM",
    "last_frost_doy", "first_frost_doy", "growing_days",
    "gdd_annual", "monthly_mean_temps",
]

# Sweden bounding box (generous)
LAT_MIN, LAT_MAX = 55.0, 70.0
LNG_MIN, LNG_MAX = 10.0, 25.0

# Plausible climate ranges for Sweden
LAST_FROST_DOY_RANGE   = (1,   200)   # last spring frost: Jan–mid-July
FIRST_FROST_DOY_RANGE  = (182, 365)   # first autumn frost: post July 1st
GROWING_DAYS_MIN       = 1
GDD_MIN, GDD_MAX       = 50.0, 3000.0
MONTHLY_TEMP_MIN       = -40.0
MONTHLY_TEMP_MAX       = 35.0


def run_checks(stations: list[WeatherStation], expected_ids: set[int]) -> None:

    # ── 1. Structure ──────────────────────────────────────────────────────────
    print("\nStructure")

    missing_fields = [
        f"{s['id']} missing {f}"
        for s in stations
        for f in REQUIRED_FIELDS
        if f not in s
    ]
    check("All required fields present", not missing_fields,
          "; ".join(missing_fields[:3]) + ("…" if len(missing_fields) > 3 else ""))

    bad_types = [
        f"{s['id']}: id={type(s['id']).__name__}"
        for s in stations
        if not isinstance(s.get("id"), int)
    ] + [
        f"{s['id']}: monthly_mean_temps not list of 12"
        for s in stations
        if not isinstance(s.get("monthly_mean_temps"), list)
        or len(s.get("monthly_mean_temps", [])) != 12
    ]
    check("Types correct (id=int, monthly_mean_temps=12-element list)",
          not bad_types, "; ".join(bad_types[:3]))

    null_identity = [
        f"{s.get('id','?')}.{f}"
        for s in stations
        for f in ("id", "name", "lat", "lng", "elevationM")
        if s.get(f) is None
    ]
    check("No nulls on identity fields (id/name/lat/lng/elevationM)",
          not null_identity, ", ".join(null_identity[:5]))

    # ── 2. Coverage ───────────────────────────────────────────────────────────
    print("\nCoverage")

    check("At least 100 stations in output", len(stations) >= 100,
          f"{len(stations)} stations")

    output_ids = {s["id"] for s in stations}
    missing_stations = expected_ids - output_ids
    check("All stations from stations.json present in output",
          not missing_stations,
          f"{len(missing_stations)} missing: "
          + ", ".join(str(i) for i in sorted(missing_stations)[:5])
          + ("…" if len(missing_stations) > 5 else ""))

    # ── 3. Climate ranges ─────────────────────────────────────────────────────
    print("\nClimate ranges")

    out_of_bounds_lat = [
        s["id"] for s in stations
        if not (LAT_MIN <= s["lat"] <= LAT_MAX)
    ]
    check(f"All lats within Sweden ({LAT_MIN}–{LAT_MAX}°N)",
          not out_of_bounds_lat, f"{len(out_of_bounds_lat)} out of range")

    out_of_bounds_lng = [
        s["id"] for s in stations
        if not (LNG_MIN <= s["lng"] <= LNG_MAX)
    ]
    check(f"All lngs within Sweden ({LNG_MIN}–{LNG_MAX}°E)",
          not out_of_bounds_lng, f"{len(out_of_bounds_lng)} out of range")

    bad_last_frost = [
        s["id"] for s in stations
        if s["last_frost_doy"] is not None
        and not (LAST_FROST_DOY_RANGE[0] <= s["last_frost_doy"] <= LAST_FROST_DOY_RANGE[1])
    ]
    check(f"last_frost_doy in range {LAST_FROST_DOY_RANGE}",
          not bad_last_frost, f"{len(bad_last_frost)} out of range: {bad_last_frost[:5]}")

    bad_first_frost = [
        s["id"] for s in stations
        if s["first_frost_doy"] is not None
        and not (FIRST_FROST_DOY_RANGE[0] <= s["first_frost_doy"] <= FIRST_FROST_DOY_RANGE[1])
    ]
    check(f"first_frost_doy in range {FIRST_FROST_DOY_RANGE}",
          not bad_first_frost, f"{len(bad_first_frost)} out of range: {bad_first_frost[:5]}")

    bad_gdd = [
        s["id"] for s in stations
        if s["gdd_annual"] is not None
        and not (GDD_MIN <= s["gdd_annual"] <= GDD_MAX)
    ]
    check(f"gdd_annual in range {GDD_MIN}–{GDD_MAX}",
          not bad_gdd, f"{len(bad_gdd)} out of range: {bad_gdd[:5]}")

    bad_monthly = [
        f"{s['id']}[{i}]={v}"
        for s in stations
        for i, v in enumerate(s.get("monthly_mean_temps") or [])
        if v is not None and not (MONTHLY_TEMP_MIN <= v <= MONTHLY_TEMP_MAX)
    ]
    check(f"All monthly_mean_temps in range {MONTHLY_TEMP_MIN}–{MONTHLY_TEMP_MAX}°C",
          not bad_monthly, "; ".join(bad_monthly[:5]))

    # ── 4. Internal consistency ───────────────────────────────────────────────
    print("\nConsistency")

    bad_frost_order = [
        s["id"] for s in stations
        if s["last_frost_doy"] is not None
        and s["first_frost_doy"] is not None
        and s["first_frost_doy"] <= s["last_frost_doy"]
    ]
    check("first_frost_doy > last_frost_doy for all stations",
          not bad_frost_order, f"{len(bad_frost_order)} violations: {bad_frost_order[:5]}")

    bad_growing = [
        f"{s['id']}: {s['first_frost_doy']}-{s['last_frost_doy']}={s['growing_days']}"
        for s in stations
        if s["last_frost_doy"] is not None
        and s["first_frost_doy"] is not None
        and s["growing_days"] != s["first_frost_doy"] - s["last_frost_doy"]
    ]
    check("growing_days == first_frost_doy − last_frost_doy",
          not bad_growing, "; ".join(bad_growing[:5]))

    bad_positive_growing = [
        s["id"] for s in stations
        if s["growing_days"] is not None and s["growing_days"] < GROWING_DAYS_MIN
    ]
    check(f"growing_days >= {GROWING_DAYS_MIN} for all stations",
          not bad_positive_growing, f"{len(bad_positive_growing)} violations")

    # Seasonal curve: peak month should be Jun/Jul/Aug (indices 5–7)
    bad_peak = [
        f"{s['id']}(peak={s['monthly_mean_temps'].index(max(s['monthly_mean_temps']))+1})"
        for s in stations
        if s["monthly_mean_temps"]
        and all(v is not None for v in s["monthly_mean_temps"])
        and s["monthly_mean_temps"].index(max(s["monthly_mean_temps"])) not in (5, 6, 7)
    ]
    check("Monthly peak temperature in Jun/Jul/Aug",
          not bad_peak, f"{len(bad_peak)} unexpected: {bad_peak[:5]}")

    # Seasonal curve: trough month should be Dec/Jan/Feb (indices 11, 0, 1)
    bad_trough = [
        f"{s['id']}(trough={s['monthly_mean_temps'].index(min(s['monthly_mean_temps']))+1})"
        for s in stations
        if s["monthly_mean_temps"]
        and all(v is not None for v in s["monthly_mean_temps"])
        and s["monthly_mean_temps"].index(min(s["monthly_mean_temps"])) not in (11, 0, 1)
    ]
    check("Monthly trough temperature in Dec/Jan/Feb",
          not bad_trough, f"{len(bad_trough)} unexpected: {bad_trough[:5]}")

    # Latitudinal gradient: last_frost_doy should increase with latitude
    # Check that the northernmost quartile has a later last frost than the southernmost
    by_lat    = sorted([s for s in stations if s["last_frost_doy"]], key=lambda s: s["lat"])
    n         = len(by_lat) // 4
    south_avg = sum(s["last_frost_doy"] for s in by_lat[:n]) / n
    north_avg = sum(s["last_frost_doy"] for s in by_lat[-n:]) / n
    check(
        "Latitudinal gradient: northern stations have later last frost than southern",
        north_avg > south_avg,
        f"south avg={south_avg:.0f} north avg={north_avg:.0f}",
    )

    # Growing season should shrink with latitude
    by_lat_gs = sorted([s for s in stations if s["growing_days"]], key=lambda s: s["lat"])
    south_gs  = sum(s["growing_days"] for s in by_lat_gs[:n]) / n
    north_gs  = sum(s["growing_days"] for s in by_lat_gs[-n:]) / n
    check(
        "Latitudinal gradient: northern stations have shorter growing season",
        north_gs < south_gs,
        f"south avg={south_gs:.0f} days, north avg={north_gs:.0f} days",
    )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    if not OUTPUT_PATH.exists():
        print(f"Output file not found: {OUTPUT_PATH}", file=sys.stderr)
        print("Run derive_normals.py first.", file=sys.stderr)
        sys.exit(1)

    stations: list[WeatherStation] = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    print(f"Validating {OUTPUT_PATH} ({len(stations)} stations)")

    expected_ids: set[int] = set()
    if STATIONS_PATH.exists():
        source: list[Station] = json.loads(STATIONS_PATH.read_text(encoding="utf-8"))
        expected_ids = {s["id"] for s in source}

    run_checks(stations, expected_ids)

    passed = sum(1 for ok, _ in _results if ok)
    failed = sum(1 for ok, _ in _results if not ok)

    print(f"\n{'─' * 45}")
    print(f"  {passed} passed  /  {failed} failed")
    print(f"{'─' * 45}")

    if failed:
        print("\nValidation FAILED — do not commit output/weather_stations.json")
        sys.exit(1)
    else:
        print("\nValidation PASSED — output/weather_stations.json is ready to commit")
        sys.exit(0)


if __name__ == "__main__":
    main()
