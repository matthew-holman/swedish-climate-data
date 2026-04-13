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

Reference-data validation (run_reference_checks) adds five further groups:
  5. Internal consistency — per-station tighter bounds and temperature curve
  6. Reference stations   — hard-coded validated ranges for three anchor stations
  7. Regional coverage    — at least 3 stations per geographic region
  8. GDD gradient         — SLU latitude/elevation gradient (warnings only)
  9. North-south ordering — GDD inversions between distant stations (warnings only)

Exits with code 0 if there are no failures (warnings are acceptable).
Exits with code 1 if any failure is present.

Usage:
    python smhi/validate.py
    make smhi-validate
"""

# Validation thresholds and gradient coefficients are derived from:
#
# SLU MarkInfo climate page (https://www.slu.se/markinfo/klimat):
#   - Temperatursumma decreases 58 degree-days per degree of latitude north
#   - Temperatursumma decreases 90 degree-days per 100m of elevation
#
# Manual cross-validation against SMHI published climate data confirmed
# for three reference stations spanning Sweden's full climate range:
#   - Falsterbo A (55.4°N)        — southern maritime extreme
#   - Stockholm-Bromma (59.4°N)   — central reference
#   - Kiruna Flygplats (67.8°N)   — northern continental extreme
#
# Reference ranges reflect 15-year median values (2011–2025) and should
# be reviewed if the pipeline's normal period window is changed.

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
_warnings: list[str] = []   # non-blocking warnings for human review


def check(name: str, passed: bool, detail: str = "") -> bool:
    status = "PASS" if passed else "FAIL"
    msg = f"  [{status}] {name}"
    if detail:
        msg += f": {detail}"
    print(msg)
    _results.append((passed, name))
    return passed


def warn(detail: str) -> None:
    _warnings.append(detail)


# ─── Checks ───────────────────────────────────────────────────────────────────

REQUIRED_FIELDS = [
    "id", "name", "lat", "lng", "elevationM",
    "last_frost_doy", "last_frost_p90",
    "first_frost_doy", "first_frost_p10",
    "growing_days",
    "gdd_annual", "gdd_p10", "gdd_p90", "gdd_cv",
    "monthly_mean_temps",
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

# ─── SLU gradient constants ───────────────────────────────────────────────────

ANCHOR_STATION_ID  = 52240   # Falsterbo A — best-validated against SLU maps
ANCHOR_LAT         = 55.4
ANCHOR_GDD         = 2105
GDD_PER_DEGREE_LAT = 58     # degree-days lost per degree north
GDD_PER_100M_ELEV  = 90     # degree-days lost per 100m elevation
GRADIENT_TOLERANCE = 0.30   # 30% — coastal/urban stations legitimately deviate

REFERENCE_STATIONS = [
    {
        "id": 52240,
        "name": "Falsterbo A",
        "last_frost_doy":  (75,  115),
        "first_frost_doy": (295, 355),
        "growing_days":    (195, 265),
        "gdd_annual":      (1800, 2400),
    },
    {
        "id": 97200,
        "name": "Stockholm-Bromma Flygplats",
        "last_frost_doy":  (110, 145),
        "first_frost_doy": (260, 300),
        "growing_days":    (130, 175),
        "gdd_annual":      (1500, 2000),
    },
    {
        "id": 180940,
        "name": "Kiruna Flygplats",
        "last_frost_doy":  (140, 170),
        "first_frost_doy": (235, 270),
        "growing_days":    (75,  125),
        "gdd_annual":      (600, 1100),
    },
]

REGIONS = {
    "Götaland (55–58°N)":   lambda s: 55.0 <= s["lat"] < 58.0,
    "Svealand (58–61°N)":   lambda s: 58.0 <= s["lat"] < 61.0,
    "Norrland S (61–64°N)": lambda s: 61.0 <= s["lat"] < 64.0,
    "Norrland N (64–70°N)": lambda s: 64.0 <= s["lat"] <= 70.0,
}


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


# ─── Reference data validation ────────────────────────────────────────────────

def run_reference_checks(stations: list[WeatherStation]) -> None:
    """Five check groups derived from SLU/SMHI published reference data."""

    print("\nReference data validation")
    n = len(stations)
    station_by_id = {s["id"]: s for s in stations}

    # ── 5. Internal consistency (per-station, tighter bounds) ─────────────────
    ic_failures: list[str] = []
    ic_total = 0

    for s in stations:
        sid = s["id"]
        lf  = s.get("last_frost_doy")
        ff  = s.get("first_frost_doy")
        gd  = s.get("growing_days")
        gdd = s.get("gdd_annual")
        mmt = s.get("monthly_mean_temps") or []

        ic_total += 1

        lf90 = s.get("last_frost_p90")
        ff10 = s.get("first_frost_p10")
        gp10 = s.get("gdd_p10")
        gp90 = s.get("gdd_p90")
        gcv  = s.get("gdd_cv")
        elev = s.get("elevationM", 0)

        if lf is not None and ff is not None and gd is not None:
            if gd != ff - lf:
                ic_failures.append(
                    f"  ✗ {sid} ({s['name']}): growing_days {gd} ≠ {ff}-{lf}={ff-lf}"
                )
                _results.append((False, f"IC growing_days math {sid}"))
            if lf >= ff:
                ic_failures.append(
                    f"  ✗ {sid} ({s['name']}): last_frost_doy {lf} ≥ first_frost_doy {ff}"
                )
                _results.append((False, f"IC frost order {sid}"))
            if elev >= 400:
                # High-altitude stations legitimately have very short seasons
                if not (1 <= gd <= 280):
                    ic_failures.append(
                        f"  ✗ {sid} ({s['name']}): growing_days {gd} outside 1–280 (high-altitude station, elev={elev}m)")
                    _results.append((False, f"IC growing_days range {sid}"))
            else:
                if not (60 <= gd <= 280):
                    ic_failures.append(f"  ✗ {sid} ({s['name']}): growing_days {gd} outside 60–280")
                    _results.append((False, f"IC growing_days range {sid}"))

        # Variability field ordering checks
        if lf is not None and lf90 is not None and lf90 < lf:
            ic_failures.append(
                f"  ✗ {sid} ({s['name']}): last_frost_p90 {lf90} < last_frost_doy {lf}"
            )
            _results.append((False, f"IC last_frost_p90 order {sid}"))
        if lf is not None and lf90 is None:
            ic_failures.append(
                f"  ✗ {sid} ({s['name']}): last_frost_p90 is null but last_frost_doy is not"
            )
            _results.append((False, f"IC last_frost_p90 null {sid}"))

        if ff is not None and ff10 is not None and ff10 > ff:
            ic_failures.append(
                f"  ✗ {sid} ({s['name']}): first_frost_p10 {ff10} > first_frost_doy {ff}"
            )
            _results.append((False, f"IC first_frost_p10 order {sid}"))
        if ff is not None and ff10 is None:
            ic_failures.append(
                f"  ✗ {sid} ({s['name']}): first_frost_p10 is null but first_frost_doy is not"
            )
            _results.append((False, f"IC first_frost_p10 null {sid}"))

        if gdd is not None and not (400 <= gdd <= 2500):
            ic_failures.append(
                f"  ✗ {sid} ({s['name']}): gdd_annual {gdd:.0f} outside 400–2500"
            )
            _results.append((False, f"IC gdd_annual range {sid}"))

        if gdd is not None:
            if gp10 is None:
                ic_failures.append(f"  ✗ {sid} ({s['name']}): gdd_p10 is null but gdd_annual is not")
                _results.append((False, f"IC gdd_p10 null {sid}"))
            elif gp10 > gdd:
                ic_failures.append(f"  ✗ {sid} ({s['name']}): gdd_p10 {gp10:.0f} > gdd_annual {gdd:.0f}")
                _results.append((False, f"IC gdd_p10 order {sid}"))

            if gp90 is None:
                ic_failures.append(f"  ✗ {sid} ({s['name']}): gdd_p90 is null but gdd_annual is not")
                _results.append((False, f"IC gdd_p90 null {sid}"))
            elif gp90 < gdd:
                ic_failures.append(f"  ✗ {sid} ({s['name']}): gdd_p90 {gp90:.0f} < gdd_annual {gdd:.0f}")
                _results.append((False, f"IC gdd_p90 order {sid}"))

            if gcv is None:
                ic_failures.append(f"  ✗ {sid} ({s['name']}): gdd_cv is null but gdd_annual is not")
                _results.append((False, f"IC gdd_cv null {sid}"))
            elif not (0.0 <= gcv <= 0.5):
                ic_failures.append(f"  ✗ {sid} ({s['name']}): gdd_cv {gcv} outside 0.0–0.5")
                _results.append((False, f"IC gdd_cv range {sid}"))

        if len(mmt) != 12:
            ic_failures.append(
                f"  ✗ {sid} ({s['name']}): monthly_mean_temps has {len(mmt)} values (expected 12)"
            )
            _results.append((False, f"IC monthly_mean_temps length {sid}"))
        elif all(v is not None for v in mmt):
            if mmt[6] <= mmt[0]:
                ic_failures.append(
                    f"  ✗ {sid} ({s['name']}): July ({mmt[6]:.1f}°C) not warmer than January ({mmt[0]:.1f}°C)"
                )
                _results.append((False, f"IC July>January {sid}"))
            summer_max = max(mmt[5:9])
            winter_min = min(mmt[i] for i in (10, 11, 0, 1, 2))
            if summer_max - winter_min < 10.0:
                ic_failures.append(
                    f"  ✗ {sid} ({s['name']}): seasonal range {summer_max - winter_min:.1f}°C < 10°C"
                )
                _results.append((False, f"IC seasonal range {sid}"))

    ic_passed = ic_total - len(ic_failures)
    if ic_failures:
        print(f"  ✗ Internal consistency       {ic_passed}/{ic_total} passed ({len(ic_failures)} failures)")
        for line in ic_failures:
            print(line)
    else:
        print(f"  ✓ Internal consistency       {ic_passed}/{ic_total} passed")

    # ── 6. Reference station checks ───────────────────────────────────────────
    ref_failures: list[str] = []
    ref_stations_passed = 0

    for ref in REFERENCE_STATIONS:
        s = station_by_id.get(ref["id"])
        station_ok = True
        if s is None:
            ref_failures.append(f"  ✗ {ref['name']} (id {ref['id']}) not found in output")
            _results.append((False, f"Reference station missing {ref['id']}"))
            station_ok = False
        else:
            for field, (lo, hi) in [
                ("last_frost_doy",  ref["last_frost_doy"]),
                ("first_frost_doy", ref["first_frost_doy"]),
                ("growing_days",    ref["growing_days"]),
                ("gdd_annual",      ref["gdd_annual"]),
            ]:
                val = s.get(field)
                if val is None or not (lo <= val <= hi):
                    ref_failures.append(
                        f"  ✗ {ref['name']}: {field}={val} outside validated range {lo}–{hi}"
                    )
                    _results.append((False, f"Reference station range {ref['id']} {field}"))
                    station_ok = False
        if station_ok:
            ref_stations_passed += 1

    if ref_failures:
        print(f"  ✗ Reference station checks   {ref_stations_passed}/3 passed")
        for line in ref_failures:
            print(line)
    else:
        print(f"  ✓ Reference station checks    3/3 passed")

    # ── 7. Regional coverage ──────────────────────────────────────────────────
    region_failures: list[str] = []

    for region_name, predicate in REGIONS.items():
        count = sum(1 for s in stations if predicate(s))
        if count < 3:
            region_failures.append(
                f"  ✗ {region_name}: only {count} station(s) (need ≥ 3)"
            )
            _results.append((False, f"Regional coverage {region_name}"))

    if region_failures:
        covered = len(REGIONS) - len(region_failures)
        print(f"  ✗ Regional coverage          {covered}/{len(REGIONS)} regions covered")
        for line in region_failures:
            print(line)
    else:
        print(f"  ✓ Regional coverage           {len(REGIONS)}/{len(REGIONS)} regions covered")

    # ── 8. GDD gradient check (warnings only) ────────────────────────────────
    gradient_warnings: list[str] = []
    gradient_total = 0

    for s in stations:
        if s["id"] == ANCHOR_STATION_ID:
            continue
        gdd = s.get("gdd_annual")
        elev = s.get("elevationM")
        if gdd is None or elev is None:
            continue
        gradient_total += 1
        expected = (
            ANCHOR_GDD
            - GDD_PER_DEGREE_LAT * (s["lat"] - ANCHOR_LAT)
            - GDD_PER_100M_ELEV  * (elev / 100.0)
        )
        if expected <= 0:
            continue
        deviation = abs(gdd - expected) / expected
        if deviation > GRADIENT_TOLERANCE:
            pct = deviation * 100
            gradient_warnings.append(
                f"  ⚠ {s['name']}: GDD {gdd:.0f} deviates {pct:.0f}% from prediction {expected:.0f}"
            )
            warn(gradient_warnings[-1])

    gw = len(gradient_warnings)
    if gw:
        print(f"  ⚠ GDD gradient check         {gradient_total - gw}/{gradient_total} passed ({gw} warnings)")
        for line in gradient_warnings:
            print(line)
    else:
        print(f"  ✓ GDD gradient check         {gradient_total}/{gradient_total} passed")

    # ── 9. North-south ordering check (warnings only) ─────────────────────────
    ns_warnings: list[str] = []
    by_lat = sorted(
        [s for s in stations if s.get("gdd_annual") is not None],
        key=lambda s: s["lat"],
    )

    for i, south in enumerate(by_lat):
        for north in by_lat[i + 1:]:
            if north["lat"] - south["lat"] <= 1.0:
                continue
            if north["gdd_annual"] > south["gdd_annual"] * 1.15:
                msg = (
                    f"  ⚠ {north['name']} GDD {north['gdd_annual']:.0f} higher than "
                    f"{south['name']} GDD {south['gdd_annual']:.0f} "
                    f"despite being {north['lat'] - south['lat']:.1f}° further north"
                )
                ns_warnings.append(msg)
                warn(msg)

    if ns_warnings:
        print(f"  ⚠ North-south ordering        {len(ns_warnings)} inversion warning(s)")
        for line in ns_warnings:
            print(line)
    else:
        print(f"  ✓ North-south ordering        no inversions")

    # ── 10. Variability summary ───────────────────────────────────────────────
    cv_values = [
        (s["name"], s["gdd_cv"])
        for s in stations
        if s.get("gdd_cv") is not None
    ]
    low  = [(name, cv) for name, cv in cv_values if cv < 0.08]
    mid  = [(name, cv) for name, cv in cv_values if 0.08 <= cv <= 0.15]
    high = [(name, cv) for name, cv in cv_values if cv > 0.15]

    low_ex  = ", ".join(f"{name} ({cv:.2f})" for name, cv in sorted(low,  key=lambda x: x[1])[:2])
    high_ex = ", ".join(f"{name} ({cv:.2f})" for name, cv in sorted(high, key=lambda x: x[1], reverse=True)[:2])

    print(f"\nVariability summary (gdd_cv):")
    print(f"  Low  (cv < 0.08):    {len(low):3d} stations" + (f" — e.g. {low_ex}" if low_ex else ""))
    print(f"  Mid  (cv 0.08–0.15): {len(mid):3d} stations")
    print(f"  High (cv > 0.15):    {len(high):3d} stations" + (f" — e.g. {high_ex}" if high_ex else ""))
    if high:
        print(f"\n  High variability stations should be reviewed — users in these areas")
        print(f"  will receive a climate uncertainty warning in the application.")


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
    run_reference_checks(stations)

    failed   = sum(1 for ok, _ in _results if not ok)
    n_warn   = len(_warnings)

    print(f"\n{'─' * 42}")
    if failed:
        print(f"FAILED ({failed} failure(s), {n_warn} warning(s))")
    elif n_warn:
        print(f"PASSED with {n_warn} warning(s)")
    else:
        print("PASSED")
    print(f"{'─' * 42}")

    if failed:
        print("\nValidation FAILED — do not commit output/weather_stations.json")
        sys.exit(1)
    else:
        print("\nValidation PASSED — output/weather_stations.json is ready to commit")
        sys.exit(0)


if __name__ == "__main__":
    main()
