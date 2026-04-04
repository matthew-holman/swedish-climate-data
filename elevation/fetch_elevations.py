#!/usr/bin/env python3
"""
fetch_elevations.py — Build output/postcodes-enriched.json

Reads the GeoNames SE.zip postal code file (tab-separated), resolves
elevation for each postcode from local SRTM .hgt tiles, and writes a
JSON array to output/postcodes-enriched.json.

Each output record contains:
    postcode    — 5-digit Swedish postal code (string)
    lat         — latitude (float, WGS84)
    lng         — longitude (float, WGS84)
    placeName   — locality name from GeoNames
    adminName1  — county/region name from GeoNames (may be null)
    elevationM  — elevation in metres (integer, or null if tile missing)

Dependencies: Python standard library only (no pip packages required).

GeoNames data licence: Creative Commons Attribution 4.0.
Attribution: geonames.org

Data directory setup
--------------------
Place all input files in a single directory and pass it via --data:

  SE.zip        — GeoNames postcode file, downloaded from
                  https://download.geonames.org/export/zip/
  N55E010.hgt   — SRTM elevation tiles (one per 1°×1° cell), downloaded from
  N55E011.hgt     https://e4ftl01.cr.usgs.gov/MEASURES/SRTMGL1.003/2000.02.11/
  ...             or http://viewfinderpanoramas.org/
  N69E024.hgt   — Tiles needed for Sweden: N55–N69, E010–E024

  Unzip each .hgt.zip so the directory contains bare .hgt files.
  USGS files arrive named e.g. N59E018.SRTMGL1.hgt — both naming patterns
  are accepted.

Usage:
    python elevation/fetch_elevations.py --data ~/path/to/data-dir

Arguments:
    --data   Directory containing SE.zip and all SRTM .hgt tile files. Required.

Output:
    output/postcodes-enriched.json
"""

import csv
import io
import json
import math
import os
import struct
import sys
import zipfile
from pathlib import Path


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> tuple[Path, Path]:
    """
    Return (input_path, data_dir) where input_path is SE.zip inside data_dir.
    Exits on error.
    """
    args = sys.argv[1:]

    data_dir: Path | None = None

    i = 0
    while i < len(args):
        if args[i] == "--data" and i + 1 < len(args):
            data_dir = Path(args[i + 1]).expanduser()
            i += 2
        else:
            i += 1

    if data_dir is None:
        print(
            "Usage:\n"
            "  python elevation/fetch_elevations.py --data ~/path/to/data-dir\n\n"
            "The data directory must contain:\n"
            "  SE.zip          — GeoNames postcode file\n"
            "  N??E???.hgt     — Unzipped SRTM elevation tiles for Sweden\n\n"
            "Download postcode data from:\n"
            "  https://download.geonames.org/export/zip/\n"
            "Download tiles for Sweden (N55–N69, E010–E024) from:\n"
            "  https://e4ftl01.cr.usgs.gov/MEASURES/SRTMGL1.003/2000.02.11/\n"
            "  http://viewfinderpanoramas.org/",
            file=sys.stderr,
        )
        sys.exit(1)

    if not data_dir.is_dir():
        print(f"Data directory not found: {data_dir}", file=sys.stderr)
        sys.exit(1)

    input_path = data_dir / "SE.zip"
    if not input_path.exists():
        print(f"SE.zip not found in data directory: {data_dir}", file=sys.stderr)
        sys.exit(1)

    return input_path, data_dir


# ---------------------------------------------------------------------------
# GeoNames TSV parsing
# ---------------------------------------------------------------------------

def _open_tsv(input_path: Path) -> io.TextIOWrapper:
    """Return a text stream for the SE.txt content, from .zip or raw .txt."""
    if input_path.suffix.lower() == ".zip":
        zf = zipfile.ZipFile(input_path)
        # Accept SE.txt or any .txt entry inside the zip
        txt_names = [n for n in zf.namelist() if n.endswith(".txt")]
        if not txt_names:
            print(f"No .txt file found inside {input_path}", file=sys.stderr)
            sys.exit(1)
        entry = "SE.txt" if "SE.txt" in txt_names else txt_names[0]
        return io.TextIOWrapper(zf.open(entry), encoding="utf-8")
    else:
        return open(input_path, encoding="utf-8")


def parse_postcodes(input_path: Path) -> list[dict]:
    """
    Parse the GeoNames tab-separated file and return a list of dicts with
    keys: postcode, lat, lng, placeName, adminName1.

    GeoNames column order:
        0  country_code
        1  postal_code
        2  place_name
        3  admin_name1   (county/region)
        4  admin_code1
        5  admin_name2
        6  admin_code2
        7  admin_name3
        8  admin_code3
        9  latitude
       10  longitude
       11  accuracy
    """
    rows = []
    stream = _open_tsv(input_path)

    with stream:
        reader = csv.reader(stream, delimiter="\t")
        for line_num, cols in enumerate(reader, start=1):
            if len(cols) < 11:
                continue

            # Normalise postcode: strip whitespace, must be exactly 5 digits
            postcode = cols[1].replace(" ", "").strip()
            if not postcode.isdigit() or len(postcode) != 5:
                continue

            try:
                lat = float(cols[9])
                lng = float(cols[10])
            except ValueError:
                continue

            place_name = cols[2].strip()
            if not place_name:
                continue

            admin_name1 = cols[3].strip() or None

            rows.append(
                {
                    "postcode": postcode,
                    "lat": lat,
                    "lng": lng,
                    "placeName": place_name,
                    "adminName1": admin_name1,
                }
            )

    return rows


# ---------------------------------------------------------------------------
# SRTM .hgt elevation lookup
# ---------------------------------------------------------------------------

def _tile_basename(lat: float, lng: float) -> str:
    """
    Derive the SRTM tile base name for a coordinate.
    e.g. lat=59.34, lng=18.06 → 'N59E018'
    """
    lat_prefix = "N" if lat >= 0 else "S"
    lng_prefix = "E" if lng >= 0 else "W"
    lat_deg = str(math.floor(abs(lat))).zfill(2)
    lng_deg = str(math.floor(abs(lng))).zfill(3)
    return f"{lat_prefix}{lat_deg}{lng_prefix}{lng_deg}"


def _find_tile_path(data_dir: Path, lat: float, lng: float) -> Path | None:
    """Return the path to the .hgt tile for a coordinate, or None."""
    base = _tile_basename(lat, lng)
    candidates = [
        data_dir / f"{base}.hgt",
        data_dir / f"{base}.SRTMGL1.hgt",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def _read_hgt_elevation(tile_path: Path, lat: float, lng: float) -> int | None:
    """
    Read the elevation in metres for (lat, lng) from an SRTM .hgt file.

    HGT format:
      - SRTM1 (1 arc-second):  3601×3601 samples, 2 bytes each, big-endian int16
      - SRTM3 (3 arc-second):  1201×1201 samples, 2 bytes each, big-endian int16
      - Row 0 = northern edge, last row = southern edge
      - Column 0 = western edge, last column = eastern edge
      - Void value: -32768

    The tile south-west corner is derived from the filename (floor of coords).
    """
    file_size = tile_path.stat().st_size
    if file_size == 3601 * 3601 * 2:
        samples = 3601
    elif file_size == 1201 * 1201 * 2:
        samples = 1201
    else:
        raise ValueError(
            f"Unrecognised .hgt file size {file_size} in {tile_path.name}; "
            "expected SRTM1 (3601×3601) or SRTM3 (1201×1201)"
        )

    sw_lat = math.floor(lat)
    sw_lng = math.floor(lng)

    # Row 0 is the north edge; convert lat offset from south to row index
    row = round((sw_lat + 1 - lat) * (samples - 1))
    col = round((lng - sw_lng) * (samples - 1))
    row = max(0, min(row, samples - 1))
    col = max(0, min(col, samples - 1))

    offset = (row * samples + col) * 2

    with tile_path.open("rb") as f:
        f.seek(offset)
        (elevation,) = struct.unpack(">h", f.read(2))

    if elevation == -32768:
        return None  # void / no-data
    return elevation


class ElevationLookup:
    """
    Resolves elevation for coordinates using locally stored SRTM .hgt tiles.
    Caches open tile data in memory to avoid redundant I/O when many postcodes
    share the same 1°×1° cell.
    """

    def __init__(self, data_dir: Path) -> None:
        self._data_dir = data_dir
        # Cache: tile_path → (samples, bytes) to avoid repeated file reads
        self._cache: dict[Path, tuple[int, bytes]] = {}

    def get(self, lat: float, lng: float) -> int | None:
        """Return elevation in metres, or None if the tile is missing/void."""
        tile_path = _find_tile_path(self._data_dir, lat, lng)
        if tile_path is None:
            return None
        try:
            return _read_hgt_elevation(tile_path, lat, lng)
        except Exception as exc:
            print(
                f"  Warning: failed to read elevation for [{lat}, {lng}] "
                f"from {tile_path.name}: {exc}",
                file=sys.stderr,
            )
            return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    input_path, data_dir = parse_args()

    print(f"Data dir : {data_dir}")
    print(f"Input    : {input_path.name}")
    print()

    # --- Parse postcodes ---
    print("Parsing postcode file...")
    rows = parse_postcodes(input_path)
    print(f"Parsed {len(rows):,} postcodes")
    print()

    # --- Resolve elevations ---
    print("Resolving elevations from SRTM tiles...")
    lookup = ElevationLookup(data_dir)

    results = []
    missing_tiles: set[str] = set()
    void_count = 0

    for i, row in enumerate(rows):
        lat = row["lat"]
        lng = row["lng"]
        elevation = lookup.get(lat, lng)

        if elevation is None:
            tile = _tile_basename(lat, lng)
            if not _find_tile_path(data_dir, lat, lng):
                missing_tiles.add(tile)
            else:
                void_count += 1  # tile exists but pixel is void

        results.append(
            {
                "postcode": row["postcode"],
                "lat": lat,
                "lng": lng,
                "placeName": row["placeName"],
                "adminName1": row["adminName1"],
                "elevationM": elevation,
            }
        )

        if (i + 1) % 1000 == 0 or (i + 1) == len(rows):
            print(f"\r  Progress: {i + 1:,}/{len(rows):,}", end="", flush=True)

    print()
    print()

    # --- Write output ---
    output_path = Path("output/postcodes-enriched.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
        f.write("\n")

    # --- Summary ---
    resolved = sum(1 for r in results if r["elevationM"] is not None)
    print("─" * 45)
    print(f"Processed  : {len(results):,}")
    print(f"Resolved   : {resolved:,}")
    print(f"Void pixels: {void_count:,}")
    print(f"No tile    : {len(results) - resolved - void_count:,}")
    print(f"Output     : {output_path}")
    print("─" * 45)

    if missing_tiles:
        print(
            f"\nMissing tiles ({len(missing_tiles)}):\n  "
            + "\n  ".join(sorted(missing_tiles))
        )
        print(
            "\nDownload the missing .hgt files and re-run to fill in elevations."
        )


if __name__ == "__main__":
    main()
