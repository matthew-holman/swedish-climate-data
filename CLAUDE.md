# Swedish Climate Data Pipeline

This repository is a data pipeline that produces static climate and geographic seed datasets for the Swedish Grow Calendar project. It has no server, no API, and no database of its own. Its sole output is a set of JSON files that are consumed by the grow-zone-api repository.

The pipeline runs locally and occasionally — when setting up the project for the first time, or annually when refreshing climate data from SMHI. It is not deployed anywhere and has no runtime dependencies in production.

## What this repo produces

Two output files, both committed to the repository under `output/`:

- **`postcodes-enriched.json`** — one record per Swedish postcode containing `postcode`, `lat`, `lng`, `placeName`, `adminName1`, and `elevationM` in metres. Produced by the elevation pipeline. Consumed by grow-zone-api to seed the `postcode_zones` table.
- **`weather_stations.json`** — one record per SMHI weather station containing station ID, name, lat, lng, elevation, and derived 30-year climate normals: median last spring frost (day-of-year), median first autumn frost (day-of-year), growing degree days above 5°C, and monthly mean temperatures. Produced by the SMHI pipeline. Consumed by grow-zone-api to seed the `weather_stations` table.

## How the output files are used

Copy both output files to the grow-zone-api repository before running its seed scripts:

```
cp output/postcodes-enriched.json ../grow-zone-api/src/data/
cp output/weather_stations.json ../grow-zone-api/src/data/
```

The grow-zone-api repository is responsible for all database seeding. This repository has no database connection and no knowledge of the API's schema.

## Repository structure

```
input/
  SE.zip                 GeoNames postcode file (tab-separated) — never modify manually

elevation/
  fetch_elevations.py    reads SE.zip + SRTM tiles → output/postcodes-enriched.json

smhi/
  fetch_stations.py      fetches active SMHI station list from open data API
  fetch_observations.py  fetches 30 years of daily temperature records per station
  derive_normals.py      computes frost dates, GDD, monthly means per station
  validate.py            sanity checks output against known reference values

output/
  postcodes-enriched.json   committed — produced by elevation pipeline
  weather_stations.json     committed — produced by SMHI pipeline

data/                    gitignored — raw SRTM tiles and SMHI observation CSVs
```

## Elevation pipeline

Language: Python 3. No external dependencies — stdlib only (`zipfile`, `csv`, `struct`, `json`).

The script reads `input/SE.zip` (the GeoNames tab-separated postcode file for Sweden). For each postcode it parses `postcode`, `lat`, `lng`, `placeName`, and `adminName1` from the file, then derives the correct SRTM tile filename from the coordinates, reads elevation directly from the binary `.hgt` tile, and writes all enriched records to `output/postcodes-enriched.json`.

All input files — `SE.zip` and the SRTM `.hgt` tiles — are kept together in a single local directory outside the repository and passed to the script via a `--data` argument. They are never committed. The `.hgt` and `.hgt.zip` extensions are gitignored.

Tile filename derivation: a coordinate at lat 59.34, lng 18.06 maps to tile `N59E018.hgt`. The tile covers the full 1°×1° cell from 59°N to 60°N, 18°E to 19°E. USGS tiles use the suffix `.SRTMGL1.hgt` — the script handles both naming patterns.

HGT format: 3601×3601 samples for SRTM1 (1 arc-second), 1201×1201 for SRTM3 (3 arc-second). Each sample is a big-endian signed 16-bit integer representing metres. Void pixels are encoded as -32768 and emitted as `null` in the output.

GeoNames data licence: Creative Commons Attribution 4.0. Attribution to geonames.org required.

Run the elevation pipeline:

```
python elevation/fetch_elevations.py --data ~/path/to/data-dir
```

The data directory must contain `SE.zip` and all unzipped SRTM `.hgt` tile files.

## SMHI pipeline

Language: Python 3. Dependencies: `pandas`, `numpy`, `requests`. See `requirements.txt`.

The pipeline fetches daily minimum temperature (parameter 26) and daily mean temperature (parameter 2) from SMHI's open data API for all active stations with records from 1991 to 2020. Raw observations are saved as CSVs to `data/observations/` which is gitignored. The derivation script reads those CSVs and computes per-station climate normals. The validate script checks output against known reference values before committing.

SMHI open data base URL: `https://opendata-download-metobs.smhi.se/api/version/1.0`

Run the SMHI pipeline in order:

```
python smhi/fetch_stations.py
python smhi/fetch_observations.py
python smhi/derive_normals.py
python smhi/validate.py
```

Only commit `output/weather_stations.json` after `validate.py` passes cleanly.

## Climate derivation methodology

**Last spring frost:** per station per year, the last calendar day where daily minimum temperature drops below 0°C. Median across 30 years gives `last_frost_doy`.

**First autumn frost:** per station per year, the first calendar day after July 1st where daily minimum temperature drops below 0°C. Median across 30 years gives `first_frost_doy`.

**Growing degree days:** per station per year, sum of (daily mean temperature − 5°C) for all days where daily mean exceeds 5°C. Mean across 30 years gives `gdd_annual`.

**Monthly mean temperatures:** per station, mean of daily mean temperature for each calendar month across 30 years. Stored as an array of 12 values indexed 0–11 (January–December).

Soil temperature is not stored. It is derived at query time in grow-zone-api using a 7-day rolling mean of daily mean air temperature plus a 1.5°C correction factor.

## Data sources

**GeoNames postcode data:** https://download.geonames.org/export/zip/ — download `SE.zip` and place it at `input/SE.zip`. Licence: Creative Commons Attribution 4.0 (attribute geonames.org). Refresh annually or when postcode boundaries change.

**SRTM elevation tiles:** USGS EarthExplorer or viewfinderpanoramas.org. Cover Sweden with tiles N55–N69, E010–E024. Files are large binary format — store outside the repository alongside `SE.zip` in the `--data` directory.

**SMHI open data:** https://opendata.smhi.se/apidocs/metobs/ — no API key required. Be a good citizen — add delays between requests.

**Normal period:** 1991–2020 (current WMO standard). Refresh when SMHI publishes 2001–2030 normals.

## Conventions

- Output files are always committed after a successful pipeline run
- Raw input data (tiles, CSVs) is always gitignored
- Scripts are self-documenting — every script has a header comment explaining what it does, what it depends on, and how to run it
- The validate script must pass before output files are committed
- No runtime API calls — all external data is fetched locally and stored in `data/` before derivation runs

## Gitignore

These are always ignored:

```
*.hgt
*.hgt.zip
data/
```

## Out of scope

This repository does not contain any application code, database connections, API routes, or business logic. It produces data files only. Any questions about how the data is used belong in the grow-zone-api repository.
