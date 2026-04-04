# Swedish Climate Data Pipeline

Produces static geographic and climate seed datasets for the [Swedish Grow Calendar](https://github.com/your-org/grow-zone-api) project. The pipeline outputs two JSON files that are consumed by the `grow-zone-api` repository to seed its database.

This repo has no server, no API, and no database. You run it locally, commit the output files, then copy them across to `grow-zone-api`.

---

## Outputs

| File | Description |
|---|---|
| `output/postcodes-enriched.json` | One record per Swedish postcode: `postcode`, `lat`, `lng`, `placeName`, `adminName1`, `elevationM` |
| `output/weather_stations.json` | One record per SMHI weather station with 30-year climate normals: frost dates, growing degree days, monthly mean temperatures |

Both files are committed to the repository after a successful pipeline run.

---

## Prerequisites

- Python 3.12+
- [Poetry](https://python-poetry.org/docs/#installation)

---

## Setup

```bash
git clone <repo-url>
cd swedish-climate-data
make install
```

---

## Running the pipelines

### Elevation pipeline

Produces `output/postcodes-enriched.json` from GeoNames postcode data and SRTM elevation tiles.

**Step 1 — Gather input data**

Place the following files in the repo's `input/` directory (the default data dir):

- **`SE.zip`** — GeoNames postcode file for Sweden.
  Download from https://download.geonames.org/export/zip/ and place the zip directly in the directory (do not unzip it).

- **SRTM `.hgt` tile files** — elevation tiles covering Sweden (N55–N69, E010–E024).
  Download from [USGS EarthExplorer](https://e4ftl01.cr.usgs.gov/MEASURES/SRTMGL1.003/2000.02.11/) (free NASA Earthdata account required) or [viewfinderpanoramas.org](http://viewfinderpanoramas.org/). Unzip each tile so the directory contains bare `.hgt` files, e.g. `N59E018.hgt`.

Both are gitignored and will never be committed.

**Step 2 — Run the script**

```bash
make elevation
```

If your data files are in a different directory, pass it as an override:

```bash
make elevation DATA=~/path/to/data-dir
```

The script prints progress and a summary of any missing tiles. Postcodes with no matching tile get `elevationM: null` in the output rather than being dropped.

---

### SMHI pipeline

Produces `output/weather_stations.json` from 30 years of SMHI daily temperature observations (1991–2020).

```bash
make smhi
```

This runs all four steps in order. You can also run them individually:

```bash
make smhi-stations   # Step 1: fetch active station list
make smhi-obs        # Step 2: download 30 years of observations (slow)
make smhi-normals    # Step 3: derive climate normals from CSVs
make smhi-validate   # Step 4: validate output before committing
```

Raw observation CSVs are saved to `data/observations/` which is gitignored. Only commit `output/weather_stations.json` after `smhi-validate` passes cleanly.

---

## Deploying the output

Copy both output files to `grow-zone-api` before running its seed scripts:

```bash
cp output/postcodes-enriched.json ../grow-zone-api/src/data/
cp output/weather_stations.json ../grow-zone-api/src/data/
```

---

## Data sources and licences

| Source | Licence |
|---|---|
| [GeoNames](https://download.geonames.org/export/zip/) postcode data | [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/) — attribution to geonames.org required |
| [USGS SRTM](https://e4ftl01.cr.usgs.gov/MEASURES/SRTMGL1.003/) elevation tiles | Public domain |
| [SMHI open data](https://opendata.smhi.se/apidocs/metobs/) temperature observations | [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/) |
