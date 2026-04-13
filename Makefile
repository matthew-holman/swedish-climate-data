DATA ?= input
export PYTHONPATH := $(shell pwd)

# ─────────────────────────────────────────────────────────────────────────────
# Help
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: help
help:
	@echo ""
	@echo "Usage: make <target> [DATA=~/path/to/data-dir]"
	@echo ""
	@echo "  DATA defaults to ./input — place SE.zip and .hgt tiles there,"
	@echo "  or override with DATA=~/path/to/your/data-dir"
	@echo ""
	@echo "Setup"
	@echo "  install       Install Python dependencies via Poetry"
	@echo ""
	@echo "Elevation pipeline"
	@echo "  elevation     Parse SE.zip + SRTM tiles → output/postcodes-enriched.json"
	@echo ""
	@echo "SMHI pipeline"
	@echo "  smhi              Run full pipeline (real SMHI observations)"
	@echo "  smhi-fake         Run full pipeline with synthetic observations"
	@echo "  smhi-stations     Step 1: fetch active station list"
	@echo "  smhi-obs          Step 2: download observations from SMHI API (slow)"
	@echo "  smhi-fake-obs     Step 2 (alt): generate synthetic observations for dev/testing"
	@echo "  smhi-normals      Step 3: derive climate normals from CSVs"
	@echo "  smhi-validate     Step 4: validate output before committing"
	@echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Setup
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: install
install:
	poetry install

# ─────────────────────────────────────────────────────────────────────────────
# Elevation pipeline
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: elevation
elevation:
	poetry run python elevation/fetch_elevations.py --data $(DATA)

# ─────────────────────────────────────────────────────────────────────────────
# SMHI pipeline
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: smhi-stations
smhi-stations:
	poetry run python smhi/fetch_stations.py

.PHONY: smhi-obs
smhi-obs:
	poetry run python smhi/fetch_observations.py

.PHONY: smhi-fake-obs
smhi-fake-obs:
	poetry run python smhi/fetch_fake_observations.py

.PHONY: smhi-normals
smhi-normals:
	poetry run python smhi/derive_normals.py

.PHONY: smhi-validate
smhi-validate:
	poetry run python smhi/validate.py

.PHONY: smhi
smhi:
	poetry run python smhi/run_pipeline.py

.PHONY: smhi-fake
smhi-fake:
	poetry run python smhi/run_pipeline.py --fake
