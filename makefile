# =============================================================================
# Storm Historian — Makefile
#
# Usage:
#   make ingest        — discover + download new NOAA files
#   make build         — full pipeline: ingest → dbt build → copy explore db
#   make app           — launch the Streamlit visualization app
#   make app-dev       — launch app reading directly from build DB
#   make dbt-build     — dbt build only (models + tests)
#   make dbt-test      — run dbt tests only
#   make dbt-run       — run dbt models only (no tests)
#   make dbt-compile   — compile SQL without executing
#   make dbt-docs      — generate dbt docs site
#   make dbt-deps      — install dbt packages
#   make dbt-debug     — check dbt connection + config
#   make dbt-clean     — remove dbt target/ and package dirs
#   make seed-crosswalk — download + write Census ZIP→county seed CSV
# =============================================================================

# --- Config (override on the command line: make build STORM_DUCKDB_PATH=...) ---
ROOT_DIR             := $(abspath .)
DBT_DIR              := $(ROOT_DIR)/storm_history_dbt
STORM_DUCKDB_PATH    ?= $(ROOT_DIR)/data/duckdb/warehouse_build.duckdb
STORM_DUCKDB_EXPLORE ?= $(ROOT_DIR)/data/duckdb/warehouse_explore.duckdb
RAW_DATA_DIR         ?= $(ROOT_DIR)/data/raw

# --- Internal helpers ---
DBT    := poetry run dbt --project-dir "$(DBT_DIR)" --profiles-dir "$(DBT_DIR)"
PYTHON := poetry run python
export STORM_DUCKDB_PATH

.PHONY: build ingest seed-crosswalk \
        dbt-build dbt-run dbt-test dbt-compile \
        dbt-docs dbt-deps dbt-debug dbt-clean

# -----------------------------------------------------------------------------
# Top-level targets
# -----------------------------------------------------------------------------

## Full pipeline: ingest → build → copy explore db
build: ingest dbt-build
	$(PYTHON) -c "import shutil; shutil.copy('$(STORM_DUCKDB_PATH)', '$(STORM_DUCKDB_EXPLORE)')"
	@echo "Build complete. Explore DB copied to $(STORM_DUCKDB_EXPLORE)"

## Discover and download new/updated NOAA storm event files
ingest:
	$(PYTHON) src/storm_historian/ingestion/discover_files.py
	$(PYTHON) src/storm_historian/ingestion/pull_file.py

## Download Census ZIP→county crosswalk and write seed CSV
seed-crosswalk:
	$(PYTHON) src/storm_historian/ingestion/prepare_crosswalk_seed.py
	$(DBT) seed --select zip_county_crosswalk

# -----------------------------------------------------------------------------
# dbt targets
# -----------------------------------------------------------------------------

## Build all dbt models and run all tests
dbt-build:
	$(DBT) build

## Run dbt models only (skip tests)
dbt-run:
	$(DBT) run
	$(PYTHON) -c "import shutil; shutil.copy('$(STORM_DUCKDB_PATH)', '$(STORM_DUCKDB_EXPLORE)')"

## Run dbt tests only
dbt-test:
	$(DBT) test

## Compile SQL without executing against the database
dbt-compile:
	$(DBT) compile

## Generate dbt documentation site
dbt-docs:
	$(DBT) docs generate

## Install / update dbt packages (run after changing packages.yml)
dbt-deps:
	$(DBT) deps

## Print dbt connection and config diagnostics
dbt-debug:
	$(DBT) debug

## Remove dbt target/ and dbt_packages/ directories
dbt-clean:
	$(DBT) clean

# -----------------------------------------------------------------------------
# App targets
# -----------------------------------------------------------------------------

## Launch the Streamlit app (reads from warehouse_explore.duckdb)
app:
	$(PYTHON) -m streamlit run src/streamlit_app.py

## Launch the app pointing at the build DB (for development/debugging only)
app-dev:
	$(PYTHON) -m streamlit run src/streamlit_app.py -- --db $(STORM_DUCKDB_PATH)
