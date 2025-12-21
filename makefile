# --- User-configurable (can be overridden per machine) ---
ROOT_DIR := $(abspath .)
DBT_PROJECT_DIR := $(ROOT_DIR)/storm_history_dbt
DBT_PROFILES_DIR ?= $(ROOT_DIR)/storm_history_dbt
STORM_DUCKDB_PATH ?= $(ROOT_DIR)/data/duckdb/

# --- Internal ---
DBT := poetry run dbt

.PHONY: dbt-debug dbt-build dbt-run dbt-test dbt-clean dbt-docs

dbt-debug:
	DBT_PROFILES_DIR="$(DBT_PROFILES_DIR)" STORM_DUCKDB_PATH="$(STORM_DUCKDB_PATH)" \
	$(DBT) debug --project-dir "$(DBT_PROJECT_DIR)"

dbt-build:
	DBT_PROFILES_DIR="$(DBT_PROFILES_DIR)" STORM_DUCKDB_PATH="$(STORM_DUCKDB_PATH)" \
	$(DBT) build --project-dir "$(DBT_PROJECT_DIR)"

dbt-run:
	DBT_PROFILES_DIR="$(DBT_PROFILES_DIR)" STORM_DUCKDB_PATH="$(STORM_DUCKDB_PATH)" \
	$(DBT) run --project-dir "$(DBT_PROJECT_DIR)"

dbt-test:
	DBT_PROFILES_DIR="$(DBT_PROFILES_DIR)" STORM_DUCKDB_PATH="$(STORM_DUCKDB_PATH)" \
	$(DBT) test --project-dir "$(DBT_PROJECT_DIR)"

dbt-clean:
	$(DBT) clean --project-dir "$(DBT_PROJECT_DIR)"

dbt-docs:
	DBT_PROFILES_DIR="$(DBT_PROFILES_DIR)" STORM_DUCKDB_PATH="$(STORM_DUCKDB_PATH)" \
	$(DBT) docs generate --project-dir "$(DBT_PROJECT_DIR)"
