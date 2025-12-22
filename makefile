# --- User-configurable (can be overridden per machine) ---
ROOT_DIR := $(abspath .)
DBT_PROJECT_DIR := $(ROOT_DIR)/storm_history_dbt
DBT_PROFILES_DIR ?= $(ROOT_DIR)/storm_history_dbt
STORM_DUCKDB_PATH ?= $(ROOT_DIR)/data/duckdb/

# --- Internal ---
DBT := poetry run dbt

.PHONY: dbt-debug dbt-build dbt-run dbt-test dbt-clean dbt-docs dbt-compile

dbt-debug:
	STORM_DUCKDB_PATH="$(STORM_DUCKDB_PATH)" \
	$(DBT) debug --project-dir "$(DBT_PROJECT_DIR)" --profiles-dir "$(DBT_PROFILES_DIR)"

dbt-build:
	STORM_DUCKDB_PATH="$(STORM_DUCKDB_PATH)" \
	$(DBT) build --project-dir "$(DBT_PROJECT_DIR)" --profiles-dir "$(DBT_PROFILES_DIR)"

dbt-run:
	STORM_DUCKDB_PATH="$(STORM_DUCKDB_PATH)" \
	$(DBT) run --project-dir "$(DBT_PROJECT_DIR)" --profiles-dir "$(DBT_PROFILES_DIR)"

dbt-test:
	STORM_DUCKDB_PATH="$(STORM_DUCKDB_PATH)" \
	$(DBT) test --project-dir "$(DBT_PROJECT_DIR)" --profiles-dir "$(DBT_PROFILES_DIR)"

dbt-compile:
	STORM_DUCKDB_PATH="$(STORM_DUCKDB_PATH)" \
	$(DBT) compile --project-dir "$(DBT_PROJECT_DIR)" --profiles-dir "$(DBT_PROFILES_DIR)"

dbt-clean:
	$(DBT) clean --project-dir "$(DBT_PROJECT_DIR)" --profiles-dir "$(DBT_PROFILES_DIR)"

dbt-docs:
	STORM_DUCKDB_PATH="$(STORM_DUCKDB_PATH)" \
	$(DBT) docs generate --project-dir "$(DBT_PROJECT_DIR)" --profiles-dir "$(DBT_PROFILES_DIR)"
