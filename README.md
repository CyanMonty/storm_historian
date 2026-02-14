# Storm Historian

A data engineering project for ingesting, transforming, and analyzing NOAA Storm Events data. This project implements a complete ELT (Extract, Load, Transform) pipeline to process historical storm event data from NOAA's Storm Events Database.

## Overview

Storm Historian automates the discovery, ingestion, and transformation of publicly available storm event data from [NOAA's Storm Events Database](https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/). The pipeline tracks file states, downloads new or updated files, and transforms the raw data into cleaned, analytics-ready datasets using dbt.

## Features

- **Automated File Discovery**: Scans NOAA's data repository for new or updated storm event files
- **State Management**: Tracks downloaded files and their modification dates using DuckDB
- **Incremental Ingestion**: Only downloads new or modified files
- **Data Transformation**: dbt models for staging and transforming raw storm data
- **Three Data Streams**:
  - Storm Event Details (main event information)
  - Storm Event Fatalities (fatality records)
  - Storm Event Locations (geographic data)

## Tech Stack

- **Python 3.12+**: Core programming language
- **DuckDB**: Embedded analytical database for data warehousing and state management
- **dbt**: Data transformation and modeling framework
- **Polars**: High-performance DataFrame library for data processing
- **Poetry**: Dependency management and packaging
- **Additional libraries**: Requests, BeautifulSoup4, Pydantic, PyYAML, Streamlit, Plotly

## Project Structure

```
storm_historian/
├── src/storm_historian/
│   ├── ingestion/
│   │   ├── discover_files.py    # Discovers available files from NOAA
│   │   ├── pull_file.py         # Downloads files to local storage
│   │   ├── source_info.yml      # Configuration for data sources
│   │   └── source_model.py      # Pydantic models for sources
│   └── data_exploration.py      # Ad-hoc data exploration scripts
├── storm_history_dbt/
│   ├── models/
│   │   ├── sources/             # Source configurations
│   │   └── staging/             # Staging transformations
│   │       ├── base/            # Base models (raw → typed)
│   │       └── stg/             # Staging models (cleaned)
│   ├── dbt_project.yml          # dbt project configuration
│   └── profiles.yml             # dbt connection profiles
├── data/
│   ├── duckdb/                  # DuckDB database files
│   │   ├── warehouse_build.duckdb    # Main transformation warehouse
│   │   ├── warehouse_explore.duckdb  # Copy for exploration
│   │   └── state.duckdb             # File tracking state
│   └── raw/                     # Downloaded CSV.GZ files
│       ├── storm_event_details/
│       ├── storm_event_fatalities/
│       └── storm_event_locations/
├── pyproject.toml               # Poetry dependencies
├── makefile                     # dbt workflow commands
└── README.md
```

## Installation

### Prerequisites

- Python 3.12 or higher
- [Poetry](https://python-poetry.org/docs/#installation) for dependency management

### Setup Steps

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd storm_historian
   ```

2. **Install dependencies using Poetry**
   ```bash
   poetry install
   ```

3. **Activate the Poetry shell**
   ```bash
   poetry shell
   ```

## Usage

### 1. Discover Available Files

Scan NOAA's repository to find available storm event files and log them to the state database:

```bash
python src/storm_historian/ingestion/discover_files.py
```

This script:
- Scrapes the NOAA FTP directory listing
- Identifies CSV.GZ files matching configured patterns
- Records file metadata (name, URL, size, last modified date) in `data/duckdb/state.duckdb`
- Only updates records if files have been modified

### 2. Download New Files

Download files that haven't been pulled yet:

```bash
python src/storm_historian/ingestion/pull_file.py
```

This script:
- Queries the state database for undownloaded files
- Downloads them to the appropriate `data/raw/` subdirectory
- Updates the state database to mark files as downloaded
- Handles partial downloads gracefully

### 3. Run dbt Transformations

Use the provided Makefile commands to run dbt workflows:

```bash
# Debug dbt connection
make dbt-debug

# Compile dbt models (verify SQL syntax)
make dbt-compile

# Run all models (staging transformations)
make dbt-run

# Build models and run tests
make dbt-build

# Run data quality tests
make dbt-test

# Generate dbt documentation
make dbt-docs

# Clean dbt artifacts
make dbt-clean
```

The dbt transformations:
- Read raw CSV.GZ files using DuckDB's `read_csv` functionality
- Apply type conversions and data cleaning
- Create staging views with standardized column names
- Prepare data for downstream analytics

### 4. Data Exploration

After running transformations, explore the data:

```bash
# Use the explore database (read-only copy)
duckdb data/duckdb/warehouse_explore.duckdb
```

Or use the provided exploration scripts in `src/data_exploration.py`.

## Configuration

### Source Configuration

Edit `src/storm_historian/ingestion/source_info.yml` to modify data sources:

```yaml
sources:
  - name: storm_event_details
    url: https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/
    prefix: StormEvents_details
    suffixes: 
      - csv.gz
    description: storm event details
    type: directory
```

### dbt Configuration

- **Project settings**: `storm_history_dbt/dbt_project.yml`
- **Connection profiles**: `storm_history_dbt/profiles.yml`
- **Model configurations**: YAML files in `models/` subdirectories

### Environment Variables

The Makefile uses the following environment variables (with defaults):

- `STORM_DUCKDB_PATH`: Path to the main DuckDB warehouse (default: `data/duckdb/warehouse_build.duckdb`)
- `DBT_PROFILES_DIR`: Path to dbt profiles directory (default: `storm_history_dbt/`)

## Data Pipeline Flow

```
1. Discovery Phase
   NOAA FTP Server → discover_files.py → state.duckdb (file_tracker table)

2. Ingestion Phase
   state.duckdb → pull_file.py → data/raw/*.csv.gz

3. Transformation Phase (dbt)
   data/raw/*.csv.gz → base models → staging models → warehouse_build.duckdb

4. Exploration Phase
   warehouse_build.duckdb → warehouse_explore.duckdb (read-only copy)
```

## Development

### Code Quality

The project uses the following tools (configured in `pyproject.toml`):

- **Ruff**: Python linting and formatting
- **pytest**: Testing framework
- **SQLFluff**: SQL linting for dbt models

### Running Tests

```bash
poetry run pytest
```

## Future Enhancements

- Apache Airflow orchestration for scheduled ingestion
- Docker containerization for deployment
- Interactive dashboards using Streamlit/Plotly
- Additional dbt mart models for analytics
- Data quality tests and validation rules

## License

[Add your license information here]

## Data Source

Data is sourced from:
- **NOAA Storm Events Database**: https://www.ncei.noaa.gov/pub/data/swdi/stormevents/
- **Data Catalog**: https://catalog.data.gov/dataset/ncdc-storm-events-database

Please refer to NOAA's usage terms and conditions when using this data.

