# dbt Models Documentation

This directory contains the dbt transformation models for the Storm Historian project.

## Directory Structure

```
models/
├── sources/          # Source configurations (raw CSV.GZ files)
├── staging/          # Staging layer (cleaning and normalization)
│   ├── base/        # Raw → typed conversion
│   └── stg/         # Cleaned and normalized views
├── intermediate/     # Business logic transformations
└── marts/           # Analytics-ready fact and dimension tables
```

## Model Layers

### Sources Layer
- Defines connections to raw CSV.GZ files from NOAA
- Uses DuckDB's `read_csv` to read compressed files directly
- No transformations, all columns as VARCHAR

### Staging Layer

**Base Models** (`staging/base/`)
- Read raw CSV.GZ files using `read_csv` with `all_varchar=true`
- Add `etl_inserted_at` timestamp
- Union all files by name
- Materialized as **tables** for performance

**Staging Models** (`staging/stg/`)
- Type casting and data cleaning
- Standardize column names (snake_case)
- Parse dates and handle nulls
- Materialized as **views**

Models:
- `stg_storm_event_details` - Main storm event data
- `stg_storm_event_fatalities` - Fatality records
- `stg_storm_event_locations` - Geographic location points

### Intermediate Layer

**Purpose:** Business logic transformations that don't belong in staging or marts

Models:
- `int_storm_event_details__damage_parsed` - Parses damage values from text format (K/M/B) to numeric USD

### Marts Layer

**Purpose:** Analytics-ready fact and dimension tables optimized for reporting

**Dimension Tables:**
- `dim_locations` - State/county/zone geographic dimension
- `dim_event_types` - Event type taxonomy with categorization

**Fact Tables:**
- `fct_storm_events` - Core fact table with all event metrics and denormalized dimensions
- `fct_fatalities` - Individual fatality records joined to event context

**Aggregate Tables:**
- `agg_events_by_state_year` - Pre-aggregated metrics for performance

All marts materialized as **tables** for query performance.

## Data Quality Tests

### Staging Tests
- Not null checks on key fields
- Uniqueness on primary keys
- Referential integrity between related tables
- Range validation (ages, years, coordinates)
- Accepted values for categorical fields

### Marts Tests
- All staging tests plus:
- Expression tests (e.g., end_date >= begin_date)
- Relationship tests to dimensions
- Custom data quality tests in `tests/` directory

### Custom Tests (`tests/`)
- `assert_reasonable_event_duration` - Events > 30 days flagged
- `assert_no_orphaned_fatalities` - Fatalities must reference valid events
- `assert_no_orphaned_locations` - Locations must reference valid events
- `assert_coordinates_in_us_bounds` - Coordinates within US geography

## Running the Models

1. **Install dbt packages:**
   ```bash
   make dbt-deps
   ```

2. **Compile models (check SQL):**
   ```bash
   make dbt-compile
   ```

3. **Run all models:**
   ```bash
   make dbt-run
   ```

4. **Run models and tests:**
   ```bash
   make dbt-build
   ```

5. **Test only:**
   ```bash
   make dbt-test
   ```

6. **Generate documentation:**
   ```bash
   make dbt-docs
   ```

## Model Dependencies

```
Sources (raw CSV.GZ)
    ↓
Base Models
    ↓
Staging Models
    ↓
Intermediate Models ←──┐
    ↓                  │
Dimensions             │
    ↓                  │
Facts ←────────────────┘
    ↓
Aggregates
```

## Key Metrics Available

### Event Metrics
- Total events, episodes
- Events by type/category
- Geographic distribution

### Impact Metrics
- Injuries (direct/indirect)
- Fatalities (direct/indirect)
- Property damage ($USD)
- Crop damage ($USD)

### Tornado Metrics
- F-scale/EF-scale ratings
- Path length and width
- Violent tornado counts (EF4/EF5)

## Notes

- All damage amounts converted from K/M/B notation to USD
- Coordinates validated for US geography
- Time zones preserved from source data
- Narratives retained for text analysis
