"""
DuckDB connection and cached query helpers for the Streamlit app.

Always reads from warehouse_explore.duckdb (never the build database).
The DB path is resolved relative to the repo root so the app can be launched
from any working directory.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_DB = _REPO_ROOT / "data" / "duckdb" / "warehouse_explore.duckdb"


def _db_path() -> str:
    """Return the explore DB path.

    Resolution order:
    1. `--db` argument passed via `streamlit run app.py -- --db /path`
    2. STORM_EXPLORE_DB environment variable
    3. Default: data/duckdb/warehouse_explore.duckdb next to repo root
    """
    # Streamlit passes script args after `--`; check sys.argv for --db
    import sys
    args = sys.argv
    if "--db" in args:
        path = Path(args[args.index("--db") + 1])
    else:
        path = Path(os.environ.get("STORM_EXPLORE_DB", str(_DEFAULT_DB)))

    if not path.exists():
        raise FileNotFoundError(
            f"Database not found at {path}. "
            "Run `make build` (or `make dbt-build`) to generate it."
        )
    return str(path)


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """Return a shared read-only DuckDB connection (cached for the app lifetime)."""
    return duckdb.connect(_db_path(), read_only=True)


@st.cache_resource
def _marts() -> str:
    """
    Return the fully-qualified marts schema name for the current connection.

    dbt-duckdb names schemas <catalog>_<layer> where the catalog is the DB
    filename stem. Discovering it at runtime makes the app work for both
    warehouse_build.duckdb and warehouse_explore.duckdb without hardcoding.
    """
    conn = get_connection()
    db_name = conn.execute("SELECT current_database()").fetchone()[0]
    return f"{db_name}.main_marts"


# ---------------------------------------------------------------------------
# Generic query helper
# ---------------------------------------------------------------------------


def query(sql: str, params: list | None = None) -> pd.DataFrame:
    """Execute SQL and return a DataFrame. Schema token {m} is replaced with
    the fully-qualified marts schema before execution."""
    conn = get_connection()
    resolved = sql.replace("{m}", _marts())
    if params:
        return conn.execute(resolved, params).df()
    return conn.execute(resolved).df()


# ---------------------------------------------------------------------------
# Cached data loaders
# ---------------------------------------------------------------------------


@st.cache_data(ttl=3600)
def load_all_zip_codes() -> pd.DataFrame:
    """Load the full ZIP code dimension for autocomplete and validation."""
    return query(
        """
        select
            zip_code,
            county_name,
            state_fips,
            is_multi_county,
            counties_in_zip,
            dominant_county_area_ratio,
            location_key
        from {m}.dim_zip_codes
        order by zip_code
        """
    )


@st.cache_data(ttl=3600)
def load_national_summary() -> dict:
    """High-level national totals for the overview page."""
    row = query(
        """
        select
            count(*)                                as total_events,
            sum(total_deaths)                       as total_deaths,
            sum(total_injuries)                     as total_injuries,
            round(sum(total_damage_amount) / 1e9, 2) as total_damage_billions,
            min(year)                               as first_year,
            max(year)                               as last_year,
            count(distinct state_fips)              as states_covered
        from {m}.fct_storm_events
        """
    ).iloc[0]
    return row.to_dict()


@st.cache_data(ttl=3600)
def load_state_summary(state: str) -> dict:
    """Totals for a single state, matching the shape of load_national_summary."""
    row = query(
        """
        select
            count(*)                                as total_events,
            sum(total_deaths)                       as total_deaths,
            sum(total_injuries)                     as total_injuries,
            round(sum(total_damage_amount) / 1e9, 2) as total_damage_billions,
            min(year)                               as first_year,
            max(year)                               as last_year
        from {m}.fct_storm_events
        where state = ?
        """,
        [state],
    ).iloc[0]
    return row.to_dict()


@st.cache_data(ttl=3600)
def load_events_by_year(state: str | None = None) -> pd.DataFrame:
    """Annual event / damage / death totals for trend charts.

    Pass *state* (NOAA upper-case name) to restrict to a single state.
    """
    if state is None:
        return query(
            """
            select
                year,
                sum(total_events)           as total_events,
                sum(total_deaths)           as total_deaths,
                sum(total_injuries)         as total_injuries,
                sum(total_damage_millions)  as total_damage_millions
            from {m}.agg_events_by_state_year
            group by year
            order by year
            """
        )
    return query(
        """
        select
            year,
            sum(total_events)           as total_events,
            sum(total_deaths)           as total_deaths,
            sum(total_injuries)         as total_injuries,
            sum(total_damage_millions)  as total_damage_millions
        from {m}.agg_events_by_state_year
        where state = ?
        group by year
        order by year
        """,
        [state],
    )


@st.cache_data(ttl=3600)
def load_events_by_year_category(state: str | None = None) -> pd.DataFrame:
    """Annual totals broken down by event_category for stacked charts."""
    if state is None:
        return query(
            """
            select
                year,
                event_category,
                count(*)                                    as total_events,
                round(sum(total_damage_amount) / 1e6, 2)   as total_damage_millions
            from {m}.fct_storm_events
            group by year, event_category
            order by year, event_category
            """
        )
    return query(
        """
        select
            year,
            event_category,
            count(*)                                    as total_events,
            round(sum(total_damage_amount) / 1e6, 2)   as total_damage_millions
        from {m}.fct_storm_events
        where state = ?
        group by year, event_category
        order by year, event_category
        """,
        [state],
    )


@st.cache_data(ttl=3600)
def load_events_by_state() -> pd.DataFrame:
    """State-level totals for the national choropleth."""
    return query(
        """
        select
            state,
            state_fips,
            sum(total_events)           as total_events,
            sum(total_deaths)           as total_deaths,
            sum(total_damage_millions)  as total_damage_millions
        from {m}.agg_events_by_state_year
        group by state, state_fips
        order by total_events desc
        """
    )


@st.cache_data(ttl=3600)
def load_state_hotspots(state: str) -> pd.DataFrame:
    """County-level event hotspots for a single state, with centroid lat/lon.

    Uses avg_latitude/avg_longitude from fct_storm_events (pre-computed from
    location points) and falls back to begin_lat/begin_lon when absent.
    """
    return query(
        """
        select
            cz_name                                             as county,
            count(*)                                            as total_events,
            sum(total_deaths)                                   as total_deaths,
            sum(total_injuries)                                 as total_injuries,
            round(sum(total_damage_amount) / 1e6, 2)           as total_damage_millions,
            avg(coalesce(avg_latitude,  begin_lat))             as lat,
            avg(coalesce(avg_longitude, begin_lon))             as lon
        from {m}.fct_storm_events
        where state = ?
          and coalesce(avg_latitude, begin_lat) is not null
          and coalesce(avg_longitude, begin_lon) is not null
        group by cz_name
        having count(*) > 0
        order by total_events desc
        """,
        [state],
    )


# ---------------------------------------------------------------------------
# ZIP-specific loaders (not cached — depend on user input)
# ---------------------------------------------------------------------------


def load_zip_summary(zip_code: str) -> pd.DataFrame:
    """Single-row summary for a ZIP from the pre-aggregated mart."""
    return query(
        """
        select *
        from {m}.agg_events_by_zip
        where zip_code = ?
        """,
        [zip_code],
    )


def load_zip_events(zip_code: str, limit: int = 500) -> pd.DataFrame:
    """
    Individual storm events for a ZIP code's dominant county.
    Joins via dim_zip_codes to resolve county FIPS, then filters fct_storm_events.
    Limited to avoid loading millions of rows into the browser.
    """
    return query(
        """
        select
            e.event_id,
            e.begin_date_time,
            e.year,
            e.event_type,
            e.event_category,
            e.cz_name                   as county,
            e.state,
            e.total_deaths,
            e.total_injuries,
            e.total_damage_amount,
            e.tor_f_scale,
            e.episode_narrative,
            e.event_narrative,
            e.begin_lat,
            e.begin_lon
        from {m}.fct_storm_events e
        inner join {m}.dim_zip_codes z
            on  lpad(cast(e.state_fips as varchar), 2, '0') = z.state_fips
            and lpad(cast(e.cz_fips as varchar), 3, '0')    = z.county_fips
        where z.zip_code = ?
          and e.cz_type = 'C'
        order by e.begin_date_time desc
        limit ?
        """,
        [zip_code, limit],
    )


def load_zip_events_by_year(zip_code: str) -> pd.DataFrame:
    """Annual breakdown of events for a ZIP's dominant county."""
    return query(
        """
        select
            e.year,
            e.event_category,
            count(*)                                        as event_count,
            sum(e.total_deaths)                             as total_deaths,
            round(sum(e.total_damage_amount) / 1e6, 2)     as damage_millions
        from {m}.fct_storm_events e
        inner join {m}.dim_zip_codes z
            on  lpad(cast(e.state_fips as varchar), 2, '0') = z.state_fips
            and lpad(cast(e.cz_fips as varchar), 3, '0')    = z.county_fips
        where z.zip_code = ?
          and e.cz_type = 'C'
        group by e.year, e.event_category
        order by e.year
        """,
        [zip_code],
    )

