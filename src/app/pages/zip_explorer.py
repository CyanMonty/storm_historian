"""
ZIP Code Explorer page — the core feature of the app.

Lets users search by ZIP code and see:
  - Summary metrics (total events, deaths, damage)
  - Event type breakdown (bar chart)
  - Historical trend (events + damage over time, stacked by category)
  - Individual event table with drill-down narratives
  - Scatter map of individual event locations
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from app.db import (
    load_all_zip_codes,
    load_zip_events,
    load_zip_events_by_year,
    load_zip_summary,
)

# Event category colors — consistent across all charts
CATEGORY_COLORS = {
    "Tornado/Funnel": "#E45756",
    "Flood": "#4C78A8",
    "Wind": "#72B7B2",
    "Hail": "#54A24B",
    "Winter Weather": "#9ECAE9",
    "Tropical": "#F58518",
    "Lightning": "#EECA3B",
    "Temperature Extreme": "#B279A2",
    "Fire": "#FF9DA6",
    "Other": "#BAB0AC",
}


def render() -> None:
    st.header("ZIP Code Explorer")
    st.caption(
        "Search by ZIP code to see the storm history for that area. "
        "Counts include all events in the dominant county for each ZIP."
    )

    # --- Load ZIP list for validation + autocomplete ---
    try:
        zip_df = load_all_zip_codes()
    except FileNotFoundError as e:
        st.error(str(e))
        return

    valid_zips = set(zip_df["zip_code"].astype(str))

    # Use a form so pressing Enter triggers the search
    with st.form("zip_search"):
        zip_code = st.text_input("Enter ZIP code", max_chars=5, placeholder="e.g. 77002")
        submitted = st.form_submit_button("Search")

    if not submitted and not zip_code:
        st.info("Enter a 5-digit ZIP code above and press Search.")
        return

    zip_code = zip_code.strip().zfill(5)

    if len(zip_code) != 5 or not zip_code.isdigit():
        st.error("Please enter a valid 5-digit ZIP code.")
        return

    if zip_code not in valid_zips:
        st.error(f"ZIP code {zip_code} was not found in the crosswalk data.")
        return

    # --- ZIP metadata ---
    zip_meta = zip_df[zip_df["zip_code"] == zip_code].iloc[0]

    st.divider()
    st.subheader(f"ZIP {zip_code} — {zip_meta['county_name']}, State FIPS {zip_meta['state_fips']}")

    if zip_meta["is_multi_county"]:
        st.info(
            f"This ZIP spans {int(zip_meta['counties_in_zip'])} counties. "
            f"Events shown are for the dominant county "
            f"({zip_meta['dominant_county_area_ratio']*100:.0f}% of ZIP land area)."
        )

    # --- Pre-aggregated summary ---
    summary_df = load_zip_summary(zip_code)
    if summary_df.empty:
        st.warning(
            "No storm events found for this ZIP code's county. "
            "This may be a sparsely populated area with no NOAA records."
        )
        return

    row = summary_df.iloc[0]

    # Summary metric cards
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Total Events", f"{int(row['total_events']):,}")
    m2.metric("Deaths", f"{int(row['total_deaths']):,}")
    m3.metric("Injuries", f"{int(row['total_injuries']):,}")
    m4.metric("Total Damage", f"${row['total_damage_millions']:.1f}M")
    m5.metric(
        "Active Years",
        f"{int(row['years_with_events'])} "
        f"({int(row['first_event_year'])}–{int(row['last_event_year'])})",
    )

    st.divider()

    # --- Event type breakdown ---
    category_cols = {
        "Tornado/Funnel": row.get("tornado_count", 0),
        "Flood": row.get("flood_count", 0),
        "Wind": row.get("wind_count", 0),
        "Hail": row.get("hail_count", 0),
        "Winter Weather": row.get("winter_weather_count", 0),
        "Tropical": row.get("tropical_count", 0),
        "Lightning": row.get("lightning_count", 0),
        "Temperature Extreme": row.get("temp_extreme_count", 0),
        "Fire": row.get("fire_count", 0),
    }
    cat_df = (
        pd.DataFrame({"category": list(category_cols.keys()), "count": list(category_cols.values())})
        .query("count > 0")
        .sort_values("count", ascending=False)
    )

    col_bar, col_map = st.columns([1, 1])

    with col_bar:
        st.subheader("Events by Type")
        fig = px.bar(
            cat_df,
            x="count",
            y="category",
            orientation="h",
            color="category",
            color_discrete_map=CATEGORY_COLORS,
            labels={"count": "Events", "category": ""},
        )
        fig.update_layout(showlegend=False, margin=dict(t=10, b=10), height=320)
        st.plotly_chart(fig, width='stretch')

    # --- Historical trend ---
    trend_df = load_zip_events_by_year(zip_code)

    st.subheader("Events Over Time")
    if trend_df.empty:
        st.caption("No detailed event history available.")
    else:
        fig = px.bar(
            trend_df,
            x="year",
            y="event_count",
            color="event_category",
            color_discrete_map=CATEGORY_COLORS,
            labels={"year": "Year", "event_count": "Events", "event_category": "Category"},
            barmode="stack",
        )
        fig.update_layout(margin=dict(t=10, b=10), height=320, legend_title_text="")
        st.plotly_chart(fig, width='stretch')

    st.divider()

    # --- Individual events table + map ---
    st.subheader("Individual Events")

    events_df = load_zip_events(zip_code, limit=500)

    if events_df.empty:
        st.caption("No individual event records found.")
    else:
        # Map of event locations (only events with coordinates)
        mapped = events_df.dropna(subset=["begin_lat", "begin_lon"])
        if not mapped.empty:
            with col_map:
                st.subheader("Event Locations")
                center_lat = float(mapped["begin_lat"].mean())
                center_lon = float(mapped["begin_lon"].mean())
                fig_map = px.scatter_map(
                    mapped,
                    lat="begin_lat",
                    lon="begin_lon",
                    color="event_category",
                    color_discrete_map=CATEGORY_COLORS,
                    hover_name="event_type",
                    hover_data={
                        "begin_lat": False,
                        "begin_lon": False,
                        "total_deaths": True,
                        "total_damage_amount": ":,.0f",
                        "county": True,
                    },
                    zoom=6,
                    center={"lat": center_lat, "lon": center_lon},
                    map_style="carto-positron",
                    size_max=14,
                )
                fig_map.update_traces(marker=dict(size=8, opacity=0.8))
                fig_map.update_layout(margin=dict(t=10, b=0, l=0, r=0), height=340, showlegend=False)
                st.plotly_chart(fig_map, width='stretch')

        # Event table
        display_cols = [
            "begin_date_time", "event_type", "event_category",
            "county", "state", "total_deaths", "total_injuries", "total_damage_amount",
            "tor_f_scale",
        ]
        display_df = events_df[display_cols].copy()
        display_df.columns = [
            "Date", "Event Type", "Category",
            "County", "State", "Deaths", "Injuries", "Damage ($)",
            "Tornado Scale",
        ]
        display_df["Damage ($)"] = display_df["Damage ($)"].fillna(0).map("${:,.0f}".format)

        selected_rows = st.dataframe(
            display_df,
            width='stretch',
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
        )

        # Narrative drill-down
        if selected_rows and selected_rows.selection.rows:
            idx = selected_rows.selection.rows[0]
            event_row = events_df.iloc[idx]
            with st.expander("📋 Event Narrative", expanded=True):
                col_ep, col_ev = st.columns(2)
                with col_ep:
                    st.markdown("**Episode Narrative**")
                    st.write(event_row.get("episode_narrative") or "_No episode narrative available._")
                with col_ev:
                    st.markdown("**Event Narrative**")
                    st.write(event_row.get("event_narrative") or "_No event narrative available._")
