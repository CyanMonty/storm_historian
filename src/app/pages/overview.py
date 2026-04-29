"""
National Overview page — high-level national statistics and trends.
"""

from __future__ import annotations

import plotly.express as px
import streamlit as st

from app.db import (
    load_events_by_state,
    load_events_by_year,
    load_events_by_year_category,
    load_national_summary,
    load_state_hotspots,
    load_state_summary,
)

# NOAA uses upper-case full state names; map them to USPS 2-letter codes for
# Plotly's built-in USA-states locationmode (no external GeoJSON needed).
_STATE_ABBREV: dict[str, str] = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR",
    "CALIFORNIA": "CA", "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE",
    "FLORIDA": "FL", "GEORGIA": "GA", "HAWAII": "HI", "IDAHO": "ID",
    "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA", "KANSAS": "KS",
    "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD",
    "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN", "MISSISSIPPI": "MS",
    "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY",
    "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK",
    "OREGON": "OR", "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT",
    "VERMONT": "VT", "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI", "WYOMING": "WY", "DISTRICT OF COLUMBIA": "DC",
}


def render() -> None:
    # --- Load state list (needed early for the filter widget) ---
    try:
        state_df = load_events_by_state()
    except FileNotFoundError as e:
        st.error(str(e))
        return

    state_df["state_abbrev"] = state_df["state"].map(_STATE_ABBREV)
    state_df_map = state_df.dropna(subset=["state_abbrev"])  # US states only

    # Title-cased names for the selectbox; keep uppercase internally for SQL.
    _ALL = "All States"
    state_options = [_ALL] + sorted(
        s.title() for s in state_df_map["state"].tolist()
    )

    # --- State filter — top of page ---
    selected_label = st.selectbox("State", state_options, index=0)
    selected_state: str | None = (
        None if selected_label == _ALL else selected_label.upper()
    )

    st.header(
        "National Storm Events Overview"
        if selected_state is None
        else f"{selected_label} — Storm Events Overview"
    )

    # --- Summary metrics ---
    if selected_state is None:
        try:
            summary = load_national_summary()
        except FileNotFoundError as e:
            st.error(str(e))
            return
        caption = (
            f"Data covers {int(summary['first_year'])}–{int(summary['last_year'])} "
            f"across {int(summary['states_covered'])} states/territories."
        )
    else:
        summary = load_state_summary(selected_state)
        caption = (
            f"Data covers {int(summary['first_year'])}–{int(summary['last_year'])}."
        )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Events", f"{int(summary['total_events']):,}")
    col2.metric("Total Deaths", f"{int(summary['total_deaths']):,}")
    col3.metric("Total Injuries", f"{int(summary['total_injuries']):,}")
    col4.metric("Total Damage", f"${summary['total_damage_billions']:.1f}B")
    st.caption(caption)

    st.divider()

    # --- Annual trends ---
    cat_df = load_events_by_year_category(state=selected_state)

    # Consistent category colors (same palette as ZIP explorer)
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

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Events per Year")
        fig = px.bar(
            cat_df,
            x="year",
            y="total_events",
            color="event_category",
            color_discrete_map=CATEGORY_COLORS,
            labels={"year": "Year", "total_events": "Events", "event_category": "Category"},
            barmode="stack",
        )
        fig.update_layout(margin=dict(t=10, b=10), height=300, legend_title_text="")
        st.plotly_chart(fig, width='stretch')

    with col_right:
        st.subheader("Total Damage per Year ($M)")
        fig = px.bar(
            cat_df,
            x="year",
            y="total_damage_millions",
            color="event_category",
            color_discrete_map=CATEGORY_COLORS,
            labels={"year": "Year", "total_damage_millions": "Damage ($M)", "event_category": "Category"},
            barmode="stack",
        )
        fig.update_layout(margin=dict(t=10, b=10), height=300, legend_title_text="")
        st.plotly_chart(fig, width='stretch')

    st.divider()

    # --- Map section ---
    label_map = {
        "total_events": "Events",
        "total_deaths": "Deaths",
        "total_damage_millions": "Damage ($M)",
    }
    metric = st.selectbox(
        "Color by",
        list(label_map.keys()),
        format_func=lambda x: {
            "total_events": "Total Events",
            "total_deaths": "Total Deaths",
            "total_damage_millions": "Total Damage ($M)",
        }[x],
    )

    if selected_state is None:
        # --- National choropleth (tile-based) ---
        st.subheader("Events by State")
        import json, urllib.request
        _STATES_GEOJSON_URL = (
            "https://raw.githubusercontent.com/python-visualization/folium"
            "/master/examples/data/us-states.json"
        )
        @st.cache_data(ttl=86400)
        def _load_states_geojson() -> dict:
            with urllib.request.urlopen(_STATES_GEOJSON_URL) as r:
                return json.loads(r.read())

        states_geo = _load_states_geojson()
        choropleth_df = state_df_map[["state", "state_abbrev", "total_events", "total_deaths", "total_damage_millions"]].copy()
        fig = px.choropleth_map(
            choropleth_df,
            geojson=states_geo,
            locations="state_abbrev",
            featureidkey="id",
            color=metric,
            hover_name="state",
            hover_data={
                "state_abbrev": False,
                "total_events": ":,",
                "total_deaths": ":,",
                "total_damage_millions": ":.1f",
            },
            color_continuous_scale="YlOrRd",
            map_style="carto-darkmatter",
            zoom=3,
            center={"lat": 38, "lon": -96},
            opacity=0.75,
            labels=label_map,
        )
        fig.update_traces(marker_line_color="rgba(255,255,255,0.3)", marker_line_width=0.8)
        fig.update_layout(
            margin=dict(t=0, b=0, l=0, r=0),
            height=480,
            coloraxis_colorbar=dict(title=label_map[metric], thickness=14, len=0.75),
        )
        st.plotly_chart(fig, width='stretch')
    else:
        # --- State hotspot bubble map ---
        st.subheader(f"Event Hotspots — {selected_label}")
        hotspot_df = load_state_hotspots(selected_state)
        if hotspot_df.empty:
            st.caption("No geocoded events found for this state.")
        else:
            fig = px.scatter_map(
                hotspot_df,
                lat="lat",
                lon="lon",
                size=metric,
                color=metric,
                hover_name="county",
                hover_data={
                    "lat": False,
                    "lon": False,
                    "total_events": ":,",
                    "total_deaths": ":,",
                    "total_damage_millions": ":.1f",
                },
                color_continuous_scale="YlOrRd",
                size_max=40,
                zoom=5,
                center={"lat": hotspot_df["lat"].mean(), "lon": hotspot_df["lon"].mean()},
                map_style="carto-darkmatter",
                labels=label_map,
                opacity=0.7,
            )
            fig.update_layout(
                margin=dict(t=0, b=0, l=0, r=0),
                height=500,
                coloraxis_colorbar=dict(title=label_map[metric], thickness=14, len=0.75),
            )
            st.plotly_chart(fig, width='stretch')
