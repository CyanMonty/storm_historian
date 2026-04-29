"""
Storm Historian — Streamlit entry point.

Placed at src/ level so Streamlit adds src/ to sys.path automatically,
making `from app.pages import ...` resolve cleanly without path manipulation.

Run with:
    streamlit run src/streamlit_app.py

Or via:
    make app
"""

import streamlit as st

from app.pages import overview, zip_explorer

st.set_page_config(
    page_title="Storm Historian",
    page_icon="🌩️",
    layout="wide",
    initial_sidebar_state="expanded",
)

pg = st.navigation(
    {
        "": [
            st.Page(overview.render, title="National Overview", icon="🌎", default=True, url_path="overview"),
            st.Page(zip_explorer.render, title="ZIP Code Explorer", icon="🔍", url_path="zip"),
        ]
    }
)

with st.sidebar:
    st.title("🌩️ Storm Historian")
    st.caption("NOAA storm events 1950–present")

pg.run()
