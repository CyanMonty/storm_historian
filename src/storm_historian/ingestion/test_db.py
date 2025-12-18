import duckdb
from pathlib import Path
import streamlit as st
import plotly.express as px

data_dir = Path(__file__).parent.resolve() / "../../../" / "data"
conn = duckdb.connect(data_dir / "duckdb" / "state.db")

resp = conn.execute("""
   delete from file_tracker
""").fetchall()
print(resp)