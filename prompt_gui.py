import streamlit as st
import time
import pandas as pd

# Component imports
from runtime_analytics.gui.components import (
    header,
    sidebar,
    tab_predefined,
    tab_interpreter,
    tab_admin,
    tab_drift_analysis,
    tab_accuracy,
)

from runtime_analytics.app_config.config import settings
from runtime_analytics.app_db.db_loader import load_df_from_db,  create_indexes
from runtime_analytics.app_db.db_operations import save_df_to_db
from runtime_analytics.loader import load_logs_from_folder

# Set layout and sidebar
header.set_layout()
mode, auto_refresh = sidebar.render_sidebar()

# Auto-refresh
if auto_refresh and mode == "Live from DB":
    time.sleep(30)
    st.experimental_rerun()

# Load data
df = None
if mode == "Live from DB":
    with st.spinner("Refreshing data from the database..."):
        try:
            df = load_df_from_db()
            if df.empty:
                st.warning("No data found in the database.")
            else:
                st.success(f"Loaded {len(df)} rows from the database.")
        except Exception as e:
            st.error(f"Error loading data from DB: {e}")

elif mode == "Parse from logs":
    with st.spinner("Parsing logs and loading into database..."):
        df = load_logs_from_folder(settings.log_dir)
        if df.empty:
            st.warning("No valid logs found in the log folder.")
        else:
            save_df_to_db(df, if_exists="append")
            create_indexes()
            st.success(f"Parsed and saved {len(df)} logs to the database.")

# Tabs
tabs = st.tabs([
    "Predefined Prompts",
    "AI Prompt Interpreter",
    "Admin",
    "Sequence & Drift Analysis",
    "Visualise Prediction Accuracy (trained vs actual)"
])

# Pass df to renderers that need it
tab_predefined.render(tabs[0], mode, df)
tab_interpreter.render(tabs[1], mode, df)
tab_admin.render(tabs[2])
tab_drift_analysis.render(tabs[3])
tab_accuracy.render(tabs[4])
