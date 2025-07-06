import streamlit as st
import sqlite3
import time
import pandas as pd
from loguru import logger

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
from runtime_analytics.app_db.db_loader import load_df_from_db, create_indexes
from runtime_analytics.app_db.db_operations import save_df_to_db
from runtime_analytics.etl.loader import load_logs_from_folder


def load_data_if_needed():
    """Load and cache DataFrame for Predefined and Interpreter tabs."""
    try:
        with sqlite3.connect(settings.log_db_path) as conn:
            result = conn.execute("SELECT MAX(run_date) FROM job_logs").fetchone()
            latest_date = result[0] if result and result[0] else None

        if not latest_date:
            st.warning("No job logs found in database.")
            return

        df_loaded = st.session_state.get("df")
        loaded_dates = (
            df_loaded["run_date"].unique()
            if isinstance(df_loaded, pd.DataFrame) and "run_date" in df_loaded.columns
            else []
        )
        needs_reload = df_loaded is None or latest_date not in loaded_dates

        if needs_reload:
            with st.spinner(f"Loading latest logs for run_date: {latest_date}"):
                df = load_df_from_db(filters={"run_date": latest_date})
                if df.empty:
                    st.warning(f"No logs found for run_date: {latest_date}")
                else:
                    st.session_state.df = df
                    st.success(f"Loaded {len(df)} job logs for {latest_date}")
        else:
            logger.debug(f"Using cached df for run_date {latest_date}")

    except Exception as e:
        logger.exception("Failed to load data")
        st.error(f"Error loading data: {e}")



header.set_layout()
mode, auto_refresh = sidebar.render_sidebar()

# Auto-refresh
if auto_refresh and mode == "Live from DB":
    time.sleep(30)
    st.experimental_rerun()

if mode == "Live from DB":
    load_data_if_needed()

elif mode == "Parse from logs":
    with st.spinner("Parsing logs and loading into database..."):
        df = load_logs_from_folder(settings.bootstrap_dir)
        if df.empty:
            st.warning("No valid logs found in the log folder.")
        else:
            save_df_to_db(df, if_exists="append")
            create_indexes()
            st.session_state.df = df
            st.success(f"Parsed and saved {len(df)} logs to the database.")


tabs = st.tabs([
    "Predefined Prompts",
    "AI Prompt Interpreter",
    "Admin",
    "Sequence & Drift Analysis",
    "Visualise Prediction Accuracy (trained vs actual)"
])

if st.button("Refresh Data"):
    st.session_state.pop("df", None)
    st.rerun()

df = st.session_state.get("df")

tab_predefined.render(tabs[0], mode, df)
tab_interpreter.render(tabs[1], mode, df)
tab_admin.render(tabs[2])
tab_drift_analysis.render(tabs[3])
tab_accuracy.render(tabs[4])
