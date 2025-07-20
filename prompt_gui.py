from __future__ import annotations

import sqlite3
import time

import pandas as pd
import streamlit as st

import logging
from runtime_analytics.app_config.config import settings
from runtime_analytics.app_db.db_loader import create_indexes
from runtime_analytics.app_db.db_operations import save_df_to_db
from runtime_analytics.etl.loader import ETLLogLoader

logger = logging.getLogger(__name__)
# Component imports
from runtime_analytics.gui.components import (
    header,
    sidebar,
    tab_accuracy,
    tab_admin,
    tab_drift_analysis,
    tab_interpreter,
    tab_predefined, tab_explorer,
tab_aggregated_duration_comparison,
)

header.set_layout()
mode, auto_refresh = sidebar.render_sidebar()

# Auto-refresh
if auto_refresh and mode == "Live from DB":
    time.sleep(30)
    st.experimental_rerun()

if mode == "Parse from logs":
    with st.spinner("Parsing logs and loading into database..."):
        logger.info("Parsing logs and loading into database...")
        try:
            etl_loader = ETLLogLoader(settings.bootstrap_dir, save_to_db=False)
            df = etl_loader.load()
            if df.empty:
                st.warning("No valid logs found in the log folder.")
            else:
                save_df_to_db(df)
                create_indexes()
                st.success(f"Parsed and saved {len(df)} logs to the database.")
        except Exception as e:
            logger.exception("ETL parsing failed")
            st.error(f"ETL error: {e}")

tabs = st.tabs([
    "🔍 Predefined Prompts",
    "💬 AI Prompt Interpreter",
    "⏱️ Duration Comparison",
    "⚙️ Admin",
    "🔁 Sequence & Drift Analysis",
    "📈 Prediction Accuracy",
    "🧪 Explore Logs (PyGWalker)"
])


if st.button("Refresh Data"):
    st.rerun()


tab_predefined.render(tabs[0])
tab_interpreter.render(tabs[1])
tab_aggregated_duration_comparison.render(tabs[2])
tab_admin.render(tabs[3])
tab_drift_analysis.render(tabs[4])
tab_accuracy.render(tabs[5])
tab_explorer.render(tabs[6])