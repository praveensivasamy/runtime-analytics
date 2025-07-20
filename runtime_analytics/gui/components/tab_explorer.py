from __future__ import annotations

import streamlit as st
import pygwalker as pyg
from runtime_analytics.repositories.job_log_repository import select_logs_from_db_with_filters


def render(tab) -> None:
    with tab:
        st.subheader("🧪 Explore Logs with PyGWalker")

        # 1. Use a hardcoded or previously known date range
        #    Or allow users to choose without knowing bounds
        start_date = st.date_input("Start Risk Date")
        end_date = st.date_input("End Risk Date")

        # 2. Load only when both dates are selected and button clicked
        if start_date and end_date:
            if st.button("🔍 Load Logs for Selected Risk Date Range"):
                try:
                    filters = {"riskdate >= ": str(start_date), "riskdate <= ": str(end_date)}
                    df = select_logs_from_db_with_filters(filters)
                except Exception as e:
                    st.error(f"Failed to load logs: {e}")
                    return

                if df.empty:
                    st.warning("No logs found for selected date range.")
                    return

                st.success(f"Loaded {len(df)} records from {start_date} to {end_date}")

                # Optional: filter out large/unneeded columns
                excluded_cols = {"raw_log", "error_trace", "json_payload"}
                visible_cols = [col for col in df.columns if col not in excluded_cols]
                df_explore = df[visible_cols].copy()
                try:
                    html = pyg.to_html(df_explore)
                    st.components.v1.html(html, height=800, scrolling=True)
                except Exception as e:
                    st.error(f"PyGWalker rendering failed: {e}")