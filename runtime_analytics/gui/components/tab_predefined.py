import io
import streamlit as st
import pandas as pd

from loguru import logger
from runtime_analytics.prompts import PREDEFINED_PROMPTS
import sqlite3
from runtime_analytics.app_db.db_loader import load_df_from_db
from runtime_analytics.app_config.config import settings

def render(tab, mode, df):
    with tab:
        # Always load latest data on tab open
        with sqlite3.connect(settings.log_db_path) as conn:
            latest_date = conn.execute("SELECT MAX(run_date) FROM job_logs").fetchone()[0]

        df = load_df_from_db(filters={"run_date": latest_date})
        if df is None or df.empty:
            st.warning("No data available to display.")
            return

        st.subheader("Select a Predefined Prompt to Run")
        selected_prompt = st.selectbox("Choose prompt", list(PREDEFINED_PROMPTS.keys()))

        prompt_info = PREDEFINED_PROMPTS[selected_prompt]
        func = prompt_info["function"]
        params = prompt_info.get("params", {})

        st.info(f"Running predefined prompt: **{selected_prompt}**")
        if params:
            param_str = ", ".join(f"{k} = {v}" for k, v in params.items())
            st.caption(f"With parameters: `{param_str}`")

        try:
            # Show data range info
            if "timestamp" in df.columns:
                min_ts = pd.to_datetime(df["timestamp"]).min()
                max_ts = pd.to_datetime(df["timestamp"]).max()
                st.info(f"Input data covers from **{min_ts.date()}** to **{max_ts.date()}** "
                        f"with {len(df)} records total.")

            result: pd.DataFrame = func(df, **params)
            if result.empty:
                st.warning("The function returned no data.")
                return

            st.success(f"Returned {len(result)} rows.")
            st.dataframe(result)

            csv_buffer = io.StringIO()
            result.to_csv(csv_buffer, index=False)
            st.download_button(
                "Download CSV",
                data=csv_buffer.getvalue(),
                file_name=f"{selected_prompt.replace(' ', '_')}.csv",
            )
        except Exception as e:
            logger.exception("Predefined prompt execution failed")
            st.error(f"Error running prompt: {e}")
