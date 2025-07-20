import logging
import sqlite3

import pandas as pd
import streamlit as st

from runtime_analytics.app_config.config import settings
from runtime_analytics.app_db.db_loader import load_df_from_db
from runtime_analytics.prompt_interpreter import interpret_prompt
from runtime_analytics.prompts import FUNCTION_MAP

logger = logging.getLogger(__name__)


def render(tab):
    with tab:
        logger.debug("Rendering AI Prompt Interpreter tab...")
        st.subheader("AI Prompt Interpreter")
        # # Always load fresh data
        # with sqlite3.connect(settings.log_db_path) as conn:
        #     latest_date = conn.execute("SELECT MAX(run_date) FROM job_logs").fetchone()[0]
        #
        # df = load_df_from_db(filters={"run_date": latest_date})
        # if df is None or df.empty:
        #     st.warning("No data available to interpret.")
        #     return
        #
        # prompt_input = st.text_area("Enter your natural language prompt", height=120)
        # if st.button("Run Prompt"):
        #     if not prompt_input.strip():
        #         st.warning("Please enter a prompt.")
        #         return
        #
        #     try:
        #         if "timestamp" in df.columns:
        #             min_ts = pd.to_datetime(df["timestamp"]).min()
        #             max_ts = pd.to_datetime(df["timestamp"]).max()
        #             st.info(f"Input data covers from **{min_ts.date()}** to **{max_ts.date()}** with {len(df)} records total.")
        #
        #         result = interpret_prompt(prompt_input)
        #         func_name = result["function"]
        #         func = FUNCTION_MAP.get(func_name)
        #
        #         if not func:
        #             st.error(f"Function `{func_name}` not found in FUNCTION_MAP.")
        #             return
        #
        #         st.info(f"Matched function: `{func_name}`")
        #         if result.get("params"):
        #             st.caption(f"Parameters: `{result['params']}`")
        #
        #         output_df = func(df, **result.get("params", {}))
        #         if output_df.empty:
        #             st.warning("No results returned.")
        #         else:
        #             st.success(f"{len(output_df)} results returned.")
        #             st.dataframe(output_df)
        #
        #     except Exception as e:
        #         logger.exception("Prompt interpretation failed")
        #         st.error(f"Error running prompt: {e}")