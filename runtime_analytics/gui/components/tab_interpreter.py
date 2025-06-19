import streamlit as st
import pandas as pd

from runtime_analytics.prompt_interpreter import interpret_prompt
from runtime_analytics.prompts import FUNCTION_MAP
from runtime_analytics.ml.learning_store import log_prompt_learning
from runtime_analytics.app_db.db_loader import load_df_from_db
from runtime_analytics.loader import load_logs_from_folder
from runtime_analytics.app_config.config import settings


def render(tab, mode, df):
    with tab:
        if df is None or df.empty:
            st.warning("No data available to display.")
            return
        st.subheader("AI-Powered Prompt Interpreter")

        df = load_logs_from_folder(settings.log_dir) if mode == "Parse from logs" else load_df_from_db()
        if df.empty:
            st.warning("No data found. Try parsing logs first.")
            return

        user_query = st.text_input("Enter your analysis question:")

        if not user_query:
            return

        query = interpret_prompt(user_query)
        func_name = query.get("function")
        params = query.get("params", {})

        if not func_name:
            st.warning("Could not interpret the prompt. Try rephrasing.")
            return

        func = FUNCTION_MAP.get(func_name)
        if not func:
            st.warning(f"Function '{func_name}' not implemented.")
            return

        try:
            st.info(f"Interpreted function: **{func_name}**")
            if params:
                param_str = ", ".join(f"{k} = {v}" for k, v in params.items())
                st.caption(f"With parameters: `{param_str}`")

            output = func(df, **params)
            if isinstance(output, pd.DataFrame):
                cols = list(output.columns)
                display_cols = params.get("include_columns", cols)
                st.dataframe(output[display_cols if display_cols else cols])
            else:
                st.write(output)

            log_prompt_learning(user_query, func_name, 1.0, accepted=True)

        except Exception as e:
            st.error(f"Error running prompt function: {e}")
            log_prompt_learning(user_query, func_name, 0.0, accepted=False)
