import io
import streamlit as st
import pandas as pd

from runtime_analytics.prompts import PREDEFINED_PROMPTS


def render(tab, mode, df):
    with tab:
        if df is None or df.empty:
            st.warning("No data available to display.")
            return
        st.subheader("Select a Predefined Prompt to Run")
        selected_prompt = st.selectbox("Choose prompt", list(PREDEFINED_PROMPTS.keys()))
        prompt_info = PREDEFINED_PROMPTS[selected_prompt]

        st.info(f"Running predefined prompt: **{selected_prompt}**")
        if prompt_info.get("params"):
            param_str = ", ".join(f"{k} = {v}" for k, v in prompt_info["params"].items())
            st.caption(f"With parameters: `{param_str}`")

        from runtime_analytics.app_db.db_loader import load_df_from_db
        from runtime_analytics.loader import load_logs_from_folder
        from runtime_analytics.app_config.config import settings

        df = load_logs_from_folder(settings.log_dir) if mode == "Parse from logs" else load_df_from_db()

        result = prompt_info["function"](df, **prompt_info["params"])
        st.dataframe(result)

        csv_buffer = io.StringIO()
        result.to_csv(csv_buffer, index=False)
        st.download_button(
            "Download CSV",
            data=csv_buffer.getvalue(),
            file_name=f"{selected_prompt.replace(' ', '_')}.csv",
        )
