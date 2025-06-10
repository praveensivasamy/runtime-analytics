import io
import time

import pandas as pd
import streamlit as st

from runtime_analytics.app_config.config import settings
from runtime_analytics.app_db.sql_db import (
    create_indexes,
    load_df_from_db,
    save_df_to_db,
)
from runtime_analytics.loader import load_logs_from_folder
from runtime_analytics.ml.learning_store import log_prompt_learning
from runtime_analytics.ml.predict_response_time import predict_response_times
from runtime_analytics.prompt_interpreter import interpret_prompt
from runtime_analytics.prompts import FUNCTION_MAP, PREDEFINED_PROMPTS

st.set_page_config(layout="wide", page_title="Runtime Analytics", page_icon="📊")
st.markdown("<h1 style='color:#4A90E2;'>Runtime Analytics Dashboard</h1>", unsafe_allow_html=True)

# Sidebar controls
mode = st.sidebar.radio("📁 Data Source", ["Parse from logs", "Live from DB"], index=0)
auto_refresh = st.sidebar.checkbox("Auto-refresh every 30s", value=False)

if auto_refresh and mode == "Live from DB":
    time.sleep(30)
    st.experimental_rerun()

# Load and predict data
if mode == "Parse from logs":
    df = load_logs_from_folder(settings.log_dir)
    if df.empty:
        st.warning("No valid logs found.")
        st.stop()
    save_df_to_db(df, if_exists="append")
    create_indexes()
    st.success("Parsed logs and updated DB.")
    df = predict_response_times(save_to_db=False)
else:
    df = load_df_from_db()
    if df.empty:
        st.warning("No data found in DB.")
        st.stop()
    df = predict_response_times(save_to_db=False)

key_fields = ["riskdate", "id", "type", "predicted_duration", "rank"]

tabs = st.tabs(["Predefined Prompts", "AI Prompt Interpreter", "Admin"])

with tabs[0]:
    st.subheader("Select a Predefined Prompt to Run")
    selected_prompt = st.selectbox("Choose prompt", list(PREDEFINED_PROMPTS.keys()))
    prompt_info = PREDEFINED_PROMPTS[selected_prompt]
    result = prompt_info["function"](df, **prompt_info["params"])
    st.dataframe(result)

    csv_buffer = io.StringIO()
    result.to_csv(csv_buffer, index=False)
    st.download_button(
        "Download CSV",
        data=csv_buffer.getvalue(),
        file_name=f"{selected_prompt.replace(' ', '_')}.csv",
    )

with tabs[1]:
    st.subheader("AI-Powered Prompt Interpreter")
    user_query = st.text_input("Enter your analysis question:")
    if user_query:
        query = interpret_prompt(user_query)
        func_name = query.get("function")
        params = query.get("params", {})
        if not func_name:
            st.warning("Could not interpret the prompt. Try rephrasing.")
        else:
            func = FUNCTION_MAP.get(func_name)
            if not func:
                st.warning(f"Function '{func_name}' not implemented.")
            else:
                try:
                    output = func(df, **params)
                    # Always show key fields if available
                    if isinstance(output, pd.DataFrame):
                        cols = list(output.columns)
                        include = params.get("include_columns", key_fields)
                        display_cols = [col for col in include if col in cols]
                        st.dataframe(output[display_cols] if display_cols else output)
                    else:
                        st.write(output)
                    log_prompt_learning(user_query, func_name, 1.0, accepted=True)
                except Exception as e:
                    st.error(f"Error running prompt function: {e}")
                    log_prompt_learning(user_query, func_name, 0.0, accepted=False)

with tabs[2]:
    st.subheader("Admin")
    if st.button("🔁 Regenerate Training Prompts CSV"):
        from runtime_analytics.scripts.generate_training_csv import (
            generate_training_prompts_csv,
        )
        output_path = settings.resource_dir / "training_prompts.csv"
        generate_training_prompts_csv(output_path)
        st.success("training_prompts.csv regenerated.")

    if st.button("Train Prompt Model"):
        from runtime_analytics.scripts.train_prompt_model_cli import (
            main as train_prompt_model,
        )
        train_prompt_model()
        st.success("Prompt model trained.")
